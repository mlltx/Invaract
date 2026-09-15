// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractParser, ContractRule, OrgPolicyParseException}
import com.invaract.sparkadapter.notification.TestNotificationSink

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Tests `ContractEnforcementRule`'s organizational-policy wiring
  * (`spark.invaract.orgPolicy`, `enforceOrgPolicy`) — see
  * docs/CONTRACT_MODEL.md's "Organizational Policy" section for the design.
  * Kept separate from `ContractEnforcementRuleSpec` (which this file
  * otherwise mirrors in style) since org-policy enforcement runs entirely
  * inside `forContract`'s outer `session => {...}` closure, *before* any
  * `LogicalPlan` exists — so most of these tests call `rule(spark)` directly
  * and assert on what happens right there, with no write, no captured plan,
  * and no second `SparkSession` needed at all. `OrgPolicyEvaluator`'s own
  * rule-by-rule behavior (scope, `when`, exemptions, mode) is already
  * exhaustively tested in `contract`'s `OrgPolicyEvaluatorTest` — these
  * tests are about the *wiring*: does the conf key get read, does an
  * Enforce violation actually block before a plan is analyzed, does
  * injection actually reach `RuleVerifier`/`VerificationOptions`, does a
  * malformed policy document fail loudly.
  */
class ContractEnforcementRuleOrgPolicySpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-org-policy-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleOrgPolicySpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def parseContract(yaml: String) = ContractParser.parse(yaml)

  private def writePolicy(name: String, yaml: String): String = {
    val path = scratchDir.resolve(name)
    Files.write(path, yaml.getBytes("UTF-8"))
    path.toString
  }

  private def withOrgPolicyConf[T](path: String)(body: => T): T = {
    spark.conf.set(ContractEnforcementRule.OrgPolicyConfKey, path)
    try body
    finally spark.conf.unset(ContractEnforcementRule.OrgPolicyConfKey)
  }

  private val noCatalogContractYaml =
    """id: org_policy_demo
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: some/output/path
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |""".stripMargin

  private val catalogRegisteredContractYaml =
    """id: org_policy_demo
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: some/output/path
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |    catalog:
      |      required: true
      |""".stripMargin

  // -- no policy configured: today's behavior, unchanged -------------------

  test("no org policy configured: forContract's session closure runs without incident") {
    val contract = parseContract(noCatalogContractYaml)
    val rule = ContractEnforcementRule.forContract(contract)
    rule(spark) // must not throw - OrgPolicyConfKey is unset
  }

  test("resolveOrgPolicy returns None when the conf key is unset") {
    assert(ContractEnforcementRule.resolveOrgPolicy(spark).isEmpty)
  }

  test("resolveOrgPolicy parses the document named by spark.invaract.orgPolicy") {
    val policyPath = writePolicy(
      "basic.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |""".stripMargin
    )
    val policy = withOrgPolicyConf(policyPath) {
      ContractEnforcementRule.resolveOrgPolicy(spark)
    }
    assert(policy.map(_.version).contains("1.0"))
    assert(policy.exists(_.policies.exists(_.id == "catalog-required")))
  }

  // -- eager, "stop ASAP" enforcement ---------------------------------------

  test("enforce mode: a contract violating org policy throws before any plan is ever analyzed") {
    val contract = parseContract(noCatalogContractYaml) // no catalog block declared
    val policyPath = writePolicy(
      "require_catalog_enforce.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)

    val ex = withOrgPolicyConf(policyPath) {
      // rule(spark) is exactly forContract's outer closure - the same
      // moment Spark itself invokes injectCheckRule's builder function
      // during real session construction. Throwing here, with no
      // LogicalPlan in sight, is the proof: a non-compliant contract can't
      // even finish installing, let alone reach a write.
      intercept[ContractViolationException] { rule(spark) }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.OrgPolicyViolation))
    assert(ex.result.violations.exists(_.message.contains("catalog-required")))
    assert(ex.getMessage.contains("organizational policy"))
  }

  test("enforce mode: a contract satisfying org policy installs without incident") {
    val contract = parseContract(catalogRegisteredContractYaml)
    val policyPath = writePolicy(
      "require_catalog_satisfied.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    withOrgPolicyConf(policyPath) {
      rule(spark) // must not throw
    }
  }

  // A real, full-path regression test for the require_field fieldType-pin
  // bug (see docs/CONTRACT_MODEL.md's "Organizational Policy" section):
  // proves the fix through the exact real path a platform would use
  // (spark.invaract.orgPolicy -> OrgPolicyParser -> ContractEnforcementRule),
  // not just contract's own parser-level test.
  test("require_field with a fieldType pin: satisfied when the field exists with a matching type") {
    val contract = parseContract(noCatalogContractYaml) // declares 'id: long'
    val policyPath = writePolicy(
      "require_field_type_pin_satisfied.yaml",
      """version: "1.0"
        |policies:
        |  - id: id-must-be-long
        |    type: require_field
        |    name: id
        |    fieldType: long
        |    scope: outputs
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    withOrgPolicyConf(policyPath) {
      rule(spark) // must not throw
    }
  }

  test("require_field with a fieldType pin: violated when the field's type disagrees") {
    val contract = parseContract(noCatalogContractYaml) // declares 'id: long'
    val policyPath = writePolicy(
      "require_field_type_pin_violated.yaml",
      """version: "1.0"
        |policies:
        |  - id: id-must-be-string
        |    type: require_field
        |    name: id
        |    fieldType: string
        |    scope: outputs
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    val ex = withOrgPolicyConf(policyPath) {
      intercept[ContractViolationException] { rule(spark) }
    }
    assert(ex.result.violations.exists(v => v.violationType == ViolationType.OrgPolicyViolation && v.message.contains("id-must-be-string")))
  }

  test("enforce mode: an unexpired exemption suppresses the violation, real conf-driven end to end") {
    val contract = parseContract(noCatalogContractYaml)
    val policyPath = writePolicy(
      "require_catalog_exempt.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |exemptions:
        |  - contractId: org_policy_demo
        |    policyIds: [catalog-required]
        |    reason: "test exemption"
        |    reviewBy: "2099-01-01"
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    withOrgPolicyConf(policyPath) {
      rule(spark) // must not throw - exempted
    }
  }

  test("enforce mode: an expired exemption no longer suppresses the violation") {
    val contract = parseContract(noCatalogContractYaml)
    val policyPath = writePolicy(
      "require_catalog_expired_exempt.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |exemptions:
        |  - contractId: org_policy_demo
        |    policyIds: [catalog-required]
        |    reason: "test exemption, expired"
        |    reviewBy: "2000-01-01"
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    withOrgPolicyConf(policyPath) {
      intercept[ContractViolationException] { rule(spark) }
    }
  }

  // -- warn mode: never blocks, publishes for visibility --------------------

  test("warn mode: a violation never throws, even with no sink configured") {
    val contract = parseContract(noCatalogContractYaml)
    val policyPath = writePolicy(
      "require_catalog_warn.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |    mode: warn
        |""".stripMargin
    )
    val rule = ContractEnforcementRule.forContract(contract)
    withOrgPolicyConf(policyPath) {
      rule(spark) // must not throw - Warn mode never blocks
    }
  }

  test("warn mode: a violation publishes a PASSED-status event carrying the violation, via the sink overload") {
    val contract = parseContract(noCatalogContractYaml)
    val policyPath = writePolicy(
      "require_catalog_warn_sink.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |    mode: warn
        |""".stripMargin
    )
    val sink = new TestNotificationSink
    val rule = ContractEnforcementRule.forContract(contract, VerificationOptions(), sink)

    withOrgPolicyConf(policyPath) {
      rule(spark) // must not throw
    }

    val events = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }
    assert(events.nonEmpty, "expected a ContractValidationEvent for the Warn-mode violation")
    val event = events.last
    // PASSED, not FAILED: nothing was actually blocked - a Warn violation
    // is advisory, and VerificationResult.of would have misreported this
    // as FAILED had enforceOrgPolicy built it that way.
    assert(event.status == "PASSED")
    assert(event.violations.exists(_.violationType == ViolationType.OrgPolicyViolation))
  }

  test("enforce mode with the sink overload: the sink receives a FAILED event before the exception propagates") {
    val contract = parseContract(noCatalogContractYaml)
    val policyPath = writePolicy(
      "require_catalog_enforce_sink.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |""".stripMargin
    )
    val sink = new TestNotificationSink
    val rule = ContractEnforcementRule.forContract(contract, VerificationOptions(), sink)

    withOrgPolicyConf(policyPath) {
      intercept[ContractViolationException] { rule(spark) }
    }

    val event = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last
    assert(event.status == "FAILED")
    assert(event.violations.exists(_.violationType == ViolationType.OrgPolicyViolation))
    assert(event.applicationId.isDefined, "the sink overload always threads a real applicationId through")
  }

  // -- injection: reuses RuleVerifier/VerificationOptions, no new evaluator ---

  test("enforceOrgPolicy injects policy.inject.rules into the governed contract's own rules") {
    val contract = parseContract(catalogRegisteredContractYaml)
    assert(contract.rules.isEmpty)
    val policyPath = writePolicy(
      "inject_rule.yaml",
      """version: "1.0"
        |inject:
        |  rules:
        |    - type: forbid_unconditional_delete
        |""".stripMargin
    )
    val (governedContract, _) = withOrgPolicyConf(policyPath) {
      ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
    }
    assert(governedContract.rules == List(ContractRule("forbid_unconditional_delete", Map.empty)))
  }

  test("enforceOrgPolicy ORs policy.inject.minVerificationOptions floors onto VerificationOptions") {
    val contract = parseContract(catalogRegisteredContractYaml)
    val policyPath = writePolicy(
      "inject_options.yaml",
      """version: "1.0"
        |inject:
        |  minVerificationOptions:
        |    rejectUndeclaredFields: true
        |""".stripMargin
    )
    val (_, governedOptions) = withOrgPolicyConf(policyPath) {
      ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
    }
    assert(governedOptions.rejectUndeclaredFields)
    // Only the floor named by the policy moves - the others stay at
    // whatever the caller's own VerificationOptions already had.
    assert(!governedOptions.rejectUndeclaredInputs)
    assert(!governedOptions.computeFingerprint)
  }

  test("enforceOrgPolicy rejects an unrecognized inject.minVerificationOptions key rather than silently ignoring it") {
    val contract = parseContract(catalogRegisteredContractYaml)
    // A real typo shape - missing the 'e' in 'Undeclared' - not a
    // hypothetical: this is exactly the class of mistake a platform team
    // could make when hand-writing YAML, and it must fail loudly rather
    // than silently leaving rejectUndeclaredFields at its default.
    val policyPath = writePolicy(
      "inject_options_typo.yaml",
      """version: "1.0"
        |inject:
        |  minVerificationOptions:
        |    rejectUndeclredFields: true
        |""".stripMargin
    )
    val ex = withOrgPolicyConf(policyPath) {
      intercept[OrgPolicyParseException] {
        ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
      }
    }
    assert(ex.getMessage.contains("rejectUndeclredFields"))
  }

  test("enforceOrgPolicy never weakens a flag the caller already set true") {
    val contract = parseContract(catalogRegisteredContractYaml)
    val policyPath = writePolicy("inject_options_noop.yaml", "version: \"1.0\"\n")
    val (_, governedOptions) = withOrgPolicyConf(policyPath) {
      ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(computeFingerprint = true), spark, None, None)
    }
    assert(governedOptions.computeFingerprint)
  }

  // -- a malformed policy document fails loudly, not silently -------------

  test("a policy document with duplicate policy ids throws OrgPolicyParseException rather than silently applying") {
    val contract = parseContract(catalogRegisteredContractYaml)
    val policyPath = writePolicy(
      "invalid_duplicate.yaml",
      """version: "1.0"
        |policies:
        |  - id: dup
        |    type: require_catalog
        |  - id: dup
        |    type: field_naming_convention
        |    pattern: "^[a-z]+$"
        |""".stripMargin
    )
    withOrgPolicyConf(policyPath) {
      intercept[OrgPolicyParseException] {
        ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
      }
    }
  }

  test("a policy document that fails to parse at all throws OrgPolicyParseException") {
    val contract = parseContract(catalogRegisteredContractYaml)
    val policyPath = writePolicy("invalid_no_version.yaml", "policies: []\n") // missing required 'version'
    withOrgPolicyConf(policyPath) {
      intercept[OrgPolicyParseException] {
        ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
      }
    }
  }
}
