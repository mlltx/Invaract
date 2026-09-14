// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.time.LocalDate

class OrgPolicyParserTest extends AnyFunSuite {

  private def fixture(name: String): File =
    new File(s"src/test/resources/fixtures/org-policy/$name")

  test("parseFile should parse a basic policy document into the object model") {
    val policy = OrgPolicyParser.parseFile(fixture("valid_basic.yaml"))

    assert(policy.version == "1.0")
    assert(policy.policies.size == 3)

    val catalogRule = policy.policies.find(_.id == "catalog-required").get
    assert(catalogRule.ruleType == PolicyType.RequireCatalog)
    assert(catalogRule.scope == PolicyScope.Outputs)
    assert(catalogRule.mode == PolicyMode.Enforce) // default
    assert(catalogRule.description.contains("All outputs must be registered in a discoverable catalog."))
    assert(catalogRule.interpret.contains(InterpretedPolicy.RequireCatalog(Some("hive"))))

    val fieldRule = policy.policies.find(_.id == "pii-flag-required").get
    assert(fieldRule.interpret.contains(InterpretedPolicy.RequireField("pii_reviewed", None)))

    val namingRule = policy.policies.find(_.id == "snake-case-fields").get
    assert(namingRule.scope == PolicyScope.All) // default, not declared in fixture
    assert(namingRule.interpret.contains(InterpretedPolicy.FieldNamingConvention("^[a-z][a-z0-9_]*$")))
  }

  // A real regression test for a real bug: require_field's optional
  // type-pin property collided with the policy rule's own 'type'
  // discriminator key when both were spelled 'type' at the same YAML
  // mapping level - SnakeYAML doesn't reject the duplicate key, it
  // silently keeps only the last one, corrupting ruleType itself into
  // whatever field type was named rather than raising anything. Every
  // other test exercising RequireField's type pin constructed PolicyRule
  // directly in Scala, bypassing OrgPolicyParser entirely, so none of
  // them could have caught this - only a real parse of real YAML can.
  test("parse should correctly decode require_field's fieldType pin through a real YAML round-trip") {
    val yaml =
      """version: "1.0"
        |policies:
        |  - id: typed-field
        |    type: require_field
        |    name: id
        |    fieldType: string
        |""".stripMargin
    val policy = OrgPolicyParser.parse(yaml)
    val rule = policy.policies.head
    assert(rule.ruleType == PolicyType.RequireField, "the ruleType must not be clobbered by the fieldType sub-property")
    assert(rule.interpret.contains(InterpretedPolicy.RequireField("id", Some("string"))))
  }

  test("parseFile should parse a 'when' condition, 'warn' mode, and 'inject' block") {
    val policy = OrgPolicyParser.parseFile(fixture("valid_with_condition_and_injection.yaml"))

    val rule = policy.policies.head
    assert(rule.when.contains(PolicyCondition("pii")))
    assert(rule.mode == PolicyMode.Warn)

    assert(policy.inject.rules == List(ContractRule("forbid_unconditional_delete", Map.empty)))
    assert(policy.inject.minVerificationOptions == Map("rejectUndeclaredFields" -> true))
  }

  test("parseFile should parse exemptions, including a reviewBy date") {
    val policy = OrgPolicyParser.parseFile(fixture("valid_with_exemptions.yaml"))

    assert(policy.exemptions.size == 2)
    val active = policy.exemptions.find(_.contractId == "legacy_customer_orders").get
    assert(active.policyIds == List("catalog-required"))
    assert(active.reviewBy.contains(LocalDate.of(2099, 1, 1)))
    assert(active.reason.nonEmpty)
  }

  test("parse should accept an equivalent YAML string with no policies/inject/exemptions") {
    val policy = OrgPolicyParser.parse("version: \"1.0\"\n")
    assert(policy.version == "1.0")
    assert(policy.policies.isEmpty)
    assert(policy.inject == InjectedDefaults())
    assert(policy.exemptions.isEmpty)
  }

  test("parseFile should throw when the file does not exist") {
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parseFile(fixture("does_not_exist.yaml"))
    }
  }

  test("parseFile should throw when 'version' is missing") {
    val ex = intercept[OrgPolicyParseException] {
      OrgPolicyParser.parseFile(fixture("invalid_missing_version.yaml"))
    }
    assert(ex.getMessage.contains("version"))
  }

  test("parseFile should throw when a policy rule is missing 'id'") {
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parseFile(fixture("invalid_policy_missing_id.yaml"))
    }
  }

  test("parseFile should throw when a policy rule is missing 'type'") {
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parseFile(fixture("invalid_policy_missing_type.yaml"))
    }
  }

  test("parseFile should accept duplicate policy ids structurally (OrgPolicyValidator's job to reject)") {
    val policy = OrgPolicyParser.parseFile(fixture("invalid_duplicate_policy_id.yaml"))
    assert(policy.policies.map(_.id) == List("dup", "dup"))
  }

  test("parseFile should throw when an exemption's reviewBy is not a valid date") {
    val ex = intercept[OrgPolicyParseException] {
      OrgPolicyParser.parseFile(fixture("invalid_exemption_bad_date.yaml"))
    }
    assert(ex.getMessage.contains("reviewBy"))
  }

  test("parse should reject an invalid 'scope' value") {
    val yaml =
      """version: "1.0"
        |policies:
        |  - id: x
        |    type: require_catalog
        |    scope: everywhere
        |""".stripMargin
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parse(yaml)
    }
  }

  test("parse should reject an invalid 'mode' value") {
    val yaml =
      """version: "1.0"
        |policies:
        |  - id: x
        |    type: require_catalog
        |    mode: sometimes
        |""".stripMargin
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parse(yaml)
    }
  }

  test("parse should reject an exemption with an empty policyIds list") {
    val yaml =
      """version: "1.0"
        |exemptions:
        |  - contractId: foo
        |    policyIds: []
        |    reason: "test"
        |""".stripMargin
    assertThrows[OrgPolicyParseException] {
      OrgPolicyParser.parse(yaml)
    }
  }
}
