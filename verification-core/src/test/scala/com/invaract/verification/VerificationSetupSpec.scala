// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, ContractRule, OrgPolicyParseException}
import com.invaract.verification.location.LocationResolutionException
import com.invaract.verification.notification.{ContractValidationEvent, JobInfo, TestNotificationSink}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** `VerificationSetup` reads configuration only through a `ConfigSource`, so every setting a
  * platform can attach is tested here with a plain map - no engine session. (The Spark specs prove
  * the same keys through a real `SparkSession`'s `--conf` surface.)
  */
class VerificationSetupSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var scratch: Path = _

  override def beforeAll(): Unit = scratch = Files.createTempDirectory("invaract-setup-test")

  override def afterAll(): Unit = {
    Files.list(scratch).forEach(p => Files.delete(p))
    Files.delete(scratch)
  }

  private def file(name: String, content: String): String = {
    val p = scratch.resolve(name)
    Files.write(p, content.getBytes("UTF-8"))
    p.toString
  }

  private def config(entries: (String, String)*): ConfigSource = ConfigSource.fromMap(entries.toMap)

  private def validationEvents(sink: TestNotificationSink) = sink.events.collect { case e: ContractValidationEvent => e }

  private val plainContract: Contract = ContractParser.parse(
    """id: setup
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
  )

  private val refContract: Contract = ContractParser.parse(
    """id: setup
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: ref://gold_out
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |""".stripMargin
  )

  // --- locations -------------------------------------------------------------------------------

  test("resolveContractLocations: with no locationMap and no ref:// a contract is returned exactly as given") {
    assert(VerificationSetup.resolveContractLocations(plainContract, ConfigSource.empty) == plainContract)
  }

  test("resolveContractLocations: ref:// locations resolve against the configured .properties file") {
    val map = file("locations.properties", "gold_out=s3://bucket/gold/out\n")
    val resolved = VerificationSetup.resolveContractLocations(refContract, config(InvaractConf.LocationMap -> map))
    assert(resolved.outputs.map(_.location) == List("s3://bucket/gold/out"))
  }

  test("resolveContractLocations: a ref:// with nothing configured fails clearly instead of becoming a downstream mismatch") {
    intercept[LocationResolutionException] {
      VerificationSetup.resolveContractLocations(refContract, ConfigSource.empty)
    }
  }

  // --- options ---------------------------------------------------------------------------------

  private val allFlags = List(
    InvaractConf.RejectUndeclaredInputs -> ((o: VerificationOptions) => o.rejectUndeclaredInputs),
    InvaractConf.RejectUndeclaredFields -> ((o: VerificationOptions) => o.rejectUndeclaredFields),
    InvaractConf.ComputeFingerprint -> ((o: VerificationOptions) => o.computeFingerprint),
    InvaractConf.StaticDataQuality -> ((o: VerificationOptions) => o.staticDataQuality),
    InvaractConf.RoleConsistency -> ((o: VerificationOptions) => o.roleConsistency)
  )

  test("resolveVerificationOptions: nothing configured leaves the caller's options exactly as passed") {
    assert(VerificationSetup.resolveVerificationOptions(VerificationOptions(), ConfigSource.empty) == VerificationOptions())
    val custom = VerificationOptions(rejectUndeclaredFields = true, computeFingerprint = true)
    assert(VerificationSetup.resolveVerificationOptions(custom, ConfigSource.empty) == custom)
  }

  test("resolveVerificationOptions: each key turns on exactly its own flag and no other") {
    allFlags.foreach { case (name, flag) =>
      val resolved = VerificationSetup.resolveVerificationOptions(VerificationOptions(), config(name -> "true"))
      assert(flag(resolved), name)
      assert(allFlags.filter(_._1 != name).forall { case (_, other) => !other(resolved) }, name)
    }
  }

  test("resolveVerificationOptions: configuration can only ever raise a flag - 'false' never turns off what code turned on") {
    allFlags.foreach { case (name, flag) =>
      assert(!flag(VerificationSetup.resolveVerificationOptions(VerificationOptions(), config(name -> "false"))), name)
    }
    val allOn = VerificationOptions(true, true, true, true, true)
    assert(VerificationSetup.resolveVerificationOptions(allOn, config(allFlags.map(_._1 -> "false"): _*)) == allOn)
  }

  // --- org policy: layers ----------------------------------------------------------------------

  private val catalogRequired =
    """version: "1.0"
      |policies:
      |  - id: catalog-required
      |    type: require_catalog
      |    scope: outputs
      |""".stripMargin

  test("resolveOrgPolicyLayers: none configured is Nil; a base is parsed and paired with its path") {
    assert(VerificationSetup.resolveOrgPolicyLayers(ConfigSource.empty).isEmpty)
    val base = file("base.yaml", catalogRequired)
    val layers = VerificationSetup.resolveOrgPolicyLayers(config(InvaractConf.OrgPolicy -> base))
    assert(layers.map(_._1) == List(base))
  }

  test("resolveOrgPolicyLayers: overlays follow the base in the order listed, blanks ignored") {
    val base = file("base2.yaml", catalogRequired)
    val o1 = file("o1.yaml", "version: \"1.0\"\n")
    val o2 = file("o2.yaml", "version: \"1.0\"\n")
    val layers = VerificationSetup.resolveOrgPolicyLayers(config(InvaractConf.OrgPolicy -> base, InvaractConf.OrgPolicyOverlays -> s"$o1 , ,$o2"))
    assert(layers.map(_._1) == List(base, o1, o2))
  }

  test("resolveOrgPolicyLayers: overlays without a base are rejected, naming both keys the way the engine spells them") {
    val source = ConfigSource.prefixed("spark.invaract.")(Map("spark.invaract.orgPolicyOverlays" -> "/o.yaml").get)
    val ex = intercept[OrgPolicyParseException] { VerificationSetup.resolveOrgPolicyLayers(source) }
    assert(ex.getMessage.contains("'spark.invaract.orgPolicyOverlays' is set (/o.yaml) but 'spark.invaract.orgPolicy' is not"))
  }

  // --- org policy: enforcement -----------------------------------------------------------------

  test("enforceOrgPolicy: no policy configured returns the contract and options untouched and publishes nothing") {
    val sink = new TestNotificationSink
    val out = VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), ConfigSource.empty, Some(sink), None)
    assert(out == ((plainContract, VerificationOptions())))
    assert(sink.events.isEmpty)
  }

  test("enforceOrgPolicy: an Enforce violation rejects the contract before any plan exists, after publishing FAILED") {
    val policy = file("enforce.yaml", catalogRequired)
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> policy), Some(sink), Some("app-3"))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.OrgPolicyViolation))
    assert(ex.result.violations.head.message.contains("catalog-required"))
    assert(ex.result.violations.head.location.contains("some/output/path"))
    assert(ex.getMessage.contains("rejected by organizational policy before any plan was analyzed"))
    assert(validationEvents(sink).map(_.status) == List("FAILED"))
    assert(validationEvents(sink).head.runId.contains("app-3"))
    assert(validationEvents(sink).head.job.isEmpty, "no job was supplied")
  }

  test("enforceOrgPolicy: a policy the contract satisfies blocks nothing and publishes nothing") {
    val satisfied = ContractParser.parse(
      """id: setup
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: some/output/path
        |    schema:
        |      fields:
        |        - name: id
        |          type: long
        |    catalog:
        |      required: true
        |""".stripMargin
    )
    val sink = new TestNotificationSink
    val (governed, options) =
      VerificationSetup.enforceOrgPolicy(satisfied, VerificationOptions(), config(InvaractConf.OrgPolicy -> file("satisfied.yaml", catalogRequired)), Some(sink), None)
    assert(governed == satisfied && options == VerificationOptions())
    assert(sink.events.isEmpty)
  }

  test("enforceOrgPolicy: a Warn policy never blocks, but its violation is published as PASSED-with-warnings") {
    val policy = file(
      "warn.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-wanted
        |    type: require_catalog
        |    scope: outputs
        |    mode: warn
        |""".stripMargin
    )
    val sink = new TestNotificationSink
    val (governed, _) = VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> policy), Some(sink), None)
    assert(governed == plainContract)
    val events = validationEvents(sink)
    assert(events.map(_.status) == List("PASSED"))
    assert(events.head.violations.map(_.violationType) == List(ViolationType.OrgPolicyViolation))
    // and with no sink it is silent but still does not block
    VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> policy), None, None)
  }

  test("enforceOrgPolicy: injected rules reach the governed contract and minVerificationOptions raise the floor") {
    val policy = file(
      "inject.yaml",
      """version: "1.0"
        |inject:
        |  rules:
        |    - type: forbid_unconditional_delete
        |  minVerificationOptions:
        |    rejectUndeclaredFields: true
        |""".stripMargin
    )
    val (governed, options) =
      VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(computeFingerprint = true), config(InvaractConf.OrgPolicy -> policy), None, None)
    assert(governed.rules == List(ContractRule("forbid_unconditional_delete", Map.empty)))
    assert(options == VerificationOptions(rejectUndeclaredFields = true, computeFingerprint = true))
  }

  test("enforceOrgPolicy: an invalid stack of layers is rejected, not partially applied") {
    val ok = file("ok_layer.yaml", catalogRequired)
    val broken = file("broken_layer.yaml", "version: \"1.0\"\npolicies:\n  - id: needs-a-name\n    type: require_field\n") // require_field without 'name'
    val ex = intercept[OrgPolicyParseException] {
      VerificationSetup.enforceOrgPolicy(
        plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> ok, InvaractConf.OrgPolicyOverlays -> broken), None, None
      )
    }
    assert(ex.getMessage.startsWith("Organizational policy layers are invalid:"))
  }

  test("enforceOrgPolicy: a typo'd minVerificationOptions key is rejected, naming the file and the known keys") {
    val bad = file("typo.yaml", "version: \"1.0\"\ninject:\n  minVerificationOptions:\n    rejectUndeclredFields: true\n")
    val ex = intercept[OrgPolicyParseException] {
      VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> bad), None, None)
    }
    assert(ex.getMessage.contains(bad) && ex.getMessage.contains("rejectUndeclredFields"))
    assert(ex.getMessage.contains("known keys: computeFingerprint, rejectUndeclaredFields, rejectUndeclaredInputs, roleConsistency, staticDataQuality"))
  }

  // --- small helpers ---------------------------------------------------------------------------

  test("applyMinVerificationOptions only raises: a floor of false or an absent floor changes nothing") {
    val policy = com.invaract.contract.OrgPolicyParser.parse("version: \"1.0\"\ninject:\n  minVerificationOptions:\n    roleConsistency: true\n    computeFingerprint: false\n")
    val raised = VerificationSetup.applyMinVerificationOptions(VerificationOptions(computeFingerprint = true), policy)
    assert(raised == VerificationOptions(computeFingerprint = true, roleConsistency = true))
  }

  test("splitCommaSeparated trims and drops blanks") {
    assert(VerificationSetup.splitCommaSeparated(" a, b ,, c ,") == List("a", "b", "c"))
    assert(VerificationSetup.splitCommaSeparated("").isEmpty)
  }

  test("enforceOrgPolicy: the rejection event names the job it was given") {
    val policy = file("enforce-job.yaml", catalogRequired)
    val job = JobInfo(runId = Some("run-1"), jobId = Some("orders_nightly"), engine = Some("toy"))
    val sink = new TestNotificationSink
    intercept[ContractViolationException] {
      VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> policy), Some(sink), None, Some(job))
    }
    assert(validationEvents(sink).map(_.job) == List(Some(job)))
    assert(validationEvents(sink).head.runId.contains("run-1"), "the run id comes from the job when none is given")
  }

  test("enforceOrgPolicy: a Warn-only policy's informational event names the job too") {
    val policy = file("warn-job.yaml", catalogRequired + "    mode: warn\n")
    val job = JobInfo(engine = Some("toy"))
    val sink = new TestNotificationSink
    VerificationSetup.enforceOrgPolicy(plainContract, VerificationOptions(), config(InvaractConf.OrgPolicy -> policy), Some(sink), None, Some(job))
    assert(validationEvents(sink).map(e => e.status -> e.job) == List("PASSED" -> Some(job)))
  }
}
