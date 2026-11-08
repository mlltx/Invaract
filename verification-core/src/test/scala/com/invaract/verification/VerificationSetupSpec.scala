// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, ContractRule, OrgPolicyParseException}
import com.invaract.verification.location.{LocationResolutionException, TestLocationService}
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
    val e = intercept[LocationResolutionException] {
      VerificationSetup.resolveContractLocations(refContract, ConfigSource.empty)
    }
    assert(
      e.getMessage == "The contract declares 'ref://gold_out', but no location map or location service is set: set 'locationMap' to a " +
        ".properties file with a line 'gold_out=<real location>', or 'locationUrl' to a location service."
    )
    val spark = ConfigSource.prefixed("spark.invaract.")(_ => None)
    val message = intercept[LocationResolutionException](VerificationSetup.resolveContractLocations(refContract, spark)).getMessage
    assert(message.contains("set 'spark.invaract.locationMap' to") && message.contains("or 'spark.invaract.locationUrl' to"))
  }

  // the location service (HttpLocationResolver; its own spec holds the protocol)

  private def located(contract: Contract, environment: Map[String, String], entries: (String, String)*): List[String] =
    VerificationSetup.resolveContractLocations(contract, config(entries: _*), environment.get).outputs.map(_.location)

  private val twoRefs: Contract = ContractParser.parse(
    """id: setup
      |version: "1.0.0"
      |outputs:
      |  - { name: gold, location: "ref://gold_out" }
      |  - { name: silver, location: "ref://silver_out" }
      |""".stripMargin
  )

  test("resolveContractLocations: locationUrl resolves each ref:// by asking the location service") {
    val service = TestLocationService.holding("gold_out" -> "s3://lake/gold", "silver_out" -> "s3://lake/silver")
    try {
      assert(located(twoRefs, Map.empty, InvaractConf.LocationUrl -> service.url) == List("s3://lake/gold", "s3://lake/silver"))
      assert(service.paths == Vector("/locations/gold_out", "/locations/silver_out") && service.authorizations.forall(_.isEmpty))
    } finally service.close()
  }

  test("resolveContractLocations: the location map is asked first, and an id it holds is never asked of the service") {
    val service = TestLocationService.holding("gold_out" -> "s3://lake/gold", "silver_out" -> "s3://lake/silver")
    try {
      val map = file("override.properties", "gold_out=/local/gold\n")
      val both = Seq(InvaractConf.LocationMap -> map, InvaractConf.LocationUrl -> service.url)
      assert(located(twoRefs, Map.empty, both: _*) == List("/local/gold", "s3://lake/silver"))
      assert(service.paths == Vector("/locations/silver_out"))
    } finally service.close()
  }

  test("resolveContractLocations: with a map and a service, an id neither holds fails with the service's reason") {
    val service = TestLocationService.holding()
    try {
      val map = file("unrelated.properties", "bronze_out=/b\n")
      val e = intercept[LocationResolutionException](
        located(refContract, Map.empty, InvaractConf.LocationMap -> map, InvaractConf.LocationUrl -> service.url)
      )
      assert(e.getMessage == s"The location service could not resolve 'ref://gold_out' (${service.url}/locations/gold_out): it has no location for that id")
    } finally service.close()
  }

  test("resolveContractLocations: locationTokenEnv sends the named variable's value as a bearer token, never naming it in config") {
    val service = TestLocationService.holding("gold_out" -> "/g")
    try {
      located(refContract, Map("LOCATIONS_TOKEN" -> " t0ken "), InvaractConf.LocationUrl -> service.url, InvaractConf.LocationTokenEnv -> "LOCATIONS_TOKEN")
      assert(service.authorizations == Vector(Some("Bearer t0ken")))
    } finally service.close()
  }

  test("resolveContractLocations: a token variable that is not set, or a token with no service, is refused even with no ref://") {
    for (environment <- List(Map.empty[String, String], Map("LOCATIONS_TOKEN" -> "  "))) {
      val e = intercept[LocationResolutionException](
        located(plainContract, environment, InvaractConf.LocationUrl -> "http://127.0.0.1:9", InvaractConf.LocationTokenEnv -> "LOCATIONS_TOKEN")
      )
      assert(e.getMessage == "'locationTokenEnv' names the environment variable 'LOCATIONS_TOKEN', which is not set.")
    }
    val e = intercept[LocationResolutionException](located(plainContract, Map("T" -> "x"), InvaractConf.LocationTokenEnv -> "T"))
    assert(e.getMessage == "'locationTokenEnv' is set, but 'locationUrl' names no location service to send the token to.")
  }

  test("resolveContractLocations: a service set for a contract with no ref:// is never asked, and blank settings count as unset") {
    val service = TestLocationService.holding()
    try {
      assert(located(plainContract, Map.empty, InvaractConf.LocationUrl -> service.url) == List("some/output/path"))
      assert(service.paths.isEmpty)
      assert(located(plainContract, Map.empty, InvaractConf.LocationUrl -> "  ", InvaractConf.LocationTokenEnv -> " ") == List("some/output/path"))
    } finally service.close()
  }

  test("resolveContractLocations: an id the map does not have names the id, the file, the setting and the ids it does map") {
    val map = file("other-locations.properties", "silver_out=/s\nbronze_out=/b\n")
    val e = intercept[LocationResolutionException](VerificationSetup.resolveContractLocations(refContract, config(InvaractConf.LocationMap -> map)))
    assert(
      e.getMessage == s"The contract declares 'ref://gold_out', but the location map '$map' (named by 'locationMap') has no line for " +
        "'gold_out'; the ids it maps are: bronze_out, silver_out."
    )
    val empty = file("empty-locations.properties", "# nothing yet\n")
    assert(intercept[LocationResolutionException](VerificationSetup.resolveContractLocations(refContract, config(InvaractConf.LocationMap -> empty)))
      .getMessage.endsWith("has no line for 'gold_out'; it maps no ids at all."))
  }

  test("resolveContractLocations: a locationMap that is not a file names the setting, even for a contract with no ref://") {
    List("/definitely/not/there.properties", scratch.toString).foreach { path =>
      val e = intercept[LocationResolutionException](VerificationSetup.resolveContractLocations(plainContract, config(InvaractConf.LocationMap -> path)))
      assert(e.getMessage == s"'locationMap' names '$path', which is not a file that exists.", path)
    }
  }

  // --- options ---------------------------------------------------------------------------------

  private val allFlags = List(
    InvaractConf.RejectUndeclaredInputs -> ((o: VerificationOptions) => o.rejectUndeclaredInputs),
    InvaractConf.RejectUndeclaredFields -> ((o: VerificationOptions) => o.rejectUndeclaredFields),
    InvaractConf.ComputeFingerprint -> ((o: VerificationOptions) => o.computeFingerprint),
    InvaractConf.StaticDataQuality -> ((o: VerificationOptions) => o.staticDataQuality),
    InvaractConf.RoleConsistency -> ((o: VerificationOptions) => o.roleConsistency),
    InvaractConf.RuntimeDataQuality -> ((o: VerificationOptions) => o.runtimeDataQuality)
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
    val allOn = VerificationOptions(true, true, true, true, true, true)
    assert(VerificationSetup.resolveVerificationOptions(allOn, config(allFlags.map(_._1 -> "false"): _*)) == allOn)
  }

  test("resolveVerificationOptions: a flag that is neither true nor false is refused by name, never read as false") {
    allFlags.foreach { case (name, flag) =>
      assert(flag(VerificationSetup.resolveVerificationOptions(VerificationOptions(), config(name -> " True "))), name)
      val e = intercept[IllegalArgumentException](VerificationSetup.resolveVerificationOptions(VerificationOptions(), config(name -> "yes")))
      assert(e.getMessage == s"'$name' must be true or false, not 'yes'.", name)
    }
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
    assert(ex.getMessage.contains("known keys: computeFingerprint, rejectUndeclaredFields, rejectUndeclaredInputs, roleConsistency, runtimeDataQuality, staticDataQuality"))
  }

  // --- small helpers ---------------------------------------------------------------------------

  test("applyMinVerificationOptions only raises: a floor of false or an absent floor changes nothing") {
    val policy = com.invaract.contract.OrgPolicyParser.parse("version: \"1.0\"\ninject:\n  minVerificationOptions:\n    roleConsistency: true\n    computeFingerprint: false\n")
    val raised = VerificationSetup.applyMinVerificationOptions(VerificationOptions(computeFingerprint = true), policy)
    assert(raised == VerificationOptions(computeFingerprint = true, roleConsistency = true))
    val runtime = com.invaract.contract.OrgPolicyParser.parse("version: \"1.0\"\ninject:\n  minVerificationOptions:\n    runtimeDataQuality: true\n")
    assert(VerificationSetup.applyMinVerificationOptions(VerificationOptions(), runtime) == VerificationOptions(runtimeDataQuality = true))
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

  // identify: the job as every event names it

  private val adapterJob = JobInfo(engine = Some("pythonio"), name = Some("orders_job.main"), jobId = Some("from-the-engine"), attributes = Map("trigger" -> "cron"))

  test("identify: with neither setting, the job is as the adapter found it") {
    assert(VerificationSetup.identify(adapterJob, ConfigSource.empty) == adapterJob)
    assert(VerificationSetup.identify(JobInfo(), ConfigSource.empty) == JobInfo())
  }

  test("identify: jobId names the job across runs, in place of any the adapter found; a blank one changes nothing") {
    assert(VerificationSetup.identify(adapterJob, config(InvaractConf.JobId -> "  orders_nightly ")).jobId == Some("orders_nightly"))
    assert(VerificationSetup.identify(JobInfo(), config(InvaractConf.JobId -> "orders_nightly")).jobId == Some("orders_nightly"))
    assert(VerificationSetup.identify(adapterJob, config(InvaractConf.JobId -> "   ")).jobId == Some("from-the-engine"))
    // nothing else about the job changes
    assert(VerificationSetup.identify(adapterJob, config(InvaractConf.JobId -> "x")).copy(jobId = adapterJob.jobId) == adapterJob)
  }

  test("identify: jobAttributes are key=value pairs, trimmed; a value may hold '=' or be empty") {
    val attributes = VerificationSetup.identify(JobInfo(), config(InvaractConf.JobAttributes -> " team = orders, dag=orders_daily ,query=a=b,empty=")).attributes
    assert(attributes == Map("team" -> "orders", "dag" -> "orders_daily", "query" -> "a=b", "empty" -> ""))
    assert(VerificationSetup.identify(JobInfo(), config(InvaractConf.JobAttributes -> " , ")).attributes.isEmpty)
    assert(VerificationSetup.identify(JobInfo(), config(InvaractConf.JobAttributes -> "")).attributes.isEmpty)
  }

  test("identify: an attribute the adapter already has keeps its value; the others are added") {
    val attributes = VerificationSetup.identify(adapterJob, config(InvaractConf.JobAttributes -> "trigger=manual,team=orders")).attributes
    assert(attributes == Map("trigger" -> "cron", "team" -> "orders"))
  }

  test("identify: malformed jobAttributes are refused, naming the setting as the engine spells it, the entry and the whole value") {
    def refused(raw: String) = intercept[IllegalArgumentException] {
      VerificationSetup.identify(JobInfo(), ConfigSource.prefixed("spark.invaract.")(Map("spark.invaract.jobAttributes" -> raw).get))
    }.getMessage
    assert(refused("team=orders,dag") ==
      "'spark.invaract.jobAttributes' must be comma-separated key=value pairs ('dag' is not key=value), not 'team=orders,dag'.")
    assert(refused("=orders").contains("('=orders' is not key=value)"))
    assert(refused(" =orders").contains("('=orders' is not key=value)"), "entries are trimmed before they are read")
    assert(refused("b=1,a=1,b=2,a=2") ==
      "'spark.invaract.jobAttributes' must be comma-separated key=value pairs ('a', 'b' given more than once), not 'b=1,a=1,b=2,a=2'.")
    assert(refused("a=1,a=1").contains("('a' given more than once)"))
  }

  // contractMetadata: what a dry run writes into the contracts it drafts

  test("contractMetadata: key=value pairs, read as jobAttributes are; unset or blank is none") {
    assert(VerificationSetup.contractMetadata(config(InvaractConf.ContractMetadata -> " owner = orders-team, source=erp,query=a=b")) ==
      Map("owner" -> "orders-team", "source" -> "erp", "query" -> "a=b"))
    assert(VerificationSetup.contractMetadata(ConfigSource.empty).isEmpty)
    assert(VerificationSetup.contractMetadata(config(InvaractConf.ContractMetadata -> " , ")).isEmpty)
    // it is its own setting: neither reads the other
    assert(VerificationSetup.contractMetadata(config(InvaractConf.JobAttributes -> "team=orders")).isEmpty)
    assert(VerificationSetup.identify(JobInfo(), config(InvaractConf.ContractMetadata -> "owner=x")).attributes.isEmpty)
  }

  test("contractMetadata: malformed pairs are refused, naming the setting as the engine spells it") {
    def refused(raw: String) = intercept[IllegalArgumentException] {
      VerificationSetup.contractMetadata(ConfigSource.prefixed("spark.invaract.")(Map("spark.invaract.contractMetadata" -> raw).get))
    }.getMessage
    assert(refused("owner=a,source") ==
      "'spark.invaract.contractMetadata' must be comma-separated key=value pairs ('source' is not key=value), not 'owner=a,source'.")
    assert(refused("owner=a,owner=b").contains("('owner' given more than once)"))
  }
}
