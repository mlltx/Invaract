// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser
import com.invaract.sparkadapter.notification.{NotificationSink, TestNotificationSink}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class ContractEnforcementRuleSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  // The check rule is fixed at SparkSession construction time, but which
  // contract it enforces needs to vary per test. injectCheckRule's function
  // is invoked fresh on every analyzed plan, so a mutable cell it reads at
  // call time — not a value captured once at registration — lets one
  // session serve every test in this suite without the overhead of
  // stopping and rebuilding a SparkSession (and its SparkContext) per case.
  @volatile private var activeContract: Option[com.invaract.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()
  @volatile private var activeSink: Option[NotificationSink] = None

  // Not read by any enforcement test above — a raw capture of every
  // analyzed plan the session produces, so a test can inspect
  // WriteCommandSupport's translation directly (e.g. its format
  // detection) without going through StructuralVerifier, which only
  // checks format when the contract *also* declares one and both sides
  // are known, and so wouldn't surface a wrong-format bug as a test
  // failure on its own.
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-enforcement-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleSpec")
      // See SparkPlanAdapterSpec's beforeAll for why this is safe to add
      // to a session every other test in this suite also shares.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      // Spark's default (200) is tuned for real clusters; every shuffle
      // (a MERGE's join included) against these few-row local fixtures
      // would otherwise spin up 200 tasks for no benefit - real,
      // measured overhead in a suite this size. Purely a physical-
      // execution parallelism knob, invisible to query results.
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          capturedPlans += plan
          activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions, activeSink))
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def parseContract(yaml: String) = ContractParser.parse(yaml)

  private def withContract[T](yaml: String, options: VerificationOptions = VerificationOptions(), sink: Option[NotificationSink] = None)(
      body: => T
  ): T = {
    activeContract = Some(parseContract(yaml))
    activeOptions = options
    activeSink = sink
    try body
    finally {
      activeContract = None
      activeSink = None
    }
  }

  private val passingContractYaml =
    """id: enforcement_demo
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: OUTPUT_PATH
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |        - name: doubled
      |          type: long
      |          required: true
      |""".stripMargin

  test("PASS: a write satisfying its contract executes normally, output file created") {
    val outputPath = scratchDir.resolve("pass.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_missing_column.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // The notification sink's PASS/FAIL pair for verifyOrThrow's write
  // branch: a ContractValidationEvent must be published for *every* check
  // (mirroring the PASS/FAIL tests above), not just failures - a caller
  // wiring up a sink cares about "the contract was evaluated," including
  // every time it was satisfied, not only when it's about to reject a
  // write.
  test("PASS: a satisfied contract still publishes a ContractValidationEvent, with status PASSED and no violations") {
    val outputPath = scratchDir.resolve("pass_notify.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val sink = new TestNotificationSink

    withContract(yaml, sink = Some(sink)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath)
    }

    val events = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }
    assert(events.nonEmpty, "expected at least one ContractValidationEvent")
    val event = events.last
    assert(event.status == "PASSED")
    assert(event.violations.isEmpty)
    assert(event.contract == "enforcement_demo@1.0.0")
    // This suite's shared check rule calls verifyOrThrow directly (not
    // through forContract), so no applicationId is ever supplied - None
    // is the correct default, not an oversight, confirmed alongside the
    // forContract(contract, options, sink) test below which does thread a
    // real one through.
    assert(event.applicationId.isEmpty)
  }

  test("FAIL: a violated contract publishes a ContractValidationEvent (status FAILED, carrying the violation) before throwing") {
    val outputPath = scratchDir.resolve("fail_notify.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin
    val sink = new TestNotificationSink

    withContract(yaml, sink = Some(sink)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    val events = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }
    assert(events.nonEmpty, "expected at least one ContractValidationEvent, even though the write was rejected")
    val event = events.last
    assert(event.status == "FAILED")
    assert(event.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  test("no sink configured: verifyOrThrow behaves exactly as before, publishing nothing and still enforcing") {
    val outputPath = scratchDir.resolve("no_sink.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    // activeSink defaults to None via withContract - this is the existing
    // PASS path, just re-asserted under the new 4-arg verifyOrThrow to
    // prove the added sink parameter changes nothing when absent.
    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  // com.invaract.sparkadapter.location - resolving a contract's ref://<id>
  // locations from Spark configuration (spark.invaract.locationMap), so a
  // platform invoking spark-submit can attach this without the job's own
  // code calling ContractLocationResolution.resolve itself. resolveContractLocations
  // is exercised directly (widened to private[sparkadapter] for exactly this,
  // matching verifyOrThrow's own doc) against the suite's real SparkSession,
  // setting/unsetting the conf key around each case so it can't leak into
  // any other test sharing this session.
  private def withLocationMapConf[T](path: String)(body: => T): T = {
    spark.conf.set(ContractEnforcementRule.LocationMapConfKey, path)
    try body
    finally spark.conf.unset(ContractEnforcementRule.LocationMapConfKey)
  }

  test("resolveContractLocations: a contract with no ref:// locations is unchanged, conf unset") {
    val contract = parseContract(passingContractYaml.replace("OUTPUT_PATH", "literal/path.parquet"))
    val resolved = ContractEnforcementRule.resolveContractLocations(contract, spark)
    assert(resolved == contract)
  }

  test("resolveContractLocations: a contract with no ref:// locations is unchanged even with a conf set") {
    val propsFile = Files.createTempFile(scratchDir, "unused-location-map", ".properties")
    Files.write(propsFile, "some-id=some/path".getBytes("UTF-8"))

    val contract = parseContract(passingContractYaml.replace("OUTPUT_PATH", "literal/path.parquet"))
    val resolved = withLocationMapConf(propsFile.toString) {
      ContractEnforcementRule.resolveContractLocations(contract, spark)
    }
    assert(resolved == contract)
  }

  test("resolveContractLocations: resolves a ref:// output location from spark.invaract.locationMap") {
    val outputPath = scratchDir.resolve("conf_resolved.parquet").toString
    val propsFile = Files.createTempFile(scratchDir, "location-map", ".properties")
    Files.write(propsFile, s"result-output=$outputPath".getBytes("UTF-8"))

    val contract = parseContract(passingContractYaml.replace("OUTPUT_PATH", "ref://result-output"))
    val resolved = withLocationMapConf(propsFile.toString) {
      ContractEnforcementRule.resolveContractLocations(contract, spark)
    }
    assert(resolved.outputs.map(_.location) == List(outputPath))
  }

  test("resolveContractLocations: a ref:// location with the conf unset fails closed with a clear message") {
    val contract = parseContract(passingContractYaml.replace("OUTPUT_PATH", "ref://result-output"))
    val ex = intercept[com.invaract.sparkadapter.location.LocationResolutionException] {
      ContractEnforcementRule.resolveContractLocations(contract, spark)
    }
    assert(ex.getMessage.contains("ref://result-output"))
  }

  // resolveVerificationOptions - the same "attachable via spark-submit
  // --conf, not only a Scala constructor argument" mechanism as
  // resolveContractLocations above, applied to VerificationOptions's three
  // Boolean flags. A generic conf helper (unlike withLocationMapConf,
  // reusable for any key) since these tests set/unset three different keys.
  private def withConf[T](key: String, value: String)(body: => T): T = {
    spark.conf.set(key, value)
    try body
    finally spark.conf.unset(key)
  }

  test("resolveVerificationOptions: no conf set leaves options exactly as the caller passed them") {
    val options = VerificationOptions(rejectUndeclaredInputs = true)
    assert(ContractEnforcementRule.resolveVerificationOptions(options, spark) == options)
    assert(ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), spark) == VerificationOptions())
  }

  test("resolveVerificationOptions: a conf key of 'false' does not turn its flag on") {
    val resolved = withConf(ContractEnforcementRule.RejectUndeclaredInputsConfKey, "false") {
      ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), spark)
    }
    assert(!resolved.rejectUndeclaredInputs)
  }

  test("resolveVerificationOptions: spark.invaract.rejectUndeclaredInputs=true turns the flag on even when the caller left it false") {
    val resolved = withConf(ContractEnforcementRule.RejectUndeclaredInputsConfKey, "true") {
      ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), spark)
    }
    assert(resolved.rejectUndeclaredInputs)
    // Only this one flag moves - the other two stay at their defaults.
    assert(!resolved.rejectUndeclaredFields)
    assert(!resolved.computeFingerprint)
  }

  test("resolveVerificationOptions: spark.invaract.rejectUndeclaredFields=true turns the flag on even when the caller left it false") {
    val resolved = withConf(ContractEnforcementRule.RejectUndeclaredFieldsConfKey, "true") {
      ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), spark)
    }
    assert(resolved.rejectUndeclaredFields)
  }

  test("resolveVerificationOptions: spark.invaract.computeFingerprint=true turns the flag on even when the caller left it false") {
    val resolved = withConf(ContractEnforcementRule.ComputeFingerprintConfKey, "true") {
      ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), spark)
    }
    assert(resolved.computeFingerprint)
  }

  test("resolveVerificationOptions: a flag the caller already set true stays true even if its conf key is unset") {
    val resolved = ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(computeFingerprint = true), spark)
    assert(resolved.computeFingerprint)
  }

  test("forContract end-to-end: rejectUndeclaredFields attached purely via conf rejects a write that would otherwise pass") {
    // Mirrors the ref:// end-to-end test above: calling rule(spark) directly
    // rather than standing up a second SparkSession, for the same
    // documented reason (getOrCreate() mid-suite reuses this suite's
    // already-active session/extensions).
    val outputPath = scratchDir.resolve("conf_reject_undeclared.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val contract = parseContract(yaml)

    // A plain, unchecked write with an extra, undeclared column - permitted
    // by the contract's own default (rejectUndeclaredFields = false), which
    // is exactly what this test needs to distinguish "conf turned the
    // stricter check on" from "the write would have failed anyway".
    val df = spark.range(5).withColumn("doubled", col("id") * 2).withColumn("extra", col("id") + 1)
    df.write.mode("overwrite").parquet(outputPath)
    val writePlan = capturedPlans.reverseIterator.find(WriteCommandSupport.combined.isDefinedAt).getOrElse(
      fail("no analyzed write plan was captured to reuse")
    )

    val rule = ContractEnforcementRule.forContract(contract) // options left at every default
    withConf(ContractEnforcementRule.RejectUndeclaredFieldsConfKey, "true") {
      intercept[ContractViolationException] {
        rule(spark)(writePlan)
      }
    }
  }

  test("forContract end-to-end: a real write against a ref:// output is resolved via spark.invaract.locationMap") {
    // Same "call the returned function directly, rather than a second
    // SparkSession" approach the "forContract builds a usable check-rule
    // function directly" test below uses, and for the same documented
    // reason: getOrCreate() mid-suite would just reuse this suite's
    // already-active session/extensions, silently never installing a
    // different check rule. forContract's own outer `session => {...}`
    // closure is exactly where resolveContractLocations runs (see its
    // doc) - calling `rule(spark)` with the conf key set on the real
    // shared session genuinely exercises that read, not just
    // resolveContractLocations in isolation.
    val outputPath = scratchDir.resolve("conf_end_to_end.parquet").toString
    val propsFile = Files.createTempFile(scratchDir, "e2e-location-map", ".properties")
    Files.write(propsFile, s"e2e-output=$outputPath".getBytes("UTF-8"))
    val contract = parseContract(passingContractYaml.replace("OUTPUT_PATH", "ref://e2e-output"))

    // A plain, unchecked write (activeContract is None outside withContract)
    // - only to capture a real analyzed write plan at outputPath, the same
    // off-the-shelf-plan technique the sink-overload test above uses.
    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    df.write.mode("overwrite").parquet(outputPath)
    val writePlan = capturedPlans.reverseIterator.find(WriteCommandSupport.combined.isDefinedAt).getOrElse(
      fail("no analyzed write plan was captured to reuse")
    )

    // The conf has to be set around invoking `rule(spark)` specifically,
    // not around building `rule` itself: forContract(contract) merely
    // returns the SparkSession => LogicalPlan => Unit function value -
    // its body (where resolveContractLocations actually reads
    // session.conf) only runs once that function is applied to a session.
    val rule = ContractEnforcementRule.forContract(contract)
    withLocationMapConf(propsFile.toString) {
      rule(spark)(writePlan) // must not throw: ref://e2e-output resolved to outputPath, matching the real write's actual location
    }
  }

  // docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14 - the Spark contract
  // extension surfacing a computed fingerprint through its two existing
  // output channels, opt-in via VerificationOptions.computeFingerprint.
  test("computeFingerprint defaults to false: no Fingerprints section printed, nothing attached to the result or a published event") {
    val outputPath = scratchDir.resolve("fail_no_fingerprint.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin
    val sink = new TestNotificationSink

    val ex = withContract(yaml, sink = Some(sink)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(ex.result.fingerprints.isEmpty)
    assert(!ex.getMessage.contains("Fingerprints"))
    val event = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last
    assert(event.fingerprints.isEmpty)
  }

  test("computeFingerprint = true: a rejected write's message and published event carry the correct TransformationFingerprint") {
    val outputPath = scratchDir.resolve("fail_with_fingerprint.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin
    val sink = new TestNotificationSink
    capturedPlans.clear()

    val ex = withContract(yaml, options = VerificationOptions(computeFingerprint = true), sink = Some(sink)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    val expected = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(SparkPlanAdapter.translate(capturedPlans.last).plan)

    assert(ex.result.fingerprints.contains(expected), "the attached fingerprint must match the plan actually checked, not merely be present")
    assert(ex.getMessage.contains("Fingerprints"))
    assert(ex.getMessage.contains(expected.overall.value))

    val event = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last
    assert(event.fingerprints.contains(expected))
  }

  // A real, confirmed false positive: Spark's own analyzer
  // (Catalyst's ResolveRandomSeed rule) bakes a fresh random Long into an
  // unseeded rand()/random()/randn() call as a genuine child expression,
  // confirmed directly by analyzing the identical .withColumn("r", rand())
  // twice in one JVM and observing two different seed literals every time.
  // Without Canonicalizer's seed exclusion (see its own SeedBearingFunctionNames
  // doc), this would make the fingerprint of the exact same, unchanged code
  // different on every single run - the "same model -> same fingerprint"
  // guarantee this whole module exists to provide, broken for what is
  // likely the single most common non-deterministic construct in practice.
  test("computeFingerprint = true: an unseeded rand() call fingerprints identically across separate analyses of the identical code") {
    val outputPath = scratchDir.resolve("rand_fp.parquet").toString
    def fingerprintOfRandColumn(): com.invaract.fingerprint.TransformationFingerprint = {
      val yaml =
        s"""id: enforcement_demo
           |version: "1.0.0"
           |outputs:
           |  - name: out
           |    location: $outputPath
           |    schema:
           |      fields:
           |        - name: id
           |          type: long
           |          required: true
           |""".stripMargin
      val sink = new TestNotificationSink
      withContract(yaml, options = VerificationOptions(computeFingerprint = true), sink = Some(sink)) {
        val df = spark.range(5).withColumn("r", rand())
        df.write.mode("overwrite").parquet(outputPath) // must not throw - this contract declares no rule
      }
      sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last.fingerprints
        .getOrElse(fail("computeFingerprint = true must always populate a fingerprint on a PASSing write too"))
    }

    val fp1 = fingerprintOfRandColumn()
    val fp2 = fingerprintOfRandColumn()
    assert(fp1.overall == fp2.overall, "rand()'s analyzer-assigned seed must never leak into the fingerprint")
    assert(fp1.outputs("r") == fp2.outputs("r"))
  }

  // A real, confirmed false NEGATIVE, now fixed by
  // SparkPlanAdapter.computeAliasDisambiguation (see that method's own
  // doc for the full mechanism): an unaliased DataFrame-API self-join of
  // the same catalog table (`spark.table("t")` on both sides, no `.as()`
  // anywhere) makes Spark's analyzer wrap BOTH physical Read occurrences
  // in a `SubqueryAlias` using the table's own name - the identical
  // string on both sides, confirmed directly by printing the analyzed
  // plan (`SubqueryAlias spark_catalog.default.t` appears twice,
  // verbatim). Before the fix, `translateNonWritePlan`'s SubqueryAlias
  // case carried that identical string into both `ir.Read.alias` fields,
  // and every `ColumnRef.qualifier` reaching a join condition or output
  // expression was *also* just that same repeated string - collapsing a
  // genuine two-occurrence join into what looked like a degenerate
  // self-comparison (`t.id = t.id`) once `Canonicalizer.buildScopeInfo`
  // ran over it.
  //
  // `.toDF(...)` (a purely positional rename over the join's own already-
  // exprId-distinct output attributes - Spark's `Dataset.join` internally
  // deduplicates the right side's exprIds specifically to make self-joins
  // usable at all, confirmed via the analyzed plan below) is the concrete,
  // always-reachable repro used here, deliberately *not* a `.select()`
  // built from `left(...)`/`right(...)` Dataset-column handles: those
  // handles are captured from each side's own *pre-join*, *pre-
  // deduplication* resolution, so Spark's own column lookup collapses
  // `right("value")` to the exact same exprId as `left("value")` once
  // Spark's `DetectAmbiguousSelfJoin` guard is turned off to allow it -
  // confirmed empirically (not assumed) by executing both "variants" and
  // observing byte-identical output rows for what looks like two
  // different queries. That is not a reachable false negative (there is
  // no genuinely different query being conflated - Spark itself cannot
  // tell the two apart via that API), so it is deliberately not asserted
  // here. `.toDF(...)`'s positional rename needs no such handle and no
  // Spark config change - it reproduces (and, after the fix, correctly
  // resolves) the real, ordinary case: a self-join whose *own* output
  // columns are read normally.
  test("an unaliased self-join of the same catalog table translates the two physical occurrences distinctly") {
    val tableName = "self_join_alias_fix_tbl"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.range(5).withColumn("value", col("id") * 10).write.saveAsTable(tableName)
    val outputPath = scratchDir.resolve("self_join_alias_fix.parquet").toString

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: lid
         |          type: long
         |          required: false
         |""".stripMargin
    val sink = new TestNotificationSink
    capturedPlans.clear()

    withContract(yaml, options = VerificationOptions(computeFingerprint = true), sink = Some(sink)) {
      val left = spark.table(tableName)
      val right = spark.table(tableName)
      val joined = left.join(right, left("id") === right("id"))
      val renamed = joined.toDF("lid", "lvalue", "rid", "rvalue")
      renamed.write.mode("overwrite").parquet(outputPath) // must not throw - this contract declares no rule
    }

    val translated = SparkPlanAdapter.translate(capturedPlans.last).plan
    def findJoin(plan: com.invaract.ir.Plan): Option[com.invaract.ir.Join] = plan match {
      case j: com.invaract.ir.Join => Some(j)
      case other                     => other.children.flatMap(findJoin).headOption
    }
    val join = findJoin(translated).getOrElse(fail(s"no Join node found in $translated"))

    val leftRead = join.left.asInstanceOf[com.invaract.ir.Read]
    val rightRead = join.right.asInstanceOf[com.invaract.ir.Read]
    // Exact expected suffixes, not just "the two differ" - catches an
    // off-by-one or sign-flipped index (e.g. `index + 1` mutated to
    // `index - 1`) that would still produce two *distinct* strings
    // ("tbl#0"/"tbl#-1") but the wrong ones; asserting the precise
    // "<name>#<index>" value for each occurrence, in encounter order,
    // pins the actual arithmetic, not merely that it varies.
    assert(leftRead.alias == Some(s"$tableName#0"), s"expected the first occurrence's alias to be '$tableName#0', got ${leftRead.alias}")
    assert(rightRead.alias == Some(s"$tableName#1"), s"expected the second occurrence's alias to be '$tableName#1', got ${rightRead.alias}")

    val condition = join.condition.getOrElse(fail("expected a join condition"))
    condition match {
      case com.invaract.ir.Comparison(
            "=",
            com.invaract.ir.ColumnReference(com.invaract.ir.ColumnRef(_, leftQualifier, _)),
            com.invaract.ir.ColumnReference(com.invaract.ir.ColumnRef(_, rightQualifier, _))
          ) =>
        assert(leftQualifier != rightQualifier, "the join condition's two sides must resolve to distinct qualifiers")
        assert(leftQualifier == leftRead.alias)
        assert(rightQualifier == rightRead.alias)
      case other => fail(s"unexpected join condition shape: $other")
    }

    val fp = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last.fingerprints
      .getOrElse(fail("computeFingerprint = true must always populate a fingerprint on a PASSing write too"))
    assert(
      fp.outputs("lvalue") != fp.outputs("rvalue"),
      "lvalue and rvalue are genuinely different physical columns (left vs. right side of the self-join) - " +
        "before the fix, both collapsed to the same colliding qualifier and fingerprinted identically"
    )
  }

  // Found via the ClickHouse connector pass's Phase 8, but not
  // ClickHouse-specific - reproduces with any connector, since it's a
  // contract/spark-adapter boundary issue, not a translation one. A
  // contract YAML missing its top-level 'outputs:' key used to parse
  // without error (ContractParser.parse never validates on its own) and
  // then crash verifyOrThrow with an unguarded
  // NoSuchElementException("head of empty list") at contract.outputs.head,
  // instead of a clean, actionable rejection - even though
  // ContractValidator already has an "outputs must be non-empty" check
  // that would have caught it. Fixed by validating the contract first.
  test("FAIL: a contract missing 'outputs' is rejected cleanly, not with an unguarded crash") {
    val outputPath = scratchDir.resolve("invalid_contract_no_outputs.parquet").toString
    val yaml =
      s"""id: invalid_contract
         |version: "1.0.0"
         |""".stripMargin // deliberately no 'outputs:' key at all

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(v =>
      v.violationType == ViolationType.InvalidContract && v.message.contains("outputs")))
  }

  // Closes the enforcement half of the gap SparkPlanAdapterSpec's Delta
  // translation test documents: before SparkPlanAdapter recognized
  // SaveIntoDataSourceCommand, a Delta write translated to UnknownPlan,
  // and ContractEnforcementRule only gates plans that translate to
  // ir.Write — meaning a Delta write passed through completely
  // unverified, silently, contract or no contract. These two tests are
  // the same PASS/FAIL pair as the Parquet tests above, proving real
  // enforcement now applies to a real Delta write end to end, not just
  // that translation produces the right IR shape in isolation.
  test("PASS: a Delta write satisfying its contract executes normally, output written") {
    val outputPath = scratchDir.resolve("pass_delta").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("overwrite").save(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a Delta write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_delta").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.format("delta").mode("overwrite").save(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the Delta write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // Closes the same kind of gap as the Delta pair above, for a third write
  // shape: `.saveAsTable(...)` against a *new* table analyzes to
  // CreateDataSourceTableAsSelectCommand, not InsertIntoHadoopFsRelationCommand
  // or SaveIntoDataSourceCommand — confirmed empirically, see
  // docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable writes" section.
  test("PASS: a .saveAsTable() write satisfying its contract executes normally, output written") {
    val outputPath = scratchDir.resolve("pass_saveAsTable").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.option("path", outputPath).mode("overwrite").saveAsTable("pass_save_as_table_tbl") // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a .saveAsTable() write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_saveAsTable").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.option("path", outputPath).mode("overwrite").saveAsTable("fail_save_as_table_tbl")
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the .saveAsTable() write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // Delta as an INPUT, not an output — a different code path from every
  // Delta test above. verifyOrThrow collects input schemas via its own
  // `plan.collect { case lr: LogicalRelation => ... }`, independent of
  // SparkPlanAdapter.translate; investigated empirically (see
  // docs/SPARK_ADAPTER.md's "Delta Lake reads" section) that this needs no
  // change: Delta's read relation (`DeltaLog$$anon$2`) is an anonymous
  // subclass of Spark's own HadoopFsRelation, not a distinct type, so both
  // this collection and translation already match it as a normal
  // HadoopFsRelation via ordinary subtyping. This PASS/FAIL pair proves
  // that through real enforcement, not just translation in isolation — a
  // contract's declared input schema is genuinely checked against a real
  // Delta read's actual schema, both when it matches and when it doesn't.
  test("PASS: a contract's declared Delta input schema is genuinely checked against a real Delta read") {
    val deltaInputPath = scratchDir.resolve("delta_input_pass").toString
    val outputPath = scratchDir.resolve("pass_delta_input").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(deltaInputPath)

    // required: false throughout - Delta reports every column nullable on
    // read-back regardless of what was written (a real, separate Delta
    // behavior, not something this test is about); nullability itself
    // already has its own dedicated coverage in StructuralVerifierSpec.
    // This test is specifically about field existence/type, checked
    // against a real Delta read's real schema.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: $deltaInputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val df = spark.read.format("delta").load(deltaInputPath).select("id")
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a contract requiring an input field genuinely absent from a real Delta read is rejected") {
    val deltaInputPath = scratchDir.resolve("delta_input_fail").toString
    val outputPath = scratchDir.resolve("fail_delta_input").toString
    // Only id/doubled actually exist in this Delta table.
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(deltaInputPath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: $deltaInputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.read.format("delta").load(deltaInputPath).select("id")
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
  }

  // Closes the last coverage-ledger gap: a streaming source is not a
  // LogicalRelation, so a contract declaring it as a required `input`
  // used to always report MISSING_INPUT even though data was genuinely
  // being read - not a silent-pass risk (a MISSING_INPUT rejection is
  // safe, just wrong), but a real false-positive gap. This PASS/FAIL pair
  // proves it through real enforcement of a real streaming Delta source,
  // not just that the false MISSING_INPUT stops firing.
  test("PASS: a contract declaring a streaming Delta source as its input is genuinely recognized") {
    val sourcePath = scratchDir.resolve("stream_input_pass_source").toString
    val sinkPath = scratchDir.resolve("stream_input_pass_sink").toString
    val checkpointPath = scratchDir.resolve("stream_input_pass_checkpoint").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(sourcePath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.format("delta").load(sourcePath)
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .start(sinkPath) // must not throw MISSING_INPUT
      query.awaitTermination()
    }

    assert(Files.exists(java.nio.file.Paths.get(sinkPath)))
  }

  test("FAIL: a contract requiring an input field genuinely absent from a real streaming Delta source is rejected") {
    val sourcePath = scratchDir.resolve("stream_input_fail_source").toString
    val sinkPath = scratchDir.resolve("stream_input_fail_sink").toString
    val checkpointPath = scratchDir.resolve("stream_input_fail_checkpoint").toString
    // Only id/doubled actually exist in this Delta table.
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(sourcePath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("delta").load(sourcePath).select("id")
      intercept[ContractViolationException] {
        val query = streamDf.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointPath)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start(sinkPath)
        query.awaitTermination()
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(sinkPath)), "the streaming write must be aborted before the query ever starts, not merely reported as failed")
    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
  }

  // Delta's row-level DML - MERGE INTO / UPDATE / DELETE - used to be a
  // real, concrete example of the fail-closed policy itself: MERGE INTO
  // analyzes to org.apache.spark.sql.delta.commands.MergeIntoCommand,
  // previously neither a recognized write nor on the known-safe list, so
  // it was rejected outright (safely, but unverified). Now
  // WriteCommandSupport's deltaRowLevelDml case recognizes all three
  // Delta-internal DML commands by reflection, checking the operation's
  // *target* against the contract's declared output location and current
  // schema - not the row-level merge/update/delete logic itself, which
  // there is no contract vocabulary to check yet (see that case's own doc
  // comment, and ROADMAP.md's "Full semantic DML verification" item).
  // This PASS/FAIL trio proves that structural check through real
  // enforcement: a satisfying MERGE actually executes (rows genuinely
  // merged, not just "didn't throw"), a schema-violating one is aborted
  // before touching the table, and - proving the "source is a contract
  // input" claim through enforcement, not just code reading - a MERGE
  // whose *source* doesn't satisfy a declared input schema is also
  // aborted, with no special-casing needed for that in this case at all
  // (ContractEnforcementRule's input-schema collection already walks the
  // whole analyzed plan, target and source alike).
  test("PASS: a MERGE INTO satisfying its contract's declared output executes normally, rows genuinely merged") {
    val tablePath = scratchDir.resolve("merge_pass_target").toString
    val tableName = "merge_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false throughout - Delta reports every column nullable on
    // read-back regardless of what was written (see the existing Delta
    // input-read PASS test above for the same, already-documented
    // behavior) - this case's outputSchema comes from the *target's*
    // read-back schema (target.schema), not a freshly-written query's
    // pre-write schema the way every other write shape's does, so it hits
    // this quirk where those don't.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("FAIL: a MERGE INTO whose target violates its contract's declared output schema is aborted before touching the table") {
    val tablePath = scratchDir.resolve("merge_fail_target").toString
    val tableName = "merge_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  test("FAIL: a MERGE INTO whose source violates a contract's declared input schema is aborted before touching the table") {
    val tablePath = scratchDir.resolve("merge_fail_input_target").toString
    val tableName = "merge_fail_input_tbl"
    val sourcePath = scratchDir.resolve("merge_fail_input_source").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    // A real file-backed source, not a temp view over an in-memory
    // DataFrame - a temp view resolves to whatever underlying plan it
    // wraps (here, not a LogicalRelation at all), so it would never be
    // recognized as a read to begin with, reporting MISSING_INPUT (the
    // declared input was never read) rather than the MISSING_INPUT_FIELD
    // this test is actually about (recognized as read, but missing a
    // required field) - confirmed the hard way by a real test failure.
    spark.createDataFrame(Seq((99L, 198L))).toDF("id", "doubled").write.mode("overwrite").parquet(sourcePath)
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: merge_source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING parquet.`${sourcePath.replace('\\', '/')}` s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  // A real bug, found by writing this test rather than assumed away: a
  // contract requiring a field that a schema-evolving MERGE is about to
  // add would previously be rejected with MISSING_OUTPUT_FIELD, even
  // though the merge would have satisfied it - because outputSchema came
  // from target.schema at analysis time (pre-merge), confirmed empirically
  // to not yet include columns schema evolution is about to add.
  // deltaRowLevelDml now detects MergeIntoCommand.schemaEvolutionEnabled()
  // and unions in the source's new fields as a best-effort approximation.
  test("PASS: a MERGE INTO with schema evolution enabled, satisfying a contract requiring the newly-added column") {
    val tablePath = scratchDir.resolve("merge_schema_evo_target").toString
    val tableName = "merge_schema_evo_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |        - name: extra_col
         |          type: string
         |          required: true
         |""".stripMargin

    // The session (and its SQL conf) is shared across this whole spec, so
    // this must not leak "enabled = true" to later tests even if the MERGE
    // itself throws unexpectedly.
    withContract(yaml) {
      spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
      try {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled, 'new' as extra_col) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect() // must not throw - extra_col is about to be added by evolution
      } finally {
        spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
      }
    }

    assert(spark.table(tableName).schema.fieldNames.contains("extra_col"), "the merge must actually have evolved the schema")
    assert(spark.table(tableName).count() == 6)
  }

  // Confirmed empirically (MergeNoEvoExtraFieldProbeSpec, since deleted):
  // with autoMerge disabled, a MERGE's source can carry a column the target
  // doesn't have - INSERT * silently drops it, the commit succeeds, and the
  // table's schema never gains it. So when schemaEvolutionEnabled() is
  // false, outputSchema must come from target.schema alone; unioning in the
  // source's extra fields regardless (as if evolution were always active)
  // would report a column as written that never actually was. This is
  // exactly the case rejectUndeclaredFields is designed to catch, so it's
  // used here as the tripwire: real code passes (extra_col was never
  // written, contract doesn't mention it); a build that ignored
  // schemaEvolutionEnabled()'s actual value would wrongly add extra_col to
  // outputSchema and abort this write.
  test("PASS: a MERGE INTO without schema evolution enabled ignores the source's extra column, not just the target's") {
    val tablePath = scratchDir.resolve("merge_no_evo_extra_target").toString
    val tableName = "merge_no_evo_extra_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml, VerificationOptions(rejectUndeclaredFields = true)) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled, 'new' as extra_col) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw - extra_col is silently dropped, never evolution-added
    }

    assert(!spark.table(tableName).schema.fieldNames.contains("extra_col"), "extra_col must not actually have been written")
    assert(spark.table(tableName).count() == 6)
  }

  // A real bug, the same class as the MERGE schema-evolution one above,
  // found the same way (real probes, not assumed): Delta generated columns
  // (GENERATED ALWAYS AS (...)) are computed by Delta itself at commit
  // time, never supplied by the writer, so AppendData's outputSchema
  // (previously always cmd.query.schema) never included them - a contract
  // requiring a generated column would be wrongly MISSING_OUTPUT_FIELD-
  // rejected for an append that would actually satisfy it once Delta
  // computed the column. Confirmed empirically that this can't be detected
  // from any DataFrame-facing schema (read-back, catalog table, or the
  // DSv2 Table handle's own .schema()) - only Delta's internal
  // Snapshot.schema() carries the delta.generationExpression metadata
  // GeneratedColumn.isGeneratedColumn actually checks -
  // outputSchemaWithGeneratedColumns/deltaGeneratedFields now read that
  // reflectively and union in the target's generated-only columns.
  test("PASS: appending to a Delta table with a generated column, satisfying a contract requiring it") {
    val tablePath = scratchDir.resolve("gen_col_target").toString
    val tableName = "gen_col_append_tbl"
    DeltaTable.create(spark)
      .tableName(tableName)
      .location(tablePath)
      .addColumn("id", org.apache.spark.sql.types.LongType)
      .addColumn("event_time", TimestampType)
      .addColumn(
        DeltaTable.columnBuilder(spark, "event_date")
          .dataType(DateType)
          .generatedAlwaysAs("CAST(event_time AS DATE)")
          .build()
      )
      .execute()

    // id/event_time: required: false - Delta reports every column nullable
    // on read-back (see the existing Delta input-read PASS test for the
    // same documented quirk). event_date: deliberately required: true -
    // this is the field under test, never supplied by this write's own
    // DataFrame (id, event_time only); required: false here would make
    // this test pass identically whether or not the generated-column fix
    // exists (a missing non-required field isn't flagged at all - see
    // "an absent field is only flagged when the contract marks it
    // required" in StructuralVerifierSpec), silently proving nothing.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: event_time
         |          type: timestamp
         |          required: false
         |        - name: event_date
         |          type: date
         |          required: true
         |          nullable: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.createDataFrame(Seq((1L, java.sql.Timestamp.valueOf("2024-01-01 00:00:00"))))
        .toDF("id", "event_time")
      df.writeTo(tableName).append() // must not throw - event_date is about to be Delta-computed
    }

    val written = spark.table(tableName).collect()
    assert(written.length == 1)
    assert(written.head.getAs[java.sql.Date]("event_date") != null, "event_date must actually have been Delta-computed")
  }

  // Direct-inspection companion to the PASS test above, same pattern as
  // the path-based DML test elsewhere in this file: an AppendData against
  // a Delta table with NO generated columns must resolve outputSchema
  // via the ordinary query.schema path, with no diagnostic attached -
  // proving outputSchemaWithGeneratedColumns's "nothing found" branch
  // stays silent, not just that the schema value happens to come out the
  // same either way.
  test("WriteCommandSupport reports no diagnostic for AppendData into a Delta table with no generated columns") {
    val tablePath = scratchDir.resolve("no_gen_col_target").toString
    val tableName = "no_gen_col_append_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    capturedPlans.clear()

    spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val append = capturedPlans.collectFirst { case p: org.apache.spark.sql.catalyst.plans.logical.AppendData => p }
      .getOrElse(fail("no AppendData plan observed"))
    val info = WriteCommandSupport.combined.lift(append).getOrElse(fail("AppendData should be recognized"))
    assert(info.outputSchema.fieldNames.toSet == Set("id", "doubled"))
    assert(info.diagnostic.isEmpty, "a table with no generated columns must not get a generated-columns diagnostic")
  }

  test("PASS: an UPDATE satisfying its contract's declared output executes normally") {
    val tablePath = scratchDir.resolve("update_pass_target").toString
    val tableName = "update_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false - see the MERGE PASS test above for why (Delta
    // reports every column nullable on read-back, and this case's
    // outputSchema comes from the target's read-back schema).
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 5, "the UPDATE must actually have run against all 5 original rows")
  }

  test("PASS: a DELETE satisfying its contract's declared output executes normally") {
    val tablePath = scratchDir.resolve("delete_pass_target").toString
    val tableName = "delete_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false - see the MERGE PASS test above for why.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  // The four tests below lock in findings from a real probing pass over
  // Delta features not otherwise exercised by this file (throwaway probes,
  // since deleted, not assumed from documentation): each is confirmed
  // transparent to Invaract - a real write against a table with the
  // feature enabled is recognized exactly the same way as one without it,
  // no special-casing needed in WriteCommandSupport. Unlike schema
  // evolution and generated columns above, none of these needed a fix.

  test("PASS: a DELETE against a table with deletion vectors enabled executes normally") {
    val tablePath = scratchDir.resolve("dv_target").toString
    val tableName = "dv_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect() // must not throw
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id <= 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  test("PASS: writes and DML against a table with column mapping mode 'name' execute normally") {
    val tablePath = scratchDir.resolve("colmap_target").toString
    val tableName = "colmap_pass_tbl"
    spark.sql(
      s"""CREATE TABLE $tableName (id LONG, doubled LONG) USING delta
         |LOCATION '${tablePath.replace('\\', '/')}'
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.minReaderVersion' = '2',
         |  'delta.minWriterVersion' = '5'
         |)
         |""".stripMargin)
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("append").saveAsTable(tableName)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id = 5").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6)
  }

  test("PASS: appending to a table with liquid clustering (CLUSTER BY) executes normally") {
    val tablePath = scratchDir.resolve("cluster_target").toString
    val tableName = "cluster_pass_tbl"
    spark.sql(
      s"""CREATE TABLE $tableName (id LONG, doubled LONG) USING delta
         |CLUSTER BY (id)
         |LOCATION '${tablePath.replace('\\', '/')}'
         |""".stripMargin)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
    }

    assert(spark.table(tableName).count() == 5)
  }

  // CHECK constraints are enforced independently by Delta itself, at
  // commit time - Invaract's structural checks and Delta's own constraint
  // enforcement operate orthogonally, with no interaction/gap: a violating
  // write is recognized by Invaract identically to a satisfying one (no
  // diagnostic, no violation - Invaract has no vocabulary for row-level
  // constraints, only schema/location/format/save-mode), but Delta itself
  // then rejects it before commit. Confirmed empirically, not assumed.
  test("PASS: a write satisfying a Delta CHECK constraint executes normally; a violating one is rejected by Delta itself, not Invaract") {
    val tablePath = scratchDir.resolve("check_target").toString
    val tableName = "check_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    spark.sql(s"ALTER TABLE $tableName ADD CONSTRAINT id_positive CHECK (id >= 0)")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val satisfying = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      satisfying.write.format("delta").mode("append").saveAsTable(tableName) // must not throw - Invaract passes, constraint satisfied

      val violating = spark.createDataFrame(Seq((-2L, -4L))).toDF("id", "doubled")
      intercept[org.apache.spark.sql.delta.schema.DeltaInvariantViolationException] {
        violating.write.format("delta").mode("append").saveAsTable(tableName)
        // Invaract itself raises nothing here (no ContractViolationException) - the row that
        // gets rejected is rejected by Delta's own commit-time constraint enforcement, not by
        // Invaract, which has no rule vocabulary for a CHECK constraint's condition.
      }
    }

    assert(spark.table(tableName).count() == 6, "only the satisfying row (plus the original 5) was ever committed")
  }

  // Every DML PASS/FAIL pair above targets a catalog table, where
  // catalogTable is always populated - real, but not the only shape.
  // `UPDATE delta.\`path\`` operates directly on a path with no catalog
  // entry at all (confirmed empirically: catalogTable is None, not just
  // missing a location), exercising deltaRowLevelDml's fallback branch -
  // no active contract needed, direct inspection instead, the same
  // pattern as the streaming format-detection test above.
  //
  // This is also the regression test for a real, fixed instability in that
  // fallback: it used to report `target.toString` (raw LogicalPlan.toString,
  // which renders any attribute reference as "name#<exprId>", a per-session
  // counter, not a property of the query) as the write's location. Confirmed
  // directly: running the identical MERGE against two separately-created,
  // equivalently-shaped path tables produces two different raw
  // `target.toString` values purely from exprId allocation order (a
  // `SubqueryAlias` recursing into `Relation [id#348L,v#349L] parquet` for
  // one table, `Relation [id#1762L,v#1763L] parquet` for the other), while
  // `.canonicalized` (which also strips the `SubqueryAlias` wrapper via its
  // own `EliminateSubqueryAliases` rule) renders both as the identical
  // `Relation [none#0L,none#1L] parquet`. Fixed by switching to
  // `target.canonicalized.toString`, mirroring deleteFromTable's own fix
  // below for the same underlying reason.
  test("WriteCommandSupport falls back to the target plan's canonicalized toString for a path-based DML op with no catalog table, stable across separate tables") {
    val tablePath = scratchDir.resolve("path_dml_target").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    capturedPlans.clear()

    spark.sql(s"UPDATE delta.`${tablePath.replace('\\', '/')}` SET doubled = doubled + 1 WHERE id > 2").collect()

    val upd = capturedPlans.collectFirst { case p if p.getClass.getSimpleName == "UpdateCommand" => p }
      .getOrElse(fail("no UpdateCommand plan observed"))
    val info = WriteCommandSupport.combined.lift(upd).getOrElse(fail("path-based UpdateCommand should still be recognized"))
    assert(info.format.contains("delta"))
    assert(info.diagnostic.isDefined, "no catalog table at all should report a fallback diagnostic, not resolve a clean location silently")

    // A second, entirely separate path-based Delta table of the identical
    // shape - different physical path, and (since Spark's exprId counter is
    // session-global and monotonic) necessarily different exprIds for its
    // own "id"/"doubled" columns - must still resolve to the exact same
    // fallback location string, since the fallback's whole purpose is a
    // location Invaract can compare across runs of the "same" job, not one
    // that happens to differ only because Spark allocated different exprIds
    // this time.
    val tablePath2 = scratchDir.resolve("path_dml_target_2").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath2)
    capturedPlans.clear()
    spark.sql(s"UPDATE delta.`${tablePath2.replace('\\', '/')}` SET doubled = doubled + 1 WHERE id > 2").collect()
    val upd2 = capturedPlans.collectFirst { case p if p.getClass.getSimpleName == "UpdateCommand" => p }
      .getOrElse(fail("no UpdateCommand plan observed for the second table"))
    val info2 = WriteCommandSupport.combined.lift(upd2).getOrElse(fail("path-based UpdateCommand should still be recognized"))
    assert(
      info.location == info2.location,
      s"the fallback location must be stable across two structurally-identical path tables with necessarily " +
        s"different exprIds, got '${info.location}' vs '${info2.location}'"
    )
  }

  // deleteFromTable's own "no NamedRelation found" fallback - reached only
  // when DeleteFromTable.table's subtree contains no NamedRelation at all,
  // a shape real Spark analysis apparently never produces (every DELETE
  // FROM test above, catalog- or path-based, resolves to a NamedRelation),
  // so this is exercised by constructing the real Catalyst node directly
  // rather than a mock - LocalRelation is a genuine Spark LogicalPlan, not
  // a NamedRelation, so wrapping one in DeleteFromTable hits exactly the
  // branch under test.
  //
  // This is the regression test for a real, fixed instability: that
  // fallback used to report `cmd.table.toString` (raw LogicalPlan.toString,
  // which renders any attribute reference as "name#<exprId>", a per-JVM-
  // session counter, not a property of the query) as the write's location.
  // Confirmed directly: constructing the identical LocalRelation shape
  // twice (each AttributeReference("id", LongType)() call mints its own
  // fresh exprId) produces two different raw strings but the identical
  // canonicalized one, since Spark's own `.canonicalized` (built for
  // exactly this kind of structural/semantic plan comparison) normalizes
  // exprIds away. Fixed by switching to `cmd.table.canonicalized.toString`.
  test("WriteCommandSupport's deleteFromTable fallback location is stable across separate exprId allocations") {
    def targetWithNoNamedRelation(): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      org.apache.spark.sql.catalyst.plans.logical.LocalRelation(
        Seq(org.apache.spark.sql.catalyst.expressions.AttributeReference("id", org.apache.spark.sql.types.LongType)())
      )

    def deleteFromTableInfo(): WriteCommandInfo = {
      val cmd = org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable(
        targetWithNoNamedRelation(),
        org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
      )
      WriteCommandSupport.combined.lift(cmd).getOrElse(fail("DeleteFromTable must always be recognized, even with no NamedRelation under its target"))
    }

    val info1 = deleteFromTableInfo()
    val info2 = deleteFromTableInfo()
    assert(info1.diagnostic.isDefined, "the no-NamedRelation fallback must report a diagnostic, not resolve a clean location silently")
    assert(
      info1.location == info2.location,
      s"the fallback location must be stable across separate exprId allocations for the identical target shape, " +
        s"got '${info1.location}' vs '${info2.location}'"
    )
  }

  // RuleVerifier: the three DML rule types (com.invaract.contract.RuleType)
  // checked against RowMutationSupport's extraction, per PASS/FAIL pair -
  // exercised against real Delta MERGE/UPDATE/DELETE, the same "must
  // actually execute, or must be aborted before touching the table"
  // discipline as every other DML test in this file.

  test("PASS: a MERGE INTO satisfying its contract's merge_condition rule executes normally") {
    val tablePath = scratchDir.resolve("rule_merge_pass_target").toString
    val tableName = "rule_merge_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("FAIL: a MERGE INTO whose ON condition doesn't match its contract's merge_condition rule is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_merge_fail_target").toString
    val tableName = "rule_merge_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id, region]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleMergeConditionViolation && v.message.contains("region")),
      s"expected a RULE_MERGE_CONDITION_VIOLATION naming 'region', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  // A real regression test for the predicate-logic upgrade: before it, a
  // declared column that was merely *referenced* anywhere in the ON
  // condition satisfied merge_condition, even via a range check that
  // never actually matches target against source on it. This MERGE's
  // condition references 'region' (in a `>` comparison) without an
  // equality pairing for it at all - this must now be rejected, where it
  // would previously have wrongly passed.
  test("FAIL: a MERGE INTO whose ON condition only range-checks a declared column, never equality-matching it, is aborted") {
    val tablePath = scratchDir.resolve("rule_merge_range_check_target").toString
    val tableName = "rule_merge_range_check_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).withColumn("region", lit("us")).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |        - name: region
         |          type: string
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id, region]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled, 'us' as region) s
             |ON t.id = s.id AND t.region > 'a'
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleMergeConditionViolation && v.message.contains("region")),
      s"expected a RULE_MERGE_CONDITION_VIOLATION naming 'region', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  test("PASS: a MERGE INTO satisfying merge_condition via a differently-named source column executes normally") {
    val tablePath = scratchDir.resolve("rule_merge_crossname_target").toString
    val tableName = "rule_merge_crossname_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as source_id, 198L as doubled) s
           |ON t.id = s.source_id
           |WHEN NOT MATCHED THEN INSERT (id, doubled) VALUES (s.source_id, s.doubled)
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  // Real regression test for the De Morgan/NOT upgrade to
  // RuleVerifier.equalityPairedColumns: before it, an ON condition
  // written as `NOT (t.id != s.id)` - logically identical to `t.id =
  // s.id`, and genuinely how Spark's own SQL parser represents `!=`
  // (always `Not(EqualTo(...))`, never a native "not equal" comparison
  // node) - would have been wrongly rejected, since the old
  // equalityPairedColumns didn't descend into NOT at all. Uses real
  // `spark.sql` parsing, not a hand-built IR node, so this proves Spark
  // genuinely produces the doubly-negated shape this fix targets.
  test("PASS: a MERGE INTO whose ON condition is written as NOT(!=) satisfies merge_condition") {
    val tablePath = scratchDir.resolve("rule_merge_not_ne_target").toString
    val tableName = "rule_merge_not_ne_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled) s
           |ON NOT (t.id != s.id)
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  // Real regression test for the target-/source-side qualifier fix: before
  // it, a same-side tautology like `ON t.id = t.id` (matching every row
  // against itself, never actually comparing target to source) was wrongly
  // accepted as satisfying merge_condition: [id], purely because the
  // declared column name appeared in an equality. Uses real spark.sql
  // parsing to confirm Spark genuinely preserves each side's own qualifier
  // ("t" on both operands here) all the way through to the analyzed plan
  // this module translates.
  test("FAIL: a MERGE INTO whose ON condition compares a target column to ITSELF (same-side tautology) is aborted") {
    val tablePath = scratchDir.resolve("rule_merge_same_side_target").toString
    val tableName = "rule_merge_same_side_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled) s
             |ON t.id = t.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleMergeConditionViolation && v.message.contains("id")),
      s"expected a RULE_MERGE_CONDITION_VIOLATION naming 'id', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  // docs/SEMANTIC_LINEAGE_FINGERPRINTING.md's RowMutation section: a MERGE's
  // ON condition (or a conditional DELETE's predicate) is real
  // transformation-defining behavior that ir.Plan alone never captures -
  // WriteCommandSupport's own doc notes `query = source` for MERGE, never
  // the ON condition. Proven here end to end through the real check rule,
  // not just at the fingerprint module's own unit-test level: the SAME
  // target/source shape, differing only in the MERGE's ON condition, must
  // still produce different published fingerprints.
  test("computeFingerprint = true: a MERGE's ON condition changing moves the published fingerprint, even though the plan shape is unchanged") {
    def runMerge(onClause: String, tableSuffix: String): com.invaract.fingerprint.TransformationFingerprint = {
      val tablePath = scratchDir.resolve(s"rule_merge_fingerprint_target_$tableSuffix").toString
      val tableName = s"rule_merge_fingerprint_tbl_$tableSuffix"
      spark.range(5).withColumn("doubled", col("id") * 2).withColumn("region", lit("us")).write.format("delta").mode("overwrite").save(tablePath)
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

      val yaml =
        s"""id: enforcement_demo
           |version: "1.0.0"
           |outputs:
           |  - name: out
           |    location: $tablePath
           |    schema:
           |      fields:
           |        - name: id
           |          type: long
           |          required: false
           |        - name: doubled
           |          type: long
           |          required: false
           |        - name: region
           |          type: string
           |          required: false
           |""".stripMargin
      val sink = new TestNotificationSink

      withContract(yaml, options = VerificationOptions(computeFingerprint = true), sink = Some(sink)) {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled, 'us' as region) s
             |ON $onClause
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect() // must not throw - this contract declares no rule
      }

      sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }.last.fingerprints
        .getOrElse(fail("computeFingerprint = true must always populate a fingerprint on a PASSing MERGE too"))
    }

    val byId = runMerge("t.id = s.id", "by_id")
    val byIdAndRegion = runMerge("t.id = s.id AND t.region = s.region", "by_id_and_region")

    assert(byId.overall != byIdAndRegion.overall, "the ON condition is real behavior - it must move the fingerprint")
    assert(byId.rowMutation.isDefined && byIdAndRegion.rowMutation.isDefined)
    assert(byId.rowMutation != byIdAndRegion.rowMutation)
    // Neither MERGE's target/source shape itself changed - only the ON
    // condition - so the plan-only pieces must stay identical.
    assert(byId.inputs.keySet == byIdAndRegion.inputs.keySet)
    assert(byId.outputs == byIdAndRegion.outputs)
  }

  test("PASS: a DELETE with a filtering predicate satisfies its contract's forbid_unconditional_delete rule") {
    val tablePath = scratchDir.resolve("rule_delete_pass_target").toString
    val tableName = "rule_delete_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: forbid_unconditional_delete
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  test("FAIL: an unconditional DELETE violates its contract's forbid_unconditional_delete rule and is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_delete_fail_target").toString
    val tableName = "rule_delete_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: forbid_unconditional_delete
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"DELETE FROM $tableName").collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.RuleUnconditionalDelete))
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the DELETE must be aborted before touching the table, not merely reported as failed")
  }

  test("PASS: an UPDATE assigning only allowed columns satisfies its contract's allowed_update_columns rule") {
    val tablePath = scratchDir.resolve("rule_update_pass_target").toString
    val tableName = "rule_update_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: allowed_update_columns
         |    columns: [doubled]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 5, "the UPDATE must actually have run against all 5 original rows")
  }

  test("FAIL: an UPDATE assigning a disallowed column violates its contract's allowed_update_columns rule and is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_update_fail_target").toString
    val tableName = "rule_update_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: allowed_update_columns
         |    columns: [doubled]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"UPDATE $tableName SET id = id + 100 WHERE id > 2").collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleDisallowedUpdateColumn && v.message.contains("id")),
      s"expected a RULE_DISALLOWED_UPDATE_COLUMN naming 'id', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the UPDATE must be aborted before touching the table, not merely reported as failed")
  }

  // Closes the "operation surface" gaps docs/ADDING_A_SPARK_CONNECTOR.md's
  // coverage ledger flagged: .format("delta").saveAsTable() on a NEW
  // table, .saveAsTable()/.insertInto() appending to an EXISTING table,
  // and DataFrameWriterV2 (.writeTo()) all analyze to V2 write commands
  // (ReplaceTableAsSelect/AppendData/OverwriteByExpression) - confirmed
  // empirically via injectCheckRule, not assumed. These used to only
  // fail closed (safely rejected, but never actually checked against a
  // contract) - now WriteCommandSupport recognizes all three, so they're
  // genuinely verified, the same as every other write shape. Location
  // differs between the two, confirmed empirically: ReplaceTableAsSelect
  // (a table that doesn't exist yet) has no physical path to resolve, so
  // it uses the qualified catalog identifier ("spark_catalog.default.
  // <table>"); AppendData/OverwriteByExpression target an *existing*
  // table, whose resolved DataSourceV2 Table reports a physical
  // warehouse path via properties() - the same "prefer a real path,
  // fall back to the identifier" asymmetry createDataSourceTableAsSelect
  // already has for V1 new-table writes.
  test("PASS: .format(\"delta\").saveAsTable() on a new table, satisfying its contract, executes normally") {
    val tableName = "pass_rtas_new_tbl"
    val expectedLocation = s"spark_catalog.default.$tableName"
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    format: delta
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: doubled
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("overwrite").saveAsTable(tableName) // must not throw
    }

    assert(spark.catalog.tableExists(tableName))
  }

  test("FAIL: .format(\"delta\").saveAsTable() on a new table, violating its contract, is aborted before any table is created") {
    val tableName = "fail_rtas_new_tbl"
    val expectedLocation = s"spark_catalog.default.$tableName"
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.format("delta").mode("overwrite").saveAsTable(tableName)
      }
    }

    assert(!spark.catalog.tableExists(tableName), "the new table must never be created, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  test("PASS: appending to an existing Delta table via .saveAsTable()/.insertInto()/.writeTo(), satisfying its contract, executes normally") {
    val tableName = "pass_append_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").saveAsTable(tableName)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: doubled
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
      df.write.insertInto(tableName) // must not throw
      df.writeTo(tableName).append() // must not throw
      df.writeTo(tableName).overwrite(org.apache.spark.sql.functions.lit(true)) // must not throw
    }

    assert(spark.table(tableName).count() > 0)
  }

  test("FAIL: appending to an existing Delta table via .saveAsTable()/.insertInto()/.writeTo(), violating its contract, is aborted before anything is written") {
    val tableName = "fail_append_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").saveAsTable(tableName)
    val beforeRows = spark.table(tableName).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)

      val exSaveAsTable = intercept[ContractViolationException](df.write.format("delta").mode("append").saveAsTable(tableName))
      assert(exSaveAsTable.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exInsertInto = intercept[ContractViolationException](df.write.insertInto(tableName))
      assert(exInsertInto.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exWriteToAppend = intercept[ContractViolationException](df.writeTo(tableName).append())
      assert(exWriteToAppend.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exWriteToOverwrite = intercept[ContractViolationException](df.writeTo(tableName).overwrite(org.apache.spark.sql.functions.lit(true)))
      assert(exWriteToOverwrite.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    }

    val afterRows = spark.table(tableName).collect().toSet
    assert(beforeRows == afterRows, "none of the rejected attempts may have actually written anything")
  }

  // Closes the most significant coverage-ledger gap found investigating
  // Delta support: a streaming write's top-level plan (WriteToStream)
  // isn't Command-shaped, so it was invisible to the fail-closed policy
  // entirely - not "fails closed, unverified" like the V2 write commands
  // above, but genuinely unenforced (confirmed via a real probe: zero of
  // the plans injectCheckRule saw during a real streaming Delta write were
  // Command-shaped, and via javap confirming WriteToStream doesn't
  // implement Command - see docs/SPARK_ADAPTER.md's "Streaming writes"
  // section). Recognizing WriteToStream in WriteCommandSupport - the same
  // registry every other write shape goes through, rather than a
  // special-cased check here - means it's genuinely verified, not merely
  // allowed or blocked wholesale. This PASS/FAIL pair proves that through
  // real enforcement of a real streaming Delta write, not just translation
  // in isolation.
  test("PASS: a streaming Delta write satisfying its contract starts and writes normally") {
    val sinkPath = scratchDir.resolve("pass_stream").toString
    val checkpointPath = scratchDir.resolve("pass_stream_checkpoint").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .start(sinkPath) // must not throw
      query.awaitTermination()
    }

    assert(Files.exists(java.nio.file.Paths.get(sinkPath)))
  }

  test("FAIL: a streaming Delta write violating its contract is aborted before the query starts, nothing written") {
    val sinkPath = scratchDir.resolve("fail_stream").toString
    val checkpointPath = scratchDir.resolve("fail_stream_checkpoint").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      intercept[ContractViolationException] {
        streamDf.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointPath)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start(sinkPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(sinkPath)), "the streaming write must be rejected before the query ever starts, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // A second, distinct WriteToStream shape: `.toTable(...)` resolves a
  // real `catalogTable` (confirmed empirically - unlike the path-based
  // `.start(path)` pair above, where it's None and the sink's own
  // reflectively-read `path()` is used instead). Exercises the other half
  // of `streamSinkLocationAndFormat`'s branching, the same way
  // `createDataSourceTableAsSelect`'s catalog-table path is exercised
  // separately from `saveIntoDataSource`'s options-map path above.
  test("PASS: a streaming Delta .toTable() write satisfying its contract starts and writes normally") {
    val tableName = "pass_stream_to_table_tbl"
    val checkpointPath = scratchDir.resolve("pass_stream_to_table_checkpoint").toString
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .toTable(tableName) // must not throw
      query.awaitTermination()
    }

    assert(spark.catalog.tableExists(tableName))
  }

  // The PASS/FAIL pair above proves end-to-end enforcement, but never
  // directly inspects the format WriteCommandSupport detected for a
  // streaming Delta write - StructuralVerifier only compares format when
  // the contract also declares one and both sides are known, so a bug in
  // `streamSinkFormatOf` (returning the wrong format, or "delta" for a
  // non-Delta sink) wouldn't necessarily surface as a PASS/FAIL test
  // failure on its own. Inspects WriteCommandInfo directly instead, no
  // active contract needed - the write completes normally either way.
  test("WriteCommandSupport detects format \"delta\" for a real streaming Delta write") {
    val sinkPath = scratchDir.resolve("format_detection_stream").toString
    val checkpointPath = scratchDir.resolve("format_detection_stream_checkpoint").toString
    capturedPlans.clear()

    val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    val query = streamDf.writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
      .start(sinkPath)
    query.awaitTermination()

    val ws = capturedPlans.collectFirst { case w: org.apache.spark.sql.catalyst.streaming.WriteToStream => w }
      .getOrElse(fail("no WriteToStream plan observed"))
    val info = WriteCommandSupport.combined.lift(ws).getOrElse(fail("WriteToStream should be recognized by WriteCommandSupport"))
    assert(info.format.contains("delta"), s"expected format 'delta' for a real Delta streaming sink, got ${info.format}")
    assert(info.location.contains("format_detection_stream"))
  }

  // Before WriteCommandSupport recognized WriteToStream, this test
  // documented the accidental consequence of streaming being entirely
  // invisible to enforcement: starting ANY streaming query while a
  // mismatched contract was active never threw, because WriteToStream fell
  // into the untranslated-and-not-Command-shaped silent no-op. Now that a
  // streaming write is a real, recognized write, it's checked against
  // whatever contract is active the same way every batch write always has
  // been - `forContract`'s own doc says "verifies any write this session
  // performs against contract", not "any write whose location happens to
  // match". A streaming write to an unrelated location under an active,
  // unrelated contract is therefore correctly rejected
  // (OUTPUT_LOCATION_MISMATCH), consistent with batch writes, not silently
  // allowed through the way it used to be. Uses the "memory" sink
  // (a genuine V2 Table, unlike Delta's legacy-wrapped one - confirmed via
  // javap) to exercise the other branch of location resolution:
  // `sink.name()` succeeding directly, no reflection needed.
  test("a streaming write to a location unrelated to the active contract is rejected, consistent with batch writes") {
    val yaml =
      """id: enforcement_demo
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: some/other/contract/output
        |    schema:
        |      fields:
        |        - name: id
        |          type: long
        |          required: true
        |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
      intercept[ContractViolationException] {
        val query = streamDf.writeStream
          .format("memory")
          .queryName("unrelated_stream_q")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start()
        query.awaitTermination()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.OutputLocationMismatch))
  }

  // Regression guard for the fail-closed policy's biggest risk: it must
  // NOT reject ordinary catalog/DDL operations just because they're
  // Command-shaped and untranslated — only FailClosedCommands' known-safe
  // list stands between "legitimate DDL" and "rejected as unverifiable",
  // so this proves that list actually works for real commands, not just
  // that it type-checks.
  test("non-data DDL commands (CREATE TABLE, ANALYZE TABLE) are never blocked by the fail-closed policy") {
    val yaml =
      """id: would_always_fail
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: nonexistent/location
        |    schema:
        |      fields:
        |        - name: impossible_field
        |          type: string
        |          required: true
        |""".stripMargin

    withContract(yaml) {
      spark.sql("CREATE TABLE IF NOT EXISTS ddl_regression_tbl (id INT) USING parquet").collect()
      spark.sql("ANALYZE TABLE ddl_regression_tbl COMPUTE STATISTICS").collect()
      spark.sql("SHOW TABLES").collect()
    }
    // no exception means every DDL/administrative statement completed
    succeed
  }

  test("the abort exception explains what/what/why/how — all four, not just a bare violation code") {
    val outputPath = scratchDir.resolve("fail_explain.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "2.1.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    val message = ex.getMessage

    // What the contract expected
    assert(message.contains("What the contract expects"))
    assert(message.contains("customer_name"))
    assert(message.contains("enforcement_demo@2.1.0"))

    // What the plan contains
    assert(message.contains("What the plan contains"))
    assert(message.contains("Write("))

    // Why it violates
    assert(message.contains("Why it violates the contract"))
    assert(message.contains("MISSING_OUTPUT_FIELD"))

    // How to correct it
    assert(message.contains("How to correct it"))
    assert(message.contains("Add a 'customer_name' column"))
  }

  test("the same violation produces a byte-identical explanation every time (deterministic)") {
    val outputPath = scratchDir.resolve("fail_deterministic.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: missing_a
         |          type: string
         |          required: true
         |        - name: missing_b
         |          type: string
         |          required: true
         |""".stripMargin

    def attempt(): String = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }.getMessage
    }

    val first = attempt()
    val second = attempt()
    val third = attempt()

    assert(first == second)
    assert(second == third)
  }

  test("non-write queries (reads, counts) never trigger verification, even against a contract they'd violate") {
    // A contract that would fail immediately if applied to *any* plan --
    // proves the check rule really does gate on "is this a Write", not on
    // "is a contract active".
    val yaml =
      """id: would_always_fail
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: nonexistent/location
        |    schema:
        |      fields:
        |        - name: impossible_field
        |          type: string
        |          required: true
        |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.count() // triggers analysis of Range/Project plans, not a Write
      df.collect()
    }
    // no exception means both actions completed successfully
    succeed
  }

  test("VerificationOptions thread through to enforcement: rejectUndeclaredFields turns an extra column into an abort") {
    val outputPath = scratchDir.resolve("fail_undeclared.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2) // 'doubled' is undeclared
      df.write.mode("overwrite").parquet(outputPath) // permissive default: passes
    }
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))

    val strictOutputPath = scratchDir.resolve("fail_undeclared_strict.parquet").toString
    val strictYaml = yaml.replace(outputPath, strictOutputPath)
    withContract(strictYaml, VerificationOptions(rejectUndeclaredFields = true)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(strictOutputPath)
      }
    }
    assert(!Files.exists(java.nio.file.Paths.get(strictOutputPath)))
  }

  test("forContract builds a usable check-rule function directly") {
    val outputPath = scratchDir.resolve("for_contract.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val contract = parseContract(yaml)
    val rule = ContractEnforcementRule.forContract(contract)

    // Exercise the built function directly against a translated plan,
    // independent of SparkSession wiring: same shape injectCheckRule wants.
    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    rule(spark)(df.queryExecution.analyzed) // no write command in this plan -> no-op, must not throw
  }

  // Exercises the public forContract(contract, options, sink) overload
  // directly — the same "call the returned function without going through
  // SparkSession/injectCheckRule wiring" approach the plain forContract
  // test above uses, rather than standing up a second SparkSession with
  // different extensions mid-suite (Spark's getOrCreate() reuses the
  // already-active session/SparkContext, so a second withExtensions call
  // wouldn't actually take effect here). capturedPlans (populated by this
  // suite's own shared check rule) already holds a real analyzed write
  // plan from any prior test in this file — reused here purely as an
  // off-the-shelf LogicalPlan value, not to re-verify it.
  test("forContract(contract, options, sink) — the public 3-arg overload — publishes to the given sink") {
    val outputPath = scratchDir.resolve("for_contract_with_sink.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val contract = parseContract(yaml)
    val sink = new TestNotificationSink
    val rule = ContractEnforcementRule.forContract(contract, VerificationOptions(), sink)

    withContract(yaml) {
      // Populates capturedPlans with a real analyzed write plan, verified
      // by this suite's own shared check rule as normal.
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath)
    }
    val writePlan = capturedPlans.reverseIterator.find(WriteCommandSupport.combined.isDefinedAt).getOrElse(
      fail("no analyzed write plan was captured to reuse")
    )

    rule(spark)(writePlan) // must not throw - it's the same passing contract

    val events = sink.events.collect { case e: com.invaract.sparkadapter.notification.ContractValidationEvent => e }
    assert(events.nonEmpty)
    assert(events.last.status == "PASSED")
    assert(events.last.applicationId.contains(spark.sparkContext.applicationId))
  }

  // Added while raising the module's mutation-testing score (see
  // ROADMAP.md Phase 1c / CLAUDE.md): explain() is private[sparkadapter],
  // so it can be exercised directly with a synthetic result — no need to
  // provoke a real Spark abort just to check its text formatting.
  test("explain pluralizes the violation count and marks optional fields distinctly") {
    val contractYaml =
      """id: explain_demo
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |        - name: note
        |          type: string
        |          required: false
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |""".stripMargin
    val contract = parseContract(contractYaml)
    val plan = com.invaract.ir.Write(com.invaract.ir.DatasetRef("gold.out"), com.invaract.ir.Read(com.invaract.ir.DatasetRef("raw.orders")))

    val oneViolation = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(Violation(ViolationType.MissingOutputField, "msg", "remediation", column = Some("id")))
    )
    val oneText = ContractEnforcementRule.explain(contract, plan, oneViolation)
    assert(oneText.contains("(1 violation):"), oneText)
    assert(!oneText.contains("(1 violations):"), oneText)

    val twoViolations = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(
        Violation(ViolationType.MissingOutputField, "msg1", "remediation1", column = Some("id")),
        Violation(ViolationType.MissingInputField, "msg2", "remediation2", column = Some("note"))
      )
    )
    val twoText = ContractEnforcementRule.explain(contract, plan, twoViolations)
    assert(twoText.contains("(2 violations):"), twoText)

    // "note" is declared optional (required: false); "id" is required.
    // Only the optional field's description should carry "(optional)".
    assert(oneText.contains("id: integer") && !oneText.contains("id: integer (optional)"), oneText)
    assert(oneText.contains("note: string (optional)"), oneText)
  }
}
