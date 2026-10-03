// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.ir
import com.invaract.sparkadapter.ContractEnforcementRule.InferenceOutcome
import com.invaract.sparkadapter.notification._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LogicalPlan}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.collection.mutable

/** A `Command` Invaract has no translation for and has not safe-listed - the shape
  * real enforcement fails closed on (`UNVERIFIABLE_WRITE`).
  */
case class UnrecognizedWriteCommand() extends LeafCommand {
  override def output: Seq[Attribute] = Nil
}

/** A sink recording events, plus every `flush` timeout it was handed. */
private[sparkadapter] class FlushRecordingSink extends TestNotificationSink {
  val flushes: mutable.ListBuffer[Long] = mutable.ListBuffer.empty[Long]
  override def flush(timeoutMs: Long): Unit = synchronized(flushes += timeoutMs)
}

/** `DryRunReporter` against a real `local[*]` `SparkSession` - the same "real Spark over a
  * mock" discipline as `ContractInferenceSpec`, whose check-rule wiring this mirrors.
  */
class DryRunReporterSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  @volatile private var current: Option[DryRunReporter] = None
  private val capturedPlans = mutable.ListBuffer.empty[LogicalPlan]

  private val job = JobInfo(
    applicationId = Some("app-1"),
    appName = Some("orders app"),
    jobId = Some("nightly_orders"),
    attributes = Map("team" -> "data-eng")
  )
  private val fixedClock: () => Long = () => 42L

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-dryrun-reporter-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DryRunReporterSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          capturedPlans.synchronized(capturedPlans += plan)
          current.foreach(_.check(plan))
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def reporterFor(
      sink: NotificationSink,
      jobInfo: JobInfo = job,
      options: VerificationOptions = VerificationOptions(),
      infer: (LogicalPlan, CheckpointRegistry) => Option[InferenceOutcome] =
        (plan, registry) => ContractEnforcementRule.inferOutcome(plan, Some(registry))
  ): DryRunReporter = new DryRunReporter(sink, jobInfo, options, fixedClock, infer)

  private def withReporter[T](reporter: DryRunReporter)(body: => T): T = {
    current = Some(reporter)
    try body
    finally current = None
  }

  private def writeTo(name: String, df: => org.apache.spark.sql.Dataset[_]): String = {
    val path = scratchDir.resolve(name).toString
    df.write.mode("overwrite").parquet(path)
    path
  }

  private def inferenceEvents(sink: TestNotificationSink): List[ContractInferenceEvent] =
    sink.events.collect { case e: ContractInferenceEvent => e }

  private def summaryOf(sink: TestNotificationSink): DryRunSummaryEvent =
    sink.events.collect { case e: DryRunSummaryEvent => e }.last

  /** A real `InferenceOutcome.Inferred`, from a real write, for tests that then alter it. */
  private def realInferred(): InferenceOutcome.Inferred = {
    capturedPlans.synchronized(capturedPlans.clear())
    writeTo("real_inferred.parquet", spark.range(3).withColumn("doubled", col("id") * 2))
    val plans = capturedPlans.synchronized(capturedPlans.toList)
    plans.flatMap(p => ContractEnforcementRule.inferOutcome(p, None)).collectFirst {
      case i: InferenceOutcome.Inferred => i
    }.getOrElse(fail("no recognized write was captured"))
  }

  private def reportingAn(outcome: InferenceOutcome): (FlushRecordingSink, ContractInferenceEvent) = {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink, infer = (_, _) => Some(outcome))
    reporter.check(spark.range(1).queryExecution.analyzed)
    (sink, inferenceEvents(sink).head)
  }

  test("a plain write publishes one INFERRED event carrying the draft, its self-check, a fingerprint and the job") {
    val sink = new TestNotificationSink
    val reporter = reporterFor(sink)
    val path = withReporter(reporter)(writeTo("plain.parquet", spark.range(5).withColumn("doubled", col("id") * 2)))

    val events = inferenceEvents(sink)
    assert(events.size == 1, s"expected exactly one event, got ${events.map(_.status)}")
    val event = events.head
    assert(event.eventType == "CONTRACT_INFERENCE")
    assert(event.status == InferenceStatus.Inferred)
    assert(event.reason.isEmpty)
    assert(event.selfCheck.contains("PASSED"))
    assert(event.selfCheckViolations.isEmpty)
    assert(event.diagnostics.isEmpty)
    assert(event.fingerprints.isDefined)
    assert(event.job == job)
    assert(event.timestamp == 42L)
    assert(event.metadata == Map("team" -> "data-eng"))

    val draft = ContractParser.parse(event.contractYaml.getOrElse(fail("no contract in an INFERRED event")))
    assert(draft.outputs.map(_.schema.fields.map(_.name)) == List(List("id", "doubled")))
    assert(event.writeLocation.contains(draft.outputs.head.location))
    assert(draft.outputs.head.location.endsWith("plain.parquet"), s"$path vs ${draft.outputs.head.location}")
  }

  test("re-observing a draft already published publishes nothing more, and is counted once") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    withReporter(reporter)(writeTo("dedupe.parquet", spark.range(3)))
    val before = inferenceEvents(sink).size
    assert(before == 1)

    val writePlan = capturedPlans.synchronized(capturedPlans.toList).filter(p => WriteCommandSupport.combined.isDefinedAt(p)).last
    reporter.check(writePlan)
    reporter.check(writePlan)
    assert(inferenceEvents(sink).size == before)

    reporter.finish()
    assert(summaryOf(sink).statusCounts == Map(InferenceStatus.Inferred -> 1L))
  }

  test("finish merges every draft of the job into one contract named for the job") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    withReporter(reporter) {
      writeTo("merge_a.parquet", spark.range(3).withColumn("a", col("id")))
      writeTo("merge_b.parquet", spark.range(3).withColumn("b", col("id")))
    }
    reporter.finish()

    val summary = summaryOf(sink)
    assert(summary.eventType == "DRY_RUN_SUMMARY")
    assert(summary.statusCounts == Map(InferenceStatus.Inferred -> 2L))
    assert(summary.mergeStatus == "MERGED")
    assert(summary.mergeConflicts.isEmpty)
    assert(summary.job == job)
    assert(summary.timestamp == 42L)
    assert(summary.metadata == Map("team" -> "data-eng"))
    val merged = ContractParser.parse(summary.mergedContractYaml.getOrElse(fail("no merged contract")))
    assert(merged.id == "nightly_orders")
    assert(merged.outputs.map(_.location.split('/').last).toSet == Set("merge_a.parquet", "merge_b.parquet"))
  }

  test("two writes to one location with different schemas are reported as a CONFLICT, never guessed at") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    withReporter(reporter) {
      writeTo("conflict.parquet", spark.range(3).withColumn("a", col("id")))
      writeTo("conflict.parquet", spark.range(3))
    }
    reporter.finish()

    val summary = summaryOf(sink)
    assert(summary.statusCounts == Map(InferenceStatus.Inferred -> 2L))
    assert(summary.mergeStatus == "CONFLICT")
    assert(summary.mergedContractYaml.isEmpty)
    assert(summary.mergeConflicts.size == 1)
    assert(summary.mergeConflicts.head.contains("conflict.parquet"))
  }

  test("a job that wrote nothing reports NO_WRITES_OBSERVED, and a plain query is not a write") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    withReporter(reporter) {
      assert(spark.range(3).count() == 3L)
    }
    assert(sink.events.isEmpty, "a read/count is not write-shaped: nothing to report until the summary")
    reporter.finish()

    val summary = summaryOf(sink)
    assert(summary.statusCounts == Map(InferenceStatus.NoWritesObserved -> 1L))
    assert(summary.mergeStatus == "NO_DRAFTS")
    assert(summary.mergedContractYaml.isEmpty)
    assert(summary.mergeConflicts.isEmpty)
    assert(sink.events.size == 1)
  }

  test("finish publishes once, flushes the sink once with the bounded timeout, and is idempotent") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    reporter.finish()
    reporter.finish()
    assert(sink.events.collect { case e: DryRunSummaryEvent => e }.size == 1)
    assert(sink.flushes.toList == List(DryRunReporter.FlushTimeoutMs))
    assert(DryRunReporter.FlushTimeoutMs == 10000L)
  }

  test("an unrecognized, un-safe-listed Command is reported SKIPPED_UNRECOGNIZED, once, with no draft") {
    val sink = new FlushRecordingSink
    val reporter = reporterFor(sink)
    val plan = UnrecognizedWriteCommand()
    reporter.check(plan)
    reporter.check(plan)

    val event = inferenceEvents(sink).head
    assert(inferenceEvents(sink).size == 1)
    assert(event.status == InferenceStatus.SkippedUnrecognized)
    assert(event.reason.exists(_.contains("UnrecognizedWriteCommand")))
    assert(event.reason.exists(_.contains("UNVERIFIABLE_WRITE")))
    assert(event.contractYaml.isEmpty)
    assert(event.selfCheck.isEmpty)
    assert(event.selfCheckViolations.isEmpty)
    assert(event.diagnostics.isEmpty)
    assert(event.fingerprints.isEmpty)
    assert(event.writeLocation.isEmpty)

    reporter.finish()
    val summary = summaryOf(sink)
    assert(summary.statusCounts == Map(InferenceStatus.SkippedUnrecognized -> 1L), "a skipped write is a write observed")
    assert(summary.mergeStatus == "NO_DRAFTS")
  }

  test("a Command real enforcement safe-lists is not reported") {
    val sink = new TestNotificationSink
    val reporter = reporterFor(sink)
    val plan = spark.sql("SHOW TABLES").queryExecution.analyzed
    assert(FailClosedCommands.isKnownSafe(plan), s"${plan.getClass.getName} was expected to be safe-listed")
    reporter.check(plan)
    assert(sink.events.isEmpty)
  }

  test("an exception during inference becomes an INFERENCE_ERROR event instead of reaching the job") {
    val sink = new TestNotificationSink
    val reporter = reporterFor(sink, infer = (_, _) => throw new RuntimeException("boom"))
    val plan = spark.range(1).queryExecution.analyzed
    reporter.check(plan)
    reporter.check(plan)

    val event = inferenceEvents(sink).head
    assert(inferenceEvents(sink).size == 1, "the same failure is reported once")
    assert(event.status == InferenceStatus.InferenceError)
    assert(event.reason.contains("RuntimeException: boom"))
    assert(event.contractYaml.isEmpty)
    assert(event.selfCheck.isEmpty)
  }

  test("a failure after inference (here: while classifying the draft) is contained and reported the same way") {
    val real = realInferred()
    val sink = new TestNotificationSink
    val reporter = reporterFor(sink, infer = (_, _) => Some(real.copy(translated = real.translated.copy(plan = null))))
    reporter.check(spark.range(1).queryExecution.analyzed) // must not throw
    assert(inferenceEvents(sink).map(_.status) == List(InferenceStatus.InferenceError))
  }

  test("a StackOverflowError during inference is contained too") {
    val sink = new TestNotificationSink
    val reporter = reporterFor(sink, infer = (_, _) => throw new StackOverflowError())
    reporter.check(spark.range(1).queryExecution.analyzed)
    assert(inferenceEvents(sink).map(_.status) == List(InferenceStatus.InferenceError))
  }

  test("a sink that throws never reaches the job, from check or from finish, and the next event is still tried") {
    val published = mutable.ListBuffer.empty[NotificationEvent]
    val sink = new NotificationSink {
      override def publish(event: NotificationEvent): Unit = {
        published += event
        throw new IllegalStateException("sink down")
      }
      override def flush(timeoutMs: Long): Unit = throw new IllegalStateException("flush down")
    }
    val reporter = reporterFor(sink)
    withReporter(reporter)(writeTo("sink_down.parquet", spark.range(2))) // must not throw
    reporter.finish() // must not throw
    assert(published.exists(_.isInstanceOf[ContractInferenceEvent]))
    assert(published.exists(_.isInstanceOf[DryRunSummaryEvent]))
  }

  test("a checkpoint resolution that had to guess makes the draft INFERRED_DEGRADED, and says why") {
    val real = realInferred()
    val guess = Diagnostic(CheckpointRegistry.ResolutionDiagnosticType, "picked the most recent of 2 candidate plans")
    val altered = real.copy(translated = real.translated.copy(diagnostics = List(guess)))
    val (_, event) = reportingAn(altered)

    assert(event.status == InferenceStatus.InferredDegraded)
    assert(event.reason.contains("picked the most recent of 2 candidate plans"))
    assert(event.diagnostics == List("picked the most recent of 2 candidate plans"))
    assert(event.contractYaml.isDefined, "a degraded draft is still published, so it can be reviewed")
  }

  test("an ordinary translation gap (an opaque node) does NOT degrade a draft: its inputs and schemas are exact") {
    val real = realInferred()
    assert(real.translated.diagnostics.nonEmpty, "premise: spark.range already translates with a diagnostic")
    val (_, event) = reportingAn(real)
    assert(event.status == InferenceStatus.Inferred)
    assert(event.diagnostics.isEmpty)
  }

  test("a write-shape diagnostic and an unresolved checkpoint boundary each degrade the draft") {
    val real = realInferred()
    val withWriteDiagnostic = real.copy(writeInfo = real.writeInfo.copy(diagnostic = Some(Diagnostic("Write", "approximate"))))
    val (_, byWrite) = reportingAn(withWriteDiagnostic)
    assert(byWrite.status == InferenceStatus.InferredDegraded)
    assert(byWrite.diagnostics == List("Write: approximate"))

    val withUnknown =
      real.copy(translated = real.translated.copy(plan = ir.UnknownPlan("opaque", sourceType = "LogicalRDD")))
    val (_, byUnknown) = reportingAn(withUnknown)
    assert(byUnknown.status == InferenceStatus.InferredDegraded)
    assert(byUnknown.diagnostics.exists(d => d.contains("(LogicalRDD)") && d.contains("unresolved")))

    val otherNode = real.copy(translated = real.translated.copy(plan = ir.UnknownPlan("opaque", sourceType = "Range")))
    assert(reportingAn(otherNode)._2.status == InferenceStatus.Inferred, "only a lineage boundary degrades")
  }

  test("the same caveat from two sources is listed once") {
    val real = realInferred()
    val altered = real.copy(
      translated = real.translated.copy(diagnostics = List(Diagnostic(CheckpointRegistry.ResolutionDiagnosticType, "Write: approximate"))),
      writeInfo = real.writeInfo.copy(diagnostic = Some(Diagnostic("Write", "approximate")))
    )
    val (_, event) = reportingAn(altered)
    assert(event.diagnostics == List("Write: approximate"))
  }

  test("a draft that fails verification against its own write reports selfCheck FAILED with the violations") {
    val real = realInferred()
    val moved = real.contract.copy(outputs = real.contract.outputs.map(_.copy(location = "/somewhere/else")))
    val (_, event) = reportingAn(real.copy(contract = moved))

    assert(event.selfCheck.contains("FAILED"))
    assert(event.selfCheckViolations.exists(_.violationType == ViolationType.OutputLocationMismatch))
    assert(event.status == InferenceStatus.Inferred, "self-check is its own field; it does not downgrade the status")
  }

  test("a structurally invalid draft reports selfCheck INVALID") {
    val real = realInferred()
    val (_, event) = reportingAn(real.copy(contract = real.contract.copy(outputs = Nil)))
    assert(event.selfCheck.contains("INVALID"))
    assert(event.selfCheckViolations.map(_.violationType) == List(ViolationType.InvalidContract))
    assert(event.selfCheckViolations.head.message.contains("outputs"))
  }

  test("a self-check that itself throws reports selfCheck ERROR and says so in diagnostics") {
    val real = realInferred()
    val (_, event) = reportingAn(real.copy(writeInfo = real.writeInfo.copy(outputSchema = null)))
    assert(event.selfCheck.contains("ERROR"))
    assert(event.selfCheckViolations.isEmpty)
    assert(event.diagnostics.exists(_.startsWith("self-check threw")))
    assert(event.contractYaml.isDefined)
  }

  test("the self-check verifies under the options enforcement would use") {
    val real = realInferred()
    val narrowed = real.contract.copy(outputs = real.contract.outputs.map(o => o.copy(schema = o.schema.copy(fields = o.schema.fields.take(1)))))
    val outcome = real.copy(contract = narrowed)

    val (_, lenient) = {
      val sink = new TestNotificationSink
      val r = reporterFor(sink, options = VerificationOptions(), infer = (_, _) => Some(outcome))
      r.check(spark.range(1).queryExecution.analyzed)
      (sink, inferenceEvents(sink).head)
    }
    assert(lenient.selfCheck.contains("PASSED"))

    val strictSink = new TestNotificationSink
    val strict = reporterFor(
      strictSink,
      options = VerificationOptions(rejectUndeclaredFields = true),
      infer = (_, _) => Some(outcome)
    )
    strict.check(spark.range(1).queryExecution.analyzed)
    val strictEvent = inferenceEvents(strictSink).head
    assert(strictEvent.selfCheck.contains("FAILED"))
    assert(strictEvent.selfCheckViolations.exists(_.violationType == ViolationType.UndeclaredOutputColumn))
  }

  test("the merged contract's id is the job id, else the app name, made safe; else the placeholder") {
    val real = realInferred()
    def mergedIdFor(jobInfo: JobInfo): String = {
      val sink = new TestNotificationSink
      val reporter = reporterFor(sink, jobInfo, infer = (_, _) => Some(real))
      reporter.check(spark.range(1).queryExecution.analyzed)
      reporter.finish()
      ContractParser.parse(summaryOf(sink).mergedContractYaml.get).id
    }
    assert(mergedIdFor(job) == "nightly_orders")
    assert(mergedIdFor(job.copy(jobId = None)) == "orders_app")
    assert(mergedIdFor(job.copy(jobId = None, appName = None)) == ContractInference.InferredId)
    assert(mergedIdFor(job.copy(jobId = Some("9 lives/x"))) == "job_9_lives_x")
    assert(mergedIdFor(job.copy(jobId = Some(""))) == ContractInference.InferredId)
  }

  test("jobInfoOf describes the session, its stable job id, and only the metadata-prefixed conf") {
    spark.conf.set(DryRunReporter.JobIdConfKey, "  stable_job  ")
    spark.conf.set(DryRunReporter.JobMetadataConfPrefix + "team", "data-eng")
    spark.conf.set(DryRunReporter.JobMetadataConfPrefix + "dag", "orders_nightly")
    spark.conf.set(DryRunReporter.JobMetadataConfPrefix, "no-key-after-the-prefix")
    spark.conf.set("spark.invaract.secret", "do-not-leak")
    try {
      val info = DryRunReporter.jobInfoOf(spark)
      val sc = spark.sparkContext
      assert(info.applicationId.contains(sc.applicationId))
      assert(info.appName.contains("DryRunReporterSpec"))
      assert(info.jobId.contains("stable_job"))
      assert(info.sparkVersion.contains(sc.version))
      assert(info.master.contains(sc.master))
      assert(info.deployMode.contains(sc.deployMode))
      assert(info.user.contains(sc.sparkUser))
      assert(info.startTimeMs.contains(sc.startTime))
      assert(info.attributes == Map("team" -> "data-eng", "dag" -> "orders_nightly"))
    } finally {
      spark.conf.unset(DryRunReporter.JobIdConfKey)
      spark.conf.unset(DryRunReporter.JobMetadataConfPrefix + "team")
      spark.conf.unset(DryRunReporter.JobMetadataConfPrefix + "dag")
      spark.conf.unset(DryRunReporter.JobMetadataConfPrefix)
      spark.conf.unset("spark.invaract.secret")
    }
  }

  test("a blank job id is no job id") {
    spark.conf.set(DryRunReporter.JobIdConfKey, "   ")
    try assert(DryRunReporter.jobInfoOf(spark).jobId.isEmpty)
    finally spark.conf.unset(DryRunReporter.JobIdConfKey)
  }
}
