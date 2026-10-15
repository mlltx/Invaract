// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{ConfigSource, ContractViolationException, InvaractConf, ViolationType}
import com.invaract.verification.notification.{ContractInferenceEvent, ContractValidationEvent, InferenceStatus, NotificationEvent, NotificationSink}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}
import org.apache.beam.sdk.io.{FileIO, GenerateSequence, TextIO}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Create, ParDo}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

/** The check itself: what it does with a pipeline whose writes it can and cannot verify, in enforcement and in dry-run mode. */
class BeamCheckSpec extends AnyFunSuite {
  import ExampleJob.OrdersSchema

  private final class Capture extends NotificationSink {
    val events = ListBuffer.empty[NotificationEvent]
    override def publish(event: NotificationEvent): Unit = events += event
  }

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private def ordersToParquet(p: Pipeline, dir: String = "/data/out/report"): Unit =
    p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to(dir))

  private def stringsToText(p: Pipeline): Unit =
    p.apply("Lines", Create.of("a", "b")).apply("WriteLines", TextIO.write().to("/data/out/lines"))

  // --- enforcement ---

  test("a write whose data exposes no schema is refused as unverifiable, naming the write, and the refusal is published") {
    val capture = new Capture
    val check = InvaractBeam.enforce(ExampleJob.contract("out/lines"), sink = Some(capture))
    val p = pipeline
    stringsToText(p)
    val e = intercept[ContractViolationException](check.verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.result.violations.head.message.contains("/data/out/lines"))
    assert(capture.events.collect { case v: ContractValidationEvent => v.status } == List("FAILED"))
  }

  test("a step that ends the pipeline without a recognised write is refused before any write is judged") {
    val check = InvaractBeam.enforce(ExampleJob.contract())
    val p = pipeline
    ordersToParquet(p)
    p.apply("Counter", GenerateSequence.from(0).to(5)).apply("PushToSomeService", ParDo.of(new BeamScenarioJobs.SideEffectLong))
    val e = intercept[ContractViolationException](check.verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("PushToSomeService"))
  }

  test("a pipeline with no write it recognises checks nothing and does not fail") {
    val capture = new Capture
    val check = InvaractBeam.enforce(ExampleJob.contract(), sink = Some(capture))
    val p = pipeline
    p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
    check.verify(p)
    assert(capture.events.isEmpty)
  }

  test("a write that keeps to the contract passes and publishes a PASSED validation naming Beam") {
    val capture = new Capture
    val check = InvaractBeam.enforce(ExampleJob.contract(), sink = Some(capture))
    val p = pipeline
    ordersToParquet(p)
    check.verify(p)
    val validations = capture.events.collect { case v: ContractValidationEvent => v }
    assert(validations.map(_.status) == List("PASSED"))
    assert(validations.head.job.flatMap(_.engine).contains("beam"))
    assert(check.enforces)
  }

  // --- dry-run mode ---

  test("dry-run mode infers a draft contract from each write it can describe, and enforces nothing") {
    val capture = new Capture
    val options = PipelineOptionsFactory.fromArgs("--invaractDryRun=true").create()
    val check = new BeamCheck(None, com.invaract.verification.VerificationOptions(), Some(capture), InvaractBeam.jobInfoOf(options), BeamCapabilities.declared)
    val p = pipeline
    ordersToParquet(p, "/data/out/anywhere")
    check.verify(p)
    val Seq(event) = capture.events.collect { case e: ContractInferenceEvent => e }
    assert(!check.enforces)
    assert(event.status == InferenceStatus.Inferred)
    assert(event.writeLocation.contains("/data/out/anywhere"))
    assert(event.contractYaml.exists(y => y.contains("anywhere") && y.contains("orders") || y.contains("in/orders")))
    assert(event.job.engine.contains("beam"))
  }

  test("dry-run mode reports, rather than guesses at, a write with no schema and a step it cannot describe") {
    val capture = new Capture
    val check = new BeamCheck(None, com.invaract.verification.VerificationOptions(), Some(capture), InvaractBeam.jobInfoOf(PipelineOptionsFactory.create()), None)
    val p = pipeline
    stringsToText(p)
    p.apply("Counter", GenerateSequence.from(0).to(5)).apply("PushToSomeService", ParDo.of(new BeamScenarioJobs.SideEffectLong))
    check.verify(p)
    val statuses = capture.events.collect { case e: ContractInferenceEvent => e.status }
    assert(statuses.nonEmpty && statuses.forall(_ == InferenceStatus.SkippedUnrecognized))
    assert(capture.events.collect { case e: ContractInferenceEvent => e.reason.getOrElse("") }.exists(_.contains("exposes no schema")))
  }

  test("dry-run mode with no sink logs and does not fail") {
    val check = new BeamCheck(None, com.invaract.verification.VerificationOptions(), None, InvaractBeam.jobInfoOf(PipelineOptionsFactory.create()), None)
    val p = pipeline
    ordersToParquet(p)
    stringsToText(p)
    check.verify(p)
  }

  // --- the options as settings ---

  test("every neutral setting is read from its --invaract option, and a blank or absent option is no setting") {
    val options = PipelineOptionsFactory
      .fromArgs(
        "--invaractContract=c.yaml", "--invaractDryRun=true", "--invaractNotifyConfig=n.properties", "--invaractRegistryUrl=http://r",
        "--invaractRegistryClientClass=a.B", "--invaractLocationMap=l.properties", "--invaractOrgPolicy=p.yaml", "--invaractOrgPolicyOverlays=o.yaml",
        "--invaractRejectUndeclaredInputs=true", "--invaractRejectUndeclaredFields=false", "--invaractComputeFingerprint=true",
        "--invaractStaticDataQuality=true", "--invaractRoleConsistency=true", "--invaractDelegate=DirectRunner"
      )
      .create()
    val config = new BeamConfigSource(options.as(classOf[InvaractBeamOptions]))
    val expected = Map(
      InvaractConf.Contract -> "c.yaml", InvaractConf.DryRun -> "true", InvaractConf.NotifyConfig -> "n.properties",
      InvaractConf.RegistryUrl -> "http://r", InvaractConf.RegistryClientClass -> "a.B", InvaractConf.LocationMap -> "l.properties",
      InvaractConf.OrgPolicy -> "p.yaml", InvaractConf.OrgPolicyOverlays -> "o.yaml", InvaractConf.RejectUndeclaredInputs -> "true",
      InvaractConf.RejectUndeclaredFields -> "false", InvaractConf.ComputeFingerprint -> "true", InvaractConf.StaticDataQuality -> "true",
      InvaractConf.RoleConsistency -> "true"
    )
    expected.foreach { case (name, value) => assert(config.get(name).contains(value), name) }
    assert(config.get("nothing") .isEmpty)
    assert(config.describe(InvaractConf.LocationMap) == "--invaractLocationMap")
    assert(config.describe(InvaractConf.RejectUndeclaredInputs) == "--invaractRejectUndeclaredInputs")

    val blank = new BeamConfigSource(PipelineOptionsFactory.fromArgs("--invaractContract=  ").create().as(classOf[InvaractBeamOptions]))
    assert(blank.get(InvaractConf.Contract).isEmpty && blank.get(InvaractConf.DryRun).isEmpty)
  }

  test("the events name Beam, its version and the job") {
    val options = PipelineOptionsFactory.fromArgs("--jobName=nightly-orders").create()
    val job = InvaractBeam.jobInfoOf(options)
    assert(job.engine.contains("beam") && job.name.contains("nightly-orders") && job.runId.contains("nightly-orders"))
    assert(job.engineVersion.exists(_.nonEmpty))
    assert(job.engineDetails.contains("runner"))
    assert(job.startTimeMs.isDefined)
  }

  // --- the runner ---

  private final class CountingRunner extends PipelineRunner[PipelineResult] {
    var runs = 0
    override def run(p: Pipeline): PipelineResult = { runs += 1; null }
  }

  test("the runner hands a pipeline to its delegate only after it passes") {
    val delegate = new CountingRunner
    val runner = InvaractRunner.enforcing(InvaractBeam.enforce(ExampleJob.contract()), delegate)
    val violating = pipeline
    ordersToParquet(violating, "/data/out/elsewhere")
    intercept[ContractViolationException](runner.run(violating))
    assert(delegate.runs == 0, "a pipeline that breaks the contract is never submitted")
    val conforming = pipeline
    ordersToParquet(conforming)
    runner.run(conforming)
    assert(delegate.runs == 1)
  }

  test("the runner takes a delegate named by class, refusing a class that is not a runner without initialising it") {
    val root = java.nio.file.Files.createTempDirectory("beam-delegate-")
    try {
      ExampleJob.withOrders(root, (1L, 10L))
      val contract = root.resolve("contract.yaml")
      java.nio.file.Files.write(contract, ExampleJob.contractYaml().getBytes(java.nio.charset.StandardCharsets.UTF_8))
      val before = NotRegisteredRunner.runs
      new ExampleJob(root).main(Array("--runner=com.invaract.beamadapter.InvaractRunner", s"--invaractContract=$contract", "--invaractDelegate=com.invaract.beamadapter.NotRegisteredRunner"))
      assert(NotRegisteredRunner.runs == before + 1, "the pipeline went to the runner the platform named")

      val e = intercept[IllegalArgumentException](
        new ExampleJob(root).main(Array("--runner=com.invaract.beamadapter.InvaractRunner", s"--invaractContract=$contract", "--invaractDelegate=com.invaract.beamadapter.InitSpy"))
      )
      assert(e.getMessage.contains("not a Beam PipelineRunner"))
      assert(System.getProperty("invaract.initspy.initialised") == null, "a configured class name is checked before it is allowed to run any code")
    } finally BeamConformanceAdapter.deleteQuietly(root)
  }

  test("the runner is registered with Beam, so a command line names it --runner=Invaract, and it refuses itself as its delegate") {
    Seq("Invaract", "InvaractRunner", "invaract").foreach { name =>
      assert(PipelineOptionsFactory.fromArgs(s"--runner=$name").create().getRunner == classOf[InvaractRunner], name)
    }
    val root = java.nio.file.Files.createTempDirectory("beam-short-name-")
    try {
      ExampleJob.withOrders(root, (1L, 10L))
      val contract = root.resolve("contract.yaml")
      java.nio.file.Files.write(contract, ExampleJob.contractYaml().getBytes(java.nio.charset.StandardCharsets.UTF_8))
      val before = NotRegisteredRunner.runs
      new ExampleJob(root).main(Array("--runner=Invaract", s"--invaractContract=$contract", "--invaractDelegate=com.invaract.beamadapter.NotRegisteredRunner"))
      assert(NotRegisteredRunner.runs == before + 1)
      Seq("InvaractRunner", "com.invaract.beamadapter.InvaractRunner").foreach { itself =>
        val e = intercept[IllegalArgumentException](new ExampleJob(root).main(Array("--runner=Invaract", s"--invaractContract=$contract", s"--invaractDelegate=$itself")))
        assert(e.getMessage.contains("names the Invaract runner itself"), itself)
      }
    } finally BeamConformanceAdapter.deleteQuietly(root)
  }

  // --- a text write and the formats a contract can declare for one ---

  private def textWrite(p: Pipeline, dir: String): Unit =
    p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteLines", FileIO.write[GenericRecord]().via(org.apache.beam.sdk.transforms.Contextful.fn[GenericRecord, String](new BeamScenarioJobs.CsvFn), TextIO.sink()).to(dir))

  private def contractWithOutputFormat(format: Option[String]) =
    com.invaract.contract.ContractParser.parse(ExampleJob.contractYaml(outputLocation = "out/lines").replace(
      "    location: out/lines\n    format: parquet\n",
      "    location: out/lines\n" + format.map(f => s"    format: $f\n").getOrElse("")
    ))

  test("a text write satisfies a declared csv, json or text format, because the graph cannot tell them apart") {
    Seq("csv", "json", "text").foreach { declared =>
      val p = pipeline
      textWrite(p, "/data/out/lines")
      InvaractBeam.enforce(contractWithOutputFormat(Some(declared))).verify(p) // does not throw
    }
  }

  test("a text write does not satisfy any other declared format, and a contract that declares none sees no mismatch") {
    val p = pipeline
    textWrite(p, "/data/out/lines")
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contractWithOutputFormat(Some("parquet"))).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputFormatMismatch))

    val q = pipeline
    textWrite(q, "/data/out/lines")
    InvaractBeam.enforce(contractWithOutputFormat(None)).verify(q)
  }

  test("a write that is not a text write is reported in its own format, whatever the contract declares") {
    val p = pipeline
    ordersToParquet(p, "/data/out/report")
    val e = intercept[ContractViolationException](
      InvaractBeam.enforce(ExampleJob.contract().copy(outputs = ExampleJob.contract().outputs.map(_.copy(format = Some("csv"))))).verify(p)
    )
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputFormatMismatch))
  }

  test("a text write to the wrong place in a single-output contract is reported for its location only, not also for its format") {
    // the declared output is the one the write is checked against even when the location misses (OutputChecker.expectedOutputFor),
    // so the text-family rule must use the same declared output, or a csv contract would also see a format mismatch
    val p = pipeline
    textWrite(p, "/data/elsewhere/lines")
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contractWithOutputFormat(Some("csv"))).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
  }

  test("a write through a sink whose format cannot be told is refused when the contract declares a format, and checked when it declares none") {
    def otherSinkWrite(p: Pipeline): Unit =
      p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
        .apply("WriteOther", FileIO.write[GenericRecord]().via(new OtherSink).to("/data/out/lines"))
    val p = pipeline
    otherSinkWrite(p)
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contractWithOutputFormat(Some("parquet"))).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("whose file format cannot be told") && e.getMessage.contains("'parquet'"))

    val q = pipeline
    otherSinkWrite(q)
    InvaractBeam.enforce(contractWithOutputFormat(None)).verify(q) // nothing declared, nothing to tell apart
    val r = pipeline
    otherSinkWrite(r)
    InvaractBeam.enforce(contractWithOutputFormat(Some("  "))).verify(r) // a blank format declares nothing
  }

  // --- reads that cannot be located, save modes, BigQuery ---

  private val bigQueryContract = com.invaract.contract.ContractParser.parse(
    """id: orders_report
      |version: "1.0.0"
      |inputs:
      |  - name: orders
      |    location: in/orders
      |    schema:
      |      fields:
      |        - { name: id, type: long }
      |outputs:
      |  - name: report
      |    location: ds/report
      |    saveMode: append
      |    schema:
      |      fields:
      |        - { name: id, type: long, required: true }
      |""".stripMargin
  )

  private def queryToBigQuery(p: Pipeline, disposition: org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition): Unit = {
    val schema = new com.google.api.services.bigquery.model.TableSchema()
      .setFields(java.util.Arrays.asList(new com.google.api.services.bigquery.model.TableFieldSchema().setName("id").setType("INT64").setMode("REQUIRED")))
    p.apply("Query", org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.readTableRows().fromQuery("SELECT id FROM `proj.in.orders`").usingStandardSql())
      .apply("Write", org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.writeTableRows().to("proj:ds.report").withSchema(schema).withWriteDisposition(disposition))
  }

  test("a declared input the job may read through a read that cannot be located is reported unverifiable, not missing") {
    val capture = new Capture
    val p = pipeline
    queryToBigQuery(p, org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    InvaractBeam.enforce(bigQueryContract, sink = Some(capture)).verify(p) // does not throw: MISSING_INPUT would be a false alarm
    val Seq(passed) = capture.events.collect { case e: ContractValidationEvent => e }
    assert(passed.status == "PASSED")
    assert(passed.unverifiableInputs.map(i => (i.inputName, i.unknownNodeTypes)) == List(("orders", List(BeamTranslator.UnlocatedSourceType))))
  }

  test("strict about undeclared inputs, a write that depends on a read that cannot be located is refused") {
    val p = pipeline
    queryToBigQuery(p, org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    val strict = com.invaract.verification.VerificationOptions(rejectUndeclaredInputs = true)
    val e = intercept[ContractViolationException](InvaractBeam.enforce(bigQueryContract, strict).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("an undeclared input cannot be ruled out") && e.getMessage.contains("Query: a BigQuery query"))
  }

  test("a BigQuery write's save mode is checked against the declared one") {
    val p = pipeline
    queryToBigQuery(p, org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
    val e = intercept[ContractViolationException](InvaractBeam.enforce(bigQueryContract).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputSaveModeMismatch))
  }

  test("a file write, which states no save mode, is refused when the contract declares one, and checked when it declares none") {
    val withSaveMode = ExampleJob.contract().copy(outputs = ExampleJob.contract().outputs.map(_.copy(saveMode = Some("append"))))
    val p = pipeline
    ordersToParquet(p)
    val e = intercept[ContractViolationException](InvaractBeam.enforce(withSaveMode).verify(p))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("states no save mode") && e.getMessage.contains("'append'"))

    val blank = ExampleJob.contract().copy(outputs = ExampleJob.contract().outputs.map(_.copy(saveMode = Some(" "))))
    val q = pipeline
    ordersToParquet(q)
    InvaractBeam.enforce(blank).verify(q) // a blank save mode declares nothing
  }

  test("dry-run mode marks a draft that cannot list every input degraded, naming the read") {
    val capture = new Capture
    val p = pipeline
    queryToBigQuery(p, org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    new BeamCheck(None, com.invaract.verification.VerificationOptions(), Some(capture), InvaractBeam.jobInfoOf(PipelineOptionsFactory.create()), BeamCapabilities.declared)
      .verify(p)
    val Seq(event) = capture.events.collect { case e: ContractInferenceEvent => e }
    assert(event.status == InferenceStatus.InferredDegraded)
    assert(event.reason.exists(_.contains("Query: a BigQuery query")))
    assert(event.contractYaml.exists(y => y.contains("saveMode: append") && y.contains("location: proj/ds/report")))
  }

  test("column names are compared case-sensitively, as Avro and Beam name them") {
    val upper = new org.apache.avro.Schema.Parser().parse(
      """{"type":"record","name":"Orders","fields":[{"name":"ID","type":"long"},{"name":"amount","type":["null","long"],"default":null}]}"""
    )
    val p = pipeline
    p.apply("ReadOrders", ParquetIO.read(upper).from("/data/in/orders/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(upper)).to("/data/out/report"))
    val e = intercept[ContractViolationException](InvaractBeam.enforce(ExampleJob.contract()).verify(p))
    assert(e.result.violations.map(_.violationType).contains(ViolationType.MissingOutputField), "'ID' is not the declared 'id'")
  }
}
