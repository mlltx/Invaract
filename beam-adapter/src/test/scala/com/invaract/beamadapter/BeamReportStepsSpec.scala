// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.invaract.verification.{ConfigSource, ContractViolationException, InvaractConf, TrustedSteps, VerificationOptions, ViolationType}
import com.invaract.verification.notification.{ContractInferenceEvent, InferenceStatus, NotificationEvent, NotificationSink}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder, VoidCoder}
import org.apache.beam.sdk.io.{FileIO, GenerateSequence}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, TableRowJsonCoder}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Create, DoFn, MapElements, ParDo, SerializableFunction, View}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection, TypeDescriptors}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Steps that end a pipeline but only report on it (docs/BEAM_ADAPTER.md, D14): one that reads only what writes report is let
  * through, as is one the platform names as trusted; everything else that ends a pipeline without a recognised write is refused. */
class BeamReportStepsSpec extends AnyFunSuite {
  import ExampleJob.OrdersSchema

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private def ordersToParquet(p: Pipeline): org.apache.beam.sdk.io.WriteFilesResult[Void] =
    p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to("/data/out/report"))

  private def refusal(p: Pipeline, trusted: TrustedSteps = TrustedSteps.Nobody): Option[String] =
    try {
      new BeamCheck(Some(ExampleJob.contract()), VerificationOptions(), None, InvaractBeam.jobInfoOf(p.getOptions), BeamCapabilities.declared, trusted).verify(p)
      None
    } catch {
      case e: ContractViolationException =>
        assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
        Some(e.getMessage)
    }

  // --- a step that reads only what writes report ---

  test("a step that reads the files a write wrote, and nothing else, ends the pipeline without being refused") {
    val p = pipeline
    ordersToParquet(p).getPerDestinationOutputFilenames.apply("Notify", ParDo.of(new Notify[KV[Void, String]]))
    assert(refusal(p).isEmpty)
    assert(BeamTranslator.translate(p).reportSteps == List("Notify (ParDo$SingleOutput)"))
  }

  test("so does one fed by steps that themselves read only what writes report") {
    val p = pipeline
    ordersToParquet(p).getPerDestinationOutputFilenames
      .apply("Names", MapElements.into(TypeDescriptors.strings()).via(new FileName))
      .apply("Notify", ParDo.of(new Notify[String]))
    assert(refusal(p).isEmpty)
  }

  test("a step that also reads data is refused, whether the data reaches it directly or as a side input") {
    val p = pipeline
    val written = ordersToParquet(p)
    val orders = p.apply("ReadAgain", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
    val lookup = orders.apply("Lookup", View.asList[GenericRecord]())
    written.getPerDestinationOutputFilenames.apply("NotifyWithOrders", ParDo.of(new NotifyWith(lookup)).withSideInputs(lookup))
    assert(refusal(p).exists(_.contains("NotifyWithOrders")))
    assert(BeamTranslator.translate(p).reportSteps.isEmpty)
  }

  test("a write's result that holds rows (a BigQuery write's failed inserts) is data, so a step reading it is refused") {
    val p = pipeline
    val row = new TableRow()
    row.set("id", Integer.valueOf(1))
    val rows = p.apply("Rows", Create.of[TableRow](row, Seq.empty[TableRow]: _*).withCoder(TableRowJsonCoder.of()))
    val schema = new TableSchema().setFields(List(new TableFieldSchema().setName("id").setType("INT64")).asJava)
    val result = rows.apply("WriteTable", BigQueryIO.writeTableRows().to("proj:ds.report").withSchema(schema).withoutValidation())
    result.getFailedInserts.apply("LogFailures", ParDo.of(new Notify[TableRow]))
    result.getSuccessfulTableLoads.apply("AnnounceLoad", ParDo.of(new Notify[org.apache.beam.sdk.io.gcp.bigquery.TableDestination]))
    val translation = BeamTranslator.translate(p)
    assert(translation.reportSteps == List("AnnounceLoad (ParDo$SingleOutput)"), "the table it loaded is a name")
    assert(translation.unverifiable.flatMap(_.step) == List("LogFailures"))
  }

  test("what a write reports is told by what it holds: names and nothing, not rows") {
    def collection(coder: org.apache.beam.sdk.coders.Coder[_]) = BeamCollection("c", bounded = true, Some(coder), None)
    assert(BeamIO.isWriteReport(collection(KvCoder.of(VoidCoder.of(), StringUtf8Coder.of()))))
    assert(BeamIO.isWriteReport(collection(StringUtf8Coder.of())))
    assert(BeamIO.isWriteReport(collection(org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV2.of())))
    assert(!BeamIO.isWriteReport(collection(TableRowJsonCoder.of())))
    assert(!BeamIO.isWriteReport(collection(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()))), "every part of a pair must be a name")
    assert(!BeamIO.isWriteReport(collection(KvCoder.of(TableRowJsonCoder.of(), StringUtf8Coder.of()))))
    assert(!BeamIO.isWriteReport(BeamCollection("c", bounded = true, None, None)), "no coder: nothing shows it holds only names")
  }

  // --- trusted steps ---

  private def counterToService(p: Pipeline): Unit =
    p.apply("Counter", GenerateSequence.from(0).to(5)).apply("PushToSomeService", ParDo.of(new BeamScenarioJobs.SideEffectLong))

  test("a step the platform trusts is not refused for ending the pipeline; one it does not name still is") {
    val p = pipeline
    ordersToParquet(p)
    counterToService(p)
    assert(refusal(p).exists(_.contains("PushToSomeService")))
    assert(refusal(p, TrustedSteps(List("Push*"))).isEmpty)
    assert(refusal(p, TrustedSteps(List("Notify"))).exists(_.contains("PushToSomeService")))
  }

  test("trusting a write it cannot verify does not let it through: trust is about steps, not destinations") {
    val p = pipeline
    ordersToParquet(p)
    p.apply("ReadAgain", ParquetIO.read(OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteNowhere", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema))
        .withNaming(FileIO.Write.defaultNaming("/data/out/elsewhere/part", ".parquet")).withTempDirectory("/tmp/invaract-temp"))
    assert(refusal(p, TrustedSteps(List("*"))).isDefined)
  }

  test("a platform names trusted steps with --invaractTrustedSteps, with no change to the job") {
    val options = PipelineOptionsFactory.fromArgs("--invaractTrustedSteps=PushTo*, Other").as(classOf[InvaractBeamOptions])
    assert(new BeamConfigSource(options).get(InvaractConf.TrustedSteps).contains("PushTo*, Other"))
    val p = pipeline
    ordersToParquet(p)
    counterToService(p)
    InvaractBeam.enforce(ExampleJob.contract(), config = new BeamConfigSource(options)).verify(p)
    intercept[ContractViolationException](InvaractBeam.enforce(ExampleJob.contract(), config = ConfigSource.empty).verify(p))
  }

  test("a dry run does not report a trusted step as one it skipped") {
    val events = ListBuffer.empty[NotificationEvent]
    val sink = new NotificationSink { override def publish(event: NotificationEvent): Unit = events += event }
    val p = pipeline
    ordersToParquet(p)
    counterToService(p)
    def skipped(trusted: TrustedSteps): Int = {
      events.clear()
      new BeamCheck(None, VerificationOptions(), Some(sink), InvaractBeam.jobInfoOf(p.getOptions), BeamCapabilities.declared, trusted).verify(p)
      events.toList.collect { case e: ContractInferenceEvent if e.status == InferenceStatus.SkippedUnrecognized => e }.size
    }
    assert(skipped(TrustedSteps.Nobody) == 1)
    assert(skipped(TrustedSteps(List("PushToSomeService"))) == 0)
  }

  // --- the steps the tests are built from ---

  private final class Notify[T] extends DoFn[T, Void] {
    @ProcessElement def process(@DoFn.Element element: T): Unit = () // imagine a message to a channel here
  }

  private final class NotifyWith(lookup: org.apache.beam.sdk.values.PCollectionView[java.util.List[GenericRecord]]) extends DoFn[KV[Void, String], Void] {
    @ProcessElement def process(c: DoFn[KV[Void, String], Void]#ProcessContext): Unit = ()
  }

  private final class FileName extends SerializableFunction[KV[Void, String], String] {
    override def apply(kv: KV[Void, String]): String = kv.getValue
  }
}
