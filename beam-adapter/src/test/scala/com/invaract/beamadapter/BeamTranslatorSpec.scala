// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{DatasetRef, Plan, Read, UnknownPlan, Write}
import com.invaract.testkit.{ColumnSource, JoinOn, OutColumn, ScenarioInput, ScenarioJob, ScenarioOutput}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.ByteArrayCoder
import org.apache.beam.sdk.extensions.avro.io.AvroIO
import org.apache.beam.sdk.io.{FileIO, GenerateSequence, TFRecordIO, TextIO}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Create, MapElements, PTransform, SerializableFunction}
import org.apache.beam.sdk.values.{PCollection, PDone, TypeDescriptor, TypeDescriptors}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** What the translator recognises in a pipeline graph, and what it refuses to guess. Pipelines are only constructed, never run:
  * the graph exists before anything executes, which is the point. */
class BeamTranslatorSpec extends AnyFunSuite {
  import ExampleJob.OrdersSchema

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private def parquetRead(p: Pipeline, pattern: String = "/data/in/orders/*.parquet"): PCollection[GenericRecord] =
    p.apply("ReadOrders", ParquetIO.read(OrdersSchema).from(pattern))

  private def parquetWrite(records: PCollection[GenericRecord], dir: String = "/data/out/report", name: String = "WriteReport"): Unit =
    records.apply(name, FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to(dir).withSuffix(".parquet"))

  private def readsIn(plan: Plan): List[String] = plan match {
    case Read(DatasetRef(location), _, _) => List(location)
    case other                               => other.children.flatMap(readsIn)
  }

  private def unknowns(plan: Plan): List[UnknownPlan] = plan match {
    case u: UnknownPlan => u :: u.children.flatMap(unknowns)
    case other          => other.children.flatMap(unknowns)
  }

  // --- reads ---

  test("the folder a read pattern reads from is what a contract declares") {
    assert(BeamTranslator.readLocation("in/orders/*.parquet") == "in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders/*.parquet") == "/data/in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders/part-?.txt") == "/data/in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders/[ab].txt") == "/data/in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders/{a,b}.txt") == "/data/in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders/") == "/data/in/orders")
    assert(BeamTranslator.readLocation("/data/in/orders") == "/data/in/orders", "no glob in the last segment: the path is the location")
    assert(BeamTranslator.readLocation("C:\\data\\in\\orders\\*.avro") == "C:/data/in/orders")
    assert(BeamTranslator.readLocation("*.parquet") == "", "a bare glob names no folder")
    assert(BeamTranslator.readLocation("/data/*/orders") == "/data/*/orders", "a glob in a folder, not the file name, is left as written")
  }

  test("Parquet, text and Avro reads are recognised by the folder they read, with the format of the IO transform") {
    val p = pipeline
    val parquet = parquetRead(p)
    parquetWrite(parquet)
    p.apply("ReadLines", TextIO.read().from("/data/in/lines/*.txt")).apply("Print", MapElements.into(TypeDescriptors.strings()).via(new Identity[String]))
      .apply("WriteLines", TextIO.write().to("/data/out/lines"))
    p.apply("ReadAvro", AvroIO.readGenericRecords(OrdersSchema).from("/data/in/avro/*.avro")).apply("WriteAvro", AvroIO.writeGenericRecords(OrdersSchema).to("/data/out/avro/part"))
    val translation = BeamTranslator.translate(p)
    assert(translation.unverifiable.isEmpty)
    // Beam resolves a text write's prefix against the file system, so on Windows it comes back as "D:/data/out/lines"
    val byLocation = translation.writes.map(w => withoutDrive(w.location) -> w).toMap
    assert(readsIn(byLocation("/data/out/report").plan) == List("/data/in/orders"))
    assert(readsIn(byLocation("/data/out/lines").plan) == List("/data/in/lines"))
    assert(readsIn(byLocation("/data/out/avro/part").plan) == List("/data/in/avro"), "an Avro write is located by the prefix the job passed to .to()")
  }

  /** A location without the drive letter Beam adds to a text write's prefix on Windows (`D:/data/x` is `/data/x`). */
  private def withoutDrive(location: String): String = location.replaceFirst("^[A-Za-z]:(?=/)", "")

  // --- writes ---

  test("a FileIO write is read for its output directory and its sink, which Beam does not publish") {
    val p = pipeline
    parquetWrite(parquetRead(p))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "/data/out/report")
    assert(w.format.contains("parquet"))
    assert(w.plan == Write(DatasetRef("/data/out/report"), Read(DatasetRef("/data/in/orders")), Some("parquet")))
    assert(w.outputSchema.exists(_.fields.map(_.name) == List("id", "amount")))
    assert(w.inputSchemas.map(_._1) == List("/data/in/orders"))
  }

  test("a separate temporary directory is not mistaken for the destination") {
    val p = pipeline
    parquetRead(p).apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to("/data/out/report").withTempDirectory("/scratch/tmp"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "/data/out/report")
  }

  test("a FileIO write through the text sink is a text write, with the columns of the data it formats") {
    val p = pipeline
    parquetRead(p).apply(
      "WriteCsv",
      FileIO.write[GenericRecord]().via(org.apache.beam.sdk.transforms.Contextful.fn[GenericRecord, String](new CsvLine), TextIO.sink()).to("/data/out/csv")
    )
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.format.contains("text") && w.location == "/data/out/csv")
    assert(w.outputSchema.nonEmpty, "the collection feeding the writer has columns even though the lines do not")
  }

  test("a plain TextIO write of strings is recognised, and has no columns to check") {
    val p = pipeline
    p.apply("Lines", Create.of("a", "b")).apply("WriteLines", TextIO.write().to("/data/out/lines/part"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(withoutDrive(w.location) == "/data/out/lines/part" && w.format.contains("text"))
    assert(w.outputSchema.isEmpty)
  }

  test("a FileIO write with a sink it cannot name has no format but is still checked where it lands") {
    val p = pipeline
    parquetRead(p).apply("WriteOther", FileIO.write[GenericRecord]().via(new OtherSink).to("/data/out/other"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.format.isEmpty && w.location == "/data/out/other")
  }

  test("a file write through a sink this adapter does not recognise is refused as unverifiable, not guessed") {
    val p = pipeline
    p.apply("Bytes", Create.of(Array[Byte](1, 2)).withCoder(ByteArrayCoder.of())).apply("WriteRecords", TFRecordIO.write().to("/data/out/tf"))
    val translation = BeamTranslator.translate(p)
    assert(translation.writes.isEmpty)
    assert(translation.unverifiable.map(_.operation).exists(m => m.contains("TFRecord") || m.contains("does not recognise")))
  }

  test("several writes are each translated, and a team's own composite around a write is seen through") {
    val p = pipeline
    val records = parquetRead(p)
    parquetWrite(records, "/data/out/first", "WriteFirst")
    records.apply("Mine", new WrapsAWrite("/data/out/second"))
    val translation = BeamTranslator.translate(p)
    assert(translation.unverifiable.isEmpty)
    assert(translation.writes.map(_.location).sorted == List("/data/out/first", "/data/out/second"))
  }

  test("a FileIO write through the Avro sink is an Avro write") {
    val p = pipeline
    val avroSink: FileIO.Sink[GenericRecord] = AvroIO.sink[GenericRecord](OrdersSchema)
    parquetRead(p).apply("WriteAvro", FileIO.write[GenericRecord]().via(avroSink).to("/data/out/avro"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.format.contains("avro") && w.location == "/data/out/avro")
  }

  test("a team's composite around a read is seen into, so the read beneath it still counts") {
    val p = pipeline
    val records = p.apply("LoadOrders", new WrapsARead("/data/in/orders/*.parquet"))
    parquetWrite(records)
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan) == List("/data/in/orders"))
    assert(w.inputSchemas.map(_._1) == List("/data/in/orders"))
  }

  test("a composite that holds the whole job, read and write, is searched deeper rather than taken for the write") {
    val p = pipeline
    p.apply("TheWholeJob", new WholeJob("/data/in/orders/*.parquet", "/data/out/report"))
    val translation = BeamTranslator.translate(p)
    assert(translation.unverifiable.isEmpty)
    val Seq(w) = translation.writes
    assert(w.location == "/data/out/report")
    assert(readsIn(w.plan) == List("/data/in/orders"), "the read inside the composite is not lost")
  }

  test("a step with several outputs is not the end of the pipeline when any of them is used") {
    val p = pipeline
    val main = new org.apache.beam.sdk.values.TupleTag[GenericRecord]("main")
    val side = new org.apache.beam.sdk.values.TupleTag[GenericRecord]("side")
    val records = parquetRead(p)
    val split = records.apply("Split", org.apache.beam.sdk.transforms.ParDo.of(new SplitFn).withOutputTags(main, org.apache.beam.sdk.values.TupleTagList.of(side)))
    val used = split.get(main).setCoder(records.getCoder)
    split.get(side).setCoder(records.getCoder)
    parquetWrite(used)
    assert(BeamTranslator.translate(p).unverifiable.isEmpty, "the unused side output does not make the step a dead end")
  }

  // --- everything else ---

  test("a step it does not recognise keeps the plan of what feeds it, so a read beneath it stays visible") {
    val p = pipeline
    val opaque = parquetRead(p).apply("Opaque", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new Identity[GenericRecord])).setCoder(parquetRead(pipeline).getCoder)
    parquetWrite(opaque)
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan) == List("/data/in/orders"))
    assert(unknowns(w.plan).nonEmpty)
    assert(w.inputSchemas.map(_._1) == List("/data/in/orders"))
  }

  test("a join keeps both reads, and reports no key it cannot see") {
    val p = pipeline
    val orders = ScenarioInput("in/orders", LogicalSchema(List(LogicalField("id", LogicalType.LongType), LogicalField("amount", LogicalType.LongType))))
    val customers = ScenarioInput("in/customers", LogicalSchema(List(LogicalField("id", LogicalType.LongType))))
    val job = ScenarioJob(List(orders, customers), List(OutColumn("id", ColumnSource.FromInput(0, "id"))), ScenarioOutput("out/joined"), join = Some(JoinOn("id", "id")))
    val root = Files.createTempDirectory("beam-translator-")
    try BeamScenarioJobs.build(p, job, root)
    finally BeamConformanceAdapter.deleteQuietly(root)
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan).map(_.split('/').last).sorted == List("customers", "orders"))
    assert(w.inputSchemas.size == 2)
  }

  test("a step that ends the pipeline without being a recognised write is refused as unverifiable") {
    val p = pipeline
    p.apply("Counter", GenerateSequence.from(0).to(10)).apply("PushToSomeService", org.apache.beam.sdk.transforms.ParDo.of(new BeamScenarioJobs.SideEffectLong))
    val translation = BeamTranslator.translate(p)
    assert(translation.writes.isEmpty)
    assert(translation.unverifiable.size == 1)
    assert(translation.unverifiable.head.operation.contains("PushToSomeService"))
  }

  test("a step whose output is used is not an end of the pipeline") {
    val p = pipeline
    val records = parquetRead(p)
    records.apply("Opaque", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new Identity[GenericRecord])).setCoder(records.getCoder)
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to("/data/out/report"))
    assert(BeamTranslator.translate(p).unverifiable.isEmpty)
  }

  test("a pipeline with no write at all translates to nothing") {
    val p = pipeline
    parquetRead(p)
    val translation = BeamTranslator.translate(p)
    assert(translation.writes.isEmpty && translation.unverifiable.isEmpty)
  }
}

private final class Identity[T] extends SerializableFunction[T, T] {
  override def apply(t: T): T = t
}

private final class CsvLine extends SerializableFunction[GenericRecord, String] {
  override def apply(r: GenericRecord): String = r.toString
}

/** A sink whose class says nothing about its format. */
private final class OtherSink extends FileIO.Sink[GenericRecord] {
  override def open(channel: java.nio.channels.WritableByteChannel): Unit = ()
  override def write(element: GenericRecord): Unit = ()
  override def flush(): Unit = ()
}

/** A team's own composite that writes: the write is inside it. */
private final class WrapsAWrite(dir: String) extends PTransform[PCollection[GenericRecord], PDone] {
  override def expand(input: PCollection[GenericRecord]): PDone = {
    input.apply("InnerWrite", FileIO.write[GenericRecord]().via(ParquetIO.sink(ExampleJob.OrdersSchema)).to(dir))
    PDone.in(input.getPipeline)
  }
}

private final class WrapsARead(pattern: String) extends PTransform[org.apache.beam.sdk.values.PBegin, PCollection[GenericRecord]] {
  override def expand(input: org.apache.beam.sdk.values.PBegin): PCollection[GenericRecord] =
    input.apply("InnerRead", ParquetIO.read(ExampleJob.OrdersSchema).from(pattern))
}

private final class WholeJob(pattern: String, dir: String) extends PTransform[org.apache.beam.sdk.values.PBegin, PDone] {
  override def expand(input: org.apache.beam.sdk.values.PBegin): PDone = {
    input.apply("InnerRead", ParquetIO.read(ExampleJob.OrdersSchema).from(pattern))
      .apply("InnerWrite", FileIO.write[GenericRecord]().via(ParquetIO.sink(ExampleJob.OrdersSchema)).to(dir))
    PDone.in(input.getPipeline)
  }
}

private final class SplitFn extends org.apache.beam.sdk.transforms.DoFn[GenericRecord, GenericRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(@org.apache.beam.sdk.transforms.DoFn.Element r: GenericRecord, out: org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver): Unit = ()
}
