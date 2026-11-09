// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema, TimePartitioning}
import com.invaract.contract.{ContractParser, PartitionOverwriteModes}
import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType, WritePartitioning}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.io.{FileIO, TextIO}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.{Contextful, Create, Requirements, SerializableFunction}
import org.apache.beam.sdk.values.Row
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** `check.partitioning` on Beam (docs/BEAM_ADAPTER.md, D23): what each write states of its layout, `HivePartitions` (the one
  * partitioned file write the adapter can read), and the refusals when a contract asks for what a write does not state. */
class BeamPartitioningSpec extends AnyFunSuite {

  private def pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private val avro = SchemaBuilder.record("Sale").fields().requiredLong("id").optionalString("region").requiredLong("day").endRecord()

  private def sale(id: Long, region: String, day: Long): GenericRecord = {
    val r = new GenericData.Record(avro)
    r.put("id", id); r.put("region", region); r.put("day", day)
    r
  }

  private def sales(p: Pipeline) = p.apply("Sales", Create.of(sale(1, "north", 4), sale(2, null, 4), sale(3, "a/b=c", 5)).withCoder(AvroGenericCoder.of(avro)))

  private def partitionedBy(keys: String*) =
    FileIO.writeDynamic[String, GenericRecord]().by(HivePartitions.by[GenericRecord](keys: _*)).withNaming(HivePartitions.naming(".parquet"))
      .withDestinationCoder(StringUtf8Coder.of()).via(ParquetIO.sink(avro))

  private def partitioningOf(p: Pipeline): Option[WritePartitioning] = BeamTranslator.translate(p).writes.loneElement.partitioning

  private implicit class Lone[A](items: List[A]) {
    def loneElement: A = { assert(items.size == 1, items); items.head }
  }

  // --- HivePartitions ---

  test("HivePartitions lays out folders as Spark does: key=value in order, escaped, a missing value the default partition") {
    val root = Files.createTempDirectory("invaract-hive-partitions")
    val p = pipeline
    sales(p).apply("Write", partitionedBy("day", "region").to(root.toString).withNumShards(1))
    p.run().waitUntilFinish()
    def folders(dir: Path): List[String] =
      Files.walk(dir).iterator().asScala.filter(f => Files.isRegularFile(f) && f.toString.endsWith(".parquet")).map(f => root.relativize(f.getParent).toString.replace('\\', '/')).toList.sorted
    assert(folders(root) == List("day=4/region=__HIVE_DEFAULT_PARTITION__", "day=4/region=north", "day=5/region=a%2Fb%3Dc"))
    assert(Files.list(root.resolve("day=4/region=north")).iterator().asScala.forall(_.getFileName.toString.startsWith("part-")))
  }

  test("a partition value is escaped character by character as Spark escapes it, and an empty one is the default partition") {
    assert(HivePartitions.folderValue("plain-value_1.2 x") == "plain-value_1.2 x")
    assert(HivePartitions.folderValue("\"#%'*/:=?\\{[]^") == "%22%23%25%27%2A%2F%3A%3D%3F%5C%7B%5B%5D%5E")
    assert(HivePartitions.folderValue("tab\there\u007F") == "tab%09here%7F")
    assert(HivePartitions.folderValue(" ") == " ", "a space is kept, the first printable character")
    assert(HivePartitions.folderValue("") == HivePartitions.DefaultPartition)
    assert(HivePartitions.folderValue(null) == HivePartitions.DefaultPartition)
    assert(HivePartitions.folderValue(42L) == "42")
  }

  test("a partition key reads a Beam row's field by name, and HivePartitions needs at least one key") {
    val schema = Schema.builder().addStringField("region").addInt64Field("day").build()
    val key = HivePartitions.by[Row]("region", "day").getClosure
    assert(key.apply(Row.withSchema(schema).addValues("south", java.lang.Long.valueOf(3)).build(), null) == "region=south/day=3")
    intercept[IllegalArgumentException](HivePartitions.by[Row]())
  }

  // --- what each write states ---

  test("a write to one folder is unpartitioned: TextIO, CsvIO's text write, FileIO.write() and Parquet through it") {
    val text = pipeline
    sales(text).apply("Lines", org.apache.beam.sdk.transforms.MapElements.into(org.apache.beam.sdk.values.TypeDescriptors.strings()).via((r: GenericRecord) => r.toString))
      .apply("Write", TextIO.write().to("/data/out/lines"))
    assert(partitioningOf(text).contains(WritePartitioning(Nil, PartitionOverwriteModes.Static)))
    val files = pipeline
    sales(files).apply("Write", FileIO.write[GenericRecord]().via(ParquetIO.sink(avro)).to("/data/out/sales"))
    assert(partitioningOf(files).contains(WritePartitioning(Nil, PartitionOverwriteModes.Static)))
  }

  test("a dynamic FileIO write is partitioned by HivePartitions' keys, and by nothing the adapter can say otherwise") {
    val hive = pipeline
    sales(hive).apply("Write", partitionedBy("day", "region").to("/data/out/sales"))
    assert(partitioningOf(hive).contains(WritePartitioning(List("day", "region"), PartitionOverwriteModes.Static)))
    // the team's own destination function: what it does with an element is its own code
    val own = pipeline
    sales(own).apply("Write", FileIO.writeDynamic[String, GenericRecord]()
      .by(new SerializableFunction[GenericRecord, String] { def apply(r: GenericRecord): String = s"region=${r.get("region")}" })
      .withNaming(HivePartitions.naming(".parquet")).withDestinationCoder(StringUtf8Coder.of()).via(ParquetIO.sink(avro)).to("/data/out/sales"))
    assert(partitioningOf(own).isEmpty)
    // HivePartitions' keys with another naming: the folders are not key=value
    val renamed = pipeline
    sales(renamed).apply("Write", FileIO.writeDynamic[String, GenericRecord]().by(HivePartitions.by[GenericRecord]("region"))
      .withNaming(Contextful.of(new Contextful.Fn[String, FileIO.Write.FileNaming] {
        def apply(key: String, c: Contextful.Fn.Context): FileIO.Write.FileNaming = FileIO.Write.defaultNaming(key, ".parquet")
      }, Requirements.empty()))
      .withDestinationCoder(StringUtf8Coder.of()).via(ParquetIO.sink(avro)).to("/data/out/sales"))
    assert(partitioningOf(renamed).isEmpty)
  }

  private val tableSchema = new TableSchema().setFields(List(new TableFieldSchema().setName("day").setType("DATE")).asJava)
  private def bigQuery(p: Pipeline, partitioning: Option[TimePartitioning]) = {
    val write = BigQueryIO.writeTableRows().to("proj:ds.sales").withSchema(tableSchema)
    p.apply("Rows", Create.of[TableRow](new TableRow()).withCoder(org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder.of()))
      .apply("Write", partitioning.fold(write)(write.withTimePartitioning))
  }

  test("a BigQuery write is partitioned by the column its time partitioning names; one that names none states nothing") {
    val byDay = pipeline
    bigQuery(byDay, Some(new TimePartitioning().setType("DAY").setField("day")))
    assert(partitioningOf(byDay).contains(WritePartitioning(List("day"), PartitionOverwriteModes.Static)))
    val ingestion = pipeline
    bigQuery(ingestion, Some(new TimePartitioning().setType("DAY")))
    assert(partitioningOf(ingestion).isEmpty, "by ingestion time: a pseudo-column no contract names")
    val unstated = pipeline
    bigQuery(unstated, None)
    assert(partitioningOf(unstated).isEmpty, "the table may be partitioned already")
  }

  // --- what a contract is held to ---

  private def contract(keys: List[String], format: String, location: String, extras: String = "") = ContractParser.parse(
    s"""id: partitioned
       |version: "1.0.0"
       |outputs:
       |  - name: sales
       |    location: $location
       |    format: $format
       |$extras
       |    schema:
       |      fields:
       |        - { name: id, type: long, required: true }
       |        - { name: region, type: string${if (keys.contains("region")) s", partitioned: true, partitionKeyPosition: ${keys.indexOf("region") + 1}" else ""} }
       |        - { name: day, type: long, required: true${if (keys.contains("day")) s", partitioned: true, partitionKeyPosition: ${keys.indexOf("day") + 1}" else ""} }
       |""".stripMargin)

  private def refused(c: com.invaract.contract.Contract, p: Pipeline) =
    intercept[ContractViolationException](InvaractBeam.enforce(c).verify(p)).result.violations.map(v => v.violationType -> v.message)

  test("a HivePartitions write passes by the declared keys and is refused, naming both, by others; one folder is no partitions") {
    def hive(keys: String*) = { val p = pipeline; sales(p).apply("Write", partitionedBy(keys: _*).to("/data/out/sales")); p }
    InvaractBeam.enforce(contract(List("day", "region"), "parquet", "/data/out/sales")).verify(hive("day", "region"))
    assert(refused(contract(List("day", "region"), "parquet", "/data/out/sales"), hive("region", "day")) ==
      List(ViolationType.OutputPartitioningMismatch -> "contract declares the output partitioned by day, region but the write partitions by region, day"))
    val oneFolder = pipeline
    sales(oneFolder).apply("Write", FileIO.write[GenericRecord]().via(ParquetIO.sink(avro)).to("/data/out/sales"))
    assert(refused(contract(List("region"), "parquet", "/data/out/sales"), oneFolder).map(_._1) == List(ViolationType.OutputPartitioningMismatch))
  }

  test("refusal: declared keys a write does not state, and a declared overwrite mode a write with no save mode cannot say") {
    val own = pipeline
    sales(own).apply("Write", FileIO.writeDynamic[String, GenericRecord]()
      .by(new SerializableFunction[GenericRecord, String] { def apply(r: GenericRecord): String = s"region=${r.get("region")}" })
      .withNaming(HivePartitions.naming(".parquet")).withDestinationCoder(StringUtf8Coder.of()).via(ParquetIO.sink(avro)).to("/data/out/sales"))
    val List((kind, message)) = refused(contract(List("region"), "parquet", "/data/out/sales"), own)
    assert(kind == ViolationType.UnverifiableWrite && message.contains("whose layout the pipeline does not state") && message.contains("partitioned by region"))
    // the same write passes a contract that declares no keys: there is nothing to hold its layout to
    InvaractBeam.enforce(contract(Nil, "parquet", "/data/out/sales")).verify(own)

    def hive = { val p = pipeline; sales(p).apply("Write", partitionedBy("region").to("/data/out/sales")); p }
    val List((overwrite, why)) = refused(contract(List("region"), "parquet", "/data/out/sales", "    partitionOverwriteMode: dynamic"), hive)
    assert(overwrite == ViolationType.UnverifiableWrite && why.contains("states no save mode") && why.contains("partitionOverwriteMode 'dynamic'"))
    // a BigQuery write states its save mode, so its overwrite mode is checked rather than refused
    val truncate = pipeline
    truncate.apply("Rows", Create.of[TableRow](new TableRow()).withCoder(org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder.of()))
      .apply("Write", BigQueryIO.writeTableRows().to("proj:ds.sales").withSchema(tableSchema)
        .withTimePartitioning(new TimePartitioning().setType("DAY").setField("day"))
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))
    val bigQueryContract = contract(List("day"), "bigquery", "proj/ds/sales", "    partitionOverwriteMode: dynamic")
    assert(BeamCheck.unstatedPartitioningOf(bigQueryContract, BeamTranslator.translate(truncate).writes.loneElement).isEmpty)
  }

  test("a dry run's draft declares the keys a HivePartitions write uses") {
    val p = pipeline
    sales(p).apply("Write", partitionedBy("day").to("/data/out/sales"))
    val capture = new com.invaract.verification.notification.NotificationSink {
      val events = scala.collection.mutable.ListBuffer.empty[com.invaract.verification.notification.NotificationEvent]
      override def publish(event: com.invaract.verification.notification.NotificationEvent): Unit = events += event
    }
    new BeamCheck(None, VerificationOptions(), Some(capture), InvaractBeam.jobInfoOf(PipelineOptionsFactory.create()), BeamCapabilities.declared).verify(p)
    val yaml = capture.events.collect { case e: com.invaract.verification.notification.ContractInferenceEvent => e }.loneElementOf.contractYaml.get
    val draft = ContractParser.parse(yaml)
    assert(draft.outputs.head.partitionKeys == List("day"))
  }

  private implicit class LoneSeq[A](items: Seq[A]) {
    def loneElementOf: A = { assert(items.size == 1, items); items.head }
  }
}
