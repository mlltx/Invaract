// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Formats, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{CatalogIdentity, PlanPrinter}
import org.scalatest.funsuite.AnyFunSuite

/** The Python SDK's steps as the translator sees them (`PythonSteps`, docs/BEAM_PYTHON_ADAPTER.md, D3): first the catalogue's rules
  * on steps built here, then what the translator makes of real Python pipelines (the recordings `record_fixtures.py` makes). */
class PythonStepsSpec extends AnyFunSuite {
  import PortablePipelineSpec.fixture
  import PythonStepsSpec._

  private def translate(name: String): BeamTranslation = BeamTranslator.translate(fixture(name), PythonSteps)

  // --- the catalogue's rules ---

  test("a file source states its pattern; the read is the folder the pattern reads, in the source's format") {
    val parquet = step("R", "apache_beam.io.parquetio.ReadFromParquet", display = Map(Pattern -> "gs://b/in/orders/*.parquet"), namespaces = Map(Pattern -> ParquetSource))
    assert(PythonSteps.read(parquet) == Some(IoRead.Located(List("gs://b/in/orders"), Some(Formats.Parquet))))
    PythonSteps.FileSources.foreach { case (source, format) =>
      val read = step("R", IobaseRead, display = Map(Pattern -> "in/x/*", "source" -> source), namespaces = Map(Pattern -> source, "source" -> IobaseRead))
      assert(PythonSteps.read(read) == Some(IoRead.Located(List("in/x"), Some(format))), source)
    }
  }

  test("a pattern published by a class that is not a source is not a read (the SDF reader beneath a read repeats it)") {
    val reader = step("R/SDF", "apache_beam.io.iobase.SDFBoundedSourceReader", display = Map(Pattern -> "in/x/*"),
      namespaces = Map(Pattern -> "apache_beam.io.iobase.SDFBoundedSourceDoFn"), inputs = List(collection("c")))
    assert(PythonSteps.read(reader).isEmpty)
  }

  test("a file read whose pattern is a template parameter is a read that is not located, naming the parameter") {
    val read = step("R", IobaseRead, display = Map(Pattern -> "RuntimeValueProvider(option: input, type: str, default_value: None)"), namespaces = Map(Pattern -> TextSource))
    assert(PythonSteps.read(read) == Some(IoRead.Unlocated("a read whose source is a template parameter (input), known only when the job runs")))
  }

  test("a read of a file source whose pattern is not stated is not located") {
    val read = step("R", IobaseRead, display = Map("source" -> TextSource), namespaces = Map("source" -> IobaseRead))
    assert(PythonSteps.read(read).exists(r => r.isInstanceOf[IoRead.Unlocated] && r.toString.contains("file pattern is not stated")))
  }

  test("a read of a source the adapter does not know is not located, naming the source") {
    val read = step("R", IobaseRead, display = Map("source" -> "my.module.OwnSource"), namespaces = Map("source" -> IobaseRead))
    assert(PythonSteps.read(read) == Some(IoRead.Unlocated("a read from my.module.OwnSource, a source this adapter does not know")))
  }

  test("a `source` item published by anything but iobase.Read does not make a read") {
    val reader = step("R/SDF", "x", display = Map("source" -> "my.module.OwnSource"), namespaces = Map("source" -> "apache_beam.io.iobase.SDFBoundedSourceReader"),
      inputs = List(collection("c")))
    assert(PythonSteps.read(reader).isEmpty)
  }

  test("a BigQuery read is located by its table, in the bigquery catalog; a query or a read of no table is not located") {
    def bq(display: Map[String, String]) =
      PythonSteps.read(step("R", IobaseRead, display = display + ("source" -> "apache_beam.io.gcp.bigquery._CustomBigQuerySource"), namespaces = Map("source" -> IobaseRead)))
    val table = "<TableReference\n datasetId: 'ds'\n projectId: 'proj'\n tableId: 'orders'>"
    assert(bq(Map("table" -> table, "query" -> "None")) ==
      Some(IoRead.Located(List("proj/ds/orders"), Some(Formats.BigQuery), Some(CatalogIdentity(Some("bigquery"), namespace = List("proj", "ds"), table = Some("orders"))))))
    assert(bq(Map("table_reference" -> table)).exists(_.isInstanceOf[IoRead.Located]), "the direct read names it table_reference")
    assert(bq(Map("table" -> "None", "query" -> "SELECT 1")) == Some(IoRead.Unlocated("a BigQuery query, whose tables are named only in its SQL")))
    assert(bq(Map("table" -> "None", "query" -> "None")) == Some(IoRead.Unlocated("a BigQuery read whose table is not stated")))
    assert(bq(Map.empty) == Some(IoRead.Unlocated("a BigQuery read whose table is not stated")))
  }

  test("a Pub/Sub read is located by its topic, or its subscription") {
    def ps(display: Map[String, String]) =
      PythonSteps.read(step("R", IobaseRead, display = display + ("source" -> "apache_beam.io.gcp.pubsub._PubSubSource"), namespaces = Map("source" -> IobaseRead)))
    assert(ps(Map("topic" -> "projects/p/topics/t")) == Some(IoRead.Located(List("projects/p/topics/t"), Some("pubsub"))))
    assert(ps(Map("subscription" -> "projects/p/subscriptions/s")) == Some(IoRead.Located(List("projects/p/subscriptions/s"), Some("pubsub"))))
    assert(ps(Map("topic" -> "None")) == Some(IoRead.Unlocated("a Pub/Sub read whose topic or subscription is not stated")))
  }

  test("a step with no input that states no read beneath it is a read that is not located, never data the job made") {
    val pandas = step("ReadCsv", "apache_beam.dataframe.io.ReadViaPandas", children = List(step("ReadCsv/Make", "apache_beam.transforms.core.Create")))
    assert(PythonSteps.read(pandas) == Some(IoRead.Unlocated("ReadViaPandas, a step that brings data into the pipeline without stating where from")))
    assert(PythonSteps.read(step("Start", "apache_beam.transforms.core.Impulse")).exists(_.isInstanceOf[IoRead.Unlocated]))
  }

  test("data the job makes itself, and a step with an input, are not reads") {
    assert(PythonSteps.read(step("Make", "apache_beam.transforms.core.Create")).isEmpty)
    assert(PythonSteps.read(step("Map", "apache_beam.transforms.core.ParDo", inputs = List(collection("c")))).isEmpty)
  }

  test("one of the SDK's read transforms is the one read it holds; a team's own step, or one holding two reads, is looked into") {
    val inner = step("R/Read", IobaseRead, display = Map("source" -> "apache_beam.io.gcp.pubsub._PubSubSource", "topic" -> "projects/p/topics/t"), namespaces = Map("source" -> IobaseRead))
    val sdk = step("R", "apache_beam.io.gcp.pubsub.ReadFromPubSub", children = List(inner, step("R/Metrics", "x", inputs = List(collection("c")))))
    assert(PythonSteps.read(sdk) == PythonSteps.read(inner))
    val own = step("R", "my_pipeline.ReadAndClean", children = List(inner))
    assert(PythonSteps.read(own).isEmpty)
    val two = step("R", "apache_beam.io.Both", children = List(inner, inner.copy(name = "R/Read2")))
    assert(PythonSteps.read(two).isEmpty)
  }

  test("a file sink states its pattern; the write lands at the pattern's prefix, in the sink's format") {
    PythonSteps.FileSinks.foreach { case (sink, format) =>
      val write = step("W", "w", display = Map(Pattern -> "out/report-%(shard_num)05d-of-%(num_shards)05d.x"), namespaces = Map(Pattern -> sink), inputs = List(collection("c")))
      assert(PythonSteps.write(write) == Some(IoWrite.Located("out/report", Some(format))), sink)
    }
  }

  test("a Parquet or Avro write has the schema its files are written with; a text write has none of its own") {
    def written(sink: String, schema: String) =
      PythonSteps.write(step("W", "w", display = Map(Pattern -> "out/r", "schema" -> schema), namespaces = Map(Pattern -> sink))).collect { case l: IoWrite.Located => l.schema }.flatten
    val columns = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = false)))
    assert(written(ParquetSink, "id: int64 not null").contains(columns))
    assert(written("apache_beam.io.avroio._FastAvroSink", "{'type': 'record', 'name': 'R', 'fields': [{'name': 'id', 'type': 'long'}]}").contains(columns))
    assert(written(TextSink, "id: int64 not null").isEmpty)
    assert(written(ParquetSink, "None").isEmpty, "an unset schema")
  }

  test("a file write to a template parameter, or with no pattern, cannot be verified") {
    def w(display: Map[String, String]) = PythonSteps.write(step("W", "w", display = display, namespaces = Map(Pattern -> TextSink)))
    assert(w(Map(Pattern -> "RuntimeValueProvider(option: output, type: str, default_value: None)")) ==
      Some(IoWrite.Unverifiable("a write whose destination is a template parameter (output), known only when the job runs")))
    assert(w(Map.empty) == Some(IoWrite.Unverifiable("a text write whose file pattern is not stated")))
  }

  test("where a write lands is its pattern up to the shard template") {
    assert(PythonSteps.prefixOf("out/report-%(shard_num)05d-of-%(num_shards)05d.parquet") == "out/report")
    assert(PythonSteps.prefixOf("out/report_%(shard_num)d") == "out/report")
    assert(PythonSteps.prefixOf("out/report.%(shard_num)d") == "out/report")
    assert(PythonSteps.prefixOf("out/a-b-%(shard_num)d") == "out/a-b", "a dash inside the prefix is kept")
    assert(PythonSteps.prefixOf("out/report/-%(shard_num)d") == "out/report/")
    assert(PythonSteps.prefixOf("out/report.csv") == "out/report.csv", "no template: the single file itself")
  }

  test("a BigQuery write is located by its table, with the disposition and schema its parts state, the first one that is set") {
    val parts = List(
      step("W/A", "x", display = Map("write_disposition" -> "None", "schema" -> "None")),
      step("W/B", "x", display = Map("write_disposition" -> "WRITE_EMPTY", "schema" -> "{'fields': [{'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'}]}")),
      step("W/C", "x", display = Map("write_disposition" -> "WRITE_APPEND"))
    )
    val write = PythonSteps.write(step("W", BigQueryWrite, display = Map("table" -> "proj:ds.report"), children = parts))
    assert(write == Some(IoWrite.Located(
      "proj/ds/report", Some(Formats.BigQuery), Some("error"), Some(CatalogIdentity(Some("bigquery"), namespace = List("proj", "ds"), table = Some("report"))),
      Some(LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = false))))
    )))
  }

  test("a BigQuery write to a table the job computes, a template parameter or nothing cannot be verified") {
    def bq(table: Option[String]) = PythonSteps.write(step("W", BigQueryWrite, display = table.map("table" -> _).toMap))
    val refused = Some(IoWrite.Unverifiable("a BigQuery write whose table is computed by the job or not stated, so where it lands cannot be told"))
    assert(bq(Some("<function <lambda> at 0x7f>")) == refused)
    assert(bq(Some("RuntimeValueProvider(option: table, type: str, default_value: None)")) == refused)
    assert(bq(Some("None")) == refused)
    assert(bq(None) == refused)
  }

  test("a Pub/Sub write is located by its topic") {
    assert(PythonSteps.write(step("W", PubSubWrite, display = Map("topic" -> "projects/p/topics/out"))) == Some(IoWrite.Located("projects/p/topics/out", Some("pubsub"))))
    assert(PythonSteps.write(step("W", PubSubWrite)) == Some(IoWrite.Unverifiable("a Pub/Sub write whose topic is not stated")))
  }

  test("any other step is not a write") {
    assert(PythonSteps.write(step("M", "apache_beam.transforms.core.ParDo", display = Map(Pattern -> "out/x"), namespaces = Map(Pattern -> "apache_beam.io.Other"))).isEmpty)
  }

  test("a BigQuery table as the SDK prints it: a TableReference, with or without a project, or a table spec") {
    assert(PythonSteps.tableReference("<TableReference\n datasetId: 'ds'\n projectId: 'p'\n tableId: 't'>").map(_.location).contains("p/ds/t"))
    assert(PythonSteps.tableReference("<TableReference\n datasetId: 'ds'\n tableId: 't'>").map(_.location).contains("ds/t"))
    assert(PythonSteps.tableReference("<TableReference\n datasetId: 'ds'>").isEmpty, "no table")
    assert(PythonSteps.tableReference("<TableReference\n tableId: 't'>").isEmpty, "no dataset")
    assert(PythonSteps.tableReference("p:ds.t").map(_.location).contains("p/ds/t"))
    assert(PythonSteps.tableReference("<function Days.table at 0x7f>").isEmpty, "a function, though it has a dot")
    assert(PythonSteps.tableReference("p:ds.t x").isEmpty)
  }

  test("a template parameter is a runtime value provider, or one computed from one; any other value is the value itself") {
    assert(PythonSteps.runtimeParameter("RuntimeValueProvider(option: input, type: str, default_value: None)").contains("input"))
    assert(PythonSteps.runtimeParameter("RuntimeValueProvider(odd)").contains("RuntimeValueProvider(odd)"))
    assert(PythonSteps.runtimeParameter("NestedValueProvider(value: x, translator: f)").contains("NestedValueProvider(value: x, translator: f)"))
    assert(PythonSteps.runtimeParameter("gs://b/in/*").isEmpty)
  }

  test("data the job makes itself, unions, and what the Python SDK has none of") {
    List("apache_beam.transforms.core.Create", "apache_beam.transforms.periodicsequence.PeriodicImpulse",
      "apache_beam.transforms.periodicsequence.PeriodicSequence", "apache_beam.testing.test_stream.TestStream", "beam:transform:teststream:v1")
      .foreach(c => assert(PythonSteps.isInJobData(step("S", c)), c))
    assert(!PythonSteps.isInJobData(step("S", "apache_beam.transforms.core.Impulse")))
    assert(PythonSteps.isUnion(step("S", "apache_beam.transforms.core.Flatten")) && PythonSteps.isUnion(step("S", "beam:transform:flatten:v1")))
    assert(!PythonSteps.isUnion(step("S", "apache_beam.transforms.util.CoGroupByKey")))
    val any = step("S", "x")
    assert(!PythonSteps.isFileWrite(any) && !PythonSteps.isTextParser(any) && PythonSteps.textFormatterFormat(any).isEmpty)
    assert(!PythonSteps.recognisesShape(any) && PythonSteps.shape(any).isEmpty)
    assert(PythonSteps.fileWrite(any).isInstanceOf[IoWrite.Unverifiable])
  }

  // --- real Python pipelines, translated ---

  test("Parquet rows, a filter and typed rows: the write, its columns from the schema it writes, and the read's columns") {
    val t = translate("parquet_job")
    assert(t.unverifiable.isEmpty && t.reportSteps.isEmpty)
    val w = t.writes.loneElement
    assert((w.location, w.format) == ("data/out/report", Some(Formats.Parquet)))
    assert(w.outputSchema.contains(LogicalSchema(List(
      LogicalField("id", LogicalType.LongType, nullable = false),
      LogicalField("amount", LogicalType.LongType),
      LogicalField("tags", LogicalType.ArrayType(LogicalType.StringType)),
      LogicalField("placed", LogicalType.TimestampType),
      LogicalField("price", LogicalType.DecimalType(10, 2)),
      LogicalField("customer", LogicalType.StructType(List(LogicalField("name", LogicalType.StringType), LogicalField("since", LogicalType.DateType, nullable = false)))),
      LogicalField("attrs", LogicalType.MapType(LogicalType.StringType, LogicalType.IntegerType))
    ))))
    assert(w.inputSchemas == List("data/in/orders" -> LogicalSchema(List(LogicalField("id", LogicalType.LongType), LogicalField("amount", LogicalType.LongType)))))
    assert(PlanPrinter.render(w.plan).contains("Read(data/in/orders)"))
    assert(w.unlocatedReads.isEmpty && w.opaqueCombines.isEmpty)
  }

  test("a read of Python dictionaries has no columns, and neither has a text write of them") {
    val w = translate("parquet_dicts").writes.loneElement
    assert((w.location, w.format, w.outputSchema, w.inputSchemas) == ("data/out/lines", Some(Formats.Text), None, Nil))
  }

  test("rows written straight to text keep the rows' columns") {
    val w = translate("rows_to_text").writes.loneElement
    assert(w.outputSchema.contains(LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = false), LogicalField("amount", LogicalType.LongType)))))
  }

  test("text, Avro and TFRecord reads and writes") {
    val writes = translate("text_and_avro").writes.map(w => (w.location, w.format, w.outputSchema.map(_.fields.map(_.name)), reads(w)))
    assert(writes == List(
      ("data/out/lines", Some(Formats.Text), None, List("data/in/lines")),
      ("data/out/avro/part", Some(Formats.Avro), Some(List("id", "amount")), List("data/in/avro")),
      ("data/out/records/part", Some("tfrecord"), None, List("data/in/records"))
    ))
  }

  test("BigQuery: located reads and writes, the save mode and schema a load states, and a write to a computed table refused") {
    val t = translate("bigquery_job")
    val byTable = t.writes.map(w => w.location -> w).toMap
    assert(byTable.keySet == Set("proj/ds/report", "proj/ds/events"))
    val load = byTable("proj/ds/report")
    assert(load.plan.saveMode.contains("overwrite") && load.plan.catalog.flatMap(_.table).contains("report"))
    assert(load.outputSchema.contains(LogicalSchema(List(
      LogicalField("id", LogicalType.LongType, nullable = false), LogicalField("tags", LogicalType.ArrayType(LogicalType.StringType), nullable = false)
    ))))
    assert(reads(load) == List("proj/ds/orders"))
    val stream = byTable("proj/ds/events")
    assert(stream.plan.saveMode.contains("append") && stream.outputSchema.isEmpty, "streaming inserts publish no schema")
    assert(t.unverifiable.map(_.operation) == List("a BigQuery write whose table is computed by the job or not stated, so where it lands cannot be told"))
    val top = fixture("bigquery_job").map(s => s.name -> s).toMap
    assert(PythonSteps.read(top("ReadDirect")).collect { case l: IoRead.Located => l.locations }.contains(List("proj/ds/customers")))
    assert(PythonSteps.read(top("ReadQuery")) == Some(IoRead.Unlocated("a BigQuery query, whose tables are named only in its SQL")))
  }

  test("Pub/Sub: a stream read and written, located by topic") {
    val w = translate("pubsub_job").writes.loneElement
    assert((w.location, w.format, reads(w)) == ("projects/my-project/topics/alerts", Some("pubsub"), List("projects/my-project/topics/events")))
  }

  test("CSV through pandas states no location: the read is not located and the write is refused") {
    val t = translate("pandas_csv")
    assert(t.writes.isEmpty)
    assert(t.unverifiable.map(_.step) == List(Some("WriteCsv")))
    assert(PythonSteps.read(fixture("pandas_csv").head).exists(_.isInstanceOf[IoRead.Unlocated]))
  }

  test("a classic template's parameters: the write is refused, and the read is not located") {
    val t = translate("template_job")
    assert(t.writes.isEmpty && t.unverifiable.map(_.operation) == List("a write whose destination is a template parameter (output), known only when the job runs"))
    assert(PythonSteps.read(fixture("template_job").head) == Some(IoRead.Unlocated("a read whose source is a template parameter (input), known only when the job runs")))
  }

  test("a step that ends the pipeline is refused by name, one that reads only what a write reports is let through, data the job makes is no read") {
    val t = translate("side_effects")
    assert(t.unverifiable.map(_.step) == List(Some("PushToService")))
    assert(t.reportSteps == List("Notify (ParDo)"))
    val made = t.writes.find(_.location == "data/out/made").get
    assert(made.unlocatedReads.isEmpty && reads(made).isEmpty)
    assert(made.outputSchema.map(_.fields.map(_.name)).contains(List("id", "amount")))
  }

  test("collections combined by CoGroupByKey are a combine the shape rules cannot see into; a Flatten is not; an untyped Select has no columns") {
    val writes = translate("joined").writes.map(w => w.location -> w).toMap
    val joined = writes("data/out/joined")
    assert(joined.opaqueCombines == List("Join (CoGroupByKey)") && reads(joined).toSet == Set("data/in/orders", "data/in/lines"))
    assert(writes("data/out/union").opaqueCombines.isEmpty)
    assert(writes("data/out/selected").outputSchema.isEmpty)
  }

  test("reads the adapter cannot locate are seen as reads, each saying why") {
    val unlocated = translate("unknown_sources").writes.map(w => w.location -> w.unlocatedReads).toMap
    assert(unlocated == Map(
      "data/out/own" -> List("ReadOwn: a read from __main__.OwnSource, a source this adapter does not know"),
      "data/out/start" -> List("Start: Impulse, a step that brings data into the pipeline without stating where from"),
      "data/out/names" -> List("Match: MatchFiles, a step that brings data into the pipeline without stating where from")
    ))
  }
}

private[beamadapter] object PythonStepsSpec {
  val Pattern = "file_pattern"
  val IobaseRead = "apache_beam.io.iobase.Read"
  val ParquetSource = "apache_beam.io.parquetio._ParquetSource"
  val TextSource = "apache_beam.io.textio._TextSource"
  val ParquetSink = "apache_beam.io.parquetio._ParquetSink"
  val TextSink = "apache_beam.io.textio._TextSink"
  val BigQueryWrite = "apache_beam.io.gcp.bigquery.WriteToBigQuery"
  val PubSubWrite = "apache_beam.io.gcp.pubsub.WriteToPubSub"

  def collection(name: String): BeamCollection = BeamCollection(name, bounded = true, None, None)

  def step(
      name: String,
      className: String,
      display: Map[String, String] = Map.empty,
      namespaces: Map[String, String] = Map.empty,
      inputs: List[BeamCollection] = Nil,
      children: List[BeamStep] = Nil
  ): BeamStep = BeamStep(name, className, inputs, Nil, display, children, displayNamespaces = namespaces)

  /** The locations a write's plan reads. */
  def reads(w: TranslatedWrite): List[String] = {
    def walk(p: com.invaract.ir.Plan): List[String] = p match {
      case r: com.invaract.ir.Read => List(r.dataset.location)
      case other   => other.children.flatMap(walk)
    }
    walk(w.plan).distinct
  }

  implicit class Lone[A](val items: List[A]) extends AnyVal {
    def loneElement: A = items match {
      case only :: Nil => only
      case other       => throw new AssertionError(s"expected exactly one, got $other")
    }
  }
}
