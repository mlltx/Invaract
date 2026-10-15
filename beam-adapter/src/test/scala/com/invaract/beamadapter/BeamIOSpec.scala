// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{CatalogIdentity, DatasetRef, Plan, Read, UnknownPlan}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, TableDestination}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.jdbc.JdbcIO
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.io.{BoundedSource, FileIO, GenerateSequence, Read => BeamRead, TextIO}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.{Create, DoFn, MapElements, ParDo, SerializableFunction}
import org.apache.beam.sdk.values.{TypeDescriptor, TypeDescriptors, ValueInSingleWindow}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** The IOs other than files, and the file reads that go through `FileIO.match()`: what the translator reads from each one's
  * display data. Every pipeline is only constructed, never run (a construction needs no BigQuery, Kafka or database). */
class BeamIOSpec extends AnyFunSuite {
  import ExampleJob.OrdersSchema

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private def readsIn(plan: Plan): List[String] = plan match {
    case Read(DatasetRef(location), _, _) => List(location)
    case other                            => other.children.flatMap(readsIn)
  }

  private def unlocatedIn(plan: Plan): List[String] = plan match {
    case u: UnknownPlan if u.sourceType == BeamTranslator.UnlocatedSourceType => u.description :: u.children.flatMap(unlocatedIn)
    case other                                                                 => other.children.flatMap(unlocatedIn)
  }

  private val tableSchema = new TableSchema().setFields(
    List(
      new TableFieldSchema().setName("id").setType("INT64").setMode("REQUIRED"),
      new TableFieldSchema().setName("amount").setType("NUMERIC")
    ).asJava
  )

  private def toRows(p: Pipeline) =
    p.apply("Rows", Create.of[TableRow](new TableRow()).withCoder(org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder.of()))

  // --- BigQuery ---

  test("a BigQuery table write is located by its table, with its save mode, catalog table and declared schema") {
    Seq(WriteDisposition.WRITE_APPEND -> "append", WriteDisposition.WRITE_TRUNCATE -> "overwrite", WriteDisposition.WRITE_EMPTY -> "error").foreach {
      case (disposition, saveMode) =>
        val p = pipeline
        toRows(p).apply(
          "Write",
          BigQueryIO.writeTableRows().to("proj:ds.report").withSchema(tableSchema).withWriteDisposition(disposition)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        )
        val Seq(w) = BeamTranslator.translate(p).writes
        assert(w.location == "proj/ds/report" && w.format.contains("bigquery"))
        assert(w.plan.saveMode.contains(saveMode), disposition)
        assert(w.plan.catalog.contains(CatalogIdentity(technology = Some("bigquery"), namespace = List("proj", "ds"), table = Some("report"))))
        assert(w.outputSchema.contains(LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = false), LogicalField("amount", LogicalType.DecimalType(38, 9), nullable = true)))))
    }
  }

  test("a BigQuery write whose schema the job computes has no declared schema, and one whose table the job computes is unverifiable") {
    val p = pipeline
    toRows(p).apply("Write", BigQueryIO.writeTableRows().to("ds.report").withCreateDisposition(CreateDisposition.CREATE_NEVER))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "ds/report", "no project is stated, so none is claimed")
    assert(w.outputSchema.isEmpty && w.plan.saveMode.contains("error"))

    val q = pipeline
    toRows(q).apply(
      "Write",
      BigQueryIO.writeTableRows()
        .to(new SerializableFunction[ValueInSingleWindow[TableRow], TableDestination] {
          def apply(v: ValueInSingleWindow[TableRow]): TableDestination = new TableDestination("proj:ds.t", null)
        })
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
    )
    val translation = BeamTranslator.translate(q)
    assert(translation.writes.isEmpty)
    assert(translation.unverifiable.map(_.operation).exists(_.contains("dynamic destinations")))
  }

  test("a BigQuery table read is a located input in the bigquery catalog; a query read is seen but not located") {
    val p = pipeline
    val write = p.apply("Read2", BigQueryIO.readTableRows().from("proj.ds.customers"))
    write.apply("Write2", BigQueryIO.writeTableRows().to("proj:ds.out").withSchema(tableSchema))
    val translated = BeamTranslator.translate(p).writes.find(_.location == "proj/ds/out").get
    assert(translated.plan.input == Read(DatasetRef("proj/ds/customers"), catalog = Some(CatalogIdentity(Some("bigquery"), namespace = List("proj", "ds"), table = Some("customers")))))

    val q = pipeline
    q.apply("Query", BigQueryIO.readTableRows().fromQuery("SELECT * FROM `proj.ds.orders`").usingStandardSql())
      .apply("Write", BigQueryIO.writeTableRows().to("proj:ds.out").withSchema(tableSchema))
    val Seq(w) = BeamTranslator.translate(q).writes
    assert(readsIn(w.plan).isEmpty)
    assert(unlocatedIn(w.plan).exists(_.contains("a BigQuery query")))
    assert(w.unlocatedReads.exists(_.contains("Query: a BigQuery query")))
  }

  // --- Kafka ---

  test("a Kafka read is located by each topic under its cluster, and a Kafka write by its topic") {
    val p = pipeline
    p.apply(
        "Read",
        KafkaIO.read[java.lang.Long, String]().withBootstrapServers("broker:9092").withTopics(List("orders", "refunds").asJava)
          .withKeyDeserializer(classOf[LongDeserializer]).withValueDeserializer(classOf[StringDeserializer]).withoutMetadata()
      )
      .apply(
        "Write",
        KafkaIO.write[java.lang.Long, String]().withBootstrapServers("broker:9092").withTopic("report")
          .withKeySerializer(classOf[LongSerializer]).withValueSerializer(classOf[StringSerializer])
      )
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "broker:9092/report" && w.format.contains("kafka"))
    assert(readsIn(w.plan) == List("broker:9092/orders", "broker:9092/refunds"))
    assert(!w.bounded, "a Kafka read is a stream")
  }

  test("a Kafka read of topics matching a pattern is seen but not located") {
    val p = pipeline
    p.apply(
        "Read",
        KafkaIO.read[java.lang.Long, String]().withBootstrapServers("broker:9092").withTopicPattern("orders-.*")
          .withKeyDeserializer(classOf[LongDeserializer]).withValueDeserializer(classOf[StringDeserializer]).withoutMetadata()
      )
      .apply("Write", KafkaIO.write[java.lang.Long, String]().withBootstrapServers("b:1").withTopic("t").withKeySerializer(classOf[LongSerializer]).withValueSerializer(classOf[StringSerializer]))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.unlocatedReads.exists(_.contains("matching the pattern orders-.*")))
  }

  // --- JDBC ---

  private val db = JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://db/shop")

  private def jdbcRead(p: Pipeline, query: String) =
    p.apply(
      "Read",
      JdbcIO.read[String]().withDataSourceConfiguration(db).withQuery(query)
        .withRowMapper(new JdbcIO.RowMapper[String] { def mapRow(rs: java.sql.ResultSet): String = rs.getString(1) })
        .withCoder(StringUtf8Coder.of())
    )

  private def jdbcWrite(statement: String) =
    JdbcIO.write[String]().withDataSourceConfiguration(db).withStatement(statement)
      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter[String] { def setParameters(e: String, ps: java.sql.PreparedStatement): Unit = () })

  test("a JDBC read of one plain table and a JDBC INSERT INTO one table are located under the connection URL") {
    val p = pipeline
    jdbcRead(p, "select id, amount from public.orders where amount > 0").apply("Write", jdbcWrite("INSERT INTO report (id) VALUES (?)"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "jdbc:postgresql://db/shop/report" && w.format.contains("jdbc"))
    assert(readsIn(w.plan) == List("jdbc:postgresql://db/shop/public/orders"))
  }

  test("a JDBC query that reads more than one table is seen but not located, and a statement that is not an INSERT is unverifiable") {
    val p = pipeline
    jdbcRead(p, "select o.id from orders o join customers c on o.cid = c.id").apply("Write", jdbcWrite("update report set x = ?"))
    val translation = BeamTranslator.translate(p)
    assert(translation.writes.isEmpty)
    assert(translation.unverifiable.map(_.operation).exists(_.contains("not a plain INSERT INTO one table")))
    assert(unlocatedIn(translation.unverifiable.head.plan).exists(_.contains("more than one plain table")))
  }

  // --- Pub/Sub ---

  test("a Pub/Sub read and write are located by their topic") {
    val p = pipeline
    p.apply("Read", PubsubIO.readStrings().fromTopic("projects/my-project/topics/orders"))
      .apply("Write", PubsubIO.writeStrings().to("projects/my-project/topics/report"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.location == "projects/my-project/topics/report" && w.format.contains("pubsub"))
    assert(readsIn(w.plan) == List("projects/my-project/topics/orders"))
  }

  // --- files matched by FileIO.match() ---

  test("files matched by FileIO.match() and read by readFiles() are the folder the pattern names, with the reader's columns") {
    val p = pipeline
    p.apply("Match", FileIO.`match`().filepattern("/data/in/orders/*.parquet")).apply("Open", FileIO.readMatches())
      .apply("Read", ParquetIO.readFiles(OrdersSchema))
      .apply("Write", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to("/data/out/report"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan) == List("/data/in/orders"))
    assert(w.inputSchemas.map(_._1) == List("/data/in/orders"))
    assert(w.inputSchemas.head._2.fields.map(_.name) == List("id", "amount"))
    assert(w.unlocatedReads.isEmpty)
  }

  test("matched files the job reads with its own function are still a read of that folder") {
    val p = pipeline
    p.apply("Match", FileIO.`match`().filepattern("/data/in/raw/*")).apply("Open", FileIO.readMatches())
      .apply("Parse", ParDo.of(new ReadOwnFormat)).apply("Write", TextIO.write().to("/data/out/lines"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan) == List("/data/in/raw"))
    assert(w.inputSchemas.isEmpty, "a function's output has no columns to check")
  }

  test("files whose patterns the job computes are seen but not located") {
    val p = pipeline
    p.apply("Patterns", Create.of("/data/in/a/*", "/data/in/b/*")).apply("MatchAll", FileIO.matchAll())
      .apply("Open", FileIO.readMatches()).apply("Read", ParquetIO.readFiles(OrdersSchema))
      .apply("Write", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to("/data/out/report"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan).isEmpty)
    assert(w.unlocatedReads.exists(_.contains("FileIO.matchAll")))
    assert(w.unlocatedReads.exists(_.contains("files handed to it by a step that does not say where they are")))
  }

  // --- data the job makes, and sources it does not know ---

  test("data the job makes itself (Create.of, GenerateSequence) is not a read from outside it") {
    val p = pipeline
    p.apply("Numbers", GenerateSequence.from(0).to(3))
      .apply("ToText", MapElements.into(TypeDescriptors.strings()).via(new SerializableFunction[java.lang.Long, String] { def apply(l: java.lang.Long) = l.toString }))
      .apply("Write", TextIO.write().to("/data/out/numbers"))
    p.apply("Words", Create.of("a")).apply("Write2", TextIO.write().to("/data/out/words"))
    val writes = BeamTranslator.translate(p).writes
    assert(writes.size == 2 && writes.forall(_.unlocatedReads.isEmpty))
  }

  test("a read from a source class the adapter does not know is seen but not located") {
    val p = pipeline
    p.apply("Read", BeamRead.from(new TeamSource)).apply("Write", TextIO.write().to("/data/out/team"))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(w.unlocatedReads.exists(_.contains("TeamSource")))
  }
}

private final class ReadOwnFormat extends DoFn[FileIO.ReadableFile, String] {
  @DoFn.ProcessElement
  def process(@DoFn.Element f: FileIO.ReadableFile, out: DoFn.OutputReceiver[String]): Unit = out.output(f.getMetadata.resourceId.toString)
}

/** A team's own bounded source, which states nothing about where it reads. */
private final class TeamSource extends BoundedSource[String] {
  override def split(desiredBundleSizeBytes: Long, options: PipelineOptions): java.util.List[_ <: BoundedSource[String]] = List(this).asJava
  override def getEstimatedSizeBytes(options: PipelineOptions): Long = 0L
  override def createReader(options: PipelineOptions): BoundedSource.BoundedReader[String] = throw new UnsupportedOperationException("graph only")
  override def getOutputCoder: org.apache.beam.sdk.coders.Coder[String] = StringUtf8Coder.of()
}
