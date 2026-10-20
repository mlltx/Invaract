// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.testkit.{ColumnSource, JoinOn, OutColumn, ReferenceAdapter, ScenarioInput, ScenarioJob}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.sdk.io.{FileIO, TextIO}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, BigQueryServices}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.transforms.Watch
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.joda.time.Duration
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils
import org.apache.beam.sdk.schemas.transforms.{CoGroup, Group, Join, Filter => SchemaFilter}
import org.apache.beam.sdk.transforms.{Combine, Contextful, Count, DoFn, Flatten, MapElements, ParDo, SerializableFunction}
import org.apache.beam.sdk.values.{PCollection, PCollectionList, PCollectionTuple, Row, TypeDescriptor}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.io.{OutputFile, PositionOutputStream}

import java.nio.file.{Files, Path => NioPath}
import scala.collection.JavaConverters._

/** A neutral `ScenarioJob` as a real Beam pipeline: every input an (empty) Parquet dataset read with `ParquetIO`, the
  * job's join, filter, grouping and projection as Beam transforms over Avro records, and the output written with
  * `FileIO.write` (Parquet) or, for a CSV output, `FileIO.write` through `TextIO.sink()`. Nothing here knows about Invaract:
  * it is what a team's job looks like, which is what the adapter's enforcement is then run on.
  *
  * The data is empty on purpose (the scenarios are about shapes: locations, columns, types, which steps exist), so the
  * functions below are never called by the conformance run itself; `BeamScenarioJobsSpec` runs a few rows through to show
  * they are real.
  */
private[beamadapter] object BeamScenarioJobs {

  /** Where a scenario job's output goes: files under the scenario's root, or a BigQuery table (the only Beam sink that states
    * a save mode and a catalog table), written to Beam's in-memory BigQuery so the job really runs. */
  sealed trait Target
  case object ToFiles extends Target
  final case class BigQuery(services: BigQueryServices, project: String) extends Target

  /** The BigQuery table a scenario output location names: `out/report` is dataset `out`, table `report`. */
  def bigQueryTable(project: String, location: String): String = {
    val parts = location.split('/')
    s"$project:${parts.init.mkString("_")}.${parts.last}"
  }

  /** The pipeline for `job`, reading and writing under `root` (the scenario's locations are relative to it). A streaming job
    * reads its input as a stream that watches the folder for new files and stops a few seconds after the last one, and writes
    * windowed output, so it runs to completion over the files that exist. */
  def build(pipeline: Pipeline, job: ScenarioJob, root: NioPath, target: Target = ToFiles): Unit = {
    require(job.rowChange.isEmpty && !job.boundary, "this adapter's jobs are read-transform-writes")
    val schemas = job.inputs.map(i => avro(s"in_${i.location.replace('/', '_')}", i.schema))
    val reads: List[PCollection[GenericRecord]] = job.inputs.zip(schemas).zipWithIndex.map { case ((input, schema), i) =>
      materialize(root, input, schema)
      val pattern = root.resolve(input.location).toString + "/*.parquet"
      if (job.streaming)
        pipeline
          .apply(s"Watch$i", FileIO.`match`().filepattern(pattern).continuously(Duration.millis(200), Watch.Growth.afterTimeSinceNewOutput[String](Duration.standardSeconds(2))))
          .apply(s"Open$i", FileIO.readMatches())
          .apply(s"Read$i", ParquetIO.readFiles(schema))
          .apply(s"Window$i", Window.into[GenericRecord](FixedWindows.of(Duration.standardMinutes(1))))
      else
        pipeline
          .apply(s"Read$i", ParquetIO.read(schema).from(pattern))
          .asInstanceOf[PCollection[GenericRecord]]
    }

    if (job.untranslatableWrite) {
      // the representative of "an operation that changes data and that the adapter has no translation for": a function
      // that pushes every record somewhere Beam cannot see
      reads.head.apply("PushToSomeService", ParDo.of(new SideEffectFn))
      return
    }

    // the filter, on the first input before anything is joined to it: Beam's schema Filter, which names the column it reads
    val firstFiltered = job.filterColumn.fold(reads.head) { column =>
      asRows(reads.head, schemas.head, "AsRows0").apply("Filter", SchemaFilter.create[GenericRecord]().whereFieldName[AnyRef](column, new PositiveValue))
    }
    val (filtered, joinedFieldOf) = combine(firstFiltered :: reads.tail, schemas, job)
    val opaque =
      if (job.opaqueStep) filtered.apply("OpaqueStep", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new IdentityFn)).setCoder(filtered.getCoder)
      else filtered
    // branches that rejoin: two steps over the same collection, merged again (Flatten) each time
    val stepped = (1 to job.rejoins).foldLeft(opaque) { (below, i) =>
      def side(name: String) =
        below.apply(s"$name$i", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new IdentityFn)).setCoder(below.getCoder)
      PCollectionList.of(side("Left")).and(side("Right")).apply(s"Rejoin$i", Flatten.pCollections[GenericRecord]())
    }
    val out = ReferenceAdapter.outputSchema(job)
    val outAvro = avro("report", out)
    val shaped = job.groupBy match {
      case Some(keys) => grouped(stepped, keys.map(joinedFieldOf(0, _)), job.columns.map(c => resolve(c, joinedFieldOf)), outAvro)
      case None =>
        stepped
          .apply("Project", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new ProjectFn(outAvro.toString, job.columns.map(c => resolve(c, joinedFieldOf)))))
          .setCoder(AvroGenericCoder.of(outAvro))
    }
    target match {
      case ToFiles                     => write(shaped, job, root, outAvro)
      case BigQuery(services, project) => writeBigQuery(shaped, job, out, services, project)
    }
    // the job's further writes, after its main one: each a copy of an input, in parquet
    job.extraWrites.zipWithIndex.foreach { case (extra, i) =>
      val schema = avro(s"extra_${extra.input.location.replace('/', '_')}", extra.input.schema)
      materialize(root, extra.input, schema)
      pipeline
        .apply(s"ReadExtra$i", ParquetIO.read(schema).from(root.resolve(extra.input.location).toString + "/*.parquet"))
        .apply(s"WriteExtra$i", FileIO.write[GenericRecord]().via(ParquetIO.sink(schema)).to(root.resolve(extra.output.location).toString).withSuffix(".parquet"))
    }
  }

  // --- reading ---

  /** An empty Parquet file of `schema` at the input's location, so a read of the folder finds a dataset. */
  private def materialize(root: NioPath, input: ScenarioInput, schema: Schema): Unit = materialize(root.resolve(input.location), schema, Nil)

  /** A Parquet file of `schema` holding `rows` in folder `dir`. Written through a plain local `OutputFile`, not Hadoop's
    * file system, so a machine without Hadoop's native libraries (Windows) writes it all the same. (Parquet's own
    * `LocalOutputFile` would do, but it arrived in Parquet 1.14 and the oldest Beam release tested brings 1.13.) */
  def materialize(dir: NioPath, schema: Schema, rows: List[GenericRecord]): Unit = {
    Files.createDirectories(dir)
    val writer = AvroParquetWriter.builder[GenericRecord](new LocalFile(dir.resolve("part-0.parquet"))).withSchema(schema).build()
    try rows.foreach(writer.write)
    finally writer.close()
  }

  private final class LocalFile(path: NioPath) extends OutputFile {
    override def create(blockSizeHint: Long): PositionOutputStream = createOrOverwrite(blockSizeHint)
    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
      val out = new java.io.BufferedOutputStream(Files.newOutputStream(path))
      new PositionOutputStream {
        private var position = 0L
        override def getPos: Long = position
        override def write(b: Int): Unit = { out.write(b); position += 1 }
        override def write(b: Array[Byte], off: Int, len: Int): Unit = { out.write(b, off, len); position += len }
        override def flush(): Unit = out.flush()
        override def close(): Unit = out.close()
      }
    }
    override def supportsBlockSize(): Boolean = false
    override def defaultBlockSize(): Long = 0L
  }

  /** `records` as a collection with a Beam schema (`schema`'s), which Beam's schema transforms need: a step of its own, so the
    * read's own output is left as the read made it. */
  private def asRows(records: PCollection[GenericRecord], schema: Schema, name: String): PCollection[GenericRecord] =
    records.apply(name, MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new IdentityFn)).setCoder(AvroUtils.schemaCoder(schema))

  // --- combining the inputs: nothing, a keyed join, or a cartesian product ---

  /** The records the rest of the job works on, and the field of those records that holds `(input, column)`. */
  private def combine(
      reads: List[PCollection[GenericRecord]],
      schemas: List[Schema],
      job: ScenarioJob
  ): (PCollection[GenericRecord], (Int, String) => String) =
    if (reads.size == 1) (reads.head, (_: Int, column: String) => column)
    else {
      val merged = mergedSchema(schemas)
      val joinedFieldOf = (i: Int, column: String) => s"${if (i == 0) "l" else "r"}_$column"
      val mergedCoder = AvroGenericCoder.of(merged)
      // Beam's schema transforms, which name the columns they match on: a Join on the two columns, or, for a cartesian product, a
      // CoGroup on no columns at all (every row of each side in the one group)
      val left = if (reads.head.hasSchema) reads.head else asRows(reads.head, schemas(0), "AsRows0")
      val right = asRows(reads(1), schemas(1), "AsRows1")
      val rows: PCollection[Row] = job.join match {
        case Some(JoinOn(l, r)) =>
          left.apply("Join", Join.innerJoin[GenericRecord, GenericRecord](right).on(Join.FieldsEqual.left(l).right(r)))
        case None =>
          PCollectionTuple.of("lhs", left).and("rhs", right).apply("CrossJoin", CoGroup.join(CoGroup.By.fieldNames()).crossProductJoin())
      }
      val combined = rows.apply("Merge", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new MergeRowFn(merged.toString))).setCoder(mergedCoder)
      (combined, joinedFieldOf)
    }

  private def mergedSchema(schemas: List[Schema]): Schema = {
    val fields = schemas.zip(List("l", "r")).flatMap { case (schema, side) =>
      schema.getFields.asScala.map(f => new Schema.Field(s"${side}_${f.name}", f.schema, null, f.defaultVal))
    }
    Schema.createRecord("joined", null, "scenario", false, fields.asJava)
  }

  // --- grouping ---

  /** Beam's schema `Group` by the key columns, counting each group's rows. */
  private def grouped(
      input: PCollection[GenericRecord],
      keyFields: List[String],
      columns: List[ResolvedColumn],
      out: Schema
  ): PCollection[GenericRecord] = {
    val rows = if (input.hasSchema) input else asRows(input, input.getCoder.asInstanceOf[AvroCoder[GenericRecord]].getSchema, "AsRowsForGroup")
    val count = Count.combineFn[AnyRef]().asInstanceOf[Combine.CombineFn[AnyRef, AnyRef, java.lang.Long]]
    rows
      .apply("Group", Group.byFieldNames[GenericRecord](keyFields: _*).aggregateField(keyFields.head, count, "total"))
      .apply("Count", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new GroupRowFn(out.toString, columns)))
      .setCoder(AvroGenericCoder.of(out))
  }

  // --- writing ---

  private def write(data: PCollection[GenericRecord], job: ScenarioJob, root: NioPath, out: Schema): Unit = {
    val destination = root.resolve(job.output.location).toString
    // a stream is written window by window, which Beam requires to name its number of files
    def sharded[D](w: FileIO.Write[D, GenericRecord]): FileIO.Write[D, GenericRecord] = if (job.streaming) w.withNumShards(1) else w
    job.output.format match {
      case "parquet" =>
        data.apply("Write", sharded(FileIO.write[GenericRecord]().via(ParquetIO.sink(out)).to(destination).withSuffix(".parquet")))
      case "csv" =>
        data.apply("Write", sharded(FileIO.write[GenericRecord]().via(Contextful.fn[GenericRecord, String](new CsvFn), TextIO.sink()).to(destination).withSuffix(".csv")))
      case other => throw new IllegalArgumentException(s"this adapter's jobs write parquet or csv, not '$other'")
    }
  }

  private val Dispositions = Map("append" -> WriteDisposition.WRITE_APPEND, "overwrite" -> WriteDisposition.WRITE_TRUNCATE, "error" -> WriteDisposition.WRITE_EMPTY)

  private def writeBigQuery(data: PCollection[GenericRecord], job: ScenarioJob, out: LogicalSchema, services: BigQueryServices, project: String): Unit = {
    val disposition = Dispositions.getOrElse(job.output.saveMode, throw new IllegalArgumentException(s"BigQuery has no write disposition for '${job.output.saveMode}'"))
    data
      .apply("ToTableRows", MapElements.into(TypeDescriptor.of(classOf[TableRow])).via(new TableRowFn))
      .apply(
        "Write",
        BigQueryIO.writeTableRows().to(bigQueryTable(project, job.output.location)).withSchema(tableSchema(out))
          .withWriteDisposition(disposition).withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withTestServices(services).withoutValidation()
      )
  }

  private def tableSchema(schema: LogicalSchema): TableSchema =
    new TableSchema().setFields(schema.fields.map { f =>
      val (bqType, repeated) = f.dataType match {
        case LogicalType.ArrayType(e) => (bigQueryType(e), true)
        case t                        => (bigQueryType(t), false)
      }
      new TableFieldSchema().setName(f.name).setType(bqType).setMode(if (repeated) "REPEATED" else if (f.nullable) "NULLABLE" else "REQUIRED")
    }.asJava)

  private def bigQueryType(t: LogicalType): String = t match {
    case LogicalType.LongType    => "INT64"
    case LogicalType.StringType  => "STRING"
    case LogicalType.BooleanType => "BOOL"
    case LogicalType.DoubleType  => "FLOAT64"
    case LogicalType.BinaryType  => "BYTES"
    case other                   => throw new IllegalArgumentException(s"no BigQuery type chosen for the scenario type ${other.catalogString}")
  }

  private final class TableRowFn extends SerializableFunction[GenericRecord, TableRow] {
    override def apply(r: GenericRecord): TableRow = {
      val row = new TableRow()
      r.getSchema.getFields.asScala.foreach(f => row.set(f.name, Option(r.get(f.name)).map(_.toString).orNull))
      row
    }
  }

  // --- columns ---

  private[beamadapter] sealed trait ResolvedColumn extends Serializable { def name: String }
  private[beamadapter] final case class Copy(name: String, field: String) extends ResolvedColumn
  private[beamadapter] final case class CastTo(name: String, field: String, to: String) extends ResolvedColumn
  private[beamadapter] final case class Constant(name: String, value: Long) extends ResolvedColumn
  private[beamadapter] final case class Uuid(name: String) extends ResolvedColumn
  private[beamadapter] final case class RowCount(name: String) extends ResolvedColumn

  private def resolve(c: OutColumn, fieldOf: (Int, String) => String): ResolvedColumn = c.source match {
    case ColumnSource.FromInput(i, col)    => Copy(c.name, fieldOf(i, col))
    case ColumnSource.CastInput(i, col, t) => CastTo(c.name, fieldOf(i, col), t.typeName)
    case ColumnSource.NonNullLong(v)       => Constant(c.name, v)
    case ColumnSource.UniqueId             => Uuid(c.name)
    case ColumnSource.RowCount             => RowCount(c.name)
  }

  private def valueOf(column: ResolvedColumn, record: GenericRecord, groupSize: Long): AnyRef = column match {
    case Copy(_, field)          => record.get(field)
    case CastTo(_, field, "string") => Option(record.get(field)).map(_.toString).orNull
    case CastTo(_, field, "long")   => Option(record.get(field)).map(v => java.lang.Long.valueOf(v.toString.toLong)).orNull
    case CastTo(_, field, _)     => record.get(field)
    case Constant(_, v)          => java.lang.Long.valueOf(v)
    case Uuid(_)                 => java.util.UUID.randomUUID().toString
    case RowCount(_)             => java.lang.Long.valueOf(groupSize)
  }

  private def recordOf(schemaJson: String, columns: List[ResolvedColumn], source: GenericRecord, groupSize: Long): GenericRecord = {
    val schema = new Schema.Parser().parse(schemaJson)
    val record = new GenericData.Record(schema)
    columns.foreach(c => record.put(c.name, valueOf(c, source, groupSize)))
    record
  }

  // --- the functions (serializable, as Beam requires) ---

  final class IdentityFn extends SerializableFunction[GenericRecord, GenericRecord] {
    override def apply(r: GenericRecord): GenericRecord = r
  }

  final class PositiveValue extends SerializableFunction[AnyRef, java.lang.Boolean] {
    override def apply(v: AnyRef): java.lang.Boolean = java.lang.Boolean.valueOf(Option(v).exists(_.toString.toLong > 0))
  }



  final class ProjectFn(schemaJson: String, columns: List[ResolvedColumn]) extends SerializableFunction[GenericRecord, GenericRecord] {
    override def apply(r: GenericRecord): GenericRecord = recordOf(schemaJson, columns, r, 0L)
  }

  final class CsvFn extends SerializableFunction[GenericRecord, String] {
    override def apply(r: GenericRecord): String = r.getSchema.getFields.asScala.map(f => String.valueOf(r.get(f.name))).mkString(",")
  }

  final class SideEffectFn extends DoFn[GenericRecord, Void] {
    @ProcessElement def process(@DoFn.Element record: GenericRecord): Unit = () // imagine an HTTP call or a JDBC insert here
  }

  /** The same representative for a pipeline of numbers (the translator tests need one that reads no file). */
  final class SideEffectLong extends DoFn[java.lang.Long, Void] {
    @ProcessElement def process(@DoFn.Element number: java.lang.Long): Unit = ()
  }

  /** A joined row (Beam's `lhs`/`rhs`: one row of each side) as one record with each side's columns prefixed `l_` and `r_`. */
  final class MergeRowFn(mergedJson: String) extends SerializableFunction[Row, GenericRecord] {
    override def apply(joined: Row): GenericRecord = {
      val record = new GenericData.Record(new Schema.Parser().parse(mergedJson))
      List("lhs" -> "l", "rhs" -> "r").foreach { case (side, prefix) =>
        val row = joined.getRow(side)
        row.getSchema.getFieldNames.asScala.foreach(f => record.put(s"${prefix}_$f", avroValue(row.getValue[AnyRef](f))))
      }
      record
    }
  }

  /** A grouped row (Beam's `key`, the group's key columns, and `value`, its `total`) as the output record. */
  final class GroupRowFn(outJson: String, columns: List[ResolvedColumn]) extends SerializableFunction[Row, GenericRecord] {
    override def apply(grouped: Row): GenericRecord = {
      val key = grouped.getRow("key")
      val record = new GenericData.Record(new Schema.Parser().parse(outJson))
      columns.foreach {
        case Copy(name, field) => record.put(name, avroValue(key.getValue[AnyRef](field)))
        case RowCount(name)    => record.put(name, grouped.getRow("value").getInt64("total"))
        case other             => throw new IllegalArgumentException(s"a grouped job's columns are its keys and its row count, not $other")
      }
      record
    }
  }

  /** A Beam row's value as an Avro record holds it (a Beam array is a Java collection, which Avro takes as it is). */
  private def avroValue(v: AnyRef): AnyRef = v

  // --- the neutral schema as Avro ---

  /** `schema` as an Avro record: a nullable column is a union with null, a required one is not. */
  def avro(name: String, schema: LogicalSchema): Schema = {
    val fields = schema.fields.map { f =>
      val base = avroType(f.dataType)
      val fieldType = if (f.nullable) Schema.createUnion(Schema.create(Schema.Type.NULL), base) else base
      new Schema.Field(f.name, fieldType, null, if (f.nullable) org.apache.avro.JsonProperties.NULL_VALUE else null)
    }
    Schema.createRecord(name, null, "scenario", false, fields.asJava)
  }

  private def avroType(t: LogicalType): Schema = t match {
    case LogicalType.LongType       => Schema.create(Schema.Type.LONG)
    case LogicalType.IntegerType    => Schema.create(Schema.Type.INT)
    case LogicalType.StringType     => Schema.create(Schema.Type.STRING)
    case LogicalType.BooleanType    => Schema.create(Schema.Type.BOOLEAN)
    case LogicalType.DoubleType     => Schema.create(Schema.Type.DOUBLE)
    case LogicalType.FloatType      => Schema.create(Schema.Type.FLOAT)
    case LogicalType.BinaryType     => Schema.create(Schema.Type.BYTES)
    case LogicalType.ArrayType(e)   => Schema.createArray(avroType(e))
    case other                      => throw new IllegalArgumentException(s"no Avro type chosen for the scenario type ${other.catalogString}")
  }
}
