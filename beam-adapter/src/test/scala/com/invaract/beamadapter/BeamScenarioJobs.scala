// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.testkit.{ColumnSource, JoinOn, OutColumn, ReferenceAdapter, ScenarioInput, ScenarioJob}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.io.{FileIO, TextIO}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.transforms.{Contextful, DoFn, Filter, GroupByKey, MapElements, ParDo, ProcessFunction, SerializableFunction, View}
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionView, TupleTag, TypeDescriptor, TypeDescriptors}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.io.LocalOutputFile

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

  /** The pipeline for `job`, reading and writing under `root` (the scenario's locations are relative to it). */
  def build(pipeline: Pipeline, job: ScenarioJob, root: NioPath): Unit = {
    require(job.rowChange.isEmpty && !job.boundary && !job.streaming, "this adapter's jobs are batch read-transform-writes")
    val schemas = job.inputs.map(i => avro(s"in_${i.location.replace('/', '_')}", i.schema))
    val reads: List[PCollection[GenericRecord]] = job.inputs.zip(schemas).zipWithIndex.map { case ((input, schema), i) =>
      materialize(root, input, schema)
      pipeline
        .apply(s"Read$i", ParquetIO.read(schema).from(root.resolve(input.location).toString + "/*.parquet"))
        .asInstanceOf[PCollection[GenericRecord]]
    }

    if (job.untranslatableWrite) {
      // the representative of "an operation that changes data and that the adapter has no translation for": a function
      // that pushes every record somewhere Beam cannot see
      reads.head.apply("PushToSomeService", ParDo.of(new SideEffectFn))
      return
    }

    val (joined, joinedFieldOf) = combine(reads, schemas, job)
    val filtered = job.filterColumn.fold(joined) { column =>
      {
        val positive: ProcessFunction[GenericRecord, java.lang.Boolean] = new PositiveFn(joinedFieldOf(0, column))
        joined.apply("Filter", Filter.by[GenericRecord, ProcessFunction[GenericRecord, java.lang.Boolean]](positive))
      }
    }
    val stepped =
      if (job.opaqueStep) filtered.apply("OpaqueStep", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new IdentityFn)).setCoder(filtered.getCoder)
      else filtered
    val out = ReferenceAdapter.outputSchema(job)
    val outAvro = avro("report", out)
    val shaped = job.groupBy match {
      case Some(keys) => grouped(stepped, keys.map(joinedFieldOf(0, _)), job.columns.map(c => resolve(c, joinedFieldOf)), outAvro)
      case None =>
        stepped
          .apply("Project", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new ProjectFn(outAvro.toString, job.columns.map(c => resolve(c, joinedFieldOf)))))
          .setCoder(AvroGenericCoder.of(outAvro))
    }
    write(shaped, job, root, outAvro)
  }

  // --- reading ---

  /** An empty Parquet file of `schema` at the input's location, so a read of the folder finds a dataset. */
  private def materialize(root: NioPath, input: ScenarioInput, schema: Schema): Unit = materialize(root.resolve(input.location), schema, Nil)

  /** A Parquet file of `schema` holding `rows` in folder `dir`. Written through parquet's own `LocalOutputFile`, not Hadoop's
    * file system, so a machine without Hadoop's native libraries (Windows) writes it all the same. */
  def materialize(dir: NioPath, schema: Schema, rows: List[GenericRecord]): Unit = {
    Files.createDirectories(dir)
    val writer = AvroParquetWriter.builder[GenericRecord](new LocalOutputFile(dir.resolve("part-0.parquet"))).withSchema(schema).build()
    try rows.foreach(writer.write)
    finally writer.close()
  }

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
      val combined: PCollection[GenericRecord] = job.join match {
        case Some(JoinOn(left, right)) =>
          val tags = List(new TupleTag[GenericRecord]("l"), new TupleTag[GenericRecord]("r"))
          val keyed = reads.zip(List(left, right)).zipWithIndex.map { case ((read, column), i) =>
            read
              .apply(s"Key$i", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(classOf[GenericRecord]))).via(new KeyFn(column)))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), read.getCoder.asInstanceOf[org.apache.beam.sdk.coders.Coder[GenericRecord]]))
          }
          KeyedPCollectionTuple.of(tags(0), keyed(0)).and(tags(1), keyed(1))
            .apply("Join", CoGroupByKey.create[String]())
            .apply("Merge", ParDo.of(new MergeFn(merged.toString, schemas(0).toString, schemas(1).toString)))
            .setCoder(mergedCoder)
        case None =>
          val other: PCollectionView[java.util.List[GenericRecord]] = reads(1).apply("OtherSide", View.asList[GenericRecord]())
          reads.head.apply("CrossJoin", ParDo.of(new CrossFn(merged.toString, other)).withSideInputs(other)).setCoder(mergedCoder)
      }
      (combined, joinedFieldOf)
    }

  private def mergedSchema(schemas: List[Schema]): Schema = {
    val fields = schemas.zip(List("l", "r")).flatMap { case (schema, side) =>
      schema.getFields.asScala.map(f => new Schema.Field(s"${side}_${f.name}", f.schema, null, f.defaultVal))
    }
    Schema.createRecord("joined", null, "scenario", false, fields.asJava)
  }

  // --- grouping ---

  private def grouped(
      input: PCollection[GenericRecord],
      keyFields: List[String],
      columns: List[ResolvedColumn],
      out: Schema
  ): PCollection[GenericRecord] =
    input
      .apply("KeyForGroup", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(classOf[GenericRecord]))).via(new KeyFn(keyFields.mkString(",")) ))
      .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder.asInstanceOf[org.apache.beam.sdk.coders.Coder[GenericRecord]]))
      .apply("Group", GroupByKey.create[String, GenericRecord]())
      .apply("Count", ParDo.of(new GroupFn(out.toString, columns)))
      .setCoder(AvroGenericCoder.of(out))

  // --- writing ---

  private def write(data: PCollection[GenericRecord], job: ScenarioJob, root: NioPath, out: Schema): Unit = {
    val destination = root.resolve(job.output.location).toString
    job.output.format match {
      case "parquet" =>
        data.apply("Write", FileIO.write[GenericRecord]().via(ParquetIO.sink(out)).to(destination).withSuffix(".parquet"))
      case "csv" =>
        data.apply("Write", FileIO.write[GenericRecord]().via(Contextful.fn[GenericRecord, String](new CsvFn), TextIO.sink()).to(destination).withSuffix(".csv"))
      case other => throw new IllegalArgumentException(s"this adapter's jobs write parquet or csv, not '$other'")
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

  final class PositiveFn(field: String) extends SerializableFunction[GenericRecord, java.lang.Boolean] {
    override def apply(r: GenericRecord): java.lang.Boolean = java.lang.Boolean.valueOf(Option(r.get(field)).exists(_.toString.toLong > 0))
  }

  final class KeyFn(columns: String) extends SerializableFunction[GenericRecord, KV[String, GenericRecord]] {
    override def apply(r: GenericRecord): KV[String, GenericRecord] = KV.of(columns.split(",").map(c => String.valueOf(r.get(c))).mkString("|"), r)
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

  final class MergeFn(mergedJson: String, leftJson: String, rightJson: String) extends DoFn[KV[String, CoGbkResult], GenericRecord] {
    @ProcessElement def process(@DoFn.Element element: KV[String, CoGbkResult], out: DoFn.OutputReceiver[GenericRecord]): Unit = {
      val merged = new Schema.Parser().parse(mergedJson)
      val lefts = element.getValue.getAll(new TupleTag[GenericRecord]("l")).asScala
      val rights = element.getValue.getAll(new TupleTag[GenericRecord]("r")).asScala
      for (l <- lefts; r <- rights) out.output(joinRecords(merged, l, r))
    }
  }

  final class CrossFn(mergedJson: String, other: PCollectionView[java.util.List[GenericRecord]]) extends DoFn[GenericRecord, GenericRecord] {
    @ProcessElement def process(c: DoFn[GenericRecord, GenericRecord]#ProcessContext): Unit = {
      val merged = new Schema.Parser().parse(mergedJson)
      c.sideInput(other).asScala.foreach(r => c.output(joinRecords(merged, c.element(), r)))
    }
  }

  private def joinRecords(merged: Schema, left: GenericRecord, right: GenericRecord): GenericRecord = {
    val record = new GenericData.Record(merged)
    left.getSchema.getFields.asScala.foreach(f => record.put(s"l_${f.name}", left.get(f.name)))
    right.getSchema.getFields.asScala.foreach(f => record.put(s"r_${f.name}", right.get(f.name)))
    record
  }

  final class GroupFn(outJson: String, columns: List[ResolvedColumn]) extends DoFn[KV[String, java.lang.Iterable[GenericRecord]], GenericRecord] {
    @ProcessElement def process(@DoFn.Element element: KV[String, java.lang.Iterable[GenericRecord]], out: DoFn.OutputReceiver[GenericRecord]): Unit = {
      val records = element.getValue.asScala.toList
      out.output(recordOf(outJson, columns, records.head, records.size.toLong))
    }
  }

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
