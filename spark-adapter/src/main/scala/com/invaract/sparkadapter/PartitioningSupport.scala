// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, PartitionOverwriteModes}
import com.invaract.verification.{OutputChecker, UnverifiableOperation, WritePartitioning}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{StagedTable, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale
import scala.util.Try

/** `check.partitioning` on Spark: how a write lays out its data (the columns it partitions by, in order, and what an
  * overwrite replaces), for `CheckedWrite.partitioning` (docs/SPARK_ADAPTER.md, "Partitioning").
  *
  * Reported only where Spark itself states both, per write shape (`WriteCommandSupport` sets `WriteCommandInfo.partitioning`):
  *
  *   - a file-source write (`InsertIntoHadoopFsRelationCommand`: `.partitionBy(...).parquet(path)`, an insert into a data
  *     source table, and an insert into a Hive table Spark converts to its own file source): its `partitionColumns`, and
  *     Spark's own rule for a dynamic overwrite (`fileWrite`);
  *   - a new data source table (`CreateDataSourceTableAsSelectCommand`): the table's partition columns; it is created whole;
  *   - a `CreatableRelationProvider` write given `.partitionBy(...)` (`SaveIntoDataSourceCommand`, Delta's `.save(path)`):
  *     those columns, and the overwrite mode the write configured; a Delta `.save(path)` given none, the partitioning Delta
  *     writes with (`deltaSave`): the table's own, read from its log, none for a table not yet created, and none for an
  *     overwrite that replaces the table's schema (`overwriteSchema`);
  *   - a streaming write (`WriteToStream`) into a Delta sink (its `.partitionBy(...)`, else the table's own, as for a batch
  *     write), a file sink (its partition columns) or a V2 table partitioned by plain columns (`streamSink`);
  *   - a DataSource V2 write (`AppendData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`, `CreateTableAsSelect`,
  *     `ReplaceTableAsSelect`) whose table is partitioned by plain columns only: those columns; `OverwritePartitionsDynamic`
  *     replaces only the partitions written, the others replace what their condition (or the whole table) says.
  *
  * Every other shape (another streaming sink, a Hive serde write Spark does not convert, a `CreatableRelationProvider` write
  * other than Delta's with no `.partitionBy(...)`, a V2 table partitioned by a derived transform such as `days(ts)`, a
  * row-level change) reports
  * nothing, and a write of one onto an output whose contract declares partition keys is refused as unverifiable (`refusal`)
  * rather than passed unchecked.
  */
private[sparkadapter] object PartitioningSupport {

  /** The write option, and the session setting, that say what an overwrite of a partitioned dataset replaces. */
  val OverwriteModeOption = "partitionOverwriteMode"

  /** The overwrite mode a write configured: its own `partitionOverwriteMode` option (any case), else the session's
    * `spark.sql.sources.partitionOverwriteMode`, lower-cased. */
  def configuredMode(options: Map[String, String]): String =
    options
      .collectFirst { case (key, value) if key.equalsIgnoreCase(OverwriteModeOption) => value }
      .getOrElse(SQLConf.get.partitionOverwriteMode.toString)
      .trim
      .toLowerCase(Locale.ROOT)

  /** A file-source write partitioned by `columns`: Spark's own rule (`InsertIntoHadoopFsRelationCommand`) replaces only the
    * partitions written when the configured mode is dynamic and some partition column takes its value from the data, not from
    * a `PARTITION (...)` clause (`staticPartitions` of them do). A Hive table Spark converts to its file source is written
    * dynamically whatever the setting, because Spark puts the dynamic option on the converted relation itself. */
  def fileWrite(columns: List[String], options: Map[String, String], staticPartitions: Int): WritePartitioning = {
    val dynamic = configuredMode(options) == PartitionOverwriteModes.Dynamic && staticPartitions < columns.size
    WritePartitioning(columns, if (dynamic) PartitionOverwriteModes.Dynamic else PartitionOverwriteModes.Static)
  }

  /** A table partitioned by `transforms`, as partition columns: the columns, in order, when every transform is a plain
    * column (`identity`), and `None` when one is derived (`bucket(4, id)`, `days(ts)`), which a contract's partition keys
    * (columns) cannot name. */
  def identityColumns(transforms: Seq[Transform]): Option[List[String]] =
    if (transforms.forall(t => t.name == "identity" && t.references.length == 1))
      Some(transforms.map(_.references.head.fieldNames.mkString(".")).toList)
    else None

  /** How a Delta `.save(path)` given no `.partitionBy(...)` lays out its data: Delta keeps an existing table's partition
    * columns, except for an overwrite with `overwriteSchema`, which replaces them with none, and creates a new table with none.
    * `None` when the table's log cannot be read. */
  def deltaSave(location: String, saveMode: Option[String], options: Map[String, String]): Option[WritePartitioning] = {
    def option(name: String) = options.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v.trim.toLowerCase(Locale.ROOT) }
    val replacesSchema = saveMode.contains("overwrite") && option("overwriteSchema").contains("true")
    deltaTablePartitionColumns(location).map(columns => WritePartitioning(if (replacesSchema) Nil else columns, configuredMode(options)))
  }

  /** The partition columns of the Delta table at `location`, as its log says now: `Some(Nil)` when there is no table there
    * yet. `None` when the log cannot be read (Delta is not on the classpath, or the path cannot be opened). Read by
    * reflection, as `SparkAdapterListener.deltaVersionOf` reads it, so this module needs no Delta dependency; the snapshot is
    * brought up to date first (`update`, with its default arguments, whatever the Delta release names them). */
  def deltaTablePartitionColumns(location: String): Option[List[String]] =
    Try {
      val session = SparkSession.active
      val log = Class.forName("org.apache.spark.sql.delta.DeltaLog")
        .getMethod("forTable", classOf[SparkSession], classOf[org.apache.hadoop.fs.Path])
        .invoke(null, session, new org.apache.hadoop.fs.Path(location))
      val update = log.getClass.getMethods.filter(_.getName == "update").minBy(_.getParameterCount)
      val defaults = (1 to update.getParameterCount).map(i => log.getClass.getMethod(s"update$$default$$$i").invoke(log))
      val snapshot = update.invoke(log, defaults: _*)
      if (snapshot.getClass.getMethod("version").invoke(snapshot).asInstanceOf[java.lang.Long].longValue() < 0) Nil
      else {
        val metadata = snapshot.getClass.getMethod("metadata").invoke(snapshot)
        metadata.getClass.getMethod("partitionColumns").invoke(metadata).asInstanceOf[scala.collection.Seq[String]].toList
      }
    }.toOption

  /** How a streaming write into `sink` lays out its data: a Delta sink's `.partitionBy(...)` columns, else its table's
    * (`deltaTablePartitionColumns`, at `location`); a file sink's partition columns; a V2 table's plain partition columns (a
    * memory sink has none). Not another V1 sink's: `foreachBatch`, console and Kafka sinks state nothing.
    * A stream appends, so nothing is replaced: the overwrite mode is never compared (`static`). `None` for any other sink. */
  def streamSink(sink: AnyRef, location: String): Option[WritePartitioning] = {
    def field(name: String): Option[AnyRef] = Try {
      val f = sink.getClass.getDeclaredField(name)
      f.setAccessible(true)
      f.get(sink)
    }.toOption
    val columns: Option[List[String]] = sink.getClass.getSimpleName match {
      case "DeltaSink" =>
        Try(sink.getClass.getMethod("partitionColumns").invoke(sink).asInstanceOf[scala.collection.Seq[String]].toList).toOption
          .flatMap(given => if (given.nonEmpty) Some(given) else deltaTablePartitionColumns(location))
      case "FileStreamSink" => field("partitionColumnNames").map(_.asInstanceOf[scala.collection.Seq[String]].toList)
      case _ =>
        sink match {
          // a V1 sink (foreachBatch, console, Kafka) is a Table only to fit the API: its empty partitioning says nothing
          case _: org.apache.spark.sql.execution.streaming.Sink => None
          case t: Table if !t.isInstanceOf[StagedTable]         => identityColumns(t.partitioning().toSeq)
          case _                                               => None
        }
    }
    columns.map(WritePartitioning(_, PartitionOverwriteModes.Static))
  }

  /** Why `write` cannot be checked against `contract`, when the output it lands on declares partition keys and Spark does not
    * report how `write` partitions its data (`partitioning` is `None`). */
  def refusal(contract: Contract, write: com.invaract.ir.Write, partitioning: Option[WritePartitioning], shape: String): Option[UnverifiableOperation] =
    if (partitioning.isDefined) None
    else
      OutputChecker.expectedOutputFor(contract.outputs, OutputChecker.namesOf(write)).filter(_.partitionKeys.nonEmpty).map { output =>
        UnverifiableOperation(
          shape,
          s"output '${output.name}' declares partition keys (${output.partitionKeys.mkString(", ")}), and a $shape write to " +
            s"${write.dataset.location} does not report how it partitions its data, so its partitioning cannot be checked",
          "Write it with `.partitionBy(...)` through Spark's file source (`.parquet(path)`, `.csv(path)`, `.format(...).save(path)`), " +
            "into a table partitioned by plain columns, or with `.writeTo(...)`; or remove the partition keys from the contract's output."
        )
      }
}
