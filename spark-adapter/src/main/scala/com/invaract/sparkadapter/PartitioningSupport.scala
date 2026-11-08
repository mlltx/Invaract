// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, PartitionOverwriteModes}
import com.invaract.verification.{OutputChecker, UnverifiableOperation, WritePartitioning}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale

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
  *     those columns, and the overwrite mode the write configured;
  *   - a DataSource V2 write (`AppendData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`, `CreateTableAsSelect`,
  *     `ReplaceTableAsSelect`) whose table is partitioned by plain columns only: those columns; `OverwritePartitionsDynamic`
  *     replaces only the partitions written, the others replace what their condition (or the whole table) says.
  *
  * Every other shape (a streaming sink, a Hive serde write Spark does not convert, a `CreatableRelationProvider` write with
  * no `.partitionBy(...)`, a V2 table partitioned by a derived transform such as `days(ts)`, a row-level change) reports
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
