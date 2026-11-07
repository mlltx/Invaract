// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.streaming.StreamingRelation

/** `check.csvOptions` on Spark: the CSV options each CSV read and write of a plan uses, by location, for
  * `CheckedWrite.formatOptions` (docs/SPARK_ADAPTER.md, "CSV options").
  *
  * What is reported is what Spark reads or writes with: the option the job set, under the contract's name for it
  * (`sep` is `delimiter`, `charset` is `encoding`, as Spark's own `CSVOptions` reads them, `sep` and `encoding` first),
  * and Spark's own default for each one it did not set. A contract declaring `header: true` against a job that never set
  * `header` is therefore a mismatch, as it is in fact: Spark reads and writes without a header line by default.
  *
  * The shapes covered are the ones Spark's file source plans: a read of CSV files (`HadoopFsRelation`, by path or
  * through a `USING csv` table, and a streaming file source), a write to a path or into an existing table
  * (`InsertIntoHadoopFsRelationCommand`) and a `CREATE TABLE ... USING csv AS SELECT`. A streaming CSV sink's options
  * are not reported, so they are not compared (declared in invaract-capabilities-spark.yaml).
  */
private[sparkadapter] object CsvOptionsSupport {

  /** Spark's own value for each option a job leaves unset (Spark's CSV data source options). */
  val SparkDefaults: Map[String, String] = Map(
    "delimiter"       -> ",",
    "header"          -> "false",
    "encoding"        -> "UTF-8",
    "quote"           -> "\"",
    "escape"          -> "\\",
    "nullValue"       -> "",
    "dateFormat"      -> "yyyy-MM-dd",
    "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"
  )

  /** Each option's Spark spellings, the one Spark reads first first (option keys are case-insensitive). */
  private val Spellings: List[(String, List[String])] = List(
    "delimiter"       -> List("sep", "delimiter"),
    "header"          -> List("header"),
    "encoding"        -> List("encoding", "charset"),
    "quote"           -> List("quote"),
    "escape"          -> List("escape"),
    "nullValue"       -> List("nullvalue"),
    "dateFormat"      -> List("dateformat"),
    "timestampFormat" -> List("timestampformat")
  )

  /** The options the job set, under the contract's names. */
  def explicit(options: Map[String, String]): Map[String, String] = {
    val lowered = options.map { case (k, v) => k.toLowerCase -> v }
    Spellings.flatMap { case (name, keys) => keys.collectFirst { case k if lowered.contains(k) => name -> lowered(k) } }.toMap
  }

  /** The options Spark reads or writes with: the job's, and Spark's default for the rest. */
  def effective(options: Map[String, String]): Map[String, String] = SparkDefaults ++ explicit(options)

  /** What a dry run's draft declares: the options the job set, and `header`, whose default differs between engines. */
  def drafted(options: Map[String, String]): Map[String, String] =
    explicit(options) + ("header" -> effective(options)("header"))

  private def isCsv(provider: AnyRef): Boolean = SparkPlanAdapter.formatOf(provider).contains("csv")

  /** The options of the CSV write `plan` is, when it is one. */
  def ofWrite(plan: LogicalPlan): Option[Map[String, String]] = plan match {
    case cmd: InsertIntoHadoopFsRelationCommand if isCsv(cmd.fileFormat)                          => Some(cmd.options)
    case cmd: CreateDataSourceTableAsSelectCommand if cmd.table.provider.exists(_.equalsIgnoreCase("csv")) =>
      Some(cmd.table.storage.properties)
    case _ => None
  }

  /** Every CSV read in `plans`, by the location the read is checked under, with the options it used. */
  def ofReads(plans: Seq[LogicalPlan]): Map[String, Map[String, String]] =
    plans.flatMap(_.collect {
      case lr @ LogicalRelation(h: HadoopFsRelation, _, _, _) if isCsv(h.fileFormat) => SparkPlanAdapter.locationOf(lr) -> h.options
      case sr: StreamingRelation if sr.dataSource.className.equalsIgnoreCase("csv") =>
        SparkPlanAdapter.streamingRelationLocationOf(sr) -> sr.dataSource.options
    }).toMap

  /** `CheckedWrite.formatOptions` for `plan`, a write to `writeLocation` (when it is one) reading through `plans`: what
    * each CSV read and write used, Spark's defaults filled in. */
  def used(plan: LogicalPlan, writeLocation: Option[String], plans: Seq[LogicalPlan]): Map[String, Map[String, String]] =
    collect(plan, writeLocation, plans, effective)

  /** `InferredWrite.formatOptions`: what a dry run's draft declares for each CSV read and write (`drafted`). */
  def forDraft(plan: LogicalPlan, writeLocation: Option[String], plans: Seq[LogicalPlan]): Map[String, Map[String, String]] =
    collect(plan, writeLocation, plans, drafted)

  private def collect(
      plan: LogicalPlan,
      writeLocation: Option[String],
      plans: Seq[LogicalPlan],
      as: Map[String, String] => Map[String, String]
  ): Map[String, Map[String, String]] =
    ofReads(plans).map { case (location, options) => location -> as(options) } ++
      (for { location <- writeLocation; options <- ofWrite(plan) } yield location -> as(options))
}
