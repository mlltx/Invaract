// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, LogicalSchema, LogicalType}
import com.invaract.ir
import com.invaract.testkit.{ColumnSource, ConformanceAdapter, RecordingSink, RowChange, ScenarioJob, ScenarioOutcome}
import com.invaract.verification.{AdapterCapabilities, ContractViolationException, VerificationOptions}

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.nio.file.{Files, Path}

/** Spark as an adapter for the conformance kit: each neutral `ScenarioJob` becomes a real Spark job
  * on a local session, run through `ContractEnforcementRule.forContract` - the same builder
  * `InvaractSparkSessionExtension` registers - so what is judged is what a real job gets.
  *
  * The session is shared across scenarios (building one per scenario would dominate the suite's
  * time); which contract it enforces is held in a cell the injected check rule reads per plan, the
  * same arrangement `ContractEnforcementRuleSpec` uses. A scenario's inputs are materialized as empty
  * parquet datasets with the scenario's schema (every check is about shape, not values) while no
  * contract is active, so only the scenario's own write is checked.
  */
class SparkConformanceAdapter extends ConformanceAdapter with AutoCloseable {

  private val scratch: Path = Files.createTempDirectory("invaract-conformance")

  @volatile private var active: Option[LogicalPlan => Unit] = None

  private lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkConformanceAdapter")
      .config("spark.sql.warehouse.dir", scratch.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      // Delta, for the row-level DML scenarios (Spark has no row-level DELETE on a plain parquet table). It
      // changes nothing for the scenarios that write parquet/csv/json by path.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .withExtensions(_.injectCheckRule(_ => (plan: LogicalPlan) => active.foreach(_(plan))))
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    // For the scenarios that run a job through a checkpoint (Spark's lineage-erasing point).
    session.sparkContext.setCheckpointDir(scratch.resolve("checkpoints").toString)
    session
  }

  override def capabilities: AdapterCapabilities =
    SparkCapabilities.declared.getOrElse(throw new IllegalStateException("Spark's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome =
    if (job.untranslatableWrite) runUntranslatable(scenarioId, contract, options)
    else if (job.streaming) runStreaming(scenarioId, contract, job, options)
    else
      job.rowChange match {
        case Some(change) => runRowChange(scenarioId, contract, job, change, options)
        case None         => runTransform(scenarioId, contract, job, options)
      }

  /** The ordinary job: read the inputs, filter, join, project, write. The reads and the transformation are built
    * inside the enforced block: a job that goes through a checkpoint is only seen through if the check rule saw the
    * plan the checkpoint was made from, which happens when that plan is analyzed.
    */
  private def runTransform(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    materializeInputs(scenarioId, job)
    val target = path(scenarioId, job.output.location)
    // A write that registers a catalog table is an append onto a table that already exists: Spark plans a
    // `.saveAsTable()` that creates a new table as an outer command plus a nested insert that carries no
    // catalog identity, which a `catalog.required` contract then rejects (docs/SPARK_ADAPTER.md, "Known limitations").
    // So the table is created first, with no contract active, and only the registered write is checked.
    job.output.registeredAs.foreach { table =>
      resultFrame(job, readInputs(scenarioId, job)).limit(0).write.format(job.output.format).mode("overwrite").option("path", target).saveAsTable(table)
    }
    enforced(contract, options) {
      val result = resultFrame(job, readInputs(scenarioId, job), materialize = job.boundary)
      val writer = result.write.format(job.output.format).mode(job.output.saveMode)
      job.output.registeredAs match {
        case Some(table) => writer.insertInto(table)
        case None        => writer.save(target)
      }
    }
  }

  /** The same job as a stream: the input is read with `readStream`, and the output is a stream sink, run to completion
    * (`availableNow`) over what exists. The check rule sees the streaming write when the query is started, so a
    * violation is thrown from `start()`, before any data moves.
    */
  private def runStreaming(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    materializeInputs(scenarioId, job)
    val target = path(scenarioId, job.output.location)
    val checkpoint = scratch.resolve(scenarioId).resolve("_checkpoint").toString
    enforced(contract, options) {
      val frames = job.inputs.map(i => spark.readStream.schema(structType(i.schema)).parquet(path(scenarioId, i.location)))
      val query = resultFrame(job, frames).writeStream
        .format(job.output.format)
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.AvailableNow())
        .start(target)
      query.awaitTermination()
    }
  }

  /** What the enforcement rule would be handed for `job`: the job's plan, translated, as a write to its output. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[ir.Plan] =
    if (job.untranslatableWrite || job.rowChange.isDefined || job.streaming) None
    else {
      materializeInputs(scenarioId, job)
      val result = resultFrame(job, readInputs(scenarioId, job))
      Some(SparkPlanAdapter.translateAsWrite(result.queryExecution.analyzed, ir.DatasetRef(path(scenarioId, job.output.location))).plan)
    }

  /** The frame a transform job writes: join, filter, then (when asked) the checkpoint every later step reads
    * through, then the projection. After a checkpoint the columns are those of the checkpointed frame, so a job
    * with a boundary reads one input (the projection refers to it by name).
    */
  private def resultFrame(job: ScenarioJob, frames: List[DataFrame], materialize: Boolean = false): DataFrame = {
    val joined = job.join.fold(frames.head)(j => frames(0).join(frames(1), frames(0)(j.leftColumn) === frames(1)(j.rightColumn)))
    val filtered = job.filterColumn.fold(joined)(c => joined.filter(frames(0)(c) > 0))
    if (materialize) {
      require(job.inputs.size == 1, "a conformance job with a boundary reads one input")
      val checkpointed = filtered.checkpoint()
      checkpointed.select(selectColumns(job, List(checkpointed)): _*)
    } else filtered.select(selectColumns(job, frames): _*)
  }

  /** Spark's representative of "a data-changing operation the adapter has no translation for": `TRUNCATE TABLE` on a
    * managed table, which Catalyst plans as a command `SparkPlanAdapter` does not turn into a write and that is not
    * on the known-safe list. The table is created while no contract is active, so only the truncate is checked.
    */
  private def runUntranslatable(scenarioId: String, contract: Contract, options: VerificationOptions): ScenarioOutcome = {
    val table = tableName(scenarioId)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $table (id BIGINT) USING parquet")
    enforced(contract, options)(spark.sql(s"TRUNCATE TABLE $table"))
  }

  /** Spark's representative of a row-level change: a Delta `DELETE FROM` on a catalog table over a Delta path.
    * The table is created first with the job's own columns while no contract is active (the neutral job's
    * inputs are materialized for that, as for any job), so only the DELETE itself is checked. A catalog
    * table, not `delta.`path``: a path-based DELETE has no catalog storage location to report, so it is the
    * best-effort case (docs/SPARK_ADAPTER.md), not the one this scenario is about.
    */
  private def runRowChange(scenarioId: String, contract: Contract, job: ScenarioJob, change: RowChange, options: VerificationOptions): ScenarioOutcome = {
    val target = path(scenarioId, job.output.location)
    materializeInputs(scenarioId, job)
    val frames = readInputs(scenarioId, job)
    frames.head.select(selectColumns(job, frames): _*).write.format("delta").mode("overwrite").save(target)
    val table = tableName(scenarioId)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING delta LOCATION '${target.replace('\\', '/')}'")
    val where = change match {
      case RowChange.UnconditionalDelete => ""
      case RowChange.FilteredDelete      => " WHERE id > 0"
    }
    enforced(contract, options)(spark.sql(s"DELETE FROM $table$where").collect())
  }

  /** Runs `body` with the contract enforced - the same rule `InvaractSparkSessionExtension` installs - and reports
    * what happened: a clean run is `Passed`, a `ContractViolationException` is `Rejected` with its violation types.
    * Everything the scenario needs to exist is created before this, while no contract is active.
    */
  private def enforced(contract: Contract, options: VerificationOptions)(body: => Unit): ScenarioOutcome = {
    val sink = new RecordingSink
    active = Some(ContractEnforcementRule.forContract(contract, options, sink)(spark))
    try {
      body
      sink.passed
    } catch {
      case e: ContractViolationException => sink.rejected(e.result.violations.map(_.violationType).toSet)
    } finally active = None
  }

  /** Writes the scenario's inputs as empty parquet datasets of their schema (every check is about shape, not
    * values). Done while no contract is active.
    */
  private def materializeInputs(scenarioId: String, job: ScenarioJob): Unit =
    job.inputs.foreach { input =>
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], structType(input.schema)).write.mode("overwrite").parquet(path(scenarioId, input.location))
    }

  /** The materialized inputs, read back as the frames the job works on. */
  private def readInputs(scenarioId: String, job: ScenarioJob): List[DataFrame] =
    job.inputs.map(i => spark.read.parquet(path(scenarioId, i.location)))

  private def selectColumns(job: ScenarioJob, frames: List[DataFrame]): List[Column] =
    job.columns.map { c =>
      (c.source match {
        case ColumnSource.FromInput(i, col)    => frames(i)(col)
        case ColumnSource.CastInput(i, col, t) => frames(i)(col).cast(DataType.fromDDL(t.catalogString))
        case ColumnSource.NonNullLong(v)       => lit(v)
        case ColumnSource.UniqueId             => expr("uuid()")
      }).as(c.name)
    }

  private def tableName(scenarioId: String): String = "conformance_" + scenarioId.replace('-', '_')

  private def path(scenarioId: String, location: String): String = scratch.resolve(scenarioId).resolve(location).toString

  private def structType(schema: LogicalSchema): StructType =
    StructType(schema.fields.map(f => StructField(f.name, DataType.fromDDL(f.dataType.catalogString), f.nullable)))

  override def close(): Unit = {
    if (spark != null) spark.stop()
  }
}
