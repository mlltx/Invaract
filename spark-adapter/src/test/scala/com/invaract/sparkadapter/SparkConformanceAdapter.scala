// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, LogicalSchema, LogicalType}
import com.invaract.testkit.{ColumnSource, ConformanceAdapter, RecordingSink, ScenarioJob, ScenarioOutcome}
import com.invaract.verification.{AdapterCapabilities, ContractViolationException, VerificationOptions}

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{expr, lit}
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
      .withExtensions(_.injectCheckRule(_ => (plan: LogicalPlan) => active.foreach(_(plan))))
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  override def capabilities: AdapterCapabilities =
    SparkCapabilities.declared.getOrElse(throw new IllegalStateException("Spark's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    val base = scratch.resolve(scenarioId)
    def path(location: String): String = base.resolve(location).toString

    if (job.untranslatableWrite) return runUntranslatable(scenarioId, contract, options)

    job.inputs.foreach { input =>
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], structType(input.schema))
      empty.write.mode("overwrite").parquet(path(input.location))
    }

    val sink = new RecordingSink
    active = Some(ContractEnforcementRule.forContract(contract, options, sink)(spark))
    try {
      val frames = job.inputs.map(i => spark.read.parquet(path(i.location)))
      val joined = job.join.fold(frames.head)(j => frames(0).join(frames(1), frames(0)(j.leftColumn) === frames(1)(j.rightColumn)))
      val filtered = job.filterColumn.fold(joined)(c => joined.filter(frames(0)(c) > 0))
      val columns: List[Column] = job.columns.map { c =>
        (c.source match {
          case ColumnSource.FromInput(i, col)    => frames(i)(col)
          case ColumnSource.CastInput(i, col, t) => frames(i)(col).cast(DataType.fromDDL(t.catalogString))
          case ColumnSource.NonNullLong(v)       => lit(v)
          case ColumnSource.UniqueId             => expr("uuid()")
        }).as(c.name)
      }
      filtered.select(columns: _*).write.format(job.output.format).mode(job.output.saveMode).save(path(job.output.location))
      ScenarioOutcome.Passed(sink.statuses, sink.nonDeterministicOutputs)
    } catch {
      case e: ContractViolationException => ScenarioOutcome.Rejected(e.result.violations.map(_.violationType).toSet, sink.statuses, sink.nonDeterministicOutputs)
    } finally active = None
  }

  /** Spark's representative of "a data-changing operation the adapter has no translation for": `TRUNCATE TABLE` on a
    * managed table, which Catalyst plans as a command `SparkPlanAdapter` does not turn into a write and that is not
    * on the known-safe list. The table is created while no contract is active, so only the truncate is checked.
    */
  private def runUntranslatable(scenarioId: String, contract: Contract, options: VerificationOptions): ScenarioOutcome = {
    val table = "conformance_" + scenarioId.replace('-', '_')
    spark.sql(s"CREATE TABLE IF NOT EXISTS $table (id BIGINT) USING parquet")
    val sink = new RecordingSink
    active = Some(ContractEnforcementRule.forContract(contract, options, sink)(spark))
    try {
      spark.sql(s"TRUNCATE TABLE $table")
      ScenarioOutcome.Passed(sink.statuses, sink.nonDeterministicOutputs)
    } catch {
      case e: ContractViolationException => ScenarioOutcome.Rejected(e.result.violations.map(_.violationType).toSet, sink.statuses, sink.nonDeterministicOutputs)
    } finally active = None
  }

  private def structType(schema: LogicalSchema): StructType =
    StructType(schema.fields.map(f => StructField(f.name, DataType.fromDDL(f.dataType.catalogString), f.nullable)))

  override def close(): Unit = {
    if (spark != null) spark.stop()
  }
}
