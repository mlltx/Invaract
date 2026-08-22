// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.util.QueryExecutionListener

/** A `QueryExecutionListener` that captures the translated IR for every
  * Spark write command a session executes, without requiring any change to
  * how those writes are called elsewhere in the application.
  *
  * Registered once via `spark.listenerManager.register(listener)` — see
  * `SparkPlanAdapter`'s class doc for why this is the least invasive of
  * Spark's plan-inspection extension points for this purpose. `onSuccess`
  * fires for *every* query a session executes, including internal ones
  * triggered by schema inference, `.count()`, and so on, so this listener
  * only records executions whose analyzed plan root is a write command;
  * everything else is ignored.
  */
class SparkAdapterListener extends QueryExecutionListener {
  @volatile private var _lastWrite: Option[TranslationResult] = None

  /** The most recently captured write's translation, if any query with a
    * write command at its root has executed since this listener was
    * registered. A single most-recent value is enough for this listener's
    * purpose — driving a demo/report of "what did the last write do" — not
    * a general-purpose multi-query capture store.
    */
  def lastWrite: Option[TranslationResult] = _lastWrite

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    qe.analyzed match {
      case _: InsertIntoHadoopFsRelationCommand =>
        _lastWrite = Some(SparkPlanAdapter.translate(qe.analyzed))
      // Delta (and any other CreatableRelationProvider-based source
      // written via `.save(...)`) analyzes to this command instead - see
      // SparkPlanAdapter's SaveIntoDataSourceCommand case and
      // docs/SPARK_ADAPTER.md's "Delta Lake support" section. This
      // listener has its own independent "is this a write" check from
      // SparkPlanAdapter's and ContractEnforcementRule's, confirmed the
      // hard way when adding Delta support here required fixing all three
      // separately, not just SparkPlanAdapter.translatePlan.
      case _: SaveIntoDataSourceCommand =>
        _lastWrite = Some(SparkPlanAdapter.translate(qe.analyzed))
      case _ => // not a write; ignore (schema inference, count(), etc.)
    }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}
