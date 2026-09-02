// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.execution.QueryExecution
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

  // Consults the same WriteCommandSupport.combined lookup
  // SparkPlanAdapter's translation and ContractEnforcementRule's
  // enforcement do, rather than a match of its own - this listener used
  // to have its own independent "is this a write" check, and that's
  // exactly what let a real write shape (Delta's) go uncaptured here even
  // after translation and enforcement were both already fixed for it. See
  // WriteCommandSupport's class doc.
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    if (WriteCommandSupport.combined.isDefinedAt(qe.analyzed)) {
      _lastWrite = Some(SparkPlanAdapter.translate(qe.analyzed))
    } // else not a write; ignore (schema inference, count(), etc.)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}
