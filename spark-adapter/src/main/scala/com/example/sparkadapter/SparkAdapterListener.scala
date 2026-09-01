// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.Contract
import com.example.sparkadapter.notification.{NotificationSink, WriteEvent, WriteFieldInfo}

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
  *
  * If `sink` is given, this is also where `WriteEvent`s are published —
  * deliberately here, not in `ContractEnforcementRule`: that check rule
  * fires at analysis time, before Spark has executed anything, so it can
  * only report "this write is allowed to proceed," not "this write
  * actually happened." `onSuccess` is Spark's own "this query executed
  * successfully" signal, so a `WriteEvent` published from here means the
  * output genuinely exists on disk — see `ContractValidationEvent`'s own
  * doc for the contrasting, earlier moment it represents.
  */
class SparkAdapterListener(
    sink: Option[NotificationSink],
    contractRef: Option[String],
    metadata: Map[String, Any]
) extends QueryExecutionListener {
  @volatile private var _lastWrite: Option[TranslationResult] = None

  /** No sink, no contract — a listener used purely for its original
    * purpose (capturing `lastWrite` for `demo/output/report.json`), the
    * same zero-argument constructor this class had before `WriteEvent`
    * publishing existed.
    */
  def this() = this(None, None, Map.empty)

  /** `contractRef`/`metadata` derived from a real `Contract` (its
    * `"id@version"` ref and `extensions` bag, respectively) — the
    * convenience most callers that do have a contract will actually want,
    * rather than pulling those two fields out by hand.
    */
  def this(sink: Option[NotificationSink], contract: Option[Contract]) =
    this(sink, contract.map(c => s"${c.id}@${c.version}"), contract.map(_.extensions).getOrElse(Map.empty))

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
  // WriteCommandSupport's class doc. `.lift` (rather than the previous
  // `.isDefinedAt` + a second, independent `SparkPlanAdapter.translate`
  // call) gets the WriteCommandInfo this case also needs for the
  // WriteEvent below, at no extra cost - `SparkPlanAdapter.translate`
  // still runs its own full recursive translation for `_lastWrite`
  // exactly as before.
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    WriteCommandSupport.combined.lift(qe.analyzed).foreach { info =>
      _lastWrite = Some(SparkPlanAdapter.translate(qe.analyzed))
      sink.foreach { s =>
        s.publish(
          WriteEvent(
            contract = contractRef,
            location = info.location,
            format = info.format,
            saveMode = info.saveMode,
            schema = info.outputSchema.fields.map(f => WriteFieldInfo(f.name, f.dataType.typeName, f.nullable)).toList,
            timestamp = System.currentTimeMillis(),
            metadata = metadata
          )
        )
      }
    } // else not a write; ignore (schema inference, count(), etc.)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}
