// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/** Closes the gap `StructuralVerifier`'s "Inputs hidden behind a lineage
  * boundary" section discloses as a heuristic: its column-overlap check
  * (`coveringUnknownPlans`) guesses which declared input a `.checkpoint()`
  * boundary might represent from the checkpoint's own *output* columns —
  * a guess that breaks the moment a checkpoint follows a multi-input join
  * with a column-dropping `.select()` (a normal, even encouraged pattern —
  * shrinking what gets persisted before a checkpoint), since the
  * checkpoint's own output no longer contains any single input's full
  * declared field set.
  *
  * This tracker replaces guessing with observation. `Dataset.checkpoint()`/
  * `.localCheckpoint()` both fire `QueryExecutionListener.onSuccess("checkpoint" |
  * "localCheckpoint", qe, _)` with `qe` being the *real, pre-checkpoint*
  * `QueryExecution` — the join, with its original `Read` nodes still
  * intact, before Spark's own checkpoint substitution ever erases them.
  * Confirmed directly against Spark 3.5.7's real bytecode (`Dataset.checkpoint(boolean,
  * boolean)` calls `withAction("checkpoint" | "localCheckpoint", qe, ...)`,
  * the exact same action-wrapping helper every other action uses to fire
  * this listener) and against a real, running session, not assumed.
  *
  * ## Why this doesn't try to correlate a capture to a specific downstream node
  *
  * An earlier design attempted to link a captured checkpoint execution to
  * the *specific* `ir.UnknownPlan`/`LogicalRDD` node it becomes in a later
  * write's plan, via the underlying RDD's `.id`. Confirmed empirically that
  * this doesn't work: the RDD Spark actually embeds in the checkpoint's
  * result is `qe.executedPlan.execute().map(...)` — a `.map()`'d RDD
  * created entirely inside `Dataset.checkpoint()`'s own private
  * implementation, never exposed to any listener or re-derivable from
  * outside it (re-executing `qe.executedPlan` produces a *different* RDD
  * object with a different id). Node-level correlation like that is real
  * lineage reconstruction — genuinely needed for fingerprinting a
  * transformation graph, not for this. Contract validation only needs a
  * much narrower, per-input membership question: "was this declared
  * input's own location genuinely read by *some* checkpoint this session
  * executed?" — answerable by accumulating every real `Read` location this
  * tracker observes, session-wide, with no node identity required at all.
  *
  * `StructuralVerifier.verify` still requires the plan being checked to
  * contain at least one checkpoint-shaped `ir.UnknownPlan` of its own
  * before treating a location observed here as corroborating evidence —
  * this registry alone is never sufficient, exactly so an unrelated
  * checkpoint captured for one write can't silently excuse a different,
  * genuinely missing input on a completely unrelated write later in the
  * same session. See that method's own doc for the full two-signal design.
  *
  * ## Lifecycle and thread-safety
  *
  * One instance per session, registered once by `ContractEnforcementRule.forContract`
  * (see that method's own doc) — its `observedLocations` accumulate for as
  * long as the session lives, the same session-scoped lifetime
  * `SparkAdapterListener` already has. `onSuccess` fires on Spark's
  * asynchronous listener-bus thread, confirmed directly (not assumed) via a
  * real running session — a few milliseconds after `.checkpoint()` returns
  * to the caller's own thread, not before it. `@volatile` plus a pure
  * immutable-`Set` swap (the same pattern `SparkAdapterListener._lastWrite`
  * already uses for its own cross-thread field) makes a read from the
  * check-rule's thread always see either the old or the fully-updated set,
  * never a partial one — but a write's own check can still race a
  * checkpoint whose completion event hasn't been processed yet. Disclosed,
  * not hidden: `StructuralVerifier`'s column-overlap heuristic remains this
  * design's fallback for exactly that narrow timing window, not just for
  * a non-checkpoint `UnknownPlan`.
  */
private[sparkadapter] class CheckpointLineageTracker extends QueryExecutionListener {
  @volatile private var _observedLocations: Set[String] = Set.empty

  /** Every real `Read` location observed feeding a `.checkpoint()`/
    * `.localCheckpoint()` call this session has executed so far — a
    * monotonically growing set for the tracker's whole lifetime, never
    * pruned (see class doc: this is a per-session accumulation, not a
    * per-write snapshot; `StructuralVerifier.verify`'s own requirement that
    * the *checked* plan itself contain a checkpoint-shaped `UnknownPlan` is
    * what keeps an old entry from wrongly excusing an unrelated later
    * write).
    */
  def observedLocations: Set[String] = _observedLocations

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    if (funcName == "checkpoint" || funcName == "localCheckpoint") {
      val preCheckpointPlan = SparkPlanAdapter.translate(qe.analyzed).plan
      val locations = StructuralVerifier.collectReads(preCheckpointPlan).map(_.dataset.location).toSet
      if (locations.nonEmpty) _observedLocations = _observedLocations ++ locations
    } // else some other action (count, collect, schema inference, ...) - not this tracker's concern

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}
