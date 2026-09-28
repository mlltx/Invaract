// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD

import java.lang.ref.SoftReference

/** Sees through a `.checkpoint()` boundary synchronously, so verification and
  * fingerprinting both work on the real transformation instead of an opaque
  * `ir.UnknownPlan`.
  *
  * ## Why this works
  *
  * `Dataset.checkpoint()`/`.localCheckpoint()` builds its result as a
  * `LogicalRDD` whose `output` is the *original* dataset's own analyzed
  * output attributes — same `exprId`s, unchanged (confirmed against a real
  * Spark 3.5.7 session, including through a chained second checkpoint). And
  * `ContractEnforcementRule`'s check rule already sees every plan Spark
  * analyzes, *synchronously, at the moment each Dataset is created* — so it
  * has seen the pre-checkpoint plan, with its real `Read` nodes, before
  * `.checkpoint()` is ever called, and it sees the checkpointed Dataset
  * itself (a bare `LogicalRDD`) the instant `.checkpoint()` returns.
  *
  *  - `record` remembers each analyzed plan under its output attribute ids.
  *  - `bind`, called when the rule first sees a bare `LogicalRDD`, snapshots
  *    whichever plan is recorded under that leaf's ids *at that moment* —
  *    the plan that was just checkpointed — and ties the snapshot to that
  *    leaf instance (a `WeakHashMap`: the snapshot lives exactly as long as
  *    the checkpointed Dataset does). The same leaf instance flows unchanged
  *    into every plan later built from that Dataset (`.filter`, `.select`,
  *    a SQL view over it), so binding once at creation is what makes
  *    resolution exact even when later plans reuse the same output ids
  *    (`ck.filter(...)` shares `ck`'s ids; an iterative
  *    `df = df.filter(...).checkpoint()` loop checkpoints several plans
  *    under one set of ids).
  *  - `substitute` replaces each bound `LogicalRDD` in a plan with its
  *    snapshot (recursively, for chained checkpoints).
  *
  * The substitution happens on the *Catalyst* plan, before translation, so
  * the result is structurally the plan the job would have had with no
  * checkpoint at all — same attribute ids, same alias disambiguation, same
  * translation. That is what lets both contract verification (the real
  * `Read` nodes are simply there) and fingerprinting (a checkpointed job
  * fingerprints like its un-checkpointed twin) see through the boundary.
  *
  * Nothing here is asynchronous, so there is no timing window (an earlier
  * design captured checkpoints from a `QueryExecutionListener`, which fires
  * on Spark's listener-bus thread *after* `.checkpoint()` returns), and the
  * evidence is per-checkpoint — only checkpoints inside the plan being
  * checked are ever resolved — rather than a session-wide set of everything
  * ever checkpointed.
  *
  * ## What it can't do
  *
  *  - Attribute `exprId`s identify a plan's *output*, not the plan: two
  *    different plans can share the same output ids (a dataset and a
  *    `.filter(...)` of it do), and `bind` takes the most recent one. That is
  *    the checkpointed plan in every ordinary flow, but if several plans
  *    sharing the ids read *different* datasets the choice is genuinely
  *    ambiguous, and the boundary is left unresolved; if they read the same
  *    datasets the most recent is used and a diagnostic says so, so the
  *    fingerprint side can disclose it.
  *  - Only a checkpoint whose creation the rule observed can be resolved
  *    (the rule must have been installed when the Dataset was made). A copy
  *    Spark makes with fresh attribute ids - the right side of a self-join
  *    of a checkpointed Dataset - is unresolved too.
  *  - Bounded (LRU, `maxEntries`), and the plans are held by `SoftReference`
  *    (a Catalyst plan keeps its relations - and a file index's listing - alive,
  *    which must not be pinned in a long-lived session): an evicted,
  *    collected or never-seen origin (a Dataset created before the rule was
  *    installed, or in another session) stays unresolved — the same
  *    fail-safe as before this registry existed.
  *  - Substitution walks the plan's own tree, so a `LogicalRDD` hidden inside
  *    a node that keeps its query outside `children` (Delta's row-level DML
  *    commands) is not reached and stays unresolved.
  *  - Per rule instance, i.e. per session state: a cloned session builds its
  *    own rule and starts empty.
  */
private[sparkadapter] final class CheckpointRegistry(maxEntries: Int = CheckpointRegistry.DefaultMaxEntries) {
  import CheckpointRegistry._

  private val entries = new java.util.LinkedHashMap[List[Long], Entry](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[List[Long], Entry]): Boolean = size() > maxEntries
  }

  // Keyed by the LogicalRDD leaf itself (structural equality includes its
  // `rdd`, so two checkpoints never collide), weakly: a snapshot dies with
  // the checkpointed Dataset that owns the leaf.
  private val bound = java.util.Collections.synchronizedMap(new java.util.WeakHashMap[LogicalRDD, Entry]())

  /** Called with every analyzed plan the rule sees, *before* `substitute`: when
    * `plan` is a bare `LogicalRDD` - the Dataset `.checkpoint()` just returned -
    * ties it to the plan currently recorded under its output ids.
    */
  def bind(plan: LogicalPlan): Unit = plan match {
    case leaf: LogicalRDD =>
      if (!bound.containsKey(leaf)) lookup(leaf).foreach(entry => bound.put(leaf, entry))
    case _ => ()
  }

  /** Records `plan` - the analyzed plan *after* `substitute`, so it holds real
    * reads rather than resolvable checkpoints - under its output attribute
    * ids; `translated` is its ir form.
    */
  def record(plan: LogicalPlan, translated: ir.Plan): Unit = {
    val key = plan.output.map(_.exprId.id).toList
    // A plan that still contains an UNRESOLVED checkpoint carrying its own
    // output ids (the bare `LogicalRDD` a `.checkpoint()` returns, or a
    // `.filter(...)` of one - a filter preserves its input's ids) is derived
    // FROM that checkpoint, never the pre-image it would have to resolve to:
    // recording it could only be followed back into itself.
    val derivedFromItself = plan.exists { case leaf: LogicalRDD => idsOf(leaf) == key; case _ => false }
    if (!derivedFromItself) {
      if (key.nonEmpty) {
        val reads = StructuralVerifier.collectReads(translated).map(_.dataset.location).toSet
        entries.synchronized {
          val merged = Option(entries.get(key)) match {
            case None => Entry(new SoftReference(plan), translated, reads, readsAmbiguous = false, shapeAmbiguous = false)
            case Some(existing) if existing.translated == translated => existing
            case Some(existing) =>
              Entry(
                new SoftReference(plan),
                translated,
                reads,
                readsAmbiguous = existing.readsAmbiguous || existing.reads != reads,
                shapeAmbiguous = true
              )
          }
          entries.put(key, merged)
        }
      }
    }
  }

  /** `plan` with every resolvable `LogicalRDD` replaced by the plan it was
    * made from (recursively, for chained checkpoints), plus diagnostics for
    * any resolution that had to be approximated or refused.
    */
  def substitute(plan: LogicalPlan): Substitution = {
    val diagnostics = scala.collection.mutable.ListBuffer.empty[Diagnostic]
    // `inProgress` holds the checkpoints currently being expanded: an entry
    // that (through a chain of plans) would expand back into itself is
    // refused rather than followed forever.
    def go(p: LogicalPlan, inProgress: Set[List[Long]]): LogicalPlan = p.transformUp {
      case leaf: LogicalRDD if !inProgress.contains(idsOf(leaf)) =>
        Option(bound.get(leaf)) match {
          case Some(entry) if entry.readsAmbiguous =>
            diagnostics += Diagnostic(
              "LogicalRDD",
              "Several plans reading different sources share this .checkpoint() boundary's output columns, so its " +
                "origin could not be determined"
            )
            leaf
          case Some(entry) =>
            Option(entry.plan.get) match {
              case None => leaf // collected under memory pressure
              case Some(preImage) =>
                if (entry.shapeAmbiguous)
                  diagnostics += Diagnostic(
                    ResolutionDiagnosticType,
                    "A .checkpoint() boundary was resolved to the most recently created of several plans sharing its " +
                      "output columns and reading the same sources; the transformation between those sources and this " +
                      "point is assumed to be that plan's"
                  )
                go(preImage, inProgress + idsOf(leaf))
            }
          case None => leaf
        }
    }
    val result = go(plan, Set.empty)
    Substitution(result, diagnostics.toList)
  }

  private def lookup(leaf: LogicalRDD): Option[Entry] = entries.synchronized(Option(entries.get(idsOf(leaf))))
}

private[sparkadapter] object CheckpointRegistry {
  val DefaultMaxEntries = 256

  private def idsOf(plan: LogicalPlan): List[Long] = plan.output.map(_.exprId.id).toList

  private final case class Entry(
    plan: SoftReference[LogicalPlan],
    translated: ir.Plan,
    reads: Set[String],
    readsAmbiguous: Boolean,
    shapeAmbiguous: Boolean
  )

  /** `ir.UnknownPlan.sourceType`s `SparkPlanAdapter` emits for a node that severs
    * lineage back to its sources (a checkpoint / a cached relation) - as
    * opposed to a node the translator merely has no case for.
    */
  val BoundarySourceTypes: Set[String] = Set("LogicalRDD", "InMemoryRelation")

  /** `Diagnostic.nodeType` for a boundary that *was* resolved, but with a caveat. */
  val ResolutionDiagnosticType = "CheckpointResolution"

  final case class Substitution(plan: LogicalPlan, diagnostics: List[Diagnostic])
}
