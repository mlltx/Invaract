// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.StructuralVerifier
import com.invaract.ir
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.execution.LogicalRDD

import java.lang.ref.SoftReference
import scala.util.control.NonFatal

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
  *    checkpoint's `rdd`, by identity (a `WeakHashMap`: the snapshot lives
  *    exactly as long as the checkpointed Dataset does). The same leaf flows
  *    unchanged into every plan later built from that Dataset (`.filter`,
  *    `.select`, a SQL view over it), so binding once at creation is what
  *    makes resolution exact even when later plans reuse the same output ids
  *    (`ck.filter(...)` shares `ck`'s ids; an iterative
  *    `df = df.filter(...).checkpoint()` loop checkpoints several plans
  *    under one set of ids).
  *  - `substitute` replaces each bound `LogicalRDD` in a plan with its
  *    snapshot (recursively, for chained checkpoints).
  *
  * ## Self-joins
  *
  * A second reference to the same checkpointed Dataset in one plan (a
  * self-join, `ck.as("a").join(ck.as("b"), ...)`) is not the same leaf:
  * Spark's analyzer gives it a `newInstance()` copy with *fresh* attribute
  * ids but the very same `rdd`. Keying the snapshot by `rdd` finds it. Its
  * origin is then spliced in as a *renewed* copy - fresh ids throughout, made
  * by Spark's own `DeduplicateRelations`, which is exactly what the analyzer
  * would have produced for the un-checkpointed self-join - and the copy's ids
  * are rewritten to the renewed plan's ids in every ancestor
  * (`transformUpWithNewOutput`). The result is structurally the plan the
  * un-checkpointed self-join has, so it verifies and fingerprints like it:
  * two occurrences of a relation keep distinct attribute ids, which is what
  * `SparkPlanAdapter`'s alias disambiguation keys on.
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
  *    of one Spark makes with fresh ids (a self-join's other side) resolves
  *    exactly when the checkpoint itself does - it is found through the
  *    shared `rdd` - so an ambiguous or unobserved checkpoint stays opaque on
  *    every side of a self-join too.
  *  - Bounded (LRU, `maxEntries`; origins over `maxPlanNodes` are not
  *    recorded at all, since a checkpoint exists to cut a lineage too big to
  *    carry). The LRU holds plans by `SoftReference` (a Catalyst plan keeps its
  *    relations - and a file index's listing - alive, which must not be pinned
  *    in a long-lived session); a checkpoint's own snapshot, taken when it is
  *    created, holds its origin strongly for exactly as long as the
  *    checkpointed Dataset lives. An evicted, collected, oversized or
  *    never-seen origin (a Dataset created before the rule was installed, or
  *    in another session) stays unresolved - the same fail-safe as before
  *    this registry existed.
  *  - Substitution walks `children`, plus the query a command holds as an
  *    inner child (`innerChildren`): `SaveIntoDataSourceCommand`
  *    (`.format("delta").save(...)`), the outer command of a CTAS, and
  *    `ReplaceTableAsSelect` are rebuilt with their resolved query. A node
  *    that keeps a query somewhere else still stays unresolved - Delta's
  *    row-level DML commands, or any command that cannot be copied with a
  *    replaced argument.
  *  - Per rule instance, i.e. per session state: a cloned session builds its
  *    own rule and starts empty.
  */
private[sparkadapter] class CheckpointRegistry(
  maxEntries: Int = CheckpointRegistry.DefaultMaxEntries,
  maxPlanNodes: Int = CheckpointRegistry.DefaultMaxPlanNodes
) {
  import CheckpointRegistry._

  private val entries = new java.util.LinkedHashMap[List[Long], Entry](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[List[Long], Entry]): Boolean = size() > maxEntries
  }

  // Keyed by the checkpoint's `rdd` (RDDs compare by identity, and every
  // `.checkpoint()` makes a new one, so two checkpoints never collide; every
  // leaf copy Spark makes of a checkpointed Dataset shares it), weakly: a
  // snapshot dies with the checkpointed Dataset that owns the rdd. The snapshot
  // holds its plan STRONGLY (unlike the LRU above): resolving a checkpoint must
  // not depend on when the garbage collector happens to run.
  private val bound = java.util.Collections.synchronizedMap(new java.util.WeakHashMap[RDD[_], Bound]())

  /** Called with every analyzed plan the rule sees, *before* `substitute`: when
    * `plan` is a bare `LogicalRDD` - the Dataset `.checkpoint()` just returned -
    * ties it to the plan currently recorded under its output ids.
    */
  def bind(plan: LogicalPlan): Unit = plan match {
    case leaf: LogicalRDD =>
      if (!bound.containsKey(leaf.rdd)) {
        for {
          entry <- lookup(leaf)
          origin <- Option(entry.plan.get) // an origin already collected is simply never resolved
        } bound.put(leaf.rdd, Bound(origin, entry.readsAmbiguous, entry.shapeAmbiguous))
      }
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
    // A checkpoint exists to CUT a lineage that has grown too large to carry
    // (iterative jobs); splicing an ever-growing origin back in on every round
    // would make each check cost proportional to the whole history. An origin
    // over the cap is not recorded, so the checkpoint made from it stays an
    // unresolved boundary - the fail-safe outcome, not a wrong one.
    if (!derivedFromItself && withinNodeCap(plan)) {
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
    def go(p: LogicalPlan, inProgress: Set[List[Long]]): LogicalPlan = p.transformUpWithNewOutput {
      case leaf: LogicalRDD if !inProgress.contains(idsOf(leaf)) =>
        Option(bound.get(leaf.rdd)) match {
          case Some(snapshot) if snapshot.readsAmbiguous =>
            diagnostics += Diagnostic(
              "LogicalRDD",
              "Several plans reading different sources share this .checkpoint() boundary's output columns, so its " +
                "origin could not be determined"
            )
            (leaf, Nil)
          case Some(snapshot) if shapeOf(leaf) != shapeOf(snapshot.plan) => (leaf, Nil)
          case Some(snapshot) =>
            if (snapshot.shapeAmbiguous)
              diagnostics += Diagnostic(
                ResolutionDiagnosticType,
                "A .checkpoint() boundary was resolved to the most recently created of several plans sharing its " +
                  "output columns and reading the same sources; the transformation between those sources and this " +
                  "point is assumed to be that plan's"
              )
            val origin = go(snapshot.plan, inProgress + idsOf(leaf))
            // The leaf itself carries the origin's ids; a self-join's copy carries fresh ones.
            val resolved = if (idsOf(leaf) == idsOf(snapshot.plan)) origin else renewed(origin)
            (resolved, leaf.output.zip(resolved.output))
          case None => (leaf, Nil)
        }
      // A command that holds its query outside `children` (see `withSubstitutedQuery`).
      case node if node.innerChildren.exists(_.isInstanceOf[LogicalPlan]) =>
        (withSubstitutedQuery(node, q => go(q, inProgress)), Nil)
    }
    val result = go(plan, Set.empty)
    Substitution(result, diagnostics.toList.distinct)
  }

  private def lookup(leaf: LogicalRDD): Option[Entry] = entries.synchronized(Option(entries.get(idsOf(leaf))))

  private def withinNodeCap(plan: LogicalPlan): Boolean = {
    var count = 0
    plan.foreach(_ => count += 1)
    count <= maxPlanNodes
  }
}

private[sparkadapter] object CheckpointRegistry {
  val DefaultMaxEntries = 256

  private def idsOf(plan: LogicalPlan): List[Long] = plan.output.map(_.exprId.id).toList

  private def shapeOf(plan: LogicalPlan) = plan.output.map(a => (a.name, a.dataType))

  /** `node` with each plan it exposes as an inner child (`innerChildren`, not
    * `children`) replaced by `substitute` of it - how a write command that
    * carries its query as a field, not a child (`SaveIntoDataSourceCommand`,
    * the outer command of a CTAS, `ReplaceTableAsSelect` once analysed), gets
    * its checkpoints resolved. A rebuilt copy of the command replaces it only
    * when a substitution actually changed something, so a node with nothing
    * to resolve is never touched; and one that cannot be copied this way is
    * left as it was, i.e. unresolved.
    */
  private def withSubstitutedQuery(node: LogicalPlan, substitute: LogicalPlan => LogicalPlan): LogicalPlan = {
    val changed = node.innerChildren.collect { case q: LogicalPlan => q -> substitute(q) }.filter { case (q, r) => q != r }
    if (changed.isEmpty) node
    else
      try node.makeCopy(node.productIterator.map { arg =>
        changed.collectFirst { case (q, r) if q eq arg.asInstanceOf[AnyRef] => r }.getOrElse(arg).asInstanceOf[AnyRef]
      }.toArray)
      catch { case NonFatal(_) => node }
  }

  /** `plan` with a fresh attribute id for everything it defines - what Spark's own
    * analyzer makes of the right side of a self-join. Uses the analyzer's own rule
    * (a join of `plan` with itself always has conflicting ids, so its right side
    * is the renewed copy), not a reimplementation of which nodes define ids.
    */
  private def renewed(plan: LogicalPlan): LogicalPlan =
    DeduplicateRelations(Join(plan, plan, Inner, None, JoinHint.NONE)).asInstanceOf[Join].right

  /** The most plan nodes an origin may have to be recorded (and so resolved). */
  val DefaultMaxPlanNodes = 5000

  /** A checkpointed Dataset's snapshot: its origin plan, held strongly. */
  private final case class Bound(plan: LogicalPlan, readsAmbiguous: Boolean, shapeAmbiguous: Boolean)

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
