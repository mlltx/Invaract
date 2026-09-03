// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

/** How an output column's *fully resolved* computation relates to its
  * source columns — a coarse, human-auditable classification, not a full
  * replay of the expression tree (see `Expr.scala`'s own doc for why the
  * expression algebra itself already distinguishes operation categories by
  * node kind — this classification is a further compression of that,
  * purpose-built for lineage consumers that want one summary judgment per
  * column rather than the whole tree).
  *
  * "Fully resolved" matters: Invaract translates Spark's *analyzed* plan,
  * not the optimized one (see ARCHITECTURE.md's ADR-002), so a chain of
  * `.withColumn()` calls produces *nested* `Project` nodes rather than one
  * flat one — the outermost `Project`'s own declaration for an untouched
  * column is often nothing more than a bare passthrough reference to an
  * inner `Project`'s real computation (`value_squared = value_squared`,
  * where the actual `value * value` lives one level down). Classifying
  * only the outermost declaring expression's own syntax would misreport
  * that column as `Direct` — this classification is instead computed
  * *through* `Lineage`'s own source resolution, the same traversal that
  * already resolves `sources`, so it reflects the column's real,
  * end-to-end derivation, not just its last hop.
  *
  * The distinction that matters most for a human (or a downstream policy)
  * auditing a transformation: is this column's derivation fully understood
  * by this IR, or does it pass through something the IR cannot see inside
  * — a `UDF` (opaque by design, see `Expr.scala`'s `UDF` doc) or an
  * `UnknownExpression` (a construct the front-end translator itself could
  * not represent)? `Opaque` wins over every other classification for a
  * column whose resolved computation contains one anywhere along the way,
  * even nested arbitrarily deep beneath otherwise-understood operations or
  * behind several layers of passthrough — a `CASE WHEN` whose result calls
  * a UDF is opaque overall, not "mostly computed."
  */
sealed trait DerivationKind
object DerivationKind {

  /** The output is exactly one source column, possibly renamed, through
    * any number of pure passthrough hops — no computation anywhere along
    * the way (`id = id`, `customer_id = orders.customer_id`, or the same
    * column re-declared unchanged across several nested `Project`s).
    */
  case object Direct extends DerivationKind

  /** The output has no source columns at all — a literal, or an
    * expression built entirely from literals (`region = 'US'`).
    */
  case object Constant extends DerivationKind

  /** The output is computed from one or more source columns using only
    * operations this IR fully understands (`Cast`/`Arithmetic`/
    * `Comparison`/`BooleanExpr`/`Conditional`/`Function`/`AggregateCall`)
    * — auditable: a human (or tooling built on this IR) can read exactly
    * what this column computes, even if that computation happened several
    * plan stages before the column's final declaration. A `CASE WHEN`
    * (`Conditional`) built from a `Comparison` is the canonical example:
    * every operation involved has known, named semantics, unlike a
    * `UDF`'s opaque body.
    */
  case object Computed extends DerivationKind

  /** The output's resolved computation contains a `UDF` or
    * `UnknownExpression` node somewhere along the way — this IR knows
    * what columns it structurally depends on, but not what it actually
    * computes from them. Distinct from `Computed` specifically so a
    * contract author or a lineage consumer can flag "this column's
    * meaning is not fully auditable from the transformation plan alone"
    * without having to separately re-walk the expression tree looking for
    * a `UDF` themselves.
    */
  case object Opaque extends DerivationKind
}

/** One aggregate function contributing to an output column's derivation —
  * e.g. `SUM`, `COUNT DISTINCT`. An output column's derivation can combine
  * more than one aggregate function (`sum(x) / count(y)`), so
  * `ColumnLineage.aggregations` is a `Set`, not a single value — collapsing
  * that down to one boolean ("is this aggregated at all") loses exactly
  * the detail (which function, over what) a human auditing *how* a column
  * changed meaning across a schema-compatible contract revision needs.
  */
case class AggregationDetail(function: String, distinct: Boolean = false)

/** The provenance of a single output column: which `Read` columns it
  * ultimately derives from, how (`derivation`), and which aggregate
  * function(s), if any, collapse many source rows into this one output
  * value (`aggregations`).
  *
  * This is "verified lineage" in the sense of MISSION.md §5: derived from
  * the transformation plan itself, not observed at runtime or merely
  * declared by metadata.
  */
case class ColumnLineage(
  output: ColumnRef,
  sources: Set[ColumnRef],
  derivation: DerivationKind,
  aggregations: Set[AggregationDetail] = Set.empty
) {

  /** Convenience for "was this column derived through any aggregation at
    * all," without inspecting `aggregations` directly.
    */
  def aggregated: Boolean = aggregations.nonEmpty
}

/** Traces column-level lineage through a transformation plan: for each
  * column a `Write` ultimately produces, which `Read` columns it derives
  * from.
  *
  * Resolution is purely structural — no symbol table, no exprIds. A bare
  * column name is looked up against the nearest plan node that declares
  * output names (`Project`/`Aggregate`/`Window`), falling through
  * pass-through nodes (`Filter`/`Sort`) until it either matches a declared
  * name or bottoms out at a `Read`. `Join` tries both branches and unions
  * the result on an ambiguous unqualified name rather than resolving
  * arbitrarily, so lineage tracing degrades to "attributed to all
  * plausible sources" instead of failing or guessing.
  */
object Lineage {

  def trace(plan: Plan): List[ColumnLineage] = plan match {
    case Write(_, input, _, _) => outputsOf(input)
    case other           => outputsOf(other)
  }

  /** An expression's fully resolved provenance: which Read columns it
    * depends on, how it was derived (following passthrough references
    * through however many plan stages, not just the immediate syntax —
    * see `DerivationKind`'s own doc), and which aggregate function(s), if
    * any, it aggregates them through.
    */
  private case class Provenance(sources: Set[ColumnRef], derivation: DerivationKind, aggregations: Set[AggregationDetail])

  private def outputsOf(plan: Plan): List[ColumnLineage] = plan match {
    case Project(input, columns) =>
      columns.map { case NamedExpr(name, expr) => named(name, resolveExpr(expr, input)) }

    case Aggregate(input, _, aggregates) =>
      aggregates.map { case NamedExpr(name, expr) => named(name, resolveExpr(expr, input)) }

    case Window(input, windowExprs, _, _) =>
      outputsOf(input) ++ windowExprs.map { case NamedExpr(name, expr) => named(name, resolveExpr(expr, input)) }

    case Filter(input, _) => outputsOf(input)
    case Sort(input, _)   => outputsOf(input)
    case Limit(input, _, _) => outputsOf(input)
    case Union(inputs)    => inputs.headOption.map(outputsOf).getOrElse(Nil)
    case Join(left, right, _, _) => outputsOf(left) ++ outputsOf(right)
    case Write(_, input, _, _)  => outputsOf(input)

    // A bare Read declares no output list of its own (see Plan.scala) —
    // there is nothing to trace until something downstream projects it.
    case Read(_, _) => Nil

    // An untranslated construct declares no known output list either —
    // there is nothing to trace past it.
    case UnknownPlan(_, _, _) => Nil
  }

  private def named(name: String, p: Provenance): ColumnLineage =
    ColumnLineage(ColumnRef(name), p.sources, p.derivation, p.aggregations)

  private def resolveExpr(expr: Expr, input: Plan): Provenance = expr match {
    case ColumnReference(ref) =>
      // Direct is the right fallback for a reference this plan can't
      // resolve at all (e.g. it sits on an UnknownPlan): with nothing to
      // inherit a real computation from, "just this one (unresolvable)
      // column, unchanged" is the honest, syntax-level answer — the same
      // one a resolvable pure passthrough chain would eventually bottom
      // out at via Read's own base case below.
      resolveInScope(ref, input).getOrElse(Provenance(Set.empty, DerivationKind.Direct, Set.empty))
    case Literal(_, _) =>
      Provenance(Set.empty, DerivationKind.Constant, Set.empty)
    case Alias(_, inner) =>
      // A rename is not a computation - inherits the inner expression's
      // resolved provenance verbatim, Direct included.
      resolveExpr(inner, input)
    case Cast(inner, _) =>
      combineOperation(List(resolveExpr(inner, input)))
    case Arithmetic(_, operands) =>
      combineOperation(operands.map(resolveExpr(_, input)))
    case Comparison(_, left, right) =>
      combineOperation(List(resolveExpr(left, input), resolveExpr(right, input)))
    case BooleanExpr(_, operands) =>
      combineOperation(operands.map(resolveExpr(_, input)))
    case Conditional(branches, elseValue) =>
      val branchProvenance = branches.flatMap { case (cond, value) => List(resolveExpr(cond, input), resolveExpr(value, input)) }
      combineOperation(branchProvenance ++ elseValue.map(resolveExpr(_, input)).toList)
    case Function(_, args) =>
      combineOperation(args.map(resolveExpr(_, input)))
    case UDF(_, args, _) =>
      // Opaque unconditionally, regardless of what its arguments resolve
      // to - a UDF's body is opaque to this IR by design (see UDF's own
      // doc in Expr.scala), never a function of its arguments' own
      // derivation.
      combineOperation(args.map(resolveExpr(_, input))).copy(derivation = DerivationKind.Opaque)
    case AggregateCall(function, arg, distinct) =>
      // combineOperation handles sources/derivation (an aggregate call is
      // always a real operation, never Direct); aggregations is replaced
      // outright, not merged with the argument's own - the argument isn't
      // itself expected to already be aggregated (nested aggregates
      // aren't valid SQL), and this call is the one aggregation that
      // matters at this level.
      combineOperation(List(resolveExpr(arg, input))).copy(aggregations = Set(AggregationDetail(function, distinct)))
    case UnknownExpression(_, _, children) =>
      // Opaque unconditionally, regardless of whether any children
      // resolved real, understood sources - an unrepresentable construct
      // is opaque by definition, the same "opaque anywhere wins" rule a
      // nested UDF gets.
      combineOperation(children.map(resolveExpr(_, input))).copy(derivation = DerivationKind.Opaque)
  }

  /** Combines the resolved provenances of a real operation's operands
    * (`Cast`/`Arithmetic`/`Comparison`/`BooleanExpr`/`Conditional`/
    * `Function`/`AggregateCall`'s own argument) into that operation's own
    * provenance. Never produces `Direct`, even when every operand does —
    * applying an actual operator is itself a computation, not a pure
    * passthrough (`Cast(ColumnReference(x), "double")` is `Computed`, not
    * `Direct`, despite depending on exactly one already-Direct column).
    * Contrast `combineUnion` below, used where multiple *candidate*
    * resolutions of the *same* reference are being combined, not multiple
    * *operands* of one real operation.
    */
  private def combineOperation(provenances: List[Provenance]): Provenance = {
    val sources = provenances.flatMap(_.sources).toSet
    val aggregations = provenances.flatMap(_.aggregations).toSet
    val derivation =
      if (provenances.exists(_.derivation == DerivationKind.Opaque)) DerivationKind.Opaque
      else if (sources.isEmpty) DerivationKind.Constant
      else DerivationKind.Computed
    Provenance(sources, derivation, aggregations)
  }

  /** Combines multiple *candidate* resolutions of one ambiguous reference
    * — `Union`'s branches, or `Join`'s two sides when both plausibly
    * declare the same unqualified name — into one Provenance. Unlike
    * `combineOperation`, this preserves `Direct` when every candidate
    * agrees it's a pure passthrough: the reference genuinely is just one
    * of several possible raw columns, with no computation on any branch,
    * so `Direct` (structurally ambiguous about origin, but computation-
    * free either way) is the accurate label, not `Computed`. An empty
    * input list is never actually reached with observable effect (see the
    * `Union` case below), but is defined to combine to `Direct` with no
    * sources — a mathematically consistent choice (vacuously "every
    * candidate is Direct").
    */
  private def combineUnion(provenances: List[Provenance]): Provenance = {
    val sources = provenances.flatMap(_.sources).toSet
    val aggregations = provenances.flatMap(_.aggregations).toSet
    val derivation =
      if (provenances.exists(_.derivation == DerivationKind.Opaque)) DerivationKind.Opaque
      else if (provenances.forall(_.derivation == DerivationKind.Direct)) DerivationKind.Direct
      else if (sources.isEmpty) DerivationKind.Constant
      else DerivationKind.Computed
    Provenance(sources, derivation, aggregations)
  }

  /** Resolves a bare column reference against a plan: does `ref` name a
    * column this plan (re)declares, or does it pass through to whatever
    * produced this plan's input? Returns `None` when `ref`'s qualifier
    * rules out this branch entirely (used by `Join` to avoid attributing a
    * qualified reference to the wrong side).
    */
  private def resolveInScope(ref: ColumnRef, plan: Plan): Option[Provenance] = plan match {
    case Read(dataset, alias) =>
      val scope = alias.getOrElse(dataset.location)
      if (ref.qualifier.forall(_ == scope))
        Some(Provenance(Set(ColumnRef(ref.name, Some(scope))), DerivationKind.Direct, Set.empty))
      else
        None

    case Project(input, columns) =>
      columns.find(_.name == ref.name).map(nc => resolveExpr(nc.expr, input))

    case Aggregate(input, _, aggregates) =>
      aggregates.find(_.name == ref.name).map(nc => resolveExpr(nc.expr, input))

    case Window(input, windowExprs, _, _) =>
      windowExprs
        .find(_.name == ref.name)
        .map(nc => resolveExpr(nc.expr, input))
        .orElse(resolveInScope(ref, input))

    case Filter(input, _) => resolveInScope(ref, input)
    case Sort(input, _)   => resolveInScope(ref, input)
    case Limit(input, _, _) => resolveInScope(ref, input)

    case Union(inputs) =>
      // A Stryker mutant flipping `found.isEmpty` to `false` here is a
      // genuine equivalent, not a coverage gap: `combineUnion` on an empty
      // list produces `Provenance(Set.empty, Direct, Set.empty)` — the
      // same value `resolveExpr`'s ColumnReference case's own
      // `getOrElse(Provenance(Set.empty, Direct, Set.empty))` fallback
      // produces for `None`, and the same value that acts as an identity
      // element in `Join`'s Some/Some combination below (an empty-sources,
      // Direct provenance changes nothing when unioned with a real one —
      // confirmed algebraically: opaque-wins and sources-empty/Computed
      // checks are unaffected by an empty-sources contributor, and the
      // all-Direct check only stays true if the other side is also
      // Direct, matching what using that other side alone would give).
      // Every caller sees the identical result either way.
      val found = inputs.flatMap(resolveInScope(ref, _))
      if (found.isEmpty) None
      else Some(combineUnion(found))

    case Join(left, right, _, _) =>
      (resolveInScope(ref, left), resolveInScope(ref, right)) match {
        case (Some(l), None)    => Some(l)
        case (None, Some(r))    => Some(r)
        case (Some(l), Some(r)) => Some(combineUnion(List(l, r)))
        case (None, None)       => None
      }

    case Write(_, input, _, _) => resolveInScope(ref, input)

    case UnknownPlan(_, _, _) => None
  }
}
