// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

/** The provenance of a single output column: which `Read` columns it
  * ultimately derives from, and whether that derivation passes through an
  * aggregation (many source rows collapsing into one output value).
  *
  * This is "verified lineage" in the sense of MISSION.md §5: derived from
  * the transformation plan itself, not observed at runtime or merely
  * declared by metadata.
  */
case class ColumnLineage(output: ColumnRef, sources: Set[ColumnRef], aggregated: Boolean)

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

  /** An expression's provenance before it has been given an output name:
    * which Read columns it depends on, and whether it aggregates them.
    */
  private case class Provenance(sources: Set[ColumnRef], aggregated: Boolean)

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

  private def named(name: String, p: Provenance): ColumnLineage = ColumnLineage(ColumnRef(name), p.sources, p.aggregated)

  private def resolveExpr(expr: Expr, input: Plan): Provenance = expr match {
    case ColumnReference(ref) =>
      resolveInScope(ref, input).getOrElse(Provenance(Set.empty, aggregated = false))
    case Literal(_, _) =>
      Provenance(Set.empty, aggregated = false)
    case Alias(_, inner) =>
      resolveExpr(inner, input)
    case Cast(inner, _) =>
      resolveExpr(inner, input)
    case Arithmetic(_, operands) =>
      combine(operands.map(resolveExpr(_, input)))
    case Comparison(_, left, right) =>
      combine(List(resolveExpr(left, input), resolveExpr(right, input)))
    case BooleanExpr(_, operands) =>
      combine(operands.map(resolveExpr(_, input)))
    case Conditional(branches, elseValue) =>
      val branchProvenance = branches.flatMap { case (cond, value) => List(resolveExpr(cond, input), resolveExpr(value, input)) }
      combine(branchProvenance ++ elseValue.map(resolveExpr(_, input)).toList)
    case Function(_, args) =>
      combine(args.map(resolveExpr(_, input)))
    case UDF(_, args, _) =>
      combine(args.map(resolveExpr(_, input)))
    case AggregateCall(_, arg, _) =>
      Provenance(resolveExpr(arg, input).sources, aggregated = true)
    case UnknownExpression(_, _, children) =>
      combine(children.map(resolveExpr(_, input)))
  }

  private def combine(provenances: List[Provenance]): Provenance =
    Provenance(provenances.flatMap(_.sources).toSet, provenances.exists(_.aggregated))

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
        Some(Provenance(Set(ColumnRef(ref.name, Some(scope))), aggregated = false))
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
      val found = inputs.flatMap(resolveInScope(ref, _))
      if (found.isEmpty) None
      else Some(Provenance(found.flatMap(_.sources).toSet, found.exists(_.aggregated)))

    case Join(left, right, _, _) =>
      (resolveInScope(ref, left), resolveInScope(ref, right)) match {
        case (Some(l), None)    => Some(l)
        case (None, Some(r))    => Some(r)
        case (Some(l), Some(r)) => Some(Provenance(l.sources ++ r.sources, l.aggregated || r.aggregated))
        case (None, None)       => None
      }

    case Write(_, input, _, _) => resolveInScope(ref, input)

    case UnknownPlan(_, _, _) => None
  }
}
