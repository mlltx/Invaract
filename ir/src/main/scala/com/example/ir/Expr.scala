// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

/** An expression computes a value from zero or more input columns.
  *
  * There is deliberately no separate node per operator the way Spark's
  * Catalyst does it (`Add`, `Subtract`, `EqualTo`, `And`, `Or`, `Cast`, and
  * dozens more `Expression` subclasses). An arithmetic operator, a
  * comparison, and a cast are all "apply a named function to some
  * arguments" as far as lineage and contract verification are concerned —
  * none of them change which source columns an output value depends on.
  * `FunctionCall` represents all of them uniformly. Only `AggregateCall` is
  * broken out separately, because aggregation is the one case that changes
  * cardinality (many input rows collapse into one output value), which
  * lineage tracing and contract verification both need to know about.
  */
sealed trait Expr {

  /** Columns this expression reads from, one level down. Not resolved
    * against a plan (a bare name isn't yet known to belong to a particular
    * Read or renamed column) — see `Lineage` for provenance tracing through
    * Reads, renames, and joins.
    */
  def references: Set[ColumnRef]
}

/** A read of a single column, optionally qualified (see `ColumnRef`). */
case class ColumnReference(ref: ColumnRef) extends Expr {
  def references: Set[ColumnRef] = Set(ref)
}

/** A constant value. `literalType` is a logical type name (the same
  * vocabulary as a contract field's `type`, see `docs/CONTRACT_MODEL.md`)
  * rather than an engine-native type, so a literal parsed from one engine's
  * plan means the same thing to a contract written independently of that
  * engine.
  */
case class Literal(value: Any, literalType: String) extends Expr {
  def references: Set[ColumnRef] = Set.empty
}

/** Application of a named function to its arguments — arithmetic,
  * comparison, boolean logic, casts, string/date functions, and anything
  * else that computes a scalar value per row without changing row count.
  */
case class FunctionCall(name: String, args: List[Expr]) extends Expr {
  def references: Set[ColumnRef] = args.flatMap(_.references).toSet
}

/** Application of an aggregate function (SUM, COUNT, AVG, MIN, MAX,
  * COLLECT_LIST, ...). Distinguished from `FunctionCall` because it
  * collapses many rows into one value — the one place a single expression
  * changes cardinality, which is exactly the information lineage tracing
  * needs to flag (see `ColumnLineage.aggregated`). The same `AggregateCall`
  * is reused, unchanged, whether it appears under an `Aggregate` plan node
  * (grouped aggregation) or a `Window` plan node (running/partitioned
  * aggregation) — windowing is a property of where the expression sits in
  * the plan, not a different kind of expression.
  */
case class AggregateCall(function: String, arg: Expr, distinct: Boolean = false) extends Expr {
  def references: Set[ColumnRef] = arg.references
}

/** An expression a front-end translator could not represent in this IR's
  * vocabulary — the expression-level counterpart to `Unsupported` on
  * `Plan`. Contributes no known column references, so lineage tracing
  * degrades to "no known source" rather than crashing or guessing.
  */
case class UnsupportedExpr(description: String) extends Expr {
  def references: Set[ColumnRef] = Set.empty
}

/** Binds a name to a computed expression. This is how a `Project`,
  * `Aggregate`, or `Window` node declares its output columns.
  *
  * There is no separate `Alias` expression node the way Spark has one
  * (`Alias` is itself an `Expression`, so it can appear nested anywhere).
  * Naming an output column is not a computation on values — it's metadata
  * about how a plan stage exposes a value downstream — so it belongs at the
  * plan boundary (a projection's output list), not inside the expression
  * algebra itself. `NamedExpr` is that boundary.
  */
case class NamedExpr(name: String, expr: Expr)

/** One key of a `Sort` or `Window` ordering. */
case class SortOrder(expr: Expr, ascending: Boolean = true, nullsFirst: Boolean = true)
