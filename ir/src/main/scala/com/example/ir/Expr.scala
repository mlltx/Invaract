// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

/** An expression computes a value from zero or more input columns.
  *
  * Unlike Catalyst, which has dozens of `Expression` subclasses, this
  * algebra distinguishes node kinds only where the distinction is
  * semantically load-bearing for lineage tracing, contract verification,
  * or (eventually) fingerprinting/diffing — but it does distinguish the
  * categories a human auditing a transformation would call out by name:
  * arithmetic, comparison, boolean logic, casts, and conditionals each get
  * their own node, because collapsing "amount * 1.2" and "amount > 100"
  * into one anonymous `FunctionCall("*"/">"...)` bucket would make a
  * future semantic diff (e.g. "a filter's comparison operator flipped")
  * harder to express than it needs to be. `Function` remains the catch-all
  * for everything that doesn't fall into one of those named categories
  * (string/date functions, null checks, ranking window functions, ...) —
  * see each node's own doc for exactly where the line is drawn.
  *
  * `Aggregate` (see `AggregateCall` below) is still broken out separately
  * from every other category, because aggregation is the one case that
  * changes cardinality (many input rows collapse into one output value),
  * which lineage tracing and contract verification both need to know
  * about.
  */
sealed trait Expr {

  /** Columns this expression reads from, one level down. Not resolved
    * against a plan (a bare name isn't yet known to belong to a particular
    * Read or renamed column) — see `Lineage` for provenance tracing through
    * Reads, renames, and joins.
    */
  def references: Set[ColumnRef]
}

/** A read of a single column, optionally qualified and identified (see
  * `ColumnRef`).
  */
case class ColumnReference(ref: ColumnRef) extends Expr {
  def references: Set[ColumnRef] = Set(ref)
}

/** A constant value. `literalType` is a logical type name (the same
  * vocabulary as a contract field's `type`, see `docs/CONTRACT_MODEL.md`)
  * rather than an engine-native type, so a literal parsed from one engine's
  * plan means the same thing to a contract written independently of that
  * engine. `value == null` (with `literalType` still populated, e.g.
  * `"integer"`) represents a typed SQL `NULL`, distinct from an
  * `UnknownExpression` — a null literal is fully understood, just empty.
  */
case class Literal(value: Any, literalType: String) extends Expr {
  def references: Set[ColumnRef] = Set.empty
}

/** Binds a name to a nested expression, mid-tree — e.g. a struct field
  * built from `struct(col("a").as("x"), col("b").as("y"))`, where each
  * field name is only visible as an `Alias` nested inside the struct
  * constructor's arguments, not at a plan's output boundary.
  *
  * This is distinct from `NamedExpr`, which is how a `Project`/
  * `Aggregate`/`Window` node declares its *output* columns (the plan
  * boundary, never itself nested inside an expression tree). Most
  * front-end translators unwrap a *top-level* alias straight into a
  * `NamedExpr` and never construct this node at all — `Alias` exists
  * specifically so a *nested* rename isn't silently discarded the way an
  * earlier version of this IR (and this module's translator) used to
  * drop it.
  */
case class Alias(name: String, expr: Expr) extends Expr {
  def references: Set[ColumnRef] = expr.references
}

/** An explicit type conversion. `targetType` uses the same logical-type
  * vocabulary as `Literal.literalType`. Broken out from `Function` because
  * a cast is a distinct, universally-recognized SQL operation with exactly
  * one operand and one piece of metadata (the target type) — not a named
  * function applied to arguments.
  */
case class Cast(expr: Expr, targetType: String) extends Expr {
  def references: Set[ColumnRef] = expr.references
}

/** A numeric operator: `+`, `-`, `*`, `/`, `%`, integer division, or unary
  * negation. `operands` has exactly one entry for a unary operator (e.g.
  * `NEGATE`) and exactly two for a binary one — modeled as a list rather
  * than fixed `left`/`right` fields so both arities share one node kind,
  * the same way Catalyst's own `UnaryMinus`/`BinaryArithmetic` are both
  * "arithmetic" despite differing arity.
  */
case class Arithmetic(operator: String, operands: List[Expr]) extends Expr {
  def references: Set[ColumnRef] = operands.flatMap(_.references).toSet
}

/** A binary comparison: `=`, `<=>` (null-safe equality), `<`, `<=`, `>`,
  * `>=`. Always exactly two operands, so — unlike `Arithmetic` — this node
  * uses plain `left`/`right` fields rather than a list.
  */
case class Comparison(operator: String, left: Expr, right: Expr) extends Expr {
  def references: Set[ColumnRef] = left.references ++ right.references
}

/** A boolean combinator: `AND`, `OR` (two operands) or `NOT` (one). Kept
  * separate from `Comparison` because a boolean combinator's operands are
  * themselves boolean-valued expressions (typically `Comparison`s or
  * nested `BooleanExpr`s), not values being compared. Named `BooleanExpr`
  * rather than the shorter `Boolean` because `scala.Boolean` (the
  * primitive type used elsewhere in this very file, e.g.
  * `SortOrder.ascending`) is implicitly in scope in every file of this
  * package — a case class literally named `Boolean` here would shadow it,
  * the same reasoning that keeps the aggregate-expression node named
  * `AggregateCall` rather than colliding with `Plan.scala`'s `Aggregate`.
  */
case class BooleanExpr(operator: String, operands: List[Expr]) extends Expr {
  def references: Set[ColumnRef] = operands.flatMap(_.references).toSet
}

/** A `CASE WHEN ... THEN ... [WHEN ... THEN ...] [ELSE ...] END` (or a
  * two-way `IF(cond, then, else)`, modeled as a single-branch case with an
  * `elseValue`). Each entry in `branches` is `(condition, value)`; the
  * first branch whose condition holds determines the result, falling back
  * to `elseValue` (or a typed `NULL`, per SQL semantics) if none do.
  */
case class Conditional(branches: List[(Expr, Expr)], elseValue: Option[Expr]) extends Expr {
  def references: Set[ColumnRef] =
    branches.flatMap { case (cond, value) => cond.references ++ value.references }.toSet ++
      elseValue.map(_.references).getOrElse(Set.empty)
}

/** Application of a named function to its arguments — the catch-all for
  * anything that computes a scalar value per row without changing row
  * count and doesn't fall into one of `Cast`/`Arithmetic`/`Comparison`/
  * `Boolean`/`Conditional`/`UDF`: string/date/math functions, null checks
  * (`IS NULL`, `COALESCE`), and non-aggregate window functions (`RANK`,
  * `ROW_NUMBER`, `LAG`, `LEAD`, ...) among them. A real engine exposes
  * dozens of built-in functions; enumerating a node type per function
  * would defeat the purpose of a small, understandable IR, so this one
  * node covers all of them uniformly by name.
  */
case class Function(name: String, args: List[Expr]) extends Expr {
  def references: Set[ColumnRef] = args.flatMap(_.references).toSet
}

/** Application of a user-defined function whose body is opaque to this IR
  * — a Scala/Java/Python/Hive UDF registered by the job itself, as opposed
  * to a built-in the engine ships. Deliberately distinct from `Function`:
  * a `Function` node is a claim that this IR (and any future semantic
  * diff built on it) understands what the named operation computes: `UDF`
  * makes no such claim, and is never silently collapsed into `Function`
  * just because a name happens to be available.
  *
  * @param name the UDF's registered/declared name, when the source engine
  *   exposes one meaningfully (not merely a generic engine-assigned
  *   default like Spark's own `"UDF"`, which is not a real identifier for
  *   the specific function and would be actively misleading if treated as
  *   one). `None` means the engine did not expose a stable name for this
  *   particular UDF invocation — an explicit "not identifiable" signal,
  *   not a guess.
  * @param args the UDF's declared input expressions — what it structurally
  *   depends on, even though what it computes from them is unknown.
  * @param engineType the concrete engine-side implementation kind, when
  *   knowable (e.g. `"ScalaUDF"`, `"PythonUDF"`), purely as diagnostic
  *   metadata — never a Catalyst/engine class reference.
  */
case class UDF(name: Option[String], args: List[Expr], engineType: Option[String] = None) extends Expr {
  def references: Set[ColumnRef] = args.flatMap(_.references).toSet
}

/** Application of an aggregate function (SUM, COUNT, AVG, MIN, MAX,
  * COLLECT_LIST, ...) — the `Aggregate` category of the expression
  * algebra (named `AggregateCall` rather than `Aggregate` to avoid
  * colliding with the plan node of that name in the same package, see
  * `Plan.scala`'s `Aggregate`). Distinguished from every other expression
  * category because it collapses many rows into one value — the one place
  * a single expression changes cardinality, which is exactly the
  * information lineage tracing needs to flag (see
  * `ColumnLineage.aggregated`). The same `AggregateCall` is reused,
  * unchanged, whether it appears under an `Aggregate` plan node (grouped
  * aggregation) or a `Window` plan node (running/partitioned aggregation)
  * — windowing is a property of where the expression sits in the plan,
  * not a different kind of expression.
  */
case class AggregateCall(function: String, arg: Expr, distinct: Boolean = false) extends Expr {
  def references: Set[ColumnRef] = arg.references
}

/** An expression a front-end translator could not represent in this IR's
  * vocabulary — the expression-level counterpart to `UnknownPlan`.
  * Contributes no known column references, so lineage tracing degrades to
  * "no known source" rather than crashing or guessing. Never used to mean
  * "not yet implemented and silently dropped" — a translator emitting this
  * node is expected to also record a diagnostic (see `spark-adapter`'s
  * `Diagnostic`) explaining what it couldn't represent and why, so an
  * unsupported construct is always visible, never invisible.
  *
  * @param description a human-readable summary of the unrepresented
  *   construct.
  * @param sourceType the front-end's own class/type name for the
  *   construct (e.g. Catalyst's `getClass.getSimpleName`), kept as a
  *   plain string — not the engine's class itself — so this IR never
  *   depends on an engine's types even in its "I don't understand this"
  *   case.
  * @param children any sub-expressions the front-end could still identify
  *   structurally even though it couldn't interpret the construct as a
  *   whole (e.g. an unrecognized expression's own operands) — so an
  *   unsupported node doesn't hide understood structure nested beneath it.
  */
case class UnknownExpression(description: String, sourceType: String = "", children: List[Expr] = Nil) extends Expr {
  def references: Set[ColumnRef] = children.flatMap(_.references).toSet
}

/** Binds a name to a computed expression. This is how a `Project`,
  * `Aggregate`, or `Window` node declares its output columns.
  *
  * There is no separate `Alias` *plan-boundary* node the way Spark has one
  * nested throughout its expression tree (`Alias` is itself a Catalyst
  * `Expression`, so it can appear nested anywhere) — naming an output
  * column is not a computation on values, it's metadata about how a plan
  * stage exposes a value downstream, so it belongs at the plan boundary
  * (a projection's output list) rather than in the expression algebra
  * itself. `NamedExpr` is that boundary; the expression-level `Alias`
  * node above exists only for a rename that occurs *mid*-expression,
  * never for a plan's own output list.
  */
case class NamedExpr(name: String, expr: Expr)

/** One key of a `Sort` or `Window` ordering. */
case class SortOrder(expr: Expr, ascending: Boolean = true, nullsFirst: Boolean = true)
