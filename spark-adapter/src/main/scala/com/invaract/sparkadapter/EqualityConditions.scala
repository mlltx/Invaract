// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir.{BooleanExpr, ColumnReference, ColumnRef, Comparison, Expr}

/** Column names a condition `Expr` genuinely, unconditionally establishes as
  * an equality match — shared by `RuleVerifier`'s `merge_condition` rule
  * (checked against a MERGE's `ON` condition) and `PlanRuleVerifier`'s
  * `required_join_columns` rule (checked against an ordinary `Join`'s
  * condition). Both ask the same question of a two-sided binary condition —
  * "does this predicate require these columns to be equal across the two
  * sides, unconditionally?" — so this is one implementation, not two that
  * could drift apart the way `RuleVerifier`'s pre-extraction history already
  * shows duplicated logic can.
  *
  * Predicate-aware, not just "referenced somewhere": a declared column must
  * appear as a bare operand of a required, top-level equality (`=`/`<=>`) —
  * `t.customer_id = s.customer_id`, or even `t.customer_id = s.cust_id`
  * (the two sides' column names are allowed to differ; the declared name
  * only has to appear on *one* side) — not merely occur anywhere in the
  * condition. This closes three genuinely weaker matches a naive "is it
  * referenced anywhere" check would wrongly accept:
  *
  *   1. **A range/inequality check, not an equality.**
  *      `t.customer_id > 0 AND t.id = s.id` references `customer_id`
  *      without actually matching on it.
  *   2. **A literal comparison, not a column-to-column match.**
  *      `t.customer_id = 'ACME'` references `customer_id`, but pins it to a
  *      constant rather than matching it against the other side.
  *   3. **An `OR` branch, not a required condition.**
  *      `t.id = s.id OR t.region = s.region` only actually requires *one*
  *      of the two to hold, not both.
  *
  * De Morgan- and `NOT`-aware (see `equalityPairedColumns`): a condition
  * written as `NOT (t.customer_id != s.customer_id)`, or
  * `NOT (t.id != s.id OR t.region != s.region)`, establishes the exact same
  * pairing(s) as the equivalent bare-`AND`-of-equalities form would, since
  * both are genuinely the same requirement — SQL's `!=` itself always
  * arrives here as `NOT(... = ...)` (Catalyst parses it that way; there is
  * no native "not equal" comparison node), so a `NOT` wrapping one is
  * already ordinary territory, not an edge case.
  *
  * Distinguishes the two sides of the condition from each other, not just
  * two differently-positioned references to columns that happen to share
  * (or differ in) name — see `isCrossSideMatch`. A copy-paste bug like
  * `ON t.customer_id = t.customer_id` (always true, matching every row
  * against itself) is never wrongly accepted as satisfying a rule declaring
  * `customer_id`, since this looks at qualifiers, not just column names.
  *
  * Still a structural approximation, not full predicate logic: `CASE WHEN`
  * is deliberately never treated as establishing a pairing — the equality
  * it contains only holds on some rows, not unconditionally, the same
  * "not a required condition" problem the `OR` case above guards against.
  * A condition with *extra* conjuncts beyond the declared columns (an
  * additional partition-pruning predicate, for example) is still not
  * flagged: checking more than required is not the failure this guards
  * against.
  */
private[sparkadapter] object EqualityConditions {

  /** Column names genuinely established as a *required* equality match by
    * `expr` — the entry point, called with `expr` asserted true (the
    * condition as a whole must hold).
    */
  def equalityPairedColumns(expr: Expr): Set[String] = requiredEqualities(expr, negated = false)

  /** The De Morgan-/`NOT`-aware core of `equalityPairedColumns`: column
    * names a required-equality reading of `expr` genuinely establishes,
    * given that `expr` is asserted to be `true` (`negated = false`) or
    * `false` (`negated = true`) — the polarity a surrounding `NOT` (however
    * deeply nested) has put it under.
    *
    * Each case is the De Morgan dual of its opposite-polarity twin:
    *
    *   - `AND(a, b)` asserted true requires *both* `a` and `b` to hold —
    *     union their pairings. Asserted false (`NOT(a AND b)` = `a` is
    *     false `OR` `b` is false, De Morgan) only guarantees *one* side
    *     failed, not which — same "can't guarantee anything" shape as an
    *     asserted-true `OR`, so both contribute `Set.empty`.
    *   - `OR(a, b)` asserted true only guarantees *one* side holds, not
    *     both — `Set.empty`. Asserted false (`NOT(a OR b)` = `a` is false
    *     `AND` `b` is false, De Morgan) requires *both* to fail — union
    *     their (negated) pairings, the dual of the true-`AND` case.
    *   - `NOT(x)` simply flips polarity and recurses — this one rule is
    *     what makes a doubly-`NOT`ed condition (`NOT(NOT(x))`, e.g. from
    *     source SQL written as `NOT (a != b)`, since `!=` itself already
    *     arrives here as `NOT(a = b)`) resolve back to `x`'s own reading
    *     without a separate double-negation special case.
    *   - `Comparison("="/"<=>", col, col)` asserted true is exactly the
    *     match this looks for **if `isCrossSideMatch` agrees the two
    *     operands are genuinely on opposite sides**; asserted false
    *     (`NOT(a = b)`, i.e. `a != b` in the source SQL) guarantees the
    *     columns *differ*, the opposite of what's needed — `Set.empty`.
    *   - `Comparison("!=", col, col)` (kept for IR built directly, e.g. by
    *     a future non-Spark front-end or a hand-built test — Spark's own
    *     translator never actually emits this operator string, always
    *     preferring `NOT(EqualTo(...))`) is the exact mirror of the
    *     `"="`/`"<=>"` case: asserted false is the genuine match (again
    *     gated on `isCrossSideMatch`), asserted true is not.
    *   - Everything else (`CASE WHEN`, a literal comparison, a range
    *     check, an unrecognized node, ...) contributes nothing under
    *     either polarity: none of them make an unconditional,
    *     column-to-column equality guarantee.
    */
  private def requiredEqualities(expr: Expr, negated: Boolean): Set[String] = expr match {
    case BooleanExpr("AND", List(left, right)) =>
      if (!negated) requiredEqualities(left, negated) ++ requiredEqualities(right, negated)
      else Set.empty
    case BooleanExpr("OR", List(left, right)) =>
      if (negated) requiredEqualities(left, negated) ++ requiredEqualities(right, negated)
      else Set.empty
    case BooleanExpr("NOT", List(child)) =>
      requiredEqualities(child, !negated)
    case Comparison(op, ColumnReference(a), ColumnReference(b)) if op == "=" || op == "<=>" =>
      if (!negated && isCrossSideMatch(a, b)) Set(a.name, b.name) else Set.empty
    case Comparison("!=", ColumnReference(a), ColumnReference(b)) =>
      if (negated && isCrossSideMatch(a, b)) Set(a.name, b.name) else Set.empty
    case _ => Set.empty
  }

  /** Whether `a`/`b` are genuinely on opposite sides of the two-relation
    * condition this belongs to (a MERGE's target/source, or a `Join`'s
    * left/right) — not merely two differently-positioned references to
    * columns that happen to share (or differ in) name. Either kind of
    * condition only ever involves exactly two relations, so two *different*
    * qualifiers necessarily means one from each side, without needing to
    * separately determine which qualifier is which. When either side's
    * qualifier is unknown (a real but rare case — this syntax practically
    * always requires qualified columns once resolved, since an unqualified
    * same-named column from either side would itself be an ambiguous
    * reference the engine rejects at analysis time), this stays permissive
    * (`true`) rather than introduce a new false negative for a condition
    * this module simply can't be sure about — the same "unknown, don't
    * guess wrong" posture `SparkPlanAdapter`'s translation-layer fallbacks
    * use elsewhere. Only a *confirmed* same-qualifier match — e.g. a
    * copy-paste bug like `ON t.customer_id = t.customer_id` — is excluded.
    */
  def isCrossSideMatch(a: ColumnRef, b: ColumnRef): Boolean =
    (a.qualifier, b.qualifier) match {
      case (Some(qa), Some(qb)) => qa != qb
      case _                    => true
    }
}
