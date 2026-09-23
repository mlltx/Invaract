// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

/** Extracts the `ColumnPropertyState` facts a boolean condition
  * *unconditionally* establishes about the columns it references, given
  * that the condition is asserted `true` (or `false`, for a `CASE`
  * branch already ruled out) — the mechanism `PropertyAnalysis` uses for
  * both a `Filter`'s condition and a `Conditional`'s per-branch narrowing.
  *
  * Structurally the same polarity-aware, De Morgan-/`NOT`-aware top-level
  * walk `com.invaract.sparkadapter.EqualityConditions` already implements
  * for `merge_condition`/`required_join_columns` — generalized from "which
  * columns are equality-paired" to a broader vocabulary of facts (not
  * null, equals/one-of a constant, a numeric bound). That existing logic
  * is not imported here: it lives in `spark-adapter` (used by
  * `RuleVerifier`, which needs `contract` types this module must not
  * depend on — see `ir`'s own module doc), while this is the same *pattern*
  * applied where it belongs, at the `ir` level, with no `contract`
  * dependency. See docs/STATIC_DATA_QUALITY_VERIFICATION.md §3.4.
  *
  * Only a flat top-level `AND` of recognized conjuncts contributes
  * anything under a `true` assertion (an `OR` only guarantees "at least
  * one side," not which, so contributes nothing) — the same
  * "under-claim rather than guess" discipline every other conservative
  * analysis in this codebase already follows.
  */
object PredicateFacts {

  def requiredFacts(expr: Expr, negated: Boolean = false): Map[ColumnRef, ColumnPropertyState] = expr match {
    case BooleanExpr("AND", List(left, right)) =>
      if (!negated) merge(requiredFacts(left, negated), requiredFacts(right, negated))
      else Map.empty
    case BooleanExpr("OR", List(left, right)) =>
      if (negated) merge(requiredFacts(left, negated), requiredFacts(right, negated))
      else Map.empty
    case BooleanExpr("NOT", List(child)) =>
      requiredFacts(child, !negated)

    case Function("ISNOTNULL", List(ColumnReference(c))) =>
      Map(c -> ColumnPropertyState(notNull = if (!negated) NullabilityFact.Proven else NullabilityFact.Refuted))
    case Function("ISNULL", List(ColumnReference(c))) =>
      Map(c -> ColumnPropertyState(notNull = if (!negated) NullabilityFact.Refuted else NullabilityFact.Proven))

    // IN (...) only contributes under a true assertion - "NOT IN" asserted
    // true tells us the value ISN'T any of a set, which doesn't tighten a
    // OneOf/Range the same way membership does, so it's left unmodeled
    // (Set.empty), the same "asserted false adds nothing" treatment `=`
    // already gets below.
    case Function("IN", ColumnReference(c) :: candidates) if !negated && candidates.nonEmpty =>
      literalsOf(candidates) match {
        case Some(vals) => Map(c -> ColumnPropertyState(oneOf = Some(Property.OneOf(vals.map(_._1).toSet, vals.head._2))))
        case None        => Map.empty
      }

    // Null-safe equality: unlike plain `=`, `c <=> literal` asserted true
    // with a non-null literal proves NotNull too (SQL's null-safe equality
    // is only true when neither side is null, given the other side is a
    // non-null literal) - a real, if minor, bonus fact plain `=` can't
    // give (see Comparison's own Expr.scala doc on the two operators).
    case Comparison("<=>", ColumnReference(c), Literal(v, t)) if !negated && v != null =>
      Map(c -> equalsFact(c, v, t, alsoNotNull = true))
    case Comparison("<=>", Literal(v, t), ColumnReference(c)) if !negated && v != null =>
      Map(c -> equalsFact(c, v, t, alsoNotNull = true))

    case Comparison("=", ColumnReference(c), Literal(v, t)) if !negated && v != null =>
      Map(c -> equalsFact(c, v, t, alsoNotNull = false))
    case Comparison("=", Literal(v, t), ColumnReference(c)) if !negated && v != null =>
      Map(c -> equalsFact(c, v, t, alsoNotNull = false))

    case Comparison(op, ColumnReference(c), Literal(v, t)) if NumericLiterals.isNumeric(t) =>
      rangeFact(c, op, v, negated)
    // A literal on the left (`0 <= amount`) is the mirror comparison with
    // the operator flipped, not a new case: `0 <= amount` means the same
    // thing as `amount >= 0`.
    case Comparison(op, Literal(v, t), ColumnReference(c)) if NumericLiterals.isNumeric(t) =>
      rangeFact(c, flip(op), v, negated)

    case _ => Map.empty
  }

  def merge(a: Map[ColumnRef, ColumnPropertyState], b: Map[ColumnRef, ColumnPropertyState]): Map[ColumnRef, ColumnPropertyState] =
    (a.keySet ++ b.keySet).map { k =>
      k -> a.getOrElse(k, ColumnPropertyState.Unknown).tightenWith(b.getOrElse(k, ColumnPropertyState.Unknown))
    }.toMap

  private def equalsFact(c: ColumnRef, v: Any, t: String, alsoNotNull: Boolean): ColumnPropertyState = {
    val pointRange =
      if (NumericLiterals.isNumeric(t)) NumericLiterals.toBigDecimal(v).map(bd => Property.Range(gte = Some(bd), lte = Some(bd))) else None
    ColumnPropertyState(
      notNull = if (alsoNotNull) NullabilityFact.Proven else NullabilityFact.Unknown,
      equalsConstant = Some(Property.EqualsConstant(v, t)),
      range = pointRange
    )
  }

  /** `asserted` is the operator as written (`>`/`>=`/`<`/`<=`) after any
    * literal-on-the-left flip has already been applied by the caller;
    * `negated` says whether the whole comparison is asserted true or
    * false. The negation table itself (asserted-false swaps to the
    * opposite-direction, opposite-inclusivity bound) is what makes a
    * `CASE` branch's `ELSE` provable from its own condition's negation —
    * see docs/STATIC_DATA_QUALITY_VERIFICATION.md §3.4's own table and
    * Example 4.
    */
  private def rangeFact(c: ColumnRef, asserted: String, v: Any, negated: Boolean): Map[ColumnRef, ColumnPropertyState] = {
    val result = for {
      bd    <- NumericLiterals.toBigDecimal(v)
      range <- (asserted, negated) match {
                 case (">", false)  => Some(Property.Range(gt = Some(bd)))
                 case (">", true)   => Some(Property.Range(lte = Some(bd)))
                 case (">=", false) => Some(Property.Range(gte = Some(bd)))
                 case (">=", true)  => Some(Property.Range(lt = Some(bd)))
                 case ("<", false)  => Some(Property.Range(lt = Some(bd)))
                 case ("<", true)   => Some(Property.Range(gte = Some(bd)))
                 case ("<=", false) => Some(Property.Range(lte = Some(bd)))
                 case ("<=", true)  => Some(Property.Range(gt = Some(bd)))
                 case _              => None
               }
    } yield Map(c -> ColumnPropertyState(range = Some(range)))
    result.getOrElse(Map.empty)
  }

  private def flip(op: String): String = op match {
    case ">"  => "<"
    case ">=" => "<="
    case "<"  => ">"
    case "<=" => ">="
    case other => other
  }

  private def literalsOf(exprs: List[Expr]): Option[List[(Any, String)]] = {
    val values = exprs.collect { case Literal(v, t) => (v, t) }
    if (values.size == exprs.size) Some(values) else None
  }
}
