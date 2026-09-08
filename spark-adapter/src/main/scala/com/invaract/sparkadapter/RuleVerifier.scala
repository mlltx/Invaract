// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractRule, InterpretedRule}
import com.invaract.ir.{BooleanExpr, ColumnReference, ColumnRef, Comparison, DeleteScope, Expr, RowMutation}

/** Checks a contract's declared DML rules (`com.invaract.contract.RuleType`)
  * against the structural facts `RowMutationSupport` extracted from one
  * real Spark row-level DML operation. The counterpart to
  * `StructuralVerifier` for exactly the three rule types
  * `ContractRule.interpret` currently understands — not a general
  * rule-expression evaluator. `merge_condition` checks genuine,
  * De Morgan-/`NOT`-aware, target-vs-source-aware column-to-column
  * equality pairing (see `equalityPairedColumns`/`requiredEqualities`/
  * `isCrossSideMatch`), not just "the column is referenced somewhere" —
  * but deeper semantic DML verification (a `CASE WHEN`-conditional match,
  * which specific rows an `UPDATE` touches) remains future work (see
  * ROADMAP.md's "Full semantic DML verification" item).
  *
  * Each rule only constrains the DML *shape* it's about — a single
  * `RowMutation` represents one concrete operation instance, and a
  * contract's rules are checked against whichever of them actually apply
  * to it:
  *
  *   - `merge_condition` is silently inapplicable (not violated) to an
  *     operation with no match condition — i.e. not a MERGE.
  *   - `forbid_unconditional_delete` is inapplicable to an operation with
  *     no delete at all (`DeleteScope.NotApplicable`).
  *   - `allowed_update_columns` is inapplicable to an operation that
  *     updates no columns.
  *
  * This mirrors `StructuralVerifier`'s own "declared but not every check
  * is always relevant" relationship between its two `VerificationOptions`
  * toggles and a plan that doesn't exercise them.
  */
private[sparkadapter] object RuleVerifier {

  /** Whether `rule` is the kind of rule `RowMutationSupport.Classification.Unverifiable(kind)`
    * would need to check — used by `ContractEnforcementRule` to decide
    * whether an operation this module recognized as DML-shaped but
    * couldn't extract facts for is actually a problem for *this*
    * contract, or just an operation kind it happens not to declare any
    * rule for (in which case there's nothing to fail closed over).
    */
  def appliesTo(rule: InterpretedRule, kind: RowMutationSupport.Kind): Boolean = (rule, kind) match {
    case (_: InterpretedRule.MergeCondition, RowMutationSupport.Kind.Merge)         => true
    case (InterpretedRule.ForbidUnconditionalDelete, RowMutationSupport.Kind.Delete) => true
    case (_: InterpretedRule.AllowedUpdateColumns, RowMutationSupport.Kind.Update)   => true
    case _                                                                          => false
  }

  def verify(rules: List[ContractRule], mutation: RowMutation): List[Violation] =
    rules.flatMap(_.interpret).flatMap {
      case InterpretedRule.MergeCondition(columns)       => checkMergeCondition(columns, mutation)
      case InterpretedRule.ForbidUnconditionalDelete     => checkForbidUnconditionalDelete(mutation)
      case InterpretedRule.AllowedUpdateColumns(columns) => checkAllowedUpdateColumns(columns, mutation)
    }

  /** Predicate-aware, not just "referenced somewhere": a declared column
    * must appear as a bare operand of a required, top-level equality
    * (`=`/`<=>`) — `t.customer_id = s.customer_id`, or even
    * `t.customer_id = s.cust_id` (source/target column names are allowed
    * to differ; the declared name only has to appear on *one* side) — not
    * merely occur anywhere in the condition. This closes three real false
    * negatives the previous "is it referenced anywhere" check had, each a
    * genuinely weaker match than the rule is meant to guarantee:
    *
    *   1. **A range/inequality check, not an equality.**
    *      `t.customer_id > 0 AND t.id = s.id` referenced `customer_id`
    *      without the MERGE actually matching target against source on
    *      it — the previous check accepted this.
    *   2. **A literal comparison, not a column-to-column match.**
    *      `t.customer_id = 'ACME'` references `customer_id`, but pins it
    *      to a constant rather than joining target to source on it.
    *   3. **An `OR` branch, not a required condition.**
    *      `t.id = s.id OR t.region = s.region` only actually requires
    *      *one* of the two to hold, not both — a strictly weaker
    *      guarantee than a contract declaring both columns intends.
    *
    * De Morgan- and `NOT`-aware (see `equalityPairedColumns`): a
    * condition written as `NOT (t.customer_id != s.customer_id)`, or
    * `NOT (t.id != s.id OR t.region != s.region)`, establishes the exact
    * same pairing(s) as the equivalent bare-`AND`-of-equalities form
    * would, since both are genuinely the same requirement — SQL's `!=`
    * itself always arrives here as `NOT(... = ...)` (Catalyst parses it
    * that way; there is no native "not equal" comparison node), so a
    * `NOT` wrapping one is already ordinary territory, not an edge case.
    *
    * Distinguishes target- from source-side qualifiers: a comparison only
    * counts as establishing a pairing when its two operands' qualifiers
    * are both known and *different* — see `isCrossSideMatch`. A copy-paste
    * bug like `ON t.customer_id = t.customer_id` (always true, matching
    * every row against itself rather than target against source) used to
    * be wrongly accepted as satisfying `merge_condition: [customer_id]`,
    * since the old check only looked at column *names*.
    *
    * Still a structural approximation, not full predicate logic:
    * `CASE WHEN` is deliberately never treated as establishing a pairing
    * — the equality it contains only holds on some rows, not
    * unconditionally, which is the same "not a required condition"
    * problem the `OR` case above guards against. A condition with *extra*
    * conjuncts beyond the declared columns (an additional
    * partition-pruning predicate, for example) is still not flagged:
    * checking more than required is not the failure this rule guards
    * against.
    */
  private def checkMergeCondition(declaredColumns: List[String], mutation: RowMutation): List[Violation] =
    mutation.matchCondition match {
      case None => Nil
      case Some(condition) =>
        val paired = equalityPairedColumns(condition)
        val missing = declaredColumns.filterNot(paired.contains)
        if (missing.isEmpty) Nil
        else
          List(
            Violation(
              ViolationType.RuleMergeConditionViolation,
              s"contract requires the MERGE to match on ${declaredColumns.mkString(", ")}, but its ON condition " +
                s"does not include an equality match on ${missing.mkString(", ")}",
              remediation =
                s"Add a 'target.${missing.head} = source.${missing.head}'-style equality to the MERGE's ON " +
                  s"condition for ${missing.mkString(", ")}, or update the contract's merge_condition rule if " +
                  "matching on fewer columns is intentional.",
              expected = Some(declaredColumns.mkString(", ")),
              actual = Some(paired.mkString(", "))
            )
          )
    }

  /** Column names genuinely established as a *required* equality match by
    * `expr` — the top-level entry point for `requiredEqualities`, called
    * with `expr` asserted true (the condition as a whole must hold).
    */
  private def equalityPairedColumns(expr: Expr): Set[String] = requiredEqualities(expr, negated = false)

  /** The De Morgan-/`NOT`-aware core of `equalityPairedColumns`: column
    * names a required-equality reading of `expr` genuinely establishes,
    * given that `expr` is asserted to be `true` (`negated = false`) or
    * `false` (`negated = true`) — the polarity a surrounding `NOT`
    * (however deeply nested) has put it under.
    *
    * Each case is the De Morgan dual of its opposite-polarity twin:
    *
    *   - `AND(a, b)` asserted true requires *both* `a` and `b` to hold —
    *     union their pairings. Asserted false (`NOT(a AND b)` = `a` is
    *     false `OR` `b` is false, De Morgan) only guarantees *one* side
    *     failed, not which — same "can't guarantee anything" shape as an
    *     asserted-true `OR`, so both contribute `Set.empty`.
    *   - `OR(a, b)` asserted true only guarantees *one* side holds, not
    *     both — `Set.empty`, unchanged from before this method existed.
    *     Asserted false (`NOT(a OR b)` = `a` is false `AND` `b` is false,
    *     De Morgan) requires *both* to fail — union their (negated)
    *     pairings, the dual of the true-`AND` case.
    *   - `NOT(x)` simply flips polarity and recurses — this one rule is
    *     what makes a doubly-`NOT`ed condition (`NOT(NOT(x))`, e.g. from
    *     source SQL written as `NOT (a != b)`, since `!=` itself already
    *     arrives here as `NOT(a = b)`) resolve back to `x`'s own reading
    *     without a separate double-negation special case.
    *   - `Comparison("="/"<=>", col, col)` asserted true is exactly the
    *     match this rule looks for **if `isCrossSideMatch` agrees the two
    *     operands are genuinely on opposite sides**; asserted false
    *     (`NOT(a = b)`, i.e. `a != b` in the source SQL) guarantees the
    *     columns *differ*, the opposite of what's needed — `Set.empty`.
    *   - `Comparison("!=", col, col)` (kept for IR built directly, e.g. by
    *     a future non-Spark front-end or a hand-built test — Spark's own
    *     translator never actually emits this operator string, always
    *     preferring `NOT(EqualTo(...))`, confirmed in
    *     `SparkPlanAdapter.translateExpr`'s `Not`/`BinaryComparison`
    *     cases) is the exact mirror of the `"="`/`"<=>"` case: asserted
    *     false is the genuine match (again gated on `isCrossSideMatch`),
    *     asserted true is not.
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

  /** Whether `a`/`b` are genuinely on opposite sides of the MERGE, not
    * merely two differently-positioned references to columns that happen
    * to share (or differ in) name. A MERGE's `ON` clause only ever
    * involves exactly two relations — target and source — so two
    * *different* qualifiers necessarily means one from each side, without
    * needing to separately determine which qualifier is which: no
    * third relation can appear for a third distinct qualifier to belong
    * to. When either side's qualifier is unknown (a real but rare case —
    * MERGE syntax practically always requires target/source columns to be
    * qualified once resolved, since referencing a same-named column from
    * either side unqualified would itself be an ambiguous reference Spark
    * rejects at analysis time), this stays permissive (`true`) rather
    * than introduce a new false negative for a condition this module
    * simply can't be sure about — the same "unknown, don't guess wrong"
    * posture `SparkPlanAdapter`'s translation-layer fallbacks use
    * elsewhere. Only a *confirmed* same-qualifier match — e.g. a
    * copy-paste bug like `ON t.customer_id = t.customer_id`, comparing a
    * target column to itself rather than to source — is excluded.
    */
  private def isCrossSideMatch(a: ColumnRef, b: ColumnRef): Boolean =
    (a.qualifier, b.qualifier) match {
      case (Some(qa), Some(qb)) => qa != qb
      case _                    => true
    }

  private def checkForbidUnconditionalDelete(mutation: RowMutation): List[Violation] =
    mutation.delete match {
      case DeleteScope.Unconditional =>
        List(
          Violation(
            ViolationType.RuleUnconditionalDelete,
            "contract forbids an unconditional DELETE, but this operation deletes every row it reaches with no filtering predicate",
            remediation =
              "Add a WHERE predicate to the DELETE, or remove the forbid_unconditional_delete rule if deleting every row is intentional."
          )
        )
      case _ => Nil
    }

  private def checkAllowedUpdateColumns(allowedColumns: List[String], mutation: RowMutation): List[Violation] = {
    val allowed = allowedColumns.toSet
    val disallowed = mutation.updatedColumns.filterNot(allowed.contains)
    if (disallowed.isEmpty) Nil
    else
      List(
        Violation(
          ViolationType.RuleDisallowedUpdateColumn,
          s"contract only allows UPDATE to assign ${allowedColumns.mkString(", ")}, but this operation also assigns ${disallowed.mkString(", ")}",
          remediation =
            s"Remove ${disallowed.mkString(", ")} from the UPDATE's SET clause, or add ${disallowed.mkString(", ")} " +
              "to the contract's allowed_update_columns rule if assigning them is intentional.",
          expected = Some(allowedColumns.mkString(", ")),
          actual = Some(mutation.updatedColumns.mkString(", "))
        )
      )
  }
}
