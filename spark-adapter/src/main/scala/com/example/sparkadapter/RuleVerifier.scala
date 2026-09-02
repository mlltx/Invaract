// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.{ContractRule, InterpretedRule}
import com.example.ir.{BooleanExpr, ColumnReference, Comparison, DeleteScope, Expr, RowMutation}

/** Checks a contract's declared DML rules (`com.example.contract.RuleType`)
  * against the structural facts `RowMutationSupport` extracted from one
  * real Spark row-level DML operation. The counterpart to
  * `StructuralVerifier` for exactly the three rule types
  * `ContractRule.interpret` currently understands — not a general
  * rule-expression evaluator. `merge_condition` checks genuine
  * column-to-column equality pairing (see `equalityPairedColumns`), not
  * just "the column is referenced somewhere" — but deeper semantic DML
  * verification (arbitrary predicate logic beyond a flat `AND` of
  * equalities, which specific rows an `UPDATE` touches) remains future
  * work (see ROADMAP.md's "Full semantic DML verification" item).
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
    * must appear as a bare operand of a top-level equality (`=`/`<=>`)
    * conjunct — `t.customer_id = s.customer_id`, or even `t.customer_id
    * = s.cust_id` (source/target column names are allowed to differ; the
    * declared name only has to appear on *one* side) — not merely occur
    * anywhere in the condition. This closes three real false negatives
    * the previous "is it referenced anywhere" check had, each a
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
    * Still a structural approximation, not full predicate logic:
    * `equalityPairedColumns` only descends through top-level `AND`
    * (`&&`) — it doesn't reason about De Morgan equivalences, `NOT`,
    * `CASE WHEN`, or whether the two sides are genuinely target vs.
    * source (as opposed to, say, two target-side columns) — and a
    * condition with *extra* conjuncts beyond the declared columns (an
    * additional partition-pruning predicate, for example) is still not
    * flagged: checking more than required is not the failure this rule
    * guards against.
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

  /** Column names genuinely established as an equality match by `expr` —
    * every name appearing as a bare operand of a top-level `=`/`<=>`
    * `Comparison`, recursively flattening a top-level `BooleanExpr("AND",
    * ...)` (confirmed via `SparkPlanAdapter.translateExpr`'s `And` case).
    * Deliberately does not descend into `OR`, `NOT`, or any other
    * expression kind: only a conjunct that's an unconditional, required
    * part of the match (everything `AND`-ed together must hold) counts,
    * and only a genuine column-to-column comparison (both operands a bare
    * `ColumnReference`) counts as establishing a match — `col = literal`
    * pins to a constant, not to the other side of the merge.
    */
  private def equalityPairedColumns(expr: Expr): Set[String] = expr match {
    case BooleanExpr("AND", List(left, right)) =>
      equalityPairedColumns(left) ++ equalityPairedColumns(right)
    case Comparison(op, ColumnReference(a), ColumnReference(b)) if op == "=" || op == "<=>" =>
      Set(a.name, b.name)
    case _ => Set.empty
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
