// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.{ContractRule, InterpretedRule}
import com.example.ir.{DeleteScope, RowMutation}

/** Checks a contract's declared DML rules (`com.example.contract.RuleType`)
  * against the structural facts `RowMutationSupport` extracted from one
  * real Spark row-level DML operation. The counterpart to
  * `StructuralVerifier` for exactly the three rule types
  * `ContractRule.interpret` currently understands — not a general
  * rule-expression evaluator; deeper semantic DML verification (a MERGE's
  * full predicate logic, which specific rows an UPDATE touches) remains
  * future work (see ROADMAP.md's "Full semantic DML verification" item).
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

  def verify(rules: List[ContractRule], mutation: RowMutation): List[Violation] =
    rules.flatMap(_.interpret).flatMap {
      case InterpretedRule.MergeCondition(columns)       => checkMergeCondition(columns, mutation)
      case InterpretedRule.ForbidUnconditionalDelete     => checkForbidUnconditionalDelete(mutation)
      case InterpretedRule.AllowedUpdateColumns(columns) => checkAllowedUpdateColumns(columns, mutation)
    }

  /** Structural approximation, not full predicate logic: this checks that
    * every declared column is *referenced somewhere* in the MERGE's `ON`
    * condition — enough to catch the real, common bug this rule exists
    * for (a MERGE silently dropping a match key, e.g. matching only on
    * `order_id` in a multi-tenant table that should also match on
    * `customer_id`) — not that those are the *only* columns referenced,
    * nor that each forms a genuine `target.col = source.col` equality
    * pair rather than, say, appearing only on one side of an unrelated
    * predicate. A condition referencing extra columns beyond the
    * declared set (an additional partition-pruning predicate, for
    * example) is not flagged — checking more than required is not the
    * failure this rule guards against.
    */
  private def checkMergeCondition(declaredColumns: List[String], mutation: RowMutation): List[Violation] =
    mutation.matchCondition match {
      case None => Nil
      case Some(condition) =>
        val referenced = condition.references.map(_.name)
        val missing = declaredColumns.filterNot(referenced.contains)
        if (missing.isEmpty) Nil
        else
          List(
            Violation(
              ViolationType.RuleMergeConditionViolation,
              s"contract requires the MERGE to match on ${declaredColumns.mkString(", ")}, but its ON condition " +
                s"does not reference ${missing.mkString(", ")}",
              remediation =
                s"Add ${missing.mkString(", ")} to the MERGE's ON condition, or update the contract's " +
                  "merge_condition rule if matching on fewer columns is intentional.",
              expected = Some(declaredColumns.mkString(", ")),
              actual = Some(referenced.mkString(", "))
            )
          )
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
