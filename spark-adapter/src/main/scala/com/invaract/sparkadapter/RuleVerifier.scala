// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractRule, InterpretedRule, RuleType}
import com.invaract.ir.{DeleteScope, RowMutation}

/** Checks a contract's declared row-level-DML rules
  * (`com.invaract.contract.RuleType.DmlTypes`) against the structural facts
  * `RowMutationSupport` extracted from one real Spark row-level DML
  * operation — the counterpart to `PlanRuleVerifier`, which checks the
  * other, plan-shape family (`RuleType.PlanShapeTypes`) against a whole
  * `ir.Plan` instead. Neither is a general rule-expression evaluator.
  * `merge_condition` checks genuine, De Morgan-/`NOT`-aware,
  * target-vs-source-aware column-to-column equality pairing (see
  * `EqualityConditions`, shared with `PlanRuleVerifier`'s
  * `required_join_columns`), not just "the column is referenced
  * somewhere" — but deeper semantic DML verification (a `CASE
  * WHEN`-conditional match, which specific rows an `UPDATE` touches)
  * remains future work (see ROADMAP.md's "Full semantic DML verification"
  * item).
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

  /** The three built-in `RuleType`s, each an ordinary `CustomRuleVerifier`
    * — the identical trait a third party's own DML rule type implements
    * via `Contract.customRuleTypes`. Nothing about a built-in type's
    * *verification* is privileged anymore; what makes it "built-in" is
    * only that it ships compiled into this module and `resolveVerifier`
    * checks this map before `customRuleTypes` is ever consulted, not a
    * separate dispatch mechanism. Each still starts from `rule.interpret`,
    * the same parsed/validated `InterpretedRule` shape `ContractValidator`'s
    * malformed-properties check already relies on.
    */
  private object MergeConditionVerifier extends CustomRuleVerifier {
    override def appliesTo(kind: MutationKind): Boolean = kind == MutationKind.Merge
    override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] = rule.interpret match {
      case Some(InterpretedRule.MergeCondition(columns)) => checkMergeCondition(columns, mutation)
      case _                                             => Nil
    }
  }

  private object ForbidUnconditionalDeleteVerifier extends CustomRuleVerifier {
    override def appliesTo(kind: MutationKind): Boolean = kind == MutationKind.Delete
    override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] = rule.interpret match {
      case Some(InterpretedRule.ForbidUnconditionalDelete) => checkForbidUnconditionalDelete(mutation)
      case _                                               => Nil
    }
  }

  private object AllowedUpdateColumnsVerifier extends CustomRuleVerifier {
    override def appliesTo(kind: MutationKind): Boolean = kind == MutationKind.Update
    override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] = rule.interpret match {
      case Some(InterpretedRule.AllowedUpdateColumns(columns)) => checkAllowedUpdateColumns(columns, mutation)
      case _                                                   => Nil
    }
  }

  private val builtinVerifiers: Map[String, CustomRuleVerifier] = Map(
    RuleType.MergeCondition -> MergeConditionVerifier,
    RuleType.ForbidUnconditionalDelete -> ForbidUnconditionalDeleteVerifier,
    RuleType.AllowedUpdateColumns -> AllowedUpdateColumnsVerifier
  )

  private def toMutationKind(kind: RowMutationSupport.Kind): MutationKind = kind match {
    case RowMutationSupport.Kind.Merge  => MutationKind.Merge
    case RowMutationSupport.Kind.Update => MutationKind.Update
    case RowMutationSupport.Kind.Delete => MutationKind.Delete
  }

  /** Looks up the `CustomRuleVerifier` that verifies `ruleType` — a
    * built-in `RuleType` (`builtinVerifiers`, above) always wins when
    * present; `customRuleTypes`'s reflective escape hatch is consulted
    * only when `ruleType` matches neither. A `customRuleTypes` entry
    * colliding with a built-in `ruleType` is therefore dead — the
    * built-in lookup succeeds first (see `ContractValidator`'s matching
    * Warning). Deliberately total, never throws: neither a `ruleType`
    * matching neither set nor a `customRuleTypes` entry naming an
    * unresolvable class is ours to crash a real job over —
    * `ContractEnforcementRule.requireValidContract`'s job to flag that.
    */
  private def resolveVerifier(ruleType: String, customRuleTypes: Map[String, String]): Option[CustomRuleVerifier] =
    builtinVerifiers.get(ruleType).orElse {
      customRuleTypes.get(ruleType).flatMap(className => CustomRuleVerifierFactory.tryResolve(className).toOption)
    }

  /** Whether `rule` is the kind of rule `RowMutationSupport.Classification.Unverifiable(kind)`
    * would need to check — used by `ContractEnforcementRule` to decide
    * whether an operation this module recognized as DML-shaped but
    * couldn't extract facts for is actually a problem for *this*
    * contract, or just an operation kind it happens not to declare any
    * rule for (in which case there's nothing to fail closed over).
    * Preserved for direct unit coverage against an already-`interpret`ed
    * `InterpretedRule`, and doubling as a compile-time completeness guard:
    * this is the one remaining exhaustive `match` over `InterpretedRule`'s
    * subtypes, so a new built-in rule type that isn't also wired into
    * `builtinVerifiers` fails to compile here rather than silently
    * never applying. `anyRuleAppliesTo`, below, is what
    * `ContractEnforcementRule` actually calls, since it also has to reach
    * a *custom* rule type, whose `ContractRule.interpret` is always `None`.
    * The four plan-shape `InterpretedRule`s (`PlanRuleVerifier`'s own
    * concern, checked against a whole `Plan`, not a `RowMutation`) always
    * answer `false` here — this question is specifically "does an
    * *unverifiable DML* classification need to fail closed over this
    * rule," which a plan-shape rule is never the reason for.
    */
  def appliesTo(rule: InterpretedRule, kind: RowMutationSupport.Kind): Boolean = rule match {
    case _: InterpretedRule.MergeCondition         => kind == RowMutationSupport.Kind.Merge
    case InterpretedRule.ForbidUnconditionalDelete => kind == RowMutationSupport.Kind.Delete
    case _: InterpretedRule.AllowedUpdateColumns   => kind == RowMutationSupport.Kind.Update
    case _: InterpretedRule.RequiredGroupBy        => false
    case InterpretedRule.ForbidCrossJoin           => false
    case _: InterpretedRule.RequiredJoinColumns    => false
    case _: InterpretedRule.RequiredFilterColumns  => false
  }

  /** Whether any of `rules` — built-in or, via `customRuleTypes`, custom —
    * applies to `kind`. The same "fail closed on an Unverifiable
    * classification only when a declared rule actually cares about this
    * DML kind" decision `appliesTo` makes for one already-interpreted
    * built-in rule, extended to a whole rule list and to custom rule
    * types: a rule whose `ruleType` `resolveVerifier` can't resolve at
    * all (unrecognized, or a `customRuleTypes` entry naming an
    * unresolvable class) simply never contributes `true` here, the same
    * total/safe behavior `verify` below has.
    */
  def anyRuleAppliesTo(
      rules: List[ContractRule],
      kind: RowMutationSupport.Kind,
      customRuleTypes: Map[String, String] = Map.empty
  ): Boolean = {
    val mutationKind = toMutationKind(kind)
    rules.exists(rule => resolveVerifier(rule.ruleType, customRuleTypes).exists(_.appliesTo(mutationKind)))
  }

  def verify(rules: List[ContractRule], mutation: RowMutation, customRuleTypes: Map[String, String] = Map.empty): List[Violation] =
    rules.flatMap(rule => resolveVerifier(rule.ruleType, customRuleTypes).map(_.verify(rule, mutation)).getOrElse(Nil))

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
        val paired = EqualityConditions.equalityPairedColumns(condition)
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
