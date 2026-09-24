// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractRule, InterpretedRule}
import com.invaract.ir.{Aggregate, ColumnRef, Filter, Join, JoinType, Plan}

/** Checks a contract's declared plan-shape rules
  * (`com.invaract.contract.RuleType.PlanShapeTypes`) against a
  * transformation's whole `ir.Plan` — the counterpart to `RuleVerifier`,
  * which checks the other, row-level-DML family (`RuleType.DmlTypes`)
  * against one extracted `ir.RowMutation` instead.
  *
  * The point of checking these *here*, structurally, before Spark executes
  * anything, rather than as a data-quality check against the materialized
  * output afterward: a plan that never groups by the declared keys, or
  * never filters out what it's supposed to, is a fact about the
  * transformation itself — true for every possible input, not something
  * that has to be rediscovered per run by profiling real data after the
  * write already happened. The same "reject before execution" principle
  * `ContractEnforcementRule` already applies to schema/location, applied to
  * a plan's *shape* instead.
  *
  * Each rule is checked against *every* matching node anywhere in the plan,
  * not just at the root — a single write's plan can itself contain several
  * `Aggregate`/`Join`/`Filter` nodes (e.g. an aggregate feeding into a
  * join) — and is satisfied if *any* node of the relevant kind satisfies
  * it. Deliberately narrow, mirroring `RuleVerifier`'s own scope: this
  * checks plan *shape* (does an aggregation exist with the right grouping
  * keys, does a join condition exist matching the right columns, does a
  * filter exist referencing the right column), not full predicate logic —
  * whether a filter's predicate is actually correct, or which specific rows
  * survive it, is not modeled (see ROADMAP.md's "Transformation checks
  * beyond structural" item).
  *
  * No `customRuleTypes` escape hatch yet, unlike `RuleVerifier`: a
  * plan-shape custom rule type would need its own extension trait (this
  * module's `CustomRuleVerifier` is shaped around `RowMutation`, not
  * `Plan`), left as future work rather than widened speculatively here.
  */
private[sparkadapter] object PlanRuleVerifier {

  def verify(rules: List[ContractRule], plan: Plan): List[Violation] =
    rules.flatMap(rule => rule.interpret.toList.flatMap(checkOne(_, plan)))

  private def checkOne(rule: InterpretedRule, plan: Plan): List[Violation] = rule match {
    case InterpretedRule.RequiredGroupBy(columns)       => checkRequiredGroupBy(columns, plan)
    case InterpretedRule.ForbidCrossJoin                => checkForbidCrossJoin(plan)
    case InterpretedRule.RequiredJoinColumns(columns)   => checkRequiredJoinColumns(columns, plan)
    case InterpretedRule.RequiredFilterColumns(columns) => checkRequiredFilterColumns(columns, plan)
    // A row-level-DML InterpretedRule (RuleVerifier's own concern) reaching
    // here is not a bug - ContractEnforcementRule passes every declared
    // rule to both verifiers, since which family a rule belongs to is
    // decided by its *interpreted shape*, not by a separate up-front
    // dispatch table the two verifiers would otherwise have to agree on.
    case _: InterpretedRule.MergeCondition         => Nil
    case InterpretedRule.ForbidUnconditionalDelete => Nil
    case _: InterpretedRule.AllowedUpdateColumns   => Nil
  }

  private def collectAggregates(plan: Plan): List[Aggregate] =
    (plan match { case a: Aggregate => List(a); case _ => Nil }) ++ plan.children.flatMap(collectAggregates)

  private def collectJoins(plan: Plan): List[Join] =
    (plan match { case j: Join => List(j); case _ => Nil }) ++ plan.children.flatMap(collectJoins)

  private def collectFilters(plan: Plan): List[Filter] =
    (plan match { case f: Filter => List(f); case _ => Nil }) ++ plan.children.flatMap(collectFilters)

  /** Every column referenced in any `Filter` condition or `Join` condition
    * anywhere in `plan` — shared by this object's own `required_filter_columns`
    * check (via `collectFilters` above) and by `ContractInference`/
    * `RoleConsistencyVerifier`'s dry-run/role-consistency usage observation,
    * which need the identical "was this column read only to gate/match
    * rows, never to compute output data" signal — written once here rather
    * than reimplemented per caller.
    */
  private[sparkadapter] def collectConditionReferences(plan: Plan): Set[ColumnRef] = {
    val filterRefs = collectFilters(plan).flatMap(_.condition.references)
    val joinRefs = collectJoins(plan).flatMap(_.condition.toList.flatMap(_.references))
    (filterRefs ++ joinRefs).toSet
  }

  /** Satisfied when at least one `Aggregate` node's `groupBy` resolves (via
    * `Expr.references`) to a column-name set that's a superset of the
    * declared columns — grouping by more than required (e.g. an extra
    * partition key) still satisfies "grouped by X."
    */
  private def checkRequiredGroupBy(columns: List[String], plan: Plan): List[Violation] = {
    val aggregates = collectAggregates(plan)
    val required = columns.toSet
    val groupings = aggregates.map(_.groupBy.flatMap(_.references).map(_.name).toSet)
    if (groupings.exists(required.subsetOf))
      Nil
    else
      List(
        Violation(
          ViolationType.RuleRequiredGroupByViolation,
          if (aggregates.isEmpty)
            s"contract requires the output to be grouped by ${columns.mkString(", ")}, but the plan performs no aggregation at all"
          else
            s"contract requires the output to be grouped by ${columns.mkString(", ")}, but no aggregation in the plan groups by all of them",
          remediation =
            s"Add a GROUP BY on ${columns.mkString(", ")} to the transformation, or update the contract's " +
              "required_group_by rule if grouping by fewer/different columns is intentional.",
          expected = Some(columns.mkString(", ")),
          actual = Some(if (groupings.isEmpty) "no aggregation" else groupings.map(_.mkString("+")).mkString("; "))
        )
      )
  }

  /** Satisfied when the plan contains no join that's a cartesian product —
    * `JoinType.Cross`, or any join with no condition at all (Spark reports
    * a plain, condition-less `.join(other)` this way too, not only an
    * explicit `.crossJoin(other)` — see `SparkPlanAdapter`'s `Join`
    * translation).
    */
  private def checkForbidCrossJoin(plan: Plan): List[Violation] = {
    val crossJoins = collectJoins(plan).filter(j => j.joinType == JoinType.Cross || j.condition.isEmpty)
    if (crossJoins.isEmpty)
      Nil
    else
      List(
        Violation(
          ViolationType.RuleCrossJoinViolation,
          s"contract forbids a cartesian-product join, but the plan contains ${crossJoins.size} join(s) with no condition",
          remediation =
            "Add a join condition to every join in the transformation, or remove the forbid_cross_join rule if a cartesian product is intentional."
        )
      )
  }

  /** Satisfied when at least one `Join` node's `condition` establishes a
    * required equality match (via `EqualityConditions.equalityPairedColumns`
    * — the same De Morgan-/`NOT`-aware, side-distinguishing logic
    * `RuleVerifier`'s `merge_condition` rule uses for a MERGE's `ON`
    * condition) covering every declared column.
    */
  private def checkRequiredJoinColumns(columns: List[String], plan: Plan): List[Violation] = {
    val allJoins = collectJoins(plan)
    val required = columns.toSet
    val satisfied = allJoins.exists(_.condition.exists(cond => required.subsetOf(EqualityConditions.equalityPairedColumns(cond))))
    if (satisfied)
      Nil
    else
      List(
        Violation(
          ViolationType.RuleRequiredJoinColumnsViolation,
          if (allJoins.isEmpty)
            s"contract requires a join matching on ${columns.mkString(", ")}, but the plan contains no join at all"
          else
            s"contract requires a join matching on ${columns.mkString(", ")}, but no join's condition establishes an equality match on all of them",
          remediation =
            s"Add a 'left.${columns.head} = right.${columns.head}'-style equality to a join's condition for " +
              s"${columns.mkString(", ")}, or update the contract's required_join_columns rule if matching on " +
              "fewer/different columns is intentional.",
          expected = Some(columns.mkString(", "))
        )
      )
  }

  /** Satisfied when every declared column is referenced by at least one
    * `Filter` node's condition somewhere in the plan — deliberately not
    * gated on any particular predicate shape (unlike the equality-pairing
    * the join/merge rules require): "the plan must filter on this column"
    * is satisfied by `col IS NOT NULL`, `col != 'deleted'`, `col > 0`, or
    * anything else that reads the column inside a `Filter`.
    */
  private def checkRequiredFilterColumns(columns: List[String], plan: Plan): List[Violation] = {
    val filteredColumns = collectFilters(plan).flatMap(_.condition.references).map(_.name).toSet
    val missing = columns.filterNot(filteredColumns.contains)
    if (missing.isEmpty)
      Nil
    else
      List(
        Violation(
          ViolationType.RuleRequiredFilterColumnsViolation,
          s"contract requires the plan to filter on ${columns.mkString(", ")}, but no filter references ${missing.mkString(", ")}",
          remediation =
            s"Add a filter referencing ${missing.mkString(", ")} to the transformation, or update the contract's " +
              "required_filter_columns rule if filtering on fewer columns is intentional.",
          expected = Some(columns.mkString(", ")),
          actual = Some(filteredColumns.mkString(", "))
        )
      )
  }
}
