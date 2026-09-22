// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractRule
import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `PlanRuleVerifier`'s four plan-shape rule types —
  * no Spark session needed, since `ir.Plan`/`ir.Expr` are both plain data.
  * Real end-to-end PASS/FAIL coverage against a live Spark session (proving
  * the wiring through `ContractEnforcementRule` actually blocks a bad
  * write) lives in `ContractEnforcementRuleSpec`.
  */
class PlanRuleVerifierSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None): ColumnReference =
    ColumnReference(ColumnRef(name, qualifier))

  private def equalityOn(leftCol: String, rightCol: String, leftQualifier: String = "l", rightQualifier: String = "r") =
    Comparison("=", col(leftCol, Some(leftQualifier)), col(rightCol, Some(rightQualifier)))

  private def baseRead = Read(DatasetRef("raw.source"))

  test("verify returns no violations when no rule is declared") {
    assert(PlanRuleVerifier.verify(Nil, baseRead).isEmpty)
  }

  test("verify contributes nothing for a DML-shaped rule (RuleVerifier's own concern)") {
    val rules = List(
      ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))),
      ContractRule("forbid_unconditional_delete", Map.empty),
      ContractRule("allowed_update_columns", Map("columns" -> java.util.Arrays.asList("status")))
    )
    assert(PlanRuleVerifier.verify(rules, baseRead).isEmpty)
  }

  test("verify contributes nothing for an unrecognized or malformed rule type") {
    val rules = List(
      ContractRule("compatibility", Map("mode" -> "backward")),
      ContractRule("required_group_by", Map.empty) // malformed: no 'columns'
    )
    assert(PlanRuleVerifier.verify(rules, baseRead).isEmpty)
  }

  // --- required_group_by ---------------------------------------------------

  private def requiredGroupBy(columns: String*) =
    List(ContractRule("required_group_by", Map("columns" -> java.util.Arrays.asList(columns: _*))))

  test("required_group_by fails when the plan performs no aggregation at all") {
    val violations = PlanRuleVerifier.verify(requiredGroupBy("customer_id"), baseRead)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredGroupByViolation)
    assert(violations.head.message.contains("no aggregation"))
  }

  test("required_group_by passes when an Aggregate groups by exactly the declared column") {
    val plan = Aggregate(baseRead, groupBy = List(col("customer_id")), aggregates = List(NamedExpr("customer_id", col("customer_id"))))
    assert(PlanRuleVerifier.verify(requiredGroupBy("customer_id"), plan).isEmpty)
  }

  test("required_group_by passes when an Aggregate groups by a superset of the declared columns") {
    val plan = Aggregate(
      baseRead,
      groupBy = List(col("customer_id"), col("region")),
      aggregates = List(NamedExpr("customer_id", col("customer_id")), NamedExpr("region", col("region")))
    )
    assert(PlanRuleVerifier.verify(requiredGroupBy("customer_id"), plan).isEmpty)
  }

  test("required_group_by fails when an Aggregate exists but doesn't group by every declared column") {
    val plan = Aggregate(baseRead, groupBy = List(col("region")), aggregates = List(NamedExpr("region", col("region"))))
    val violations = PlanRuleVerifier.verify(requiredGroupBy("customer_id", "region"), plan)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredGroupByViolation)
    assert(!violations.head.message.contains("no aggregation at all"), "an aggregation exists, just not the right one")
  }

  test("required_group_by finds an Aggregate nested beneath other plan nodes") {
    val agg = Aggregate(baseRead, groupBy = List(col("customer_id")), aggregates = List(NamedExpr("customer_id", col("customer_id"))))
    val plan = Sort(Filter(agg, Comparison(">", col("customer_id"), Literal(0, "integer"))), order = Nil)
    assert(PlanRuleVerifier.verify(requiredGroupBy("customer_id"), plan).isEmpty)
  }

  test("required_group_by passes if any one of several Aggregates satisfies it") {
    val wrongAgg = Aggregate(baseRead, groupBy = List(col("region")), aggregates = List(NamedExpr("region", col("region"))))
    val rightAgg = Aggregate(baseRead, groupBy = List(col("customer_id")), aggregates = List(NamedExpr("customer_id", col("customer_id"))))
    val plan = Union(List(wrongAgg, rightAgg))
    assert(PlanRuleVerifier.verify(requiredGroupBy("customer_id"), plan).isEmpty)
  }

  // --- forbid_cross_join -----------------------------------------------------

  private val forbidCrossJoin = List(ContractRule("forbid_cross_join", Map.empty))

  test("forbid_cross_join passes when the plan contains no join at all") {
    assert(PlanRuleVerifier.verify(forbidCrossJoin, baseRead).isEmpty)
  }

  test("forbid_cross_join passes for an ordinary conditioned Inner join") {
    val plan = Join(baseRead, baseRead, JoinType.Inner, Some(equalityOn("id", "id")))
    assert(PlanRuleVerifier.verify(forbidCrossJoin, plan).isEmpty)
  }

  test("forbid_cross_join fails for an explicit JoinType.Cross") {
    val plan = Join(baseRead, baseRead, JoinType.Cross, None)
    val violations = PlanRuleVerifier.verify(forbidCrossJoin, plan)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleCrossJoinViolation)
  }

  test("forbid_cross_join fails for a join with no condition at all, regardless of declared type") {
    val plan = Join(baseRead, baseRead, JoinType.Inner, condition = None)
    val violations = PlanRuleVerifier.verify(forbidCrossJoin, plan)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleCrossJoinViolation)
  }

  test("forbid_cross_join finds a cross join nested beneath other plan nodes") {
    val crossJoin = Join(baseRead, baseRead, JoinType.Cross, None)
    val plan = Project(crossJoin, List(NamedExpr("id", col("id"))))
    assert(PlanRuleVerifier.verify(forbidCrossJoin, plan).nonEmpty)
  }

  // --- required_join_columns --------------------------------------------------

  private def requiredJoinColumns(columns: String*) =
    List(ContractRule("required_join_columns", Map("columns" -> java.util.Arrays.asList(columns: _*))))

  test("required_join_columns fails when the plan contains no join at all") {
    val violations = PlanRuleVerifier.verify(requiredJoinColumns("id"), baseRead)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredJoinColumnsViolation)
    assert(violations.head.message.contains("no join at all"))
  }

  test("required_join_columns passes when a join's condition equality-pairs the declared column") {
    val plan = Join(baseRead, baseRead, JoinType.Inner, Some(equalityOn("id", "id")))
    assert(PlanRuleVerifier.verify(requiredJoinColumns("id"), plan).isEmpty)
  }

  test("required_join_columns fails when a join exists but its condition doesn't pair every declared column") {
    val plan = Join(baseRead, baseRead, JoinType.Inner, Some(equalityOn("id", "id")))
    val violations = PlanRuleVerifier.verify(requiredJoinColumns("id", "region"), plan)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredJoinColumnsViolation)
    assert(!violations.head.message.contains("no join at all"), "a join exists, just not one matching on the right columns")
  }

  test("required_join_columns is not satisfied by a column merely referenced, not equality-paired (reuses EqualityConditions)") {
    // A range check, not an equality - the same false-negative EqualityConditions
    // already guards merge_condition against.
    val rangeCheck = Comparison(">", col("id", Some("l")), Literal(0, "integer"))
    val plan = Join(baseRead, baseRead, JoinType.Inner, Some(rangeCheck))
    assert(PlanRuleVerifier.verify(requiredJoinColumns("id"), plan).nonEmpty)
  }

  test("required_join_columns passes if any one of several joins satisfies it") {
    val wrongJoin = Join(baseRead, baseRead, JoinType.Inner, Some(equalityOn("region", "region")))
    val rightJoin = Join(baseRead, baseRead, JoinType.Inner, Some(equalityOn("id", "id")))
    val plan = Union(List(wrongJoin, rightJoin))
    assert(PlanRuleVerifier.verify(requiredJoinColumns("id"), plan).isEmpty)
  }

  // --- required_filter_columns -------------------------------------------------

  private def requiredFilterColumns(columns: String*) =
    List(ContractRule("required_filter_columns", Map("columns" -> java.util.Arrays.asList(columns: _*))))

  test("required_filter_columns fails when the plan contains no filter at all") {
    val violations = PlanRuleVerifier.verify(requiredFilterColumns("is_deleted"), baseRead)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredFilterColumnsViolation)
  }

  test("required_filter_columns passes when a Filter's condition references the declared column") {
    val plan = Filter(baseRead, Comparison("=", col("is_deleted"), Literal(false, "boolean")))
    assert(PlanRuleVerifier.verify(requiredFilterColumns("is_deleted"), plan).isEmpty)
  }

  test("required_filter_columns is satisfied by any predicate shape referencing the column, not just equality") {
    val plan = Filter(baseRead, Function("isnotnull", List(col("is_deleted"))))
    assert(PlanRuleVerifier.verify(requiredFilterColumns("is_deleted"), plan).isEmpty)
  }

  test("required_filter_columns fails when a Filter exists but doesn't reference every declared column") {
    val plan = Filter(baseRead, Comparison("=", col("region"), Literal("US", "string")))
    val violations = PlanRuleVerifier.verify(requiredFilterColumns("is_deleted"), plan)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleRequiredFilterColumnsViolation)
    assert(violations.head.message.contains("is_deleted"))
  }

  test("required_filter_columns is satisfied by declared columns spread across multiple filters") {
    val innerFilter = Filter(baseRead, Comparison("=", col("is_deleted"), Literal(false, "boolean")))
    val outerFilter = Filter(innerFilter, Comparison(">", col("value"), Literal(0, "integer")))
    assert(PlanRuleVerifier.verify(requiredFilterColumns("is_deleted", "value"), outerFilter).isEmpty)
  }
}
