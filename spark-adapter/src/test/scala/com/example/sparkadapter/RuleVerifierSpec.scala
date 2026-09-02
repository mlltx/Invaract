// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.{ContractRule, InterpretedRule}
import com.example.ir.{BooleanExpr, ColumnReference, ColumnRef, Comparison, DeleteScope, Literal, RowMutation}

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `RuleVerifier`'s inapplicable-rule cases and
  * violation shapes — no Spark session needed, since `RowMutation` and
  * `ContractRule` are both plain data. Real end-to-end PASS/FAIL coverage
  * against a live Delta session lives in `ContractEnforcementRuleSpec`.
  */
class RuleVerifierSpec extends AnyFunSuite {

  private def equalityOn(leftCol: String, rightCol: String) =
    Comparison(
      "=",
      ColumnReference(ColumnRef(leftCol, Some("t"))),
      ColumnReference(ColumnRef(rightCol, Some("s")))
    )

  test("verify returns no violations when no rule is declared") {
    val mutation = RowMutation(updatedColumns = List("id"), delete = DeleteScope.Unconditional)
    assert(RuleVerifier.verify(Nil, mutation).isEmpty)
  }

  test("merge_condition is inapplicable to a mutation with no match condition") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    assert(RuleVerifier.verify(rules, RowMutation()).isEmpty)
  }

  test("merge_condition passes when every declared column is equality-paired in the match condition") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("id", "id")))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition passes via null-safe equality (<=>), not just plain =") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val nullSafeEq = Comparison("<=>", ColumnReference(ColumnRef("id", Some("t"))), ColumnReference(ColumnRef("id", Some("s"))))
    val mutation = RowMutation(matchCondition = Some(nullSafeEq))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition fails when a declared column has no equality pairing at all") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("id", "id")))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleMergeConditionViolation)
    assert(violations.head.message.contains("region"))
    assert(violations.head.actual.contains("id"), "'id' was genuinely paired and should be reported as such")
  }

  test("merge_condition tolerates an extra, non-equality conjunct beyond the declared columns") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val extraConjunct = Comparison(">", ColumnReference(ColumnRef("created_date", Some("t"))), Literal("2024-01-01", "string"))
    val condition = BooleanExpr("AND", List(equalityOn("id", "id"), extraConjunct))
    val mutation = RowMutation(matchCondition = Some(condition))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition allows a declared column to be paired with a differently-named source column") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("customer_id"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("customer_id", "cust_id")))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition also accepts the source-side name from a cross-named pairing") {
    // The pairing above (customer_id = cust_id) establishes both names -
    // a contract could equally have been authored against the source's
    // own naming.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("cust_id"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("customer_id", "cust_id")))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition fails when a declared column is only checked by a range comparison, not an equality") {
    // Real gap the old "referenced anywhere" check missed: customer_id
    // appears in the condition, but nothing actually matches target
    // against source on it.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("customer_id", "id"))))
    val rangeCheck = Comparison(">", ColumnReference(ColumnRef("customer_id", Some("t"))), Literal(0, "integer"))
    val condition = BooleanExpr("AND", List(rangeCheck, equalityOn("id", "id")))
    val mutation = RowMutation(matchCondition = Some(condition))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("customer_id"))
  }

  test("merge_condition fails when a declared column is only compared to a literal, not another column") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("customer_id"))))
    val literalEquality = Comparison("=", ColumnReference(ColumnRef("customer_id", Some("t"))), Literal("ACME", "string"))
    val mutation = RowMutation(matchCondition = Some(literalEquality))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("customer_id"))
  }

  test("merge_condition fails when the only equality is inside an OR branch, not a required conjunct") {
    // Only one of the two needs to hold - a strictly weaker guarantee
    // than declaring both columns intends.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val condition = BooleanExpr("OR", List(equalityOn("id", "id"), equalityOn("region", "region")))
    val mutation = RowMutation(matchCondition = Some(condition))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("id"))
    assert(violations.head.message.contains("region"))
  }

  test("merge_condition handles a nested (three-way) AND conjunction") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("a", "b", "c"))))
    val nested = BooleanExpr("AND", List(BooleanExpr("AND", List(equalityOn("a", "a"), equalityOn("b", "b"))), equalityOn("c", "c")))
    val mutation = RowMutation(matchCondition = Some(nested))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("forbid_unconditional_delete is inapplicable to a mutation with no delete") {
    val rules = List(ContractRule("forbid_unconditional_delete", Map.empty))
    assert(RuleVerifier.verify(rules, RowMutation(delete = DeleteScope.NotApplicable)).isEmpty)
  }

  test("forbid_unconditional_delete passes for a conditional delete") {
    val rules = List(ContractRule("forbid_unconditional_delete", Map.empty))
    val mutation = RowMutation(delete = DeleteScope.Conditional(ColumnReference(ColumnRef("is_archived"))))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("forbid_unconditional_delete fails for an unconditional delete") {
    val rules = List(ContractRule("forbid_unconditional_delete", Map.empty))
    val violations = RuleVerifier.verify(rules, RowMutation(delete = DeleteScope.Unconditional))
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleUnconditionalDelete)
  }

  test("allowed_update_columns is inapplicable to a mutation that updates no columns") {
    val rules = List(ContractRule("allowed_update_columns", Map("columns" -> java.util.Arrays.asList("status"))))
    assert(RuleVerifier.verify(rules, RowMutation(updatedColumns = Nil)).isEmpty)
  }

  test("allowed_update_columns passes when every updated column is allowed") {
    val rules = List(ContractRule("allowed_update_columns", Map("columns" -> java.util.Arrays.asList("status", "updated_at"))))
    val mutation = RowMutation(updatedColumns = List("status"))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("allowed_update_columns fails when an updated column isn't allowed") {
    val rules = List(ContractRule("allowed_update_columns", Map("columns" -> java.util.Arrays.asList("status"))))
    val mutation = RowMutation(updatedColumns = List("status", "id"))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleDisallowedUpdateColumn)
    assert(violations.head.message.contains("id"))
  }

  test("an unrecognized or malformed rule contributes no violations") {
    val rules = List(
      ContractRule("compatibility", Map("mode" -> "backward")),
      ContractRule("merge_condition", Map.empty) // malformed: no 'columns'
    )
    val mutation = RowMutation(matchCondition = Some(equalityOn("id", "id")), updatedColumns = List("anything"))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  // --- appliesTo: decides whether an Unverifiable(kind) classification
  // is actually a problem for a given contract (RULE_UNVERIFIABLE_DML),
  // or an operation kind the contract simply declares no rule for. ---

  test("appliesTo: merge_condition applies only to Kind.Merge") {
    val rule = InterpretedRule.MergeCondition(List("id"))
    assert(RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Merge))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Update))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Delete))
  }

  test("appliesTo: forbid_unconditional_delete applies only to Kind.Delete") {
    val rule = InterpretedRule.ForbidUnconditionalDelete
    assert(RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Delete))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Merge))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Update))
  }

  test("appliesTo: allowed_update_columns applies only to Kind.Update") {
    val rule = InterpretedRule.AllowedUpdateColumns(List("status"))
    assert(RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Update))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Merge))
    assert(!RuleVerifier.appliesTo(rule, RowMutationSupport.Kind.Delete))
  }
}
