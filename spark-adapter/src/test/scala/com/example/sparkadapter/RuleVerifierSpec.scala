// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.{ContractRule, InterpretedRule}
import com.example.ir.{ColumnReference, ColumnRef, DeleteScope, FunctionCall, RowMutation}

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `RuleVerifier`'s inapplicable-rule cases and
  * violation shapes — no Spark session needed, since `RowMutation` and
  * `ContractRule` are both plain data. Real end-to-end PASS/FAIL coverage
  * against a live Delta session lives in `ContractEnforcementRuleSpec`.
  */
class RuleVerifierSpec extends AnyFunSuite {

  private def equalityOn(leftCol: String, rightCol: String) =
    FunctionCall(
      "=",
      List(
        ColumnReference(ColumnRef(leftCol, Some("t"))),
        ColumnReference(ColumnRef(rightCol, Some("s")))
      )
    )

  test("verify returns no violations when no rule is declared") {
    val mutation = RowMutation(updatedColumns = List("id"), delete = DeleteScope.Unconditional)
    assert(RuleVerifier.verify(Nil, mutation).isEmpty)
  }

  test("merge_condition is inapplicable to a mutation with no match condition") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    assert(RuleVerifier.verify(rules, RowMutation()).isEmpty)
  }

  test("merge_condition passes when every declared column is referenced by the match condition") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("id", "id")))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition fails when a declared column isn't referenced by the match condition") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val mutation = RowMutation(matchCondition = Some(equalityOn("id", "id")))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RuleMergeConditionViolation)
    assert(violations.head.message.contains("region"))
  }

  test("merge_condition tolerates a condition referencing extra columns beyond the declared set") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val extraPredicate = FunctionCall("AND", List(equalityOn("id", "id"), ColumnReference(ColumnRef("region", Some("t")))))
    val mutation = RowMutation(matchCondition = Some(extraPredicate))
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
