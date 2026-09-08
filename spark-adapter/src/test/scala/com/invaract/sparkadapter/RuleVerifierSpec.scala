// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractRule, InterpretedRule}
import com.invaract.ir.{BooleanExpr, ColumnReference, ColumnRef, Comparison, Conditional, DeleteScope, Literal, RowMutation}

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

  // `!=` never actually arrives from Spark as a `Comparison("!=", ...)` —
  // Catalyst always represents it as `Not(EqualTo(...))` (see
  // SparkPlanAdapter.translateExpr) — but RuleVerifier still understands
  // it directly, both because the IR is meant to be engine-independent
  // and to keep these tests reading the way the source SQL would.
  private def inequalityOn(leftCol: String, rightCol: String) =
    Comparison(
      "!=",
      ColumnReference(ColumnRef(leftCol, Some("t"))),
      ColumnReference(ColumnRef(rightCol, Some("s")))
    )

  private def not(expr: com.invaract.ir.Expr) = BooleanExpr("NOT", List(expr))

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

  test("merge_condition passes via NOT(!=), the double-negation form of an equality") {
    // NOT (t.id != s.id) is logically identical to t.id = s.id - and since
    // Spark itself always represents `!=` as Not(EqualTo(...)), this is
    // literally what Spark's own translated IR looks like for source SQL
    // written as `ON NOT (t.id != s.id)`.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val mutation = RowMutation(matchCondition = Some(not(inequalityOn("id", "id"))))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition passes via NOT(OR(!=, !=)), the De Morgan form of an AND of equalities") {
    // NOT (t.id != s.id OR t.region != s.region) is De Morgan-equivalent
    // to t.id = s.id AND t.region = s.region - a genuine double match.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val condition = not(BooleanExpr("OR", List(inequalityOn("id", "id"), inequalityOn("region", "region"))))
    val mutation = RowMutation(matchCondition = Some(condition))
    assert(RuleVerifier.verify(rules, mutation).isEmpty)
  }

  test("merge_condition fails via NOT(AND(!=, !=)) - De Morgan only guarantees one side, not both") {
    // NOT (t.id != s.id AND t.region != s.region) is De Morgan-equivalent
    // to t.id = s.id OR t.region = s.region - only one side is guaranteed,
    // the same weaker-guarantee problem a bare OR already fails on.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val condition = not(BooleanExpr("AND", List(inequalityOn("id", "id"), inequalityOn("region", "region"))))
    val mutation = RowMutation(matchCondition = Some(condition))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("id"))
    assert(violations.head.message.contains("region"))
  }

  test("merge_condition fails when the equality itself is negated (NOT of an equality is not a match)") {
    // NOT (t.id = s.id) genuinely means the columns must differ - the
    // opposite of what merge_condition requires.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val mutation = RowMutation(matchCondition = Some(not(equalityOn("id", "id"))))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("id"))
  }

  test("merge_condition handles a triple-negated equality (NOT(NOT(NOT(=))) is still NOT(=))") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val mutation = RowMutation(matchCondition = Some(not(not(not(equalityOn("id", "id"))))))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1, "an odd number of NOTs must not be mistaken for a match")
  }

  test("merge_condition fails when the only equality is inside a CASE WHEN - never an unconditional match") {
    // t.id = s.id only holds when s.is_active is true - a strictly weaker
    // guarantee than an unconditional equality, the same problem OR
    // guards against. Confirms the Set.empty fallback for Conditional
    // stays that way as this method grows more cases around it.
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))))
    val caseWhen = Conditional(
      branches = List((ColumnReference(ColumnRef("is_active", Some("s"))), equalityOn("id", "id"))),
      elseValue = None
    )
    val mutation = RowMutation(matchCondition = Some(caseWhen))
    val violations = RuleVerifier.verify(rules, mutation)
    assert(violations.size == 1)
    assert(violations.head.message.contains("id"))
  }

  test("merge_condition combines a De Morgan pairing with an ordinary AND-ed equality") {
    val rules = List(ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id", "region"))))
    val condition = BooleanExpr("AND", List(not(inequalityOn("id", "id")), equalityOn("region", "region")))
    val mutation = RowMutation(matchCondition = Some(condition))
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
