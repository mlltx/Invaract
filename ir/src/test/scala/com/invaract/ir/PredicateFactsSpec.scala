// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

/** Direct coverage of `PredicateFacts.requiredFacts` — exercised indirectly
  * through `PropertyAnalysisSpec`'s `Filter`/`Conditional` tests, but the
  * AND/OR/NOT polarity table and each recognized predicate shape (both
  * operand orders) are precise enough to deserve direct tests of their
  * own, mirroring `RuleVerifierSpec`'s equivalent direct coverage of
  * `EqualityConditions`.
  */
class PredicateFactsSpec extends AnyFunSuite {

  private def col(name: String) = ColumnReference(ColumnRef(name))
  private val a = ColumnRef("a")
  private val b = ColumnRef("b")

  test("AND asserted true unions both sides' facts; asserted false proves nothing") {
    val cond = BooleanExpr("AND", List(Function("ISNOTNULL", List(col("a"))), Function("ISNOTNULL", List(col("b")))))
    val trueFacts = PredicateFacts.requiredFacts(cond)
    assert(trueFacts(a).notNull == NullabilityFact.Proven)
    assert(trueFacts(b).notNull == NullabilityFact.Proven)
    assert(PredicateFacts.requiredFacts(cond, negated = true).isEmpty)
  }

  test("OR asserted true proves nothing; asserted false (De Morgan) unions both sides negated") {
    val cond = BooleanExpr("OR", List(Function("ISNOTNULL", List(col("a"))), Function("ISNOTNULL", List(col("b")))))
    assert(PredicateFacts.requiredFacts(cond).isEmpty)
    val falseFacts = PredicateFacts.requiredFacts(cond, negated = true)
    assert(falseFacts(a).notNull == NullabilityFact.Refuted)
    assert(falseFacts(b).notNull == NullabilityFact.Refuted)
  }

  test("NOT flips polarity, including a double negation resolving back to the original") {
    val inner = Function("ISNOTNULL", List(col("a")))
    assert(PredicateFacts.requiredFacts(BooleanExpr("NOT", List(inner))) == PredicateFacts.requiredFacts(inner, negated = true))
    assert(PredicateFacts.requiredFacts(BooleanExpr("NOT", List(BooleanExpr("NOT", List(inner))))) == PredicateFacts.requiredFacts(inner))
  }

  test("ISNOTNULL/ISNULL both polarities") {
    assert(PredicateFacts.requiredFacts(Function("ISNOTNULL", List(col("a"))))(a).notNull == NullabilityFact.Proven)
    assert(PredicateFacts.requiredFacts(Function("ISNOTNULL", List(col("a"))), negated = true)(a).notNull == NullabilityFact.Refuted)
    assert(PredicateFacts.requiredFacts(Function("ISNULL", List(col("a"))))(a).notNull == NullabilityFact.Refuted)
    assert(PredicateFacts.requiredFacts(Function("ISNULL", List(col("a"))), negated = true)(a).notNull == NullabilityFact.Proven)
  }

  test("IN (...) establishes OneOf only under a true assertion") {
    val cond = Function("IN", List(col("a"), Literal("X", "string"), Literal("Y", "string")))
    assert(PredicateFacts.requiredFacts(cond)(a).oneOf.contains(Property.OneOf(Set("X", "Y"), "string")))
    assert(PredicateFacts.requiredFacts(cond, negated = true).isEmpty)
  }

  test("null-safe equality (<=>) with a non-null literal proves EqualsConstant AND NotNull, both operand orders") {
    val rightLiteral = Comparison("<=>", col("a"), Literal("X", "string"))
    assert(PredicateFacts.requiredFacts(rightLiteral)(a).equalsConstant.contains(Property.EqualsConstant("X", "string")))
    assert(PredicateFacts.requiredFacts(rightLiteral)(a).notNull == NullabilityFact.Proven)
    val leftLiteral = Comparison("<=>", Literal("X", "string"), col("a"))
    assert(PredicateFacts.requiredFacts(leftLiteral)(a).equalsConstant.contains(Property.EqualsConstant("X", "string")))
    assert(PredicateFacts.requiredFacts(leftLiteral)(a).notNull == NullabilityFact.Proven)
  }

  test("plain equality (=) proves EqualsConstant but NOT NotNull, both operand orders") {
    val rightLiteral = Comparison("=", col("a"), Literal("X", "string"))
    val facts = PredicateFacts.requiredFacts(rightLiteral)
    assert(facts(a).equalsConstant.contains(Property.EqualsConstant("X", "string")))
    assert(facts(a).notNull == NullabilityFact.Unknown)
    val leftLiteral = Comparison("=", Literal("X", "string"), col("a"))
    assert(PredicateFacts.requiredFacts(leftLiteral)(a).equalsConstant.contains(Property.EqualsConstant("X", "string")))
  }

  test("equality asserted false proves nothing (not equal to a value doesn't tighten a range/one-of)") {
    assert(PredicateFacts.requiredFacts(Comparison("=", col("a"), Literal(5, "integer")), negated = true).isEmpty)
  }

  test("range comparisons establish the matching bound, and the negated bound under a false assertion") {
    def boundsFor(op: String, negated: Boolean) = PredicateFacts.requiredFacts(Comparison(op, col("a"), Literal(5, "integer")), negated)(a).range.get
    assert(boundsFor(">", negated = false) == Property.Range(gt = Some(5)))
    assert(boundsFor(">", negated = true) == Property.Range(lte = Some(5)))
    assert(boundsFor(">=", negated = false) == Property.Range(gte = Some(5)))
    assert(boundsFor(">=", negated = true) == Property.Range(lt = Some(5)))
    assert(boundsFor("<", negated = false) == Property.Range(lt = Some(5)))
    assert(boundsFor("<", negated = true) == Property.Range(gte = Some(5)))
    assert(boundsFor("<=", negated = false) == Property.Range(lte = Some(5)))
    assert(boundsFor("<=", negated = true) == Property.Range(gt = Some(5)))
  }

  test("a literal on the left flips the comparison operator (5 <= a means a >= 5)") {
    val facts = PredicateFacts.requiredFacts(Comparison("<=", Literal(5, "integer"), col("a")))
    assert(facts(a).range.contains(Property.Range(gte = Some(5))))
  }

  test("a non-numeric literal type is never treated as a range bound") {
    assert(PredicateFacts.requiredFacts(Comparison(">", col("a"), Literal("X", "string"))).isEmpty)
  }

  test("an unrecognized predicate shape contributes nothing") {
    assert(PredicateFacts.requiredFacts(Function("SOME_OTHER_FN", List(col("a")))).isEmpty)
  }

  test("merge tightens per-column facts from two independent maps, not just overwriting") {
    val left = Map(a -> ColumnPropertyState(range = Some(Property.Range(gte = Some(0)))))
    val right = Map(a -> ColumnPropertyState(range = Some(Property.Range(lte = Some(100)))))
    val merged = PredicateFacts.merge(left, right)
    assert(merged(a).range.contains(Property.Range(gte = Some(0), lte = Some(100))))
  }
}
