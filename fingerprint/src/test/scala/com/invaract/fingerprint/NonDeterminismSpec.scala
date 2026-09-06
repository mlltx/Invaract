// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite

class NonDeterminismSpec extends AnyFunSuite {

  test("a plain column reference classifies as deterministic") {
    assert(NonDeterminism.classify(ColumnReference(ColumnRef("amount"))) == Some(false))
  }

  test("a literal classifies as deterministic") {
    assert(NonDeterminism.classify(Literal(5, "integer")) == Some(false))
  }

  test("a known non-deterministic function classifies as Some(true)") {
    assert(NonDeterminism.classify(Function("rand", Nil)) == Some(true))
    assert(NonDeterminism.classify(Function("current_timestamp", Nil)) == Some(true))
  }

  test("function name matching is case-insensitive") {
    assert(NonDeterminism.classify(Function("RAND", Nil)) == Some(true))
  }

  test("an ordinary, non-allowlisted function classifies as deterministic") {
    assert(NonDeterminism.classify(Function("upper", List(ColumnReference(ColumnRef("name"))))) == Some(false))
  }

  test("a UDF always classifies as unknown (None), regardless of its arguments") {
    assert(NonDeterminism.classify(UDF(Some("f"), List(Literal(1, "integer")))) == None)
  }

  test("a non-deterministic call buried inside an otherwise-deterministic expression still surfaces as Some(true)") {
    val expr = Arithmetic("+", List(ColumnReference(ColumnRef("amount")), Function("rand", Nil)))
    assert(NonDeterminism.classify(expr) == Some(true))
  }

  test("a UDF buried anywhere in an expression forces the whole combination to None") {
    val expr = Arithmetic("+", List(ColumnReference(ColumnRef("amount")), UDF(Some("f"), Nil)))
    assert(NonDeterminism.classify(expr) == None)
  }

  test("None (UDF) wins over Some(true) (a non-deterministic function) when both are present") {
    val expr = Arithmetic("+", List(Function("rand", Nil), UDF(Some("f"), Nil)))
    assert(NonDeterminism.classify(expr) == None)
  }

  test("an UnknownExpression classifies as unknown (None)") {
    assert(NonDeterminism.classify(UnknownExpression("x", "Kind", Nil)) == None)
  }

  test("a Conditional's branches and else value are all considered") {
    val withNonDetInCondition = Conditional(List((Function("rand", Nil), Literal(1, "integer"))), Some(Literal(0, "integer")))
    val withNonDetInElse = Conditional(List((Literal(true, "boolean"), Literal(1, "integer"))), Some(Function("rand", Nil)))
    val allDeterministic = Conditional(List((Literal(true, "boolean"), Literal(1, "integer"))), Some(Literal(0, "integer")))
    assert(NonDeterminism.classify(withNonDetInCondition) == Some(true))
    assert(NonDeterminism.classify(withNonDetInElse) == Some(true))
    assert(NonDeterminism.classify(allDeterministic) == Some(false))
  }

  test("an AggregateCall over a deterministic argument classifies as deterministic") {
    assert(NonDeterminism.classify(AggregateCall("SUM", ColumnReference(ColumnRef("amount")))) == Some(false))
  }
}
