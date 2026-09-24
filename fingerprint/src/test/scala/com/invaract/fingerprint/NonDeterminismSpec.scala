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

  test("Alias/Cast classify transparently, by their own inner expression") {
    assert(NonDeterminism.classify(Alias("renamed", Function("rand", Nil))) == Some(true))
    assert(NonDeterminism.classify(Alias("renamed", ColumnReference(ColumnRef("amount")))) == Some(false))
    assert(NonDeterminism.classify(Cast(Function("rand", Nil), "double")) == Some(true))
    assert(NonDeterminism.classify(Cast(ColumnReference(ColumnRef("amount")), "double")) == Some(false))
  }

  test("Comparison/BooleanExpr consider both/all operands, not just the first") {
    val comparison = Comparison("=", ColumnReference(ColumnRef("amount")), Function("rand", Nil))
    assert(NonDeterminism.classify(comparison) == Some(true))
    val boolExpr = BooleanExpr("AND", List(ColumnReference(ColumnRef("active")), Function("rand", Nil)))
    assert(NonDeterminism.classify(boolExpr) == Some(true))
    val allDeterministic = BooleanExpr("AND", List(ColumnReference(ColumnRef("active")), Literal(true, "boolean")))
    assert(NonDeterminism.classify(allDeterministic) == Some(false))
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

  test("StructField over a deterministic struct classifies as deterministic") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    assert(NonDeterminism.classify(StructField(built, "zip")) == Some(false))
  }

  test("StructField over a struct containing a non-deterministic field classifies as Some(true)") {
    val built = StructConstruct(List("ts" -> Function("current_timestamp", Nil)))
    assert(NonDeterminism.classify(StructField(built, "ts")) == Some(true))
  }

  test("StructField over a struct containing a UDF classifies as unknown (None)") {
    val built = StructConstruct(List("risk" -> UDF(Some("f"), Nil)))
    assert(NonDeterminism.classify(StructField(built, "risk")) == None)
  }

  test("StructConstruct is deterministic when every field is deterministic") {
    val built = StructConstruct(List("a" -> Literal(1, "integer"), "b" -> ColumnReference(ColumnRef("name"))))
    assert(NonDeterminism.classify(built) == Some(false))
  }

  test("StructConstruct is Some(true) when any one field is non-deterministic") {
    val built = StructConstruct(List("clean" -> Literal(1, "integer"), "id" -> Function("uuid", Nil)))
    assert(NonDeterminism.classify(built) == Some(true))
  }

  test("StructConstruct is None when any one field is opaque (a UDF), even alongside a non-deterministic field") {
    val built = StructConstruct(List("rnd" -> Function("rand", Nil), "opaque" -> UDF(Some("f"), Nil)))
    assert(NonDeterminism.classify(built) == None)
  }

  test("an empty StructConstruct classifies as deterministic") {
    assert(NonDeterminism.classify(StructConstruct(Nil)) == Some(false))
  }
}
