// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

class PlanSpec extends AnyFunSuite {

  test("Read has no children") {
    assert(Read(DatasetRef("raw.orders")).children.isEmpty)
  }

  test("Write's child is its input plan") {
    val read = Read(DatasetRef("raw.orders"))
    val write = Write(DatasetRef("gold.orders"), read)
    assert(write.children == List(read))
  }

  test("Join exposes both branches as children") {
    val left = Read(DatasetRef("raw.orders"))
    val right = Read(DatasetRef("raw.customers"))
    val join = Join(
      left,
      right,
      JoinType.Inner,
      Some(Comparison(
        "=",
        ColumnReference(ColumnRef("customer_id", Some("raw.orders"))),
        ColumnReference(ColumnRef("id", Some("raw.customers")))
      ))
    )
    assert(join.children == List(left, right))
  }

  test("Union exposes every branch as a child") {
    val a = Read(DatasetRef("raw.a"))
    val b = Read(DatasetRef("raw.b"))
    val c = Read(DatasetRef("raw.c"))
    assert(Union(List(a, b, c)).children == List(a, b, c))
  }

  test("Project, Filter, Aggregate, Sort, and Window each expose a single child: their input") {
    val input = Read(DatasetRef("raw.orders"))

    assert(Project(input, Nil).children == List(input))
    assert(Filter(input, Literal(true, "boolean")).children == List(input))
    assert(Aggregate(input, Nil, Nil).children == List(input))
    assert(Sort(input, Nil).children == List(input))
    assert(Window(input, Nil).children == List(input))
    assert(Limit(input, 5).children == List(input))
  }

  test("Expr.references reports the columns an expression reads, through nested arithmetic") {
    val expr = Arithmetic(
      "+",
      List(
        ColumnReference(ColumnRef("a")),
        AggregateCall("SUM", ColumnReference(ColumnRef("b")))
      )
    )
    assert(expr.references == Set(ColumnRef("a"), ColumnRef("b")))
  }

  test("Literal contributes no references; a bare column reference contributes exactly itself") {
    assert(Literal(1, "integer").references.isEmpty)
    assert(ColumnReference(ColumnRef("x")).references == Set(ColumnRef("x")))
  }

  test("ColumnRef.toString qualifies a name only when a qualifier is present") {
    assert(ColumnRef("id").toString == "id")
    assert(ColumnRef("id", Some("raw.orders")).toString == "raw.orders.id")
  }

  test("UnknownPlan carries through whatever children a translator could still resolve, plus its sourceType") {
    val known = Read(DatasetRef("raw.orders"))
    val unknown = UnknownPlan("Generate(explode)", sourceType = "Generate", children = List(known))
    assert(unknown.children == List(known))
    assert(unknown.sourceType == "Generate")
  }

  test("UnknownExpression contributes no references when it has no resolvable children") {
    assert(UnknownExpression("ScalaUDF(myFunc)", sourceType = "ScalaUDF").references.isEmpty)
  }

  test("UnknownExpression contributes its resolvable children's references, even though the node itself is unknown") {
    val expr = UnknownExpression("Generate(explode)", sourceType = "Generate", children = List(ColumnReference(ColumnRef("tags"))))
    assert(expr.references == Set(ColumnRef("tags")))
  }

  test("Limit exposes its input as its single child") {
    val input = Read(DatasetRef("raw.orders"))
    assert(Limit(input, 10).children == List(input))
  }
}
