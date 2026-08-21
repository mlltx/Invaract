// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.ir

import org.scalatest.funsuite.AnyFunSuite

class PlanPrinterSpec extends AnyFunSuite {

  test("render shows the worked example's structure and output derivations") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.customer_orders"),
      Project(
        orders,
        List(
          NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          NamedExpr("lifetime_value", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
        )
      )
    )

    val rendered = PlanPrinter.render(plan)

    assert(rendered.contains("Write(gold.customer_orders)"))
    assert(rendered.contains("Project"))
    assert(rendered.contains("Read(raw.orders)"))
    assert(rendered.contains("customer_id = raw.orders.customer_id"))
    assert(rendered.contains("lifetime_value = SUM(raw.orders.amount)"))
  }

  test("render shows join type and renders the condition infix") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val join = Join(
      orders,
      customers,
      JoinType.Inner,
      Some(FunctionCall(
        "=",
        List(
          ColumnReference(ColumnRef("customer_id", Some("raw.orders"))),
          ColumnReference(ColumnRef("id", Some("raw.customers")))
        )
      ))
    )

    val rendered = PlanPrinter.render(join)
    assert(rendered.contains("Join(Inner"))
    assert(rendered.contains("raw.orders.customer_id = raw.customers.id"))
    assert(rendered.contains("Read(raw.orders)"))
    assert(rendered.contains("Read(raw.customers)"))
  }

  test("render lists every branch of a Union") {
    val a = Read(DatasetRef("raw.a"))
    val b = Read(DatasetRef("raw.b"))
    val rendered = PlanPrinter.render(Union(List(a, b)))

    assert(rendered.contains("Union"))
    assert(rendered.contains("Read(raw.a)"))
    assert(rendered.contains("Read(raw.b)"))
  }

  test("render shows GROUP BY and each aggregate on its own line") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Aggregate(
      orders,
      groupBy = List(ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
      aggregates = List(
        NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
        NamedExpr("order_count", AggregateCall("COUNT", ColumnReference(ColumnRef("order_id", Some("raw.orders")))))
      )
    )

    val rendered = PlanPrinter.render(plan)
    assert(rendered.contains("GROUP BY raw.orders.customer_id"))
    assert(rendered.contains("order_count = COUNT(raw.orders.order_id)"))
  }

  test("render shows an aliased self-join Read distinctly per side") {
    val current = Read(DatasetRef("raw.orders"), alias = Some("cur"))
    val archived = Read(DatasetRef("raw.orders"), alias = Some("arch"))
    val rendered = PlanPrinter.render(Union(List(current, archived)))

    assert(rendered.contains("Read(raw.orders AS cur)"))
    assert(rendered.contains("Read(raw.orders AS arch)"))
  }
}
