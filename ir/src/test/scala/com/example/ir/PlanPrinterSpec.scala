// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

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

  test("render shows an Unsupported node's description and any resolvable children") {
    val known = Read(DatasetRef("raw.orders"))
    val rendered = PlanPrinter.render(Unsupported("Generate(explode)", List(known)))

    assert(rendered.contains("Unsupported(Generate(explode))"))
    assert(rendered.contains("Read(raw.orders)"))
  }

  test("render shows an UnsupportedExpr inline within a Project column") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Project(orders, List(NamedExpr("mystery", UnsupportedExpr("ScalaUDF(myFunc)"))))

    val rendered = PlanPrinter.render(plan)
    assert(rendered.contains("mystery = <unsupported: ScalaUDF(myFunc)>"))
  }

  // The tests above only ever assert `.contains(...)` on a node's own
  // content, never on which branch/continuation character precedes it —
  // so a bug that swapped which child is treated as "last" (mutating
  // renderChildren's `isLast`) would still leave every asserted substring
  // present somewhere in the output, just under the wrong prefix. Only a
  // full-string comparison against a real multi-level tree pins that down.
  test("render distinguishes branch and continuation prefixes exactly, at nested depth") {
    val plan = Join(
      Union(List(Read(DatasetRef("raw.a")), Read(DatasetRef("raw.b")))),
      Union(List(Read(DatasetRef("raw.c")), Read(DatasetRef("raw.d")))),
      JoinType.Inner
    )

    val rendered = PlanPrinter.render(plan)
    // Built from explicit "\n"-joined lines, not a multi-line
    // stripMargin literal: a stripMargin string's embedded newlines are
    // real bytes in this source file, which a Windows checkout with
    // core.autocrlf can convert to CRLF - silently breaking equality
    // against PlanPrinter's own hardcoded "\n" (an escape sequence, not a
    // literal newline byte, so it survives any line-ending conversion
    // unchanged). Confirmed by a real CI failure on windows-latest.
    val expected = List(
      "Join(Inner)",
      "├─ Union",
      "│  ├─ Read(raw.a)",
      "│  └─ Read(raw.b)",
      "└─ Union",
      "   ├─ Read(raw.c)",
      "   └─ Read(raw.d)",
      ""
    ).mkString("\n")

    assert(rendered == expected)
  }

  test("render shows DISTINCT for an AggregateCall marked distinct, and omits it otherwise") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Project(
      orders,
      List(
        NamedExpr("unique_customers", AggregateCall("COUNT", ColumnReference(ColumnRef("customer_id")), distinct = true)),
        NamedExpr("total_orders", AggregateCall("COUNT", ColumnReference(ColumnRef("order_id")), distinct = false))
      )
    )

    val rendered = PlanPrinter.render(plan)
    assert(rendered.contains("unique_customers = COUNT(DISTINCT customer_id)"))
    assert(rendered.contains("total_orders = COUNT(order_id)"))
  }

  test("render omits GROUP BY when an Aggregate has no grouping keys") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Aggregate(
      orders,
      groupBy = Nil,
      aggregates = List(NamedExpr("total", AggregateCall("COUNT", ColumnReference(ColumnRef("id")))))
    )

    val rendered = PlanPrinter.render(plan)
    assert(!rendered.contains("GROUP BY"))
    assert(rendered.contains("total = COUNT(id)"))
  }

  test("render shows Sort's direction for ascending and descending keys") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Sort(
      orders,
      List(
        SortOrder(ColumnReference(ColumnRef("id")), ascending = true),
        SortOrder(ColumnReference(ColumnRef("value")), ascending = false)
      )
    )

    val rendered = PlanPrinter.render(plan)
    assert(rendered.contains("Sort(id ASC, value DESC)"))
  }

  test("render shows Window's PARTITION BY and ORDER BY, and omits both when absent") {
    val orders = Read(DatasetRef("raw.orders"))
    val windowExprs = List(NamedExpr("rn", FunctionCall("ROW_NUMBER", Nil)))

    val withSpec = Window(
      orders,
      windowExprs,
      partitionBy = List(ColumnReference(ColumnRef("customer_id"))),
      orderBy = List(SortOrder(ColumnReference(ColumnRef("order_date")), ascending = true))
    )
    assert(PlanPrinter.render(withSpec).contains("Window(PARTITION BY customer_id ORDER BY order_date)"))

    val withoutSpec = Window(orders, windowExprs)
    assert(PlanPrinter.render(withoutSpec).contains("Window()"))
  }
}
