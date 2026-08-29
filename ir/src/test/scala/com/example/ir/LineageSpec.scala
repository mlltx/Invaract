// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

import org.scalatest.funsuite.AnyFunSuite

class LineageSpec extends AnyFunSuite {

  test("trace resolves the worked example: customer_id passes through, lifetime_value is aggregated") {
    // Write(gold.customer_orders)
    //   └── Project
    //        ├── customer_id    <- Read(raw.orders).customer_id
    //        └── lifetime_value <- SUM(Read(raw.orders).amount)
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

    val lineage = Lineage.trace(plan)

    val customerId = lineage.find(_.output.name == "customer_id").get
    assert(customerId.sources == Set(ColumnRef("customer_id", Some("raw.orders"))))
    assert(!customerId.aggregated)

    val lifetimeValue = lineage.find(_.output.name == "lifetime_value").get
    assert(lifetimeValue.sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(lifetimeValue.aggregated)
  }

  test("trace resolves a full GROUP BY plan stage, not just an inline aggregate expression") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.customer_orders"),
      Aggregate(
        input = orders,
        groupBy = List(ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
        aggregates = List(
          NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          NamedExpr("order_count", AggregateCall("COUNT", ColumnReference(ColumnRef("order_id", Some("raw.orders")))))
        )
      )
    )

    val lineage = Lineage.trace(plan)
    assert(lineage.size == 2)
    assert(lineage.exists(l => l.output.name == "customer_id" && !l.aggregated))
    assert(lineage.exists(l => l.output.name == "order_count" && l.aggregated))
  }

  test("trace passes columns through Filter and Sort unchanged") {
    val orders = Read(DatasetRef("raw.orders"))
    val projected =
      Project(orders, List(NamedExpr("amount", ColumnReference(ColumnRef("amount", Some("raw.orders"))))))
    val filtered = Filter(projected, FunctionCall(">", List(ColumnReference(ColumnRef("amount")), Literal(0, "integer"))))
    val sorted = Sort(filtered, List(SortOrder(ColumnReference(ColumnRef("amount")))))
    val plan = Write(DatasetRef("gold.amounts"), sorted)

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("amount"), Set(ColumnRef("amount", Some("raw.orders"))), aggregated = false)))
  }

  test("trace attributes each output column across a Join to the correct side") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val joined = Join(
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
    val projected = Project(
      joined,
      List(
        NamedExpr("order_id", ColumnReference(ColumnRef("order_id", Some("raw.orders")))),
        NamedExpr("customer_name", ColumnReference(ColumnRef("name", Some("raw.customers"))))
      )
    )
    val plan = Write(DatasetRef("gold.order_details"), projected)

    val lineage = Lineage.trace(plan)
    assert(lineage.find(_.output.name == "order_id").get.sources == Set(ColumnRef("order_id", Some("raw.orders"))))
    assert(lineage.find(_.output.name == "customer_name").get.sources == Set(ColumnRef("name", Some("raw.customers"))))
  }

  test("trace attributes an ambiguous unqualified reference across a Join to both sides") {
    val orders = Read(DatasetRef("raw.orders"), alias = Some("o"))
    val archive = Read(DatasetRef("raw.orders_archive"), alias = Some("a"))
    val joined = Join(orders, archive, JoinType.Inner)
    val projected = Project(joined, List(NamedExpr("id", ColumnReference(ColumnRef("id")))))
    val plan = Write(DatasetRef("gold.ids"), projected)

    val lineage = Lineage.trace(plan)
    assert(lineage.head.sources == Set(ColumnRef("id", Some("o")), ColumnRef("id", Some("a"))))
  }

  test("trace resolves Window pass-through columns alongside new windowed columns") {
    val orders = Read(DatasetRef("raw.orders"))
    val projected = Project(
      orders,
      List(
        NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
        NamedExpr("amount", ColumnReference(ColumnRef("amount", Some("raw.orders"))))
      )
    )
    val windowed = Window(
      projected,
      windowExprs = List(NamedExpr("running_total", AggregateCall("SUM", ColumnReference(ColumnRef("amount"))))),
      partitionBy = List(ColumnReference(ColumnRef("customer_id")))
    )
    val plan = Write(DatasetRef("gold.running_totals"), windowed)

    val lineage = Lineage.trace(plan)
    assert(lineage.map(_.output.name).toSet == Set("customer_id", "amount", "running_total"))
    assert(lineage.find(_.output.name == "running_total").get.aggregated)
    // Resolving "amount" against the Project below must find the *matching*
    // declared column, not merely *a* declared column: asserting only
    // `.aggregated` above wouldn't notice a bug that resolved "amount" to
    // the Project's other declared column ("customer_id") instead, since
    // the wrapping AggregateCall would still report aggregated = true
    // either way. Only checking `.sources` distinguishes the two.
    assert(lineage.find(_.output.name == "running_total").get.sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(!lineage.find(_.output.name == "customer_id").get.aggregated)
  }

  test("trace resolves a bare reference to an Aggregate's declared output column, when a Project sits directly on top") {
    val orders = Read(DatasetRef("raw.orders"))
    val aggregated = Aggregate(
      orders,
      groupBy = Nil,
      aggregates = List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders"))))))
    )
    val plan = Write(DatasetRef("gold.out"), Project(aggregated, List(NamedExpr("total_value", ColumnReference(ColumnRef("total"))))))

    val lineage = Lineage.trace(plan)
    val totalValue = lineage.find(_.output.name == "total_value").get
    assert(totalValue.aggregated)
    assert(totalValue.sources == Set(ColumnRef("amount", Some("raw.orders"))))
  }

  test("trace resolves a bare reference to a Window's declared output column, when a Project sits directly on top") {
    val orders = Read(DatasetRef("raw.orders"))
    val windowed = Window(
      orders,
      windowExprs = List(NamedExpr("running_total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))),
      partitionBy = List(ColumnReference(ColumnRef("customer_id", Some("raw.orders"))))
    )
    val plan = Write(DatasetRef("gold.out"), Project(windowed, List(NamedExpr("total_so_far", ColumnReference(ColumnRef("running_total"))))))

    val lineage = Lineage.trace(plan)
    val totalSoFar = lineage.find(_.output.name == "total_so_far").get
    assert(totalSoFar.aggregated)
    assert(totalSoFar.sources == Set(ColumnRef("amount", Some("raw.orders"))))
  }

  test("trace resolves a Union with mixed aggregation status: either branch aggregating marks the result aggregated") {
    val plainBranch = Read(DatasetRef("raw.orders_current"))
    val aggregatedBranch = Aggregate(
      Read(DatasetRef("raw.orders_detail")),
      groupBy = Nil,
      aggregates = List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders_detail"))))))
    )
    val union = Union(List(aggregatedBranch, plainBranch))
    val plan = Write(DatasetRef("gold.out"), Project(union, List(NamedExpr("combined_total", ColumnReference(ColumnRef("total"))))))

    val lineage = Lineage.trace(plan)
    val combinedTotal = lineage.find(_.output.name == "combined_total").get
    assert(combinedTotal.aggregated, "either branch aggregating the reference should mark the union result aggregated")
    assert(combinedTotal.sources.exists(s => s.name == "amount" && s.qualifier.contains("raw.orders_detail")))
    assert(combinedTotal.sources.exists(s => s.name == "total" && s.qualifier.contains("raw.orders_current")))
  }

  test("trace marks an ambiguous Join reference aggregated when either side aggregates it") {
    val plainSide = Read(DatasetRef("raw.orders"), alias = Some("plain"))
    val aggregatedSide = Aggregate(
      Read(DatasetRef("raw.orders_detail")),
      groupBy = Nil,
      aggregates = List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders_detail"))))))
    )
    val joined = Join(plainSide, aggregatedSide, JoinType.Inner)
    val plan = Write(DatasetRef("gold.out"), Project(joined, List(NamedExpr("total", ColumnReference(ColumnRef("total"))))))

    val lineage = Lineage.trace(plan)
    val total = lineage.find(_.output.name == "total").get
    assert(total.aggregated, "either join side aggregating the reference should mark the combined result aggregated")
    assert(total.sources.exists(s => s.name == "total" && s.qualifier.contains("plain")))
    assert(total.sources.exists(s => s.name == "amount" && s.qualifier.contains("raw.orders_detail")))
  }

  test("trace marks a literal-derived output as non-aggregated with no sources") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(DatasetRef("gold.out"), Project(orders, List(NamedExpr("constant_flag", Literal(true, "boolean")))))

    val lineage = Lineage.trace(plan)
    val constantFlag = lineage.find(_.output.name == "constant_flag").get
    assert(constantFlag.sources.isEmpty)
    assert(!constantFlag.aggregated)
  }

  test("trace propagates aggregation through a multi-argument function call when only one argument aggregates") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        orders,
        List(
          NamedExpr(
            "flagged_total",
            FunctionCall(
              ">",
              List(
                AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))),
                Literal(0, "integer")
              )
            )
          )
        )
      )
    )

    val lineage = Lineage.trace(plan)
    val flaggedTotal = lineage.find(_.output.name == "flagged_total").get
    assert(flaggedTotal.aggregated)
    assert(flaggedTotal.sources == Set(ColumnRef("amount", Some("raw.orders"))))
  }

  test("trace resolves a Union using the first branch's output names") {
    val current = Read(DatasetRef("raw.orders"))
    val archived = Read(DatasetRef("raw.orders_archive"))
    val currentProjected =
      Project(current, List(NamedExpr("id", ColumnReference(ColumnRef("id", Some("raw.orders"))))))
    val archivedProjected =
      Project(archived, List(NamedExpr("id", ColumnReference(ColumnRef("id", Some("raw.orders_archive"))))))
    val plan = Write(DatasetRef("gold.all_orders"), Union(List(currentProjected, archivedProjected)))

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("id"), Set(ColumnRef("id", Some("raw.orders"))), aggregated = false)))
  }

  test("trace returns no columns for a bare Read with no Project above it") {
    assert(Lineage.trace(Read(DatasetRef("raw.orders"))).isEmpty)
  }

  test("trace resolves an unsupported reference to no known source instead of crashing") {
    val plan = Write(
      DatasetRef("gold.out"),
      Project(Unsupported("Generate(explode)"), List(NamedExpr("x", ColumnReference(ColumnRef("x")))))
    )

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("x"), Set.empty, aggregated = false)))
  }

  test("trace treats an UnsupportedExpr as contributing no sources") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(orders, List(NamedExpr("mystery", UnsupportedExpr("ScalaUDF(myFunc)"))))
    )

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("mystery"), Set.empty, aggregated = false)))
  }
}
