// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

class LineageSpec extends AnyFunSuite {

  /** Traces a single-column `Project(Read(raw.orders), out = expr)` and
    * returns that one column's lineage — shared by every `DerivationKind`/
    * `AggregationDetail` test below, which only care about how a single
    * declaring expression classifies, not about plan-shape resolution
    * (already covered by the tests above).
    */
  private def lineageOf(expr: Expr): ColumnLineage = {
    val orders = Read(DatasetRef("raw.orders"))
    Lineage.trace(Write(DatasetRef("gold.out"), Project(orders, List(NamedExpr("out", expr))))).head
  }

  private val amount = ColumnReference(ColumnRef("amount", Some("raw.orders")))
  private val tax = ColumnReference(ColumnRef("tax", Some("raw.orders")))

  test("classify: Direct for a bare column reference and for an Alias wrapping one") {
    assert(lineageOf(amount).derivation == DerivationKind.Direct)
    assert(lineageOf(Alias("renamed", amount)).derivation == DerivationKind.Direct)
  }

  test("classify: Constant for a bare literal and for an expression built entirely from literals") {
    assert(lineageOf(Literal(5, "integer")).derivation == DerivationKind.Constant)
    assert(
      lineageOf(Arithmetic("+", List(Literal(1, "integer"), Literal(2, "integer")))).derivation == DerivationKind.Constant
    )
    // A Cast still classifies Constant when its operand is a literal - the
    // cast is a real operation, but there is still no source column, so
    // Computed (which implies "reads from real columns") would be
    // misleading here.
    assert(lineageOf(Cast(Literal("5", "string"), "integer")).derivation == DerivationKind.Constant)
  }

  test("classify: Computed for a Cast, Arithmetic, Comparison, BooleanExpr, Conditional, Function, and AggregateCall over real columns") {
    assert(lineageOf(Cast(amount, "double")).derivation == DerivationKind.Computed)
    assert(lineageOf(Arithmetic("*", List(amount, tax))).derivation == DerivationKind.Computed)
    assert(lineageOf(Comparison(">", amount, Literal(0, "integer"))).derivation == DerivationKind.Computed)
    assert(lineageOf(BooleanExpr("AND", List(Comparison(">", amount, Literal(0, "integer")), Comparison(">", tax, Literal(0, "integer"))))).derivation == DerivationKind.Computed)
    assert(
      lineageOf(Conditional(List((Comparison(">", amount, Literal(50, "integer")), Literal("high", "string"))), Some(Literal("low", "string"))))
        .derivation == DerivationKind.Computed
    )
    assert(lineageOf(Function("upper", List(amount))).derivation == DerivationKind.Computed)
    assert(lineageOf(AggregateCall("SUM", amount)).derivation == DerivationKind.Computed)
  }

  test("classify: a bare UDF is Opaque") {
    assert(lineageOf(UDF(Some("calculateRisk"), List(amount))).derivation == DerivationKind.Opaque)
  }

  test("classify: a bare UnknownExpression is Opaque") {
    assert(lineageOf(UnknownExpression("ScalaUDF(myFunc)", sourceType = "ScalaUDF")).derivation == DerivationKind.Opaque)
  }

  test("classify: Opaque propagates through Cast and Alias") {
    assert(lineageOf(Cast(UDF(Some("f"), List(amount)), "double")).derivation == DerivationKind.Opaque)
    assert(lineageOf(Alias("renamed", UDF(Some("f"), List(amount)))).derivation == DerivationKind.Opaque)
  }

  test("classify: Opaque propagates through Arithmetic regardless of which operand carries the UDF") {
    val udf = UDF(Some("f"), List(amount))
    assert(lineageOf(Arithmetic("+", List(udf, tax))).derivation == DerivationKind.Opaque, "opaque left operand")
    assert(lineageOf(Arithmetic("+", List(amount, udf))).derivation == DerivationKind.Opaque, "opaque right operand")
    assert(lineageOf(Arithmetic("+", List(amount, tax))).derivation == DerivationKind.Computed, "neither operand opaque")
  }

  test("classify: Opaque propagates through Comparison regardless of which side carries the UDF") {
    val udf = UDF(Some("f"), List(amount))
    assert(lineageOf(Comparison(">", udf, tax)).derivation == DerivationKind.Opaque, "opaque left side")
    assert(lineageOf(Comparison(">", amount, udf)).derivation == DerivationKind.Opaque, "opaque right side")
  }

  test("classify: Opaque propagates through BooleanExpr regardless of which operand carries the UDF") {
    val opaqueCond = Comparison(">", UDF(Some("f"), List(amount)), Literal(0, "integer"))
    val plainCond = Comparison(">", tax, Literal(0, "integer"))
    assert(lineageOf(BooleanExpr("AND", List(opaqueCond, plainCond))).derivation == DerivationKind.Opaque, "opaque first operand")
    assert(lineageOf(BooleanExpr("AND", List(plainCond, opaqueCond))).derivation == DerivationKind.Opaque, "opaque second operand")
  }

  test("classify: Opaque propagates through a Conditional's branch condition, branch value, and elseValue independently") {
    val udf = UDF(Some("f"), List(amount))
    val plainCond = Comparison(">", tax, Literal(0, "integer"))
    assert(
      lineageOf(Conditional(List((Comparison(">", udf, Literal(0, "integer")), Literal("x", "string"))), None)).derivation == DerivationKind.Opaque,
      "opaque branch condition"
    )
    assert(
      lineageOf(Conditional(List((plainCond, udf)), None)).derivation == DerivationKind.Opaque,
      "opaque branch value"
    )
    assert(
      lineageOf(Conditional(List((plainCond, Literal("x", "string"))), Some(udf))).derivation == DerivationKind.Opaque,
      "opaque elseValue"
    )
    assert(
      lineageOf(Conditional(List((plainCond, Literal("x", "string"))), Some(Literal("y", "string")))).derivation == DerivationKind.Computed,
      "nothing opaque"
    )
    assert(
      lineageOf(Conditional(List((plainCond, Literal("x", "string"))), None)).derivation == DerivationKind.Computed,
      "no elseValue at all"
    )
    // Zero branches and a literal elseValue means no column references at
    // all - Constant, not Computed, matching the "expression built
    // entirely from literals" rule the Constant test above already
    // covers. (This alone doesn't reach `combineOperation`'s empty-list
    // case, since elseValue still contributes one resolved Provenance to
    // its input list - see "no branches and no elseValue at all" below
    // for the genuinely empty case that distinguishes exists from forall
    // on combineOperation's own opaque check.)
    assert(
      lineageOf(Conditional(Nil, Some(Literal("x", "string")))).derivation == DerivationKind.Constant,
      "no branches at all"
    )
    // Both branches and elseValue empty is the one Conditional shape that
    // makes combineOperation's own input list genuinely empty (every
    // other case above still contributes at least one resolved
    // Provenance). `List.exists` on an empty list is false; a Stryker
    // mutant flipping it to `forall` is vacuously true there, wrongly
    // reporting Opaque instead of the correct Constant (no branches, no
    // elseValue, no sources at all).
    assert(lineageOf(Conditional(Nil, None)).derivation == DerivationKind.Constant, "no branches and no elseValue at all")
  }

  test("classify: Opaque propagates through Function args regardless of which one carries the UDF") {
    val udf = UDF(Some("f"), List(amount))
    assert(lineageOf(Function("concat", List(udf, tax))).derivation == DerivationKind.Opaque, "opaque first arg")
    assert(lineageOf(Function("concat", List(amount, udf))).derivation == DerivationKind.Opaque, "opaque second arg")
  }

  test("classify: Opaque propagates through an AggregateCall's argument") {
    assert(lineageOf(AggregateCall("SUM", UDF(Some("f"), List(amount)))).derivation == DerivationKind.Opaque)
  }

  test("aggregations: captures the aggregate function name and distinct flag") {
    val sum = lineageOf(AggregateCall("SUM", amount))
    assert(sum.aggregations == Set(AggregationDetail("SUM", distinct = false)))
    assert(sum.aggregated)

    val countDistinct = lineageOf(AggregateCall("COUNT", amount, distinct = true))
    assert(countDistinct.aggregations == Set(AggregationDetail("COUNT", distinct = true)))
  }

  test("aggregations: a column combining two different aggregate functions reports both") {
    val combined = lineageOf(Arithmetic("/", List(AggregateCall("SUM", amount), AggregateCall("COUNT", tax))))
    assert(combined.aggregations == Set(AggregationDetail("SUM", distinct = false), AggregationDetail("COUNT", distinct = false)))
  }

  test("aggregations: a non-aggregated column has an empty aggregations set and aggregated is false") {
    val plain = lineageOf(amount)
    assert(plain.aggregations.isEmpty)
    assert(!plain.aggregated)
  }

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
    val filtered = Filter(projected, Comparison(">", ColumnReference(ColumnRef("amount")), Literal(0, "integer")))
    val sorted = Sort(filtered, List(SortOrder(ColumnReference(ColumnRef("amount")))))
    val plan = Write(DatasetRef("gold.amounts"), sorted)

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("amount"), Set(ColumnRef("amount", Some("raw.orders"))), DerivationKind.Direct)))
  }

  test("trace attributes each output column across a Join to the correct side") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val joined = Join(
      orders,
      customers,
      JoinType.Inner,
      Some(Comparison(
        "=",
        ColumnReference(ColumnRef("customer_id", Some("raw.orders"))),
        ColumnReference(ColumnRef("id", Some("raw.customers")))
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

  test("trace preserves Direct through an ambiguous Join reference when both sides are pure passthroughs") {
    // Both "o.id" and "a.id" are plain Read columns with no computation on
    // either side - the reference is structurally ambiguous about origin,
    // but genuinely computation-free either way, so the honest answer is
    // Direct, not Computed. This is combineUnion's `forall(_ == Direct)`
    // check; a Stryker mutant flipping that forall to exists would still
    // report Direct here (since at least one side is Direct too), so this
    // alone doesn't kill that mutant - see the mixed-derivation test right
    // below for the case that actually distinguishes them.
    val orders = Read(DatasetRef("raw.orders"), alias = Some("o"))
    val archive = Read(DatasetRef("raw.orders_archive"), alias = Some("a"))
    val joined = Join(orders, archive, JoinType.Inner)
    val projected = Project(joined, List(NamedExpr("id", ColumnReference(ColumnRef("id")))))
    val plan = Write(DatasetRef("gold.ids"), projected)

    assert(Lineage.trace(plan).head.derivation == DerivationKind.Direct)
  }

  test("trace reports Computed, not Direct, for an ambiguous Join reference when only one side is a pure passthrough") {
    // One side ("o.total") is a plain passthrough Read column; the other
    // ("a.total") is computed via Arithmetic. combineUnion must not
    // collapse this to Direct just because at least one side is - only
    // when *every* candidate resolution is Direct does Direct survive.
    // This is what distinguishes combineUnion's `forall` from a mutant
    // `exists` (which would wrongly report Direct here, since one side
    // does resolve Direct).
    val plainSide = Read(DatasetRef("raw.orders"), alias = Some("o"))
    val computedSide = Project(
      Read(DatasetRef("raw.orders_detail"), alias = Some("a")),
      List(NamedExpr("total", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("a"))), Literal(2, "integer")))))
    )
    val joined = Join(plainSide, computedSide, JoinType.Inner)
    val projected = Project(joined, List(NamedExpr("total", ColumnReference(ColumnRef("total")))))
    val plan = Write(DatasetRef("gold.out"), projected)

    val lineage = Lineage.trace(plan)
    assert(lineage.head.derivation == DerivationKind.Computed)
    assert(lineage.head.sources == Set(ColumnRef("total", Some("o")), ColumnRef("amount", Some("a"))))
  }

  test("trace reports Opaque for an ambiguous Join reference when only one side is opaque") {
    // "o.risk" resolves as a plain (schema-unaware) passthrough Read
    // column; "a.risk" resolves through a UDF. Opacity must win even
    // though the *other* side is a plain Direct passthrough - this
    // distinguishes combineUnion's opaque `exists` check from a mutant
    // `forall`/`false` (which would fall through to the Direct/Computed
    // checks instead, since not every candidate is opaque).
    val plainSide = Read(DatasetRef("raw.orders"), alias = Some("o"))
    val opaqueSide = Project(
      Read(DatasetRef("raw.orders_detail"), alias = Some("a")),
      List(NamedExpr("risk", UDF(Some("calculateRisk"), List(ColumnReference(ColumnRef("amount", Some("a")))))))
    )
    val joined = Join(plainSide, opaqueSide, JoinType.Inner)
    val projected = Project(joined, List(NamedExpr("risk", ColumnReference(ColumnRef("risk")))))
    val plan = Write(DatasetRef("gold.out"), projected)

    assert(Lineage.trace(plan).head.derivation == DerivationKind.Opaque)
  }

  test("trace reports Constant for an ambiguous Join reference when every candidate resolves empty-sources but none is Direct") {
    // Both sides resolve "flag" via a Cast over a bare literal - Cast is a
    // real operation (never Direct, see Expr.scala's Cast doc), but has
    // no source columns at all on either side. Neither opaque nor
    // all-Direct, yet the combined sources are empty - Constant is the
    // only case left that fits, and only reachable if combineUnion's own
    // sources.isEmpty check actually runs (a mutant forcing it to `false`
    // would report Computed instead, despite there being nothing computed
    // *from* - no source column exists on either side).
    val leftSide = Project(Read(DatasetRef("raw.a")), List(NamedExpr("flag", Cast(Literal(1, "integer"), "boolean"))))
    val rightSide = Project(Read(DatasetRef("raw.b")), List(NamedExpr("flag", Cast(Literal(0, "integer"), "boolean"))))
    val joined = Join(leftSide, rightSide, JoinType.Inner)
    val projected = Project(joined, List(NamedExpr("flag", ColumnReference(ColumnRef("flag")))))
    val plan = Write(DatasetRef("gold.out"), projected)

    val lineage = Lineage.trace(plan)
    assert(lineage.head.derivation == DerivationKind.Constant)
    assert(lineage.head.sources.isEmpty)
  }

  test("trace resolves a computed column's derivation through nested Projects, not just its final passthrough hop") {
    // Mirrors the real InvaractPlugin demo shape: chaining two
    // .withColumn() calls produces *nested* Project nodes in Spark's
    // analyzed plan (Invaract translates .analyzed, not .optimizedPlan -
    // see ARCHITECTURE.md's ADR-002), so the outer Project's own
    // declaration for an untouched column is often nothing more than a
    // bare passthrough reference to an inner Project's real computation:
    //
    //   Project(outer): value_squared = value_squared   <- bare passthrough
    //     Project(inner): value_squared = (value * value) <- real computation
    //       Read(orders)
    //
    // Classifying only the outer NamedExpr's own syntax would wrongly
    // report this column Direct - it must resolve through to the inner
    // Project's real Arithmetic to correctly report Computed. Found via a
    // real ./dev/test run against InvaractPlugin's own output, not
    // invented: the first implementation of this feature reported
    // value_squared as Direct against exactly this real plan shape.
    val orders = Read(DatasetRef("raw.orders"))
    val amount = ColumnReference(ColumnRef("amount", Some("raw.orders")))
    val inner = Project(
      orders,
      List(
        NamedExpr("id", ColumnReference(ColumnRef("id", Some("raw.orders")))),
        NamedExpr("amount_squared", Arithmetic("*", List(amount, amount)))
      )
    )
    val outer = Project(
      inner,
      List(
        NamedExpr("id", ColumnReference(ColumnRef("id"))),
        NamedExpr("amount_squared", ColumnReference(ColumnRef("amount_squared"))),
        NamedExpr("tier", Conditional(List((Comparison(">", ColumnReference(ColumnRef("amount_squared")), Literal(100, "integer")), Literal("high", "string"))), Some(Literal("low", "string"))))
      )
    )
    val plan = Write(DatasetRef("gold.out"), outer)

    val lineage = Lineage.trace(plan)
    assert(lineage.find(_.output.name == "id").get.derivation == DerivationKind.Direct)
    assert(lineage.find(_.output.name == "amount_squared").get.derivation == DerivationKind.Computed)
    assert(lineage.find(_.output.name == "amount_squared").get.sources == Set(ColumnRef("amount", Some("raw.orders"))))
    // "tier" is itself declared directly on the outer Project (not a
    // passthrough), but its own Comparison references "amount_squared" by
    // bare name - resolving that reference must *also* look through the
    // inner Project's real computation, not treat "amount_squared" as
    // just another opaque/unresolvable name at this scope.
    assert(lineage.find(_.output.name == "tier").get.derivation == DerivationKind.Computed)
    assert(lineage.find(_.output.name == "tier").get.sources == Set(ColumnRef("amount", Some("raw.orders"))))
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
            Comparison(
              ">",
              AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))),
              Literal(0, "integer")
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
    assert(lineage == List(ColumnLineage(ColumnRef("id"), Set(ColumnRef("id", Some("raw.orders"))), DerivationKind.Direct)))
  }

  test("trace returns no columns for a bare Read with no Project above it") {
    assert(Lineage.trace(Read(DatasetRef("raw.orders"))).isEmpty)
  }

  test("trace resolves a reference against an UnknownPlan to no known source instead of crashing") {
    val plan = Write(
      DatasetRef("gold.out"),
      Project(UnknownPlan("Generate(explode)"), List(NamedExpr("x", ColumnReference(ColumnRef("x")))))
    )

    val lineage = Lineage.trace(plan)
    // The declaring expression (a bare ColumnReference) is still Direct
    // even though resolution found no real source — classification looks
    // at the expression's own shape, not whether resolution succeeded.
    assert(lineage == List(ColumnLineage(ColumnRef("x"), Set.empty, DerivationKind.Direct)))
  }

  test("trace treats a childless UnknownExpression as contributing no sources, and classifies it Opaque") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(orders, List(NamedExpr("mystery", UnknownExpression("ScalaUDF(myFunc)", sourceType = "ScalaUDF"))))
    )

    val lineage = Lineage.trace(plan)
    assert(lineage == List(ColumnLineage(ColumnRef("mystery"), Set.empty, DerivationKind.Opaque)))
  }

  test("trace still resolves an UnknownExpression's understood children, even though the node itself is opaque") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        orders,
        List(
          NamedExpr(
            "mystery",
            UnknownExpression(
              "Generate(explode)",
              sourceType = "Generate",
              children = List(ColumnReference(ColumnRef("tags", Some("raw.orders"))))
            )
          )
        )
      )
    )

    val lineage = Lineage.trace(plan)
    // Real, understood sources resolve underneath an UnknownExpression,
    // but the column's overall derivation is still Opaque — an
    // UnknownExpression anywhere in the tree wins over whatever's beneath
    // it, the same "opaque anywhere -> opaque overall" rule a nested UDF
    // gets.
    assert(lineage == List(ColumnLineage(ColumnRef("mystery"), Set(ColumnRef("tags", Some("raw.orders"))), DerivationKind.Opaque)))
  }

  test("trace resolves Cast/Alias/Arithmetic/Comparison/BooleanExpr/Conditional/UDF transparently to their operands' sources") {
    val orders = Read(DatasetRef("raw.orders"))

    def lineageOf(expr: Expr): ColumnLineage =
      Lineage.trace(Write(DatasetRef("gold.out"), Project(orders, List(NamedExpr("out", expr))))).head

    val amount = ColumnReference(ColumnRef("amount", Some("raw.orders")))
    val status = ColumnReference(ColumnRef("status", Some("raw.orders")))
    val tax = ColumnReference(ColumnRef("tax", Some("raw.orders")))

    assert(lineageOf(Cast(amount, "double")).sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(lineageOf(Alias("renamed", amount)).sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(lineageOf(Arithmetic("*", List(amount, Literal(1.2, "double")))).sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(lineageOf(Comparison("=", status, Literal("ACTIVE", "string"))).sources == Set(ColumnRef("status", Some("raw.orders"))))
    assert(
      lineageOf(BooleanExpr("AND", List(Comparison("=", status, Literal("ACTIVE", "string")), Comparison(">", amount, Literal(0, "integer")))))
        .sources == Set(ColumnRef("status", Some("raw.orders")), ColumnRef("amount", Some("raw.orders")))
    )
    assert(
      lineageOf(UDF(Some("calculateRisk"), List(amount, status))).sources ==
        Set(ColumnRef("amount", Some("raw.orders")), ColumnRef("status", Some("raw.orders")))
    )
    // Every operand is a real column reference here (no literal on either
    // side) so a mutant that silently drops one operand's contribution
    // (rather than merely flipping `exists`/`forall` on the aggregation
    // flag) is actually observable: the resulting sources Set would be
    // missing one of the two columns.
    assert(lineageOf(Arithmetic("+", List(amount, tax))).sources == Set(ColumnRef("amount", Some("raw.orders")), ColumnRef("tax", Some("raw.orders"))))
    assert(lineageOf(Comparison("=", amount, tax)).sources == Set(ColumnRef("amount", Some("raw.orders")), ColumnRef("tax", Some("raw.orders"))))
    assert(lineageOf(BooleanExpr("AND", List(status, ColumnReference(ColumnRef("active", Some("raw.orders")))))).sources ==
      Set(ColumnRef("status", Some("raw.orders")), ColumnRef("active", Some("raw.orders"))))
  }

  test("trace includes a Conditional's branch conditions in its sources, not just its result values") {
    val orders = Read(DatasetRef("raw.orders"))
    val status = ColumnReference(ColumnRef("status", Some("raw.orders")))
    val tier = ColumnReference(ColumnRef("tier", Some("raw.orders")))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        orders,
        List(
          NamedExpr(
            "priority",
            Conditional(
              List((Comparison("=", status, Literal("ACTIVE", "string")), tier)),
              Some(Literal("none", "string"))
            )
          )
        )
      )
    )

    val lineage = Lineage.trace(plan)
    assert(lineage.head.sources == Set(ColumnRef("status", Some("raw.orders")), ColumnRef("tier", Some("raw.orders"))))
  }
}
