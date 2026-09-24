// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `PropertyAnalysis` against hand-built `Plan`/
  * `Expr` trees — no Spark session needed, mirroring `LineageSpec`'s own
  * style. Test names cross-reference the worked examples in
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md §7 where applicable.
  */
class PropertyAnalysisSpec extends AnyFunSuite {

  private val source = DatasetRef("raw.source")
  private def read(alias: Option[String] = None) = Read(source, alias)
  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))

  private def analyzeOne(plan: Plan, axioms: Map[ColumnRef, ColumnPropertyState] = Map.empty): ColumnPropertyState =
    PropertyAnalysis.analyze(Write(DatasetRef("gold.out"), plan), axioms).head.state

  private def project(input: Plan, name: String, expr: Expr): Plan = Project(input, List(NamedExpr(name, expr)))

  // --- NotNull ---------------------------------------------------------------

  test("NotNull: no axiom and no filter is Unknown") {
    val state = analyzeOne(project(read(), "customer_id", col("customer_id")))
    assert(state.notNull == NullabilityFact.Unknown)
  }

  test("NotNull: propagates through a pure passthrough from an axiom (Example 5)") {
    val axioms = Map(ColumnRef("customer_id", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val state = analyzeOne(project(read(), "customer_id", col("customer_id", Some(source.location))), axioms)
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("NotNull: established by a Filter(IS NOT NULL) with no input axiom at all (Example 1)") {
    val filtered = Filter(read(), Function("ISNOTNULL", List(col("customer_id"))))
    val state = analyzeOne(project(filtered, "customer_id", col("customer_id")))
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("NotNull: removing the filter drops back to Unknown, not Proven (Example 1's negative case)") {
    val state = analyzeOne(project(read(), "customer_id", col("customer_id")))
    assert(state.notNull == NullabilityFact.Unknown)
  }

  test("NotNull: a NOT(IS NULL) has the same effect as a direct IS NOT NULL") {
    val filtered = Filter(read(), BooleanExpr("NOT", List(Function("ISNULL", List(col("customer_id"))))))
    val state = analyzeOne(project(filtered, "customer_id", col("customer_id")))
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("NotNull: an OR of two IS NOT NULL checks proves nothing (only one side is guaranteed)") {
    val filtered = Filter(read(), BooleanExpr("OR", List(Function("ISNOTNULL", List(col("a"))), Function("ISNOTNULL", List(col("b"))))))
    val stateA = analyzeOne(project(filtered, "a", col("a")))
    assert(stateA.notNull == NullabilityFact.Unknown)
  }

  test("NotNull: a literal null is Refuted, not merely Unknown") {
    val state = analyzeOne(project(read(), "x", Literal(null, "string")))
    assert(state.notNull == NullabilityFact.Refuted)
  }

  test("NotNull: a UDF is always Unknown-with-unsupported, even over an otherwise-Proven column") {
    val axioms = Map(ColumnRef("customer_id", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val state = analyzeOne(project(read(), "x", UDF(Some("myFn"), List(col("customer_id", Some(source.location))))), axioms)
    assert(state.notNull == NullabilityFact.Unknown)
    assert(state.unsupported)
  }

  // --- Join semantics (Example 6) --------------------------------------------

  private def joinScenario(joinType: JoinType): ColumnPropertyState = {
    val customers = Read(DatasetRef("raw.customers"), alias = Some("c"))
    val transactions = Read(DatasetRef("raw.transactions"), alias = Some("t"))
    val axioms = Map(ColumnRef("customer_id", Some("c")) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val joined = Join(transactions, customers, joinType, Some(Comparison("=", col("customer_id", Some("t")), col("customer_id", Some("c")))))
    analyzeOne(project(joined, "customer_id", col("customer_id", Some("c"))), axioms)
  }

  test("Join: INNER JOIN preserves the joined side's own NotNull axiom (Example 6)") {
    assert(joinScenario(JoinType.Inner).notNull == NullabilityFact.Proven)
  }

  test("Join: LEFT OUTER JOIN demotes the right side's NotNull to Unknown, same input contract (Example 6's negative case)") {
    assert(joinScenario(JoinType.LeftOuter).notNull == NullabilityFact.Unknown)
  }

  test("Join: RIGHT OUTER JOIN preserves the right side (the demoted side is the left)") {
    assert(joinScenario(JoinType.RightOuter).notNull == NullabilityFact.Proven)
  }

  test("Join: FULL OUTER JOIN demotes both sides") {
    assert(joinScenario(JoinType.FullOuter).notNull == NullabilityFact.Unknown)
  }

  test("Join: CROSS JOIN preserves both sides, the same as INNER") {
    assert(joinScenario(JoinType.Cross).notNull == NullabilityFact.Proven)
  }

  test("Join: LEFT SEMI/ANTI preserve the left side's own axiom (only left-side columns ever appear in the output)") {
    val transactions = Read(DatasetRef("raw.transactions"), alias = Some("t"))
    val customers = Read(DatasetRef("raw.customers"), alias = Some("c"))
    val axioms = Map(ColumnRef("id", Some("t")) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    def leftSideState(joinType: JoinType): ColumnPropertyState = {
      val joined = Join(transactions, customers, joinType, Some(Comparison("=", col("id", Some("t")), col("customer_id", Some("c")))))
      analyzeOne(project(joined, "id", col("id", Some("t"))), axioms)
    }
    assert(leftSideState(JoinType.LeftSemi).notNull == NullabilityFact.Proven)
    assert(leftSideState(JoinType.LeftAnti).notNull == NullabilityFact.Proven)
  }

  test("Join: LEFT SEMI/ANTI also preserve (never demote) a right-side reference, even though real Spark output never projects one") {
    val transactions = Read(DatasetRef("raw.transactions"), alias = Some("t"))
    val customers = Read(DatasetRef("raw.customers"), alias = Some("c"))
    val axioms = Map(ColumnRef("customer_id", Some("c")) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val joined = Join(transactions, customers, JoinType.LeftSemi, Some(Comparison("=", col("id", Some("t")), col("customer_id", Some("c")))))
    val state = analyzeOne(project(joined, "customer_id", col("customer_id", Some("c"))), axioms)
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("Join: outputsOfT's own Join case (a bare Join directly beneath Write, no Project above it) demotes correctly too") {
    val left = Project(Read(DatasetRef("raw.transactions"), alias = Some("t")), List(NamedExpr("id", col("id", Some("t")))))
    val right = Project(Read(DatasetRef("raw.customers"), alias = Some("c")), List(NamedExpr("customer_id", col("customer_id", Some("c")))))
    val axioms = Map(ColumnRef("customer_id", Some("c")) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val joined = Join(left, right, JoinType.LeftOuter, Some(Comparison("=", col("id", Some("t")), col("customer_id", Some("c")))))
    val results = PropertyAnalysis.analyze(Write(DatasetRef("gold.out"), joined), axioms)
    val customerIdResult = results.find(_.output.name == "customer_id").get
    assert(customerIdResult.state.notNull == NullabilityFact.Unknown, "the right side of a LEFT OUTER join must be demoted here too")
  }

  // --- EqualsConstant ----------------------------------------------------------

  test("EqualsConstant: a literal constant is proven directly (Example 2)") {
    val state = analyzeOne(project(read(), "currency", Literal("GBP", "string")))
    assert(state.equalsConstant.contains(Property.EqualsConstant("GBP", "string")))
  }

  test("EqualsConstant: a plain passthrough with no axiom proves nothing (Example 2's negative case)") {
    val state = analyzeOne(project(read(), "currency", col("currency")))
    assert(state.equalsConstant.isEmpty)
  }

  test("EqualsConstant: two different constants across a Union collapse to unproven, not one silently winning") {
    val left = project(read(Some("l")), "currency", Literal("GBP", "string"))
    val right = project(read(Some("r")), "currency", Literal("USD", "string"))
    val state = analyzeOne(Union(List(left, right)))
    assert(state.equalsConstant.isEmpty, "differing per-branch constants must not collapse to either one")
  }

  test("EqualsConstant: the same constant on every Union branch is preserved") {
    val left = project(read(Some("l")), "currency", Literal("GBP", "string"))
    val right = project(read(Some("r")), "currency", Literal("GBP", "string"))
    val state = analyzeOne(Union(List(left, right)))
    assert(state.equalsConstant.contains(Property.EqualsConstant("GBP", "string")))
  }

  // --- OneOf (Example 3) --------------------------------------------------------

  private def caseWhenStatus(withElse: Boolean): Plan = {
    val branches = List((col("is_active"), Literal("ACTIVE", "string")))
    val elseValue = if (withElse) Some(Literal("INACTIVE", "string")) else None
    project(read(), "status", Conditional(branches, elseValue))
  }

  test("OneOf: a CASE WHEN with an ELSE, both branches constants, proves the finite value set (Example 3)") {
    val state = analyzeOne(caseWhenStatus(withElse = true))
    assert(state.oneOf.contains(Property.OneOf(Set("ACTIVE", "INACTIVE"), "string")))
  }

  test("OneOf: the same CASE WHEN with no ELSE proves nothing - never collapses to a false Guaranteed") {
    val state = analyzeOne(caseWhenStatus(withElse = false))
    assert(state.oneOf.isEmpty)
    assert(state.notNull == NullabilityFact.Unknown, "a missing ELSE means the result may be null for a non-matching row")
  }

  test("OneOf: a plain passthrough with no axiom proves nothing (Example 3's negative case)") {
    val state = analyzeOne(project(read(), "status", col("status")))
    assert(state.oneOf.isEmpty)
  }

  test("OneOf: established directly from an IN (...) filter") {
    val filtered = Filter(read(), Function("IN", List(col("status"), Literal("ACTIVE", "string"), Literal("INACTIVE", "string"))))
    val state = analyzeOne(project(filtered, "status", col("status")))
    assert(state.oneOf.contains(Property.OneOf(Set("ACTIVE", "INACTIVE"), "string")))
  }

  // --- Range (Examples 4 and 7) --------------------------------------------------

  test("Range: a CASE WHEN clamp proves the bound with NO input axiom at all (Example 4)") {
    val branches = List((Comparison("<", col("amount"), Literal(0, "integer")), Literal(0, "integer")))
    val plan = project(read(), "amount", Conditional(branches, Some(col("amount"))))
    val state = analyzeOne(plan)
    assert(state.range.contains(Property.Range(gte = Some(0))), s"expected gte=0, got ${state.range}")
  }

  test("Range: the CASE clamp's ELSE branch narrowing wins even over a contradicting input axiom") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(lte = Some(-5)))))
    val branches = List((Comparison("<", col("amount", Some(source.location)), Literal(0, "integer")), Literal(0, "integer")))
    val plan = project(read(), "amount", Conditional(branches, Some(col("amount", Some(source.location)))))
    val state = analyzeOne(plan, axioms)
    assert(state.range.exists(_.gte.contains(BigDecimal(0))))
  }

  test("Range: negating a non-negative axiom produces an upper-bounded-at-zero envelope (Example 7's setup)") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(0)))))
    val plan = project(read(), "amount", Arithmetic("NEGATE", List(col("amount", Some(source.location)))))
    val state = analyzeOne(plan, axioms)
    assert(state.range.contains(Property.Range(lte = Some(0))), s"expected lte=0, got ${state.range}")
  }

  test("Range: ABS establishes gte=0 unconditionally, regardless of the argument's own range") {
    val state = analyzeOne(project(read(), "x", Function("ABS", List(col("amount")))))
    assert(state.range.contains(Property.Range(gte = Some(0))))
  }

  test("NotNull: COALESCE(x, <non-null literal>) is Proven regardless of x's own nullability") {
    val state = analyzeOne(project(read(), "x", Function("COALESCE", List(col("maybe_null"), Literal(0, "integer")))))
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("NotNull: COALESCE(x, y) with no literal fallback is Proven only if every argument is Proven") {
    val axioms = Map(ColumnRef("a", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val state = analyzeOne(project(read(), "x", Function("COALESCE", List(col("a", Some(source.location)), col("b")))))
    assert(state.notNull == NullabilityFact.Unknown, "b has no axiom, so the fallback itself isn't proven non-null")
  }

  test("unsupported propagates through COALESCE when an argument is opaque") {
    val state = analyzeOne(project(read(), "x", Function("COALESCE", List(UDF(None, Nil), Literal(0, "integer")))))
    assert(state.unsupported)
  }

  test("Range: a literal shift (amount + 10) shifts a known axiom's bound") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(0)))))
    val plan = project(read(), "x", Arithmetic("+", List(col("amount", Some(source.location)), Literal(10, "integer"))))
    val state = analyzeOne(plan, axioms)
    assert(state.range.contains(Property.Range(gte = Some(10))))
  }

  test("Range: subtraction is order-sensitive (amount - 10 vs 10 - amount)") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(0)))))
    val aMinus10 = analyzeOne(project(read(), "x", Arithmetic("-", List(col("amount", Some(source.location)), Literal(10, "integer")))), axioms)
    assert(aMinus10.range.contains(Property.Range(gte = Some(-10))))
    val tenMinusA = analyzeOne(project(read(), "x", Arithmetic("-", List(Literal(10, "integer"), col("amount", Some(source.location))))), axioms)
    assert(tenMinusA.range.contains(Property.Range(lte = Some(10))))
  }

  test("Range: multiplying by a non-negative literal scales a known bound") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(2)))))
    val state = analyzeOne(project(read(), "x", Arithmetic("*", List(col("amount", Some(source.location)), Literal(3, "integer")))), axioms)
    assert(state.range.contains(Property.Range(gte = Some(6))))
  }

  test("Range: multiplying by a non-negative literal on the LEFT also scales the bound (3 * amount, not just amount * 3)") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(2)))))
    val state = analyzeOne(project(read(), "x", Arithmetic("*", List(Literal(3, "integer"), col("amount", Some(source.location))))), axioms)
    assert(state.range.contains(Property.Range(gte = Some(6))))
  }

  test("Range: multiplying by a NEGATIVE literal on the LEFT is also excluded from the whitelist") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(2)))))
    val state = analyzeOne(project(read(), "x", Arithmetic("*", List(Literal(-3, "integer"), col("amount", Some(source.location))))), axioms)
    assert(state.range.isEmpty)
  }

  test("Range: multiplying by a NEGATIVE literal is deliberately not on the whitelist (no false Guaranteed from a wrong sign flip)") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(2)))))
    val state = analyzeOne(project(read(), "x", Arithmetic("*", List(col("amount", Some(source.location)), Literal(-3, "integer")))), axioms)
    assert(state.range.isEmpty, s"a negative scaling factor must not be treated as a plain scale; got ${state.range}")
  }

  test("Range: a Filter establishing a lower bound, tightened by a second AND-ed upper bound") {
    val filtered = Filter(read(), BooleanExpr("AND", List(Comparison(">=", col("amount"), Literal(0, "integer")), Comparison("<=", col("amount"), Literal(100, "integer")))))
    val state = analyzeOne(project(filtered, "amount", col("amount")))
    assert(state.range.contains(Property.Range(gte = Some(0), lte = Some(100))))
  }

  test("Range: a literal on the left of a comparison flips the operator correctly (0 <= amount means amount >= 0)") {
    val filtered = Filter(read(), Comparison("<=", Literal(0, "integer"), col("amount")))
    val state = analyzeOne(project(filtered, "amount", col("amount")))
    assert(state.range.contains(Property.Range(gte = Some(0))))
  }

  // --- Cast --------------------------------------------------------------------

  test("Cast: preserves NotNull but drops Range/EqualsConstant/OneOf") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven, range = Some(Property.Range(gte = Some(0)))))
    val state = analyzeOne(project(read(), "x", Cast(col("amount", Some(source.location)), "double")), axioms)
    assert(state.notNull == NullabilityFact.Proven)
    assert(state.range.isEmpty)
  }

  // --- Aggregate/Window (MVP: Unknown, groupBy passthrough excepted) -----------

  test("Aggregate: a non-groupBy aggregate output is Unknown-with-unsupported even over a Proven-axiom column") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val agg = Aggregate(read(), groupBy = Nil, aggregates = List(NamedExpr("total", AggregateCall("SUM", col("amount", Some(source.location))))))
    val state = analyzeOne(agg, axioms)
    assert(state.notNull == NullabilityFact.Unknown)
    assert(state.unsupported)
  }

  test("Aggregate: a reference from an outer Project to the aggregate's own declared name resolves the same way outputsOfT would") {
    val axioms = Map(ColumnRef("customer_id", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val agg = Aggregate(
      read(),
      groupBy = List(col("customer_id", Some(source.location))),
      aggregates = List(NamedExpr("customer_id", col("customer_id", Some(source.location))), NamedExpr("total", AggregateCall("SUM", col("amount"))))
    )
    val outer = Project(agg, List(NamedExpr("out", col("customer_id"))))
    val state = analyzeOne(outer, axioms)
    assert(state.notNull == NullabilityFact.Proven)

    val outerTotal = Project(agg, List(NamedExpr("out", col("total"))))
    assert(analyzeOne(outerTotal, axioms).unsupported)
  }

  test("Aggregate: a bare groupBy passthrough column keeps its own axiom") {
    val axioms = Map(ColumnRef("customer_id", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val agg = Aggregate(
      read(),
      groupBy = List(col("customer_id", Some(source.location))),
      aggregates = List(NamedExpr("customer_id", col("customer_id", Some(source.location))))
    )
    val state = analyzeOne(agg, axioms)
    assert(state.notNull == NullabilityFact.Proven)
    assert(!state.unsupported)
  }

  test("Window: every windowed output column is Unknown-with-unsupported in MVP") {
    val windowed = Window(read(), windowExprs = List(NamedExpr("rn", Function("ROW_NUMBER", Nil))))
    val state = analyzeOne(project(windowed, "rn", col("rn")))
    assert(state.unsupported)
  }

  // --- Unsupported-construct propagation (§3.7) --------------------------------

  test("an unrecognized Function is NOT flagged unsupported on its own") {
    val state = analyzeOne(project(read(), "x", Function("SOME_UNKNOWN_FN", List(col("amount")))))
    assert(!state.unsupported, "an unrecognized Function is still a fully-understood shape, just with no value-domain rule")
  }

  test("an UnknownExpression is flagged unsupported") {
    val state = analyzeOne(project(read(), "x", UnknownExpression("opaque construct")))
    assert(state.unsupported)
  }

  test("NotNull: a Comparison of two Proven operands is itself Proven") {
    val axioms = Map(
      ColumnRef("a", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven),
      ColumnRef("b", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven)
    )
    val state = analyzeOne(project(read(), "x", Comparison(">", col("a", Some(source.location)), col("b", Some(source.location)))), axioms)
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("unsupported propagates through a Comparison when only ONE side is opaque") {
    val state = analyzeOne(project(read(), "x", Comparison("=", UDF(None, Nil), Literal(1, "integer"))))
    assert(state.unsupported)
  }

  test("an AggregateCall appearing outside a real Aggregate/Window (a malformed hand-built plan) resolves safely to Unknown-unsupported") {
    val state = analyzeOne(project(read(), "x", AggregateCall("SUM", col("amount"))))
    assert(state.unsupported)
  }

  test("a reference an inner Project simply doesn't declare resolves to Unknown, not a crash") {
    val inner = Project(read(), List(NamedExpr("a", col("a"))))
    val outer = Project(inner, List(NamedExpr("b", col("b"))))
    val state = analyzeOne(outer)
    assert(state == ColumnPropertyState.Unknown)
  }

  test("an inner Project with multiple declared columns resolves the one actually named, not a different one") {
    val axioms = Map(
      ColumnRef("a", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven),
      ColumnRef("b", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Unknown)
    )
    val inner = Project(read(), List(NamedExpr("a", col("a", Some(source.location))), NamedExpr("b", col("b", Some(source.location)))))
    val outer = Project(inner, List(NamedExpr("out", col("b"))))
    val state = analyzeOne(outer, axioms)
    assert(state.notNull == NullabilityFact.Unknown, "must resolve 'b' specifically, not accidentally fall through to 'a'")
  }

  test("a Literal typed as a non-numeric type never gets a Range, even if its value would parse as one") {
    val state = analyzeOne(project(read(), "x", Literal("123", "string")))
    assert(state.range.isEmpty, "a string-typed literal must never be treated as numeric just because its text happens to look numeric")
  }

  test("unsupported propagates through an otherwise-ordinary operation wrapping it") {
    val state = analyzeOne(project(read(), "x", Arithmetic("+", List(UDF(None, Nil), Literal(1, "integer")))))
    assert(state.unsupported)
  }

  test("Function: unsupported propagates when ANY argument is unsupported, not only when ALL are") {
    val oneBad = Function("SOME_FN", List(col("clean"), UDF(None, Nil)))
    val state = analyzeOne(project(read(), "x", oneBad))
    assert(state.unsupported, "a single opaque argument must taint the whole call, not require every argument to be opaque")
  }

  test("Union: branches of different widths align by position up to the narrowest one, not the widest") {
    val wide = Project(read(Some("l")), List(NamedExpr("a", col("a")), NamedExpr("b", col("b"))))
    val narrow = Project(read(Some("r")), List(NamedExpr("a", Literal(1, "integer"))))
    // The narrower branch has only 1 column; the analysis must not crash or
    // fabricate a phantom second column out of the wider branch alone.
    val results = PropertyAnalysis.analyze(Write(DatasetRef("gold.out"), Union(List(wide, narrow))), Map.empty)
    assert(results.size == 1, s"expected alignment to the narrowest (1-column) branch, got ${results.size} columns")
  }

  // --- Struct/nested fields: construct-then-extract (§3.8) --------------------

  test("StructField(StructConstruct(...), name): resolves straight through to the matching field's own value state") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string"), "city" -> Literal("SF", "string")))
    val state = analyzeOne(project(read(), "z", StructField(built, "zip")))
    assert(state.equalsConstant.contains(Property.EqualsConstant("94107", "string")))
    assert(!state.unsupported)
  }

  test("StructField(StructConstruct(...), name): picks the field actually named, not merely the first one") {
    val built = StructConstruct(List("a" -> Literal(1, "integer"), "b" -> Literal(2, "integer")))
    val stateA = analyzeOne(project(read(), "x", StructField(built, "a")))
    val stateB = analyzeOne(project(read(), "x", StructField(built, "b")))
    assert(stateA.equalsConstant.contains(Property.EqualsConstant(1, "integer")))
    assert(stateB.equalsConstant.contains(Property.EqualsConstant(2, "integer")))
  }

  test("StructField(StructConstruct(...), name): a real axiom-backed range flows through the extracted field too") {
    val axioms = Map(ColumnRef("amount", Some(source.location)) -> ColumnPropertyState(range = Some(Property.Range(gte = Some(0)))))
    val built = StructConstruct(List("amt" -> col("amount", Some(source.location))))
    val state = analyzeOne(project(read(), "x", StructField(built, "amt")), axioms)
    assert(state.range.contains(Property.Range(gte = Some(0))))
  }

  test("StructField(StructConstruct(...), name): a name not present among the constructed fields is Unknown-with-unsupported") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val state = analyzeOne(project(read(), "x", StructField(built, "missing_field")))
    assert(state == ColumnPropertyState.Unknown.copy(unsupported = true))
  }

  test("StructField(other, name): any non-StructConstruct struct expression is Unknown-with-unsupported, even over a Proven-axiom column") {
    val axioms = Map(ColumnRef("address", Some(source.location)) -> ColumnPropertyState(notNull = NullabilityFact.Proven))
    val state = analyzeOne(project(read(), "x", StructField(col("address", Some(source.location)), "zip")), axioms)
    assert(state.unsupported)
    assert(state.notNull == NullabilityFact.Unknown, "the input struct's own axiom must not leak into an unresolvable nested-field access")
  }

  test("StructField(other, name): a struct value wrapped in a UDF stays Unknown-with-unsupported, not a crash") {
    val state = analyzeOne(project(read(), "x", StructField(UDF(Some("f"), Nil), "zip")))
    assert(state.unsupported)
  }

  test("StructField(StructField(StructConstruct(...), name1), name2): resolves through a doubly-nested struct construction") {
    // struct(geo = struct(code = "XYZ")).geo.code
    val built = StructConstruct(List("geo" -> StructConstruct(List("code" -> Literal("XYZ", "string")))))
    val state = analyzeOne(project(read(), "x", StructField(StructField(built, "geo"), "code")))
    assert(state.notNull == NullabilityFact.Proven)
    assert(state.equalsConstant.contains(Property.EqualsConstant("XYZ", "string")))
  }

  test("StructField(StructField(StructConstruct(...), name1), name2): None when the OUTER field name isn't present at the inner level") {
    // struct(geo = struct(code = "XYZ")).missing.code - "missing" isn't a field geo's struct declares
    val built = StructConstruct(List("geo" -> StructConstruct(List("code" -> Literal("XYZ", "string")))))
    val state = analyzeOne(project(read(), "x", StructField(StructField(built, "missing"), "code")))
    assert(state.unsupported)
  }

  test("StructField(StructField(StructConstruct(...), name1), name2): unsupported when the intermediate field's value isn't itself a struct") {
    // struct(geo = "not a struct").geo.code - "geo" resolves to a plain Literal, not a StructConstruct,
    // so nothing beneath it (.code) can be traced through - the type-guard in
    // reduceToStructConstruct, not just the field-name match, is what decides this.
    val built = StructConstruct(List("geo" -> Literal("flat", "string")))
    val state = analyzeOne(project(read(), "x", StructField(StructField(built, "geo"), "code")))
    assert(state.unsupported)
  }

  test("StructConstruct: a freshly-built struct is provably NotNull, regardless of any individual field's own nullability") {
    val built = StructConstruct(List("zip" -> Literal(null, "string"), "city" -> Literal("SF", "string")))
    val state = analyzeOne(project(read(), "addr", built))
    assert(state.notNull == NullabilityFact.Proven, "constructing the struct itself never yields SQL NULL, independent of its fields")
  }

  test("StructConstruct: NotNull holds even when a field's own value is opaque (a UDF)") {
    val built = StructConstruct(List("risk" -> UDF(Some("f"), Nil)))
    val state = analyzeOne(project(read(), "addr", built))
    assert(state.notNull == NullabilityFact.Proven)
  }

  test("StructConstruct: an empty struct construction is still provably NotNull") {
    val state = analyzeOne(project(read(), "addr", StructConstruct(Nil)))
    assert(state.notNull == NullabilityFact.Proven)
  }

  // --- Length (string constraints, §3.9) ---------------------------------------

  test("Length: a string literal is provably its own exact length") {
    val state = analyzeOne(project(read(), "x", Literal("ABCDEFGHIJ", "string")))
    assert(state.length.contains(Property.Length(exact = Some(10))))
  }

  test("Length: an empty-string literal has exact length 0, not treated as absent") {
    val state = analyzeOne(project(read(), "x", Literal("", "string")))
    assert(state.length.contains(Property.Length(exact = Some(0))))
  }

  test("Length: a numeric literal never gets a Length fact, even though its printed form has a length") {
    val state = analyzeOne(project(read(), "x", Literal(12345, "integer")))
    assert(state.length.isEmpty)
  }

  test("Length: propagates through a pure passthrough from an input axiom, the same as Range/NotNull (Example 5's shape)") {
    val axioms = Map(ColumnRef("identifier", Some(source.location)) -> ColumnPropertyState(length = Some(Property.Length(exact = Some(10)))))
    val state = analyzeOne(project(read(), "identifier", col("identifier", Some(source.location))), axioms)
    assert(state.length.contains(Property.Length(exact = Some(10))))
  }

  test("Length: a plain passthrough with no axiom proves nothing") {
    val state = analyzeOne(project(read(), "identifier", col("identifier")))
    assert(state.length.isEmpty)
  }

  test("Length: LENGTH(...) bridges to a numeric Range equal to its argument's own exact length") {
    val state = analyzeOne(project(read(), "x", Function("LENGTH", List(Literal("ABCDEFGHIJ", "string")))))
    assert(state.range.contains(Property.Range(gte = Some(10), lte = Some(10))))
  }

  test("Length: LENGTH(...) bridges a min/max Length envelope to the equivalent gte/lte Range") {
    val axioms = Map(ColumnRef("identifier", Some(source.location)) -> ColumnPropertyState(length = Some(Property.Length(min = Some(1), max = Some(50)))))
    val state = analyzeOne(project(read(), "x", Function("LENGTH", List(col("identifier", Some(source.location))))), axioms)
    assert(state.range.contains(Property.Range(gte = Some(1), lte = Some(50))))
  }

  test("Length: LENGTH(...) proves no Range at all when its argument's own length is unknown") {
    val state = analyzeOne(project(read(), "x", Function("LENGTH", List(col("identifier")))))
    assert(state.range.isEmpty)
    assert(!state.unsupported, "LENGTH is still a recognized, understood construct even with no length rule to apply")
  }

  test("Length: CHAR_LENGTH and CHARACTER_LENGTH are recognized aliases of LENGTH") {
    val charLength = analyzeOne(project(read(), "x", Function("CHAR_LENGTH", List(Literal("ABCDE", "string")))))
    val characterLength = analyzeOne(project(read(), "x", Function("CHARACTER_LENGTH", List(Literal("ABCDE", "string")))))
    assert(charLength.range.contains(Property.Range(gte = Some(5), lte = Some(5))))
    assert(characterLength.range.contains(Property.Range(gte = Some(5), lte = Some(5))))
  }

  test("Length: UPPER/LOWER preserve the argument's own exact Length verbatim") {
    val upper = analyzeOne(project(read(), "x", Function("UPPER", List(Literal("hello", "string")))))
    val lower = analyzeOne(project(read(), "x", Function("LOWER", List(Literal("hello", "string")))))
    assert(upper.length.contains(Property.Length(exact = Some(5))))
    assert(lower.length.contains(Property.Length(exact = Some(5))))
  }

  test("Length: TRIM narrows an exact argument length to an upper bound only, not the same exact value") {
    val state = analyzeOne(project(read(), "x", Function("TRIM", List(Literal("  hi  ", "string")))))
    assert(state.length.contains(Property.Length(max = Some(6))), s"expected an upper bound of 6 (the untrimmed length), got ${state.length}")
  }

  test("Length: TRIM over an argument with only a known max keeps that same max, not a fresh exact") {
    val axioms = Map(ColumnRef("identifier", Some(source.location)) -> ColumnPropertyState(length = Some(Property.Length(max = Some(20)))))
    val state = analyzeOne(project(read(), "x", Function("TRIM", List(col("identifier", Some(source.location))))), axioms)
    assert(state.length.contains(Property.Length(max = Some(20))))
  }

  test("Length: TRIM over an argument with no known length at all proves nothing") {
    val state = analyzeOne(project(read(), "x", Function("TRIM", List(col("identifier")))))
    assert(state.length.isEmpty)
  }

  test("Length: LTRIM and RTRIM are recognized aliases of TRIM, with the same upper-bound-only rule") {
    val ltrim = analyzeOne(project(read(), "x", Function("LTRIM", List(Literal("  hi  ", "string")))))
    val rtrim = analyzeOne(project(read(), "x", Function("RTRIM", List(Literal("  hi  ", "string")))))
    assert(ltrim.length.contains(Property.Length(max = Some(6))))
    assert(rtrim.length.contains(Property.Length(max = Some(6))))
  }

  test("Length: unsupported propagates through LENGTH/UPPER/TRIM when the argument is opaque") {
    assert(analyzeOne(project(read(), "x", Function("LENGTH", List(UDF(None, Nil))))).unsupported)
    assert(analyzeOne(project(read(), "x", Function("UPPER", List(UDF(None, Nil))))).unsupported)
    assert(analyzeOne(project(read(), "x", Function("TRIM", List(UDF(None, Nil))))).unsupported)
  }

  test("Length: Cast drops the Length fact, the same as it already drops Range/EqualsConstant/OneOf") {
    val axioms = Map(ColumnRef("identifier", Some(source.location)) -> ColumnPropertyState(length = Some(Property.Length(exact = Some(10)))))
    val state = analyzeOne(project(read(), "x", Cast(col("identifier", Some(source.location)), "string")), axioms)
    assert(state.length.isEmpty)
  }

  test("Length: a CASE WHEN of two different-length string literals proves only the enclosing min/max envelope, never a false exact") {
    val branches = List((col("is_short"), Literal("ab", "string")))
    val state = analyzeOne(project(read(), "x", Conditional(branches, Some(Literal("abcdefghij", "string")))))
    assert(state.length.contains(Property.Length(min = Some(2), max = Some(10))))
  }

  test("Conditional: a later branch's narrowing correctly inherits an EARLIER branch's condition negated, not asserted true") {
    // WHEN amount < 0 THEN -1
    // WHEN amount < 10 THEN amount   -- must be narrowed by NOT(amount < 0) i.e. amount >= 0, combined with amount < 10
    // ELSE 999
    val branch1Cond = Comparison("<", col("amount"), Literal(0, "integer"))
    val branch2Cond = Comparison("<", col("amount"), Literal(10, "integer"))
    val plan = project(
      read(),
      "amount",
      Conditional(List((branch1Cond, Literal(-1, "integer")), (branch2Cond, col("amount"))), Some(Literal(999, "integer")))
    )
    val state = analyzeOne(plan)
    // If branch2's narrowing wrongly inherited branch1's condition asserted
    // TRUE instead of negated, the union's lower bound collapses to
    // unbounded (None) instead of the correct gte=-1 - see this test's
    // derivation in the commit that added it for the full arithmetic.
    assert(state.range.exists(_.gte.contains(BigDecimal(-1))), s"expected a provable lower bound of -1, got ${state.range}")
    assert(state.range.exists(_.lte.contains(BigDecimal(999))), s"expected a provable upper bound of 999, got ${state.range}")
  }

  // --- definingExpr / analyzeExpr (nested-field tracing support) -------------

  test("definingExpr: returns the defining Expr and its resolving input Plan directly beneath a Write's Project") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val plan = Write(DatasetRef("gold.out"), project(read(), "address", built))
    assert(PropertyAnalysis.definingExpr(plan, "address") == Some((built, read())))
  }

  test("definingExpr: returns None when the requested name isn't defined by the top Project at all") {
    val plan = Write(DatasetRef("gold.out"), project(read(), "other", col("other")))
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: chases a single level of ColumnReference indirection back to the real defining Expr") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val inner = project(read(), "address", built)
    val outer = project(inner, "address", col("address"))
    val plan = Write(DatasetRef("gold.out"), outer)
    assert(PropertyAnalysis.definingExpr(plan, "address") == Some((built, read())))
  }

  test("definingExpr: chases multiple levels of ColumnReference indirection through an intervening Filter") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val innermost = project(read(), "address", built)
    val filtered = Filter(innermost, Function("ISNOTNULL", List(col("address"))))
    val mid = project(filtered, "address", col("address"))
    val outer = project(mid, "address", col("address"))
    val plan = Write(DatasetRef("gold.out"), outer)
    assert(PropertyAnalysis.definingExpr(plan, "address") == Some((built, read())))
  }

  test("definingExpr: passes through Sort and Limit exactly as it does Filter") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val sorted = Sort(project(read(), "address", built), List(SortOrder(col("address"), ascending = true)))
    val limited = Limit(sorted, 10)
    val plan = Write(DatasetRef("gold.out"), limited)
    assert(PropertyAnalysis.definingExpr(plan, "address") == Some((built, read())))
  }

  test("definingExpr: returns None for a struct read straight from an input Read, with no intervening Project at all") {
    val plan = Write(DatasetRef("gold.out"), read())
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: returns None when a ColumnReference chain bottoms out at a bare Read (struct passed straight through)") {
    val plan = Write(DatasetRef("gold.out"), project(read(), "address", col("address")))
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: returns None for an Aggregate output - no single defining Expr in the same sense") {
    val agg = Aggregate(read(), groupBy = Nil, aggregates = List(NamedExpr("address", AggregateCall("SUM", col("amount")))))
    val plan = Write(DatasetRef("gold.out"), agg)
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: returns None for a Window output") {
    val windowed = Window(read(), windowExprs = List(NamedExpr("address", Function("ROW_NUMBER", Nil))))
    val plan = Write(DatasetRef("gold.out"), windowed)
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: returns None for a Union output") {
    val left = project(read(Some("l")), "address", StructConstruct(List("zip" -> Literal("1", "string"))))
    val right = project(read(Some("r")), "address", StructConstruct(List("zip" -> Literal("2", "string"))))
    val plan = Write(DatasetRef("gold.out"), Union(List(left, right)))
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("definingExpr: returns None for a Join output") {
    val left = project(read(Some("l")), "address", StructConstruct(List("zip" -> Literal("1", "string"))))
    val right = read(Some("r"))
    val joined = Join(left, right, JoinType.Inner, None)
    val plan = Write(DatasetRef("gold.out"), joined)
    assert(PropertyAnalysis.definingExpr(plan, "address") == None)
  }

  test("analyzeExpr: resolves an already-extracted Expr the same way analyze resolves it inline") {
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val state = PropertyAnalysis.analyzeExpr(StructField(built, "zip"), read(), Map.empty)
    assert(state.notNull == NullabilityFact.Proven)
    assert(state.equalsConstant.contains(Property.EqualsConstant("94107", "string")))
  }
}
