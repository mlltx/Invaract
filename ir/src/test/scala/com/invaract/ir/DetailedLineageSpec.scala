// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

/** `Lineage.traceDetailed`: values against conditions, masking, windows, generators, expansions, a read's known
  * columns, and the plan's own indirect lineage resolved through it. */
class DetailedLineageSpec extends AnyFunSuite {
  private val orders = Read(DatasetRef("orders"))
  private val customers = Read(DatasetRef("customers"))
  private def col(name: String, q: String = "orders") = ColumnReference(ColumnRef(name, Some(q)))
  private def bare(name: String) = ColumnReference(ColumnRef(name))
  private def src(name: String, q: String = "orders") = ColumnRef(name, Some(q))
  private val ordersColumns: Read => Option[Set[String]] = {
    case Read(DatasetRef("orders"), _, _)    => Some(Set("id", "customer_id", "amount", "status", "email"))
    case Read(DatasetRef("customers"), _, _) => Some(Set("customer_id", "name", "country"))
    case _                                   => None
  }

  private def column(plan: Plan, name: String, readColumns: Read => Option[Set[String]] = Lineage.NoReadColumns): ColumnDependencies =
    Lineage.traceDetailed(Write(DatasetRef("out"), plan), readColumns).columns.find(_.lineage.output.name == name).get

  private def one(expr: Expr): ColumnDependencies = column(Project(orders, List(NamedExpr("out", expr))), "out")

  // --- a read's known columns ------------------------------------------------------------------------------------------

  test("a read whose columns are known gives a column only a name it has; one not known gives any name, as before") {
    val joined = Join(orders, customers, JoinType.Inner, Some(Comparison("=", col("customer_id"), col("customer_id", "customers"))))
    val plan = Project(joined, List(NamedExpr("name", bare("name")), NamedExpr("amount", bare("amount"))))
    assert(column(plan, "name", ordersColumns).values == Set(src("name", "customers")))
    assert(column(plan, "amount", ordersColumns).values == Set(src("amount")))
    assert(column(plan, "name").values == Set(src("name"), src("name", "customers")), "unknown columns: both reads, as trace always did")
    assert(column(plan, "name", { case r if r eq orders => Some(Set("id")); case _ => None }).values == Set(src("name", "customers")))
  }

  test("a read's known columns are matched ignoring case; a name it lacks makes the reference resolve to nothing") {
    val plan = Project(orders, List(NamedExpr("a", bare("AMOUNT")), NamedExpr("z", bare("zzz"))))
    val upper: Read => Option[Set[String]] = _ => Some(Set("Amount"))
    assert(column(plan, "a", upper).values == Set(src("AMOUNT")))
    assert(column(plan, "z", upper).values.isEmpty)
    assert(column(plan, "z", upper).lineage.derivation == DerivationKind.Direct, "an unresolvable reference keeps its fallback")
  }

  test("a qualifier naming another read still rules a read out, whatever its columns") {
    val plan = Project(orders, List(NamedExpr("n", col("name", "customers"))))
    assert(column(plan, "n", ordersColumns).values.isEmpty)
    assert(column(plan, "n").values.isEmpty)
  }

  test("trace with known columns is traceDetailed's lineage; trace without them knows none") {
    val plan = Write(DatasetRef("out"), Project(Join(orders, customers, JoinType.Inner), List(NamedExpr("name", bare("name")))))
    assert(Lineage.trace(plan, ordersColumns) == Lineage.traceDetailed(plan, ordersColumns).columns.map(_.lineage))
    assert(Lineage.trace(plan) == Lineage.trace(plan, Lineage.NoReadColumns))
    assert(Lineage.trace(plan, ordersColumns).head.sources == Set(src("name", "customers")))
    assert(Lineage.NoReadColumns(orders).isEmpty)
  }

  // --- conditions ------------------------------------------------------------------------------------------------------

  test("CASE WHEN: the value's columns are its values, the tested columns its Conditional lineage; trace counts both") {
    val c = one(Conditional(List(Comparison(">", col("amount"), Literal(1, "int")) -> col("status")), Some(Literal("small", "string"))))
    assert(c.values == Set(src("status")))
    assert(c.indirect == Set(IndirectLineage(src("amount"), IndirectKind.Conditional, Some("out"))))
    assert(c.lineage.sources == Set(src("amount"), src("status")))
    assert(c.lineage.derivation == DerivationKind.Computed)
  }

  test("a column both tested and taken is a value and a condition; constant values on a tested column still compute") {
    val both = one(Conditional(List(Comparison("=", col("status"), Literal("a", "string")) -> col("status")), None))
    assert(both.values == Set(src("status")) && both.indirect.map(_.source) == Set(src("status")))
    val constants = one(Conditional(List(Comparison(">", col("amount"), Literal(1, "int")) -> Literal("a", "string")), Some(Literal("b", "string"))))
    assert(constants.values.isEmpty && constants.lineage.sources == Set(src("amount")))
    assert(constants.lineage.derivation == DerivationKind.Computed, "tested columns make it a computation, not a constant")
    val noBranchColumns = one(Conditional(List(Literal(true, "boolean") -> Literal("a", "string")), None))
    assert(noBranchColumns.lineage.derivation == DerivationKind.Constant)
  }

  test("conditions are carried through the operations above them, and through a projection above") {
    val cw = Conditional(List(Comparison(">", col("amount"), Literal(1, "int")) -> col("status")), None)
    val wrapped = one(Function("upper", List(cw)))
    assert(wrapped.values == Set(src("status")) && wrapped.indirect.map(_.source) == Set(src("amount")))
    val inner = Project(orders, List(NamedExpr("size", cw)))
    val outer = column(Project(inner, List(NamedExpr("label", bare("size")))), "label")
    assert(outer.values == Set(src("status")))
    assert(outer.indirect == Set(IndirectLineage(src("amount"), IndirectKind.Conditional, Some("label"))))
  }

  test("a condition's own conditions are conditions too") {
    val nested = Conditional(List(Conditional(List(Comparison(">", col("id"), Literal(0, "int")) -> col("amount")), None) -> col("status")), None)
    assert(one(nested).indirect.map(_.source) == Set(src("id"), src("amount")))
    assert(one(nested).values == Set(src("status")))
  }

  // --- masking ---------------------------------------------------------------------------------------------------------

  test("the masking functions are the hashes and mask, named in any case") {
    assert(Lineage.MaskingFunctions == Set("sha", "sha1", "sha2", "md5", "crc32", "hash", "xxhash64", "mask"))
    assert(one(Function("SHA2", List(col("email"), Literal(256, "int")))).masked == Set(src("email")))
    assert(one(Function("md5", List(col("email")))).masked == Set(src("email")))
    assert(one(Function("upper", List(col("email")))).masked.isEmpty)
  }

  test("a count masks what it counts; a sum does not") {
    assert(Lineage.MaskingAggregates == Set("count", "approx_count_distinct"))
    assert(one(AggregateCall("COUNT", col("id"))).masked == Set(src("id")))
    assert(one(AggregateCall("approx_count_distinct", col("id"), distinct = true)).masked == Set(src("id")))
    assert(one(AggregateCall("SUM", col("amount"))).masked.isEmpty)
    assert(one(Arithmetic("/", List(AggregateCall("SUM", col("amount")), AggregateCall("COUNT", col("amount"))))).masked.isEmpty,
      "summed in the clear too: not masked")
  }

  test("a source is masked only when it reaches the column through no unmasked path") {
    val hashed = Function("sha2", List(col("email"), Literal(256, "int")))
    assert(one(Function("concat", List(hashed, col("email")))).masked.isEmpty)
    assert(one(Function("concat", List(hashed, col("status")))).masked == Set(src("email")))
    assert(one(Function("concat", List(hashed, col("status")))).values == Set(src("email"), src("status")))
    assert(one(Arithmetic("+", List(Function("hash", List(col("id"))), col("amount")))).masked == Set(src("id")))
  }

  test("masking follows a CASE WHEN's values, and a masked value tested elsewhere is still masked") {
    val cw = Conditional(List(Comparison(">", col("amount"), Literal(1, "int")) -> Function("md5", List(col("email")))), Some(col("status")))
    assert(one(cw).masked == Set(src("email")))
    val unmaskedElse = Conditional(List(Literal(true, "boolean") -> Function("md5", List(col("email")))), Some(col("email")))
    assert(one(unmaskedElse).masked.isEmpty)
    val testedRaw = Conditional(List(Comparison("=", col("email"), Literal("x", "string")) -> Function("md5", List(col("email")))), None)
    assert(one(testedRaw).masked == Set(src("email")), "a test reveals nothing in the value: the value is still masked")
    assert(one(Alias("e", Function("sha1", List(col("email"))))).masked == Set(src("email")))
  }

  // --- windows ---------------------------------------------------------------------------------------------------------

  test("a window column's partitioning and ordering are its Window lineage; its arguments are its values") {
    val w = Window(orders, List(NamedExpr("rank", Function("row_number", Nil)), NamedExpr("running", AggregateCall("SUM", col("amount")))),
      List(col("customer_id")), List(SortOrder(col("id"))))
    val rank = column(Project(w, List(NamedExpr("rank", bare("rank")))), "rank")
    assert(rank.values.isEmpty && rank.lineage.sources.isEmpty, "trace never counted a window's spec as a source")
    assert(rank.indirect == Set(IndirectLineage(src("customer_id"), IndirectKind.Window, Some("rank")), IndirectLineage(src("id"), IndirectKind.Window, Some("rank"))))
    assert(rank.lineage.derivation == DerivationKind.Constant)
    val running = column(w, "running")
    assert(running.values == Set(src("amount")) && running.indirect.map(_.kind) == Set(IndirectKind.Window))
    val id = column(Project(w, List(NamedExpr("id", bare("id")))), "id")
    assert(id.values == Set(src("id")) && id.indirect.isEmpty, "a passed-through column: its own value, no window lineage")
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), w)).indirect.isEmpty, "not the whole output's")
  }

  // --- generators and expansions ----------------------------------------------------------------------------------------

  test("a generated column comes from the generator's arguments; the input's columns pass through") {
    val g = Generate(orders, Function("explode", List(col("status"))), List("tag"))
    val plan = Project(g, List(NamedExpr("id", bare("id")), NamedExpr("tag", bare("tag"))))
    assert(column(plan, "tag").values == Set(src("status")))
    assert(column(plan, "tag").lineage.derivation == DerivationKind.Computed)
    assert(column(plan, "id").values == Set(src("id")) && column(plan, "id").lineage.derivation == DerivationKind.Direct)
    val posexplode = Generate(orders, Function("posexplode", List(col("status"))), List("pos", "col"), outer = true)
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), posexplode)).columns.map(_.lineage.output.name) == List("pos", "col"))
    assert(column(posexplode, "pos").values == Set(src("status")))
  }

  test("an expanded column's sources are every projection's i-th expression; a literal one adds none") {
    val e = Expand(orders,
      List(List(col("amount"), col("customer_id"), Literal(0, "int")), List(col("amount"), Literal(null, "null"), Literal(1, "int"))),
      List("amount", "customer_id", "gid"))
    val agg = Aggregate(e, List(bare("customer_id"), bare("gid")), List(NamedExpr("customer_id", bare("customer_id")), NamedExpr("total", AggregateCall("SUM", bare("amount")))))
    assert(column(agg, "total").values == Set(src("amount")) && column(agg, "total").lineage.aggregated)
    assert(column(agg, "customer_id").values == Set(src("customer_id")))
    assert(column(agg, "customer_id").lineage.derivation == DerivationKind.Computed, "a key or a null: not a pure passthrough")
    assert(column(agg, "total").lineage.derivation == DerivationKind.Computed)
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), agg)).indirect == List(IndirectLineage(src("customer_id"), IndirectKind.GroupBy)))
    val gid = column(Project(e, List(NamedExpr("g", bare("gid")))), "g")
    assert(gid.values.isEmpty && gid.lineage.derivation == DerivationKind.Constant)
    assert(column(Project(e, List(NamedExpr("x", bare("nope")))), "x").values.isEmpty, "a name the expansion lacks is not passed through")
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), e)).columns.map(_.lineage.output.name) == List("amount", "customer_id", "gid"))
  }

  test("a name an expansion declares twice is its first declaration's; a projection short of a column adds nothing") {
    val e = Expand(orders, List(List(col("customer_id"), Literal(null, "null")), List(col("customer_id"))), List("k", "k"))
    assert(column(Project(e, List(NamedExpr("k", bare("k")))), "k").lineage.derivation == DerivationKind.Direct)
    val short = Expand(orders, List(List(col("amount")), List(col("amount"), col("status"))), List("a", "s"))
    assert(column(Project(short, List(NamedExpr("s", bare("s")))), "s").values == Set(src("status")))
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), e)).columns.size == 1)
  }

  // --- the plan's own indirect lineage ---------------------------------------------------------------------------------

  test("a filter on a computed column is a filter on what it was computed from") {
    val computed = Project(orders, List(NamedExpr("x", Arithmetic("*", List(col("amount"), Literal(2, "int")))), NamedExpr("id", col("id"))))
    val plan = Project(Filter(computed, Comparison(">", bare("x"), Literal(1, "int"))), List(NamedExpr("id", bare("id"))))
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), plan)).indirect == List(IndirectLineage(src("amount"), IndirectKind.Filter)))
  }

  test("every filter, join, grouping and sort, in plan order, each source once per kind; a condition-less join adds none") {
    val joined = Join(orders, customers, JoinType.Inner, Some(Comparison("=", col("customer_id"), col("customer_id", "customers"))))
    val filtered = Filter(joined, Comparison("=", col("country", "customers"), Literal("uk", "string")))
    val grouped = Aggregate(filtered, List(col("name", "customers")), List(NamedExpr("name", col("name", "customers")), NamedExpr("n", AggregateCall("COUNT", col("id")))))
    val sorted = Sort(grouped, List(SortOrder(bare("n"))))
    val found = Lineage.traceDetailed(Write(DatasetRef("out"), sorted), ordersColumns).indirect
    assert(found == List(
      IndirectLineage(src("id"), IndirectKind.Sort),
      IndirectLineage(src("name", "customers"), IndirectKind.GroupBy),
      IndirectLineage(src("country", "customers"), IndirectKind.Filter),
      IndirectLineage(src("customer_id", "customers"), IndirectKind.Join),
      IndirectLineage(src("customer_id"), IndirectKind.Join)
    ))
    assert(Lineage.traceDetailed(Project(Join(orders, customers, JoinType.Cross), List(NamedExpr("id", col("id"))))).indirect.isEmpty)
  }

  test("a subtree two branches share is walked once; a source two filters test is listed once") {
    val shared = Filter(orders, Comparison(">", col("amount"), Literal(1, "int")))
    val plan = Union(List(Project(shared, List(NamedExpr("id", col("id")))), Project(Filter(shared, Comparison(">", col("amount"), Literal(2, "int"))), List(NamedExpr("id", col("id"))))))
    assert(Lineage.traceDetailed(Write(DatasetRef("out"), plan)).indirect == List(IndirectLineage(src("amount"), IndirectKind.Filter)))
  }

  test("a plan that is not a Write is traced from its top; the kinds are the six OpenLineage subtypes") {
    val plan = Project(Sort(orders, List(SortOrder(col("amount")))), List(NamedExpr("id", col("id"))))
    val detailed = Lineage.traceDetailed(plan)
    assert(detailed.columns.map(_.lineage.output.name) == List("id"))
    assert(detailed.indirect == List(IndirectLineage(src("amount"), IndirectKind.Sort)))
    assert(IndirectKind.all == List(IndirectKind.Filter, IndirectKind.Join, IndirectKind.GroupBy, IndirectKind.Sort, IndirectKind.Window, IndirectKind.Conditional))
  }
}
