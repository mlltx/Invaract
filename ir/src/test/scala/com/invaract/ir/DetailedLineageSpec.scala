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

  // --- a name both sides of a join have ----------------------------------------------------------------------------

  private val events = Read(DatasetRef("events"))
  private def keyed(right: Plan) = Join(orders, right, JoinType.Inner, Some(Comparison("=", col("id"), col("id", "events"))))
  private val eventsKey = Project(events, List(NamedExpr("id", col("id", "events")), NamedExpr("b", col("b", "events"))))

  test("a USING join's key is the side it is qualified by, not a side projecting another read's column of that name") {
    def key(q: String, right: Plan = eventsKey) = column(Project(keyed(right), List(NamedExpr("id", col("id", q)))), "id").values
    assert(key("orders") == Set(src("id")), "an inner or left join's key: the left side's")
    assert(key("events") == Set(src("id", "events")), "a right join's key: the right side's")
    assert(key("orders", Filter(Sort(Limit(eventsKey, 5), List(SortOrder(col("b", "events")))), Literal(true, "boolean"))) == Set(src("id")),
      "through filters, sorts and limits")
    val flipped = Join(eventsKey, orders, JoinType.Inner, None)
    assert(column(Project(flipped, List(NamedExpr("id", col("id")))), "id").values == Set(src("id")), "whichever side it is on")
  }

  test("a join's name stays both sides' when neither or both are ruled out") {
    def key(ref: Expr, right: Plan) = column(Project(keyed(right), List(NamedExpr("id", ref))), "id").values
    val both = Set(src("id"), src("id", "events"))
    assert(key(bare("id"), eventsKey) == both, "an unqualified reference: no side is ruled out")
    assert(key(col("id"), Project(events, List(NamedExpr("id", Arithmetic("+", List(col("id", "events"), Literal(1, "int"))))))) == both,
      "a computed column: whose it is is not known")
    assert(key(col("id"), Project(events, List(NamedExpr("id", bare("id"))))) == both, "a passed-through column with no qualifier")
    assert(key(col("id"), Join(eventsKey, Read(DatasetRef("x")), JoinType.Inner, None)) == both, "a join on the side: not followed")
    val twice = Join(eventsKey, Project(Read(DatasetRef("other")), List(NamedExpr("id", col("id", "other")))), JoinType.Inner, None)
    assert(column(Project(twice, List(NamedExpr("id", col("id")))), "id").values == Set(src("id", "events"), src("id", "other")),
      "both sides another's: stays both")
  }

  // --- copied and transformed, source by source ------------------------------------------------------------------

  test("each source is copied, transformed, or both: a union's branches each give their own; an operation transforms") {
    val copiedBranch = Project(orders, List(NamedExpr("v", col("amount"))))
    val computedBranch = Project(customers, List(NamedExpr("v", Arithmetic("+", List(col("customer_id", "customers"), Literal(1, "int"))))))
    val both = Project(orders, List(NamedExpr("v", Arithmetic("*", List(col("amount"), Literal(100, "int"))))))
    val constant = Project(customers, List(NamedExpr("v", Literal(null, "long"))))
    def of(branches: Plan*) = {
      val c = column(Project(Union(branches.toList), List(NamedExpr("v", bare("v")))), "v")
      (c.copied, c.transformed)
    }
    assert(of(copiedBranch, computedBranch) == ((Set(src("amount")), Set(src("customer_id", "customers")))))
    assert(of(copiedBranch, both) == ((Set(src("amount")), Set(src("amount")))), "the same source copied by one branch and computed by another")
    assert(of(copiedBranch, constant) == ((Set(src("amount")), Set.empty)), "a constant branch neither copies nor computes")
    assert(of(copiedBranch, constant)._1.nonEmpty && column(Project(Union(List(copiedBranch, constant)), List(NamedExpr("v", bare("v")))), "v").lineage.derivation != DerivationKind.Direct,
      "the column as a whole is still not a plain copy")
    val passed = one(col("amount"))
    assert((passed.copied, passed.transformed) == ((Set(src("amount")), Set.empty)), "passed through: copied")
    val computed = one(Arithmetic("+", List(col("amount"), col("id"))))
    assert((computed.copied, computed.transformed) == ((Set.empty, Set(src("amount"), src("id")))), "an operation: transformed")
    val chosen = one(Conditional(List(Comparison(">", col("amount"), Literal(1, "int")) -> col("status")), Some(col("email"))))
    assert((chosen.copied, chosen.transformed) == ((Set.empty, Set(src("status"), src("email")))), "a CASE WHEN's values: transformed, as OpenLineage counts them")
    val joined = Project(Join(copiedBranch, computedBranch, JoinType.Inner, None), List(NamedExpr("v", bare("v"))))
    assert(column(joined, "v").copied == Set(src("amount")) && column(joined, "v").transformed == Set(src("customer_id", "customers")), "both sides of a join keep theirs")
  }

  test("a read's known columns are matched ignoring case, and named as the read has them; a name it lacks makes the reference resolve to nothing") {
    val plan = Project(orders, List(NamedExpr("a", bare("AMOUNT")), NamedExpr("z", bare("zzz"))))
    val upper: Read => Option[Set[String]] = _ => Some(Set("Amount"))
    assert(column(plan, "a", upper).values == Set(src("Amount")), "the read's column, not the reference's spelling of it")
    assert(column(plan, "z", upper).values.isEmpty)
    assert(column(plan, "z", upper).lineage.derivation == DerivationKind.Direct, "an unresolvable reference keeps its fallback")
  }

  // --- names: an engine matches them ignoring case ---------------------------------------------------------------------

  private val amount: Read => Option[Set[String]] = { case r if r eq orders => Some(Set("amount", "id")); case _ => None }

  test("a column referred to in another case is the column of that name, at every node that declares columns") {
    val projected = Project(orders, List(NamedExpr("amount", bare("amount")), NamedExpr("id", bare("id"))))
    assert(column(Project(projected, List(NamedExpr("x", bare("AMOUNT")))), "x", amount).values == Set(src("amount")), "a projection")
    val agg = Aggregate(orders, List(bare("id")), List(NamedExpr("total", AggregateCall("SUM", bare("amount")))))
    assert(column(Project(agg, List(NamedExpr("x", bare("TOTAL")))), "x", amount).values == Set(src("amount")), "an aggregate")
    val win = Window(projected, List(NamedExpr("rank", Function("rank", Nil))), List(bare("id")), Nil)
    assert(column(Project(win, List(NamedExpr("x", bare("RANK")))), "x", amount).indirect.map(_.source) == Set(src("id")), "a window column")
    assert(column(Project(win, List(NamedExpr("x", bare("AMOUNT")))), "x", amount).values == Set(src("amount")), "through a window")
    val gen = Generate(projected, Function("explode", List(bare("amount"))), List("item"))
    assert(column(Project(gen, List(NamedExpr("x", bare("ITEM")))), "x", amount).values == Set(src("amount")), "a generated column")
    assert(column(Project(gen, List(NamedExpr("x", bare("ID")))), "x", amount).values == Set(src("id")), "through a generator")
    val exp = Expand(orders, List(List(bare("amount"), bare("id")), List(bare("amount"), Literal(null, "null"))), List("amount", "key"))
    assert(column(Project(exp, List(NamedExpr("x", bare("KEY")))), "x", amount).values == Set(src("id")), "an expansion's second column")
  }

  test("an exact name wins over one equal ignoring case; two equal ignoring case and none exact match neither") {
    val both = Project(orders, List(NamedExpr("Amount", bare("id")), NamedExpr("amount", bare("amount"))))
    assert(column(Project(both, List(NamedExpr("x", bare("amount")))), "x").values == Set(src("amount")))
    assert(column(Project(both, List(NamedExpr("x", bare("Amount")))), "x").values == Set(src("id")))
    assert(column(Project(both, List(NamedExpr("x", bare("AMOUNT")))), "x").values.isEmpty, "ambiguous: neither")
    val twice = Project(orders, List(NamedExpr("Amount", bare("amount")), NamedExpr("Amount", bare("id"))))
    assert(column(Project(twice, List(NamedExpr("x", bare("AMOUNT")))), "x").values == Set(src("amount")), "one name, declared twice: its first")
  }

  test("a read whose columns are not known names a source as the reference does") {
    assert(column(Project(orders, List(NamedExpr("x", bare("AMOUNT")))), "x").values == Set(src("AMOUNT")))
  }

  // --- unions match columns by position --------------------------------------------------------------------------------

  private val swapped = Union(List(
    Project(orders, List(NamedExpr("id", bare("id")), NamedExpr("amount", bare("amount")))),
    Project(customers, List(NamedExpr("name", col("name", "customers")), NamedExpr("customer_id", col("customer_id", "customers"))))
  ))

  test("a union's column is each input's column at its position, whatever that input names it") {
    assert(column(Project(swapped, List(NamedExpr("x", bare("id")))), "x").values == Set(src("id"), src("name", "customers")))
    assert(column(Project(swapped, List(NamedExpr("x", bare("AMOUNT")))), "x").values == Set(src("amount"), src("customer_id", "customers")))
    assert(column(swapped, "amount").values == Set(src("amount"), src("customer_id", "customers")), "a union written as it is")
    assert(column(Project(swapped, List(NamedExpr("x", bare("name")))), "x").values == Set(src("name", "customers")),
      "a name the first input does not declare (it may declare only some of its columns) is asked of each by name")
  }

  test("an input that declares no columns is asked for the name; a union whose first input declares none is asked by name") {
    val withRead = Union(List(Project(orders, List(NamedExpr("id", bare("id")))), customers))
    assert(column(Project(withRead, List(NamedExpr("x", bare("id")))), "x").values == Set(src("id"), src("id", "customers")))
    val readFirst = Union(List(customers, Project(orders, List(NamedExpr("id", bare("id"))))))
    assert(column(Project(readFirst, List(NamedExpr("x", bare("id")))), "x").values == Set(src("id", "customers"), src("id")))
    assert(column(Project(readFirst, List(NamedExpr("x", bare("zzz")))), "x", ordersColumns).values.isEmpty, "a name no input has")
  }

  test("a union of reads alone (one read of several paths) gives a name from every read, whatever its qualifier") {
    val paths = Union(List(orders, customers))
    assert(column(Project(paths, List(NamedExpr("x", col("customer_id")))), "x").values == Set(src("customer_id"), src("customer_id", "customers")))
    assert(column(Project(paths, List(NamedExpr("x", col("email")))), "x", ordersColumns).values == Set(src("email")), "a read without the column gives none")
    assert(column(Project(Union(Nil), List(NamedExpr("x", bare("id")))), "x").values.isEmpty)
  }

  // --- a field of a struct the plan builds ----------------------------------------------------------------------------

  private val built = Project(orders, List(NamedExpr("_1", StructConstruct(List("id" -> bare("id"), "amount" -> Arithmetic("*", List(bare("amount"), Literal(2, "int"))))))))

  test("a field of a struct the plan builds is that field's value, copied; not every column of the struct") {
    val id = column(Project(built, List(NamedExpr("id", StructField(bare("_1"), "id")))), "id")
    assert(id.values == Set(src("id")) && id.lineage.derivation == DerivationKind.Direct)
    val doubled = column(Project(built, List(NamedExpr("a", StructField(bare("_1"), "AMOUNT")))), "a")
    assert(doubled.values == Set(src("amount")) && doubled.lineage.derivation == DerivationKind.Computed, "a field computed in the struct, named in another case")
    assert(one(StructField(StructConstruct(List("id" -> bare("id"))), "id")).values == Set(src("id")), "built in place")
    assert(one(StructField(Alias("s", StructConstruct(List("id" -> bare("id")))), "id")).lineage.derivation == DerivationKind.Direct, "through an alias")
  }

  test("a built struct is found through the operators that pass columns through, and one side of a join") {
    val through = Limit(Sort(Filter(built, Comparison(">", bare("id"), Literal(1, "int"))), List(SortOrder(bare("id")))), 5)
    assert(column(Project(through, List(NamedExpr("id", StructField(bare("_1"), "id")))), "id").values == Set(src("id")))
    val renamed = Project(built, List(NamedExpr("s", bare("_1"))))
    assert(column(Project(renamed, List(NamedExpr("id", StructField(bare("s"), "id")))), "id").lineage.derivation == DerivationKind.Direct, "a reference to it")
    val left = Join(built, customers, JoinType.Inner)
    assert(column(Project(left, List(NamedExpr("id", StructField(bare("_1"), "id")))), "id").values == Set(src("id")), "the left side")
    val right = Join(customers, built, JoinType.Inner)
    assert(column(Project(right, List(NamedExpr("id", StructField(bare("_1"), "id")))), "id").values == Set(src("id")), "the right side")
  }

  test("a struct the plan does not build here, or a field it lacks, is the whole struct's, computed") {
    val both = Join(built, built.copy(input = customers), JoinType.Inner)
    assert(column(Project(both, List(NamedExpr("x", StructField(bare("_1"), "id")))), "x").lineage.derivation == DerivationKind.Computed, "declared by both sides")
    val read = column(Project(orders, List(NamedExpr("x", StructField(bare("payload"), "a")))), "x")
    assert(read.values == Set(src("payload")) && read.lineage.derivation == DerivationKind.Computed, "a struct read from an input")
    val missing = column(Project(built, List(NamedExpr("x", StructField(bare("_1"), "zzz")))), "x")
    assert(missing.values == Set(src("id"), src("amount")) && missing.lineage.derivation == DerivationKind.Computed, "a field it lacks")
    assert(one(StructField(Function("named_struct_of", List(bare("id"))), "id")).lineage.derivation == DerivationKind.Computed, "a function's result")
    val agg = Aggregate(built, Nil, List(NamedExpr("_1", bare("_1"))))
    assert(column(Project(agg, List(NamedExpr("x", StructField(bare("_1"), "id")))), "x").lineage.derivation == DerivationKind.Computed, "an operator that makes its own columns")
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
