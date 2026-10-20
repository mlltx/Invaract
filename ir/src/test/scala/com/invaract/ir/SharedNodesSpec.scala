// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

import scala.util.control.TailCalls._

/** The rules `Plan`'s doc sets for a plan that shares nodes, held for the walks in this module: each node object is
  * worked out once (so a chain of `k` branches that rejoin, `2^k` paths, costs `k` steps), and what is worked out
  * is the same as for the plan with the shared node copied. The same blow-up arises in a tree when a column is
  * defined from the same column twice, repeatedly; that is covered too.
  */
class SharedNodesSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))
  private val orders = Read(DatasetRef("raw.orders"))
  private def base(read: Plan) = Project(read, List(NamedExpr("v", col("amount")), NamedExpr("k", col("key"))))

  // 40 times over: two steps over the same node, merged again (Union) or joined (Join)
  private def rejoining(steps: Int, join: Boolean): Plan =
    (1 to steps).foldLeft[Plan](base(orders)) { (below, i) =>
      val left = Filter(below, Comparison(">", col("v"), Literal(i, "int")))
      val right = Project(below, List(NamedExpr("v", col("v")), NamedExpr("k", col("k"))))
      if (join) Project(Join(left, right, JoinType.Inner), List(NamedExpr("v", col("v")), NamedExpr("k", col("k")))) else Union(List(left, right))
    }

  // 40 times over: v = v + v, a tree in which each step refers to the one below twice
  private def selfReferring(steps: Int): Plan =
    (1 to steps).foldLeft[Plan](base(orders))((below, _) => Project(below, List(NamedExpr("v", Arithmetic("+", List(col("v"), col("v")))))))

  // --- IdentityMemo -------------------------------------------------------------------------------------

  test("IdentityMemo works a value out once per object, and keeps two equal objects apart") {
    val memo = new IdentityMemo[Plan, Int]
    var computed = 0
    val (a, b) = (Read(DatasetRef("t")), Read(DatasetRef("t")))
    assert(a == b && !(a eq b))
    assert(memo.get(a).isEmpty)
    assert(memo.getOrElseUpdate(a, { computed += 1; 1 }) == 1)
    assert(memo.getOrElseUpdate(a, { computed += 1; 2 }) == 1)
    assert(memo.get(a).contains(1) && memo.get(b).isEmpty)
    assert(memo.getOrElseUpdate(b, { computed += 1; 3 }) == 3)
    assert(computed == 2)
  }

  test("IdentityMemo's trampolined form remembers a result once it completes") {
    val memo = new IdentityMemo[Plan, Int]
    var computed = 0
    val key = Read(DatasetRef("t"))
    def work = { computed += 1; done(7) }
    assert(memo.getOrElseUpdateT(key)(work).result == 7)
    assert(memo.getOrElseUpdateT(key)(work).result == 7)
    assert(computed == 1 && memo.get(key).contains(7))
  }

  // --- Lineage ----------------------------------------------------------------------------------------------

  test("Lineage: a shared node traces exactly as the same plan with that node copied") {
    val shared = Filter(base(orders), Comparison(">", col("v"), Literal(0, "int")))
    def copy = Filter(base(Read(DatasetRef("raw.orders"))), Comparison(">", col("v"), Literal(0, "int")))
    for (join <- Seq(false, true)) {
      def over(l: Plan, r: Plan) = if (join) Join(l, r, JoinType.Inner) else Union(List(l, r))
      assert(Lineage.trace(Write(DatasetRef("out"), over(shared, shared))) == Lineage.trace(Write(DatasetRef("out"), over(copy, copy))))
    }
  }

  test("Lineage: 40 rejoining steps (2^40 paths) trace at once, back to the read") {
    for (join <- Seq(false, true)) {
      val lineage = Lineage.trace(Write(DatasetRef("out"), rejoining(40, join))).map(l => l.output.name -> l).toMap
      assert(lineage("v").sources == Set(ColumnRef("amount", Some("raw.orders"))))
      assert(lineage("v").derivation == DerivationKind.Direct)
      assert(lineage("k").sources == Set(ColumnRef("key", Some("raw.orders"))))
    }
  }

  test("Lineage: a column defined from itself twice, 40 times in a row, traces at once") {
    val List(v) = Lineage.trace(Write(DatasetRef("out"), selfReferring(40)))
    assert(v.sources == Set(ColumnRef("amount", Some("raw.orders"))))
    assert(v.derivation == DerivationKind.Computed)
  }

  // --- PropertyAnalysis -------------------------------------------------------------------------------------

  private val notNullAmount = Map(ColumnRef("amount", Some("raw.orders")) -> ColumnPropertyState.Unknown.copy(notNull = NullabilityFact.Proven))

  test("PropertyAnalysis: a shared node analyzes exactly as the same plan with that node copied") {
    val shared = Filter(base(orders), Comparison(">", col("v"), Literal(0, "int")))
    def copy = Filter(base(Read(DatasetRef("raw.orders"))), Comparison(">", col("v"), Literal(0, "int")))
    val asGraph = PropertyAnalysis.analyze(Write(DatasetRef("out"), Union(List(shared, shared))), notNullAmount)
    val asTree = PropertyAnalysis.analyze(Write(DatasetRef("out"), Union(List(copy, copy))), notNullAmount)
    assert(asGraph == asTree)
    assert(asGraph.find(_.output.name == "v").get.state.notNull == NullabilityFact.Proven)
  }

  test("PropertyAnalysis: 40 rejoining steps and 40 self-referring steps analyze at once, keeping what the read guarantees") {
    for (plan <- Seq(rejoining(40, join = false), rejoining(40, join = true), selfReferring(40))) {
      val v = PropertyAnalysis.analyze(Write(DatasetRef("out"), plan), notNullAmount).find(_.output.name == "v").get
      assert(v.state.notNull == NullabilityFact.Proven, plan.getClass)
    }
    val (expr, input) = PropertyAnalysis.definingExpr(selfReferring(40), "v").get
    assert(PropertyAnalysis.analyzeExpr(expr, input, notNullAmount).notNull == NullabilityFact.Proven)
  }
}
