// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

class PlanFactsSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))
  private def eq(l: Expr, r: Expr) = Comparison("=", l, r)

  // The recursive walks the verifiers used before PlanFacts, kept here as the reference.
  private def refReads(p: Plan): List[Read] = p match {
    case r: Read => List(r)
    case o       => o.children.flatMap(refReads)
  }
  private def refUnknown(p: Plan): List[UnknownPlan] = p match {
    case u: UnknownPlan => u :: u.children.flatMap(refUnknown)
    case o              => o.children.flatMap(refUnknown)
  }
  private def refAgg(p: Plan): List[Aggregate] = (p match { case a: Aggregate => List(a); case _ => Nil }) ++ p.children.flatMap(refAgg)
  private def refJoin(p: Plan): List[Join] = (p match { case j: Join => List(j); case _ => Nil }) ++ p.children.flatMap(refJoin)
  private def refFilter(p: Plan): List[Filter] = (p match { case f: Filter => List(f); case _ => Nil }) ++ p.children.flatMap(refFilter)

  private val a = Read(DatasetRef("a"))
  private val b = Read(DatasetRef("b"), alias = Some("bb"))
  private val c = Read(DatasetRef("c"))
  private val d = Read(DatasetRef("d"))

  // A tree touching every node kind the facts track, in an order where pre-order
  // differs from post-order and from breadth-first.
  private val tree: Plan =
    Write(
      DatasetRef("out"),
      Union(
        List(
          Filter(Join(a, Filter(b, eq(col("x", Some("bb")), col("y"))), JoinType.Inner, Some(eq(col("k", Some("a")), col("k", Some("bb"))))), eq(col("z"), col("w"))),
          Aggregate(UnknownPlan("boundary", "LogicalRDD", List(c)), List(col("g")), Nil),
          Project(Join(d, UnknownPlan("outer", "InMemoryRelation", List(UnknownPlan("inner", "x", List(a)))), JoinType.Cross, None), Nil)
        )
      )
    )

  test("every list matches the recursive walks it replaced - same elements, same pre-order") {
    val facts = PlanFacts.of(tree)
    assert(facts.plan eq tree)
    assert(facts.reads == refReads(tree))
    assert(facts.unknownPlans == refUnknown(tree))
    assert(facts.aggregates == refAgg(tree))
    assert(facts.joins == refJoin(tree))
    assert(facts.filters == refFilter(tree))
    // And they are not vacuous:
    assert(facts.reads.size == 5)
    assert(facts.reads.map(_.dataset.location) == List("a", "b", "c", "d", "a"))
    assert(facts.unknownPlans.map(_.sourceType) == List("LogicalRDD", "InMemoryRelation", "x"))
    assert(facts.aggregates.size == 1)
    assert(facts.joins.size == 2)
    assert(facts.filters.size == 2)
  }

  test("an UnknownPlan's own children are still searched (nested boundaries are all reported, outermost first)") {
    val facts = PlanFacts.of(UnknownPlan("outer", "O", List(UnknownPlan("inner", "I", List(a)))))
    assert(facts.unknownPlans.map(_.sourceType) == List("O", "I"))
    assert(facts.reads == List(a))
  }

  test("a plan with nothing of interest yields empty lists") {
    val facts = PlanFacts.of(Project(Sort(Limit(Window(Project(Union(Nil), Nil), Nil, Nil, Nil), 1), Nil), Nil))
    assert(facts.reads.isEmpty && facts.unknownPlans.isEmpty && facts.aggregates.isEmpty && facts.joins.isEmpty && facts.filters.isEmpty)
  }

  test("a very deep plan does not overflow the stack") {
    val deep = (1 to 50000).foldLeft[Plan](a)((plan, i) => Filter(plan, eq(col("c" + (i % 7)), col("d"))))
    val facts = PlanFacts.of(deep)
    assert(facts.reads == List(a))
    assert(facts.filters.size == 50000)
  }

  test("conditionReferences: columns referenced in Filter and Join conditions only, as a set") {
    val facts = PlanFacts.of(tree)
    val expected = Set(
      ColumnRef("x", Some("bb")), ColumnRef("y"), ColumnRef("k", Some("a")), ColumnRef("k", Some("bb")), ColumnRef("z"), ColumnRef("w")
    )
    assert(facts.conditionReferences == expected)
    // not the Aggregate's grouping column:
    assert(!facts.conditionReferences.contains(ColumnRef("g")))
    assert(PlanFacts.of(a).conditionReferences.isEmpty)
    // a join with no condition contributes nothing:
    assert(PlanFacts.of(Join(a, b, JoinType.Cross, None)).conditionReferences.isEmpty)
  }

  test("locationResolver maps an alias to its location, an unaliased scope to itself, and leaves an ambiguous scope alone") {
    val facts = PlanFacts.of(
      Union(List(Read(DatasetRef("one"), alias = Some("t")), Read(DatasetRef("two"), alias = Some("t")), Read(DatasetRef("three"), alias = Some("u")), Read(DatasetRef("four"))))
    )
    val resolve = facts.locationResolver
    assert(resolve("u") == "three")
    assert(resolve("four") == "four")
    assert(resolve("t") == "t") // two different locations share the alias: unresolved, not guessed
    assert(resolve("never-seen") == "never-seen")
    // the same location read twice under one scope is still unambiguous:
    val twice = PlanFacts.of(Union(List(Read(DatasetRef("p"), alias = Some("s")), Read(DatasetRef("p"), alias = Some("s")))))
    assert(twice.locationResolver("s") == "p")
  }

  test("PlanRuleVerifier's plan-based helpers agree with the facts-based ones") {
    assert(PlanRuleVerifier.collectConditionReferences(tree) == PlanFacts.of(tree).conditionReferences)
    val resolve = PlanRuleVerifier.locationResolver(tree)
    assert(resolve("bb") == "b")
    assert(StructuralVerifier.collectReads(tree) == refReads(tree))
    assert(StructuralVerifier.collectUnknownPlans(tree) == refUnknown(tree))
  }
}
