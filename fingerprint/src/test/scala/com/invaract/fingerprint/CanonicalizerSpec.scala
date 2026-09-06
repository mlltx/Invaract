// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite

class CanonicalizerSpec extends AnyFunSuite {

  private def encodeExpr(expr: Expr, scope: Map[String, String] = Map.empty): Vector[Byte] =
    Encoding.encode(Canonicalizer.canonicalizeExpr(expr, scope)).toVector

  private def encodePlan(plan: Plan, scope: Map[String, String] = Map.empty): Vector[Byte] =
    Encoding.encode(Canonicalizer.canonicalizePlan(plan, scope)).toVector

  // -----------------------------------------------------------------
  // ColumnRef.id must never affect the fingerprint (§1/§2.2)
  // -----------------------------------------------------------------

  test("ColumnRef.id never affects the canonical encoding") {
    val withId = ColumnReference(ColumnRef("amount", Some("raw.orders"), id = Some(101L)))
    val withDifferentId = ColumnReference(ColumnRef("amount", Some("raw.orders"), id = Some(202L)))
    val withNoId = ColumnReference(ColumnRef("amount", Some("raw.orders"), id = None))
    assert(encodeExpr(withId) == encodeExpr(withDifferentId))
    assert(encodeExpr(withId) == encodeExpr(withNoId))
  }

  // -----------------------------------------------------------------
  // Alias-relabelling invariance / stable source identity (§2.3)
  // -----------------------------------------------------------------

  test("renaming a self-join's aliases consistently does not change the canonical encoding") {
    def selfJoinCondition(leftAlias: String, rightAlias: String): Plan = {
      val left = Read(DatasetRef("raw.orders"), Some(leftAlias))
      val right = Read(DatasetRef("raw.orders"), Some(rightAlias))
      Join(
        left,
        right,
        JoinType.Inner,
        Some(
          Comparison(
            "=",
            ColumnReference(ColumnRef("customer_id", Some(leftAlias))),
            ColumnReference(ColumnRef("customer_id", Some(rightAlias)))
          )
        )
      )
    }
    val variant1 = selfJoinCondition("o1", "o2")
    val variant2 = selfJoinCondition("a", "b")

    val scope1 = Canonicalizer.buildScopeInfo(variant1).substitution
    val scope2 = Canonicalizer.buildScopeInfo(variant2).substitution

    assert(encodePlan(variant1, scope1) == encodePlan(variant2, scope2))
  }

  test("swapping which physical Read occupies the two self-join positions changes the encoding (accepted conservatism)") {
    val o1 = Read(DatasetRef("raw.orders"), Some("o1"))
    val o2 = Read(DatasetRef("raw.orders"), Some("o2"))
    val condition = Comparison("=", ColumnReference(ColumnRef("customer_id", Some("o1"))), ColumnReference(ColumnRef("customer_id", Some("o2"))))
    val original = Join(o1, o2, JoinType.Inner, Some(condition))
    val swapped = Join(o2, o1, JoinType.Inner, Some(condition))

    val scopeOriginal = Canonicalizer.buildScopeInfo(original).substitution
    val scopeSwapped = Canonicalizer.buildScopeInfo(swapped).substitution

    assert(encodePlan(original, scopeOriginal) != encodePlan(swapped, scopeSwapped))
  }

  test("a join on two distinct datasets never collapses their qualifiers (orders.id vs customers.id)") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val joined = Join(orders, customers, JoinType.Inner, Some(Comparison(
      "=",
      ColumnReference(ColumnRef("id", Some("raw.orders"))),
      ColumnReference(ColumnRef("id", Some("raw.customers")))
    )))
    val scope = Canonicalizer.buildScopeInfo(joined).substitution
    assert(scope("raw.orders") != scope("raw.customers"), "two different physical datasets must get distinct positional ids")

    // And the encoded ColumnRefs really do differ, not just the scope table.
    val leftRef = Canonicalizer.canonicalizeColumnRef(ColumnRef("id", Some("raw.orders")), scope)
    val rightRef = Canonicalizer.canonicalizeColumnRef(ColumnRef("id", Some("raw.customers")), scope)
    assert(leftRef != rightRef)
  }

  test("an unresolvable qualifier (matching no known Read scope) is left untouched, not dropped") {
    val ref = ColumnRef("amount", Some("unknown_scope"))
    val node = Canonicalizer.canonicalizeColumnRef(ref, Map("raw.orders" -> "src0"))
    assert(node == CTag("ColumnRef", List(CanonicalNode.stringLeaf("amount"), CTag("Option", List(CanonicalNode.stringLeaf("unknown_scope"))))))
  }

  // -----------------------------------------------------------------
  // Field ordering (§2.4)
  // -----------------------------------------------------------------

  test("Aggregate.groupBy order does not affect the encoding (set-like partitioning keys)") {
    val input = Read(DatasetRef("raw.orders"))
    val keyA = ColumnReference(ColumnRef("customer_id"))
    val keyB = ColumnReference(ColumnRef("region"))
    val agg1 = Aggregate(input, List(keyA, keyB), List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount"))))))
    val agg2 = Aggregate(input, List(keyB, keyA), List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount"))))))
    assert(encodePlan(agg1) == encodePlan(agg2))
  }

  test("Window.partitionBy order does not affect the encoding") {
    val input = Read(DatasetRef("raw.orders"))
    val keyA = ColumnReference(ColumnRef("customer_id"))
    val keyB = ColumnReference(ColumnRef("region"))
    val w1 = Window(input, List(NamedExpr("rk", Function("RANK", Nil))), partitionBy = List(keyA, keyB))
    val w2 = Window(input, List(NamedExpr("rk", Function("RANK", Nil))), partitionBy = List(keyB, keyA))
    assert(encodePlan(w1) == encodePlan(w2))
  }

  test("Sort.order order DOES affect the encoding (row ordering is observable)") {
    val input = Read(DatasetRef("raw.orders"))
    val byA = SortOrder(ColumnReference(ColumnRef("a")))
    val byB = SortOrder(ColumnReference(ColumnRef("b")))
    val s1 = Sort(input, List(byA, byB))
    val s2 = Sort(input, List(byB, byA))
    assert(encodePlan(s1) != encodePlan(s2))
  }

  test("Arithmetic operand order DOES affect the encoding (no commutative normalisation)") {
    val a = ColumnReference(ColumnRef("a"))
    val b = ColumnReference(ColumnRef("b"))
    assert(encodeExpr(Arithmetic("+", List(a, b))) != encodeExpr(Arithmetic("+", List(b, a))))
  }

  test("BooleanExpr operand order DOES affect the encoding (no commutative normalisation)") {
    val a = Comparison("=", ColumnReference(ColumnRef("a")), Literal(1, "integer"))
    val b = Comparison("=", ColumnReference(ColumnRef("b")), Literal(2, "integer"))
    assert(encodeExpr(BooleanExpr("AND", List(a, b))) != encodeExpr(BooleanExpr("AND", List(b, a))))
  }

  test("Join left/right order DOES affect the encoding, even though it is not sorted like groupBy") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val j1 = Join(orders, customers, JoinType.Inner)
    val j2 = Join(customers, orders, JoinType.Inner)
    assert(encodePlan(j1) != encodePlan(j2))
  }

  test("Conditional branch order DOES affect the encoding (first match wins)") {
    val branch1 = (Comparison(">", ColumnReference(ColumnRef("x")), Literal(0, "integer")), Literal("pos", "string"))
    val branch2 = (Comparison("<", ColumnReference(ColumnRef("x")), Literal(0, "integer")), Literal("neg", "string"))
    val c1 = Conditional(List(branch1, branch2), Some(Literal("zero", "string")))
    val c2 = Conditional(List(branch2, branch1), Some(Literal("zero", "string")))
    assert(encodeExpr(c1) != encodeExpr(c2))
  }

  // -----------------------------------------------------------------
  // UDF strategy (§7)
  // -----------------------------------------------------------------

  test("UDF.engineType does not affect the encoding") {
    val u1 = UDF(Some("score_risk"), List(ColumnReference(ColumnRef("amount"))), engineType = Some("PythonUDF"))
    val u2 = UDF(Some("score_risk"), List(ColumnReference(ColumnRef("amount"))), engineType = Some("ScalaUDF"))
    assert(encodeExpr(u1) == encodeExpr(u2))
  }

  test("UDF.name changing DOES affect the encoding") {
    val u1 = UDF(Some("score_risk"), List(ColumnReference(ColumnRef("amount"))))
    val u2 = UDF(Some("score_risk_v2"), List(ColumnReference(ColumnRef("amount"))))
    assert(encodeExpr(u1) != encodeExpr(u2))
  }

  test("UDF.args changing DOES affect the encoding") {
    val u1 = UDF(Some("f"), List(ColumnReference(ColumnRef("amount"))))
    val u2 = UDF(Some("f"), List(ColumnReference(ColumnRef("quantity"))))
    assert(encodeExpr(u1) != encodeExpr(u2))
  }

  test("UDF arity changing DOES affect the encoding") {
    val u1 = UDF(Some("f"), List(ColumnReference(ColumnRef("amount"))))
    val u2 = UDF(Some("f"), List(ColumnReference(ColumnRef("amount")), ColumnReference(ColumnRef("country"))))
    assert(encodeExpr(u1) != encodeExpr(u2))
  }

  test("two UDFs with identical name/args/engineType (standing in for a silently-edited implementation) encode identically") {
    val u1 = UDF(Some("score_risk"), List(ColumnReference(ColumnRef("amount"))), Some("PythonUDF"))
    val u2 = UDF(Some("score_risk"), List(ColumnReference(ColumnRef("amount"))), Some("PythonUDF"))
    assert(encodeExpr(u1) == encodeExpr(u2), "documented limitation: an implementation change invisible to the model must not be invented")
  }

  test("UDF.name Some vs None encodes differently") {
    val named = UDF(Some("f"), Nil)
    val anonymous = UDF(None, Nil)
    assert(encodeExpr(named) != encodeExpr(anonymous))
  }

  // -----------------------------------------------------------------
  // Unknown-node strategy (§8)
  // -----------------------------------------------------------------

  test("UnknownExpression.description does not affect the encoding") {
    val u1 = UnknownExpression("some construct we cannot represent", "PythonUDFWrapper", List(ColumnReference(ColumnRef("amount"))))
    val u2 = UnknownExpression("a totally differently worded description", "PythonUDFWrapper", List(ColumnReference(ColumnRef("amount"))))
    assert(encodeExpr(u1) == encodeExpr(u2))
  }

  test("UnknownExpression.sourceType DOES affect the encoding") {
    val u1 = UnknownExpression("x", "KindA", Nil)
    val u2 = UnknownExpression("x", "KindB", Nil)
    assert(encodeExpr(u1) != encodeExpr(u2))
  }

  test("UnknownExpression's children remain visible in the encoding, even nested") {
    val inner = UnknownExpression("x", "Kind", List(ColumnReference(ColumnRef("amount"))))
    val outer = Cast(inner, "double")
    val withDifferentChild = Cast(UnknownExpression("x", "Kind", List(ColumnReference(ColumnRef("quantity")))), "double")
    assert(encodeExpr(outer) != encodeExpr(withDifferentChild), "a change nested inside an unknown node's children must still be visible")
  }

  test("UnknownPlan.description does not affect the encoding, sourceType does") {
    val p1 = UnknownPlan("desc one", "SomeCommand", Nil)
    val p2 = UnknownPlan("desc two, totally different wording", "SomeCommand", Nil)
    val p3 = UnknownPlan("desc one", "OtherCommand", Nil)
    assert(encodePlan(p1) == encodePlan(p2))
    assert(encodePlan(p1) != encodePlan(p3))
  }

  // -----------------------------------------------------------------
  // Literal/type participation (§5, "Types")
  // -----------------------------------------------------------------

  test("Cast.targetType participates in the encoding") {
    val e1 = Cast(ColumnReference(ColumnRef("amount")), "decimal(18,2)")
    val e2 = Cast(ColumnReference(ColumnRef("amount")), "double")
    assert(encodeExpr(e1) != encodeExpr(e2))
  }

  // -----------------------------------------------------------------
  // Deep expression resolution through nested/passthrough Projects
  // -----------------------------------------------------------------

  test("resolveExprDeep finds a real computation buried under a passthrough outer Project") {
    val orders = Read(DatasetRef("raw.orders"))
    def plan(rate: BigDecimal): Plan = {
      val inner = Project(
        orders,
        List(
          NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          NamedExpr("value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(rate, "decimal"))))
        )
      )
      // The outer Project re-declares "value" as a bare passthrough - the
      // realistic shape a chain of .withColumn() calls produces (see
      // docs/TRANSFORMATION_IR.md's own "Derivation classification"
      // section).
      Project(inner, List(NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id"))), NamedExpr("value", ColumnReference(ColumnRef("value")))))
    }
    val before = Canonicalizer.resolvedOutputs(plan(BigDecimal("1.20")))("value")
    val after = Canonicalizer.resolvedOutputs(plan(BigDecimal("1.25")))("value")
    assert(encodeExpr(before) != encodeExpr(after), "the real computation, several Projects down, must still be reachable")
    assert(!before.isInstanceOf[ColumnReference], "resolution must not stop at the outer passthrough reference")
  }

  test("resolveExprDeep matches the correct name among several candidates declared by an Aggregate it passes through") {
    // A Project sitting directly on an Aggregate, referencing one of its
    // declared names by a bare (unqualified) reference - this is exactly
    // the shape that sends resolution through resolveRefDeep's own
    // Aggregate case (`aggregates.find(_.name == ref.name)`), not through
    // resolvedOutputs' own top-level Aggregate dispatch.
    val agg = Aggregate(
      Read(DatasetRef("raw.orders")),
      List(ColumnReference(ColumnRef("customer_id"))),
      List(
        NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
        NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
      )
    )
    val outer = Project(agg, List(NamedExpr("out", ColumnReference(ColumnRef("total")))))
    val resolved = Canonicalizer.resolvedOutputs(outer)("out")
    // Must resolve to the "total" declaration specifically, not "customer_id" -
    // if name-matching used `!=` instead of `==` here, this would either
    // resolve to the wrong candidate or fail to resolve at all.
    assert(resolved == AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
  }

  test("resolveExprDeep matches the correct name among several candidates declared by a Window it passes through") {
    val window = Window(
      Read(DatasetRef("raw.orders")),
      List(
        NamedExpr("rk", Function("RANK", Nil)),
        NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
      )
    )
    val outer = Project(window, List(NamedExpr("out", ColumnReference(ColumnRef("total")))))
    val resolved = Canonicalizer.resolvedOutputs(outer)("out")
    assert(resolved == AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
  }

  test("an unqualified reference resolves against a Read (forall over None is vacuously true)") {
    val orders = Read(DatasetRef("raw.orders"))
    val outer = Project(orders, List(NamedExpr("out", ColumnReference(ColumnRef("amount")))))
    assert(Canonicalizer.resolvedOutputs(outer)("out") == ColumnReference(ColumnRef("amount", Some("raw.orders"))))
  }

  test("a reference qualified with a non-matching scope does not resolve against that Read") {
    val orders = Read(DatasetRef("raw.orders"))
    val mismatched = ColumnReference(ColumnRef("amount", Some("some_other_scope")))
    val outer = Project(orders, List(NamedExpr("out", mismatched)))
    // No match anywhere - resolveExprDeep falls back to the original,
    // unresolved reference rather than inventing a source.
    assert(Canonicalizer.resolvedOutputs(outer)("out") == mismatched)
  }

  test("a reference qualified with the matching scope resolves against that Read") {
    val orders = Read(DatasetRef("raw.orders"))
    val qualified = ColumnReference(ColumnRef("amount", Some("raw.orders")))
    val outer = Project(orders, List(NamedExpr("out", qualified)))
    assert(Canonicalizer.resolvedOutputs(outer)("out") == qualified)
  }

  test("resolvedOutputs leaves an unrelated output's resolved expression unaffected by a change to a sibling") {
    val orders = Read(DatasetRef("raw.orders"))
    def plan(rate: BigDecimal): Plan = Project(
      orders,
      List(
        NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
        NamedExpr("value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(rate, "decimal"))))
      )
    )
    val before = Canonicalizer.resolvedOutputs(plan(BigDecimal("1.20")))
    val after = Canonicalizer.resolvedOutputs(plan(BigDecimal("1.25")))
    assert(encodeExpr(before("customer_id")) == encodeExpr(after("customer_id")))
    assert(encodeExpr(before("value")) != encodeExpr(after("value")))
  }

  // -----------------------------------------------------------------
  // resolveRefDeep's Union and ambiguous-Join branches (previously
  // untested - documented in code comments as a narrower-than-ir.Lineage
  // limitation, but never actually exercised).
  // -----------------------------------------------------------------

  private def unionBranch(dataset: String, rate: String): Plan =
    Project(
      Read(DatasetRef(dataset)),
      List(NamedExpr("value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some(dataset))), Literal(BigDecimal(rate), "decimal")))))
    )

  test("resolveExprDeep resolves through a Union by taking the first branch that declares the name") {
    val branch1 = unionBranch("a", "1.1")
    val branch2 = unionBranch("b", "1.2")
    val outer = Project(Union(List(branch1, branch2)), List(NamedExpr("out", ColumnReference(ColumnRef("value")))))
    val resolved = Canonicalizer.resolvedOutputs(outer)("out")
    assert(resolved == Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("a"))), Literal(BigDecimal("1.1"), "decimal"))))
  }

  test("Union branch order affects which branch's expression a bare reference resolves to") {
    val branch1 = unionBranch("a", "1.1")
    val branch2 = unionBranch("b", "1.2")
    val outerOriginal = Project(Union(List(branch1, branch2)), List(NamedExpr("out", ColumnReference(ColumnRef("value")))))
    val outerSwapped = Project(Union(List(branch2, branch1)), List(NamedExpr("out", ColumnReference(ColumnRef("value")))))
    val resolvedOriginal = Canonicalizer.resolvedOutputs(outerOriginal)("out")
    val resolvedSwapped = Canonicalizer.resolvedOutputs(outerSwapped)("out")
    assert(resolvedOriginal != resolvedSwapped)
    assert(resolvedSwapped == Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("b"))), Literal(BigDecimal("1.2"), "decimal"))))
  }

  test("a Union branch whose qualifier can't match is skipped in favor of one that does") {
    // A bare Read vacuously "matches" any *unqualified* reference (it
    // declares no columns of its own - see ir.Lineage's identical Read
    // case and its own "no schema catalog" doc), so a qualified reference
    // is needed to make branch1 genuinely return None here: its qualifier
    // ("b") can't match branch1's own Read scope ("x"), so resolveRefDeep
    // correctly falls through to branch2 rather than stopping at branch1.
    val branch1 = Read(DatasetRef("x"))
    val branch2 = unionBranch("b", "1.2")
    val outer = Project(Union(List(branch1, branch2)), List(NamedExpr("out", ColumnReference(ColumnRef("value", Some("b"))))))
    val resolved = Canonicalizer.resolvedOutputs(outer)("out")
    assert(resolved == Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("b"))), Literal(BigDecimal("1.2"), "decimal"))))
  }

  test("resolveExprDeep resolves an ambiguous unqualified reference matching both Join sides by preferring the left side") {
    val left = Read(DatasetRef("raw.orders"))
    val right = Read(DatasetRef("raw.customers"))
    // An unqualified "id" - ref.qualifier.forall(_ == scope) is vacuously
    // true for None regardless of scope, so this genuinely matches both
    // sides, unlike a qualified reference (already covered by the
    // "locality across a Join" test, which only ever hits the
    // unambiguous Some/None and None/Some cases).
    val outer = Project(Join(left, right, JoinType.Inner), List(NamedExpr("out", ColumnReference(ColumnRef("id")))))
    val resolved = Canonicalizer.resolvedOutputs(outer)("out")
    assert(resolved == ColumnReference(ColumnRef("id", Some("raw.orders"))))
  }

  test("Join side order affects which side an ambiguous unqualified reference resolves to") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val outerOriginal = Project(Join(orders, customers, JoinType.Inner), List(NamedExpr("out", ColumnReference(ColumnRef("id")))))
    val outerSwapped = Project(Join(customers, orders, JoinType.Inner), List(NamedExpr("out", ColumnReference(ColumnRef("id")))))
    assert(Canonicalizer.resolvedOutputs(outerOriginal)("out") == ColumnReference(ColumnRef("id", Some("raw.orders"))))
    assert(Canonicalizer.resolvedOutputs(outerSwapped)("out") == ColumnReference(ColumnRef("id", Some("raw.customers"))))
  }
}
