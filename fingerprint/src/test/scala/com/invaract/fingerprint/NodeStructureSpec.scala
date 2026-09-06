// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite

import CanonicalNode._

/** Exact structural assertions for every node kind `Canonicalizer` and
  * `LiteralEncoding` produce — pinning each one's literal tag string,
  * field count, and field order via direct `CanonicalNode` equality,
  * rather than only comparing relative differences between two encodings
  * (as `CanonicalizerSpec`/`LiteralEncodingSpec` mostly do).
  *
  * This exists specifically because a tag string (`"Read"`, `"Join"`,
  * `"Arithmetic"`, ...) is structural identity, not display prose — unlike
  * the human-readable message/remediation text `spark-adapter`'s own
  * mutation-testing history documents as a legitimate `StringLiteral`
  * exclusion (see docs/SPARK_ADAPTER.md's "Mutation testing" section), a
  * tag being silently folded to `""` is a real correctness bug (it can
  * only fail to collide with another node kind's own encoding by chance
  * of differing field shape) that a "does this differ from that" test
  * alone will not always catch if neither side's assertion actually reads
  * the tag string itself. An exact `==` against a hand-built
  * `CTag(...)`/`CLeaf(...)` value pins it directly.
  */
class NodeStructureSpec extends AnyFunSuite {

  private val emptyScope: Map[String, String] = Map.empty
  private val x = ColumnReference(ColumnRef("x"))
  private def canonExpr(e: Expr): CanonicalNode = Canonicalizer.canonicalizeExpr(e, emptyScope)
  private def canonPlan(p: Plan): CanonicalNode = Canonicalizer.canonicalizePlan(p, emptyScope)
  private def cRef(name: String): CanonicalNode = CTag("ColumnReference", List(CTag("ColumnRef", List(stringLeaf(name), CTag("Option")))))

  // --- Expr node kinds ---

  test("ColumnReference/ColumnRef structure") {
    assert(canonExpr(ColumnReference(ColumnRef("amount"))) ==
      CTag("ColumnReference", List(CTag("ColumnRef", List(stringLeaf("amount"), CTag("Option"))))))
  }

  test("ColumnRef with a qualifier structure") {
    assert(canonExpr(ColumnReference(ColumnRef("amount", Some("raw.orders")))) ==
      CTag("ColumnReference", List(CTag("ColumnRef", List(stringLeaf("amount"), CTag("Option", List(stringLeaf("raw.orders"))))))))
  }

  test("Alias structure") {
    assert(canonExpr(Alias("renamed", x)) == CTag("Alias", List(stringLeaf("renamed"), cRef("x"))))
  }

  test("Cast structure") {
    assert(canonExpr(Cast(x, "double")) == CTag("Cast", List(cRef("x"), stringLeaf("double"))))
  }

  test("Arithmetic structure") {
    assert(canonExpr(Arithmetic("*", List(x, x))) == CTag("Arithmetic", List(stringLeaf("*"), cRef("x"), cRef("x"))))
  }

  test("Comparison structure") {
    assert(canonExpr(Comparison("=", x, x)) == CTag("Comparison", List(stringLeaf("="), cRef("x"), cRef("x"))))
  }

  test("BooleanExpr structure") {
    assert(canonExpr(BooleanExpr("AND", List(x, x))) == CTag("BooleanExpr", List(stringLeaf("AND"), cRef("x"), cRef("x"))))
  }

  test("Conditional structure, with an elseValue") {
    val cond = Conditional(List((x, x)), Some(x))
    assert(canonExpr(cond) ==
      CTag("Conditional", List(CTag("Branch", List(cRef("x"), cRef("x"))), CTag("Else", List(CTag("Option", List(cRef("x"))))))))
  }

  test("Conditional structure, with no elseValue") {
    val cond = Conditional(List((x, x)), None)
    assert(canonExpr(cond) == CTag("Conditional", List(CTag("Branch", List(cRef("x"), cRef("x"))), CTag("Else", List(CTag("Option"))))))
  }

  test("Function structure") {
    assert(canonExpr(Function("upper", List(x))) == CTag("Function", List(stringLeaf("upper"), cRef("x"))))
  }

  test("UDF structure, named with one arg") {
    assert(canonExpr(UDF(Some("f"), List(x))) ==
      CTag("UDF", List(CTag("Option", List(stringLeaf("f"))), CTag("Args", List(cRef("x"))))))
  }

  test("UDF structure, anonymous with no args") {
    assert(canonExpr(UDF(None, Nil)) == CTag("UDF", List(CTag("Option"), CTag("Args"))))
  }

  test("AggregateCall structure") {
    assert(canonExpr(AggregateCall("SUM", x, distinct = true)) ==
      CTag("AggregateCall", List(stringLeaf("SUM"), boolLeaf(true), cRef("x"))))
  }

  test("UnknownExpression structure") {
    assert(canonExpr(UnknownExpression("desc", "Kind", List(x))) ==
      CTag("UnknownExpression", List(stringLeaf("Kind"), CTag("Children", List(cRef("x"))))))
  }

  // --- Plan node kinds ---

  test("Read structure") {
    assert(canonPlan(Read(DatasetRef("raw.orders"))) == CTag("Read", List(stringLeaf("raw.orders"))))
  }

  test("Write structure, with format and saveMode") {
    val plan = Write(DatasetRef("gold.out"), Read(DatasetRef("raw.orders")), Some("parquet"), Some("append"))
    assert(canonPlan(plan) ==
      CTag(
        "Write",
        List(
          stringLeaf("gold.out"),
          CTag("Read", List(stringLeaf("raw.orders"))),
          CTag("Option", List(stringLeaf("parquet"))),
          CTag("Option", List(stringLeaf("append")))
        )
      ))
  }

  test("Write structure, with no format/saveMode") {
    val plan = Write(DatasetRef("gold.out"), Read(DatasetRef("raw.orders")))
    assert(canonPlan(plan) ==
      CTag("Write", List(stringLeaf("gold.out"), CTag("Read", List(stringLeaf("raw.orders"))), CTag("Option"), CTag("Option"))))
  }

  test("Project structure") {
    val plan = Project(Read(DatasetRef("raw.orders")), List(NamedExpr("out", x)))
    assert(canonPlan(plan) ==
      CTag("Project", List(CTag("Read", List(stringLeaf("raw.orders"))), CTag("NamedExpr", List(stringLeaf("out"), cRef("x"))))))
  }

  test("Filter structure") {
    val plan = Filter(Read(DatasetRef("raw.orders")), Comparison("=", x, x))
    assert(canonPlan(plan) ==
      CTag("Filter", List(CTag("Read", List(stringLeaf("raw.orders"))), CTag("Comparison", List(stringLeaf("="), cRef("x"), cRef("x"))))))
  }

  test("Join structure, with a condition") {
    val left = Read(DatasetRef("raw.orders"))
    val right = Read(DatasetRef("raw.customers"))
    val plan = Join(left, right, JoinType.LeftOuter, Some(x))
    assert(canonPlan(plan) ==
      CTag(
        "Join",
        List(
          CTag("Read", List(stringLeaf("raw.orders"))),
          CTag("Read", List(stringLeaf("raw.customers"))),
          stringLeaf("LeftOuter"),
          CTag("Option", List(cRef("x")))
        )
      ))
  }

  test("Join structure, with no condition") {
    val plan = Join(Read(DatasetRef("a")), Read(DatasetRef("b")), JoinType.Cross, None)
    assert(canonPlan(plan) ==
      CTag("Join", List(CTag("Read", List(stringLeaf("a"))), CTag("Read", List(stringLeaf("b"))), stringLeaf("Cross"), CTag("Option"))))
  }

  test("Aggregate structure") {
    val plan = Aggregate(Read(DatasetRef("raw.orders")), List(x), List(NamedExpr("total", AggregateCall("SUM", x))))
    assert(canonPlan(plan) ==
      CTag(
        "Aggregate",
        List(
          CTag("Read", List(stringLeaf("raw.orders"))),
          CTag("GroupBy", List(cRef("x"))),
          CTag("NamedExpr", List(stringLeaf("total"), CTag("AggregateCall", List(stringLeaf("SUM"), boolLeaf(false), cRef("x")))))
        )
      ))
  }

  test("Union structure") {
    val plan = Union(List(Read(DatasetRef("a")), Read(DatasetRef("b"))))
    assert(canonPlan(plan) == CTag("Union", List(CTag("Read", List(stringLeaf("a"))), CTag("Read", List(stringLeaf("b"))))))
  }

  test("Sort structure") {
    val plan = Sort(Read(DatasetRef("raw.orders")), List(SortOrder(x, ascending = false, nullsFirst = true)))
    assert(canonPlan(plan) ==
      CTag("Sort", List(CTag("Read", List(stringLeaf("raw.orders"))), CTag("SortOrder", List(cRef("x"), boolLeaf(false), boolLeaf(true))))))
  }

  test("Limit structure") {
    val plan = Limit(Read(DatasetRef("raw.orders")), 10, 5)
    assert(canonPlan(plan) == CTag("Limit", List(CTag("Read", List(stringLeaf("raw.orders"))), intLeaf(10), intLeaf(5))))
  }

  test("Window structure") {
    val plan = Window(Read(DatasetRef("raw.orders")), List(NamedExpr("rk", Function("RANK", Nil))), List(x), List(SortOrder(x)))
    assert(canonPlan(plan) ==
      CTag(
        "Window",
        List(
          CTag("Read", List(stringLeaf("raw.orders"))),
          CTag("PartitionBy", List(cRef("x"))),
          CTag("OrderBy", List(CTag("SortOrder", List(cRef("x"), boolLeaf(true), boolLeaf(true))))),
          CTag("NamedExpr", List(stringLeaf("rk"), CTag("Function", List(stringLeaf("RANK")))))
        )
      ))
  }

  test("UnknownPlan structure") {
    val plan = UnknownPlan("desc", "Kind", List(Read(DatasetRef("raw.orders"))))
    assert(canonPlan(plan) == CTag("UnknownPlan", List(stringLeaf("Kind"), CTag("Read", List(stringLeaf("raw.orders"))))))
  }

  // --- Lineage summary layer ---

  test("ColumnLineage structure") {
    val lineage = ColumnLineage(
      ColumnRef("total"),
      Set(ColumnRef("amount", Some("raw.orders"))),
      DerivationKind.Computed,
      Set(AggregationDetail("SUM", distinct = false))
    )
    assert(Canonicalizer.canonicalizeLineage(lineage, emptyScope) ==
      CTag(
        "ColumnLineage",
        List(
          CTag("Sources", List(CTag("ColumnRef", List(stringLeaf("amount"), CTag("Option", List(stringLeaf("raw.orders"))))))),
          stringLeaf("Computed"),
          CTag("Aggregations", List(CTag("AggregationDetail", List(stringLeaf("SUM"), boolLeaf(false)))))
        )
      ))
  }

  // --- Row mutation facts (MERGE/UPDATE/DELETE) ---

  test("RowMutation structure, with a match condition, a conditional delete, and updated columns") {
    val mutation = RowMutation(Some(Comparison("=", x, x)), DeleteScope.Conditional(Comparison(">", x, x)), List("b", "a"))
    assert(Canonicalizer.canonicalizeRowMutation(mutation, emptyScope) ==
      CTag(
        "RowMutation",
        List(
          CTag("Option", List(CTag("Comparison", List(stringLeaf("="), cRef("x"), cRef("x"))))),
          CTag("Conditional", List(CTag("Comparison", List(stringLeaf(">"), cRef("x"), cRef("x"))))),
          CTag("UpdatedColumns", List(stringLeaf("a"), stringLeaf("b")))
        )
      ))
  }

  test("RowMutation structure, with no match condition, an unconditional delete, and no updated columns") {
    val mutation = RowMutation(None, DeleteScope.Unconditional, Nil)
    assert(Canonicalizer.canonicalizeRowMutation(mutation, emptyScope) ==
      CTag("RowMutation", List(CTag("Option"), CTag("Unconditional"), CTag("UpdatedColumns"))))
  }

  test("RowMutation structure, with DeleteScope.NotApplicable (a plain UPDATE/MERGE, no delete branch)") {
    val mutation = RowMutation(Some(x), DeleteScope.NotApplicable, List("a"))
    assert(Canonicalizer.canonicalizeRowMutation(mutation, emptyScope) ==
      CTag("RowMutation", List(CTag("Option", List(cRef("x"))), CTag("NotApplicable"), CTag("UpdatedColumns", List(stringLeaf("a"))))))
  }

  test("RowMutation.updatedColumns is canonically sorted - declaration order never affects the encoding") {
    val a = RowMutation(None, DeleteScope.NotApplicable, List("z", "a", "m"))
    val b = RowMutation(None, DeleteScope.NotApplicable, List("m", "z", "a"))
    assert(Canonicalizer.canonicalizeRowMutation(a, emptyScope) == Canonicalizer.canonicalizeRowMutation(b, emptyScope))
  }

  // --- Literal value-kind tags (LiteralEncoding) ---

  private def literalValueNode(value: Any): CanonicalNode = LiteralEncoding.encode(value, "t") match {
    case CTag("Literal", List(valueNode, _)) => valueNode
    case other                               => fail(s"expected a Literal wrapper, got $other")
  }

  test("Int-valued literal encoding tags every integral runtime type as \"Int\"") {
    assert(literalValueNode(5: Int) == CTag("Int", List(stringLeaf("5"))))
    assert(literalValueNode(5L: Long) == CTag("Int", List(stringLeaf("5"))))
    assert(literalValueNode(5: Short) == CTag("Int", List(stringLeaf("5"))))
    assert(literalValueNode(5: Byte) == CTag("Int", List(stringLeaf("5"))))
    assert(literalValueNode(BigInt(5)) == CTag("Int", List(stringLeaf("5"))))
    assert(literalValueNode(java.math.BigInteger.valueOf(5L)) == CTag("Int", List(stringLeaf("5"))))
  }

  test("negative integral values keep an explicit '-' sign") {
    assert(literalValueNode(-5: Int) == CTag("Int", List(stringLeaf("-5"))))
  }

  test("Decimal-valued literal encoding structure") {
    assert(literalValueNode(BigDecimal("1.20")) == CTag("Decimal", List(stringLeaf("120"), stringLeaf("2"))))
  }

  test("Double-valued literal encoding structure") {
    assert(literalValueNode(1.0) == CTag("Double", List(stringLeaf(java.lang.Double.doubleToLongBits(1.0).toString))))
  }

  test("Float-valued literal encoding structure") {
    assert(literalValueNode(1.0f) == CTag("Float", List(stringLeaf(java.lang.Float.floatToIntBits(1.0f).toString))))
  }

  test("Boolean-valued literal encoding structure") {
    assert(literalValueNode(true) == CTag("Boolean", List(boolLeaf(true))))
  }

  test("Array[Byte] (BinaryType)-valued literal encoding structure, hashed by content via CLeaf, never Array's own toString") {
    assert(literalValueNode(Array[Byte](1, 2, 3)) == CTag("Binary", List(CLeaf(Vector[Byte](1, 2, 3)))))
  }

  test("String-valued literal encoding structure") {
    assert(literalValueNode("hi") == CTag("String", List(stringLeaf("hi"))))
  }

  test("null-valued literal encoding structure") {
    assert(literalValueNode(null) == CTag("Null"))
  }

  test("unrecognized-type literal encoding structure") {
    case class Weird(x: Int) { override def toString: String = "weird!" }
    assert(literalValueNode(Weird(1)) == CTag("UnrecognizedLiteralValueType", List(stringLeaf("weird!"))))
  }
}
