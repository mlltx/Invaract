// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

/** Property-based coverage for determinism and the ordering invariants
  * §2.4/§12 of docs/SEMANTIC_LINEAGE_FINGERPRINTING.md call for — a
  * generalization of `CanonicalizerSpec`'s individual hand-written cases
  * over many generated shapes, rather than a handful of hand-picked ones.
  */
class PropertyBasedSpec extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val genColumnName: Gen[String] = Gen.oneOf("a", "b", "c", "amount", "customer_id")
  private val genQualifier: Gen[Option[String]] = Gen.oneOf(None, Some("raw.orders"), Some("raw.customers"))
  private val genLiteral: Gen[Literal] = Gen.choose(-100, 100).map(i => Literal(i, "integer"))
  private val genColumnRef: Gen[ColumnRef] = for {
    name <- genColumnName
    qualifier <- genQualifier
    id <- Gen.oneOf(None, Some(1L), Some(2L))
  } yield ColumnRef(name, qualifier, id)

  private def genExpr(depth: Int): Gen[Expr] =
    if (depth <= 0) Gen.oneOf(genColumnRef.map(ColumnReference), genLiteral)
    else
      Gen.frequency(
        3 -> genColumnRef.map(ColumnReference),
        3 -> genLiteral,
        2 -> (for {
          op <- Gen.oneOf("+", "-", "*")
          l <- genExpr(depth - 1)
          r <- genExpr(depth - 1)
        } yield Arithmetic(op, List(l, r))),
        2 -> (for {
          op <- Gen.oneOf("=", "<", ">")
          l <- genExpr(depth - 1)
          r <- genExpr(depth - 1)
        } yield Comparison(op, l, r)),
        1 -> (for {
          inner <- genExpr(depth - 1)
          targetType <- Gen.oneOf("double", "decimal", "string")
        } yield Cast(inner, targetType))
      )

  test("determinism: canonicalizing the same generated expression twice always yields identical bytes") {
    forAll(genExpr(3)) { expr =>
      val scope = Map("raw.orders" -> "src0", "raw.customers" -> "src1")
      val once = Encoding.encode(Canonicalizer.canonicalizeExpr(expr, scope)).toVector
      val twice = Encoding.encode(Canonicalizer.canonicalizeExpr(expr, scope)).toVector
      assert(once == twice)
    }
  }

  test("round-trip: decode(encode(canonicalize(expr))) reconstructs the same canonical tree") {
    forAll(genExpr(3)) { expr =>
      val scope = Map("raw.orders" -> "src0", "raw.customers" -> "src1")
      val node = Canonicalizer.canonicalizeExpr(expr, scope)
      assert(Encoding.decode(Encoding.encode(node)) == node)
    }
  }

  test("ColumnRef.id never affects the canonical encoding, for arbitrary generated refs") {
    forAll(genColumnRef) { ref =>
      val withoutId = ref.copy(id = None)
      val a = Encoding.encode(Canonicalizer.canonicalizeColumnRef(ref, Map.empty)).toVector
      val b = Encoding.encode(Canonicalizer.canonicalizeColumnRef(withoutId, Map.empty)).toVector
      assert(a == b)
    }
  }

  test("Aggregate.groupBy: any permutation of the grouping keys canonicalizes identically") {
    val genDistinctKeys: Gen[List[Expr]] =
      Gen.someOf(List("a", "b", "c").map(n => ColumnReference(ColumnRef(n)): Expr)).map(_.toList).suchThat(_.size >= 2)

    forAll(genDistinctKeys) { keys =>
      val input = Read(DatasetRef("raw.orders"))
      val aggregates = List(NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount")))))
      val original = Aggregate(input, keys, aggregates)
      val shuffled = Aggregate(input, keys.reverse, aggregates)
      val a = Encoding.encode(Canonicalizer.canonicalizePlan(original, Map.empty)).toVector
      val b = Encoding.encode(Canonicalizer.canonicalizePlan(shuffled, Map.empty)).toVector
      assert(a == b)
    }
  }

  test("Sort.order: reversing distinct keys changes the canonical encoding (order is meaningful, unlike groupBy)") {
    val genDistinctKeys: Gen[List[SortOrder]] =
      Gen.someOf(List("a", "b", "c").map(n => SortOrder(ColumnReference(ColumnRef(n))))).map(_.toList).suchThat(_.size >= 2)

    forAll(genDistinctKeys) { keys =>
      val input = Read(DatasetRef("raw.orders"))
      val original = Sort(input, keys)
      val reversed = Sort(input, keys.reverse)
      val a = Encoding.encode(Canonicalizer.canonicalizePlan(original, Map.empty)).toVector
      val b = Encoding.encode(Canonicalizer.canonicalizePlan(reversed, Map.empty)).toVector
      assert(a != b)
    }
  }
}
