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
  *
  * `genExpr` covers every `Expr` node kind (including `Conditional`,
  * `Function`, `UDF`, `AggregateCall`, and `UnknownExpression` — not just
  * the arithmetic/comparison subset an earlier version of this file
  * generated) and `genPlan` covers every `Plan` node kind, so the
  * determinism/round-trip properties below actually exercise the whole
  * canonicalisation surface, not a narrow slice of it.
  */
class PropertyBasedSpec extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val genColumnName: Gen[String] = Gen.oneOf("a", "b", "c", "amount", "customer_id")
  private val genQualifier: Gen[Option[String]] = Gen.oneOf(None, Some("raw.orders"), Some("raw.customers"))
  private val genLiteral: Gen[Literal] = Gen.oneOf(
    Gen.choose(-100, 100).map(i => Literal(i, "integer")),
    Gen.choose(-100.0, 100.0).map(d => Literal(d, "double")),
    Gen.oneOf("ACTIVE", "INACTIVE", "x", "y").map(s => Literal(s, "string")),
    Gen.oneOf(true, false).map(b => Literal(b, "boolean"))
  )
  private val genColumnRef: Gen[ColumnRef] = for {
    name <- genColumnName
    qualifier <- genQualifier
    id <- Gen.oneOf(None, Some(1L), Some(2L))
  } yield ColumnRef(name, qualifier, id)

  /** Every `Expr` node kind, bounded by `depth`. */
  private def genExpr(depth: Int): Gen[Expr] =
    if (depth <= 0) Gen.oneOf(genColumnRef.map(ColumnReference), genLiteral)
    else
      Gen.frequency(
        3 -> genColumnRef.map(ColumnReference),
        3 -> genLiteral,
        2 -> (for {
          op <- Gen.oneOf("+", "-", "*", "/")
          l <- genExpr(depth - 1)
          r <- genExpr(depth - 1)
        } yield Arithmetic(op, List(l, r))),
        2 -> (for {
          op <- Gen.oneOf("=", "<", ">", "<=", ">=")
          l <- genExpr(depth - 1)
          r <- genExpr(depth - 1)
        } yield Comparison(op, l, r)),
        1 -> (for {
          op <- Gen.oneOf("AND", "OR")
          l <- genExpr(depth - 1)
          r <- genExpr(depth - 1)
        } yield BooleanExpr(op, List(l, r))),
        1 -> (for {
          inner <- genExpr(depth - 1)
          targetType <- Gen.oneOf("double", "decimal", "string")
        } yield Cast(inner, targetType)),
        1 -> (for {
          name <- Gen.identifier
          inner <- genExpr(depth - 1)
        } yield Alias(name, inner)),
        1 -> (for {
          cond <- genExpr(depth - 1)
          thenValue <- genExpr(depth - 1)
          elseValue <- genExpr(depth - 1)
        } yield Conditional(List((cond, thenValue)), Some(elseValue))),
        1 -> (for {
          name <- Gen.oneOf("upper", "lower", "coalesce", "length")
          args <- Gen.listOfN(2, genExpr(depth - 1))
        } yield Function(name, args)),
        1 -> (for {
          name <- Gen.option(Gen.identifier)
          args <- Gen.listOfN(2, genExpr(depth - 1))
        } yield UDF(name, args)),
        1 -> (for {
          fn <- Gen.oneOf("SUM", "AVG", "COUNT", "MIN", "MAX")
          arg <- genExpr(depth - 1)
          distinct <- Gen.oneOf(true, false)
        } yield AggregateCall(fn, arg, distinct)),
        1 -> (for {
          sourceType <- Gen.oneOf("SomeUnknownExpr", "OtherKind")
          children <- Gen.listOfN(1, genExpr(depth - 1))
        } yield UnknownExpression("generated", sourceType, children))
      )

  private val genDatasetLocation: Gen[String] = Gen.oneOf("raw.orders", "raw.customers")

  /** Every `Plan` node kind, bounded by `depth`. Terminates at `Read` once
    * `depth` is exhausted, mirroring `genExpr`'s own base case.
    */
  private def genPlan(depth: Int): Gen[Plan] =
    if (depth <= 0) genDatasetLocation.map(loc => Read(DatasetRef(loc)))
    else
      Gen.frequency(
        4 -> genDatasetLocation.map(loc => Read(DatasetRef(loc))),
        3 -> (for {
          input <- genPlan(depth - 1)
          name <- genColumnName
          expr <- genExpr(2)
        } yield Project(input, List(NamedExpr(name, expr)))),
        2 -> (for {
          input <- genPlan(depth - 1)
          cond <- genExpr(2)
        } yield Filter(input, cond)),
        2 -> (for {
          left <- genPlan(depth - 1)
          right <- genPlan(depth - 1)
          joinType <- Gen.oneOf(JoinType.Inner, JoinType.LeftOuter, JoinType.RightOuter, JoinType.FullOuter, JoinType.Cross)
        } yield Join(left, right, joinType, None)),
        1 -> (for {
          input <- genPlan(depth - 1)
          groupKey <- genColumnName
          fn <- Gen.oneOf("SUM", "COUNT")
          aggName <- genColumnName
        } yield Aggregate(
          input,
          List(ColumnReference(ColumnRef(groupKey))),
          List(NamedExpr(aggName, AggregateCall(fn, ColumnReference(ColumnRef("amount")))))
        )),
        1 -> (for {
          left <- genPlan(depth - 1)
          right <- genPlan(depth - 1)
        } yield Union(List(left, right))),
        1 -> (for {
          input <- genPlan(depth - 1)
          col <- genColumnName
          asc <- Gen.oneOf(true, false)
        } yield Sort(input, List(SortOrder(ColumnReference(ColumnRef(col)), ascending = asc)))),
        1 -> (for {
          input <- genPlan(depth - 1)
          n <- Gen.choose(1, 1000)
        } yield Limit(input, n, 0)),
        1 -> (for {
          input <- genPlan(depth - 1)
          sourceType <- Gen.oneOf("SomeUnknownPlan", "OtherPlanKind")
        } yield UnknownPlan("generated", sourceType, List(input)))
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

  test("determinism: canonicalizing the same generated plan twice always yields identical bytes") {
    forAll(genPlan(3)) { plan =>
      val scope = Canonicalizer.buildScopeInfo(plan).substitution
      val once = Encoding.encode(Canonicalizer.canonicalizePlan(plan, scope)).toVector
      val twice = Encoding.encode(Canonicalizer.canonicalizePlan(plan, scope)).toVector
      assert(once == twice)
    }
  }

  test("round-trip: decode(encode(canonicalize(plan))) reconstructs the same canonical tree") {
    forAll(genPlan(3)) { plan =>
      val scope = Canonicalizer.buildScopeInfo(plan).substitution
      val node = Canonicalizer.canonicalizePlan(plan, scope)
      assert(Encoding.decode(Encoding.encode(node)) == node)
    }
  }

  test("determinism: fingerprinting the same generated plan (wrapped in a Write) twice always yields an identical result") {
    forAll(genPlan(3)) { plan =>
      val write = Write(DatasetRef("gold.out"), plan)
      assert(TransformationFingerprinter.fingerprint(write) == TransformationFingerprinter.fingerprint(write))
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

  test("injectivity in practice: changing a generated expression's operator changes its canonical encoding") {
    // A generalization of CanonicalizerSpec's hand-written per-field cases:
    // for a generated binary Arithmetic/Comparison node, replacing only
    // the operator string (leaving both operands untouched) must always
    // produce a different encoding - the kind of check that would catch a
    // forgotten field (an operator silently not wired into the encoder).
    val genBinaryArithmetic: Gen[(Arithmetic, Arithmetic)] = for {
      ops <- Gen.pick(2, List("+", "-", "*", "/"))
      l <- genExpr(2)
      r <- genExpr(2)
    } yield (Arithmetic(ops(0), List(l, r)), Arithmetic(ops(1), List(l, r)))

    forAll(genBinaryArithmetic) { case (a, b) =>
      val scope = Map("raw.orders" -> "src0", "raw.customers" -> "src1")
      assert(Encoding.encode(Canonicalizer.canonicalizeExpr(a, scope)).toVector != Encoding.encode(Canonicalizer.canonicalizeExpr(b, scope)).toVector)
    }
  }
}
