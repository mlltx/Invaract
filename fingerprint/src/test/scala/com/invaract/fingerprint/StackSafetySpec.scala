// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** A plain-recursive version of `Canonicalizer` stack-overflowed on a
  * forked default-stack JVM at a depth of roughly 700-1700 nodes -
  * measured directly, not assumed - well within reach of a real
  * generated pipeline (hundreds of chained `.withColumn()` calls is an
  * ordinary, expected translation shape per docs/TRANSFORMATION_IR.md's
  * own "Derivation classification" section, not a pathological input).
  * `Canonicalizer`'s internals are trampolined via
  * `scala.util.control.TailCalls` specifically to fix this; these tests
  * are the regression check for that fix, at a depth two orders of
  * magnitude past where the untrampolined version broke.
  */
class StackSafetySpec extends AnyFunSuite with Matchers {

  private val Depth = 50000

  private def deepWithColumnChain(depth: Int): Plan = {
    var plan: Plan = Read(DatasetRef("raw.orders"))
    var i = 0
    while (i < depth) {
      plan = Project(plan, List(NamedExpr("value", ColumnReference(ColumnRef("value")))))
      i += 1
    }
    plan
  }

  private def deeplyNestedArithmetic(depth: Int): Expr = {
    var expr: Expr = ColumnReference(ColumnRef("amount", Some("raw.orders")))
    var i = 0
    while (i < depth) {
      expr = Arithmetic("+", List(expr, Literal(1, "integer")))
      i += 1
    }
    expr
  }

  private def deeplyNestedCast(depth: Int): Expr = {
    var expr: Expr = ColumnReference(ColumnRef("amount", Some("raw.orders")))
    var i = 0
    while (i < depth) {
      expr = Cast(expr, "double")
      i += 1
    }
    expr
  }

  private def deepPassthroughChain(depth: Int): Plan = {
    val bottom = Project(
      Read(DatasetRef("raw.orders")),
      List(NamedExpr("value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(2, "integer")))))
    )
    var plan: Plan = bottom
    var i = 0
    while (i < depth) {
      plan = Project(plan, List(NamedExpr("value", ColumnReference(ColumnRef("value")))))
      i += 1
    }
    plan
  }

  test("canonicalizeExpr does not stack-overflow on a deeply nested arithmetic expression") {
    noException should be thrownBy Canonicalizer.canonicalizeExpr(deeplyNestedArithmetic(Depth), Map.empty)
  }

  test("canonicalizeExpr does not stack-overflow on a deeply nested cast chain") {
    noException should be thrownBy Canonicalizer.canonicalizeExpr(deeplyNestedCast(Depth), Map.empty)
  }

  test("canonicalizePlan does not stack-overflow on a long chain of nested Projects") {
    noException should be thrownBy Canonicalizer.canonicalizePlan(deepWithColumnChain(Depth), Map.empty)
  }

  test("resolveExprDeep/resolvedOutputs does not stack-overflow on a long passthrough Project chain, and still resolves through it") {
    // The realistic trigger this fix targets: a real Invaract-translated
    // plan with hundreds/thousands of chained .withColumn() calls, where
    // the value actually being computed lives at the very bottom and every
    // intervening Project is a bare passthrough (exactly the shape
    // Canonicalizer's own "Deep expression resolution" doc describes).
    val write = Write(DatasetRef("gold.out"), deepPassthroughChain(Depth))
    val resolved = Canonicalizer.resolvedOutputs(write)
    assert(resolved("value").isInstanceOf[Arithmetic])
  }

  test("buildScopeInfo does not stack-overflow on a long plan chain") {
    noException should be thrownBy Canonicalizer.buildScopeInfo(deepWithColumnChain(Depth))
  }

  test("TransformationFingerprinter.fingerprint does not stack-overflow end to end on a deep plan") {
    val write = Write(DatasetRef("gold.out"), deepPassthroughChain(Depth))
    noException should be thrownBy TransformationFingerprinter.fingerprint(write)
  }
}
