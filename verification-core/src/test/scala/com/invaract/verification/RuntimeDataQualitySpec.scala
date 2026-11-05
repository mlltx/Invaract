// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{ContractParser, Dataset}
import com.invaract.contract.InterpretedFieldConstraint.{Equals, FieldRange, Length, OneOf, Range}

import org.scalatest.funsuite.AnyFunSuite

class RuntimeDataQualitySpec extends AnyFunSuite {

  private def output(fields: String): Dataset =
    ContractParser.parse(
      s"""id: rdq
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: out/report
         |    schema:
         |      fields:
         |$fields
         |""".stripMargin
    ).outputs.head

  private def range(gte: Option[BigDecimal] = None, gt: Option[BigDecimal] = None, lte: Option[BigDecimal] = None, lt: Option[BigDecimal] = None) =
    DataCheck("v", Range(gte, gt, lte, lt))

  private def fails(check: DataCheck, value: Any): Boolean = check.fails(Map("v" -> value))

  // --- checks ----------------------------------------------------------------------------------

  test("checks: every interpretable constraint of every top-level field, in field order then declaration order") {
    val checks = RuntimeDataQuality.checks(output(
      """        - name: total
        |          type: long
        |          constraints:
        |            - type: range
        |              gte: 0
        |            - type: oneOf
        |              values: [1, 2]
        |        - name: plain
        |          type: long
        |        - name: code
        |          type: string
        |          constraints:
        |            - type: length
        |              exact: 3
        |            - type: myOwnCheck
        |              anything: 1""".stripMargin))
    assert(checks.map(c => c.field -> c.describe) == List("total" -> "range gte 0", "total" -> "oneOf [1, 2]", "code" -> "length exact 3"))
  }

  test("checks: a constraint on a field nested in a struct is not among them, and no constraints give none") {
    val nested = output(
      """        - name: address
        |          type: struct
        |          properties:
        |            - name: zip
        |              type: string
        |              constraints:
        |                - type: length
        |                  exact: 5""".stripMargin)
    assert(RuntimeDataQuality.checks(nested).isEmpty)
    assert(RuntimeDataQuality.checks(output("        - name: id\n          type: long")).isEmpty)
  }

  // --- what each check means -------------------------------------------------------------------

  test("a null or missing value never fails any check") {
    val all = List(
      DataCheck("v", Equals(1, "integer")), DataCheck("v", OneOf(Set("a"), "string")), range(gte = Some(0)),
      DataCheck("v", Length(Some(1), None, None)), DataCheck("v", FieldRange(Some("w"), None, None, None))
    )
    all.foreach { c =>
      assert(!c.fails(Map("v" -> null, "w" -> 1)), c)
      assert(!c.fails(Map("w" -> 1)), c)
    }
  }

  test("equals and oneOf compare numbers by value whatever their type, anything else by equality") {
    val one = DataCheck("v", Equals(1, "integer"))
    assert(!fails(one, 1) && !fails(one, 1L) && !fails(one, 1.0) && !fails(one, BigDecimal(1)) && !fails(one, new java.math.BigDecimal("1.00")))
    assert(fails(one, 2) && fails(one, "1") && fails(one, 0.5))
    val text = DataCheck("v", Equals("EU", "string"))
    assert(!fails(text, "EU") && fails(text, "eu"))
    val flag = DataCheck("v", Equals(true, "boolean"))
    assert(!fails(flag, true) && fails(flag, false))
    val some = DataCheck("v", OneOf(Set(1, 3), "integer"))
    assert(!fails(some, 1L) && !fails(some, 3) && fails(some, 2) && fails(some, "1"))
  }

  test("range: each bound holds at and inside its edge, and only there") {
    assert(!fails(range(gte = Some(0)), 0) && !fails(range(gte = Some(0)), 1) && fails(range(gte = Some(0)), -1))
    assert(fails(range(gt = Some(0)), 0) && !fails(range(gt = Some(0)), 1) && fails(range(gt = Some(0)), -1))
    assert(!fails(range(lte = Some(10)), 10) && !fails(range(lte = Some(10)), 9) && fails(range(lte = Some(10)), 11))
    assert(fails(range(lt = Some(10)), 10) && !fails(range(lt = Some(10)), 9) && fails(range(lt = Some(10)), 11))
    val both = range(gte = Some(0), lt = Some(10))
    assert(!fails(both, 0) && !fails(both, 9.5) && fails(both, -0.5) && fails(both, 10))
    assert(!fails(range(gte = Some(BigDecimal("0.5"))), 0.5f) && fails(range(gte = Some(BigDecimal("0.5"))), 0.25))
  }

  test("range: a value that is not a number fails it") {
    assert(fails(range(gte = Some(0)), "5"))
    assert(fails(range(gte = Some(0)), true))
  }

  test("length: counts characters, not bytes, and each bound holds at its edge") {
    val exact = DataCheck("v", Length(Some(3), None, None))
    assert(!fails(exact, "abc") && fails(exact, "ab") && fails(exact, "abcd"))
    assert(!fails(exact, "éü😀"), "three characters, more bytes and UTF-16 units")
    val min = DataCheck("v", Length(None, Some(2), None))
    assert(!fails(min, "ab") && !fails(min, "abc") && fails(min, "a"))
    val max = DataCheck("v", Length(None, None, Some(2)))
    assert(!fails(max, "ab") && !fails(max, "a") && fails(max, "abc"))
    assert(fails(exact, 12) && !fails(exact, 123), "a non-string value is measured as its text")
  }

  test("fieldRange: compares with the other field of the same row; a null or missing other field passes") {
    val atLeastStart = DataCheck("end", FieldRange(Some("start"), None, None, None))
    assert(!atLeastStart.fails(Map("end" -> 5, "start" -> 5)))
    assert(!atLeastStart.fails(Map("end" -> 6, "start" -> 5L)))
    assert(atLeastStart.fails(Map("end" -> 4, "start" -> 5)))
    assert(!atLeastStart.fails(Map("end" -> 4, "start" -> null)))
    assert(!atLeastStart.fails(Map("end" -> 4)))
    val after = DataCheck("end", FieldRange(None, Some("start"), None, None))
    assert(after.fails(Map("end" -> 5, "start" -> 5)) && !after.fails(Map("end" -> 6, "start" -> 5)))
    val atMost = DataCheck("price", FieldRange(None, None, Some("list"), None))
    assert(!atMost.fails(Map("price" -> 5, "list" -> 5)) && atMost.fails(Map("price" -> 6, "list" -> 5)))
    val below = DataCheck("price", FieldRange(None, None, None, Some("list")))
    assert(below.fails(Map("price" -> 5, "list" -> 5)) && !below.fails(Map("price" -> 4, "list" -> 5)))
    val between = DataCheck("v", FieldRange(Some("lo"), None, Some("hi"), None))
    assert(!between.fails(Map("v" -> 3, "lo" -> 1, "hi" -> 3)) && between.fails(Map("v" -> 4, "lo" -> 1, "hi" -> 3)))
    assert(!between.fails(Map("v" -> 4, "lo" -> 1, "hi" -> null)), "one null bound is enough to pass")
    assert(atLeastStart.fails(Map("end" -> "x", "start" -> 5)), "a value that is not a number fails it")
  }

  // --- how a check is described and sent -------------------------------------------------------

  test("describe spells each constraint as the contract does") {
    assert(DataCheck("v", Equals("EU", "string")).describe == "equals 'EU'")
    assert(DataCheck("v", Equals(7, "integer")).describe == "equals 7")
    assert(DataCheck("v", OneOf(Set("b", "a"), "string")).describe == "oneOf ['a', 'b']")
    assert(range(gte = Some(0), gt = None, lte = None, lt = Some(10)).describe == "range gte 0, lt 10")
    assert(range(gt = Some(1), lte = Some(2)).describe == "range gt 1, lte 2")
    assert(DataCheck("v", Length(None, Some(1), Some(5))).describe == "length min 1, max 5")
    assert(DataCheck("v", Length(Some(3), None, None)).describe == "length exact 3")
    assert(DataCheck("v", FieldRange(Some("a"), None, None, Some("b"))).describe == "fieldRange gte a, lt b")
    assert(DataCheck("v", FieldRange(None, Some("a"), Some("b"), None)).describe == "fieldRange gt a, lte b")
  }

  test("toMap gives the field, the contract's constraint type and its properties, and nothing else") {
    assert(DataCheck("v", Equals("EU", "string")).toMap == Map("field" -> "v", "type" -> "equals", "value" -> "EU"))
    assert(DataCheck("v", OneOf(Set(3, 1), "integer")).toMap == Map("field" -> "v", "type" -> "oneOf", "values" -> List(1, 3)))
    assert(range(gte = Some(0), lt = Some(10)).toMap == Map("field" -> "v", "type" -> "range", "gte" -> BigDecimal(0), "lt" -> BigDecimal(10)))
    assert(range(gt = Some(1), lte = Some(2)).toMap == Map("field" -> "v", "type" -> "range", "gt" -> BigDecimal(1), "lte" -> BigDecimal(2)))
    assert(DataCheck("v", Length(Some(3), None, None)).toMap == Map("field" -> "v", "type" -> "length", "exact" -> 3))
    assert(DataCheck("v", Length(None, Some(1), Some(5))).toMap == Map("field" -> "v", "type" -> "length", "min" -> 1, "max" -> 5))
    assert(DataCheck("v", FieldRange(Some("a"), None, None, Some("b"))).toMap == Map("field" -> "v", "type" -> "fieldRange", "gte" -> "a", "lt" -> "b"))
    assert(DataCheck("v", FieldRange(None, Some("a"), Some("b"), None)).toMap == Map("field" -> "v", "type" -> "fieldRange", "gt" -> "a", "lte" -> "b"))
  }

  // --- evaluate and violations -----------------------------------------------------------------

  test("evaluate: one result per check, in order, counting failing rows and keeping up to five distinct example values") {
    val nonNegative = range(gte = Some(0))
    val small = range(lt = Some(100))
    val rows = List(-1, -2, -1, -3, -4, -5, -6, 5, 7).map(v => Map[String, Any]("v" -> v))
    val List(first, second) = RuntimeDataQuality.evaluate(List(nonNegative, small), rows)
    assert(first == DataCheckResult(nonNegative, 7, List("-1", "-2", "-3", "-4", "-5")))
    assert(second == DataCheckResult(small, 0, Nil))
    assert(RuntimeDataQuality.evaluate(List(nonNegative), Nil) == List(DataCheckResult(nonNegative, 0, Nil)))
    assert(RuntimeDataQuality.evaluate(Nil, rows).isEmpty)
  }

  test("violations: one per check with a failing row, saying how many and showing the examples") {
    val nonNegative = range(gte = Some(0))
    val small = range(lt = Some(100))
    val violations = RuntimeDataQuality.violations(
      List(DataCheckResult(nonNegative, 2, List("-1", "-5")), DataCheckResult(small, 0, Nil), DataCheckResult(small, 1, Nil)),
      Some("out/report")
    )
    assert(violations.map(_.violationType) == List.fill(2)(ViolationType.RuntimeDataQualityViolation))
    val List(first, second) = violations
    assert(first.column.contains("v") && first.location.contains("out/report"))
    assert(first.expected.contains("range gte 0"))
    assert(first.actual.contains("2 row(s), for example -1, -5"))
    assert(first.message == "output field 'v' must meet its declared constraint (range gte 0), but this write breaks it in 2 row(s), for example -1, -5")
    assert(second.message.endsWith("breaks it in 1 row(s)"))
    assert(second.actual.contains("1 row(s)"))
    assert(RuntimeDataQuality.violations(Nil, None).isEmpty)
  }
}
