// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Dataset, DatasetType, Schema}
import com.invaract.sparkadapter.SchemaChecker.Side

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.lang.reflect.Modifier

import scala.io.Source

/** Every kind of finding has one constructor in `Violations`; this pins what each one carries, and that
  * the vocabulary, the constructors and the user-facing docs can't drift apart.
  */
class ViolationsSpec extends AnyFunSuite {

  private val in = Dataset("in", "bronze/in", None, Schema(Nil), derivedFrom = None)
  private val out = Dataset("out", "gold/out", None, Schema(Nil))

  /** `L` = location, `C` = column, `E` = expected and actual together (never one without the other). */
  private def shape(v: Violation): String = {
    assert(v.expected.isDefined == v.actual.isDefined, s"expected and actual must come as a pair: $v")
    List(v.location.map(_ => "L"), v.column.map(_ => "C"), v.expected.map(_ => "E")).flatten.mkString
  }

  // kind -> (the violation a constructor builds, the shape it must have)
  private val sample: List[(Violation, String)] = List(
    Violations.missingInput(in, nudgeTowardDerivedFrom = false) -> "L",
    Violations.undeclaredInput("bronze/x") -> "L",
    Violations.undeclaredInputNotSourceOf("bronze/in", in, out) -> "L",
    Violations.missingOutput(out) -> "L",
    Violations.outputLocationMismatch(List("gold/a"), "gold/b") -> "LE",
    Violations.outputFormatMismatch("parquet", "csv", "gold/out") -> "LE",
    Violations.outputSaveModeMismatch("overwrite", "append", "gold/out") -> "LE",
    Violations.missingCatalogRegistration(Side.Input, "bronze/in") -> "L",
    Violations.missingCatalogRegistration(Side.Output, "gold/out") -> "L",
    Violations.catalogMismatch(Side.Input, "bronze/in", List("table (expected 'a', actual 'b')"), "table=a", "table=b") -> "LE",
    Violations.catalogMismatch(Side.Output, "gold/out", List("table (expected 'a', actual 'b')"), "table=a", "table=b") -> "LE",
    Violations.missingField(Side.Input, "bronze/in", "id", "integer") -> "LC",
    Violations.missingField(Side.Output, "gold/out", "id", "integer") -> "LC",
    Violations.fieldTypeMismatch(Side.Input, "bronze/in", "id", "integer", "string") -> "LCE",
    Violations.fieldTypeMismatch(Side.Output, "gold/out", "id", "integer", "string") -> "LCE",
    Violations.fieldNullabilityMismatch(Side.Input, "bronze/in", "id") -> "LCE",
    Violations.fieldNullabilityMismatch(Side.Output, "gold/out", "id") -> "LCE",
    Violations.undeclaredColumn(Side.Input, "bronze/in", "x") -> "LC",
    Violations.undeclaredColumn(Side.Output, "gold/out", "x") -> "LC",
    Violations.mergeConditionRule(List("id"), List("id"), Set.empty) -> "E",
    Violations.unconditionalDeleteRule() -> "",
    Violations.disallowedUpdateColumnRule(List("a"), List("b"), List("a", "b")) -> "E",
    Violations.unverifiableDml("MERGE") -> "",
    Violations.requiredGroupByRule(List("k"), Nil) -> "E",
    Violations.crossJoinRule(1) -> "",
    Violations.requiredJoinColumnsRule(List("k"), planHasAnyJoin = true) -> "",
    Violations.requiredFilterColumnsRule(List("k"), List("k"), Set.empty) -> "E",
    Violations.dataQuality("amount", "range") -> "C",
    Violations.roleConsistency("cal", "raw.cal", DatasetType.Control, "detail") -> "L",
    Violations.invalidContract("c@1.0.0", "outputs", "empty") -> "",
    Violations.unresolvableCustomRuleType("c@1.0.0", "t", "x.Y", "boom") -> "",
    Violations.invalidInferredContract("outputs", "empty") -> "",
    Violations.orgPolicy("m", "r") -> "",
    Violations.unverifiableWrite("SomeCommand", "c@1.0.0") -> ""
  )

  test("every constructor builds the shape the table in Violations' doc promises") {
    // requiredJoinColumns carries `expected` only by design (the plan has no single 'actual' to name):
    val special = Set(ViolationType.RuleRequiredJoinColumnsViolation)
    sample.foreach { case (v, wanted) =>
      if (special.contains(v.violationType)) {
        assert(v.expected.isDefined && v.actual.isEmpty && v.location.isEmpty && v.column.isEmpty, v.toString)
      } else {
        assert(shape(v) == wanted, s"${v.violationType}: expected shape '$wanted', got '${shape(v)}': $v")
      }
      assert(v.message.nonEmpty && v.remediation.nonEmpty, v.toString)
    }
  }

  test("input and output constructors differ in violation type and wording, in lockstep with the Side") {
    val types = List(
      Violations.missingField(Side.Input, "l", "p", "t").violationType -> ViolationType.MissingInputField,
      Violations.missingField(Side.Output, "l", "p", "t").violationType -> ViolationType.MissingOutputField,
      Violations.undeclaredColumn(Side.Input, "l", "p").violationType -> ViolationType.UndeclaredInputColumn,
      Violations.undeclaredColumn(Side.Output, "l", "p").violationType -> ViolationType.UndeclaredOutputColumn,
      Violations.fieldTypeMismatch(Side.Input, "l", "p", "a", "b").violationType -> ViolationType.InputFieldTypeMismatch,
      Violations.fieldTypeMismatch(Side.Output, "l", "p", "a", "b").violationType -> ViolationType.OutputFieldTypeMismatch,
      Violations.fieldNullabilityMismatch(Side.Input, "l", "p").violationType -> ViolationType.InputFieldNullabilityMismatch,
      Violations.fieldNullabilityMismatch(Side.Output, "l", "p").violationType -> ViolationType.OutputFieldNullabilityMismatch,
      Violations.missingCatalogRegistration(Side.Input, "l").violationType -> ViolationType.MissingInputCatalogRegistration,
      Violations.missingCatalogRegistration(Side.Output, "l").violationType -> ViolationType.MissingOutputCatalogRegistration,
      Violations.catalogMismatch(Side.Input, "l", List("m"), "e", "a").violationType -> ViolationType.InputCatalogMismatch,
      Violations.catalogMismatch(Side.Output, "l", List("m"), "e", "a").violationType -> ViolationType.OutputCatalogMismatch
    )
    types.foreach { case (actual, wanted) => assert(actual == wanted) }
    assert(Violations.missingField(Side.Input, "l", "p", "t").message.contains("INPUT schema"))
    assert(Violations.missingField(Side.Output, "l", "p", "t").message.contains("OUTPUT schema"))
  }

  test("the unit-style kinds are fixed: their wording and optional pieces") {
    assert(Violations.requiredGroupByRule(List("a", "b"), Nil).message.endsWith("performs no aggregation at all"))
    assert(Violations.requiredGroupByRule(List("a", "b"), Nil).actual.contains("no aggregation"))
    val some = Violations.requiredGroupByRule(List("a", "b"), List(Set("a"), Set("b", "c")))
    assert(some.message.endsWith("no aggregation in the plan groups by all of them"))
    assert(some.actual.contains("a; b+c") || some.actual.contains("a; c+b"))
    assert(Violations.requiredJoinColumnsRule(List("k"), planHasAnyJoin = false).message.endsWith("the plan contains no join at all"))
    assert(Violations.requiredJoinColumnsRule(List("k"), planHasAnyJoin = true).message.endsWith("establishes an equality match on all of them"))
    assert(Violations.crossJoinRule(3).message.contains("3 join(s) with no condition"))
    assert(Violations.unverifiableDml("UPDATE").message.startsWith("this operation is a UPDATE"))
    assert(Violations.unverifiableWrite("Foo", "c@1").message.startsWith("'Foo' looks like it may write"))
    assert(Violations.unverifiableWrite("Foo", "c@1").message.contains("against contract 'c@1'"))
  }

  // --- the vocabulary, the constructors and the docs cannot drift apart --------------------------------

  private def allViolationTypes: Set[String] =
    ViolationType.getClass.getDeclaredMethods.toList
      .filter(m => m.getParameterCount == 0 && m.getReturnType == classOf[String] && Modifier.isPublic(m.getModifiers))
      .map(m => m.invoke(ViolationType).asInstanceOf[String])
      .toSet

  test("every ViolationType in the vocabulary is built by a constructor in Violations (a new type needs a constructor and a row in `sample`)") {
    val built = sample.map(_._1.violationType).toSet
    val missing = allViolationTypes -- built
    assert(missing.isEmpty, s"ViolationType constants with no constructor sample in ViolationsSpec: $missing")
    assert(allViolationTypes.size >= 30, "reflection should find the whole vocabulary")
  }

  test("every ViolationType is documented in docs-site's violation-types reference") {
    val docs = new File("../docs-site/src/content/docs/reference/violation-types.md")
    assert(docs.exists(), s"docs not found at ${docs.getAbsolutePath} (tests run from spark-adapter/)")
    val text = Source.fromFile(docs, "UTF-8").mkString
    val undocumented = allViolationTypes.filterNot(t => text.contains(s"`$t`"))
    assert(undocumented.isEmpty, s"ViolationTypes missing from violation-types.md: $undocumented")
  }
}
