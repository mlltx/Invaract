// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractRule, Dataset, DatasetType, Schema}
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

  /** Which fields a violation fills, always in the order `L`ocation, `C`olumn, `E`xpected, `A`ctual, `R`ule. */
  private def shape(v: Violation): String =
    List(
      v.location.map(_ => "L"),
      v.column.map(_ => "C"),
      v.expected.map(_ => "E"),
      v.actual.map(_ => "A"),
      v.rule.map(_ => "R")
    ).flatten.mkString

  // kind -> (the violation a constructor builds, the shape it must have)
  private val sample: List[(Violation, String)] = List(
    Violations.missingInput(in, nudgeTowardDerivedFrom = false) -> "L",
    Violations.undeclaredInput("bronze/x") -> "L",
    Violations.undeclaredInputNotSourceOf("bronze/in", in, out) -> "L",
    Violations.missingOutput(out) -> "L",
    Violations.outputLocationMismatch(List("gold/a"), "gold/b") -> "LEA",
    Violations.outputFormatMismatch("parquet", "csv", "gold/out") -> "LEA",
    Violations.outputSaveModeMismatch("overwrite", "append", "gold/out") -> "LEA",
    Violations.missingCatalogRegistration(Side.Input, "bronze/in") -> "L",
    Violations.missingCatalogRegistration(Side.Output, "gold/out") -> "L",
    Violations.catalogMismatch(Side.Input, "bronze/in", List("table (expected 'a', actual 'b')"), "table=a", "table=b") -> "LEA",
    Violations.catalogMismatch(Side.Output, "gold/out", List("table (expected 'a', actual 'b')"), "table=a", "table=b") -> "LEA",
    Violations.missingField(Side.Input, "bronze/in", "id", "integer") -> "LCE",
    Violations.missingField(Side.Output, "gold/out", "id", "integer") -> "LCE",
    Violations.fieldTypeMismatch(Side.Input, "bronze/in", "id", "integer", "string") -> "LCEA",
    Violations.fieldTypeMismatch(Side.Output, "gold/out", "id", "integer", "string") -> "LCEA",
    Violations.fieldNullabilityMismatch(Side.Input, "bronze/in", "id") -> "LCEA",
    Violations.fieldNullabilityMismatch(Side.Output, "gold/out", "id") -> "LCEA",
    Violations.undeclaredColumn(Side.Input, "bronze/in", "x", "string") -> "LCA",
    Violations.undeclaredColumn(Side.Output, "gold/out", "x", "string") -> "LCA",
    Violations.mergeConditionRule(List("id"), List("id"), Set.empty) -> "EA",
    Violations.unconditionalDeleteRule() -> "",
    Violations.disallowedUpdateColumnRule(List("a"), List("b"), List("a", "b")) -> "EA",
    Violations.unverifiableDml("MERGE", Some("gold/out")) -> "LA",
    Violations.requiredGroupByRule(List("k"), Nil) -> "EA",
    Violations.crossJoinRule(1) -> "",
    Violations.requiredJoinColumnsRule(List("k"), planHasAnyJoin = true) -> "E",
    Violations.requiredFilterColumnsRule(List("k"), List("k"), Set.empty) -> "EA",
    Violations.dataQuality("amount", "range", Some("gold/out")) -> "LCE",
    Violations.roleConsistency("cal", "raw.cal", DatasetType.Control, "detail") -> "L",
    Violations.invalidContract("c@1.0.0", "outputs", "empty") -> "",
    Violations.unresolvableCustomRuleType("c@1.0.0", "t", "x.Y", "boom") -> "",
    Violations.invalidInferredContract("outputs", "empty") -> "",
    Violations.orgPolicy("m", "r", Some("gold/out"), "p1") -> "LR",
    Violations.unverifiableWrite("SomeCommand", "c@1.0.0") -> "A"
  )

  test("every constructor builds the shape the table in Violations' doc promises") {
    sample.foreach { case (v, wanted) =>
      assert(shape(v) == wanted, s"${v.violationType}: expected shape '$wanted', got '${shape(v)}': $v")
      assert(v.message.nonEmpty && v.remediation.nonEmpty, v.toString)
    }
  }

  test("input and output constructors differ in violation type and wording, in lockstep with the Side") {
    val types = List(
      Violations.missingField(Side.Input, "l", "p", "t").violationType -> ViolationType.MissingInputField,
      Violations.missingField(Side.Output, "l", "p", "t").violationType -> ViolationType.MissingOutputField,
      Violations.undeclaredColumn(Side.Input, "l", "p", "t").violationType -> ViolationType.UndeclaredInputColumn,
      Violations.undeclaredColumn(Side.Output, "l", "p", "t").violationType -> ViolationType.UndeclaredOutputColumn,
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
    assert(Violations.unverifiableDml("UPDATE", None).message.startsWith("this operation is a UPDATE"))
    assert(Violations.unverifiableWrite("Foo", "c@1").message.startsWith("'Foo' looks like it may write"))
    assert(Violations.unverifiableWrite("Foo", "c@1").message.contains("against contract 'c@1'"))
  }

  test("missingInput only points at derivedFrom when asked to") {
    val plain = Violations.missingInput(in, nudgeTowardDerivedFrom = false)
    val nudged = Violations.missingInput(in, nudgeTowardDerivedFrom = true)
    assert(!plain.remediation.contains("derivedFrom"))
    assert(plain.remediation.endsWith("if it is no longer needed."))
    assert(nudged.remediation.startsWith(plain.remediation))
    assert(nudged.remediation.contains("'derivedFrom'"))
    assert(nudged.remediation.contains("'in' feeds only some"))
    assert(nudged.message == plain.message)
  }

  test("outputLocationMismatch words one declared location and several differently, and joins them for `expected`") {
    val one = Violations.outputLocationMismatch(List("gold/a"), "gold/b")
    assert(one.message == "contract declares output location 'gold/a' but the plan writes to 'gold/b'")
    assert(one.remediation.startsWith("Write to 'gold/a' instead"))
    assert(one.remediation.contains("update the contract's declared output location to 'gold/b'"))
    assert(one.expected.contains("gold/a"))
    assert(one.actual.contains("gold/b"))
    assert(one.location.contains("gold/b"))

    val many = Violations.outputLocationMismatch(List("gold/a", "gold/c"), "gold/b")
    assert(many.message.contains("does not match any of the contract's 2 declared output locations (gold/a, gold/c)"))
    assert(many.remediation.startsWith("Write to one of the contract's declared output locations (gold/a, gold/c)"))
    assert(many.remediation.contains("add 'gold/b' as a new declared output"))
    assert(many.expected.contains("gold/a, gold/c"))
    assert(!many.message.contains("contract declares output location"))
  }

  test("undeclaredInputNotSourceOf names the output's derivedFrom list, or says it lists none") {
    def msg(derivedFrom: Option[List[String]]) =
      Violations.undeclaredInputNotSourceOf("bronze/in", in, out.copy(derivedFrom = derivedFrom)).message
    assert(msg(Some(List("a", "b"))).endsWith("(its derivedFrom lists 'a', 'b')"))
    assert(msg(Some(List("a"))).endsWith("(its derivedFrom lists 'a')"))
    assert(msg(None).endsWith("(its derivedFrom lists no inputs)"))
    assert(msg(Some(Nil)).endsWith("(its derivedFrom lists no inputs)"))
    val v = Violations.undeclaredInputNotSourceOf("bronze/in", in, out)
    assert(v.message.startsWith("plan reads 'bronze/in' (input 'in'), which this contract declares but not as a source of output 'out'"))
    assert(v.remediation.startsWith("Add 'in' to output 'out''s derivedFrom"))
  }

  test("a rule's findings are stamped with the write they are about and the rule that raised them") {
    val raw = Violations.requiredGroupByRule(List("k"), Nil)
    val stamped = RuleVerifier.stamp(raw, ContractRule("required_group_by", Map.empty), Some("gold/out"))
    assert(stamped.location.contains("gold/out"))
    assert(stamped.rule.contains("required_group_by"))
    assert(stamped.copy(location = None, rule = None) == raw)
    // no write known: location stays unset, rule is still recorded
    val noWrite = RuleVerifier.stamp(raw, ContractRule("required_group_by", Map.empty), None)
    assert(noWrite.location.isEmpty && noWrite.rule.contains("required_group_by"))
    // a verifier that already filled either field keeps its own value
    val own = raw.copy(location = Some("custom/loc"), rule = Some("custom-rule"))
    val kept = RuleVerifier.stamp(own, ContractRule("required_group_by", Map.empty), Some("gold/out"))
    assert(kept.location.contains("custom/loc") && kept.rule.contains("custom-rule"))
  }

  test("new fields carry the values the checker has: missing field's declared type, stray column's actual type, unverifiable kinds") {
    assert(Violations.missingField(Side.Output, "gold/out", "id", "integer").expected.contains("integer"))
    assert(Violations.undeclaredColumn(Side.Input, "bronze/in", "x", "array<int>").actual.contains("array<int>"))
    val dml = Violations.unverifiableDml("UPDATE", Some("gold/out"))
    assert(dml.location.contains("gold/out") && dml.actual.contains("UPDATE"))
    assert(Violations.unverifiableDml("UPDATE", None).location.isEmpty)
    assert(Violations.unverifiableWrite("SomeCommand", "c@1").actual.contains("SomeCommand"))
    val dq = Violations.dataQuality("amount", "range >= 0", Some("gold/out"))
    assert(dq.expected.contains("range >= 0") && dq.location.contains("gold/out") && dq.column.contains("amount"))
    val policy = Violations.orgPolicy("m", "r", None, "pol-1")
    assert(policy.rule.contains("pol-1") && policy.location.isEmpty)
  }

  test("rule is part of toMap only when set") {
    assert(!Violations.missingOutput(out).toMap.contains("rule"))
    assert(Violations.orgPolicy("m", "r", None, "pol-1").toMap("rule") == "pol-1")
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
    // Walk up from the working directory: tests run from spark-adapter/, but Stryker4s runs them
    // from a copy of it under target/, one or two levels deeper.
    val rel = "docs-site/src/content/docs/reference/violation-types.md"
    val docs = Iterator
      .iterate(new File(".").getAbsoluteFile.getParentFile)(_.getParentFile)
      .takeWhile(_ != null)
      .map(new File(_, rel))
      .find(_.exists())
      .getOrElse(fail(s"$rel not found in any parent of ${new File(".").getAbsolutePath}"))
    val text = Source.fromFile(docs, "UTF-8").mkString
    val undocumented = allViolationTypes.filterNot(t => text.contains(s"`$t`"))
    assert(undocumented.isEmpty, s"ViolationTypes missing from violation-types.md: $undocumented")
  }
}
