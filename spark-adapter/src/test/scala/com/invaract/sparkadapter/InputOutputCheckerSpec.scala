// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{ContractParser, Dataset, Schema}
import com.invaract.ir.{CatalogIdentity, DatasetRef, Read, UnknownPlan, Write}

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** The small functions `InputChecker` and `OutputChecker` are made of, tested directly
  * (`StructuralVerifierSpec` covers the same behaviour end to end through `verify`).
  */
class InputOutputCheckerSpec extends AnyFunSuite {

  private def ds(name: String, location: String, format: Option[String] = None, saveMode: Option[String] = None) =
    Dataset(name, location, format, Schema(Nil), saveMode = saveMode)

  // --- OutputChecker --------------------------------------------------------------------------

  test("expectedOutputFor: a single declared output is the target whatever the location; several are chosen by location") {
    val only = ds("only", "gold/only")
    assert(OutputChecker.expectedOutputFor(List(only), "somewhere/else").contains(only))
    val a = ds("a", "gold/a")
    val b = ds("b", "gold/b")
    assert(OutputChecker.expectedOutputFor(List(a, b), "s3://x/gold/b").contains(b))
    assert(OutputChecker.expectedOutputFor(List(a, b), "somewhere/else").isEmpty)
    assert(OutputChecker.expectedOutputFor(Nil, "anything").isEmpty)
  }

  test("matchOutput: first declared output whose location matches; none otherwise") {
    val first = ds("first", "gold/x")
    val second = ds("second", "gold/x")
    assert(OutputChecker.matchOutput(List(first, second), "file:/w/gold/x").contains(first))
    assert(OutputChecker.matchOutput(List(first), "gold/y").isEmpty)
    assert(OutputChecker.matchOutput(Nil, "gold/x").isEmpty)
  }

  test("missingOutputs: one MISSING_OUTPUT per declared output, in declaration order, naming it and its location") {
    val vs = OutputChecker.missingOutputs(List(ds("a", "gold/a"), ds("b", "gold/b")))
    assert(vs.map(_.violationType) == List(ViolationType.MissingOutput, ViolationType.MissingOutput))
    assert(vs.map(_.location) == List(Some("gold/a"), Some("gold/b")))
    assert(vs.head.message == "the plan does not produce a write; expected output 'a' (gold/a)")
    assert(vs.head.remediation == "Add a write to 'gold/a' to the transformation.")
    assert(OutputChecker.missingOutputs(Nil).isEmpty)
  }

  test("locationFinding: nothing when a declared output matches; otherwise single- vs multi-output wording") {
    assert(OutputChecker.locationFinding(List(ds("a", "gold/a")), "file:/w/gold/a").isEmpty)
    val single = OutputChecker.locationFinding(List(ds("a", "gold/a")), "gold/typo").head
    assert(single.message == "contract declares output location 'gold/a' but the plan writes to 'gold/typo'")
    assert(single.remediation.startsWith("Write to 'gold/a' instead"))
    assert(single.expected.contains("gold/a") && single.actual.contains("gold/typo"))
    val multi = OutputChecker.locationFinding(List(ds("a", "gold/a"), ds("b", "gold/b")), "gold/typo").head
    assert(multi.message.startsWith("the plan writes to 'gold/typo', which does not match any of the contract's 2 declared output locations (gold/a, gold/b)"))
    assert(multi.remediation.startsWith("Write to one of the contract's declared output locations (gold/a, gold/b) instead"))
    assert(multi.expected.contains("gold/a, gold/b"))
  }

  test("formatFinding and saveModeFinding: compared case-insensitively, and only when both sides are known") {
    val out = ds("o", "gold/o", format = Some("parquet"), saveMode = Some("overwrite"))
    assert(OutputChecker.formatFinding(out, Some("PARQUET")).isEmpty)
    assert(OutputChecker.formatFinding(out, None).isEmpty)
    assert(OutputChecker.formatFinding(ds("o", "gold/o"), Some("csv")).isEmpty)
    val f = OutputChecker.formatFinding(out, Some("csv")).head
    assert(f.violationType == ViolationType.OutputFormatMismatch && f.expected.contains("parquet") && f.actual.contains("csv"))
    assert(f.message == "contract declares output format 'parquet' but the plan writes in format 'csv'")

    assert(OutputChecker.saveModeFinding(out, Some("OVERWRITE")).isEmpty)
    assert(OutputChecker.saveModeFinding(out, None).isEmpty)
    assert(OutputChecker.saveModeFinding(ds("o", "gold/o"), Some("append")).isEmpty)
    val m = OutputChecker.saveModeFinding(out, Some("append")).head
    assert(m.violationType == ViolationType.OutputSaveModeMismatch && m.expected.contains("overwrite") && m.actual.contains("append"))
    assert(m.message == "contract declares output save mode 'overwrite' but the plan writes with save mode 'append'")
  }

  test("catalogFinding: only when the output declares a catalog block") {
    val noCatalog = ds("o", "gold/o")
    val write = Write(DatasetRef("gold/o"), Read(DatasetRef("src")), catalog = None)
    assert(OutputChecker.catalogFinding(noCatalog, write).isEmpty)
    val contract = ContractParser.parse(
      """id: c
        |version: "1.0.0"
        |outputs:
        |  - name: o
        |    location: gold/o
        |    catalog: {required: true, technology: iceberg}
        |    schema: {fields: [{name: id, type: integer}]}
        |""".stripMargin
    )
    val vs = OutputChecker.catalogFinding(contract.outputs.head, write)
    assert(vs.map(_.violationType) == List(ViolationType.MissingOutputCatalogRegistration))
    assert(vs.head.location.contains("gold/o"))
  }

  test("check: with a Write the findings come in report order; without one, MISSING_OUTPUT") {
    val contract = ContractParser.parse(
      """id: c
        |version: "1.0.0"
        |outputs:
        |  - name: o
        |    location: gold/o
        |    format: parquet
        |    saveMode: overwrite
        |    catalog: {required: true, technology: iceberg}
        |    schema: {fields: [{name: id, type: integer, required: true}]}
        |""".stripMargin
    )
    val write = Write(DatasetRef("gold/typo"), Read(DatasetRef("src")), format = Some("csv"), saveMode = Some("append"))
    val vs = OutputChecker.check(contract, write, new StructType().add("x", StringType), VerificationOptions(), caseSensitive = false)
    assert(
      vs.map(_.violationType) == List(
        ViolationType.OutputLocationMismatch,
        ViolationType.OutputFormatMismatch,
        ViolationType.OutputSaveModeMismatch,
        ViolationType.MissingOutputCatalogRegistration,
        ViolationType.MissingOutputField
      )
    )
    assert(OutputChecker.check(contract, Read(DatasetRef("src")), new StructType(), VerificationOptions(), caseSensitive = false).map(_.violationType) == List(ViolationType.MissingOutput))
  }

  // --- InputChecker ---------------------------------------------------------------------------

  private def inputs(spec: String) = ContractParser.parse(
    s"""id: c
       |version: "1.0.0"
       |inputs:
       |$spec
       |outputs:
       |  - name: o
       |    location: gold/o
       |    schema: {fields: [{name: id, type: integer}]}
       |""".stripMargin
  )

  private val twoInputs = inputs(
    """  - name: a
      |    location: bronze/a
      |    schema: {fields: [{name: id, type: integer}]}
      |  - name: b
      |    location: bronze/b
      |    schema: {fields: [{name: id, type: integer}]}""".stripMargin
  )

  test("notRead: the declared inputs no read location matches, in declaration order") {
    val declared = InputChecker.Declared(twoInputs.inputs)
    assert(InputChecker.notRead(declared, List("s3://x/bronze/a")).map(_.name) == List("b"))
    assert(InputChecker.notRead(declared, Nil).map(_.name) == List("a", "b"))
    assert(InputChecker.notRead(declared, List("bronze/a", "bronze/b")).isEmpty)
    assert(InputChecker.notRead(InputChecker.Declared(Nil), List("bronze/a")).isEmpty)
  }

  test("unverifiableEvidenceFor: only boundary node types count, each listed once in first-seen order") {
    assert(InputChecker.unverifiableEvidenceFor(Nil).isEmpty)
    assert(InputChecker.unverifiableEvidenceFor(List(UnknownPlan("x", "SomethingElse"))).isEmpty)
    val boundary = CheckpointRegistry.BoundarySourceTypes.head
    assert(InputChecker.unverifiableEvidenceFor(List(UnknownPlan("a", boundary), UnknownPlan("b", "Other"), UnknownPlan("c", boundary))).contains(List(boundary)))
  }

  test("classifyUnread: no evidence => MISSING_INPUT; evidence => unverifiable instead; the derivedFrom nudge only for a derivedFrom-less output of a multi-output contract") {
    val unread = twoInputs.inputs
    val (missing, unverifiable) = InputChecker.classifyUnread(unread, Nil, None, declaredOutputCount = 1)
    assert(missing.map(_.violationType) == List(ViolationType.MissingInput, ViolationType.MissingInput))
    assert(unverifiable.isEmpty)
    assert(missing.head.message == "declared input 'a' (bronze/a) was not read by this plan")
    assert(missing.head.remediation.startsWith("Add a read of 'bronze/a' to the transformation"))
    assert(!missing.head.remediation.contains("derivedFrom"))

    val boundary = CheckpointRegistry.BoundarySourceTypes.head
    val (m2, u2) = InputChecker.classifyUnread(unread, List(UnknownPlan("x", boundary)), None, 1)
    assert(m2.isEmpty)
    assert(u2 == List(UnverifiableInput("a", "bronze/a", List(boundary)), UnverifiableInput("b", "bronze/b", List(boundary))))

    val noDerived = Some(ds("o", "gold/o")) // derivedFrom = None
    assert(InputChecker.classifyUnread(unread, Nil, noDerived, declaredOutputCount = 2)._1.head.remediation.contains("derivedFrom"))
    assert(!InputChecker.classifyUnread(unread, Nil, noDerived, declaredOutputCount = 1)._1.head.remediation.contains("derivedFrom"))
    assert(!InputChecker.classifyUnread(unread, Nil, None, declaredOutputCount = 2)._1.head.remediation.contains("derivedFrom"))
    val withDerived = Some(ds("o", "gold/o").copy(derivedFrom = Some(List("a"))))
    assert(!InputChecker.classifyUnread(unread, Nil, withDerived, declaredOutputCount = 2)._1.head.remediation.contains("derivedFrom"))
  }

  test("undeclaredReads: wholly undeclared vs declared-but-not-this-output's-source wording; nothing when all reads are scoped") {
    val all = InputChecker.Declared(twoInputs.inputs)
    val scopedToA = InputChecker.Declared(List(twoInputs.inputs.head))
    val output = Some(ds("o", "gold/o").copy(derivedFrom = Some(List("a"))))
    assert(InputChecker.undeclaredReads(List("bronze/a"), all, all, None).isEmpty)

    val wholly = InputChecker.undeclaredReads(List("bronze/zzz"), all, scopedToA, output).head
    assert(wholly.violationType == ViolationType.UndeclaredInput)
    assert(wholly.message == "plan reads 'bronze/zzz' which is not declared as a contract input")
    assert(wholly.remediation == "Declare 'bronze/zzz' as an input in the contract, or remove this read from the transformation.")

    val outside = InputChecker.undeclaredReads(List("bronze/b"), all, scopedToA, output).head
    assert(outside.message.startsWith("plan reads 'bronze/b' (input 'b'), which this contract declares but not as a source of output 'o' (its derivedFrom lists 'a')"))
    assert(outside.remediation.startsWith("Add 'b' to output 'o''s derivedFrom"))

    // with no scoped output a declared-but-unscoped read can't be told apart: the generic wording
    assert(InputChecker.undeclaredReads(List("bronze/b"), all, scopedToA, None).head.message == "plan reads 'bronze/b' which is not declared as a contract input")
    // an empty derivedFrom lists no inputs:
    val emptyDerived = Some(ds("o", "gold/o").copy(derivedFrom = Some(Nil)))
    assert(InputChecker.undeclaredReads(List("bronze/b"), all, scopedToA, emptyDerived).head.message.endsWith("(its derivedFrom lists no inputs)"))
  }

  test("schemaFindings and catalogFindings: only inputs with a matching supplied schema / read are checked") {
    val contract = inputs(
      """  - name: a
        |    location: bronze/a
        |    catalog: {required: true, technology: iceberg}
        |    schema: {fields: [{name: id, type: integer, required: true, nullable: false}]}
        |  - name: b
        |    location: bronze/b
        |    schema: {fields: [{name: id, type: integer, required: true, nullable: false}]}""".stripMargin
    )
    val all = InputChecker.Declared(contract.inputs)
    val wrong = new StructType().add("id", StringType, nullable = false)
    assert(InputChecker.schemaFindings(all, Nil, rejectUndeclaredFields = false, caseSensitive = false).isEmpty)
    val vs = InputChecker.schemaFindings(all, List("s3://x/bronze/b" -> wrong), rejectUndeclaredFields = false, caseSensitive = false)
    assert(vs.map(_.violationType) == List(ViolationType.InputFieldTypeMismatch))
    val ok = new StructType().add("id", IntegerType, nullable = false)
    assert(InputChecker.schemaFindings(all, List("bronze/a" -> ok, "bronze/b" -> ok), rejectUndeclaredFields = false, caseSensitive = false).isEmpty)

    assert(InputChecker.catalogFindings(all, Nil).isEmpty) // not read: MISSING_INPUT's job
    val hive = Read(DatasetRef("file:/w/bronze/a"), catalog = Some(CatalogIdentity(technology = Some("hive"))))
    assert(InputChecker.catalogFindings(all, List(hive)).map(_.violationType) == List(ViolationType.InputCatalogMismatch))
    val iceberg = Read(DatasetRef("file:/w/bronze/a"), catalog = Some(CatalogIdentity(technology = Some("iceberg"))))
    assert(InputChecker.catalogFindings(all, List(iceberg)).isEmpty)
    // input b declares no catalog, so reading it with any identity is fine:
    assert(InputChecker.catalogFindings(all, List(iceberg, Read(DatasetRef("bronze/b"), catalog = None))).isEmpty)
  }
}
