// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, CsvOptions, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{DatasetRef, Read, UnknownPlan, Write}

import org.scalatest.funsuite.AnyFunSuite

/** `check.csvOptions`: a dataset's declared CSV options against the ones its reads and writes used. */
class CsvOptionsCheckSpec extends AnyFunSuite {

  private def contract(inputCsv: String = "", outputCsv: String = "", secondOutput: Boolean = false): Contract = ContractParser.parse(
    s"""id: csv
       |version: "1.0.0"
       |inputs:
       |  - name: orders
       |    location: in/orders.csv
       |    format: csv
       |$inputCsv    schema:
       |      fields:
       |        - { name: id, type: long }
       |outputs:
       |  - name: report
       |    location: out/report
       |    format: csv
       |$outputCsv    schema:
       |      fields:
       |        - { name: id, type: long }
       |${if (secondOutput) "  - { name: audit, location: out/audit, schema: { fields: [ { name: id, type: long } ] } }" else ""}
       |""".stripMargin
  )

  private val semicolonNoHeader = "    csv: { delimiter: \";\", header: false }\n"

  private val writeReport = Write(DatasetRef("out/report"), Read(DatasetRef("in/orders.csv")), Some("csv"), Some("overwrite"))
  private val reads = List(Read(DatasetRef("in/orders.csv")))

  private def check(c: Contract, used: Map[String, Map[String, String]], plan: com.invaract.ir.Plan = writeReport, r: List[Read] = reads) =
    CsvOptionsChecker.check(c, plan, r, used)

  test("an output written with the declared options passes; with others, one violation lists every option that differs") {
    val c = contract(outputCsv = semicolonNoHeader)
    assert(check(c, Map("out/report" -> Map("delimiter" -> ";", "header" -> "false", "encoding" -> "UTF-8"))).isEmpty)
    val List(v) = check(c, Map("out/report" -> Map("delimiter" -> ",", "header" -> "true")))
    assert(v.violationType == ViolationType.OutputCsvOptionsMismatch && v.location.contains("out/report"))
    assert(v.expected.contains("delimiter ';', header false") && v.actual.contains("delimiter ',', header true"))
    assert(v.message == "contract declares the output CSV options delimiter ';', header false but the write uses delimiter ',', header true")
    assert(v.remediation.startsWith("Write the file with delimiter ';', header false"))
  }

  test("an input read with other options than declared is an input violation naming the read") {
    val c = contract(inputCsv = semicolonNoHeader)
    val List(v) = check(c, Map("in/orders.csv" -> Map("delimiter" -> ";", "header" -> "true")))
    assert(v.violationType == ViolationType.InputCsvOptionsMismatch && v.location.contains("in/orders.csv"))
    assert(v.message == "contract declares the input CSV options header false but the read uses header true")
    assert(v.remediation.startsWith("Read the file with header false"))
    assert(check(c, Map("in/orders.csv" -> Map("delimiter" -> ";", "header" -> "false"))).isEmpty)
  }

  test("both sides must be known: nothing declared, nothing reported, or an option on one side only, is not compared") {
    assert(check(contract(), Map("out/report" -> Map("delimiter" -> "|"))).isEmpty)
    assert(check(contract(outputCsv = semicolonNoHeader), Map.empty).isEmpty)
    assert(check(contract(outputCsv = semicolonNoHeader), Map("elsewhere" -> Map("delimiter" -> "|"))).isEmpty)
    assert(check(contract(outputCsv = semicolonNoHeader), Map("out/report" -> Map("encoding" -> "UTF-16"))).isEmpty)
    assert(check(contract(inputCsv = semicolonNoHeader), Map("in/orders.csv" -> Map("quote" -> "'"))).isEmpty)
  }

  test("a write that is no write, or lands on no declared output, and a read of no declared input, are not checked") {
    val c = contract(outputCsv = semicolonNoHeader, inputCsv = semicolonNoHeader, secondOutput = true)
    val used = Map("out/elsewhere" -> Map("delimiter" -> ","), "in/other.csv" -> Map("delimiter" -> ","))
    assert(check(c, used, plan = Write(DatasetRef("out/elsewhere"), Read(DatasetRef("in/other.csv")), Some("csv"), None), r = List(Read(DatasetRef("in/other.csv")))).isEmpty)
    assert(check(c, Map("out/report" -> Map("delimiter" -> ",")), plan = UnknownPlan("job", "Python", Nil), r = Nil).isEmpty)
  }

  test("an input read twice is reported once") {
    val c = contract(inputCsv = semicolonNoHeader)
    assert(check(c, Map("in/orders.csv" -> Map("header" -> "true")), r = reads ++ reads).size == 1)
  }

  test("values are compared by meaning: a tab written \\t, a character set by any name, true in any case") {
    assert(CsvOptionsChecker.same("delimiter", "\\t", "\t") && CsvOptionsChecker.same("delimiter", "\t", "\\t"))
    assert(!CsvOptionsChecker.same("delimiter", ";", ",") && !CsvOptionsChecker.same("quote", "'", "\""))
    assert(CsvOptionsChecker.same("delimiter", "||", "||") && !CsvOptionsChecker.same("delimiter", "||", "|"))
    assert(CsvOptionsChecker.same("escape", "\\", "\\"))
    assert(CsvOptionsChecker.same("encoding", "latin1", "ISO-8859-1") && CsvOptionsChecker.same("encoding", "utf-8", "UTF8"))
    assert(!CsvOptionsChecker.same("encoding", "UTF-8", "UTF-16"))
    assert(CsvOptionsChecker.same("header", "true", " TRUE ") && !CsvOptionsChecker.same("header", "true", "false"))
    assert(CsvOptionsChecker.same("dateFormat", "dd/MM/yyyy", "dd/MM/yyyy") && !CsvOptionsChecker.same("dateFormat", "dd/MM/yyyy", "dd/MM/yy"))
    assert(!CsvOptionsChecker.same("nullValue", "NA", "na"))
  }

  test("a tab is shown as \\t") {
    val List(v) = check(contract(outputCsv = "    csv: { delimiter: \"\\t\" }\n"), Map("out/report" -> Map("delimiter" -> ",")))
    assert(v.expected.contains("delimiter '\\t'"))
  }

  test("the pipeline reports it with every other finding of the write") {
    val c = contract(outputCsv = semicolonNoHeader)
    val schema = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true)))
    val checked = CheckedWrite(writeReport, List("in/orders.csv" -> schema), schema, caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty,
      formatOptions = Map("out/report" -> Map("delimiter" -> ",")))
    val result = VerificationPipeline.evaluateWrite(c, checked, VerificationOptions(), None)
    assert(result.violations.map(_.violationType) == List(ViolationType.OutputCsvOptionsMismatch))
    assert(VerificationPipeline.evaluateWrite(c, checked.copy(formatOptions = Map.empty), VerificationOptions(), None).violations.isEmpty)
  }

  test("a contract declaring CSV options needs check.csvOptions; an empty csv block declares none") {
    val required = CapabilityCheck.required(contract(inputCsv = semicolonNoHeader), VerificationOptions())
    assert(required.contains(Capability.CheckCsvOptions -> "dataset 'orders' declares CSV options"))
    assert(!CapabilityCheck.required(contract(outputCsv = "    csv: {}\n"), VerificationOptions()).exists(_._1 == Capability.CheckCsvOptions))
    assert(Capability.all.dropWhile(_ != Capability.CheckPartitioning).take(2) == List(Capability.CheckPartitioning, Capability.CheckCsvOptions))
  }

  test("a dry run's draft declares the options each read and write reported, and only CSV options") {
    val schema = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true)))
    val options = Map("out/report" -> Map("delimiter" -> ";", "header" -> "TRUE", "compression" -> "gzip"), "in/orders.csv" -> Map("encoding" -> "ISO-8859-1"))
    val draft = ContractInference.infer(InferredWrite("out/report", Some("csv"), Some("overwrite"), schema, formatOptions = options), List("in/orders.csv" -> schema), writeReport)
    assert(draft.outputs.head.csv.contains(CsvOptions(delimiter = Some(";"), header = Some(true))))
    assert(draft.inputs.head.csv.contains(CsvOptions(encoding = Some("ISO-8859-1"))))
    assert(ContractInference.csvOf(None).isEmpty && ContractInference.csvOf(Some(Map("compression" -> "gzip"))).isEmpty)
    assert(ContractInference.csvOf(Some(Map("header" -> "no"))).flatMap(_.header).contains(false))
  }
}
