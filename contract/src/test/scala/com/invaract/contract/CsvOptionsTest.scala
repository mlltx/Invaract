// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

/** A dataset's `csv:` options: parsed, written back, validated, and classified when they change. */
class CsvOptionsTest extends AnyFunSuite {

  private def contract(csv: String, format: String = "csv"): Contract =
    ContractParser.parse(
      s"""id: orders
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: in/orders.csv
         |    format: $format
         |    schema:
         |      fields:
         |        - { name: id, type: long }
         |    csv:
         |$csv
         |outputs:
         |  - name: report
         |    location: out/report
         |    schema:
         |      fields:
         |        - { name: id, type: long }
         |""".stripMargin
    )

  private def options(csv: String): CsvOptions = contract(csv).inputs.head.csv.get

  private def issues(c: Contract, severity: ValidationSeverity): List[String] =
    ContractValidator.validate(c).issues.filter(_.severity == severity).map(i => s"${i.path}: ${i.message}")
  private def errors(c: Contract) = issues(c, ValidationSeverity.Error)
  private def warnings(c: Contract) = issues(c, ValidationSeverity.Warning)

  private val all =
    """      delimiter: ";"
      |      header: false
      |      encoding: ISO-8859-1
      |      quote: "'"
      |      escape: "\\"
      |      nullValue: NA
      |      dateFormat: dd/MM/yyyy
      |      timestampFormat: "dd/MM/yyyy HH:mm:ss"""".stripMargin

  // --- parsing and writing back ----------------------------------------------------------------

  test("every option is read under Spark's name, and listed as declared in that order") {
    val o = options(all)
    assert(o == CsvOptions(Some(";"), Some(false), Some("ISO-8859-1"), Some("'"), Some("\\"), Some("NA"), Some("dd/MM/yyyy"), Some("dd/MM/yyyy HH:mm:ss")))
    assert(o.declared.map(_._1) == CsvOptions.Names)
    assert(o.declared.toMap.apply("header") == "false")
  }

  test("an option not declared is not listed, and a dataset with no csv block has none") {
    assert(options("      delimiter: \"|\"").declared == List("delimiter" -> "|"))
    assert(ContractParser.parse("id: o\nversion: \"1.0.0\"\ninputs:\n  - { name: a, location: in/a }\n").inputs.head.csv.isEmpty)
  }

  test("Spark's sep is the delimiter; given both, they must agree") {
    assert(options("      sep: \"|\"").delimiter.contains("|"))
    assert(options("      sep: \"|\"\n      delimiter: \"|\"").delimiter.contains("|"))
    val e = intercept[ContractParseException](options("      sep: \"|\"\n      delimiter: \";\""))
    assert(e.getMessage.contains("gives delimiter ';' and sep '|'"))
  }

  test("a value YAML reads as a number is its text") {
    assert(options("      nullValue: 0").nullValue.contains("0"))
  }

  test("an option the block does not take is an error, not ignored") {
    val e = intercept[ContractParseException](options("      delimeter: \";\"\n      header: true"))
    assert(e.getMessage.contains("Unknown CSV option(s) delimeter in"))
    assert(e.getMessage.contains("it takes delimiter, header, encoding, quote, escape, nullValue, dateFormat, timestampFormat"))
  }

  test("the options are written back as they were declared") {
    val original = contract(all)
    val again = ContractParser.parse(ContractParser.write(original))
    assert(again.inputs.head.csv == original.inputs.head.csv)
    assert(ContractParser.write(contract("      header: true")).contains("header: true"))
  }

  // --- validation --------------------------------------------------------------------------------

  test("a complete, well-formed csv block has no issues") {
    assert(errors(contract(all)).isEmpty && warnings(contract(all)).isEmpty)
  }

  test("CSV options on a dataset of another format have no effect") {
    assert(warnings(contract("      delimiter: \";\"", format = "parquet")) ==
      List("inputs[0].csv: Declares CSV options on a dataset whose format is 'parquet', where they have no effect"))
    assert(warnings(contract("      delimiter: \";\"", format = "CSV")).isEmpty)
  }

  test("a delimiter is one character: empty is an error, longer is read by Spark alone") {
    assert(errors(contract("      delimiter: \"\"")) == List("inputs[0].csv.delimiter: A delimiter cannot be empty"))
    assert(warnings(contract("      delimiter: \"||\"")).exists(_.contains("A delimiter of more than one character ('||')")))
    assert(errors(contract("      delimiter: \"||\"")).isEmpty)
    assert(warnings(contract("      delimiter: \\t")).isEmpty)
  }

  test("a quote and an escape are one character each") {
    assert(errors(contract("      quote: \"''\"")) == List("inputs[0].csv.quote: A quote is one character, not ''''"))
    assert(errors(contract("      escape: \"\"")) == List("inputs[0].csv.escape: A escape is one character, not ''"))
    assert(errors(contract("      quote: \"'\"\n      escape: \\t")).isEmpty)
  }

  test("an encoding is a character set the JVM knows, under any of its names") {
    assert(errors(contract("      encoding: klingon-1")).map(_.split(":").head) == List("inputs[0].csv.encoding"))
    assert(errors(contract("      encoding: latin1")).isEmpty && errors(contract("      encoding: utf8")).isEmpty)
  }

  test("date and timestamp patterns are DateTimeFormatter's") {
    assert(errors(contract("      dateFormat: \"yyyy-MM-dd'T\"")).map(_.split(":").head) == List("inputs[0].csv.dateFormat"))
    assert(errors(contract("      timestampFormat: \"yyyy-MM-dd {\"")).map(_.split(":").head) == List("inputs[0].csv.timestampFormat"))
    assert(errors(contract("      dateFormat: \"MM/dd/yy\"\n      timestampFormat: \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"")).isEmpty)
  }

  // --- compatibility -----------------------------------------------------------------------------

  private def changes(before: String, after: String): List[(CompatibilityLevel, String, String)] =
    ContractCompatibility.diff(contract(before), contract(after)).changes.filter(_.path.contains(".csv")).map(c => (c.level, c.path, c.description))

  test("declaring an option, or changing one, is breaking; dropping one only loosens what is checked") {
    assert(changes("      header: true", "      header: true\n      delimiter: \";\"") ==
      List((CompatibilityLevel.Breaking, "inputs.orders.csv.delimiter", "'delimiter' is now required to be ';'")))
    assert(changes("      delimiter: \",\"", "      delimiter: \";\"") ==
      List((CompatibilityLevel.Breaking, "inputs.orders.csv.delimiter", "'delimiter' changed from ',' to ';'")))
    assert(changes("      header: true\n      delimiter: \";\"", "      header: true").isEmpty)
    assert(changes(all, all).isEmpty)
  }

  // --- CsvText -----------------------------------------------------------------------------------

  test("the text a contract writes for a character, a character set and a pattern") {
    assert(CsvText.character("\\t").contains('\t') && CsvText.character("\t").contains('\t') && CsvText.character(";").contains(';'))
    assert(CsvText.character("").isEmpty && CsvText.character(";;").isEmpty)
    assert(CsvText.canonicalEncoding("latin1") == "ISO-8859-1" && CsvText.canonicalEncoding(" utf8 ") == "UTF-8")
    assert(CsvText.canonicalEncoding("klingon-1") == "klingon-1")
    assert(!CsvText.knownEncoding("klingon-1") && !CsvText.knownEncoding("bad name!") && CsvText.knownEncoding("windows-1252"))
    assert(CsvText.patternError("dd/MM/yyyy").isEmpty && CsvText.patternError("yyyy-MM-dd'T").isDefined)
  }
}
