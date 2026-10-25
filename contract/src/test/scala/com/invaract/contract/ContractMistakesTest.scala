// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** The mistakes people make writing a contract, and what they are told: a typo gets a "did you mean", a key spelt close
  * to one Invaract reads is no longer ignored without a word, and a YAML syntax error says where it is.
  */
class ContractMistakesTest extends AnyFunSuite {

  // --- Suggestions ---

  test("distance counts insertions, deletions and substitutions") {
    assert(Suggestions.distance("", "abc") == 3)
    assert(Suggestions.distance("abc", "") == 3)
    assert(Suggestions.distance("abc", "abc") == 0)
    assert(Suggestions.distance("kitten", "sitting") == 3)
    assert(Suggestions.distance("ab", "ba") == 2)
    assert(Suggestions.distance("nulable", "nullable") == 1)
    assert(Suggestions.distance("abcd", "abd") == 1)
    assert(Suggestions.distance("abc", "axc") == 1)
  }

  test("closest allows one edit up to four characters, two beyond, and never three") {
    assert(Suggestions.closest("tabl", List("table")).contains("table"))
    assert(Suggestions.closest("abcd", List("abxy")).isEmpty, "two edits on four characters is too far")
    assert(Suggestions.closest("abcd", List("abcx")).contains("abcx"))
    assert(Suggestions.closest("parqet", List("parquet")).contains("parquet"))
    assert(Suggestions.closest("abcde", List("abcxy")).contains("abcxy"), "two edits on five characters is close enough")
    assert(Suggestions.closest("abcde", List("abxyz")).isEmpty, "three edits never are")
  }

  test("closest ignores case and surrounding space, but never suggests the value itself") {
    assert(Suggestions.closest("SaveMode", List("saveMode")).contains("saveMode"))
    assert(Suggestions.closest(" parqet ", List("parquet")).contains("parquet"))
    assert(Suggestions.closest("type", List("type")).isEmpty)
    assert(Suggestions.closest("anything", Nil).isEmpty)
  }

  test("closest prefers the nearest candidate, then the alphabetically first") {
    assert(Suggestions.closest("abcdef", List("abcdxy", "abcdex")).contains("abcdex"))
    assert(Suggestions.closest("abcdef", List("abcdeg", "abcdeh")).contains("abcdeg"))
    assert(Suggestions.closest("abcdef", List("abcdeh", "abcdeg")).contains("abcdeg"))
  }

  test("didYouMean is a sentence when something is close, and nothing otherwise") {
    assert(Suggestions.didYouMean("strng", List("string")) == " Did you mean 'string'?")
    assert(Suggestions.didYouMean("variant", List("string")) == "")
  }

  // --- ContractValidator's typo warnings ---

  private def contract(fieldType: String = "string", format: Option[String] = None, saveMode: Option[String] = None) =
    Contract(
      "c", ContractVersion(1, 0, 0), "active", Nil,
      List(Dataset("out", "/out", format, Schema(List(Field("id", fieldType))), saveMode)),
      Nil, Map.empty, Map.empty
    )

  private def warning(c: Contract, path: String): String =
    ContractValidator.validate(c).warnings.find(_.path == path).map(_.message).getOrElse(fail(s"no warning at $path"))

  test("an unknown field type suggests the known type it is closest to") {
    val message = warning(contract(fieldType = "strng"), "outputs[0].schema.id")
    assert(message.startsWith("Unrecognized field type 'strng'. Did you mean 'string'? Known types: array, binary, boolean,"))
    assert(warning(contract(fieldType = "strng(5)"), "outputs[0].schema.id").contains("Did you mean 'string'?"))
    assert(!warning(contract(fieldType = "variant"), "outputs[0].schema.id").contains("Did you mean"))
  }

  test("a non-canonical format and save mode suggest the canonical one they are closest to") {
    val format = warning(contract(format = Some("parqet")), "outputs[0].format")
    assert(format.endsWith("may fail on another engine. Did you mean 'parquet'?"))
    assert(warning(contract(saveMode = Some("overwite")), "outputs[0].saveMode").endsWith("Did you mean 'overwrite'?"))
    assert(warning(contract(format = Some("BIGQUERY_TABLE")), "outputs[0].format").endsWith("another engine."))
  }

  // --- ContractParser: booleans and YAML syntax ---

  private def withField(extra: String) =
    s"""id: c
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: /out
       |    schema:
       |      fields:
       |        - name: id
       |          type: string
       |$extra
       |""".stripMargin

  test("a quoted true or false is read in any case; anything else names the key and the value") {
    assert(!ContractParser.parse(withField("""          nullable: " FALSE """")).outputs.head.schema.fields.head.nullable)
    assert(ContractParser.parse(withField("""          nullable: "True"""")).outputs.head.schema.fields.head.nullable)
    val e = intercept[ContractParseException](ContractParser.parse(withField("""          nullable: "no way"""")))
    assert(e.getMessage == "Expected true or false for 'outputs[0].schema.fields[0].nullable', got: no way")
  }

  test("a YAML syntax error says so, and where it is") {
    val e = intercept[ContractParseException](ContractParser.parse("id: c\nversion: [1.0.0\noutputs: []\n"))
    assert(e.getMessage.startsWith("The contract is not valid YAML: "))
    assert(e.getMessage.contains("line 3"))
    assert(e.getCause.isInstanceOf[org.yaml.snakeyaml.error.YAMLException])
  }

  test("a contract file that fails to parse names the file before the reason") {
    val file = Files.createTempFile("broken-contract", ".yaml")
    file.toFile.deleteOnExit()
    Files.write(file, "id: c\nversion: [1.0.0\n".getBytes(StandardCharsets.UTF_8))
    val syntax = intercept[ContractParseException](ContractParser.parseFile(file.toString))
    assert(syntax.getMessage.startsWith(s"$file: The contract is not valid YAML: "))
    Files.write(file, "version: \"1.0.0\"\n".getBytes(StandardCharsets.UTF_8))
    assert(intercept[ContractParseException](ContractParser.parseFile(file.toString)).getMessage == s"$file: Missing required field 'id' in contract")
  }

  // --- ContractParser.unknownKeyWarnings ---

  private def keyWarnings(yaml: String): List[(String, String)] = ContractParser.unknownKeyWarnings(yaml).map(w => (w.path, w.message))

  test("a valid contract has no key warnings") {
    assert(keyWarnings(withField("          nullable: false")).isEmpty)
  }

  test("a field key spelt close to one Invaract reads is reported with its path and the key it was probably meant to be") {
    assert(
      keyWarnings(withField("          nulable: false")) ==
        List("outputs[0].schema.fields[0].nulable" -> "Unknown key 'nulable' is ignored. Did you mean 'nullable'?")
    )
    assert(ContractParser.unknownKeyWarnings(withField("          nulable: false")).forall(_.severity == ValidationSeverity.Warning))
  }

  test("near misses are found at every level: the contract, a dataset, its catalog, its schema and nested fields") {
    val yaml =
      """id: c
        |version: "1.0.0"
        |input: []
        |outputs:
        |  - name: out
        |    location: /out
        |    saveMod: append
        |    catalog:
        |      requird: true
        |    schema:
        |      feilds: []
        |  - name: other
        |    location: /other
        |    schema:
        |      fields:
        |        - name: s
        |          type: struct
        |          properties:
        |            - name: inner
        |              typ: string
        |""".stripMargin
    assert(
      keyWarnings(yaml).map(_._1) == List(
        "input",
        "outputs[0].saveMod",
        "outputs[0].catalog.requird",
        "outputs[0].schema.feilds",
        "outputs[1].schema.fields[0].properties[0].typ"
      )
    )
    assert(keyWarnings(yaml).head._2 == "Unknown key 'input' is ignored. Did you mean 'inputs'?")
  }

  test("inputs are checked as well as outputs") {
    val yaml = "id: c\nversion: \"1.0.0\"\ninputs:\n  - name: in\n    locaton: /in\n"
    assert(keyWarnings(yaml) == List("inputs[0].locaton" -> "Unknown key 'locaton' is ignored. Did you mean 'location'?"))
  }

  test("unknown keys that are not near misses, and ODCS keys that look like one, are left alone") {
    val yaml = withField("          description: an id\n          unique: true").replace("id: c\n", "id: c\nowner: team\nroles: []\n")
    assert(keyWarnings(yaml).isEmpty)
  }

  test("a document that is not valid YAML, or not a mapping, has no key warnings") {
    assert(keyWarnings("id: [unclosed").isEmpty)
    assert(keyWarnings("- a\n- b\n").isEmpty)
    assert(keyWarnings("id: c\noutputs: not-a-list\n").isEmpty)
  }
}
