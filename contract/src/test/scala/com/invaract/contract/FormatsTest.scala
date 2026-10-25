// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class FormatsTest extends AnyFunSuite {

  private def contractWith(format: Option[String], technology: Option[String]): Contract =
    ContractParser.parse(
      s"""id: fm
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: out/o
         |${format.map(f => s"    format: $f\n").getOrElse("")}${technology
        .map(t => s"    catalog:\n      required: true\n      technology: $t\n")
        .getOrElse("")}    schema:
         |      fields:
         |        - name: id
         |          type: long
         |""".stripMargin
    )

  test("the canonical formats are exactly the documented set, all lower-case") {
    assert(
      Formats.All == Set("parquet", "orc", "avro", "csv", "json", "text", "table", "delta", "iceberg", "hudi", "hive", "jdbc", "bigquery", "kafka")
    )
    assert(Formats.All.forall(f => f == f.toLowerCase))
    assert(Formats.Parquet == "parquet" && Formats.BigQuery == "bigquery" && Formats.Delta == "delta")
  }

  test("the canonical technologies are exactly the documented set, all lower-case") {
    assert(CatalogTechnologies.All == Set("hive", "delta", "iceberg", "hudi", "jdbc", "glue", "bigquery", "in-memory"))
    assert(CatalogTechnologies.All.forall(t => t == t.toLowerCase))
  }

  test("isCanonical ignores case and surrounding whitespace, and rejects anything else") {
    assert(Formats.isCanonical("parquet") && Formats.isCanonical("PARQUET") && Formats.isCanonical(" Csv "))
    assert(!Formats.isCanonical("BIGQUERY_TABLE") && !Formats.isCanonical("") && !Formats.isCanonical("parquet,orc"))
    assert(CatalogTechnologies.isCanonical("Hive") && CatalogTechnologies.isCanonical(" iceberg "))
    assert(!CatalogTechnologies.isCanonical("SparkCatalog") && !CatalogTechnologies.isCanonical(""))
  }

  test("canonical or absent format and technology raise no warning") {
    assert(ContractValidator.validate(contractWith(None, None)).warnings.isEmpty)
    Formats.All.foreach(f => assert(ContractValidator.validate(contractWith(Some(f), None)).warnings.isEmpty, f))
    CatalogTechnologies.All.foreach(t => assert(ContractValidator.validate(contractWith(None, Some(t))).warnings.isEmpty, t))
  }

  test("a non-canonical format is a warning naming the path and the canonical set, never an error") {
    val result = ContractValidator.validate(contractWith(Some("BIGQUERY_TABLE"), None))
    assert(result.isValid, "the vocabulary is open: an unknown format must not fail validation")
    val warning = result.warnings.find(_.path == "outputs[0].format").getOrElse(fail(s"no format warning in ${result.issues}"))
    assert(warning.message.contains("BIGQUERY_TABLE"))
    assert(warning.message.contains("avro, bigquery, csv"))
  }

  test("a non-canonical catalog technology is a warning on its own path, never an error") {
    val result = ContractValidator.validate(contractWith(None, Some("SparkCatalog")))
    assert(result.isValid)
    val warning = result.warnings.find(_.path == "outputs[0].catalog.technology").getOrElse(fail(s"no technology warning in ${result.issues}"))
    assert(warning.message.contains("SparkCatalog"))
    assert(warning.message.contains("bigquery, delta, glue"))
  }

  test("a non-canonical format and technology are reported together, each once") {
    val warnings = ContractValidator.validate(contractWith(Some("weird"), Some("odd"))).warnings
    assert(warnings.map(_.path).sorted == List("outputs[0].catalog.technology", "outputs[0].format"))
  }
}
