// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{CatalogIdentity, DatasetRef, Join, JoinType, Read, Write}

import org.scalatest.funsuite.AnyFunSuite

/** `check.inputFormat`: each read's format, as the adapter reports it, against its declared input's (`INPUT_FORMAT_MISMATCH`). */
class InputFormatCheckSpec extends AnyFunSuite {

  private def contract(ordersFormat: Option[String] = Some("parquet"), customersFormat: Option[String] = None): Contract = {
    def format(f: Option[String]) = f.fold("")(v => s"    format: $v\n")
    ContractParser.parse(
      s"""id: formats
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: in/orders
         |${format(ordersFormat)}  - name: customers
         |    location: sales/customers
         |${format(customersFormat)}outputs:
         |  - name: report
         |    location: out/report
         |""".stripMargin
    )
  }

  private val orders = Read(DatasetRef("in/orders"))

  private def check(c: Contract, used: Map[String, String], reads: List[Read] = List(orders)) =
    InputChecker.formatFindings(c, reads, used)

  test("a read in the declared format passes, whatever the case; one in another format is refused, naming the read") {
    assert(check(contract(), Map("in/orders" -> "parquet")).isEmpty)
    assert(check(contract(Some("Parquet")), Map("in/orders" -> "PARQUET")).isEmpty)
    val List(v) = check(contract(), Map("in/orders" -> "csv"))
    assert(v.violationType == ViolationType.InputFormatMismatch)
    assert((v.location, v.expected, v.actual) == ((Some("in/orders"), Some("parquet"), Some("csv"))))
    assert(v.message == "contract declares input format 'parquet' but the plan reads 'in/orders' in format 'csv'")
    assert(v.remediation == "Read 'in/orders' as 'parquet' instead, or update the contract's declared format to 'csv' if the input's format has changed.")
  }

  test("both sides must be known: an input declaring no format, or a read whose format is not reported, is not compared") {
    assert(check(contract(ordersFormat = None), Map("in/orders" -> "csv")).isEmpty)
    assert(check(contract(), Map.empty).isEmpty)
    assert(check(contract(), Map("in/elsewhere" -> "csv")).isEmpty)
  }

  test("formatFinding: one input against one read, for an adapter that checks each read as it is made") {
    val orders = contract().inputs.head
    assert(InputChecker.formatFinding(orders, Some("PARQUET"), "in/orders").isEmpty)
    assert(InputChecker.formatFinding(orders, None, "in/orders").isEmpty)
    assert(InputChecker.formatFinding(orders.copy(format = None), Some("csv"), "in/orders").isEmpty)
    assert(InputChecker.formatFinding(orders, Some("csv"), "data/orders").map(v => (v.violationType, v.location)) ==
      Some((ViolationType.InputFormatMismatch, Some("data/orders"))))
  }

  test("a read of no declared input is not this check's concern, even in a format no input declares") {
    assert(check(contract(), Map("in/other" -> "csv"), List(Read(DatasetRef("in/other")))).isEmpty)
  }

  test("an input read twice is one finding; each input is checked, in the order read") {
    assert(check(contract(), Map("in/orders" -> "csv"), List(orders, orders)).size == 1)
    val customers = Read(DatasetRef("/warehouse/sales.db/customers"), catalog = Some(CatalogIdentity(namespace = List("sales"), table = Some("customers"))))
    val both = contract(customersFormat = Some("iceberg"))
    val found = check(both, Map("in/orders" -> "csv", "/warehouse/sales.db/customers" -> "delta"), List(customers, orders))
    assert(found.map(_.location) == List(Some("/warehouse/sales.db/customers"), Some("in/orders")))
    assert(found.map(_.expected) == List(Some("iceberg"), Some("parquet")))
    // only the input in another format
    assert(check(both, Map("in/orders" -> "parquet", "/warehouse/sales.db/customers" -> "delta"), List(customers, orders)).map(_.location) ==
      List(Some("/warehouse/sales.db/customers")))
  }

  test("a catalog table is matched to its declared input by its table name, as every input check matches it") {
    val table = Read(DatasetRef("/warehouse/sales.db/customers"), catalog = Some(CatalogIdentity(namespace = List("sales"), table = Some("customers"))))
    val c = contract(customersFormat = Some("iceberg"))
    assert(check(c, Map("/warehouse/sales.db/customers" -> "iceberg"), List(table)).isEmpty)
    assert(check(c, Map("/warehouse/sales.db/customers" -> "hive"), List(table)).map(_.violationType) == List(ViolationType.InputFormatMismatch))
    // without its catalog identity the files' location names no declared input
    assert(check(c, Map("/warehouse/sales.db/customers" -> "hive"), List(table.copy(catalog = None))).isEmpty)
  }

  test("the pipeline checks every read's format: CheckedWrite.inputFormats") {
    val schema = LogicalSchema(Nil)
    val plan = Write(DatasetRef("out/report"), Join(orders, Read(DatasetRef("sales/customers")), JoinType.Cross), Some("parquet"), Some("overwrite"))
    def evaluate(formats: Map[String, String]) = VerificationPipeline.evaluateWrite(
      contract(customersFormat = Some("jdbc")),
      CheckedWrite(plan, Nil, schema, caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty, inputFormats = formats),
      VerificationOptions(),
      None
    )
    assert(evaluate(Map("in/orders" -> "parquet", "sales/customers" -> "jdbc")).passed)
    val failed = evaluate(Map("in/orders" -> "json", "sales/customers" -> "jdbc"))
    assert(failed.violations.map(_.violationType) == List(ViolationType.InputFormatMismatch))
    assert(evaluate(Map.empty).passed)
  }

  test("a dry run's draft declares the format each input was read in, where the adapter reports it") {
    val schema = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true)))
    val plan = Write(DatasetRef("out/report"), Join(orders, Read(DatasetRef("in/customers")), JoinType.Cross), Some("parquet"), Some("overwrite"))
    val draft = ContractInference.infer(
      InferredWrite("out/report", Some("parquet"), Some("overwrite"), schema, inputFormats = Map("in/orders" -> "csv")),
      List("in/orders" -> schema, "in/customers" -> schema),
      plan
    )
    assert(draft.inputs.map(i => i.location -> i.format) == List("in/orders" -> Some("csv"), "in/customers" -> None))
  }
}
