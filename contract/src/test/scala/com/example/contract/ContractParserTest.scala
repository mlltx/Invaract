// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.contract

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractParserTest extends AnyFunSuite {

  private def fixture(name: String): File =
    new File(s"src/test/resources/fixtures/$name")

  test("parseFile should parse a valid contract into the object model") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))

    assert(contract.id == "customer_orders")
    assert(contract.version == ContractVersion(1, 0, 0))
    assert(contract.status == "active")

    assert(contract.inputs.size == 1)
    assert(contract.outputs.size == 1)

    val orders = contract.input("orders").get
    assert(orders.location == "raw.orders")
    assert(orders.format.contains("table"))
    assert(orders.schema.fields.map(_.name) == List("order_id", "customer_id", "amount"))

    val output = contract.output("customer_orders").get
    assert(output.location == "gold.customer_orders")
    assert(output.saveMode.contains("overwrite"))
    assert(output.schema.field("total_orders").exists(_.fieldType == "integer"))
  }

  test("parseFile should treat saveMode as optional, defaulting to None when absent") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))
    val orders = contract.input("orders").get

    assert(orders.saveMode.isEmpty)
  }

  test("parseFile should capture field nullability and required flags") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))
    val customerId = contract.output("customer_orders").get.schema.field("customer_id").get

    assert(customerId.required)
    assert(!customerId.nullable)
  }

  test("parseFile should capture declared rules") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))

    assert(contract.rules.size == 1)
    assert(contract.rules.head.ruleType == "compatibility")
    assert(contract.rules.head.properties.get("mode").contains("backward"))
  }

  test("parseFile should capture declared extensions") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))

    assert(contract.extensions.get("owner").contains("data-platform-team"))
  }

  test("parse should accept an equivalent YAML string") {
    val yaml =
      """id: minimal_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |""".stripMargin

    val contract = ContractParser.parse(yaml)
    assert(contract.id == "minimal_contract")
    assert(contract.outputs.size == 1)
  }

  test("parse should default status to 'active' when omitted") {
    val yaml =
      """id: minimal_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |""".stripMargin

    assert(ContractParser.parse(yaml).status == "active")
  }

  test("parse should default nullable to the inverse of required when omitted") {
    val yaml =
      """id: minimal_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: required_field
        |          type: string
        |          required: true
        |        - name: optional_field
        |          type: string
        |""".stripMargin

    val contract = ContractParser.parse(yaml)
    val schema = contract.output("out").get.schema

    assert(!schema.field("required_field").get.nullable)
    assert(schema.field("optional_field").get.nullable)
  }

  test("parse should support nested struct fields via 'properties'") {
    val yaml =
      """id: nested_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: address
        |          type: struct
        |          properties:
        |            - name: city
        |              type: string
        |            - name: zip
        |              type: string
        |""".stripMargin

    val contract = ContractParser.parse(yaml)
    val address = contract.output("out").get.schema.field("address").get

    assert(address.isStruct)
    assert(address.properties.map(_.name) == List("city", "zip"))
  }

  test("parse should preserve unrecognized top-level keys as extensions") {
    val yaml =
      """id: minimal_contract
        |version: "1.0.0"
        |domain: sales
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |""".stripMargin

    val contract = ContractParser.parse(yaml)
    assert(contract.extensions.get("domain").contains("sales"))
  }

  test("parse should decode a well-formed merge_condition rule via interpret") {
    val yaml =
      """id: dml_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |rules:
        |  - type: merge_condition
        |    columns: [customer_id, order_id]
        |  - type: forbid_unconditional_delete
        |  - type: allowed_update_columns
        |    columns: [status, updated_at]
        |""".stripMargin

    val rules = ContractParser.parse(yaml).rules
    assert(rules.size == 3)
    assert(rules(0).interpret.contains(InterpretedRule.MergeCondition(List("customer_id", "order_id"))))
    assert(rules(1).interpret.contains(InterpretedRule.ForbidUnconditionalDelete))
    assert(rules(2).interpret.contains(InterpretedRule.AllowedUpdateColumns(List("status", "updated_at"))))
  }

  test("interpret should be None for a known rule type with malformed properties") {
    val yaml =
      """id: dml_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |rules:
        |  - type: merge_condition
        |  - type: allowed_update_columns
        |    columns: []
        |""".stripMargin

    val rules = ContractParser.parse(yaml).rules
    assert(rules(0).interpret.isEmpty)
    assert(rules(1).interpret.isEmpty)
  }

  test("interpret should be None for an unrecognized rule type") {
    val yaml =
      """id: dml_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |rules:
        |  - type: compatibility
        |    mode: backward
        |""".stripMargin

    assert(ContractParser.parse(yaml).rules.head.interpret.isEmpty)
  }

  test("parseFile should raise ContractParseException for a missing file") {
    val ex = intercept[ContractParseException] {
      ContractParser.parseFile(fixture("does_not_exist.yaml"))
    }
    assert(ex.getMessage.contains("not found"))
  }

  test("parseFile should raise ContractParseException when 'id' is missing") {
    val ex = intercept[ContractParseException] {
      ContractParser.parseFile(fixture("invalid_missing_id.yaml"))
    }
    assert(ex.getMessage.contains("id"))
  }

  test("parse should raise ContractParseException for an invalid version string") {
    val yaml =
      """id: bad_version
        |version: "not-a-version"
        |outputs: []
        |""".stripMargin

    val ex = intercept[ContractParseException] {
      ContractParser.parse(yaml)
    }
    assert(ex.getMessage.contains("Invalid contract version"))
  }

  test("parse should raise ContractParseException when a dataset is missing 'location'") {
    val yaml =
      """id: missing_location
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |""".stripMargin

    val ex = intercept[ContractParseException] {
      ContractParser.parse(yaml)
    }
    assert(ex.getMessage.contains("location"))
  }

  // write() is the inverse of parse/parseFile - these lock in the round
  // trip a caller like ContractEnforcementRule.dryRun depends on: a
  // Contract built programmatically (or parsed from a real file) must
  // come back byte-for-byte equal after write() then parse(), covering
  // every shape parseContract itself decodes (nested struct fields,
  // interpreted rules, extensions, saveMode, optional format).

  test("write should round-trip a full contract parsed from a real fixture") {
    val original = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))
    val roundTripped = ContractParser.parse(ContractParser.write(original))

    assert(roundTripped == original)
  }

  test("write should round-trip a contract with nested struct fields, rules, and extensions") {
    val yaml =
      """id: nested_contract
        |version: "2.3.1"
        |status: draft
        |inputs:
        |  - name: in
        |    location: raw.in
        |    format: table
        |    schema:
        |      fields:
        |        - name: address
        |          type: struct
        |          required: true
        |          nullable: false
        |          properties:
        |            - name: city
        |              type: string
        |            - name: zip
        |              type: string
        |outputs:
        |  - name: out
        |    location: gold.out
        |    saveMode: overwrite
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |          required: true
        |rules:
        |  - type: merge_condition
        |    columns: [customer_id, order_id]
        |  - type: forbid_unconditional_delete
        |extensions:
        |  owner: data-platform-team
        |  domain: sales
        |""".stripMargin

    val original = ContractParser.parse(yaml)
    val roundTripped = ContractParser.parse(ContractParser.write(original))

    assert(roundTripped == original)
  }

  test("write should omit empty inputs/rules/extensions rather than emitting empty collections") {
    val yaml =
      """id: minimal_contract
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: string
        |""".stripMargin

    val contract = ContractParser.parse(yaml)
    val written = ContractParser.write(contract)

    assert(!written.contains("inputs"))
    assert(!written.contains("rules"))
    assert(!written.contains("extensions"))
    assert(ContractParser.parse(written) == contract)
  }
}
