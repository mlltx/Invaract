// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

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
    assert(output.schema.field("total_orders").exists(_.fieldType == "integer"))
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
}
