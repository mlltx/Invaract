// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.contract

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractValidatorTest extends AnyFunSuite {

  private def fixture(name: String): File =
    new File(s"src/test/resources/fixtures/$name")

  test("validate should accept a well-formed contract with no errors or warnings") {
    val contract = ContractParser.parseFile(fixture("customer_orders_v1.yaml"))
    val result = ContractValidator.validate(contract)

    assert(result.isValid)
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
  }

  test("validate should error when the contract has no outputs") {
    val contract = ContractParser.parseFile(fixture("invalid_no_outputs.yaml"))
    val result = ContractValidator.validate(contract)

    assert(!result.isValid)
    assert(result.errors.exists(_.path == "outputs"))
  }

  test("validate should error on duplicate field names within a schema") {
    val contract = ContractParser.parseFile(fixture("warnings_field_issues.yaml"))
    val result = ContractValidator.validate(contract)

    assert(!result.isValid)
    assert(result.errors.exists(_.message.contains("Duplicate field name 'customer_id'")))
  }

  test("validate should warn when a field is both required and nullable") {
    val contract = ContractParser.parseFile(fixture("warnings_field_issues.yaml"))
    val result = ContractValidator.validate(contract)

    assert(result.warnings.exists(_.message.contains("required but also nullable")))
  }

  test("validate should warn on an unrecognized field type") {
    val contract = ContractParser.parseFile(fixture("warnings_field_issues.yaml"))
    val result = ContractValidator.validate(contract)

    assert(result.warnings.exists(_.message.contains("Unrecognized field type 'variant'")))
  }

  test("validate should error when a dataset schema has no fields") {
    val contract = Contract(
      id = "empty_schema",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, Schema(Nil))),
      rules = Nil,
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(result.errors.exists(_.message.contains("at least one field")))
  }

  test("validate should error on duplicate output dataset names") {
    val schema = Schema(List(Field("id", "string", required = true, nullable = false)))
    val contract = Contract(
      id = "dup_outputs",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out_a", None, schema), Dataset("out", "gold.out_b", None, schema)),
      rules = Nil,
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(result.errors.exists(_.message.contains("Duplicate output dataset name 'out'")))
  }

  test("validate should error when a merge_condition rule has no 'columns' list") {
    val schema = Schema(List(Field("id", "string", required = true, nullable = false)))
    val contract = Contract(
      id = "bad_rule",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, schema)),
      rules = List(ContractRule(RuleType.MergeCondition, Map.empty)),
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(!result.isValid)
    assert(result.errors.exists(_.message.contains("merge_condition")))
  }

  test("validate should error when an allowed_update_columns rule has an empty 'columns' list") {
    val schema = Schema(List(Field("id", "string", required = true, nullable = false)))
    val contract = Contract(
      id = "bad_rule",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, schema)),
      rules = List(ContractRule(RuleType.AllowedUpdateColumns, Map("columns" -> new java.util.ArrayList[String]()))),
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(!result.isValid)
    assert(result.errors.exists(_.message.contains("allowed_update_columns")))
  }

  test("validate should accept a well-formed forbid_unconditional_delete rule with no properties") {
    val schema = Schema(List(Field("id", "string", required = true, nullable = false)))
    val contract = Contract(
      id = "good_rule",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, schema)),
      rules = List(ContractRule(RuleType.ForbidUnconditionalDelete, Map.empty)),
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(result.isValid)
  }

  test("validate should not flag an unrecognized rule type as malformed") {
    val schema = Schema(List(Field("id", "string", required = true, nullable = false)))
    val contract = Contract(
      id = "custom_rule",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, schema)),
      rules = List(ContractRule("compatibility", Map("mode" -> "backward"))),
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(result.isValid)
  }

  test("validate should recurse into nested struct fields") {
    val nested = Field("zip", "unknown_type", required = false, nullable = true)
    val struct = Field("address", "struct", required = false, nullable = true, properties = List(nested))
    val schema = Schema(List(struct))
    val contract = Contract(
      id = "nested_contract",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = Nil,
      outputs = List(Dataset("out", "gold.out", None, schema)),
      rules = Nil,
      extensions = Map.empty
    )

    val result = ContractValidator.validate(contract)
    assert(result.warnings.exists(_.path.endsWith("address.zip")))
  }
}
