// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class TypeGuaranteeValidatorTest extends AnyFunSuite {

  private def field(name: String, fieldType: String = "string", required: Boolean = false): Field =
    Field(name, fieldType, required = required)

  private def dataset(name: String, location: String, datasetType: Option[DatasetType], fields: Field*): Dataset =
    Dataset(name, location, Some("parquet"), Schema(fields.toList), datasetType = datasetType)

  private def contract(id: String, version: ContractVersion = ContractVersion(1, 0, 0), inputs: List[Dataset] = Nil, outputs: List[Dataset] = Nil): Contract =
    Contract(id, version, "active", inputs, outputs, Nil, Map.empty)

  private def policyWith(enabled: List[String], mode: PolicyMode = PolicyMode.Enforce, custom: Map[String, String] = Map.empty): OrgPolicy =
    OrgPolicy("1.0", typeGuarantees = TypeGuaranteeConfig(enabled, mode, custom))

  // -- data_asset_schema_consistency ------------------------------------------

  test("data_asset_schema_consistency: no result when only one contract declares a DATA_ASSET at a location") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.isEmpty)
  }

  test("data_asset_schema_consistency: Conforms (no result at all) when two contracts agree on the shared DATA_ASSET's schema") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long", required = true), field("name"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id", "long", required = true))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.isEmpty)
  }

  test("data_asset_schema_consistency: Contradicts when a common field's type disagrees") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id", "string"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.size == 1)
    val result = results.head
    assert(result.verdict == TypeGuaranteeVerdict.Contradicts)
    assert(result.checkType == TypeGuaranteeType.DataAssetSchemaConsistency)
    assert(result.location == "gold.customer")
    assert(result.involvedContracts.toSet == Set("producer@1.0.0", "consumer@1.0.0"))
    assert(result.message.contains("id"))
  }

  test("data_asset_schema_consistency: Contradicts when a field required on one side is entirely absent on the other") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"), field("ssn", required = true))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.size == 1)
    assert(results.head.verdict == TypeGuaranteeVerdict.Contradicts)
    assert(results.head.message.contains("ssn"))
  }

  test("data_asset_schema_consistency: an optional field present on only one side is not a contradiction") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"), field("nickname"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.isEmpty)
  }

  test("data_asset_schema_consistency: a contract is never compared against another version of itself") {
    val a1 = contract("shared", version = ContractVersion(1, 0, 0), outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long"))))
    val a2 = contract("shared", version = ContractVersion(2, 0, 0), outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "string"))))
    val results = TypeGuaranteeValidator.validate(List(a1, a2), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.isEmpty)
  }

  test("data_asset_schema_consistency: datasets at different locations are never compared") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long"))))
    val b = contract("b", inputs = List(dataset("in", "gold.something_else", Some(DatasetType.DataAsset), field("id", "string"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetSchemaConsistency)))
    assert(results.isEmpty)
  }

  // -- data_asset_downstream_consumption --------------------------------------

  test("data_asset_downstream_consumption: Conforms when another contract consumes the DATA_ASSET output") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetDownstreamConsumption)))
    assert(results.size == 1)
    assert(results.head.verdict == TypeGuaranteeVerdict.Conforms)
  }

  test("data_asset_downstream_consumption: CannotDetermine (never Contradicts) when no other contract consumes it") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetDownstreamConsumption)))
    assert(results.size == 1)
    assert(results.head.verdict == TypeGuaranteeVerdict.CannotDetermine)
  }

  test("data_asset_downstream_consumption: a contract's own input at the same location doesn't count as downstream consumption") {
    val a = contract(
      "self",
      inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id"))),
      outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id")))
    )
    val results = TypeGuaranteeValidator.validate(List(a), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetDownstreamConsumption)))
    assert(results.size == 1)
    assert(results.head.verdict == TypeGuaranteeVerdict.CannotDetermine)
  }

  test("data_asset_downstream_consumption never produces a Contradicts verdict, whatever the input") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val b = contract("other", outputs = List(dataset("out2", "gold.unrelated", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetDownstreamConsumption)))
    assert(results.forall(_.verdict != TypeGuaranteeVerdict.Contradicts))
  }

  // -- evaluate / mode ---------------------------------------------------------

  test("evaluate: nothing enabled produces no results and never blocks") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val evaluation = TypeGuaranteeValidator.evaluate(List(a), policyWith(Nil))
    assert(evaluation.results.isEmpty)
    assert(!evaluation.hasBlockingIssues)
  }

  test("evaluate: a real Contradicts in Enforce mode is blocking") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id", "string"))))
    val evaluation = TypeGuaranteeValidator.evaluate(List(a, b), policyWith(List(TypeGuaranteeType.DataAssetSchemaConsistency), mode = PolicyMode.Enforce))
    assert(evaluation.hasBlockingIssues)
    assert(evaluation.blocking.size == 1)
    assert(evaluation.results.size == 1)
  }

  test("evaluate: the identical Contradicts in Warn mode is reported but never blocking") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id", "long"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id", "string"))))
    val evaluation = TypeGuaranteeValidator.evaluate(List(a, b), policyWith(List(TypeGuaranteeType.DataAssetSchemaConsistency), mode = PolicyMode.Warn))
    assert(!evaluation.hasBlockingIssues)
    assert(evaluation.blocking.isEmpty)
    assert(evaluation.results.size == 1)
  }

  test("evaluate: a CannotDetermine result never blocks, even in Enforce mode") {
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val evaluation = TypeGuaranteeValidator.evaluate(List(a), policyWith(List(TypeGuaranteeType.DataAssetDownstreamConsumption), mode = PolicyMode.Enforce))
    assert(!evaluation.hasBlockingIssues)
    assert(evaluation.results.size == 1)
    assert(evaluation.results.head.verdict == TypeGuaranteeVerdict.CannotDetermine)
  }

  // -- custom checks -----------------------------------------------------------

  test("a customTypeGuaranteeTypes entry is resolved and actually runs, seeing the real contract list") {
    val a = contract("a")
    val b = contract("b")
    val custom = Map("my_check" -> classOf[AlwaysConformsTypeGuaranteeCheck].getName)
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List("my_check"), customTypeGuaranteeTypes = custom))
    assert(results.size == 2)
    assert(results.forall(_.verdict == TypeGuaranteeVerdict.Conforms))
    assert(results.map(_.message).toSet == Set("custom check ran for contract 'a'", "custom check ran for contract 'b'"))
  }

  test("a custom check's Contradicts result becomes blocking under evaluate, exactly like a built-in check's") {
    val custom = Map("always_bad" -> classOf[AlwaysContradictsTypeGuaranteeCheck].getName)
    val evaluation = TypeGuaranteeValidator.evaluate(List(contract("a")), policyWith(List("always_bad"), custom = custom))
    assert(evaluation.hasBlockingIssues)
  }

  test("a built-in TypeGuaranteeType name always wins over a colliding customTypeGuaranteeTypes entry") {
    // Names the built-in data_asset_downstream_consumption type but points it
    // at a fixture that would always produce a Contradicts if it ran - since
    // the built-in wins, and this input never contradicts that built-in
    // check, no Contradicts should appear.
    val custom = Map(TypeGuaranteeType.DataAssetDownstreamConsumption -> classOf[AlwaysContradictsTypeGuaranteeCheck].getName)
    val a = contract("producer", outputs = List(dataset("out", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.customer", Some(DatasetType.DataAsset), field("id"))))
    val results = TypeGuaranteeValidator.validate(List(a, b), TypeGuaranteeConfig(List(TypeGuaranteeType.DataAssetDownstreamConsumption), customTypeGuaranteeTypes = custom))
    assert(results.forall(_.verdict != TypeGuaranteeVerdict.Contradicts))
  }

  test("an unresolvable customTypeGuaranteeTypes entry contributes nothing, rather than throwing") {
    val custom = Map("broken" -> "com.invaract.contract.NoSuchClassAtAll")
    val results = TypeGuaranteeValidator.validate(List(contract("a")), TypeGuaranteeConfig(List("broken"), customTypeGuaranteeTypes = custom))
    assert(results.isEmpty)
  }

  test("an enabled name with no matching built-in or custom entry at all contributes nothing") {
    val results = TypeGuaranteeValidator.validate(List(contract("a")), TypeGuaranteeConfig(List("totally_unknown_check")))
    assert(results.isEmpty)
  }
}
