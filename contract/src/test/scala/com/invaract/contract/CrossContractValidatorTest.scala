// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class CrossContractValidatorTest extends AnyFunSuite {

  private def dataset(name: String, location: String, datasetType: Option[DatasetType]): Dataset =
    Dataset(name, location, Some("parquet"), Schema(List(Field("id", "string"))), datasetType = datasetType)

  private def contract(id: String, inputs: List[Dataset] = Nil, outputs: List[Dataset] = Nil): Contract =
    Contract(id, ContractVersion(1, 0, 0), "active", inputs, outputs, Nil, Map.empty)

  test("no issue when only one contract exists, whatever it declares") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer_master", Some(DatasetType.DataAsset))))
    assert(CrossContractValidator.validate(List(a)).isEmpty)
  }

  test("no issue when no contract declares a DATA_ASSET output at all") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer_master", None)))
    val b = contract("b", inputs = List(dataset("customer_master", "gold.customer_master", Some(DatasetType.Source))))
    assert(CrossContractValidator.validate(List(a, b)).isEmpty)
  }

  test("no issue when no other contract declares the same location as a SOURCE input") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer_master", Some(DatasetType.DataAsset))))
    val b = contract("b", inputs = List(dataset("customer_master", "gold.customer_master", Some(DatasetType.DataAsset))))
    assert(CrossContractValidator.validate(List(a, b)).isEmpty)
  }

  test("no issue when the two datasets are at different locations") {
    val a = contract("a", outputs = List(dataset("out", "gold.customer_master", Some(DatasetType.DataAsset))))
    val b = contract("b", inputs = List(dataset("customer_master", "gold.something_else", Some(DatasetType.Source))))
    assert(CrossContractValidator.validate(List(a, b)).isEmpty)
  }

  test("an issue is found when contract A's DATA_ASSET output is contract B's SOURCE input at the same location") {
    val a = contract("customer_master_pipeline", outputs = List(dataset("customer_master", "gold.customer_master", Some(DatasetType.DataAsset))))
    val b = contract("downstream_pipeline", inputs = List(dataset("customer_master", "gold.customer_master", Some(DatasetType.Source))))

    val issues = CrossContractValidator.validate(List(a, b))
    assert(issues.size == 1)
    val issue = issues.head
    assert(issue.location == "gold.customer_master")
    assert(issue.producingContract == "customer_master_pipeline@1.0.0")
    assert(issue.consumingContract == "downstream_pipeline@1.0.0")
    assert(issue.consumingDataset == "customer_master")
    assert(issue.message.contains("DATA_ASSET"))
    assert(issue.message.contains("SOURCE"))
  }

  test("order of the input list doesn't matter - the check finds the issue regardless of which contract is listed first") {
    val a = contract("producer", outputs = List(dataset("out", "gold.shared", Some(DatasetType.DataAsset))))
    val b = contract("consumer", inputs = List(dataset("in", "gold.shared", Some(DatasetType.Source))))
    assert(CrossContractValidator.validate(List(a, b)).size == 1)
    assert(CrossContractValidator.validate(List(b, a)).size == 1)
  }

  test("a contract is never compared against another contract sharing the same id") {
    val a1 = contract("shared_id", outputs = List(dataset("out", "gold.shared", Some(DatasetType.DataAsset))))
    val a2 = contract("shared_id", inputs = List(dataset("in", "gold.shared", Some(DatasetType.Source))))
    assert(CrossContractValidator.validate(List(a1, a2)).isEmpty)
  }

  test("multiple independent issues across three contracts are all reported") {
    val a = contract("a", outputs = List(dataset("out", "gold.shared_1", Some(DatasetType.DataAsset))))
    val b = contract("b", outputs = List(dataset("out", "gold.shared_2", Some(DatasetType.DataAsset))))
    val c = contract(
      "c",
      inputs = List(
        dataset("in1", "gold.shared_1", Some(DatasetType.Source)),
        dataset("in2", "gold.shared_2", Some(DatasetType.Source))
      )
    )
    val issues = CrossContractValidator.validate(List(a, b, c))
    assert(issues.size == 2)
    assert(issues.map(_.location).toSet == Set("gold.shared_1", "gold.shared_2"))
  }

  test("a backslash-authored location still matches its forward-slash counterpart") {
    val a = contract("a", outputs = List(dataset("out", "gold\\customer_master", Some(DatasetType.DataAsset))))
    val b = contract("b", inputs = List(dataset("in", "gold/customer_master", Some(DatasetType.Source))))
    assert(CrossContractValidator.validate(List(a, b)).size == 1)
  }

  test("CONTROL and DATA_ASSET-vs-DATA_ASSET combinations at the same location are not flagged - only DATA_ASSET-output-vs-SOURCE-input is checked") {
    val a = contract("a", outputs = List(dataset("out", "gold.calendar", Some(DatasetType.DataAsset))))
    val b = contract("b", inputs = List(dataset("in", "gold.calendar", Some(DatasetType.Control))))
    assert(CrossContractValidator.validate(List(a, b)).isEmpty)
  }
}
