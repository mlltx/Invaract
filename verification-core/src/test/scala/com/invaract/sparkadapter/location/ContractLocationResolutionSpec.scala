// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

import com.invaract.contract.ContractParser

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractLocationResolutionSpec extends AnyFunSuite {

  private def realDemoContract() =
    ContractParser.parseFile(new File("../demo/contracts/invaract_output.yaml"))

  test("a contract with no ref:// locations at all is returned unchanged") {
    val contract = realDemoContract()
    val resolved = ContractLocationResolution.resolve(contract, NoOpLocationResolver)
    assert(resolved == contract)
  }

  test("resolves a ref:// input location via the given resolver") {
    val contract = realDemoContract()
    val withRef = contract.copy(inputs = contract.inputs.map(_.copy(location = "ref://orders-input")))
    val resolver = new StaticMapLocationResolver(Map("orders-input" -> "demo/input/sample.csv"))

    val resolved = ContractLocationResolution.resolve(withRef, resolver)

    assert(resolved.inputs.map(_.location) == List("demo/input/sample.csv"))
    // Nothing else about the dataset changes.
    assert(resolved.inputs.head.copy(location = "") == withRef.inputs.head.copy(location = ""))
  }

  test("resolves a ref:// output location via the given resolver") {
    val contract = realDemoContract()
    val withRef = contract.copy(outputs = contract.outputs.map(_.copy(location = "ref://result-output")))
    val resolver = new StaticMapLocationResolver(Map("result-output" -> "demo/output/result.parquet"))

    val resolved = ContractLocationResolution.resolve(withRef, resolver)

    assert(resolved.outputs.map(_.location) == List("demo/output/result.parquet"))
  }

  test("a contract mixing a literal and a reference resolves only the reference") {
    val contract = realDemoContract()
    val mixed = contract.copy(
      inputs = contract.inputs.map(_.copy(location = "ref://orders-input")),
      outputs = contract.outputs // left as a literal
    )
    val resolver = new StaticMapLocationResolver(Map("orders-input" -> "demo/input/sample.csv"))

    val resolved = ContractLocationResolution.resolve(mixed, resolver)

    assert(resolved.inputs.map(_.location) == List("demo/input/sample.csv"))
    assert(resolved.outputs == contract.outputs)
  }

  test("an unresolvable reference propagates LocationResolutionException, from the input side") {
    val contract = realDemoContract()
    val withRef = contract.copy(inputs = contract.inputs.map(_.copy(location = "ref://unknown-id")))

    val ex = intercept[LocationResolutionException] {
      ContractLocationResolution.resolve(withRef, new StaticMapLocationResolver(Map.empty))
    }
    assert(ex.getMessage.contains("unknown-id"))
  }

  test("a ref:// contract resolved with NoOpLocationResolver fails with a clear, actionable message") {
    val contract = realDemoContract()
    val withRef = contract.copy(inputs = contract.inputs.map(_.copy(location = "ref://orders-input")))

    val ex = intercept[LocationResolutionException] {
      ContractLocationResolution.resolve(withRef, NoOpLocationResolver)
    }
    assert(ex.getMessage.contains("ref://orders-input"))
    assert(ex.getMessage.contains("no LocationResolver was configured"))
  }
}
