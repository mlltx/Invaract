// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.contract

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractCompatibilityTest extends AnyFunSuite {

  private def fixture(name: String): Contract =
    ContractParser.parseFile(new File(s"src/test/resources/fixtures/$name"))

  /** Rewrites the fields of every output dataset's schema, so tests can
    * isolate a single field-level change without hand-writing the
    * Contract -> Dataset -> Schema copy chain at each call site.
    */
  private def withOutputFields(contract: Contract)(f: List[Field] => List[Field]): Contract =
    contract.copy(outputs = contract.outputs.map(ds => ds.copy(schema = Schema(f(ds.schema.fields)))))

  test("diff should classify adding an optional field as MINOR") {
    val v1 = fixture("customer_orders_v1.yaml")
    val v1_1 = fixture("customer_orders_v1_1_compatible.yaml")

    val report = ContractCompatibility.diff(v1, v1_1)

    assert(!report.isBreaking)
    assert(report.requiredLevel == CompatibilityLevel.Minor)
    assert(report.changes.exists(_.description.contains("avg_order_value")))
  }

  test("diff should classify removing a field as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val v2 = fixture("customer_orders_v2_breaking.yaml")

    val report = ContractCompatibility.diff(v1, v2)

    assert(report.isBreaking)
    assert(report.breakingChanges.exists(_.description.contains("total_orders' was removed")))
  }

  test("diff should classify a type change as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val v2 = fixture("customer_orders_v2_breaking.yaml")

    val report = ContractCompatibility.diff(v1, v2)

    assert(report.breakingChanges.exists(c => c.path.endsWith("total_amount.type")))
  }

  test("diff should classify adding a new required field as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")

    val v1WithRequiredAddition =
      withOutputFields(v1)(_ :+ Field("region", "string", required = true, nullable = false))

    val report = ContractCompatibility.diff(v1, v1WithRequiredAddition)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(_.description.contains("Required field 'region'")))
  }

  test("diff should classify narrowing nullability as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")

    // Start from a variant where total_orders is nullable, then tighten it,
    // isolating the nullable -> non-nullable transition from the required flag.
    val relaxed = withOutputFields(v1)(_.map {
      case f if f.name == "total_orders" => f.copy(nullable = true, required = false)
      case f                              => f
    })
    val tightened = withOutputFields(relaxed)(_.map {
      case f if f.name == "total_orders" => f.copy(nullable = false)
      case f                              => f
    })

    val report = ContractCompatibility.diff(relaxed, tightened)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(_.path.endsWith("total_orders.nullable")))
  }

  test("diff should report no changes between identical contracts") {
    val v1 = fixture("customer_orders_v1.yaml")
    val report = ContractCompatibility.diff(v1, v1)

    assert(report.changes.isEmpty)
    assert(report.requiredLevel == CompatibilityLevel.Patch)
  }

  test("verifyVersionBump should accept a correct MINOR bump for an additive change") {
    val v1 = fixture("customer_orders_v1.yaml")
    val v1_1 = fixture("customer_orders_v1_1_compatible.yaml")

    assert(ContractCompatibility.verifyVersionBump(v1, v1_1).isEmpty)
  }

  test("verifyVersionBump should accept a correct MAJOR bump for a breaking change") {
    val v1 = fixture("customer_orders_v1.yaml")
    val v2 = fixture("customer_orders_v2_breaking.yaml")

    assert(ContractCompatibility.verifyVersionBump(v1, v2).isEmpty)
  }

  test("verifyVersionBump should flag a breaking change declared as only a PATCH bump") {
    val v1 = fixture("customer_orders_v1.yaml")
    val breakingButPatchVersioned = fixture("customer_orders_v2_breaking.yaml").copy(version = ContractVersion(1, 0, 1))

    val problems = ContractCompatibility.verifyVersionBump(v1, breakingButPatchVersioned)
    assert(problems.nonEmpty)
    assert(problems.head.contains("MAJOR"))
  }

  test("verifyVersionBump should flag an additive change declared as only a PATCH bump") {
    val v1 = fixture("customer_orders_v1.yaml")
    val additiveButPatchVersioned =
      fixture("customer_orders_v1_1_compatible.yaml").copy(version = ContractVersion(1, 0, 1))

    val problems = ContractCompatibility.verifyVersionBump(v1, additiveButPatchVersioned)
    assert(problems.nonEmpty)
    assert(problems.head.contains("MINOR"))
  }
}
