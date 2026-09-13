// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

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

  /** Same idea as `withOutputFields`, one level up - rewrites each output
    * `Dataset` itself (format/saveMode/catalog), not just its schema.
    */
  private def withOutputs(contract: Contract)(f: Dataset => Dataset): Contract =
    contract.copy(outputs = contract.outputs.map(f))

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

  // --- format/saveMode/catalog: previously an undocumented, unaddressed
  // gap (docs/CONTRACT_MODEL.md's "Version Compatibility" section used to
  // call this out explicitly) - a contract that changed only its declared
  // format, save mode, or catalog registration requirement was invisible
  // to this whole check. Closed by mirroring the exact asymmetric
  // philosophy the schema diff above already uses: newly declaring (or
  // changing) a constraint that a real write must now match is BREAKING;
  // only loosening/removing one is not flagged, the same way a field
  // going from required to optional isn't flagged today.

  test("diff should classify newly declaring 'format' as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val noFormat = withOutputs(v1)(_.copy(format = None))

    val report = ContractCompatibility.diff(noFormat, v1)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(c => c.path.endsWith(".format") && c.description.contains("now required to be 'table'")))
  }

  test("diff should classify changing 'format' to a different value as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val parquet = withOutputs(v1)(_.copy(format = Some("parquet")))

    val report = ContractCompatibility.diff(v1, parquet)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(c => c.path.endsWith(".format") && c.description.contains("changed from 'table' to 'parquet'")))
  }

  test("diff should NOT flag removing a 'format' declaration (loosening, not tightening)") {
    val v1 = fixture("customer_orders_v1.yaml")
    val noFormat = withOutputs(v1)(_.copy(format = None))

    val report = ContractCompatibility.diff(v1, noFormat)
    assert(!report.changes.exists(_.path.endsWith(".format")))
  }

  test("diff should classify changing 'saveMode' to a different value as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val append = withOutputs(v1)(_.copy(saveMode = Some("append")))

    val report = ContractCompatibility.diff(v1, append)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(c => c.path.endsWith(".saveMode") && c.description.contains("changed from 'overwrite' to 'append'")))
  }

  test("diff should NOT flag removing a 'saveMode' declaration (loosening, not tightening)") {
    val v1 = fixture("customer_orders_v1.yaml")
    val noSaveMode = withOutputs(v1)(_.copy(saveMode = None))

    val report = ContractCompatibility.diff(v1, noSaveMode)
    assert(!report.changes.exists(_.path.endsWith(".saveMode")))
  }

  test("diff should classify newly requiring catalog registration as BREAKING") {
    val v1 = fixture("customer_orders_v1.yaml")
    val catalogRequired = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("hive")))))

    val report = ContractCompatibility.diff(v1, catalogRequired)
    assert(report.isBreaking)
    assert(report.breakingChanges.exists(c => c.path.endsWith(".catalog") && c.description.contains("now required")))
  }

  test("diff should NOT flag declaring catalog.required=false (informational only, gates nothing)") {
    val v1 = fixture("customer_orders_v1.yaml")
    val informationalOnly = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = false, technology = Some("hive")))))

    val report = ContractCompatibility.diff(v1, informationalOnly)
    assert(!report.changes.exists(_.path.endsWith(".catalog")))
  }

  test("diff should classify a changed sub-field on an already-required catalog block as BREAKING, naming the changed field") {
    val v1 = fixture("customer_orders_v1.yaml")
    val hive = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("hive")))))
    val delta = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("delta")))))

    val report = ContractCompatibility.diff(hive, delta)
    assert(report.isBreaking)
    val change = report.breakingChanges.find(_.path.endsWith(".catalog"))
      .getOrElse(fail(s"expected a .catalog change, got: ${report.changes}"))
    assert(change.description.contains("technology"))
  }

  test("diff should NOT flag removing a required catalog block entirely (loosening, not tightening)") {
    val v1 = fixture("customer_orders_v1.yaml")
    val catalogRequired = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("hive")))))

    val report = ContractCompatibility.diff(catalogRequired, v1)
    assert(!report.changes.exists(_.path.endsWith(".catalog")))
  }

  test("diff should NOT flag relaxing an already-required catalog block to required=false") {
    val v1 = fixture("customer_orders_v1.yaml")
    val required = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("hive")))))
    val relaxed = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = false, technology = Some("hive")))))

    val report = ContractCompatibility.diff(required, relaxed)
    assert(!report.changes.exists(_.path.endsWith(".catalog")))
  }

  test("verifyVersionBump should flag a newly-required catalog block declared as only a MINOR bump") {
    val v1 = fixture("customer_orders_v1.yaml")
    val catalogRequired = withOutputs(v1)(_.copy(catalog = Some(CatalogRequirement(required = true, technology = Some("hive")))))
      .copy(version = ContractVersion(1, 1, 0))

    val problems = ContractCompatibility.verifyVersionBump(v1, catalogRequired)
    assert(problems.nonEmpty)
    assert(problems.head.contains("MAJOR"))
  }
}
