// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.CatalogRequirement
import com.invaract.ir.CatalogIdentity
import com.invaract.verification.SchemaChecker.Side

import org.scalatest.funsuite.AnyFunSuite

class CatalogCheckerSpec extends AnyFunSuite {

  private def req(
      required: Boolean = true,
      technology: Option[String] = None,
      catalogName: Option[String] = None,
      location: Option[String] = None,
      namespace: List[String] = Nil,
      table: Option[String] = None
  ) = CatalogRequirement(required, technology, catalogName, location, namespace, table)

  private def identity(
      technology: Option[String] = None,
      catalogName: Option[String] = None,
      location: Option[String] = None,
      namespace: List[String] = Nil,
      table: Option[String] = None
  ) = CatalogIdentity(technology = technology, catalogName = catalogName, location = location, namespace = namespace, table = table)

  test("required: false is informational - never a violation, whatever the actual registration is") {
    assert(CatalogChecker.check(req(required = false, technology = Some("iceberg")), None, "p", Side.Output).isEmpty)
    assert(CatalogChecker.check(req(required = false, technology = Some("iceberg")), Some(identity(technology = Some("hive"))), "p", Side.Output).isEmpty)
  }

  test("no catalog registration at all: MISSING_*_CATALOG_REGISTRATION, typed and worded per side") {
    val out = CatalogChecker.check(req(), None, "warehouse/t", Side.Output)
    val in = CatalogChecker.check(req(), None, "warehouse/t", Side.Input)
    assert(out.map(_.violationType) == List(ViolationType.MissingOutputCatalogRegistration))
    assert(in.map(_.violationType) == List(ViolationType.MissingInputCatalogRegistration))
    assert(out.head.message == "contract requires the output at 'warehouse/t' to be registered in a catalog, but it has no catalog registration")
    assert(in.head.message.startsWith("contract requires the input at 'warehouse/t'"))
    assert(out.head.location.contains("warehouse/t"))
    assert(out.head.remediation.startsWith("Register 'warehouse/t' in a catalog"))
  }

  test("a registration that agrees on every declared sub-field is clean") {
    val r = req(technology = Some("iceberg"), catalogName = Some("prod"), namespace = List("db"), table = Some("t"))
    val actual = identity(technology = Some("ICEBERG"), catalogName = Some("prod"), namespace = List("db"), table = Some("t"))
    assert(CatalogChecker.check(r, Some(actual), "p", Side.Output).isEmpty) // technology compared case-insensitively
  }

  test("a mismatch is one violation per side, naming every disagreeing sub-field with both values") {
    val r = req(technology = Some("iceberg"), catalogName = Some("prod"), namespace = List("db"), table = Some("t"))
    val actual = identity(technology = Some("hive"), catalogName = Some("dev"), namespace = List("other"), table = Some("u"))
    val out = CatalogChecker.check(r, Some(actual), "p", Side.Output)
    val in = CatalogChecker.check(r, Some(actual), "p", Side.Input)
    assert(out.map(_.violationType) == List(ViolationType.OutputCatalogMismatch))
    assert(in.map(_.violationType) == List(ViolationType.InputCatalogMismatch))
    assert(
      out.head.message == "contract's declared catalog registration for the output at 'p' does not match the actual registration: " +
        "technology (expected 'iceberg', actual 'hive'); catalogName (expected 'prod', actual 'dev'); " +
        "namespace (expected 'db', actual 'other'); table (expected 't', actual 'u')"
    )
    assert(in.head.message.contains("for the input at 'p'"))
    assert(out.head.expected.contains("technology=iceberg, catalogName=prod, namespace=db, table=t"))
    assert(out.head.actual.contains("technology=hive, catalogName=dev, namespace=other, table=u"))
    assert(out.head.location.contains("p"))
  }

  test("only sub-fields the contract declares AND the actual side reports are compared") {
    // contract pins nothing the actual lacks, and the actual has extra identity the contract never asked about:
    assert(CatalogChecker.catalogFieldMismatches(req(technology = Some("iceberg")), identity(technology = Some("iceberg"), catalogName = Some("x"), table = Some("t"))).isEmpty)
    // contract declares a sub-field the actual cannot report (e.g. location for a DSv2 table): not a mismatch
    assert(CatalogChecker.catalogFieldMismatches(req(location = Some("s3://x")), identity()).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(catalogName = Some("prod")), identity()).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(table = Some("t")), identity()).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(technology = Some("iceberg")), identity()).isEmpty)
    // namespace is compared only when both sides are non-empty:
    assert(CatalogChecker.catalogFieldMismatches(req(namespace = List("a")), identity()).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(), identity(namespace = List("a"))).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(namespace = List("a", "b")), identity(namespace = List("a", "b"))).isEmpty)
    assert(CatalogChecker.catalogFieldMismatches(req(namespace = List("a", "b")), identity(namespace = List("a"))) ==
      List("namespace (expected 'a.b', actual 'a')"))
    // location compared when both report one:
    assert(CatalogChecker.catalogFieldMismatches(req(location = Some("s3://x")), identity(location = Some("s3://y"))) ==
      List("location (expected 's3://x', actual 's3://y')"))
    assert(CatalogChecker.catalogFieldMismatches(req(location = Some("s3://x")), identity(location = Some("s3://x"))).isEmpty)
  }

  test("describe helpers list only the sub-fields that are present, in a fixed order") {
    assert(CatalogChecker.describeCatalogRequirement(req()) == "")
    assert(
      CatalogChecker.describeCatalogRequirement(req(technology = Some("t"), catalogName = Some("c"), location = Some("l"), namespace = List("a", "b"), table = Some("x"))) ==
        "technology=t, catalogName=c, location=l, namespace=a.b, table=x"
    )
    assert(CatalogChecker.describeCatalogIdentity(identity()) == "")
    assert(
      CatalogChecker.describeCatalogIdentity(identity(technology = Some("t"), catalogName = Some("c"), location = Some("l"), namespace = List("a", "b"), table = Some("x"))) ==
        "technology=t, catalogName=c, location=l, namespace=a.b, table=x"
    )
    assert(CatalogChecker.describeCatalogIdentity(identity(table = Some("x"))) == "table=x")
  }
}
