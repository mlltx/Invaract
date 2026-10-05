// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.CatalogRequirement
import com.invaract.ir.CatalogIdentity

/** Checks a dataset's actual data-catalog registration against the contract's
  * declared `catalog:` requirement — one of the structural checks
  * `StructuralVerifier.verify` runs, for inputs and outputs alike, extracted so
  * it can be read, tested and mutation-tested on its own (see
  * docs/SPARK_ADAPTER.md's "`StructuralVerifier` internals").
  *
  * Opt-in per dataset: a dataset whose contract entry declares no `catalog`, or
  * `required: false`, is never checked. `MISSING_*_CATALOG_REGISTRATION` means the
  * read/write has no catalog registration at all (a bare path); `*_CATALOG_MISMATCH`
  * means one exists but disagrees with what the contract declares.
  */
private[sparkadapter] object CatalogChecker {

  /** Checks one dataset's actual `CatalogIdentity` against the contract's
    * declared `CatalogRequirement` — shared by both input and output
    * checking, the same "one rule set applied twice" pattern `SchemaChecker`
    * uses for schema (and the same `SchemaChecker.Side`). `req.required == false` means the
    * contract declares an *expected* shape without gating on it
    * (informational only, accepted by `ContractValidator`) — never a
    * violation on its own, matching the plan's documented convention.
    */
  def check(
    req: CatalogRequirement,
    actual: Option[CatalogIdentity],
    location: String,
    side: SchemaChecker.Side
  ): List[Violation] = {
    if (!req.required) Nil
    else
      actual match {
        case None =>
          List(
            Violation(
              if (side == SchemaChecker.Side.Input) ViolationType.MissingInputCatalogRegistration
              else ViolationType.MissingOutputCatalogRegistration,
              s"contract requires the ${side.noun} at '$location' to be registered in a catalog, but it has no catalog registration",
              remediation =
                s"Register '$location' in a catalog (e.g. CREATE EXTERNAL TABLE, .saveAsTable(), or a DSv2 catalog read/write) instead of a bare path, or set catalog.required to false in the contract if registration isn't actually required.",
              location = Some(location)
            )
          )
        case Some(actualCatalog) =>
          val mismatches = catalogFieldMismatches(req, actualCatalog)
          if (mismatches.isEmpty) Nil
          else
            List(
              Violation(
                if (side == SchemaChecker.Side.Input) ViolationType.InputCatalogMismatch else ViolationType.OutputCatalogMismatch,
                s"contract's declared catalog registration for the ${side.noun} at '$location' does not match the actual registration: ${mismatches
                  .mkString("; ")}",
                remediation =
                  s"Update the catalog registration for '$location' to match the contract's declared catalog fields, or update the contract if this change is intentional.",
                location = Some(location),
                expected = Some(describeCatalogRequirement(req)),
                actual = Some(describeCatalogIdentity(actualCatalog))
              )
            )
      }
  }

  /** Compares only the sub-fields the contract actually declares AND that
    * the actual side reports a real value for — the same both-sides-known
    * convention `formatViolation`/`saveModeViolation` in `verify` already
    * use for the overall `format`/`saveMode` check, applied per sub-field
    * here. Two independent reasons a sub-field can be left uncompared:
    * a `CatalogRequirement` that only pins `technology` doesn't
    * spuriously fail over an unrelated `catalogName`/`location` the
    * contract author never asked to check (`req`'s side is `None`); and
    * `location` specifically is a real, disclosed capability gap for
    * every DSv2 connector today (`ir.CatalogIdentity.location` is always
    * `None` for Delta/Iceberg/JDBC - see `CatalogIdentitySupport.fromV2`'s
    * own doc) - comparing it there would make declaring `catalog.location`
    * against such an output permanently, unfixably fail regardless of the
    * value chosen, exactly the false-rejection-on-unknown-information risk
    * `formatViolation`'s own doc already warns against (`actual`'s side is
    * `None`). Confirmed by a real Delta enforcement test, not assumed:
    * without this, every DSv2 catalog write that ever declares a
    * `catalog.location` requirement would be un-satisfiable.
    */
  private[sparkadapter] def catalogFieldMismatches(req: CatalogRequirement, actual: CatalogIdentity): List[String] = {
    val technology = req.technology.flatMap(expected =>
      actual.technology
        .filterNot(_.equalsIgnoreCase(expected))
        .map(actualValue => s"technology (expected '$expected', actual '$actualValue')")
    )
    val catalogName = req.catalogName.flatMap(expected =>
      actual.catalogName
        .filterNot(_ == expected)
        .map(actualValue => s"catalogName (expected '$expected', actual '$actualValue')")
    )
    val location = req.location.flatMap(expected =>
      actual.location
        .filterNot(_ == expected)
        .map(actualValue => s"location (expected '$expected', actual '$actualValue')")
    )
    val namespace =
      if (req.namespace.nonEmpty && actual.namespace.nonEmpty && req.namespace != actual.namespace)
        Some(s"namespace (expected '${req.namespace.mkString(".")}', actual '${actual.namespace.mkString(".")}')")
      else None
    val table = req.table.flatMap(expected =>
      actual.table
        .filterNot(_ == expected)
        .map(actualValue => s"table (expected '$expected', actual '$actualValue')")
    )

    List(technology, catalogName, location, namespace, table).flatten
  }

  private[sparkadapter] def describeCatalogRequirement(req: CatalogRequirement): String =
    List(
      req.technology.map(t => s"technology=$t"),
      req.catalogName.map(c => s"catalogName=$c"),
      req.location.map(l => s"location=$l"),
      if (req.namespace.nonEmpty) Some(s"namespace=${req.namespace.mkString(".")}") else None,
      req.table.map(t => s"table=$t")
    ).flatten.mkString(", ")

  private[sparkadapter] def describeCatalogIdentity(actual: CatalogIdentity): String =
    List(
      actual.technology.map(t => s"technology=$t"),
      actual.catalogName.map(c => s"catalogName=$c"),
      actual.location.map(l => s"location=$l"),
      if (actual.namespace.nonEmpty) Some(s"namespace=${actual.namespace.mkString(".")}") else None,
      actual.table.map(t => s"table=$t")
    ).flatten.mkString(", ")
}
