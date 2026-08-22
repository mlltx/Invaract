// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.contract

/** Raised when a contract document cannot be parsed into the object model. */
class ContractParseException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Semantic version for a contract: MAJOR.MINOR.PATCH.
  *
  * Contract versioning follows the same MAJOR/MINOR/PATCH semantics as the
  * project itself (see docs/VERSIONING.md): MAJOR for breaking schema
  * changes, MINOR for backward-compatible additions, PATCH for non-schema
  * changes (description, metadata).
  */
case class ContractVersion(major: Int, minor: Int, patch: Int) extends Ordered[ContractVersion] {
  def compare(that: ContractVersion): Int = {
    if (major != that.major) major compare that.major
    else if (minor != that.minor) minor compare that.minor
    else patch compare that.patch
  }

  override def toString: String = s"$major.$minor.$patch"
}

object ContractVersion {
  private val Pattern = """^(\d+)\.(\d+)(?:\.(\d+))?$""".r

  def parse(raw: String): ContractVersion = raw.trim match {
    case Pattern(maj, min, null)  => ContractVersion(maj.toInt, min.toInt, 0)
    case Pattern(maj, min, patch) => ContractVersion(maj.toInt, min.toInt, patch.toInt)
    case other =>
      throw new ContractParseException(
        s"Invalid contract version '$other'. Expected MAJOR.MINOR[.PATCH], e.g. '1.0.0'"
      )
  }
}

/** A single field within a schema.
  *
  * Fields may nest via `properties` to represent struct/record types. A field
  * with a non-empty `properties` list is treated as a struct regardless of
  * its declared `fieldType`.
  *
  * @param required whether the field must be present (contract-level semantics)
  * @param nullable whether the field's value may be null when present
  */
case class Field(
  name: String,
  fieldType: String,
  required: Boolean = false,
  nullable: Boolean = true,
  properties: List[Field] = Nil
) {
  def isStruct: Boolean = properties.nonEmpty
}

/** An ordered collection of fields describing the shape of a dataset. */
case class Schema(fields: List[Field]) {
  def field(name: String): Option[Field] = fields.find(_.name == name)
}

/** A dataset the contract reads from (input) or writes to (output).
  *
  * @param location physical location of the dataset (table name, path, topic, etc.)
  * @param format   optional storage/serialization format (e.g. "table", "parquet", "delta")
  * @param saveMode optional expected write behavior for an output dataset
  *   toward data already present at `location` (e.g. "append", "overwrite",
  *   "ignore", "error"). Meaningless for an input dataset; only checked by
  *   `StructuralVerifier` against a plan's `Write` node.
  */
case class Dataset(
  name: String,
  location: String,
  format: Option[String],
  schema: Schema,
  saveMode: Option[String] = None
)

/** A declarative rule attached to the contract (e.g. compatibility mode,
  * quality expectation). Invariant does not interpret rule semantics in
  * Phase 1 beyond recording them; verification of rules against a
  * transformation plan is future work (see ROADMAP.md, Phase 1 scope).
  */
case class ContractRule(ruleType: String, properties: Map[String, Any])

/** The root contract object: the minimum representation required to verify
  * a transformation, modeled after the Open Data Contract Standard (ODCS).
  *
  * Invariant intentionally does not redefine concepts ODCS already
  * standardizes (schema, fields, types). Fields not recognized by Invariant
  * are preserved verbatim in `extensions` rather than rejected, so contracts
  * authored for other ODCS-based tooling remain valid inputs.
  */
case class Contract(
  id: String,
  version: ContractVersion,
  status: String,
  inputs: List[Dataset],
  outputs: List[Dataset],
  rules: List[ContractRule],
  extensions: Map[String, Any]
) {
  def input(name: String): Option[Dataset] = inputs.find(_.name == name)
  def output(name: String): Option[Dataset] = outputs.find(_.name == name)
}
