// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.{Contract, ContractVersion, Dataset, Field, Schema}

import org.apache.spark.sql.types.StructType

/** Builds a best-effort starting-point `Contract` from a real write's
  * actually-observed inputs/outputs — the inverse of what
  * `StructuralVerifier` does with an existing contract. Used by
  * `ContractEnforcementRule.dryRun` (ROADMAP.md's "dry-run mode"): rather
  * than verifying a transformation against a contract the caller supplies,
  * dry-run mode has no contract to check against at all, and instead
  * infers what one covering this exact write would look like, so a user
  * exploring a transformation for the first time has something concrete to
  * copy, edit, and turn into a real contract — see docs-site's "Dry-run
  * mode" guide.
  *
  * Deliberately narrow: this infers structure (locations, formats, save
  * mode, field names/types/nullability), never business rules — there is
  * no way to observe "this MERGE must always match on customer_id" from a
  * single execution the way `RuleType`'s rule vocabulary expresses it, so
  * `rules` is always empty in an inferred contract. A user is expected to
  * add rules by hand once they know what they want enforced.
  */
private[sparkadapter] object ContractInference {

  /** A freshly inferred contract is never something the writer intended to
    * ship as-is — `id`/`version`/`status` are placeholders a user is
    * expected to replace once they've reviewed the inferred inputs/outputs,
    * not a real identity Invaract invented on their behalf.
    */
  val InferredId = "inferred_contract"
  val InferredVersion: ContractVersion = ContractVersion(0, 1, 0)
  val InferredStatus = "draft"

  /** @param inputSchemas every recognized read this write's plan depends on
    *   (location, schema) — the same collection
    *   `ContractEnforcementRule.verifyOrThrow` gathers via `collectInputSchemas`,
    *   reused here rather than re-derived so dry-run mode and real
    *   enforcement can never disagree about what counts as an input.
    */
  def infer(writeInfo: WriteCommandInfo, inputSchemas: List[(String, StructType)]): Contract = {
    val inputs = inputSchemas.zipWithIndex.map { case ((location, schema), index) =>
      Dataset(
        name = inputName(index, inputSchemas.size),
        location = normalizeLocation(location),
        format = None, // recognizedRead only ever yields a (location, schema) pair - no format is collected alongside it
        schema = schemaOf(schema)
      )
    }
    val output = Dataset(
      name = "output",
      location = normalizeLocation(writeInfo.location),
      format = writeInfo.format,
      schema = schemaOf(writeInfo.outputSchema),
      saveMode = writeInfo.saveMode
    )
    Contract(
      id = InferredId,
      version = InferredVersion,
      status = InferredStatus,
      inputs = inputs,
      outputs = List(output),
      rules = Nil,
      extensions = Map.empty
    )
  }

  private def inputName(index: Int, total: Int): String =
    if (total <= 1) "input" else s"input_${index + 1}"

  /** Strips a local `file:` scheme prefix Spark itself adds when qualifying
    * a plan's actual location (see `WriteCommandSupport`/`SparkPlanAdapter`'s
    * location-extraction helpers), matching the bare-path form a
    * hand-authored contract normally declares (see the YAML files under
    * `demo/contracts` for real examples) — the exact same asymmetry
    * `StructuralVerifier.locationsMatch` already bridges by stripping
    * `actual`'s `"file:"` prefix before comparing it against a contract's
    * declared value. Without this, an inferred contract would declare a
    * `"file:..."`-prefixed location that `locationsMatch` never strips from
    * the *declared* side, so re-checking the inferred contract's own write
    * with `forContract` would wrongly report OUTPUT_LOCATION_MISMATCH
    * against the exact write it was inferred from — confirmed the hard way
    * by a real round-trip test failing this way before this fix, not
    * assumed. A non-file location (a table name, `s3://...`, `jdbc:...`)
    * has no `"file:"` prefix to strip, so this is a no-op for every other
    * location shape.
    */
  private def normalizeLocation(location: String): String = location.stripPrefix("file:")

  /** `required = true` for every field: unlike a hand-authored contract
    * (where "required" expresses intent the author holds independently of
    * any one run), every field here was genuinely present in the schema
    * this write actually produced/consumed — the only fact dry-run mode
    * has to go on. `nullable` instead reflects Spark's own tracked
    * nullability exactly (`StructField.nullable`), since that one *is*
    * something Spark already knows precisely, not a guess.
    */
  private def schemaOf(structType: StructType): Schema =
    Schema(structType.fields.map { field =>
      Field(
        name = field.name,
        fieldType = field.dataType.typeName,
        required = true,
        nullable = field.nullable
      )
    }.toList)
}
