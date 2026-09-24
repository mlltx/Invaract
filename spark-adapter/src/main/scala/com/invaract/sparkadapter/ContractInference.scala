// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractVersion, Dataset, Field, Schema}
import com.invaract.ir.{Lineage, Plan}

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
    * @param plan the write's translated `ir.Plan` (`ContractEnforcementRule.inferOrIgnore`
    *   runs the exact same `SparkPlanAdapter.translate` real enforcement
    *   does), used only to observe each input's usage (see
    *   `observedUsageDescription`'s own doc) — never to assert a declared
    *   `Dataset.datasetType`, which stays `None` for every inferred
    *   dataset regardless of what's observed (see this object's own doc
    *   section 8 of the Input/Output Types spec: an inferred contract
    *   represents observed implementation behavior, not proof of a
    *   semantic role).
    */
  def infer(writeInfo: WriteCommandInfo, inputSchemas: List[(String, StructType)], plan: Plan): Contract = {
    val totalInputs = inputSchemas.size
    val outputContributingQualifiers = Lineage.trace(plan).flatMap(_.sources).flatMap(_.qualifier).toSet
    val conditionReferencedQualifiers = PlanRuleVerifier.collectConditionReferences(plan).flatMap(_.qualifier)
    val inputs = inputSchemas.zipWithIndex.map { case ((location, schema), index) =>
      val normalizedLocation = normalizeLocation(location)
      Dataset(
        name = inputName(index, totalInputs),
        location = normalizedLocation,
        format = None, // recognizedRead only ever yields a (location, schema) pair - no format is collected alongside it
        schema = schemaOf(schema),
        description = observedUsageDescription(normalizedLocation, outputContributingQualifiers, conditionReferencedQualifiers)
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

  /** A human-readable note on what this input was actually seen doing in
    * the transformation — the type-aware half of dry-run generation (see
    * docs/CONTRACT_MODEL.md's "Input and Output Types" section, "Dry-run
    * contract generation"). Deliberately reuses `Dataset.description`
    * (already documentary-only, never checked by any verifier) instead of
    * inventing new structure for it, and deliberately stops short of
    * setting `Dataset.datasetType`: dry-run analysis can establish that an
    * object was *read and used to filter*, but "cannot necessarily
    * establish that the object is organisationally defined as a CONTROL"
    * (the spec's own example) — the distinction between what the
    * implementation demonstrates and what a contract declares must be
    * retained, so this only ever writes an observation for a human to
    * review, never a proven classification.
    *
    * `matches` (a location's qualifier set) mirrors `SensitivityLineage`'s
    * own `StructuralVerifier.locationsMatch`-based resolution — a
    * `ColumnRef.qualifier` is either a `Read`'s alias or its dataset's raw
    * (un-normalized) location (see `ir.Lineage.resolveInScopeT`'s `Read`
    * case), so matching against this already-normalized `location` needs
    * the same normalization-aware comparison, not a bare string `==`.
    */
  private def observedUsageDescription(
      location: String,
      outputContributingQualifiers: Set[String],
      conditionReferencedQualifiers: Set[String]
  ): Option[String] = {
    def matches(qualifiers: Set[String]): Boolean = qualifiers.exists(q => StructuralVerifier.locationsMatch(location, q))
    if (matches(outputContributingQualifiers))
      Some("Observed: contributes to at least one produced output column.")
    else if (matches(conditionReferencedQualifiers))
      Some(
        "Observed: referenced only in a Filter/Join condition; never observed contributing to a produced output " +
          "column. Review whether this input's role is CONTROL rather than DATA_ASSET/SOURCE."
      )
    else
      None
  }

  /** Delegates to `StructuralVerifier.normalizeSparkLocation` rather than
    * stripping `"file:"` independently here — see that method's own doc
    * for why the two must share one definition, not two copies that could
    * silently drift apart.
    */
  private def normalizeLocation(location: String): String = StructuralVerifier.normalizeSparkLocation(location)

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
