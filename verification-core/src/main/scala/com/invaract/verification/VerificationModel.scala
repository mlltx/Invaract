// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{CatalogRequirement, Contract, Dataset, DatasetType}
import com.invaract.fingerprint.TransformationFingerprint
import com.invaract.ir.{Plan, Read, UnknownPlan, Write}

/** One structural rule a plan violated, relative to a contract. `column`,
  * `location`, `expected`, and `actual` are populated only where relevant
  * to `violationType` — see `ViolationType` for which fields each type
  * carries. `remediation` is always present: a concrete, actionable next
  * step, not just a restatement of `message` — see ROADMAP.md Phase 5's
  * requirement that a developer understand not only what and why, but how
  * to correct the transformation.
  */
case class Violation(
  violationType: String,
  message: String,
  remediation: String,
  column: Option[String] = None,
  location: Option[String] = None,
  expected: Option[String] = None,
  actual: Option[String] = None,
  rule: Option[String] = None
) {
  def toMap: Map[String, Any] =
    Map("type" -> violationType, "message" -> message, "remediation" -> remediation) ++
      column.map("column" -> _) ++
      location.map("location" -> _) ++
      expected.map("expected" -> _) ++
      actual.map("actual" -> _) ++
      rule.map("rule" -> _)
}

/** The violation type vocabulary `StructuralVerifier` produces. Plain
  * string constants rather than a sealed trait: violations cross a JSON
  * boundary (`demo/output/report.json`) as their final destination, so a
  * closed Scala ADT would just need converting straight back to these same
  * strings.
  */
object ViolationType {
  val MissingInput = "MISSING_INPUT"
  val UndeclaredInput = "UNDECLARED_INPUT"
  val MissingInputField = "MISSING_INPUT_FIELD"
  val UndeclaredInputColumn = "UNDECLARED_INPUT_COLUMN"
  val InputFieldTypeMismatch = "INPUT_FIELD_TYPE_MISMATCH"
  val InputFieldNullabilityMismatch = "INPUT_FIELD_NULLABILITY_MISMATCH"

  val MissingOutput = "MISSING_OUTPUT"
  val OutputLocationMismatch = "OUTPUT_LOCATION_MISMATCH"
  val OutputFormatMismatch = "OUTPUT_FORMAT_MISMATCH"
  val OutputSaveModeMismatch = "OUTPUT_SAVE_MODE_MISMATCH"
  val MissingOutputField = "MISSING_OUTPUT_FIELD"
  val UndeclaredOutputColumn = "UNDECLARED_OUTPUT_COLUMN"
  val OutputFieldTypeMismatch = "OUTPUT_FIELD_TYPE_MISMATCH"
  val OutputFieldNullabilityMismatch = "OUTPUT_FIELD_NULLABILITY_MISMATCH"

  /** Only produced when a dataset's contract entry declares
    * `catalog: { required: true }` — see `Dataset.catalog`/
    * `CatalogRequirement`, docs/CONTRACT_MODEL.md. A dataset with no
    * `catalog` declared, or `required: false`, is never checked at all
    * (opt-in, per dataset). "Missing" here means the write/read has no
    * catalog registration at all (e.g. a bare `.parquet(path)`/`.save(path)`
    * with no `CatalogTable`) — distinct from `OutputCatalogMismatch` below,
    * which means a registration exists but disagrees with the contract's
    * declared technology/catalogName/location/namespace/table.
    */
  val MissingOutputCatalogRegistration = "MISSING_OUTPUT_CATALOG_REGISTRATION"
  val OutputCatalogMismatch = "OUTPUT_CATALOG_MISMATCH"

  /** Input-side mirrors of the two above — same opt-in-per-dataset
    * semantics, checked against `contract.inputs`' `catalog` field instead.
    */
  val MissingInputCatalogRegistration = "MISSING_INPUT_CATALOG_REGISTRATION"
  val InputCatalogMismatch = "INPUT_CATALOG_MISMATCH"

  /** Not produced by `StructuralVerifier` itself — this is
    * `ContractEnforcementRule`'s fail-closed response when a Spark command
    * looks like it writes or otherwise mutates data (it's `Command`-shaped
    * and not on the known-safe list) but `SparkPlanAdapter` has no
    * translation for it, so it was never actually checked against the
    * contract. See `ContractEnforcementRule`'s "Fail-closed on unverifiable
    * writes" doc.
    */
  val UnverifiableWrite = "UNVERIFIABLE_WRITE"

  /** Not produced by `StructuralVerifier` itself — `ContractEnforcementRule`
    * produces this when `ContractValidator.validate` finds the contract
    * itself structurally unsound (e.g. no declared outputs) before any
    * plan is even checked against it. Found via a real crash: a contract
    * missing `outputs` entirely used to reach `StructuralVerifier.verify`
    * unvalidated and fail with an unguarded `NoSuchElementException` at
    * `contract.outputs.head` (the pre-multi-output-matching lookup this
    * module used before the "Multi-output contracts" behavior described on
    * `verify` below existed), rather than a clean, actionable rejection.
    */
  val InvalidContract = "INVALID_CONTRACT"

  /** Produced by `CapabilityCheck` (via `VerificationPipeline`) - the contract relies
    * on something the engine adapter's own capability declaration says it cannot
    * verify (`Support.Unsupported` on a capability that `enforcesContract`), so
    * passing the write would represent an unchecked requirement as checked. Fail
    * closed: the write is rejected and the message names the capability, why the
    * contract needs it, and the adapter's stated reason. `expected` is the
    * capability id; `actual` says which adapter declared it unsupported. See
    * docs/MULTI_ENGINE_ADAPTERS.md, Stage 3.
    */
  val UnsupportedContractFeature = "UNSUPPORTED_CONTRACT_FEATURE"

  /** Produced by `RuleVerifier`, not `StructuralVerifier` — a MERGE's `ON`
    * condition doesn't reference every column a `merge_condition` rule
    * declares.
    */
  val RuleMergeConditionViolation = "RULE_MERGE_CONDITION_VIOLATION"

  /** Produced by `RuleVerifier` — a DELETE (or DSv2 `DeleteFromTable`)
    * deletes every row it reaches, with no filtering predicate, under a
    * contract declaring `forbid_unconditional_delete`.
    */
  val RuleUnconditionalDelete = "RULE_UNCONDITIONAL_DELETE"

  /** Produced by `RuleVerifier` — a standalone UPDATE assigns a column
    * outside an `allowed_update_columns` rule's declared list.
    */
  val RuleDisallowedUpdateColumn = "RULE_DISALLOWED_UPDATE_COLUMN"

  /** Not produced by `StructuralVerifier`/`RuleVerifier` — `ContractEnforcementRule`
    * produces this when a `spark.invaract.orgPolicy`-configured organizational
    * policy (`com.invaract.contract.OrgPolicy`) rejects the contract itself,
    * independent of any transformation: the contract doesn't satisfy a
    * platform-wide rule (e.g. every output must be catalog-registered) that
    * applies regardless of who authored the contract. Checked once, eagerly,
    * at session-build time — before any plan is even analyzed — since a
    * policy rule depends only on the contract's own declared shape, never on
    * what a job actually writes. `message`/`remediation` name the specific
    * policy rule (by id) and dataset involved; there is deliberately no
    * further sub-vocabulary the way structural violations have one per
    * check, since a policy document's own `id`/`description` already carry
    * that specificity (see docs/CONTRACT_MODEL.md's "Organizational Policy"
    * section).
    */
  val OrgPolicyViolation = "ORG_POLICY_VIOLATION"

  /** Not produced by `RuleVerifier` — `ContractEnforcementRule`'s
    * fail-closed response when a plan is genuinely row-level DML of a
    * kind the active contract declares a rule for (`merge_condition`/
    * `forbid_unconditional_delete`/`allowed_update_columns`), but
    * `RowMutationSupport` couldn't extract the fact that rule needs to
    * check (`RowMutationSupport.Classification.Unverifiable`) — a future
    * Delta version renaming a reflected method, or Iceberg's
    * merge-on-read UPDATE, whose rewritten plan has no per-column
    * before/after pairing to compare. Mirrors `UnverifiableWrite`'s
    * "unverifiable, not passed" principle, scoped to DML rule checking
    * specifically rather than the write as a whole.
    */
  val RuleUnverifiableDml = "RULE_UNVERIFIABLE_DML"

  /** Produced by `PlanRuleVerifier` — no `Aggregate` node anywhere in the
    * plan groups by every column a `required_group_by` rule declares.
    */
  val RuleRequiredGroupByViolation = "RULE_REQUIRED_GROUP_BY_VIOLATION"

  /** Produced by `PlanRuleVerifier` — the plan contains a cartesian-product
    * join (a `CROSS JOIN`, or any join with no condition at all) under a
    * contract declaring `forbid_cross_join`.
    */
  val RuleCrossJoinViolation = "RULE_CROSS_JOIN_VIOLATION"

  /** Produced by `PlanRuleVerifier` — no `Join` node's condition anywhere
    * in the plan establishes an equality match on every column a
    * `required_join_columns` rule declares.
    */
  val RuleRequiredJoinColumnsViolation = "RULE_REQUIRED_JOIN_COLUMNS_VIOLATION"

  /** Produced by `PlanRuleVerifier` — no `Filter` node anywhere in the plan
    * references a column a `required_filter_columns` rule declares.
    */
  val RuleRequiredFilterColumnsViolation = "RULE_REQUIRED_FILTER_COLUMNS_VIOLATION"

  /** Produced by `StaticDataQualityVerifier` — the transformation's own
    * semantics *prove* an output field's declared `nullable`/`constraints`
    * property cannot hold (`DataQualityVerdict.Violated`), e.g. a filter's
    * negation guarantees a column excluded by an `equals`/`oneOf`
    * constraint can still reach the output. Only `Violated` ever becomes a
    * `Violation` — `NotGuaranteed`/`NotStaticallyVerifiable` are reported
    * (`VerificationResult.dataQuality`) but never block a write, since
    * static analysis proving nothing is not the same as static analysis
    * proving a violation (see docs/STATIC_DATA_QUALITY_VERIFICATION.md's
    * conservatism principle).
    */
  val DataQualityViolation = "DATA_QUALITY_VIOLATION"

  /** Produced by `RoleConsistencyVerifier` — a high-confidence contradiction
    * between a dataset's declared `DatasetType` and its observed role in the
    * transformation (see docs/CONTRACT_MODEL.md's "Input and Output Types"
    * section). Only `RoleConformanceVerdict.Contradicts` ever becomes a
    * `Violation`; `Conforms`/`CannotDetermine` are reported
    * (`VerificationResult.roleConformance`) but never block a write — the
    * same "only the verdict that proves a violation blocks, everything else
    * is informational" split `DataQualityViolation` already establishes for
    * static data-quality checks.
    */
  val RoleConsistencyViolation = "ROLE_CONSISTENCY_VIOLATION"
}

/** The three-state verdict `RoleConsistencyVerifier` reaches for one
  * dataset's declared `com.invaract.contract.DatasetType` against its
  * observed role in the transformation — the spec's own vocabulary
  * (docs/CONTRACT_MODEL.md's "Input and Output Types" section, "Unknown /
  * Unprovable Cases"): "the important requirement is that validation
  * results can distinguish between Conforms, Contradicts, Cannot
  * determine." Mirrors `DataQualityVerdict`'s own three-or-four-state shape
  * and its reasoning for never collapsing to a plain pass/fail: an
  * unproven semantic property must never be represented as proven.
  */
sealed trait RoleConformanceVerdict
object RoleConformanceVerdict {

  /** The dataset's observed usage is consistent with its declared type —
    * e.g. a `CONTROL`-typed input referenced only in a `Filter`/`Join`
    * condition, never contributing to a produced output column.
    */
  case object Conforms extends RoleConformanceVerdict

  /** The dataset's observed usage directly contradicts its declared type —
    * becomes a `Violation` (`ViolationType.RoleConsistencyViolation`) and
    * blocks the write, the same as any other structural violation. Reserved
    * for the spec's own high-confidence example: a `CONTROL`-typed input
    * whose data reaches a produced output column, i.e. it is substantive
    * business data despite being declared pipeline-only.
    */
  case object Contradicts extends RoleConformanceVerdict

  /** The transformation's structure alone cannot establish whether the
    * declared type is correct — e.g. a `DATA_ASSET`/`SOURCE`-typed input
    * observed only in `Filter`/`Join` conditions, never in output-column
    * lineage: genuinely ambiguous (a join-only input can still gate which
    * rows survive without any column of its own deriving into the output),
    * so this is surfaced for a human to review, never asserted as a
    * violation — never blocks a write.
    */
  case object CannotDetermine extends RoleConformanceVerdict
}

/** One role-consistency check `RoleConsistencyVerifier` performed for a
  * single dataset against its declared `datasetType` — `detail` is a short,
  * human-readable rendering of what was observed, the same role
  * `DataQualityCheckResult.constraint` plays.
  */
case class RoleConformanceCheckResult(
  dataset: String,
  datasetType: DatasetType,
  verdict: RoleConformanceVerdict,
  detail: String
) {

  /** `verdict` rendered as its bare case-object name — the same
    * plain-string-across-a-JSON-boundary convention `DataQualityCheckResult.toMap`
    * already uses.
    */
  def toMap: Map[String, Any] =
    Map("dataset" -> dataset, "datasetType" -> datasetType.name, "verdict" -> verdict.toString, "detail" -> detail)
}

/** The four-state verdict `StaticDataQualityVerifier` reaches for one
  * output field against one of its contract-declared static properties
  * (`nullable: false`, or a `FieldConstraint`) — see
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md §2.1. Deliberately never
  * collapsed to pass/fail: `NotGuaranteed` and `NotStaticallyVerifiable`
  * both mean "verification did not block this write," but they are not the
  * same claim, and `VerificationResult.dataQuality` keeps them distinct so
  * a human or downstream tool can tell "the transformation might still
  * violate this at runtime" apart from "this needs a runtime DQ check,
  * static analysis has nothing to say about it at all."
  */
sealed trait DataQualityVerdict
object DataQualityVerdict {

  /** The transformation's own semantics prove this property holds for
    * every possible row that reaches the output — no runtime check needed.
    */
  case object Guaranteed extends DataQualityVerdict

  /** Static analysis could not prove the property holds, but also found no
    * proof that it's violated. The common, honest "I don't know" result —
    * covers everything from "this column merely passes through a filter
    * that happens not to narrow it" to "this depends on input data static
    * analysis correctly refuses to assume anything about." Runtime DQ
    * checking remains necessary.
    */
  case object NotGuaranteed extends DataQualityVerdict

  /** The transformation's own semantics prove this property CANNOT hold —
    * becomes a `Violation` (`ViolationType.DataQualityViolation`) and
    * blocks the write, the same as any other structural violation.
    */
  case object Violated extends DataQualityVerdict

  /** The output column's derivation involves something this analysis
    * deliberately does not attempt to reason about (a UDF, a window
    * function, an aggregate, ...) — distinct from `NotGuaranteed` so a
    * consumer can tell "nothing to prove" apart from "declined to even
    * try, this needs a real runtime check."
    */
  case object NotStaticallyVerifiable extends DataQualityVerdict
}

/** One field-level static data-quality check `StaticDataQualityVerifier`
  * performed against an output's contract-declared `nullable`/`constraints`
  * — `constraint` is a short, human-readable rendering of what was checked
  * (e.g. `"NOT NULL"`, `"IN (ACTIVE, INACTIVE)"`, `">= 0"`), not a
  * machine-parseable encoding; a caller that needs the underlying shape
  * already has it via `Contract.output(...).schema.field(field)`.
  */
case class DataQualityCheckResult(field: String, constraint: String, verdict: DataQualityVerdict) {

  /** `verdict` rendered as its bare case-object name (`"Guaranteed"`,
    * `"NotGuaranteed"`, `"Violated"`, `"NotStaticallyVerifiable"`) — the
    * same plain-string-across-a-JSON-boundary convention `Violation.toMap`
    * already uses for `violationType`, so a sink or `report.json` consumer
    * never needs to reflect on the Scala type itself.
    */
  def toMap: Map[String, Any] = Map("field" -> field, "constraint" -> constraint, "verdict" -> verdict.toString)
}

/** One declared input `StructuralVerifier.verify` could not confirm was
  * read, but also could not confidently report as `MissingInput` — the
  * checked plan still contains a lineage boundary (`.checkpoint()` or a
  * cached relation) that `SparkPlanAdapter` could not resolve back to the
  * plan it was made from (see `CheckpointRegistry`: an origin the check
  * rule never saw, an evicted one, or an ambiguous one), so a real read of
  * this input may be hidden behind it rather than genuinely absent.
  * Spark's own analyzed plan no longer retains the original source(s) read
  * before such a boundary, so `collectReads` can find no matching `Read`
  * node even though the input really was read.
  *
  * Only *unresolved* boundaries count: a checkpoint whose origin the
  * registry resolved has its real `Read` nodes spliced back into the plan,
  * so an input read through it is simply read, and one that isn't is a
  * plain `MissingInput` — an unrelated, fully-resolved checkpoint elsewhere
  * in the plan can never excuse a genuinely missing input. Any other
  * `UnknownPlan` (a node the translator merely has no case for) doesn't
  * count either: it is not evidence a *read* was hidden.
  *
  * Report-only, the same `Conforms`/`CannotDetermine`-style convention
  * `RoleConformanceCheckResult` already uses for "verification declined to
  * assert a fact it can't actually prove": an `UnverifiableInput` entry
  * never becomes a `Violation` and never affects `passed`. Blocking a write
  * on an absence this analysis cannot actually confirm would be a false
  * positive, not a caught defect — the same reasoning `ir.UnknownPlan`'s own
  * doc already establishes ("the rest of the tree stays inspectable... an
  * unsupported construct must always be visible in the model, never
  * silently dropped") applied one level up, to a *consumer* of the plan
  * rather than the plan itself.
  *
  * @param unknownNodeTypes the distinct `ir.UnknownPlan.sourceType` values
  *   of the unresolved boundary node(s) in the plan, in the order first
  *   encountered (e.g. `"LogicalRDD"`) — named directly rather than folded
  *   into a generic "something was unrecognized" message, so a person
  *   reading a report can immediately tell *why* this input's absence isn't
  *   proven.
  */
case class UnverifiableInput(inputName: String, inputLocation: String, unknownNodeTypes: List[String]) {
  def toMap: Map[String, Any] =
    Map("inputName" -> inputName, "inputLocation" -> inputLocation, "unknownNodeTypes" -> unknownNodeTypes)
}

/** The two "unexpected X can be rejected" toggles from the check list —
  * off by default, matching how most contract/schema tooling treats an
  * unlisted extra column: permitted unless a caller opts into strict mode.
  *
  * `computeFingerprint` is a third, independent opt-in (see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.1): when true,
  * `ContractEnforcementRule.verifyOrThrow` computes a
  * `com.invaract.fingerprint.TransformationFingerprint` for the plan being
  * checked and attaches it to `VerificationResult.fingerprints`. Off by
  * default for the same reason as the other two — canonicalising and
  * hashing a whole plan on every check is real additional work this
  * module should not impose on every existing caller by default.
  *
  * `staticDataQuality` is a fourth, independent opt-in (see
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md): when true,
  * `ContractEnforcementRule.verifyOrThrow` runs
  * `StaticDataQualityVerifier.verify` against the plan being checked,
  * populating `VerificationResult.dataQuality` and folding any
  * `DataQualityVerdict.Violated` result into `violations`. Off by default,
  * same reasoning as the other three: proving static data-quality
  * properties is real additional analysis work this module should not
  * impose on every existing caller by default.
  *
  * `roleConsistency` is a fifth, independent opt-in (see
  * docs/CONTRACT_MODEL.md's "Input and Output Types" section): when true,
  * `ContractEnforcementRule.verifyOrThrow` runs
  * `RoleConsistencyVerifier.verify` against the plan being checked,
  * populating `VerificationResult.roleConformance` and folding any
  * `RoleConformanceVerdict.Contradicts` result into `violations`. Off by
  * default, same reasoning as the other four — attachable via
  * `spark.invaract.roleConsistency` per the External Attachability
  * Requirement, exactly like `staticDataQuality`'s own
  * `spark.invaract.staticDataQuality`.
  */
case class VerificationOptions(
  rejectUndeclaredInputs: Boolean = false,
  rejectUndeclaredFields: Boolean = false,
  computeFingerprint: Boolean = false,
  staticDataQuality: Boolean = false,
  roleConsistency: Boolean = false
)

/** `fingerprints` is `None` unless the check that produced this result ran
  * with `VerificationOptions.computeFingerprint = true` *and* had a real
  * `ir.Plan` to fingerprint — see `ContractEnforcementRule`'s own doc for
  * exactly which branches populate it (only a real `ir.Write` check does;
  * a state-changing CALL or an invalid-contract rejection has no real
  * transformation plan behind the synthetic `UnknownPlan` `explain` renders
  * for those, so fingerprinting it would carry no real information — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.2).
  *
  * `dataQuality` is `Nil` unless the check that produced this result ran
  * with `VerificationOptions.staticDataQuality = true` — populated with
  * one `DataQualityCheckResult` per output field with a declared
  * `nullable: false`/`constraints` property, whatever the verdict (not
  * only `Violated`, which is instead folded into `violations` above — see
  * `ViolationType.DataQualityViolation`'s own doc). Report-only otherwise:
  * a `NotGuaranteed`/`NotStaticallyVerifiable` entry here never affects
  * `passed`.
  *
  * `roleConformance` is `Nil` unless the check that produced this result
  * ran with `VerificationOptions.roleConsistency = true` — populated with
  * one `RoleConformanceCheckResult` per dataset `RoleConsistencyVerifier`
  * could form a verdict for, whatever the verdict (not only `Contradicts`,
  * which is instead folded into `violations` above — see
  * `ViolationType.RoleConsistencyViolation`'s own doc). Report-only
  * otherwise: a `Conforms`/`CannotDetermine` entry here never affects
  * `passed`.
  *
  * `unverifiableInputs` is always populated when applicable — unlike
  * `dataQuality`/`roleConformance` above, there is no `VerificationOptions`
  * flag gating it, since this isn't new opt-in instrumentation: it's the
  * honest half of `MissingInput`'s own always-on check (see
  * `UnverifiableInput`'s own doc for why a declared input the checked plan
  * couldn't confirm reading sometimes can't be confidently reported as
  * missing either). Report-only: an entry here never becomes a `Violation`
  * and never affects `passed`.
  */
case class VerificationResult(
  status: String,
  contract: String,
  violations: List[Violation],
  fingerprints: Option[TransformationFingerprint] = None,
  dataQuality: List[DataQualityCheckResult] = Nil,
  roleConformance: List[RoleConformanceCheckResult] = Nil,
  unverifiableInputs: List[UnverifiableInput] = Nil
) {
  def passed: Boolean = status == "PASSED"
}

object VerificationResult {
  def of(
      contractRef: String,
      violations: List[Violation],
      fingerprints: Option[TransformationFingerprint] = None,
      dataQuality: List[DataQualityCheckResult] = Nil,
      roleConformance: List[RoleConformanceCheckResult] = Nil,
      unverifiableInputs: List[UnverifiableInput] = Nil
  ): VerificationResult =
    VerificationResult(
      if (violations.isEmpty) "PASSED" else "FAILED",
      contractRef,
      violations,
      fingerprints,
      dataQuality,
      roleConformance,
      unverifiableInputs
    )
}
