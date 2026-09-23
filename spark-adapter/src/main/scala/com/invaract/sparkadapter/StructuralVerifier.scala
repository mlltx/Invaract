// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{CatalogRequirement, Contract, Dataset, Field => ContractField}
import com.invaract.fingerprint.TransformationFingerprint
import com.invaract.ir.{CatalogIdentity, Plan, Read, Write}

import org.apache.spark.sql.types.StructType

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
  actual: Option[String] = None
) {
  def toMap: Map[String, Any] =
    Map("type" -> violationType, "message" -> message, "remediation" -> remediation) ++
      column.map("column" -> _) ++
      location.map("location" -> _) ++
      expected.map("expected" -> _) ++
      actual.map("actual" -> _)
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
  */
case class VerificationOptions(
  rejectUndeclaredInputs: Boolean = false,
  rejectUndeclaredFields: Boolean = false,
  computeFingerprint: Boolean = false,
  staticDataQuality: Boolean = false
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
  */
case class VerificationResult(
  status: String,
  contract: String,
  violations: List[Violation],
  fingerprints: Option[TransformationFingerprint] = None,
  dataQuality: List[DataQualityCheckResult] = Nil
) {
  def passed: Boolean = status == "PASSED"
}

object VerificationResult {
  def of(
      contractRef: String,
      violations: List[Violation],
      fingerprints: Option[TransformationFingerprint] = None,
      dataQuality: List[DataQualityCheckResult] = Nil
  ): VerificationResult =
    VerificationResult(if (violations.isEmpty) "PASSED" else "FAILED", contractRef, violations, fingerprints, dataQuality)
}

/** Checks a transformation plan's actual inputs and output against a
  * `Contract`'s declarations. This is ROADMAP.md Phase 4: the first
  * *useful* verifier, checking exactly the "Structural" class of property
  * from MISSION.md §8 — existence, location, schema, and (for outputs)
  * format, for both inputs and outputs — not yet dependency,
  * transformation, or governance checks.
  *
  * Two kinds of information feed a check, and they come from different
  * places:
  *
  *   - **Existence and location** are read directly off the `Plan`
  *     (`Read`/`Write` nodes' `DatasetRef.location`) — no Spark-specific
  *     data needed, since `ir.Plan` already carries this.
  *   - **Schema** (field presence, type, nullability) needs the actual
  *     Spark `StructType` for each dataset, because the IR deliberately
  *     carries no schema of its own (see `ir.Read`'s doc) — only which
  *     columns were *referenced*, not the dataset's full column set. The
  *     caller supplies these. Every resolved Catalyst `LogicalPlan` exposes
  *     its own `.schema` derived from resolved attributes, so a caller can
  *     get these directly from the *analyzed* plan — before anything
  *     executes — rather than needing a materialized `DataFrame`; see
  *     `ContractEnforcementRule`, which does exactly this to verify a write
  *     before Spark runs it.
  *
  * ## Determinism
  *
  * Given the same `contract`, `plan`, `inputSchemas`, and `outputSchema`,
  * `verify` always returns the same violations in the same order — no
  * hash-based `Set`/`Map` iteration in the result-building path (`Set`s are
  * used only for membership tests, never iterated to produce output). This
  * matters beyond reproducible tests: ROADMAP.md Phase 5 gates a real
  * Spark write on this result, and a nondeterministic verdict — or even a
  * deterministic verdict with nondeterministically-ordered violations —
  * would make a failure impossible to reliably reproduce or explain.
  *
  * ## Location matching
  *
  * A contract declares portable, relative locations
  * (`"demo/input/sample.csv"`); Spark reports absolute `file:` URIs at
  * runtime (`"file:/home/user/.../demo/input/sample.csv"`) — confirmed
  * empirically, see docs/SPARK_ADAPTER.md. Comparing these with `==` would
  * fail every declared input/output on every real run for a reason that
  * has nothing to do with contract compliance. Locations are matched by
  * normalized suffix instead: strip a `file:` scheme, then a declared
  * location matches if it equals the actual location or is a path-boundary
  * suffix of it.
  *
  * ## Multi-output contracts
  *
  * A `Contract` can declare multiple outputs (`contract.outputs: List`),
  * but one verification run only ever observes one `Write` — each write a
  * job performs triggers its own, independent check (see
  * `ContractEnforcementRule`'s "Fires on every analyzed plan" doc), so there
  * is never more than one actual output to reconcile against a contract's
  * several declared ones in a single `verify` call.
  *
  * The plan's actual write location is matched against every declared
  * output's `location` (via the same `locationsMatch` normalized-suffix
  * rule used everywhere else in this method), not just `contract.outputs.head`:
  *
  *   - A contract with exactly one declared output keeps its original
  *     behavior unchanged: format/saveMode/catalog/schema are all checked
  *     against that one output regardless of whether its location matches
  *     the actual write (a location mismatch is reported *in addition to*,
  *     not instead of, those other checks) — a real test relies on this
  *     (a contract deliberately declaring a location that never matches
  *     any real write, purely to check catalog identity independent of
  *     location).
  *   - A contract with more than one declared output has no such single
  *     default to fall back to. Exactly one declared output matching the
  *     write's location → every other check (format, saveMode, catalog,
  *     schema) runs against *that* output specifically. No declared output
  *     matching → `OUTPUT_LOCATION_MISMATCH`, naming every declared output
  *     location as a candidate, and no format/saveMode/catalog/schema check
  *     runs at all — there is no non-ambiguous output left to check them
  *     against.
  *   - The plan produces no write at all → `MISSING_OUTPUT` once per
  *     declared output (for a single-output contract, exactly the original
  *     one violation).
  *
  * A contract with two declared outputs sharing the same `location` is
  * flagged by `ContractValidator` as a Warning (ambiguous, not rejected —
  * see its own doc); `verify` picks whichever matches first when that
  * happens, and doesn't special-case it further.
  *
  * ## Visibility
  *
  * `private[sparkadapter]`: nothing outside this module calls `verify`
  * directly (confirmed by grep before narrowing it —
  * `ContractEnforcementRule` is the only real caller). A real Invaract
  * user gets verification automatically via the installed extension
  * (`ContractEnforcementRule.forContract`) and never needs to call this
  * raw function themselves; `VerificationResult`/`Violation` (the payload
  * of `ContractViolationException.result`) remain public since a user
  * does need to inspect those. As with `SparkPlanAdapter`, this is a
  * Scala-compiler-enforced restriction, not a JVM one — the compiled
  * class stays `public` in bytecode, so MiMa doesn't (and structurally
  * can't) enforce this particular boundary; it only stops real Scala code
  * from depending on this by accident.
  */
private[sparkadapter] object StructuralVerifier {

  def verify(
    contract: Contract,
    plan: Plan,
    inputSchemas: List[(String, StructType)],
    outputSchema: StructType,
    options: VerificationOptions = VerificationOptions()
  ): VerificationResult = {
    val actualReads = collectReads(plan)
    val actualReadLocations = actualReads.map(_.dataset.location).distinct

    val missingInputs = contract.inputs
      .filterNot(input => actualReadLocations.exists(locationsMatch(input.location, _)))
      .map(input =>
        Violation(
          ViolationType.MissingInput,
          s"declared input '${input.name}' (${input.location}) was not read by this plan",
          remediation =
            s"Add a read of '${input.location}' to the transformation, or remove '${input.name}' from the contract's inputs if it is no longer needed.",
          location = Some(input.location)
        )
      )

    val undeclaredInputs =
      if (options.rejectUndeclaredInputs)
        actualReadLocations
          .filterNot(loc => contract.inputs.exists(input => locationsMatch(input.location, loc)))
          .map(loc =>
            Violation(
              ViolationType.UndeclaredInput,
              s"plan reads '$loc' which is not declared as a contract input",
              remediation = s"Declare '$loc' as an input in the contract, or remove this read from the transformation.",
              location = Some(loc)
            )
          )
      else Nil

    val inputSchemaViolations = contract.inputs.flatMap { input =>
      inputSchemas.find { case (loc, _) => locationsMatch(input.location, loc) } match {
        case Some((_, schema)) =>
          checkSchema(input.schema.fields, schema, "INPUT", options.rejectUndeclaredFields)
        case None =>
          Nil // no actual schema supplied for this input; existence was already checked above
      }
    }

    // Read side: matched by declared/actual location, the same
    // `locationsMatch` rule every other input check in this method already
    // uses. Each `ir.Read` node already carries its own resolved
    // `CatalogIdentity` (populated by `SparkPlanAdapter`/`WriteCommandSupport`
    // at translation time), so no extra plumbing is needed beyond what
    // `collectReads` already gathers - unlike schema, which the IR
    // deliberately doesn't carry and callers must supply separately.
    val inputCatalogViolations = contract.inputs.flatMap { input =>
      input.catalog match {
        case None => Nil
        case Some(req) =>
          actualReads.find(r => locationsMatch(input.location, r.dataset.location)) match {
            case Some(read) => catalogViolations(req, read.catalog, input.location, "INPUT")
            // No matching read at all: already reported as MissingInput
            // above: nothing more useful to say about its catalog identity.
            case None => Nil
          }
      }
    }

    val (outputExistenceViolations, outputSchemaViolations) = plan match {
      case Write(dataset, _, actualFormat, actualSaveMode, actualCatalog) =>
        val matched = matchOutput(contract.outputs, dataset.location)
        // A single-output contract keeps its pre-existing behavior exactly:
        // format/saveMode/catalog/schema are always checked against
        // contract.outputs.head, regardless of whether the location itself
        // matches - a real test relies on this (a contract deliberately
        // declaring a location that never matches any real write, purely to
        // check catalog identity independent of location - see
        // HiveConnectorSpec's `assertBothRejectedForWrongTechnology`).
        // A multi-output contract has no such single default to fall back
        // to: if the write's location doesn't identify which declared
        // output it belongs to, there is no non-ambiguous output left to
        // check the rest against.
        val expectedOutputOpt: Option[Dataset] =
          if (contract.outputs.size == 1) Some(contract.outputs.head) else matched

        val locationViolation = matched match {
          case Some(_) => Nil
          case None =>
            val candidateLocations = contract.outputs.map(_.location)
            List(
              Violation(
                ViolationType.OutputLocationMismatch,
                if (contract.outputs.size == 1)
                  s"contract declares output location '${candidateLocations.head}' but the plan writes to '${dataset.location}'"
                else
                  s"the plan writes to '${dataset.location}', which does not match any of the contract's " +
                    s"${contract.outputs.size} declared output locations (${candidateLocations.mkString(", ")})",
                remediation =
                  if (contract.outputs.size == 1)
                    s"Write to '${candidateLocations.head}' instead, or update the contract's declared output location to '${dataset.location}' if this location change is intentional."
                  else
                    s"Write to one of the contract's declared output locations (${candidateLocations.mkString(", ")}) instead, or add '${dataset.location}' as a new declared output if this is intentional.",
                expected = Some(candidateLocations.mkString(", ")),
                actual = Some(dataset.location)
              )
            )
        }

        val (formatViolation, saveModeViolation, catalogViolation, schemaViolations) = expectedOutputOpt match {
          case None => (Nil, Nil, Nil, Nil)
          case Some(expectedOutput) =>
            // Only checked when both sides are known: a contract that
            // doesn't declare a format isn't opting into this check at all,
            // and a plan whose format the adapter couldn't determine
            // (formatOf returned None) can't be compared without risking a
            // false rejection on a write this IR simply doesn't have
            // precise format information for yet.
            val format = (expectedOutput.format, actualFormat) match {
              case (Some(expected), Some(actual)) if !expected.equalsIgnoreCase(actual) =>
                List(
                  Violation(
                    ViolationType.OutputFormatMismatch,
                    s"contract declares output format '$expected' but the plan writes in format '$actual'",
                    remediation =
                      s"Write in '$expected' format instead, or update the contract's declared format to '$actual' if this format change is intentional.",
                    expected = Some(expected),
                    actual = Some(actual)
                  )
                )
              case _ => Nil
            }
            // Same both-sides-known convention as format above.
            val saveMode = (expectedOutput.saveMode, actualSaveMode) match {
              case (Some(expected), Some(actual)) if !expected.equalsIgnoreCase(actual) =>
                List(
                  Violation(
                    ViolationType.OutputSaveModeMismatch,
                    s"contract declares output save mode '$expected' but the plan writes with save mode '$actual'",
                    remediation =
                      s"Write with save mode '$expected' instead, or update the contract's declared saveMode to '$actual' if this change is intentional.",
                    expected = Some(expected),
                    actual = Some(actual)
                  )
                )
              case _ => Nil
            }
            // Same both-sides-known, opt-in-per-dataset convention: only
            // checked when the contract's output declares `catalog:` at all.
            val catalog = expectedOutput.catalog match {
              case Some(req) => catalogViolations(req, actualCatalog, dataset.location, "OUTPUT")
              case None       => Nil
            }
            val schema = checkSchema(expectedOutput.schema.fields, outputSchema, "OUTPUT", options.rejectUndeclaredFields)
            (format, saveMode, catalog, schema)
        }

        (locationViolation ++ formatViolation ++ saveModeViolation ++ catalogViolation, schemaViolations)
      case _ =>
        // No write at all: every declared output is unsatisfied by this
        // plan - one MissingOutput violation each (a single-output contract
        // reduces to exactly the original one violation).
        val violations = contract.outputs.map(expectedOutput =>
          Violation(
            ViolationType.MissingOutput,
            s"the plan does not produce a write; expected output '${expectedOutput.name}' (${expectedOutput.location})",
            remediation = s"Add a write to '${expectedOutput.location}' to the transformation.",
            location = Some(expectedOutput.location)
          )
        )
        (violations, Nil)
    }

    val violations =
      missingInputs ++ undeclaredInputs ++ inputSchemaViolations ++ inputCatalogViolations ++
        outputExistenceViolations ++ outputSchemaViolations

    VerificationResult.of(s"${contract.id}@${contract.version}", violations)
  }

  /** For state-changing, non-write operations that still result in a
    * committed schema change at a location - currently just Iceberg's
    * `rollback_to_snapshot` (see `StateChangingCallSupport`) - checked
    * against a contract's declared output. Deliberately narrower than
    * `verify` above: no `ir.Plan` to walk (there's no Spark query being
    * written, so no reads to collect and no input-side checking applies).
    *
    * Location is a *scoping* gate here, not a violation the way it is for
    * `verify`'s `Write` case above: a real write happening under an active
    * contract, to the wrong place, is worth flagging (something WAS
    * written, just not where expected). A state-changing operation on a
    * table this contract doesn't declare at all isn't a wrong-place write -
    * it's simply not this contract's concern, since one contract's
    * presence shouldn't gate every operation on every table in a job that
    * happens to run under it. So a location that doesn't match returns a
    * clean pass (no violations, schema not even checked) rather than an
    * `OutputLocationMismatch` violation - confirmed by a real test
    * (`IcebergConnectorSpec`'s "rollback_to_snapshot on a table the active
    * contract doesn't govern") that first caught this getting it backwards.
    * Schema checking only happens once location scoping says this
    * operation IS the contract's concern - reuses `checkSchema` directly
    * rather than duplicating it, same rules/violation types/remediation
    * wording as every other output check in this file.
    *
    * Multi-output contracts: the same `matchOutput` lookup `verify` uses
    * above decides which declared output (if any) this location belongs
    * to - a contract declaring several outputs is scoped exactly the same
    * way a single-output one already was, just checked against whichever
    * one actually matches instead of always `contract.outputs.head`.
    */
  private[sparkadapter] def verifyStateChange(
    contract: Contract,
    location: String,
    resultingSchema: StructType,
    options: VerificationOptions = VerificationOptions()
  ): VerificationResult =
    matchOutput(contract.outputs, location) match {
      case None => VerificationResult.of(s"${contract.id}@${contract.version}", Nil)
      case Some(expectedOutput) =>
        val schemaViolations = checkSchema(expectedOutput.schema.fields, resultingSchema, "OUTPUT", options.rejectUndeclaredFields)
        VerificationResult.of(s"${contract.id}@${contract.version}", schemaViolations)
    }

  /** The declared output (if any) whose location matches `actualLocation`,
    * via the same `locationsMatch` normalized-suffix rule every other
    * location check in this file uses. Shared by `verify` and
    * `verifyStateChange` so the two can't drift into using two different
    * notions of "which output does this location belong to." Returns the
    * first match when more than one declared output shares a location
    * (see `verify`'s "Multi-output contracts" doc) - `outputs` is a `List`,
    * not a `Set`, specifically so this stays deterministic.
    */
  private def matchOutput(outputs: List[Dataset], actualLocation: String): Option[Dataset] =
    outputs.find(o => locationsMatch(o.location, actualLocation))

  /** `private[sparkadapter]`: reused by `StaticDataQualityVerifier` to
    * discover a plan's real `Read` scopes before matching them against
    * `contract.inputs` — the same reason `locationsMatch` below is widened.
    */
  private[sparkadapter] def collectReads(plan: Plan): List[Read] = plan match {
    case r: Read => List(r)
    case other    => other.children.flatMap(collectReads)
  }

  // private[sparkadapter], not private: reused by SensitivityLineage to
  // match a traced ColumnRef's qualifier (a Read's actual reported
  // location) against a contract input's declared, portable location -
  // the same normalized-suffix rule this method already documents, not a
  // second copy of it (mirrors why normalizeSparkLocation below already
  // has this same widened visibility, for ContractInference's reuse).
  private[sparkadapter] def locationsMatch(declared: String, actual: String): Boolean = {
    // A contract's declared location can come from anywhere (a config file
    // authored on Windows, e.g.), while Spark always reports actual plan
    // locations with forward slashes regardless of OS. Normalize both
    // sides so a Windows-style declared path (C:\...\out.parquet) still
    // matches Spark's file:/C:/.../out.parquet.
    val normalizedDeclared = declared.replace('\\', '/')
    val normalizedActual = normalizeSparkLocation(actual)
    normalizedActual == normalizedDeclared || normalizedActual.endsWith("/" + normalizedDeclared)
  }

  /** The bare, OS-agnostic form a contract's `declared` location is
    * expected to already be in, derived from a location as Spark itself
    * reports it (always forward-slash, often `file:`-scheme-prefixed for a
    * local path) - factored out of `locationsMatch` above so
    * `ContractInference` (dry-run mode) can apply the exact same
    * normalization when inferring a location a user will *declare* in a
    * contract, not just when comparing one against it. The two must use
    * one shared definition: an inferred contract that skipped this
    * normalization would carry a `"file:..."`-prefixed declared location
    * that `locationsMatch` never strips from the *declared* side, wrongly
    * rejecting the exact write it was inferred from - confirmed the hard
    * way by a real test failing this way before `ContractInference` was
    * fixed to call this instead of its own separate copy.
    */
  private[sparkadapter] def normalizeSparkLocation(actual: String): String =
    actual.stripPrefix("file:").replace('\\', '/')

  /** Shared by both input and output checking — "Schema" in the check list
    * is one set of rules, applied twice (once per side), not two separate
    * rule sets. `contextPrefix` ("INPUT"/"OUTPUT") only changes which
    * violation type and wording each finding gets.
    */
  private def checkSchema(
    contractFields: List[ContractField],
    actualSchema: StructType,
    contextPrefix: String,
    rejectUndeclaredFields: Boolean
  ): List[Violation] = {
    val actualByName = actualSchema.fields.map(f => f.name -> f).toMap
    val declaredNames = contractFields.map(_.name).toSet

    val (missingFieldType, undeclaredColumnType, typeMismatchType, nullabilityMismatchType) =
      if (contextPrefix == "INPUT")
        (
          ViolationType.MissingInputField,
          ViolationType.UndeclaredInputColumn,
          ViolationType.InputFieldTypeMismatch,
          ViolationType.InputFieldNullabilityMismatch
        )
      else
        (
          ViolationType.MissingOutputField,
          ViolationType.UndeclaredOutputColumn,
          ViolationType.OutputFieldTypeMismatch,
          ViolationType.OutputFieldNullabilityMismatch
        )

    val datasetNoun = if (contextPrefix == "INPUT") "input" else "output"

    val fieldViolations = contractFields.flatMap { field =>
      actualByName.get(field.name) match {
        case None =>
          if (field.required)
            List(
              Violation(
                missingFieldType,
                s"required field '${field.name}' is absent from the actual $contextPrefix schema",
                remediation =
                  s"Add a '${field.name}' column (type '${field.fieldType}') to the $datasetNoun, or mark it optional in the contract if it isn't always produced.",
                column = Some(field.name)
              )
            )
          else Nil

        case Some(actualField) =>
          val actualType = actualField.dataType.typeName
          val typeViolation =
            if (actualType != field.fieldType.toLowerCase)
              List(
                Violation(
                  typeMismatchType,
                  s"field '${field.name}' declares type '${field.fieldType}' but the actual $contextPrefix schema has type '$actualType'",
                  remediation =
                    s"Cast '${field.name}' to '${field.fieldType}' in the transformation, or update the contract to declare '$actualType' if the new type is intentional.",
                  column = Some(field.name),
                  expected = Some(field.fieldType),
                  actual = Some(actualType)
                )
              )
            else Nil

          // Compatible, not identical: a contract requiring non-null
          // (nullable = false) is violated by an actual column that
          // permits nulls; the reverse (contract allows null, actual
          // guarantees non-null) is a stricter-than-required guarantee,
          // not a violation.
          val nullabilityViolation =
            if (!field.nullable && actualField.nullable)
              List(
                Violation(
                  nullabilityMismatchType,
                  s"field '${field.name}' is declared non-nullable but the actual $contextPrefix schema permits nulls",
                  remediation =
                    s"Filter or coalesce nulls out of '${field.name}' before the $datasetNoun is produced, or relax the contract to allow nulls if they're expected.",
                  column = Some(field.name),
                  expected = Some("not null"),
                  actual = Some("nullable")
                )
              )
            else Nil

          typeViolation ++ nullabilityViolation
      }
    }

    val undeclaredViolations =
      if (rejectUndeclaredFields)
        actualSchema.fieldNames
          .filterNot(declaredNames.contains)
          .map(name =>
            Violation(
              undeclaredColumnType,
              s"column '$name' is present in the actual $contextPrefix schema but not declared by the contract",
              remediation =
                s"Remove '$name' from the transformation's $datasetNoun, or add it to the contract's declared schema if it's intentional.",
              column = Some(name)
            )
          )
          .toList
      else Nil

    fieldViolations ++ undeclaredViolations
  }

  /** Checks one dataset's actual `CatalogIdentity` against the contract's
    * declared `CatalogRequirement` — shared by both input and output
    * checking, the same "one rule set applied twice" pattern `checkSchema`
    * above already uses for schema. `req.required == false` means the
    * contract declares an *expected* shape without gating on it
    * (informational only, accepted by `ContractValidator`) — never a
    * violation on its own, matching the plan's documented convention.
    */
  private def catalogViolations(
    req: CatalogRequirement,
    actual: Option[CatalogIdentity],
    location: String,
    contextPrefix: String
  ): List[Violation] = {
    if (!req.required) Nil
    else
      actual match {
        case None =>
          List(
            Violation(
              if (contextPrefix == "INPUT") ViolationType.MissingInputCatalogRegistration
              else ViolationType.MissingOutputCatalogRegistration,
              s"contract requires the ${contextPrefix.toLowerCase} at '$location' to be registered in a catalog, but it has no catalog registration",
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
                if (contextPrefix == "INPUT") ViolationType.InputCatalogMismatch else ViolationType.OutputCatalogMismatch,
                s"contract's declared catalog registration for the ${contextPrefix.toLowerCase} at '$location' does not match the actual registration: ${mismatches
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
  private def catalogFieldMismatches(req: CatalogRequirement, actual: CatalogIdentity): List[String] = {
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

  private def describeCatalogRequirement(req: CatalogRequirement): String =
    List(
      req.technology.map(t => s"technology=$t"),
      req.catalogName.map(c => s"catalogName=$c"),
      req.location.map(l => s"location=$l"),
      if (req.namespace.nonEmpty) Some(s"namespace=${req.namespace.mkString(".")}") else None,
      req.table.map(t => s"table=$t")
    ).flatten.mkString(", ")

  private def describeCatalogIdentity(actual: CatalogIdentity): String =
    List(
      actual.technology.map(t => s"technology=$t"),
      actual.catalogName.map(c => s"catalogName=$c"),
      actual.location.map(l => s"location=$l"),
      if (actual.namespace.nonEmpty) Some(s"namespace=${actual.namespace.mkString(".")}") else None,
      actual.table.map(t => s"table=$t")
    ).flatten.mkString(", ")
}
