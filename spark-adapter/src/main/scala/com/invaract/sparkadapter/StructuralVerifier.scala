// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, Field => ContractField}
import com.invaract.ir.{Plan, Read, Write}

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
    * `contract.outputs.head`, rather than a clean, actionable rejection.
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
}

/** The two "unexpected X can be rejected" toggles from the check list —
  * off by default, matching how most contract/schema tooling treats an
  * unlisted extra column: permitted unless a caller opts into strict mode.
  */
case class VerificationOptions(rejectUndeclaredInputs: Boolean = false, rejectUndeclaredFields: Boolean = false)

case class VerificationResult(status: String, contract: String, violations: List[Violation]) {
  def passed: Boolean = status == "PASSED"
}

object VerificationResult {
  def of(contractRef: String, violations: List[Violation]): VerificationResult =
    VerificationResult(if (violations.isEmpty) "PASSED" else "FAILED", contractRef, violations)
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
  * but one verification run only ever observes one `Write`. This checks
  * the plan's single output against `contract.outputs.head` — the same
  * single-output assumption the rest of this demo pipeline makes. Checking
  * a plan against whichever of several declared outputs it actually
  * produced is future work, not exercised by anything in this repo today.
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
    val actualReadLocations = collectReads(plan).map(_.dataset.location).distinct

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

    val expectedOutput = contract.outputs.head
    val (outputExistenceViolations, outputSchemaViolations) = plan match {
      case Write(dataset, _, actualFormat, actualSaveMode) =>
        val locationViolation =
          if (locationsMatch(expectedOutput.location, dataset.location)) Nil
          else
            List(
              Violation(
                ViolationType.OutputLocationMismatch,
                s"contract declares output location '${expectedOutput.location}' but the plan writes to '${dataset.location}'",
                remediation =
                  s"Write to '${expectedOutput.location}' instead, or update the contract's declared output location to '${dataset.location}' if this location change is intentional.",
                expected = Some(expectedOutput.location),
                actual = Some(dataset.location)
              )
            )
        // Only checked when both sides are known: a contract that doesn't
        // declare a format isn't opting into this check at all, and a plan
        // whose format the adapter couldn't determine (formatOf returned
        // None) can't be compared without risking a false rejection on a
        // write this IR simply doesn't have precise format information
        // for yet.
        val formatViolation = (expectedOutput.format, actualFormat) match {
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
        // Same both-sides-known convention as formatViolation above.
        val saveModeViolation = (expectedOutput.saveMode, actualSaveMode) match {
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
        (locationViolation ++ formatViolation ++ saveModeViolation, checkSchema(expectedOutput.schema.fields, outputSchema, "OUTPUT", options.rejectUndeclaredFields))
      case _ =>
        val violation = Violation(
          ViolationType.MissingOutput,
          s"the plan does not produce a write; expected output '${expectedOutput.name}' (${expectedOutput.location})",
          remediation = s"Add a write to '${expectedOutput.location}' to the transformation.",
          location = Some(expectedOutput.location)
        )
        (List(violation), Nil)
    }

    val violations =
      missingInputs ++ undeclaredInputs ++ inputSchemaViolations ++ outputExistenceViolations ++ outputSchemaViolations

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
    */
  private[sparkadapter] def verifyStateChange(
    contract: Contract,
    location: String,
    resultingSchema: StructType,
    options: VerificationOptions = VerificationOptions()
  ): VerificationResult = {
    val expectedOutput = contract.outputs.head
    if (!locationsMatch(expectedOutput.location, location))
      VerificationResult.of(s"${contract.id}@${contract.version}", Nil)
    else {
      val schemaViolations = checkSchema(expectedOutput.schema.fields, resultingSchema, "OUTPUT", options.rejectUndeclaredFields)
      VerificationResult.of(s"${contract.id}@${contract.version}", schemaViolations)
    }
  }

  private def collectReads(plan: Plan): List[Read] = plan match {
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
}
