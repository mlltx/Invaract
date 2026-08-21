// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.{Contract, Field => ContractField}
import com.example.ir.{Plan, Read, Write}

import org.apache.spark.sql.types.StructType

/** One structural rule a plan violated, relative to a contract. `column`,
  * `location`, `expected`, and `actual` are populated only where relevant
  * to `violationType` — see `ViolationType` for which fields each type
  * carries.
  */
case class Violation(
  violationType: String,
  message: String,
  column: Option[String] = None,
  location: Option[String] = None,
  expected: Option[String] = None,
  actual: Option[String] = None
) {
  def toMap: Map[String, Any] =
    Map("type" -> violationType, "message" -> message) ++
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
  val MissingOutputField = "MISSING_OUTPUT_FIELD"
  val UndeclaredOutputColumn = "UNDECLARED_OUTPUT_COLUMN"
  val OutputFieldTypeMismatch = "OUTPUT_FIELD_TYPE_MISMATCH"
  val OutputFieldNullabilityMismatch = "OUTPUT_FIELD_NULLABILITY_MISMATCH"
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
  * from MISSION.md §8 — existence, location, and schema, for both inputs
  * and outputs — not yet dependency, transformation, or governance checks.
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
  *     caller supplies these (in `runner/PluginRunner.scala`, straight off
  *     `inputDf.schema`/`outputDf.schema`).
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
  */
object StructuralVerifier {

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
      case Write(dataset, _) =>
        val locationViolation =
          if (locationsMatch(expectedOutput.location, dataset.location)) Nil
          else
            List(
              Violation(
                ViolationType.OutputLocationMismatch,
                s"contract declares output location '${expectedOutput.location}' but the plan writes to '${dataset.location}'",
                expected = Some(expectedOutput.location),
                actual = Some(dataset.location)
              )
            )
        (locationViolation, checkSchema(expectedOutput.schema.fields, outputSchema, "OUTPUT", options.rejectUndeclaredFields))
      case _ =>
        val violation = Violation(
          ViolationType.MissingOutput,
          s"the plan does not produce a write; expected output '${expectedOutput.name}' (${expectedOutput.location})",
          location = Some(expectedOutput.location)
        )
        (List(violation), Nil)
    }

    val violations =
      missingInputs ++ undeclaredInputs ++ inputSchemaViolations ++ outputExistenceViolations ++ outputSchemaViolations

    VerificationResult.of(s"${contract.id}@${contract.version}", violations)
  }

  private def collectReads(plan: Plan): List[Read] = plan match {
    case r: Read => List(r)
    case other    => other.children.flatMap(collectReads)
  }

  private def locationsMatch(declared: String, actual: String): Boolean = {
    val normalizedActual = actual.stripPrefix("file:")
    normalizedActual == declared || normalizedActual.endsWith("/" + declared)
  }

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

    val fieldViolations = contractFields.flatMap { field =>
      actualByName.get(field.name) match {
        case None =>
          if (field.required)
            List(
              Violation(
                missingFieldType,
                s"required field '${field.name}' is absent from the actual $contextPrefix schema",
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
              column = Some(name)
            )
          )
          .toList
      else Nil

    fieldViolations ++ undeclaredViolations
  }
}
