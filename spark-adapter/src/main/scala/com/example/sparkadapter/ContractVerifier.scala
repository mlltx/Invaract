// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.Contract
import com.example.ir.ColumnLineage

import org.apache.spark.sql.types.StructType

/** The outcome of checking one contract-declared field against the actual
  * Spark output. */
case class FieldCheck(name: String, passed: Boolean, message: String)

/** The outcome of checking a `Contract`'s declared output against a real
  * Spark job's actual output schema and traced lineage.
  */
case class VerificationResult(datasetName: String, passed: Boolean, checks: List[FieldCheck])

/** Checks whether a Spark job's actual output satisfies a `Contract`'s
  * declared output schema — the first, structural slice of ROADMAP.md
  * Phase 1c's "contract verification algorithm" (see MISSION.md §8,
  * "Structural" checks: output exists, schema matches, required fields
  * exist, types are compatible).
  *
  * This intentionally checks less than the full contract model supports.
  * What it verifies, per contract-declared field:
  *
  *  1. **Presence** — does the actual output have a column with this name?
  *  2. **Type compatibility** — does the actual column's Spark type name
  *     match the contract's declared logical type? (Spark's
  *     `DataType.typeName` and this project's contract type vocabulary
  *     already use the same strings for the common types — `"integer"`,
  *     `"long"`, `"string"`, `"double"`, etc. — so this is a direct
  *     comparison, not a translation table.)
  *  3. **Lineage** — for a *required* field, can `Lineage.trace` account
  *     for where its value comes from at all?
  *
  * What it deliberately does *not* check yet: nullability (Spark's
  * inferred `nullable` flag is permissive by default — even a column that
  * happens to contain no nulls is usually reported nullable — so comparing
  * it against a contract's declared `nullable` would produce false
  * failures far more often than real ones); `rules` (compatibility mode,
  * quality expectations — recorded by the contract model, not yet
  * interpreted by anything); and extra output columns the contract doesn't
  * mention (not flagged, since a contract narrowing its declared surface
  * over time is a normal, compatible evolution — see
  * `docs/CONTRACT_MODEL.md`).
  *
  * Placed in `spark-adapter` rather than `contract` because it needs a
  * real Spark `StructType` for type checking — `contract` itself has no
  * Spark dependency and stays that way.
  */
object ContractVerifier {

  def verify(contract: Contract, actualSchema: StructType, lineage: List[ColumnLineage]): VerificationResult = {
    val output = contract.outputs.headOption.getOrElse(
      throw new IllegalArgumentException(s"Contract '${contract.id}' declares no outputs to verify against")
    )

    val actualFields = actualSchema.fields.map(f => f.name -> f).toMap
    val lineageByName = lineage.map(l => l.output.name -> l).toMap

    val checks = output.schema.fields.map { field =>
      actualFields.get(field.name) match {
        case None =>
          FieldCheck(field.name, passed = false, s"required by contract but absent from the actual output")

        case Some(actualField) =>
          val actualType = actualField.dataType.typeName
          if (actualType != field.fieldType.toLowerCase) {
            FieldCheck(
              field.name,
              passed = false,
              s"contract declares type '${field.fieldType}' but actual output has type '$actualType'"
            )
          } else if (field.required && !lineageByName.contains(field.name)) {
            FieldCheck(field.name, passed = false, s"required field, but no lineage could be traced for it")
          } else {
            val sources = lineageByName.get(field.name).map(_.sources.mkString(", ")).getOrElse("(no lineage traced)")
            FieldCheck(field.name, passed = true, s"type '$actualType' matches; sources: $sources")
          }
      }
    }

    VerificationResult(output.name, checks.forall(_.passed), checks)
  }
}
