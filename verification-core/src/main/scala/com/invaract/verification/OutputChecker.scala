// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, Dataset, LogicalSchema}
import com.invaract.ir.{Plan, Write}


/** The output half of `StructuralVerifier.verify`: does the plan's write land
  * where, in the format, with the save mode, catalog registration and schema the
  * contract declares? Each question is its own small function below, so it can
  * be read, tested and mutation-tested on its own; `check` only strings them
  * together in the order the violations have always been reported
  * (location, format, save mode, catalog, schema).
  *
  * Also the one home of "which declared output does a location belong to"
  * (`expectedOutputFor` / `matchOutput`), shared with `verify`'s input scoping
  * and `verifyStateChange` so those can never disagree about it.
  */
private[invaract] object OutputChecker {

  /** The declared output a write to `actualLocation` is checked against: a
    * single-output contract's only output regardless of location (its
    * location mismatch is reported in addition to, not instead of, every
    * other check), otherwise whichever declared output matches by location.
    * That single-output rule is deliberate, not an accident of any test: with
    * one declared output there is no ambiguity about which output the author
    * meant, so a write that missed the location (a typo, a moved path) is
    * checked against it and reported with *every* finding at once, rather than
    * one finding per fix-and-rerun. `StructuralVerifierSpec` pins it.
    */
  def expectedOutputFor(outputs: List[Dataset], actualLocation: String): Option[Dataset] =
    if (outputs.size == 1) Some(outputs.head) else matchOutput(outputs, actualLocation)

  /** The declared output (if any) whose location matches `actualLocation`,
    * via the same `locationsMatch` normalized-suffix rule every other
    * location check uses. Returns the first match when more than one declared
    * output shares a location (see `StructuralVerifier.verify`'s "Multi-output
    * contracts" doc) - `outputs` is a `List`, not a `Set`, specifically so this
    * stays deterministic.
    */
  def matchOutput(outputs: List[Dataset], actualLocation: String): Option[Dataset] =
    outputs.find(o => LocationMatching.matches(o.location, actualLocation))

  /** Every output-side finding for `plan`, in report order. A plan with no
    * `Write` leaves every declared output unsatisfied (`MISSING_OUTPUT`, one
    * each); a `Write` is checked as described on each step below.
    */
  def check(contract: Contract, plan: Plan, outputSchema: LogicalSchema, options: VerificationOptions, caseSensitive: Boolean): List[Violation] =
    plan match {
      case write: Write => checkWrite(contract, write, outputSchema, options, caseSensitive)
      case _            => missingOutputs(contract.outputs)
    }

  /** No write at all: one `MISSING_OUTPUT` per declared output (a single-output
    * contract reduces to exactly one violation).
    */
  private[invaract] def missingOutputs(outputs: List[Dataset]): List[Violation] =
    outputs.map(Violations.missingOutput)

  private def checkWrite(contract: Contract, write: Write, outputSchema: LogicalSchema, options: VerificationOptions, caseSensitive: Boolean): List[Violation] = {
    val location = locationFinding(contract.outputs, write.dataset.location)
    // A single-output contract checks format/saveMode/catalog/schema against its
    // only declared output whether or not the location itself matches (see
    // `expectedOutputFor`). A multi-output contract has no such default: if the
    // write's location doesn't identify which declared output it belongs to,
    // there is no non-ambiguous output left to check the rest against.
    val rest = expectedOutputFor(contract.outputs, write.dataset.location) match {
      case None => Nil
      case Some(expectedOutput) =>
        formatFinding(expectedOutput, write.format, write.dataset.location) ++
          saveModeFinding(expectedOutput, write.saveMode, write.dataset.location) ++
          catalogFinding(expectedOutput, write) ++
          SchemaChecker.check(expectedOutput.schema.fields, outputSchema, SchemaChecker.Side.Output, write.dataset.location, options.rejectUndeclaredFields, caseSensitive)
    }
    location ++ rest
  }

  /** `OUTPUT_LOCATION_MISMATCH` when no declared output's location matches the write. */
  private[invaract] def locationFinding(outputs: List[Dataset], actualLocation: String): List[Violation] =
    matchOutput(outputs, actualLocation) match {
      case Some(_) => Nil
      case None    => List(Violations.outputLocationMismatch(outputs.map(_.location), actualLocation))
    }

  /** Only checked when both sides are known: a contract that doesn't declare a
    * format isn't opting into this check at all, and a plan whose format the
    * adapter couldn't determine can't be compared without risking a false
    * rejection on a write this IR simply doesn't have precise format
    * information for yet.
    */
  private[invaract] def formatFinding(expectedOutput: Dataset, actualFormat: Option[String], writeLocation: String): List[Violation] =
    (expectedOutput.format, actualFormat) match {
      case (Some(expected), Some(actual)) if !expected.equalsIgnoreCase(actual) =>
        List(Violations.outputFormatMismatch(expected, actual, writeLocation))
      case _ => Nil
    }

  /** Same both-sides-known convention as `formatFinding`. */
  private[invaract] def saveModeFinding(expectedOutput: Dataset, actualSaveMode: Option[String], writeLocation: String): List[Violation] =
    (expectedOutput.saveMode, actualSaveMode) match {
      case (Some(expected), Some(actual)) if !expected.equalsIgnoreCase(actual) =>
        List(Violations.outputSaveModeMismatch(expected, actual, writeLocation))
      case _ => Nil
    }

  /** Same both-sides-known, opt-in-per-dataset convention: only checked when the
    * contract's output declares `catalog:` at all.
    */
  private[invaract] def catalogFinding(expectedOutput: Dataset, write: Write): List[Violation] =
    expectedOutput.catalog match {
      case Some(req) => CatalogChecker.check(req, write.catalog, write.dataset.location, SchemaChecker.Side.Output)
      case None      => Nil
    }
}
