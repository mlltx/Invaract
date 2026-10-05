// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, Dataset, DatasetType}
import com.invaract.ir.{Lineage, Plan, Write}

/** Checks a contract's declared input `DatasetType`s against how each input
  * is actually observed being used in the translated transformation plan —
  * docs/CONTRACT_MODEL.md's "Input and Output Types" section, "Contract
  * Conformance" / "Role-consistency checks." Deliberately narrow, per that
  * section's own instruction to "focus on high-confidence contradictions
  * rather than attempting to infer organisational semantics": this checks
  * exactly one high-confidence shape (a `CONTROL`-declared input whose data
  * reaches a produced output column) as a blocking `Contradicts`, and
  * reports — never blocks — the genuinely ambiguous mirror case
  * (`DATA_ASSET`/`SOURCE` observed only gating rows, never contributing a
  * column) as `CannotDetermine`, per the spec's own conservatism example:
  * dry-run analysis (and this check, which runs the identical observation)
  * can establish that an object was "read and used to filter processing
  * dates," but "cannot necessarily establish that the object is
  * organisationally defined as a CONTROL" — the same restraint applies in
  * reverse, to an object *declared* something other than CONTROL that
  * merely happens to gate rows without contributing a column.
  *
  * Lives in `spark-adapter`, not `contract`/`ir`: this needs both a real
  * `Contract` (declared types) and traced lineage (observed usage) together
  * — the same division of labor `SensitivityLineage`/`StaticDataQualityVerifier`
  * already establish for a check that needs both concepts at once.
  *
  * Only `contract.inputs` are checked — `DatasetType` on an *output* has no
  * role-consistency check here (a declared output is either produced or
  * not, already `ViolationType.MissingOutput`'s job; whether it's
  * "genuinely a business data product" rather than pipeline-only state is
  * exactly the kind of organisational judgment structural analysis alone
  * cannot establish, so this deliberately makes no claim about it — see
  * ROADMAP.md's note on the spec's own deferred Phase 6).
  */
private[sparkadapter] object RoleConsistencyVerifier {

  /** One `RoleConformanceCheckResult` per input that declares a
    * `datasetType` *and* is observed anywhere in `plan` (contributing to an
    * output column, or referenced in a `Filter`/`Join` condition) — an
    * input with no declared type contributes nothing (there is no
    * declaration to check consistency against), and an input never observed
    * at all in `plan` contributes nothing either (that fact is already
    * `ViolationType.MissingInput`'s job, via `StructuralVerifier` itself —
    * reporting it again here would just be duplicate noise for the same
    * underlying gap). `Nil` for any `plan` that isn't a `Write` — mirrors
    * `StaticDataQualityVerifier.verify`'s own scope, since a role-consistency
    * check only makes sense once there is a real output to trace lineage
    * into.
    */
  def verify(contract: Contract, plan: Plan): List[RoleConformanceCheckResult] = verify(contract, PlanFacts.of(plan))

  /** The same check over a plan whose shape `ContractEnforcementRule` already gathered (see `PlanFacts`). */
  def verify(contract: Contract, facts: PlanFacts): List[RoleConformanceCheckResult] = facts.plan match {
    case plan: Write =>
      // Qualifiers are scopes (aliases, `location#n`), resolved to the reads' real locations first.
      val locationOf = facts.locationResolver
      val outputContributingQualifiers = Lineage.trace(plan).flatMap(_.sources).flatMap(_.qualifier).toSet.map(locationOf)
      val conditionReferencedQualifiers = facts.conditionReferences.flatMap(_.qualifier).map(locationOf)
      // Which declared inputs each set of observed qualifiers matches, found by
      // looking the qualifiers up in an index of the declared locations rather
      // than testing every input against every qualifier.
      val inputIndex = LocationIndex(contract.inputs.zipWithIndex.map { case (input, i) => input.location -> i })
      val contributing: Set[Int] = outputContributingQualifiers.flatMap(inputIndex.matchingIndices)
      val referenced: Set[Int] = conditionReferencedQualifiers.flatMap(inputIndex.matchingIndices)
      contract.inputs.zipWithIndex.flatMap { case (input, i) => checkInput(input, contributing.contains(i), referenced.contains(i)) }
    case _ => Nil
  }

  private def checkInput(
      input: Dataset,
      contributesToOutput: Boolean,
      referencedInCondition: Boolean
  ): Option[RoleConformanceCheckResult] =
    input.datasetType.flatMap { datasetType =>
      if (!contributesToOutput && !referencedInCondition) {
        None // never observed at all in this plan - StructuralVerifier's MissingInput already covers this
      } else {
        val verdict = (datasetType, contributesToOutput) match {
          case (DatasetType.Control, true)  => RoleConformanceVerdict.Contradicts
          case (DatasetType.Control, false) => RoleConformanceVerdict.Conforms
          case (_, true)                    => RoleConformanceVerdict.Conforms
          case (_, false)                   => RoleConformanceVerdict.CannotDetermine
        }
        Some(RoleConformanceCheckResult(input.name, datasetType, verdict, detail(datasetType, contributesToOutput, verdict)))
      }
    }

  private def detail(datasetType: DatasetType, contributesToOutput: Boolean, verdict: RoleConformanceVerdict): String =
    (verdict, contributesToOutput) match {
      case (RoleConformanceVerdict.Contradicts, _) =>
        "observed contributing to a produced output column - CONTROL data must not become part of the resulting " +
          "business data asset it is declared to only operate/control"
      case (_, true) =>
        s"observed contributing to a produced output column, consistent with its declared ${datasetType.name}"
      case (RoleConformanceVerdict.CannotDetermine, false) =>
        s"observed only in a Filter/Join condition, never contributing to a produced output column - a join-only " +
          s"input can still gate which rows survive without any column of its own deriving into the output, so " +
          s"this cannot be confirmed as a contradiction of its declared ${datasetType.name}"
      case (_, false) =>
        // Only Control reaches here (Conforms, not contributing) - every
        // other declared type not contributing to output is CannotDetermine
        // above, never a plain Conforms.
        "observed only in a Filter/Join condition, never contributing to a produced output column, consistent with its declared CONTROL"
    }

  /** `Violation`s for every `Contradicts` entry in `results` — the only
    * verdict that ever blocks a write; see `ViolationType.RoleConsistencyViolation`'s
    * own doc for why `Conforms`/`CannotDetermine` never reach here. Mirrors
    * `StaticDataQualityVerifier.violations`'s identical shape.
    */
  def violations(contract: Contract, results: List[RoleConformanceCheckResult]): List[Violation] =
    results.filter(_.verdict == RoleConformanceVerdict.Contradicts).map(toViolation(contract, _))

  /** The result names its input; the violation also carries that input's declared
    * location (it used to put the input's *name* in the `location` field).
    */
  private def toViolation(contract: Contract, result: RoleConformanceCheckResult): Violation = {
    val location = contract.inputs.find(_.name == result.dataset).map(_.location).getOrElse(result.dataset)
    Violations.roleConsistency(result.dataset, location, result.datasetType, result.detail)
  }
}
