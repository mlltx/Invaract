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
  def verify(contract: Contract, plan: Plan): List[RoleConformanceCheckResult] = plan match {
    case _: Write =>
      val outputContributingQualifiers = Lineage.trace(plan).flatMap(_.sources).flatMap(_.qualifier).toSet
      val conditionReferencedQualifiers = PlanRuleVerifier.collectConditionReferences(plan).flatMap(_.qualifier)
      contract.inputs.flatMap(checkInput(_, outputContributingQualifiers, conditionReferencedQualifiers))
    case _ => Nil
  }

  private def checkInput(
      input: Dataset,
      outputContributingQualifiers: Set[String],
      conditionReferencedQualifiers: Set[String]
  ): Option[RoleConformanceCheckResult] =
    input.datasetType.flatMap { datasetType =>
      val contributesToOutput = StructuralVerifier.matchesAny(input.location, outputContributingQualifiers)
      val referencedInCondition = StructuralVerifier.matchesAny(input.location, conditionReferencedQualifiers)
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
  def violations(results: List[RoleConformanceCheckResult]): List[Violation] =
    results.filter(_.verdict == RoleConformanceVerdict.Contradicts).map(toViolation)

  private def toViolation(result: RoleConformanceCheckResult): Violation = Violation(
    ViolationType.RoleConsistencyViolation,
    s"input '${result.dataset}' is declared ${result.datasetType.name} but ${result.detail}",
    remediation =
      s"Review the transformation logic deriving output data from '${result.dataset}' - either declare it " +
        "DATA_ASSET/SOURCE instead of CONTROL if it genuinely is substantive business data, or remove the output " +
        "column(s) that derive from it if it is meant to remain control-only.",
    column = None,
    location = Some(result.dataset)
  )
}
