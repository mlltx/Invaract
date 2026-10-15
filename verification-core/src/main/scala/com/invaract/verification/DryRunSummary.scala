// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractDraftMerger, ContractParser, ContractValidator, LogicalSchema}
import com.invaract.ir.Plan
import com.invaract.verification.notification.{DryRunSummaryEvent, InferenceStatus, JobInfo, MergeStatus, SelfCheckStatus}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** The once-per-job dry-run summary every adapter publishes (`DryRunSummaryEvent`): how many writes landed in each
  * `InferenceStatus`, and every draft inferred merged (`ContractDraftMerger`) into one contract named after the job. Spark
  * builds it when the application ends; an adapter that sees the whole job before it runs (Beam) builds it straight away. */
private[invaract] object DryRunSummary {

  def event(statusCounts: Map[String, Long], drafts: List[Contract], job: JobInfo, timestamp: Long, metadata: Map[String, Any]): DryRunSummaryEvent = {
    val (mergeStatus, mergedYaml, conflicts) =
      if (drafts.isEmpty) (MergeStatus.NoDrafts, None, Nil)
      else
        Try(ContractDraftMerger.merge(drafts, mergedContractId(job), ContractInference.InferredVersion)) match {
          case Success(Right(merged))         => (MergeStatus.Merged, Some(ContractParser.write(merged)), Nil)
          case Success(Left(mergeConflicts)) => (MergeStatus.Conflict, None, mergeConflicts.map(_.message))
          case Failure(e)                     => (MergeStatus.Conflict, None, List(s"merge threw: $e"))
        }
    DryRunSummaryEvent(
      // a job that wrote nothing Invaract recognised reports it here, the only place that outcome is ever visible
      statusCounts = if (statusCounts.isEmpty) Map(InferenceStatus.NoWritesObserved -> 1L) else statusCounts,
      mergeStatus = mergeStatus,
      mergedContractYaml = mergedYaml,
      mergeConflicts = conflicts,
      job = job,
      timestamp = timestamp,
      metadata = metadata
    )
  }

  /** The merged contract's id: the job's stable identity, made safe for a contract id (`ContractValidator` wants a letter first,
    * then alphanumerics, `.`, `_` or `-`). */
  def mergedContractId(job: JobInfo): String = {
    val cleaned = job.jobId.orElse(job.name).map(_.replaceAll("[^A-Za-z0-9._-]", "_")).getOrElse("")
    if (cleaned.isEmpty) ContractInference.InferredId
    else if (cleaned.head.isLetter) cleaned
    else "job_" + cleaned
  }
}

/** A dry run's check of a draft against the very write it was inferred from (`ContractInferenceEvent.selfCheck`): a draft that
  * is not valid, or that its own write would not pass, describes the write wrongly, and says so rather than being published as
  * if it were sound. Both adapters run it; the arguments are those the adapter's enforcement passes `StructuralVerifier`. */
private[invaract] object DryRunSelfCheck {

  /** `(status, violations, diagnostics)`: a `SelfCheckStatus`, the violations behind `FAILED` or `INVALID`, and what went wrong
    * for `ERROR`. `outputSchema` is taken by name, so an adapter's conversion of the write's schema that throws is an `ERROR` too. */
  def of(
      draft: Contract,
      plan: Plan,
      inputSchemas: List[(String, LogicalSchema)],
      outputSchema: => LogicalSchema,
      options: VerificationOptions,
      caseSensitive: Boolean,
      lineageBoundaryTypes: Set[String]
  ): (String, List[Violation], List[String]) =
    try {
      val validation = ContractValidator.validate(draft)
      if (!validation.isValid) (SelfCheckStatus.Invalid, validation.errors.map(i => Violations.invalidInferredContract(i.path, i.message)), Nil)
      else {
        val result = StructuralVerifier.verify(draft, plan, inputSchemas, outputSchema, options, caseSensitive, lineageBoundaryTypes)
        (if (result.passed) SelfCheckStatus.Passed else SelfCheckStatus.Failed, result.violations, Nil)
      }
    } catch {
      case NonFatal(e) => (SelfCheckStatus.Error, Nil, List(s"self-check threw ${e.getClass.getSimpleName}: ${e.getMessage}"))
    }
}
