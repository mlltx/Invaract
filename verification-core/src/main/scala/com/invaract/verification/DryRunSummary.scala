// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractDraftMerger, ContractParser, ContractValidator, LogicalSchema}
import com.invaract.ir.Plan
import com.invaract.verification.notification.{
  ContractInferenceEvent, DryRunSummaryEvent, InferenceStatus, JobInfo, MergeStatus, NotificationSink, SelfCheckStatus
}
import org.slf4j.Logger

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

/** How an adapter that sees each write as it is judged (Beam's pipeline, the Python I/O adapter's run) reports its dry run: each
  * write's draft (`ContractInferenceEvent`) and the job's summary (`DryRunSummaryEvent`) published to the sink, or, with none,
  * logged at INFO, in the same words for every such adapter. Spark reports through `DryRunReporter`, which publishes at the end of
  * the application. */
private[invaract] object DryRunReporting {

  /** One write's outcome: `status` an `InferenceStatus`, `yaml` the draft, `selfCheck` its `DryRunSelfCheck`. */
  def inference(
      sink: Option[NotificationSink],
      logger: Logger,
      job: JobInfo,
      status: String,
      reason: Option[String],
      location: Option[String],
      yaml: Option[String],
      selfCheck: Option[(String, List[Violation], List[String])]
  ): Unit =
    sink match {
      case Some(s) =>
        s.publish(
          ContractInferenceEvent(
            status, reason, location, yaml, selfCheck.map(_._1), selfCheck.fold(List.empty[Violation])(_._2), selfCheck.fold(List.empty[String])(_._3),
            None, job, System.currentTimeMillis(), Map.empty
          )
        )
      case None =>
        logger.info(s"Invaract dry run: $status ${location.getOrElse("")}${yaml.map("\n" + _).getOrElse(reason.map(" - " + _).getOrElse(""))}")
    }

  /** The job's summary: `outcomes` is each write's status, with its draft when one was inferred. */
  def summary(sink: Option[NotificationSink], logger: Logger, job: JobInfo, outcomes: Seq[(String, Option[Contract])]): Unit = {
    val counts = outcomes.groupBy(_._1).map { case (status, all) => status -> all.size.toLong }
    val summary = DryRunSummary.event(counts, outcomes.flatMap(_._2).toList, job, System.currentTimeMillis(), Map.empty)
    sink match {
      case Some(s) => s.publish(summary)
      case None =>
        logger.info(s"Invaract dry run summary: ${summary.statusCounts.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString(", ")}; " +
          s"merge ${summary.mergeStatus}${summary.mergedContractYaml.map("\n" + _).getOrElse(summary.mergeConflicts.map("\n  " + _).mkString)}")
    }
  }

  /** Waits up to `timeoutMs` for the sink to deliver what it was given; a sink that cannot is logged, never thrown, so a run's
    * own outcome does not depend on its notifications. */
  def flush(sink: Option[NotificationSink], logger: Logger, timeoutMs: Long): Unit =
    sink.foreach { s =>
      try s.flush(timeoutMs)
      catch { case NonFatal(e) => logger.warn(s"Invaract could not flush its notification sink: $e") }
    }
}
