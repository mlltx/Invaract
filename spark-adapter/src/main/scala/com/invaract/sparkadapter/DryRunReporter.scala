// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{ContractInference, StructuralVerifier, VerificationOptions, Violation, Violations}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import com.invaract.contract.{Contract, ContractDraftMerger, ContractParser, ContractValidator}
import com.invaract.fingerprint.{TransformationFingerprint, TransformationFingerprinter}
import com.invaract.sparkadapter.ContractEnforcementRule.InferenceOutcome
import com.invaract.verification.notification.{
  ContractInferenceEvent,
  DryRunSummaryEvent,
  InferenceStatus,
  JobInfo,
  NotificationEvent,
  NotificationSink
}

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

/** Dry-run mode's reporting half: where `ContractEnforcementRule.dryRun` hands a
  * caller-supplied callback each inferred contract and stays silent about
  * everything else, this publishes a `ContractInferenceEvent` to a
  * `NotificationSink` for *every* write-shaped plan dry-run mode met - the
  * ones it could infer a contract from and the ones it could not - and one
  * `DryRunSummaryEvent` when the application ends.
  *
  * The purpose is migration: the `INFERRED` events are raw material for a
  * contract repository, and the rest tell a platform team how much they can
  * trust turning enforcement on for this job (see `InferenceStatus`). Two
  * mechanisms carry that second purpose:
  *
  *  - Each inferred draft is verified against the plan it came from, under
  *    the options enforcement would use (`ContractInferenceEvent.selfCheck`).
  *  - The summary counts every status and so makes "this job ran and Invaract
  *    saw no write at all" visible - a state with no per-write event to carry it.
  *
  * Never throws into the job: inference, self-check, fingerprinting and
  * publishing are each failure-isolated. A broken sink or an unexpected plan
  * shape costs a log line, not a query.
  *
  * Identical drafts and identical skips are reported once per reporter
  * (`injectCheckRule` fires on every analyzed plan, so one logical write can
  * arrive several times - see `ContractEnforcementRule.dryRun`).
  *
  * @param options what the self-check verifies under: the job's own
  *   `spark.invaract.*` conf, so "PASSED" means "would pass as configured".
  * @param contractMetadata user-defined metadata about the *contract*
  *   (`spark.invaract.contract.metadata.<key>`): put in every inferred draft's
  *   `extensions` - the same bag a hand-written contract keeps its owner/team/
  *   source system in - and echoed as every event's `metadata`. Because
  *   enforcement copies `Contract.extensions` into `ContractValidationEvent`/
  *   `WriteEvent.metadata`, the same keys then appear on the enforcement
  *   events once a draft is promoted: one set of keys links a dry-run event to
  *   the events of the real runs that follow. (Facts about one *run* belong in
  *   `job.attributes` instead: they must not persist in a contract.)
  */
private[sparkadapter] final class DryRunReporter(
    sink: NotificationSink,
    job: JobInfo,
    options: VerificationOptions,
    clock: () => Long = () => System.currentTimeMillis(),
    // A seam, not a feature: lets a test make inference throw, which no real plan reliably does.
    infer: (LogicalPlan, CheckpointRegistry) => Option[InferenceOutcome] =
      (plan, registry) => ContractEnforcementRule.inferOutcome(plan, Some(registry)),
    contractMetadata: Map[String, String] = Map.empty
) {
  import DryRunReporter._

  private val registry = new CheckpointRegistry
  private val lock = new Object
  private val reported = mutable.Set.empty[String]
  private val counts = mutable.LinkedHashMap.empty[String, Long]
  private val drafts = mutable.ListBuffer.empty[Contract]
  private val finished = new AtomicBoolean(false)

  /** The check rule body: offered every analyzed plan the session produces. Everything
    * past this point - inference, classification, self-check, fingerprinting, rendering -
    * is inside one failure boundary, so no bug in any of it can reach the job.
    */
  def check(plan: LogicalPlan): Unit =
    try {
      infer(plan, registry).foreach {
        case inferred: InferenceOutcome.Inferred => reportInferred(inferred)
        case InferenceOutcome.Skipped(status, reason) =>
          record(s"$status:$reason", status, None)(eventOf(status, Some(reason), None, None, None, Nil, Nil, None))
      }
    } catch {
      case NonFatal(e)           => reportFailure(e)
      case e: StackOverflowError => reportFailure(e)
    }

  private def reportFailure(e: Throwable): Unit = {
    val reason = s"${e.getClass.getSimpleName}: ${e.getMessage}"
    record(s"${InferenceStatus.InferenceError}:$reason", InferenceStatus.InferenceError, None)(
      eventOf(InferenceStatus.InferenceError, Some(reason), None, None, None, Nil, Nil, None)
    )
  }

  private def reportInferred(untagged: InferenceOutcome.Inferred): Unit = {
    // Tagged before anything else looks at the draft, so the YAML that is published, the draft that
    // is self-checked and the draft that is merged at the end are all the same contract.
    val inferred = untagged.copy(contract = untagged.contract.copy(extensions = untagged.contract.extensions ++ contractMetadata))
    val yaml = ContractParser.write(inferred.contract)
    val degradation = degradationOf(inferred)
    val status = if (degradation.isEmpty) InferenceStatus.Inferred else InferenceStatus.InferredDegraded
    val (selfCheck, selfCheckViolations, selfCheckNote) = selfCheckOf(inferred)
    val location = inferred.contract.outputs.headOption.map(_.location)
    val diagnostics = degradation ++ selfCheckNote
    record(s"draft:$yaml", status, Some(inferred.contract))(
      eventOf(
        status,
        degradation.headOption,
        location,
        Some(yaml),
        Some(selfCheck),
        selfCheckViolations,
        diagnostics,
        fingerprintOf(inferred)
      )
    )
  }

  /** Why this draft is a weaker description of the write than a plain `INFERRED` one.
    *
    * Deliberately narrow. A draft's inputs and schemas come from the Catalyst plan
    * itself, not from the translated IR, so an IR node Invaract merely has no
    * translation for (`Range`, a UDF, ...) does not make them less trustworthy - flagging
    * it would mark nearly every real job degraded and make `INFERRED` unreachable.
    * What does: a lineage boundary the plan cannot be seen through, a resolution that
    * had to guess, a write shape that recorded a caveat, and row-level DML.
    */
  private def degradationOf(inferred: InferenceOutcome.Inferred): List[String] = {
    val assumedResolution =
      inferred.translated.diagnostics.filter(_.nodeType == CheckpointRegistry.ResolutionDiagnosticType).map(_.message)
    val unresolved = StructuralVerifier.collectUnknownPlans(inferred.translated.plan).map(_.sourceType)
      .filter(CheckpointRegistry.BoundarySourceTypes.contains).distinct
      .map(source => s"an unresolved .checkpoint()/cache boundary ($source): inputs behind it are not in this draft")
    val writeShape = inferred.writeInfo.diagnostic.map(d => s"${d.nodeType}: ${d.message}").toList
    val dml =
      if (RowMutationSupport.classify(inferred.plan).isDefined)
        List(
          "row-level DML (MERGE/UPDATE/DELETE): the draft describes the target's current schema; " +
            "the operation's own logic is not inferred"
        )
      else Nil
    (unresolved ++ assumedResolution ++ writeShape ++ dml).distinct
  }

  /** `(verdict, violations, note)`: verdict is `PASSED`, `FAILED`, `INVALID` or `ERROR`. */
  private def selfCheckOf(inferred: InferenceOutcome.Inferred): (String, List[Violation], List[String]) =
    try {
      val validation = ContractValidator.validate(inferred.contract)
      if (!validation.isValid) {
        val violations = validation.errors.map(issue => Violations.invalidInferredContract(issue.path, issue.message))
        (SelfCheckInvalid, violations, Nil)
      } else {
        val result = StructuralVerifier.verify(
          inferred.contract,
          inferred.translated.plan,
          inferred.inputSchemas,
          SparkSchemas.toLogicalSchema(inferred.writeInfo.outputSchema),
          options,
          caseSensitive = org.apache.spark.sql.internal.SQLConf.get.caseSensitiveAnalysis,
          lineageBoundaryTypes = CheckpointRegistry.BoundarySourceTypes
        )
        (if (result.passed) SelfCheckPassed else SelfCheckFailed, result.violations, Nil)
      }
    } catch {
      case NonFatal(e) => (SelfCheckError, Nil, List(s"self-check threw ${e.getClass.getSimpleName}: ${e.getMessage}"))
    }

  private def fingerprintOf(inferred: InferenceOutcome.Inferred): Option[TransformationFingerprint] =
    Try {
      val mutation = RowMutationSupport.classify(inferred.plan).collect {
        case RowMutationSupport.Classification.Extracted(_, m) => m
      }
      TransformationFingerprinter.fingerprint(inferred.translated.plan, mutation)
    }.toOption

  private def eventOf(
      status: String,
      reason: Option[String],
      writeLocation: Option[String],
      contractYaml: Option[String],
      selfCheck: Option[String],
      selfCheckViolations: List[Violation],
      diagnostics: List[String],
      fingerprints: Option[TransformationFingerprint]
  ): ContractInferenceEvent =
    ContractInferenceEvent(
      status = status,
      reason = reason,
      writeLocation = writeLocation,
      contractYaml = contractYaml,
      selfCheck = selfCheck,
      selfCheckViolations = selfCheckViolations,
      diagnostics = diagnostics,
      fingerprints = fingerprints,
      job = job,
      timestamp = clock(),
      metadata = contractMetadata
    )

  /** Counts, de-duplicates and publishes one event. `draft` is kept for the
    * end-of-job merge only when this is a first sighting.
    */
  private def record(key: String, status: String, draft: Option[Contract])(event: => ContractInferenceEvent): Unit = {
    val firstSighting = lock.synchronized {
      val isNew = reported.add(key)
      if (isNew) {
        counts(status) = counts.getOrElse(status, 0L) + 1L
        draft.foreach(drafts += _)
      }
      isNew
    }
    if (firstSighting) emit(event)
  }

  private def emit(event: => NotificationEvent): Unit =
    try sink.publish(event)
    catch { case NonFatal(e) => logger.warn(s"Dry-run reporting could not publish an event: $e") }

  /** Publishes the once-per-job summary, then gives the sink a bounded chance
    * to finish in-flight delivery - a short job can otherwise exit before an
    * asynchronous sink's last request completes. Idempotent.
    */
  def finish(): Unit =
    if (finished.compareAndSet(false, true)) {
      val (statusCounts, mergeable) = lock.synchronized((counts.toMap, drafts.toList))
      val (mergeStatus, mergedYaml, conflicts) =
        if (mergeable.isEmpty) (MergeNoDrafts, None, Nil)
        else
          Try(ContractDraftMerger.merge(mergeable, mergedContractId, ContractInference.InferredVersion)) match {
            case scala.util.Success(Right(merged))    => (MergeMerged, Some(ContractParser.write(merged)), Nil)
            case scala.util.Success(Left(mergeConflicts)) => (MergeConflict, None, mergeConflicts.map(_.message))
            case scala.util.Failure(e)                => (MergeConflict, None, List(s"merge threw: $e"))
          }
      emit(
        DryRunSummaryEvent(
          statusCounts = if (statusCounts.isEmpty) Map(InferenceStatus.NoWritesObserved -> 1L) else statusCounts,
          mergeStatus = mergeStatus,
          mergedContractYaml = mergedYaml,
          mergeConflicts = conflicts,
          job = job,
          timestamp = clock(),
          metadata = contractMetadata
        )
      )
      try sink.flush(FlushTimeoutMs)
      catch { case NonFatal(e) => logger.warn(s"Dry-run reporting could not flush its sink: $e") }
    }

  /** The merged contract's id: the job's stable identity, made safe for a contract id
    * (`ContractValidator` wants a letter first, then alphanumerics/`.`/`_`/`-`).
    */
  private def mergedContractId: String = {
    val cleaned = job.jobId.orElse(job.appName).map(_.replaceAll("[^A-Za-z0-9._-]", "_")).getOrElse("")
    if (cleaned.isEmpty) ContractInference.InferredId
    else if (cleaned.head.isLetter) cleaned
    else "job_" + cleaned
  }
}

private[sparkadapter] object DryRunReporter {
  private val logger = LoggerFactory.getLogger(classOf[DryRunReporter])

  val SelfCheckPassed = "PASSED"
  val SelfCheckFailed = "FAILED"
  val SelfCheckInvalid = "INVALID"
  val SelfCheckError = "ERROR"

  val MergeMerged = "MERGED"
  val MergeConflict = "CONFLICT"
  val MergeNoDrafts = "NO_DRAFTS"

  /** How long `finish` lets an asynchronous sink drain. Bounded: this runs on Spark's
    * listener thread during shutdown, and must never be able to hang it.
    */
  val FlushTimeoutMs = 10000L

  /** `spark.invaract.jobId` - the stable identity of this job across runs. Unlike
    * `spark.app.name`, which many schedulers vary per run, it lets a consumer
    * recognize "this is the same job as last night".
    */
  val JobIdConfKey = "spark.invaract.jobId"

  /** `spark.invaract.job.metadata.<key>=<value>` entries ride along on every dry-run event as
    * `job.attributes`, prefix stripped (DAG id, orchestrator run id, trigger, ...): facts about
    * *this run*, which is why they are not put in the contract (see `ContractMetadataConfPrefix`).
    */
  val JobMetadataConfPrefix = "spark.invaract.job.metadata."

  /** `spark.invaract.contract.metadata.<key>=<value>` entries become the inferred draft's
    * `extensions` (and every event's `metadata`), prefix stripped: owner, team, source system,
    * a ticket - anything that describes the contract rather than one run of the job.
    */
  val ContractMetadataConfPrefix = "spark.invaract.contract.metadata."

  /** Every conf entry under `prefix` with something after it, prefix stripped. */
  private def withPrefix(session: SparkSession, prefix: String): Map[String, String] =
    session.conf.getAll.collect {
      case (key, value) if key.startsWith(prefix) && key.length > prefix.length => key.stripPrefix(prefix) -> value
    }

  def contractMetadataOf(session: SparkSession): Map[String, String] = withPrefix(session, ContractMetadataConfPrefix)

  def jobInfoOf(session: SparkSession): JobInfo = {
    val sc = session.sparkContext
    JobInfo(
      applicationId = Some(sc.applicationId),
      appName = Some(sc.appName),
      jobId = session.conf.getOption(JobIdConfKey).map(_.trim).filter(_.nonEmpty),
      sparkVersion = Some(sc.version),
      master = Some(sc.master),
      deployMode = Some(sc.deployMode),
      user = Some(sc.sparkUser),
      startTimeMs = Some(sc.startTime),
      attributes = withPrefix(session, JobMetadataConfPrefix)
    )
  }

  /** Builds the reporter for `session` and arranges for its summary to be published
    * when the application ends. Spark delivers `SparkListenerApplicationEnd` from
    * `SparkContext.stop`, which its own shutdown hook calls for a job that never
    * stops the session itself.
    */
  def installFor(session: SparkSession, sink: NotificationSink): DryRunReporter = {
    VersionCompatibilityGuard.check(session)
    val reporter = new DryRunReporter(
      sink,
      jobInfoOf(session),
      ContractEnforcementRule.resolveVerificationOptions(VerificationOptions(), session),
      contractMetadata = contractMetadataOf(session)
    )
    session.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = reporter.finish()
    })
    reporter
  }
}
