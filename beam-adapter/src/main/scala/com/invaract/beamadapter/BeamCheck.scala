// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.verification.{AdapterCapabilities, CheckedWrite, ContractInference, DryRunSummary, InferredWrite, OutputChecker, VerificationOptions, VerificationPipeline}
import com.invaract.verification.notification.{ContractInferenceEvent, InferenceStatus, JobInfo, NotificationSink}
import org.apache.beam.sdk.Pipeline
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** What a constructed Beam pipeline is checked against, and what happens to the outcome: the Beam adapter's whole
  * enforcement, run by `InvaractRunner` just before it hands the pipeline to the real runner (or by an explicit
  * `InvaractBeam.verify(pipeline)`), while nothing has started yet - which is what lets it refuse a job rather than only report on it.
  *
  * It owns the engine-specific half only: translating the pipeline (`BeamTranslator`) and handing each write over, as a
  * `CheckedWrite`, to `VerificationPipeline.verifyWrite`, which does the validation, the verifier order, the events and the
  * rejection text for every engine alike. In dry-run mode it infers a draft contract from each write instead and enforces nothing.
  */
final class BeamCheck private[beamadapter] (
    contract: Option[Contract],
    options: VerificationOptions,
    sink: Option[NotificationSink],
    job: JobInfo,
    capabilities: Option[AdapterCapabilities]
) {
  private val logger = LoggerFactory.getLogger(classOf[BeamCheck])
  private def runId: Option[String] = job.runId

  /** Whether this check enforces a contract (`false`: dry-run, which only infers drafts). */
  def enforces: Boolean = contract.isDefined

  /** Verifies every write `pipeline` makes and throws `ContractViolationException` for the first that breaks the contract
    * (or that cannot be verified), publishing each outcome to the sink first. In dry-run mode it throws nothing. */
  def verify(pipeline: Pipeline): Unit = {
    val translation = BeamTranslator.translate(pipeline)
    contract match {
      case Some(c) => enforce(c, translation)
      case None    => infer(translation)
    }
  }

  private def enforce(c: Contract, translation: BeamTranslation): Unit = {
    // a step that may change data but that cannot be checked is refused before any write is judged (fail closed)
    translation.unverifiable.headOption.foreach(u => VerificationPipeline.rejectUnverifiableWrite(c, u.operation, u.plan, sink, runId, Some(job)))
    if (translation.writes.isEmpty)
      logger.warn(s"Invaract found no write it recognises in this Beam pipeline, so nothing was checked against '${c.id}@${c.version}'.")
    translation.writes.foreach(w => BeamCheck.refusal(c, w, options).fold(check(c, w))(why => reject(c, why, w)))
  }

  private def check(c: Contract, w: TranslatedWrite): Unit =
    VerificationPipeline.verifyWrite(
      c,
      // Avro and Beam column names are case-sensitive; there is no cast, row-level change or checkpoint to account for. A read
      // the adapter sees but cannot locate is where a declared input may be read unseen: its plan node is that boundary.
      CheckedWrite(
        BeamCheck.asDeclared(c, w),
        w.inputSchemas,
        w.outputSchema.get, // `refusal` refuses a write with no schema before it gets here
        caseSensitive = true,
        rowMutation = None,
        lineageBoundaryTypes = Set(BeamTranslator.UnlocatedSourceType)
      ),
      options,
      sink,
      runId,
      capabilities,
      Some(job)
    )

  private def reject(c: Contract, why: String, w: TranslatedWrite): Unit =
    VerificationPipeline.rejectUnverifiableWrite(c, why, w.plan, sink, runId, Some(job))

  private def infer(translation: BeamTranslation): Unit = {
    val outcomes = translation.writes.map { w =>
      w.outputSchema match {
        case Some(schema) =>
          try {
            val draft = ContractInference.infer(InferredWrite(w.location, w.format, w.plan.saveMode, schema), w.inputSchemas, w.plan)
            // a draft that cannot list every input (a read it could not locate) is degraded, and says which read
            val (status, reason) =
              if (w.unlocatedReads.isEmpty) (InferenceStatus.Inferred, None)
              else (InferenceStatus.InferredDegraded, Some(s"inputs the pipeline reads but that cannot be located are missing from the draft: ${w.unlocatedReads.mkString("; ")}"))
            report(status, reason, Some(w.location), Some(ContractParser.write(draft)))
            status -> Some(draft)
          } catch {
            case NonFatal(e) =>
              report(InferenceStatus.InferenceError, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), Some(w.location), None)
              InferenceStatus.InferenceError -> None
          }
        case None =>
          report(InferenceStatus.SkippedUnrecognized, Some(s"the write to ${w.location} exposes no schema to infer a contract from"), Some(w.location), None)
          InferenceStatus.SkippedUnrecognized -> None
      }
    } ++ translation.unverifiable.map { u =>
      report(InferenceStatus.SkippedUnrecognized, Some(u.operation), None, None)
      InferenceStatus.SkippedUnrecognized -> None
    }
    // the whole job is known before it runs, so its summary is published straight away rather than when it ends; every draft
    // is merged, a degraded one included (as Spark merges it), so the summary describes every write a draft describes
    val counts = outcomes.groupBy(_._1).map { case (status, all) => status -> all.size.toLong }
    val mergeable = outcomes.flatMap(_._2)
    val summary = DryRunSummary.event(counts, mergeable, job, System.currentTimeMillis(), Map.empty)
    sink match {
      case Some(s) =>
        s.publish(summary)
        try s.flush(BeamCheck.FlushTimeoutMs)
        catch { case NonFatal(e) => logger.warn(s"Invaract dry run could not flush its sink: $e") }
      case None =>
        logger.info(s"Invaract dry run summary: ${summary.statusCounts.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString(", ")}; " +
          s"merge ${summary.mergeStatus}${summary.mergedContractYaml.map("\n" + _).getOrElse(summary.mergeConflicts.map("\n  " + _).mkString)}")
    }
  }

  private def report(status: String, reason: Option[String], location: Option[String], yaml: Option[String]): Unit =
    sink match {
      case Some(s) =>
        s.publish(ContractInferenceEvent(status, reason, location, yaml, None, Nil, Nil, None, job, System.currentTimeMillis(), Map.empty))
      case None =>
        logger.info(s"Invaract dry run: $status ${location.getOrElse("")}${yaml.map("\n" + _).getOrElse(reason.map(" - " + _).getOrElse(""))}")
    }
}

private[beamadapter] object BeamCheck {

  /** How long the dry run waits for its sink to deliver the job's events before the pipeline is handed on. */
  val FlushTimeoutMs = 10000L

  /** The formats a text writer can be producing: Beam's text writers publish no delimiter or encoding, so a write through one
    * is `text` and nothing in the graph says whether its lines are CSV or JSON (docs/BEAM_ADAPTER.md, D5). */
  private val TextFamily = Set("text", "csv", "json")

  /** `write`'s plan, with a text write reported in the text-family format the contract declares for the dataset it is checked
    * against (`csv` and `json` included), since the graph cannot tell them apart; a contract that declares any other format for
    * it, or none, sees `text`, and a mismatch stays a mismatch. The declared dataset is the one the core's format check uses
    * (`OutputChecker.expectedOutputFor`: a single-output contract's only output even when the location missed), so the two
    * never disagree. The limit is the declaration's (`check.format` is partial), not a guess. */
  def asDeclared(contract: Contract, write: TranslatedWrite): com.invaract.ir.Write =
    if (!write.format.contains("text")) write.plan
    else declaredFormat(contract, write).filter(TextFamily.contains).fold(write.plan)(f => write.plan.copy(format = Some(f)))

  /** The format the contract declares for the dataset `write` is checked against, normalised as the core compares it. */
  def declaredFormat(contract: Contract, write: TranslatedWrite): Option[String] =
    OutputChecker.expectedOutputFor(contract.outputs, write.location).flatMap(_.format).map(_.trim.toLowerCase).filter(_.nonEmpty)

  /** The save mode the contract declares for the dataset `write` is checked against. */
  def declaredSaveMode(contract: Contract, write: TranslatedWrite): Option[String] =
    OutputChecker.expectedOutputFor(contract.outputs, write.location).flatMap(_.saveMode).map(_.trim.toLowerCase).filter(_.nonEmpty)

  /** Why `write` is refused without being checked, if it is: what the contract asks about it cannot be told from the pipeline.
    * The core skips a comparison when the actual side is unknown, so each of these would otherwise pass unchecked
    * (docs/BEAM_ADAPTER.md, D10 to D12). */
  def refusal(contract: Contract, write: TranslatedWrite, options: VerificationOptions): Option[String] =
    if (write.outputSchema.isEmpty)
      Some(s"a write to ${write.location} whose data exposes no schema (Beam shows only plain Java objects), so its columns cannot be checked")
    else if (write.format.isEmpty && declaredFormat(contract, write).isDefined)
      // the core skips a format check when the actual format is unknown, so a named format would pass a write nobody can name
      Some(s"a write to ${write.location} through a sink whose file format cannot be told, while the contract declares '${declaredFormat(contract, write).get}'")
    else if (write.plan.saveMode.isEmpty && declaredSaveMode(contract, write).isDefined)
      // a file write has no save mode in Beam (it always replaces the files it names): only a BigQuery write states one
      Some(s"a write to ${write.location} that states no save mode (only a BigQuery write does), while the contract declares '${declaredSaveMode(contract, write).get}'")
    else if (options.rejectUndeclaredInputs && write.unlocatedReads.nonEmpty)
      // strict mode promises no read goes undeclared, which cannot be shown for a read whose source is not known
      Some(s"the write to ${write.location} depends on reads whose source cannot be located (${write.unlocatedReads.mkString("; ")}), so an undeclared input cannot be ruled out")
    else None
}
