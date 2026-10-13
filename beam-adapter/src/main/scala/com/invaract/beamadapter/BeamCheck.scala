// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.verification.{AdapterCapabilities, CheckedWrite, ContractInference, InferredWrite, LocationMatching, VerificationOptions, VerificationPipeline}
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
      logger.warn(s"Invaract found no file write it recognises in this Beam pipeline, so nothing was checked against '${c.id}@${c.version}'.")
    translation.writes.foreach { w =>
      w.outputSchema match {
        case None =>
          VerificationPipeline.rejectUnverifiableWrite(
            c,
            s"a write to ${w.location} whose data exposes no schema (Beam shows only plain Java objects), so its columns cannot be checked",
            w.plan,
            sink,
            runId,
            Some(job)
          )
        case Some(schema) =>
          VerificationPipeline.verifyWrite(
            c,
            // Avro and Beam column names are case-sensitive; there is no cast, row-level change or checkpoint to account for
            CheckedWrite(BeamCheck.asDeclared(c, w), w.inputSchemas, schema, caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty),
            options,
            sink,
            runId,
            capabilities,
            Some(job)
          )
      }
    }
  }

  private def infer(translation: BeamTranslation): Unit = {
    translation.writes.foreach { w =>
      w.outputSchema match {
        case Some(schema) =>
          try {
            val draft = ContractInference.infer(InferredWrite(w.location, w.format, None, schema), w.inputSchemas, w.plan)
            report(InferenceStatus.Inferred, None, Some(w.location), Some(ContractParser.write(draft)))
          } catch { case NonFatal(e) => report(InferenceStatus.InferenceError, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), Some(w.location), None) }
        case None =>
          report(InferenceStatus.SkippedUnrecognized, Some(s"the write to ${w.location} exposes no schema to infer a contract from"), Some(w.location), None)
      }
    }
    translation.unverifiable.foreach(u => report(InferenceStatus.SkippedUnrecognized, Some(u.operation), None, None))
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

  /** The formats a text writer can be producing: Beam's text writers publish no delimiter or encoding, so a write through one
    * is `text` and nothing in the graph says whether its lines are CSV or JSON (docs/BEAM_ADAPTER.md, D5). */
  private val TextFamily = Set("text", "csv", "json")

  /** `write`'s plan, with a text write reported in the text-family format the contract declares for the dataset it lands on
    * (`csv` and `json` included), since the graph cannot tell them apart; a contract that declares any other format for it, or
    * none, sees `text`, and a mismatch stays a mismatch. The limit is the declaration's (`check.format` is partial), not a guess. */
  def asDeclared(contract: Contract, write: TranslatedWrite): com.invaract.ir.Write =
    if (!write.format.contains("text")) write.plan
    else {
      val declared = contract.outputs.find(o => LocationMatching.matches(o.location, write.location)).flatMap(_.format).map(_.trim.toLowerCase)
      declared.filter(TextFamily.contains).fold(write.plan)(f => write.plan.copy(format = Some(f)))
    }
}
