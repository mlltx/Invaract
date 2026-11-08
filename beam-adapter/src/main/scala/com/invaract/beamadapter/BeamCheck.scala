// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Contract, ContractParser, Dataset, Formats, LogicalSchema, RuleType}
import com.invaract.ir.{UnknownPlan, Write}
import com.invaract.verification.{
  AdapterCapabilities, CheckedWrite, ContractInference, DryRunReporting, DryRunSelfCheck, InferredWrite, InputChecker, LocationMatching, OutputChecker, TrustedSteps,
  VerificationOptions, VerificationPipeline, Violation
}
import com.invaract.verification.notification.{InferenceStatus, JobInfo, NotificationSink}
import org.apache.beam.sdk.Pipeline
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** What a constructed Beam pipeline is checked against, and what happens to the outcome: the Beam adapter's whole
  * enforcement, run by `InvaractRunner` just before it hands the pipeline to the real runner (or by an explicit
  * `InvaractBeam.verify(pipeline)`), while nothing has started yet - which is what lets it refuse a job rather than only report on it.
  *
  * It owns the engine-specific half only: translating the pipeline (`BeamTranslator`) and handing each write over, as a
  * `CheckedWrite`, to `VerificationPipeline.verifyWrite`, which does the validation, the verifier order, the events and the
  * rejection text for every engine alike. In dry-run mode it infers a draft contract from each write instead and enforces nothing;
  * `contractMetadata` (the platform's `contractMetadata` setting) goes into every draft's `extensions` and every dry-run event.
  */
final class BeamCheck private[beamadapter] (
    contract: Option[Contract],
    options: VerificationOptions,
    sink: Option[NotificationSink],
    job: JobInfo,
    capabilities: Option[AdapterCapabilities],
    trusted: TrustedSteps = TrustedSteps.Nobody,
    advice: BeamAdvice = BeamAdvice.Java,
    contractMetadata: Map[String, String] = Map.empty
) {
  private val logger = LoggerFactory.getLogger(classOf[BeamCheck])
  private def runId: Option[String] = job.runId

  /** Whether this check enforces a contract (`false`: dry-run, which only infers drafts). */
  def enforces: Boolean = contract.isDefined

  /** Verifies every write `pipeline` makes and throws `ContractViolationException` for the first that breaks the contract
    * (or that cannot be verified), publishing each outcome to the sink first. In dry-run mode it throws nothing. */
  def verify(pipeline: Pipeline): Unit = verifyTranslation(BeamTranslator.translate(pipeline))

  /** `verify`, for a pipeline already translated: how a pipeline read from its portable form is checked (`PortableCheck`). */
  private[beamadapter] def verifyTranslation(translated: BeamTranslation): Unit = {
    // a step that ends the pipeline is let through when it reads only what writes report, or when the platform trusts it (D14)
    translated.reportSteps.foreach(s => logger.info(s"Invaract: $s ends the pipeline but reads only what writes report (file or table names), not data; let through"))
    val (trustedSteps, unverifiable) = translated.unverifiable.partition(u => u.trustable && trusted.trusts(u.name))
    trustedSteps.foreach(u => logger.info(s"Invaract: ${u.name} ends the pipeline without a recognised write and is a trusted step; let through"))
    val translation = translated.copy(unverifiable = unverifiable)
    contract match {
      case Some(c) => enforce(c, translation)
      case None    => infer(translation)
    }
  }

  private def enforce(c: Contract, translation: BeamTranslation): Unit = {
    val pipelinePlan = UnknownPlan("the pipeline's writes", "Pipeline", translation.writes.map(_.plan))
    // a contract that asks for something this adapter declares it cannot check is refused for that, first
    capabilities.foreach(caps => VerificationPipeline.requireSupported(c, options, caps, pipelinePlan, sink, runId, Some(job)))
    // a step that may change data but that cannot be checked is refused before any write is judged (fail closed)
    translation.unverifiable.headOption.foreach { u =>
      val operation = if (u.trustable) advice.endingStep(u.name, u.operation) else advice.unplacedWrite(u.name, u.operation)
      VerificationPipeline.rejectUnverifiableWrite(c, operation, u.plan, sink, runId, Some(job))
    }
    // the whole pipeline is known: an output the contract declares that no write lands on refuses the job (docs/BEAM_ADAPTER.md, D17)
    VerificationPipeline.requireOutputsWritten(c, translation.writes.map(_.location), pipelinePlan, sink, runId, Some(job))
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
        // `refusal` refuses a write with no schema unless the output it lands on declares no columns either
        w.outputSchema.getOrElse(LogicalSchema(Nil)),
        caseSensitive = true,
        rowMutation = None,
        lineageBoundaryTypes = Set(BeamTranslator.UnlocatedSourceType),
        inputFormats = BeamCheck.inputFormatsAsDeclared(c, w),
        // what a CsvIO write or parse states (check.csvOptions); `refusal` refuses a declared option no step states
        formatOptions = w.formatOptions
      ),
      options,
      sink,
      runId,
      capabilities,
      Some(job)
    )

  private def reject(c: Contract, why: String, w: TranslatedWrite): Unit =
    VerificationPipeline.rejectUnverifiableWrite(c, advice.unstatedWrite(w.location, why), w.plan, sink, runId, Some(job))

  private def infer(translation: BeamTranslation): Unit = {
    val outcomes = translation.writes.map { w =>
      // a write whose columns cannot be told gets a draft that declares none for it (D18)
      val (schema, noColumns) = w.outputSchema.fold[(LogicalSchema, Option[String])](
        LogicalSchema(Nil) -> Some(s"the write to ${w.location} exposes no columns, so the draft declares none for it")
      )(_ -> None)
      try {
        val draft = DryRunReporting.tagged(
          ContractInference.infer(InferredWrite(w.location, w.format, w.plan.saveMode, schema, formatOptions = w.formatOptions, inputFormats = w.readFormats), w.draftInputs, w.plan),
          contractMetadata
        )
        // a draft that cannot list every input (a read it could not locate) is degraded, and says which read
        val unlocated =
          if (w.unlocatedReads.isEmpty) None
          else Some(s"inputs the pipeline reads but that cannot be located are missing from the draft: ${w.unlocatedReads.mkString("; ")}")
        // a read the pipeline states no columns for is listed, with none, and the draft says so: a reviewer fills them in
        val inputsWithoutColumns =
          if (w.readsWithoutColumns.isEmpty) None
          else Some(s"the pipeline states no columns for the read of ${w.readsWithoutColumns.mkString(", ")}, so the draft declares none for it")
        val degradations = noColumns.toList ++ unlocated ++ inputsWithoutColumns
        val (status, reason) =
          if (degradations.isEmpty) (InferenceStatus.Inferred, None) else (InferenceStatus.InferredDegraded, Some(degradations.mkString("; ")))
        // the draft checked against the write it describes, as enforcement would check it
        val selfCheck = DryRunSelfCheck.of(
          draft, BeamCheck.asDeclared(draft, w), w.inputSchemas, schema, options, caseSensitive = true, Set(BeamTranslator.UnlocatedSourceType)
        )
        report(status, reason, Some(w.location), Some(ContractParser.write(draft)), Some(selfCheck))
        status -> Some(draft)
      } catch {
        case NonFatal(e) =>
          report(InferenceStatus.InferenceError, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), Some(w.location), None)
          InferenceStatus.InferenceError -> None
      }
    } ++ translation.unverifiable.map { u =>
      report(InferenceStatus.SkippedUnrecognized, Some(u.operation), None, None)
      InferenceStatus.SkippedUnrecognized -> None
    }
    // the whole job is known before it runs, so its summary is published straight away rather than when it ends; every draft
    // is merged, a degraded one included (as Spark merges it), so the summary describes every write a draft describes
    DryRunReporting.summary(sink, logger, job, outcomes, contractMetadata)
    DryRunReporting.flush(sink, logger, BeamCheck.FlushTimeoutMs)
  }

  private def report(
      status: String,
      reason: Option[String],
      location: Option[String],
      yaml: Option[String],
      selfCheck: Option[(String, List[Violation], List[String])] = None
  ): Unit = DryRunReporting.inference(sink, logger, job, status, reason, location, yaml, selfCheck, contractMetadata)
}

private[beamadapter] object BeamCheck {

  /** How long the dry run waits for its sink to deliver the job's events before the pipeline is handed on. */
  val FlushTimeoutMs = 10000L

  /** The formats a text writer can be producing: Beam's text writers publish no delimiter or encoding, so a write through one
    * is `text` and nothing in the graph says whether its lines are CSV or JSON (docs/BEAM_ADAPTER.md, D5). */
  private val TextFamily = Set(Formats.Text, Formats.Csv, Formats.Json)

  /** `write`'s plan, with a text write reported in the text-family format the contract declares for the dataset it is checked
    * against (`csv` and `json` included), since the graph cannot tell them apart; a contract that declares any other format for
    * it, or none, sees `text`, and a mismatch stays a mismatch. The declared dataset is the one the core's format check uses
    * (`OutputChecker.expectedOutputFor`: a single-output contract's only output even when the location missed), so the two
    * never disagree. The limit is the declaration's (`check.format` is partial), not a guess. */
  def asDeclared(contract: Contract, write: TranslatedWrite): Write =
    if (!write.format.contains(Formats.Text)) write.plan
    else declaredFormat(contract, write).filter(TextFamily.contains).fold(write.plan)(f => write.plan.copy(format = Some(f)))

  /** `write`'s read formats, a text read reported in the text-family format its declared input declares (`csv` and `json`
    * included), for the reason `asDeclared` gives a text write's: nothing in the graph says what its lines are. A read the
    * contract declares any other format for, or none, stays `text`, and a mismatch stays a mismatch. The declared input is
    * the one the core's input format check matches the read to. */
  def inputFormatsAsDeclared(contract: Contract, write: TranslatedWrite): Map[String, String] = {
    val declared = InputChecker.Declared(contract.inputs)
    write.readFormats.map {
      case (location, Formats.Text) =>
        val matched = declared.index.matchingIndicesOf(LocationMatching.namesOf(location, None)).headOption.flatMap(declared.inputs(_).format)
        location -> matched.map(_.trim.toLowerCase).filter(TextFamily.contains).getOrElse(Formats.Text)
      case other => other
    }
  }

  /** The format the contract declares for the dataset `write` is checked against, normalised as the core compares it. */
  def declaredFormat(contract: Contract, write: TranslatedWrite): Option[String] =
    OutputChecker.expectedOutputFor(contract.outputs, write.location).flatMap(_.format).map(_.trim.toLowerCase).filter(_.nonEmpty)

  /** Whether the dataset `write` is checked against declares columns (`true` when it lands on no declared output). One that
    * declares none (a dead-letter folder of text lines) does not govern its content, so a write whose columns cannot be told
    * is no reason to refuse it (docs/BEAM_ADAPTER.md, D18). */
  def declaresColumns(contract: Contract, write: TranslatedWrite): Boolean =
    OutputChecker.expectedOutputFor(contract.outputs, write.location).forall(_.schema.fields.nonEmpty)

  /** The save mode the contract declares for the dataset `write` is checked against. */
  def declaredSaveMode(contract: Contract, write: TranslatedWrite): Option[String] =
    OutputChecker.expectedOutputFor(contract.outputs, write.location).flatMap(_.saveMode).map(_.trim.toLowerCase).filter(_.nonEmpty)

  /** Why `write` is refused without being checked, if it is: what the contract asks about it cannot be told from the pipeline.
    * The core skips a comparison when the actual side is unknown, so each of these would otherwise pass unchecked
    * (docs/BEAM_ADAPTER.md, D10 to D12). */
  def refusal(contract: Contract, write: TranslatedWrite, options: VerificationOptions): Option[String] = {
    // the core skips a comparison whose actual side is unknown, so a format or save mode the contract names would pass unseen
    lazy val unknownFormat = if (write.format.isEmpty) declaredFormat(contract, write) else None
    // a file write has no save mode in Beam (it always replaces the files it names): only a BigQuery write states one
    lazy val unknownSaveMode = if (write.plan.saveMode.isEmpty) declaredSaveMode(contract, write) else None
    // a CSV option the contract declares that no CsvIO step states would pass unchecked (check.csvOptions is partial)
    lazy val unstatedCsv = unstatedCsvOptions(contract, write)
    if (write.outputSchema.isEmpty && declaresColumns(contract, write))
      Some(s"a write to ${write.location} whose data exposes no schema (its items are plain objects, not rows with a schema), so its columns cannot be checked")
    else if (unknownFormat.isDefined)
      Some(s"a write to ${write.location} through a sink whose file format cannot be told, while the contract declares '${unknownFormat.get}'")
    else if (unknownSaveMode.isDefined)
      Some(s"a write to ${write.location} that states no save mode (only a BigQuery write does), while the contract declares '${unknownSaveMode.get}'")
    else if (unstatedCsv.isDefined)
      unstatedCsv
    else if (options.rejectUndeclaredInputs && write.unlocatedReads.nonEmpty)
      // strict mode promises no read goes undeclared, which cannot be shown for a read whose source is not known
      Some(s"the write to ${write.location} depends on reads whose source cannot be located (${write.unlocatedReads.mkString("; ")}), so an undeclared input cannot be ruled out")
    else if (ruleTypes(contract).exists(RuleType.PlanShapeTypes.contains) && write.unreadableShapes.nonEmpty)
      // a schema transform it could not read could be the filter, grouping or join a rule asks about (D19)
      Some(s"the write to ${write.location} depends on schema transforms whose fields could not be read on this Beam release (${write.unreadableShapes.mkString("; ")}), so the contract's transformation-shape rules cannot be checked")
    else if (ruleTypes(contract).contains(RuleType.ForbidCrossJoin) && write.opaqueCombines.nonEmpty)
      // a join written as code is not seen, and the rule passes when no join is seen: refuse rather than pass a cross join unseen (D19)
      Some(s"the write to ${write.location} depends on steps that combine several collections in code (${write.opaqueCombines.mkString("; ")}), any of which could be a cross join, so forbid_cross_join cannot be checked")
    else None
  }

  private def ruleTypes(contract: Contract): Set[String] = contract.rules.map(_.ruleType).toSet

  /** Why `write`, or a read it depends on, cannot be held to the CSV options its declared dataset names: only a `CsvIO` write or
    * parse states its options (`BeamIO.csvOptions`), and never a date or timestamp pattern, so a text write or read whose lines
    * are not CSV to Beam would otherwise pass any declared option unseen. A write or read in a format other than text or csv is
    * left to the format check. */
  def unstatedCsvOptions(contract: Contract, write: TranslatedWrite): Option[String] = {
    def unstated(dataset: Dataset, location: String): List[String] =
      dataset.csv.toList.flatMap(_.declared).map(_._1).filterNot(write.formatOptions.getOrElse(location, Map.empty).contains)
    def couldBeCsv(format: Option[String]) = format.forall(f => f == Formats.Text || f == Formats.Csv)
    val output = OutputChecker.expectedOutputFor(contract.outputs, write.location).filter(_ => couldBeCsv(write.format))
      .map(d => unstated(d, write.location)).filter(_.nonEmpty)
      .map(names => s"a write to ${write.location} that does not state the CSV options ${names.mkString(", ")} (only a CsvIO write states its options, and never a date or timestamp pattern), while the contract declares them")
    val declared = InputChecker.Declared(contract.inputs)
    lazy val input = write.readLocations.distinct.filter(l => couldBeCsv(write.readFormats.get(l))).iterator.flatMap { location =>
      declared.index.matchingIndicesOf(LocationMatching.namesOf(location, None)).headOption.map(declared.inputs(_)).map(unstated(_, location))
        .filter(_.nonEmpty)
        .map(names => s"a read of $location that does not state the CSV options ${names.mkString(", ")} (only a CsvIO parse of the lines a text read yields states its options, and never a date or timestamp pattern), while the contract declares them")
    }.toList.headOption
    output.orElse(input)
  }
}
