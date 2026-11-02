// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, ContractParser, Dataset, LogicalSchema}
import com.invaract.ir
import com.invaract.ir.{DatasetRef, UnknownPlan}
import com.invaract.verification.{
  AttachPlan, AttachSetup, CheckedWrite, ContractInference, ContractViolationException, DryRunSelfCheck, DryRunSummary, InferredWrite,
  SchemaChecker, UnverifiableOperation, VerificationOptions, VerificationPipeline, VerificationResult, VerificationSetup, Violation,
  Violations
}
import com.invaract.verification.notification.{ContractInferenceEvent, InferenceStatus, JobInfo, NotificationJson, NotificationSink}
import org.slf4j.LoggerFactory
import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/** The checker a Python job reading and writing through the `invaract` package is checked by
  * (docs/PYTHON_IO_ADAPTER_DESIGN.md). The package starts it once per run and talks to it over standard input and output, one
  * JSON object per line each way:
  *
  * {{{
  * {"op": "open", "settings": {"contract": "contract.yaml"}, "job": {"name": "orders_job.main", "pythonVersion": "3.11"}}
  * {"op": "read", "name": "orders", "schema": [{"name": "id", "type": "int64", "nullable": true, "nulls": 0}]}
  * {"op": "write", "name": "report", "format": "parquet", "saveMode": "overwrite", "schema": [{"name": "total", "type": "int64"}]}
  * {"op": "written", "name": "report", "rows": 120, "nulls": {"total": 0}}
  * {"op": "finish", "completed": true}
  * }}}
  *
  * Each request gets one reply: `{"ok": true, ...}`; `{"ok": false, "status": "refused", "violations": [...], "explanation": ...}`
  * when the contract refuses it; or `{"ok": false, "status": "not-checked", "reason": ...}` when it could not be checked, which
  * the package treats as a refusal too, so nothing is published unchecked. The checks are the shared ones
  * (`VerificationPipeline`), with this adapter's declaration and events naming the engine `pythonio`; this program only turns
  * requests into the plans and schemas they describe. Column types are Arrow's own names, mapped by the shared `ArrowSchemas`.
  * With `dryRun` set there is no contract: reads and writes name their locations, and each staged write gets a draft contract
  * (`ContractInferenceEvent`), merged into one for the job at `finish`. It is compiled ahead of time into a native program for the package's
  * wheels, so no Java is needed where jobs run (milestone 0 of the design).
  */
object PythonIoCheck {

  /** The engine every event this checker publishes names (`job.engine`). */
  val Engine = "pythonio"

  /** What stands between a job's reads and its writes: the team's own code, which the checker never sees. */
  private[pythonioadapter] val JobCode = "the job's own Python code"

  /** How long the end of a run waits for the sink to deliver its events. */
  private[pythonioadapter] val FlushTimeoutMs = 10000L

  def main(args: Array[String]): Unit = {
    serve(new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)), new PrintStream(System.out, true, "UTF-8"))
    // a sink that keeps a thread alive must not keep the job waiting
    System.exit(0)
  }

  /** Answers every request on `in` until a `finish` or the end of the input, one reply per request on `out`. */
  def serve(in: BufferedReader, out: PrintStream): Unit = {
    val run = new Run
    Iterator.continually(in.readLine()).takeWhile(_ != null).filter(_.trim.nonEmpty).foreach { line =>
      out.println(NotificationJson.anyToJson(run.answer(line)))
    }
    run.close()
  }
}

/** One run's state: nothing before `open`; after it, the mode (the contract enforced, or a dry run), the inputs read, the writes
  * described but not yet staged, the outputs staged, and whether the run ended. */
private[pythonioadapter] final class Run {
  import Run._

  private var opened: Option[Opened] = None
  private var finished = false
  private var reads = Vector.empty[(String, LogicalSchema)]
  private var pending = Map.empty[String, Pending]
  private var written = Vector.empty[String]
  private var drafts = Vector.empty[(String, Option[Contract])]

  def answer(line: String): Map[String, Any] =
    Requests.parse(line) match {
      case Left(why) => notChecked(why)
      case Right(request) =>
        try respond(request)
        catch {
          case e: ContractViolationException => refused(e)
          case NonFatal(e)                   => notChecked(s"${e.getClass.getSimpleName}: ${e.getMessage}")
        }
    }

  private def respond(request: Request): Map[String, Any] = (request, opened) match {
    case (_, _) if finished             => notChecked("the run has already finished")
    case (o: Request.Open, None)        => open(o)
    case (_: Request.Open, Some(_))     => notChecked("the run is already open")
    case (r: Request.Read, Some(run))   => read(r, run)
    case (w: Request.Write, Some(run))  => write(w, run)
    case (w: Request.Written, Some(run)) => staged(w, run)
    case (f: Request.Finish, Some(run)) => finish(f, run)
    case (_, None)                      => notChecked("the run must be opened first")
  }

  private def open(o: Request.Open): Map[String, Any] = {
    val unknown = (o.settings.keySet -- PythonIoConfigSource.Known).toList.sorted
    if (unknown.nonEmpty) notChecked(s"unknown setting(s): ${unknown.mkString(", ")}")
    else {
      val config = new PythonIoConfigSource(o.settings)
      val job = jobInfo(o.job)
      AttachSetup.select(config) match {
        case AttachPlan.DryRun(sink) =>
          opened = Some(Opened(None, VerificationOptions(), sink, job))
          Map("ok" -> true, "mode" -> DryRunMode)
        case AttachPlan.Enforce(contract, sink) =>
          val located = VerificationSetup.resolveContractLocations(contract, config)
          val resolved = VerificationSetup.resolveVerificationOptions(VerificationOptions(), config)
          val (governed, options) = VerificationSetup.enforceOrgPolicy(located, resolved, config, sink, job.runId, Some(job))
          start(governed, options, sink, job)
      }
    }
  }

  /** Opens the run on a contract already resolved and governed: what `open` does once the settings are read, and where the
    * conformance kit, which hands the checker its contract and options directly, starts a run. */
  private[pythonioadapter] def start(contract: Contract, options: VerificationOptions, sink: Option[NotificationSink], job: JobInfo): Map[String, Any] = {
    // a contract that asks for something this adapter cannot check is refused before the job's code runs
    PythonIoCapabilities.declared.foreach(VerificationPipeline.requireSupported(contract, options, _, JobPlan, sink, job.runId, Some(job)))
    opened = Some(Opened(Some(contract), options, sink, job))
    Map("ok" -> true, "mode" -> EnforceMode, "contract" -> describe(contract))
  }

  /** A read: in enforcement, of a declared input (the package has no location for any other name), its columns checked against
    * the input's; in a dry run, of the location the job names. Either way it counts toward every later write. */
  private def read(r: Request.Read, run: Opened): Map[String, Any] = {
    val schema = LogicalSchema(r.schema.map(c => c.field(c.holdsNulls)))
    val location = run.contract match {
      case None => r.location.getOrElse(throw new IllegalArgumentException(s"a dry-run read of '${r.name}' must say where it reads (location)"))
      case Some(c) =>
        val input = c.inputs.find(_.name == r.name)
        val at = input.fold(r.name)(_.location)
        val violations = input.fold(List(Violations.undeclaredInput(r.name))) { i =>
          SchemaChecker.check(i.schema.fields, schema, SchemaChecker.Side.Input, at, run.options.rejectUndeclaredFields, caseSensitive = true)
        }
        if (violations.nonEmpty) refuse(c, violations, ir.Read(DatasetRef(at)), run)
        at
    }
    reads = reads.filterNot(_._1 == location) :+ (location -> schema)
    Map("ok" -> true)
  }

  /** A write, described before any of it is staged: everything but the empty values, which only the data can show, is checked
    * now, so a write that cannot pass is refused before the job spends time writing it. The verdict on the whole write, and its
    * event, come with `written`. */
  private def write(w: Request.Write, run: Opened): Map[String, Any] = {
    val location = locationOf(w, run)
    val plan = writePlan(location, w.format, w.saveMode)
    (w.unverifiable, run.contract) match {
      case (Some(why), Some(c)) =>
        VerificationPipeline.rejectUnverifiableWrite(c, unverifiable(w.name, why), plan, run.sink, run.job.runId, Some(run.job))
      case (Some(why), None) =>
        drafts :+= InferenceStatus.SkippedUnrecognized -> None
        report(run, InferenceStatus.SkippedUnrecognized, Some(s"the write of '${w.name}': $why"), Some(location), None, None)
        notChecked(s"the write of '${w.name}' cannot be described: $why")
      case (None, contract) =>
        // no empty values yet: the pre-check judges the rest, and publishes only a refusal
        contract.foreach(c => preCheck(c, plan, LogicalSchema(w.schema.map(_.field(nullable = false))), run))
        pending += w.name -> Pending(location, w.format, w.saveMode, w.schema)
        Map("ok" -> true)
    }
  }

  private def preCheck(c: Contract, plan: ir.Write, schema: LogicalSchema, run: Opened): Unit =
    try check(c, plan, schema, run, sink = None)
    catch {
      // the same check again, with the sink, so the refusal is published as one FAILED validation
      case _: ContractViolationException => check(c, plan, schema, run, run.sink)
    }

  /** A write the package has staged: the final check, with the empty values each column turned out to hold. */
  private def staged(w: Request.Written, run: Opened): Map[String, Any] = {
    val p = pending.getOrElse(w.name, throw new IllegalStateException(s"no write of '${w.name}' was described before it was staged"))
    pending -= w.name
    val schema = LogicalSchema(p.schema.map(c => c.field(w.nulls.get(c.name).fold(c.nullable)(_ > 0))))
    val plan = writePlan(p.location, p.format, p.saveMode)
    run.contract match {
      case Some(c) => check(c, plan, schema, run, run.sink)
      case None    => infer(p, plan, schema, run)
    }
    written :+= p.location
    Map("ok" -> true)
  }

  private def check(c: Contract, plan: ir.Write, schema: LogicalSchema, run: Opened, sink: Option[NotificationSink]): Unit =
    VerificationPipeline.verifyWrite(
      c,
      CheckedWrite(plan, reads.toList, schema, caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty),
      run.options,
      sink,
      run.job.runId,
      PythonIoCapabilities.declared,
      Some(run.job)
    )

  private def finish(f: Request.Finish, run: Opened): Map[String, Any] = {
    finished = true
    try {
      run.contract match {
        // a job that failed publishes nothing, so there is nothing to hold its outputs to
        case Some(c) => if (f.completed) VerificationPipeline.requireOutputsWritten(c, written.toList, JobPlan, run.sink, run.job.runId, Some(run.job))
        case None    => summarise(run)
      }
      Map("ok" -> true, "publish" -> f.completed)
    } finally flush(run)
  }

  /** Delivers the events of a run that ended without a `finish` (the package was interrupted). */
  def close(): Unit = if (!finished) opened.foreach(flush)

  private def flush(run: Opened): Unit = run.sink.foreach(s => s.flush(PythonIoCheck.FlushTimeoutMs))

  private def locationOf(w: Request.Write, run: Opened): String = run.contract match {
    case None => w.location.getOrElse(throw new IllegalArgumentException(s"a dry-run write of '${w.name}' must say where it writes (location)"))
    // an output the contract does not declare has no location: its name stands in, and lands on no declared output
    case Some(c) => c.outputs.find(_.name == w.name).fold(w.name)(_.location)
  }

  /** The write as the checker sees it: what the run has read, then the team's own code, then the write. */
  private def writePlan(location: String, format: Option[String], saveMode: Option[String]): ir.Write =
    ir.Write(DatasetRef(location), UnknownPlan(PythonIoCheck.JobCode, "Python", reads.toList.map(r => ir.Read(DatasetRef(r._1)))), format, saveMode)

  private def refuse(c: Contract, violations: List[Violation], plan: ir.Plan, run: Opened): Nothing = {
    val result = VerificationResult.of(s"${c.id}@${c.version}", violations)
    VerificationPipeline.publishValidation(c, result, run.sink, run.job.runId, Some(run.job))
    throw new ContractViolationException(result, VerificationPipeline.explain(c, plan, result))
  }

  // --- dry run: a draft contract for each write, and one for the whole job at the end ---------------------------------------

  private def infer(p: Pending, plan: ir.Write, schema: LogicalSchema, run: Opened): Unit = {
    val (status, draft) =
      try {
        val draft = ContractInference.infer(InferredWrite(p.location, p.format, p.saveMode, schema), reads.toList, plan)
        val selfCheck = DryRunSelfCheck.of(draft, plan, reads.toList, schema, run.options, caseSensitive = true, Set.empty)
        report(run, InferenceStatus.Inferred, None, Some(p.location), Some(ContractParser.write(draft)), Some(selfCheck))
        InferenceStatus.Inferred -> Some(draft)
      } catch {
        case NonFatal(e) =>
          report(run, InferenceStatus.InferenceError, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), Some(p.location), None, None)
          InferenceStatus.InferenceError -> None
      }
    drafts :+= status -> draft
  }

  private def summarise(run: Opened): Unit = {
    val counts = drafts.groupBy(_._1).map { case (status, all) => status -> all.size.toLong }
    val summary = DryRunSummary.event(counts, drafts.toList.flatMap(_._2), run.job, System.currentTimeMillis(), Map.empty)
    run.sink match {
      case Some(s) => s.publish(summary)
      case None =>
        logger.info(s"Invaract dry run summary: merge ${summary.mergeStatus}${summary.mergedContractYaml.map("\n" + _).getOrElse(summary.mergeConflicts.map("\n  " + _).mkString)}")
    }
  }

  private def report(
      run: Opened,
      status: String,
      reason: Option[String],
      location: Option[String],
      yaml: Option[String],
      selfCheck: Option[(String, List[Violation], List[String])]
  ): Unit =
    run.sink match {
      case Some(s) =>
        s.publish(
          ContractInferenceEvent(
            status, reason, location, yaml, selfCheck.map(_._1), selfCheck.fold(List.empty[Violation])(_._2), selfCheck.fold(List.empty[String])(_._3),
            None, run.job, System.currentTimeMillis(), Map.empty
          )
        )
      case None => logger.info(s"Invaract dry run: $status ${location.getOrElse("")}${yaml.map("\n" + _).getOrElse(reason.map(" - " + _).getOrElse(""))}")
    }
}

private[pythonioadapter] object Run {

  private val logger = LoggerFactory.getLogger(classOf[Run])

  val EnforceMode = "enforce"
  val DryRunMode = "dry-run"

  /** What an open run enforces (no contract in a dry run), and where its events go. */
  final case class Opened(contract: Option[Contract], options: VerificationOptions, sink: Option[NotificationSink], job: JobInfo)

  /** A write the package described and has not yet staged. */
  final case class Pending(location: String, format: Option[String], saveMode: Option[String], schema: List[Column])

  /** The whole job, as far as the checker can see it: the team's code, never inside it. */
  val JobPlan: UnknownPlan = UnknownPlan(PythonIoCheck.JobCode, "Python")

  def jobInfo(d: JobDetails): JobInfo =
    JobInfo(
      runId = d.runId,
      name = d.name,
      engine = Some(PythonIoCheck.Engine),
      engineVersion = d.pythonVersion,
      engineDetails = d.libraries,
      user = Option(System.getProperty("user.name")),
      startTimeMs = Some(System.currentTimeMillis())
    )

  /** A table the package could not describe: refused, since what it holds was never checked. */
  def unverifiable(name: String, why: String): UnverifiableOperation =
    UnverifiableOperation(
      s"invaract.write('$name')",
      s"the table handed to invaract.write('$name') cannot be described: $why",
      "Hand invaract.write a table that offers an Arrow stream (a pandas, Polars or DuckDB frame, or a pyarrow table)."
    )

  /** What the package needs from the contract: where each dataset is, and what each output must look like. */
  def describe(c: Contract): Map[String, Any] =
    Map(
      "id" -> c.id,
      "version" -> c.version,
      "inputs" -> c.inputs.map(dataset(_, withColumns = false)),
      "outputs" -> c.outputs.map(dataset(_, withColumns = true))
    )

  private def dataset(d: Dataset, withColumns: Boolean): Map[String, Any] =
    Map("name" -> d.name, "location" -> d.location) ++
      d.format.map("format" -> _) ++
      d.saveMode.map("saveMode" -> _) ++
      (if (withColumns) Map("fields" -> d.schema.fields.map(f => Map("name" -> f.name, "type" -> f.fieldType, "required" -> f.required))) else Map.empty)

  def refused(e: ContractViolationException): Map[String, Any] =
    Map("ok" -> false, "status" -> "refused", "violations" -> e.result.violations.map(_.violationType), "explanation" -> e.getMessage)

  def notChecked(reason: String): Map[String, Any] = Map("ok" -> false, "status" -> "not-checked", "reason" -> reason)
}
