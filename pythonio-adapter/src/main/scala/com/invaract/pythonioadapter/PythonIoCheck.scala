// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, Dataset, LogicalSchema}
import com.invaract.ir.{DatasetRef, Read, UnknownPlan, Write}
import com.invaract.verification.{AttachPlan, AttachSetup, CheckedWrite, ContractViolationException, VerificationOptions, VerificationPipeline, VerificationSetup}
import com.invaract.verification.notification.{JobInfo, NotificationJson, NotificationSink}
import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/** The checker a Python job reading and writing through the `invaract` package is checked by
  * (docs/PYTHON_IO_ADAPTER_DESIGN.md). The package starts it once per run and talks to it over standard input and output, one
  * JSON object per line each way:
  *
  * {{{
  * {"op": "open", "settings": {"contract": "contract.yaml"}, "job": {"name": "orders_job.main", "pythonVersion": "3.11"}}
  * {"op": "write", "name": "report", "format": "parquet", "schema": [{"name": "total", "type": "long", "nullable": false}],
  *  "reads": [{"name": "orders"}]}
  * {"op": "finish", "completed": true}
  * }}}
  *
  * Each request gets one reply: `{"ok": true, ...}`; `{"ok": false, "status": "refused", "violations": [...], "explanation": ...}`
  * when the contract refuses it; or `{"ok": false, "status": "not-checked", "reason": ...}` when it could not be checked, which
  * the package treats as a refusal too, so nothing is published unchecked. The checks are the shared ones
  * (`VerificationPipeline`), with this adapter's declaration and events naming the engine `pythonio`; this program only turns
  * requests into the plans and schemas they describe. It is compiled ahead of time into a native program for the package's
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

/** One run's state: nothing before `open`, the enforced contract after it, the outputs written so far, and whether it ended. */
private[pythonioadapter] final class Run {
  import Run._

  private var opened: Option[Opened] = None
  private var finished = false
  private var written = Vector.empty[String]

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
    case (_, _) if finished                => notChecked("the run has already finished")
    case (o: Request.Open, None)           => open(o)
    case (_: Request.Open, Some(_))        => notChecked("the run is already open")
    case (w: Request.Write, Some(run))     => write(w, run)
    case (f: Request.Finish, Some(run))    => finish(f, run)
    case (_, None)                         => notChecked("the run must be opened first")
  }

  private def open(o: Request.Open): Map[String, Any] = {
    val unknown = (o.settings.keySet -- PythonIoConfigSource.Known).toList.sorted
    if (unknown.nonEmpty) notChecked(s"unknown setting(s): ${unknown.mkString(", ")}")
    else {
      val config = new PythonIoConfigSource(o.settings)
      AttachSetup.select(config) match {
        case AttachPlan.DryRun(_) => notChecked("dry-run mode is not available in this version of the checker")
        case AttachPlan.Enforce(contract, sink) =>
          val job = jobInfo(o.job)
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
    opened = Some(Opened(contract, options, sink, job))
    Map("ok" -> true, "contract" -> describe(contract))
  }

  private def write(w: Request.Write, run: Opened): Map[String, Any] = {
    val c = run.contract
    // an output the contract does not declare has no location: its name stands in, and lands on no declared output
    val location = c.outputs.find(_.name == w.name).fold(w.name)(_.location)
    val reads = w.reads.map(r => (c.inputs.find(_.name == r.name).fold(r.name)(_.location), r))
    val plan = Write(DatasetRef(location), UnknownPlan(PythonIoCheck.JobCode, "Python", reads.map(r => Read(DatasetRef(r._1)))), w.format, w.saveMode)
    val inputSchemas = reads.collect { case (at, r) if r.schema.nonEmpty => at -> LogicalSchema(r.schema) }
    VerificationPipeline.verifyWrite(
      c,
      CheckedWrite(plan, inputSchemas, LogicalSchema(w.schema), caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty),
      run.options,
      run.sink,
      run.job.runId,
      PythonIoCapabilities.declared,
      Some(run.job)
    )
    written :+= location
    Map("ok" -> true)
  }

  private def finish(f: Request.Finish, run: Opened): Map[String, Any] = {
    finished = true
    try {
      // a job that failed publishes nothing, so there is nothing to hold its outputs to
      if (f.completed) VerificationPipeline.requireOutputsWritten(run.contract, written.toList, JobPlan, run.sink, run.job.runId, Some(run.job))
      Map("ok" -> true, "publish" -> f.completed)
    } finally flush(run)
  }

  /** Delivers the events of a run that ended without a `finish` (the package was interrupted). */
  def close(): Unit = if (!finished) opened.foreach(flush)

  private def flush(run: Opened): Unit = run.sink.foreach(s => s.flush(PythonIoCheck.FlushTimeoutMs))
}

private[pythonioadapter] object Run {

  /** What an open run enforces, and where its events go. */
  final case class Opened(contract: Contract, options: VerificationOptions, sink: Option[NotificationSink], job: JobInfo)

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
