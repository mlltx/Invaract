// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, ContractParser, Dataset, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir
import com.invaract.ir.{CatalogIdentity, DatasetRef, UnknownPlan}
import com.invaract.verification.{
  AttachPlan, AttachSetup, CatalogChecker, CheckedWrite, CsvOptionsChecker, ContractInference, InputChecker, ContractViolationException, DataCheckResult, DryRunReporting, DryRunSelfCheck,
  InferredWrite, LocationMatching, RuntimeDataQuality, SchemaChecker, UnverifiableOperation, VerificationOptions, VerificationPipeline, VerificationResult,
  VerificationSetup, Violations
}
import com.invaract.verification.notification.{
  CatalogInfo, ContractValidationEvent, InferenceStatus, JobInfo, JobSummaryEvent, NotificationEvent, NotificationJson, NotificationSink, WriteEvent,
  WriteFieldInfo
}
import org.slf4j.LoggerFactory
import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/** The checker a Python job reading and writing through the `invaract` package is checked by
  * (docs/PYTHON_IO_ADAPTER_DESIGN.md). The package starts it once per run and talks to it over standard input and output, one
  * JSON object per line each way:
  *
  * {{{
  * {"op": "open", "protocol": 1, "settings": {"contract": "contract.yaml"}, "job": {"name": "orders_job.main", "pythonVersion": "3.11"}}
  * {"op": "read", "name": "orders", "format": "parquet", "schema": [{"name": "id", "type": "int64", "nullable": true, "nulls": 0}]}
  * {"op": "write", "name": "report", "format": "parquet", "saveMode": "overwrite", "schema": [{"name": "total", "type": "int64"}]}
  * {"op": "written", "name": "report", "nulls": {"total": 0}, "checks": [{"failed": 0, "examples": []}]}
  * {"op": "finish", "completed": true}
  * {"op": "published", "outputs": [{"name": "report", "rows": 12, "files": 1, "bytes": 2048, "durationMs": 35}]}
  * }}}
  *
  * Each request gets one reply: `{"ok": true, ...}` (to `open`, with the protocol version the checker speaks); `{"ok": false, "status": "refused", "violations": [...], "explanation": ...}`
  * when the contract refuses it; or `{"ok": false, "status": "not-checked", "reason": ...}` when it could not be checked, which
  * the package treats as a refusal too, so nothing is published unchecked. The checks are the shared ones
  * (`VerificationPipeline`), with this adapter's declaration and events naming the engine `pythonio`; this program only turns
  * requests into the plans and schemas they describe. Column types are Arrow's own names, mapped by the shared `ArrowSchemas`.
  * With `dryRun` set there is no contract: reads and writes name their locations, and each staged write gets a draft contract
  * (`ContractInferenceEvent`), merged into one for the job at `finish`. After a `finish` that said to publish, `published`
  * says what the package put in place: each output gets a `WriteEvent`, and every run ends with a `JobSummaryEvent`. It is compiled ahead of time into a native program for
  * the package's wheels, so no Java is needed where jobs run (milestone 0 of the design). Every request and reply is described
  * by the protocol's JSON Schema (docs/PYTHONIO_PROTOCOL.md).
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

/** One run: its lifecycle (`State`), and, once open, the inputs read, the writes described but not yet staged, the outputs
  * staged (each output's first staged write, for its `WriteEvent`), and in a dry run each write's outcome. */
private[pythonioadapter] final class Run {
  import Run._

  private var state: State = NotOpen
  private var reads = Vector.empty[(String, LogicalSchema)]
  /** The catalog each location read is registered in, where it is a catalog table. */
  private var readCatalogs = Map.empty[String, CatalogIdentity]
  /** The CSV options each read was made with, by location (`check.csvOptions`). */
  private var readOptions = Map.empty[String, Map[String, String]]
  /** The format each read was made in, by location (`check.inputFormat`). */
  private var readFormats = Map.empty[String, String]
  private var pending = Map.empty[String, (Request.Write, String)]
  private var written = Vector.empty[String]
  /** Each output staged, by name: its first staged write, and where it lands. */
  private var staged = Map.empty[String, (Request.Write, String)]
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

  private def respond(request: Request): Map[String, Any] = (request, state) match {
    case (_, Finished)                    => notChecked("the run has already finished")
    case (p: Request.Published, Publishing(run)) => published(p, run)
    case (_, Publishing(_))               => notChecked("the run has finished: only what was published may follow")
    case (_: Request.Published, _)        => notChecked("published follows a finish that said to publish")
    case (o: Request.Open, NotOpen)       => open(o)
    case (_, NotOpen)                     => notChecked("the run must be opened first")
    case (_: Request.Open, _: Opened)     => notChecked("the run is already open")
    case (r: Request.Read, run: Opened)   => read(r, run)
    case (w: Request.Write, run: Opened)  => write(w, run)
    case (w: Request.Written, run: Opened) => stage(w, run)
    case (f: Request.Finish, run: Opened) => finish(f, run)
  }

  private def open(o: Request.Open): Map[String, Any] = {
    val unknown = (o.settings.keySet -- PythonIoConfigSource.Known).toList.sorted
    if (o.protocol != Requests.Version)
      notChecked(s"this checker speaks protocol version ${Requests.Version}, and the package speaks protocol version ${o.protocol}: install the invaract package and its checker from the same release")
    else if (unknown.nonEmpty) notChecked(s"unknown setting(s): ${unknown.mkString(", ")}")
    else {
      val config = new PythonIoConfigSource(o.settings)
      val job = jobInfo(o.job)
      AttachSetup.select(config) match {
        case AttachPlan.DryRun(_) if o.check => notChecked("a check needs a contract to judge the frame against")
        case AttachPlan.DryRun(sink) =>
          state = Opened(None, VerificationOptions(), sink.map(new RunTally(_)), job)
          Map("ok" -> true, "protocol" -> Requests.Version, "mode" -> DryRunMode)
        case AttachPlan.Enforce(contract, sink) =>
          val located = VerificationSetup.resolveContractLocations(contract, config)
          val resolved = VerificationSetup.resolveVerificationOptions(VerificationOptions(), config)
          val (governed, options) = VerificationSetup.enforceOrgPolicy(located, resolved, config, sink, job.runId, Some(job))
          val reply = start(governed, options, sink, job)
          if (o.check) assumeDeclaredInputsRead(governed)
          reply
      }
    }
  }

  /** A check asks whether one frame would pass as a write, whatever the job has read: every declared input counts as read, with
    * its declared columns and the catalog it must be registered in, so the write is judged on its own. */
  private def assumeDeclaredInputsRead(contract: Contract): Unit = {
    reads = contract.inputs.toVector.map(i => i.location -> declaredSchema(i))
    readFormats = contract.inputs.flatMap(i => i.format.map(i.location -> _)).toMap
    readCatalogs = contract.inputs.flatMap { i =>
      i.catalog.filter(_.required).map(c => i.location -> CatalogIdentity(c.technology, c.catalogName, c.location, c.namespace, c.table))
    }.toMap
  }

  /** A dataset's columns as its contract declares them: a required column never empty. */
  private def declaredSchema(d: Dataset): LogicalSchema =
    LogicalSchema(d.schema.fields.map { f =>
      LogicalField(f.name, LogicalType.parse(f.fieldType).getOrElse(LogicalType.OtherType(f.fieldType, f.fieldType)), f.nullable && !f.required)
    })

  /** Opens the run on a contract already resolved and governed: what `open` does once the settings are read, and where the
    * conformance kit, which hands the checker its contract and options directly, starts a run. */
  private[pythonioadapter] def start(contract: Contract, options: VerificationOptions, sink: Option[NotificationSink], job: JobInfo): Map[String, Any] = {
    // a contract that asks for something this adapter cannot check is refused before the job's code runs
    PythonIoCapabilities.declared.foreach(VerificationPipeline.requireSupported(contract, options, _, JobPlan, sink, job.runId, Some(job)))
    state = Opened(Some(contract), options, sink.map(new RunTally(_)), job)
    Map("ok" -> true, "protocol" -> Requests.Version, "mode" -> EnforceMode, "contract" -> describe(contract, options))
  }

  /** A read: in enforcement, of a declared input (the package has no location for any other name, so any other is refused
    * whatever `rejectUndeclaredInputs` says), its format and columns checked against the input's; in a dry run, of the
    * location the job names. Either way it counts toward every later write. A read described without its columns, which
    * the package sends when it already knows the read disagrees with the contract, is refused or, if nothing about it
    * disagrees after all, not checked: its columns are never taken on trust. */
  private def read(r: Request.Read, run: Opened): Map[String, Any] = {
    val schema = LogicalSchema(r.schema.getOrElse(Nil).map(c => c.field(c.nulls)))
    val location = locate(r.name, r.location, "read", run)(_.inputs)
    run.contract.foreach { c =>
      val violations = c.inputs.find(_.name == r.name) match {
        case Some(input) if !LocationMatching.matches(input.location, location) => List(Violations.undeclaredInput(location))
        case Some(input) =>
          InputChecker.formatFinding(input, r.format, location).toList ++
            r.schema.toList.flatMap(_ => SchemaChecker.check(input.schema.fields, schema, SchemaChecker.Side.Input, location, run.options.rejectUndeclaredFields, caseSensitive = true)) ++
            input.catalog.toList.flatMap(CatalogChecker.check(_, r.catalog, location, SchemaChecker.Side.Input)) ++
            CsvOptionsChecker.findings(input, r.csv, SchemaChecker.Side.Input, location)
        case None => List(Violations.undeclaredInput(r.name))
      }
      if (violations.nonEmpty) reject(c, VerificationResult.of(s"${c.id}@${c.version}", violations), ir.Read(DatasetRef(location), catalog = r.catalog), run)
    }
    if (r.schema.isEmpty) throw new IllegalArgumentException(s"the read of '${r.name}' describes no columns")
    reads = reads.filterNot(_._1 == location) :+ (location -> schema)
    readCatalogs = r.catalog.fold(readCatalogs - location)(registered => readCatalogs + (location -> registered))
    readOptions = if (r.csv.isEmpty) readOptions - location else readOptions + (location -> r.csv)
    readFormats = r.format.fold(readFormats - location)(format => readFormats + (location -> format))
    Map("ok" -> true)
  }

  /** A write, described before any of it is staged: everything but the empty values, which only the data can show, is checked
    * now, so a write that cannot pass is refused before the job spends time writing it. The verdict on the whole write, and its
    * event, come with `written`. */
  private def write(w: Request.Write, run: Opened): Map[String, Any] = {
    val location = locate(w.name, w.location, "write", run)(_.outputs)
    val plan = writePlan(location, w)
    (w.unverifiable, run.contract) match {
      case (Some(why), Some(c)) =>
        VerificationPipeline.rejectUnverifiableWrite(c, unverifiable(w.name, why), plan, run.sink, run.job.runId, Some(run.job))
      case (Some(why), None) =>
        drafts :+= InferenceStatus.SkippedUnrecognized -> None
        DryRunReporting.inference(run.sink, logger, run.job, InferenceStatus.SkippedUnrecognized, Some(s"the write of '${w.name}': $why"), Some(location), None, None)
        notChecked(s"the write of '${w.name}' cannot be described: $why")
      case (None, contract) =>
        // no data yet, so no empty values: everything else is judged, and only a refusal is published
        contract.foreach { c =>
          val result = VerificationPipeline.evaluateWrite(
            c, checkedWrite(plan, LogicalSchema(w.schema.map(_.field(Some(0L)))), w.csv).copy(partitioning = Some(w.partitioning)), run.options, PythonIoCapabilities.declared
          )
          if (!result.passed) reject(c, result, plan, run)
        }
        pending += w.name -> (w -> location)
        Map("ok" -> true)
    }
  }

  /** A write the package has staged: the final check, with the empty values each column turned out to hold. */
  private def stage(s: Request.Written, run: Opened): Map[String, Any] = {
    val (w, location) = pending.getOrElse(s.name, throw new IllegalStateException(s"no write of '${s.name}' was described before it was staged"))
    pending -= s.name
    val schema = LogicalSchema(w.schema.map(c => c.field(s.nulls.get(c.name))))
    val plan = writePlan(location, w)
    run.contract match {
      case Some(c) =>
        val found = dataChecks(c, w.name, s, run.options)
        val checked = checkedWrite(plan, schema, w.csv).copy(dataChecks = found, partitioning = Some(w.partitioning))
        VerificationPipeline.verifyWrite(c, checked, run.options, run.sink, run.job.runId, PythonIoCapabilities.declared, Some(run.job))
      case None => infer(location, w, plan, schema, run)
    }
    written :+= location
    if (!staged.contains(s.name)) staged += s.name -> ((w, location))
    Map("ok" -> true)
  }

  /** The end of the job. A completed run is held to every declared output having been written; in a dry run, the drafts are
    * merged. When the reply says to publish, the run waits for `published`; otherwise it ends here. */
  private def finish(f: Request.Finish, run: Opened): Map[String, Any] = {
    state = Finished
    try {
      run.contract match {
        // a job that failed publishes nothing, so there is nothing to hold its outputs to
        case Some(c) => if (f.completed) VerificationPipeline.requireOutputsWritten(c, written.toList, JobPlan, run.sink, run.job.runId, Some(run.job))
        case None    => DryRunReporting.summary(run.sink, logger, run.job, drafts)
      }
    } catch {
      case NonFatal(e) =>
        end(run)
        throw e
    }
    if (f.completed) state = Publishing(run) else end(run)
    Map("ok" -> true, "publish" -> f.completed)
  }

  /** What the package put in place: a `WriteEvent` for each output, then the run's end. */
  private def published(p: Request.Published, run: Opened): Map[String, Any] = {
    state = Finished
    try {
      val unknown = p.outputs.map(_.name).filterNot(staged.contains)
      if (unknown.nonEmpty) throw new IllegalArgumentException(s"the package published ${unknown.mkString("'", "', '", "'")}, which this run did not stage")
      p.outputs.foreach(o => run.sink.foreach(_.publish(writeEvent(o, run))))
      Map("ok" -> true)
    } finally end(run)
  }

  /** Delivers the events of a run that ended without a `finish` or a `published` (the package was interrupted). */
  def close(): Unit = state match {
    case run: Opened     => end(run)
    case Publishing(run) => end(run)
    case _               => ()
  }

  /** The run's end: its `JobSummaryEvent`, then every event delivered. */
  private def end(run: Opened): Unit = {
    run.sink.foreach(tally => tally.publish(tally.summary(run.job, run.contract.fold(Map.empty[String, Any])(_.extensions))))
    flush(run)
  }

  /** A published output as its `WriteEvent`: what the run checked of its first staged write (location, format, save mode,
    * columns as written, catalog, partition columns) and what the package says it put in place. */
  private def writeEvent(o: Request.PublishedOutput, run: Opened): WriteEvent = {
    val (w, location) = staged(o.name)
    val required = run.contract.flatMap(_.outputs.find(_.name == o.name)).toList.flatMap(_.schema.fields).filter(_.required).map(_.name).toSet
    WriteEvent(
      contract = run.contract.map(c => s"${c.id}@${c.version}"),
      location = location,
      format = w.format,
      saveMode = w.saveMode,
      // as the output is written: a required column not-null, any other as its table's Arrow type says
      schema = w.schema.map(c => WriteFieldInfo(c.name, c.dataType.typeName, c.nullable && !required(c.name))),
      timestamp = System.currentTimeMillis(),
      metadata = run.contract.fold(Map.empty[String, Any])(_.extensions),
      durationMs = o.durationMs.getOrElse(0L),
      rowCount = o.rows,
      bytesWritten = o.bytes,
      fileCount = o.files,
      runId = run.job.runId,
      deltaVersion = o.deltaVersion,
      icebergSnapshotId = o.icebergSnapshotId,
      catalog = w.catalog.map(CatalogInfo.from),
      partitionColumns = w.partitioning.columns,
      datasetType = run.contract.flatMap(_.outputs.find(_.name == o.name)).flatMap(_.datasetType),
      job = Some(run.job)
    )
  }

  private def flush(run: Opened): Unit = DryRunReporting.flush(run.sink, logger, PythonIoCheck.FlushTimeoutMs)

  /** Where a read or write lands: where the job says, if it says (`location=`, which a dry run needs and which, with a contract
    * in force, is checked like any other location, so a forgotten one cannot point a write elsewhere); otherwise where the
    * contract declares the dataset it names, or, for a name it does not declare, the name itself, which lands on no declared
    * dataset. */
  private def locate(name: String, location: Option[String], verb: String, run: Opened)(declared: Contract => List[Dataset]): String =
    run.contract match {
      case None    => location.getOrElse(throw new IllegalArgumentException(s"a dry-run $verb of '$name' must say where it ${verb}s (location)"))
      case Some(c) => location.getOrElse(declared(c).find(_.name == name).fold(name)(_.location))
    }

  /** What the package found in the staged data, matched to the checks `open` sent it for the output named `name`: none unless
    * `runtimeDataQuality` is on. A package that reports a different number of checks than it was sent has not checked what
    * the contract asks, so the write is not checked (and so not published). */
  private def dataChecks(c: Contract, name: String, s: Request.Written, options: VerificationOptions): List[DataCheckResult] =
    if (!options.runtimeDataQuality) Nil
    else {
      val checks = c.outputs.find(_.name == name).toList.flatMap(RuntimeDataQuality.checks)
      if (s.checks.size != checks.size)
        throw new IllegalArgumentException(s"the package reported ${s.checks.size} data check(s) for '$name', but its contract declares ${checks.size}")
      checks.zip(s.checks).map { case (check, f) => DataCheckResult(check, f.failed, f.examples.take(RuntimeDataQuality.MaxExamples)) }
    }

  /** The write as the checker sees it: what the run has read, then the team's own code, then the write. */
  private def writePlan(location: String, w: Request.Write): ir.Write =
    ir.Write(
      DatasetRef(location),
      UnknownPlan(PythonIoCheck.JobCode, "Python", reads.toList.map(r => ir.Read(DatasetRef(r._1), catalog = readCatalogs.get(r._1)))),
      w.format,
      w.saveMode,
      w.catalog
    )

  private def checkedWrite(plan: ir.Write, schema: LogicalSchema, csv: Map[String, String]): CheckedWrite =
    CheckedWrite(plan, reads.toList, schema, caseSensitive = true, rowMutation = None, lineageBoundaryTypes = Set.empty,
      formatOptions = formatOptions(plan.dataset.location, csv), inputFormats = readFormats)

  /** The CSV options of every read so far, and of the write to `location` (`check.csvOptions`). */
  private def formatOptions(location: String, csv: Map[String, String]): Map[String, Map[String, String]] =
    if (csv.isEmpty) readOptions else readOptions + (location -> csv)

  private def reject(c: Contract, result: VerificationResult, plan: ir.Plan, run: Opened): Nothing =
    VerificationPipeline.reject(c, result, plan, run.sink, run.job.runId, Some(run.job))

  /** A dry run's staged write: a draft contract for it, checked against the write it describes. */
  private def infer(location: String, w: Request.Write, plan: ir.Write, schema: LogicalSchema, run: Opened): Unit = {
    val outcome =
      try {
        val draft = ContractInference.infer(InferredWrite(location, w.format, w.saveMode, schema, Some(w.partitioning), formatOptions(location, w.csv), readFormats), reads.toList, plan)
        val selfCheck = DryRunSelfCheck.of(draft, plan, reads.toList, schema, run.options, caseSensitive = true, Set.empty)
        DryRunReporting.inference(run.sink, logger, run.job, InferenceStatus.Inferred, None, Some(location), Some(ContractParser.write(draft)), Some(selfCheck))
        InferenceStatus.Inferred -> Some(draft)
      } catch {
        case NonFatal(e) =>
          DryRunReporting.inference(run.sink, logger, run.job, InferenceStatus.InferenceError, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), Some(location), None, None)
          InferenceStatus.InferenceError -> None
      }
    drafts :+= outcome
  }
}

private[pythonioadapter] object Run {

  private val logger = LoggerFactory.getLogger(classOf[Run])

  val EnforceMode = "enforce"
  val DryRunMode = "dry-run"

  /** Where a run is: before `open`; open (enforcing a contract, or a dry run with none); finished and waiting to hear what
    * the package published; or over. */
  sealed trait State
  case object NotOpen extends State
  case object Finished extends State
  final case class Opened(contract: Option[Contract], options: VerificationOptions, sink: Option[RunTally], job: JobInfo) extends State
  final case class Publishing(run: Opened) extends State

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

  /** What the package needs from the contract: where each dataset is, and what each output must look like; with
    * `runtimeDataQuality` on, also each output's data checks (`RuntimeDataQuality.checks`), which the package evaluates on the
    * data it stages and reports, in the same order, with `written`. */
  def describe(c: Contract, options: VerificationOptions): Map[String, Any] =
    Map(
      "id" -> c.id,
      "version" -> c.version,
      "inputs" -> c.inputs.map(dataset),
      "outputs" -> c.outputs.map(o => dataset(o) ++ (if (options.runtimeDataQuality) Map("checks" -> RuntimeDataQuality.checks(o).map(_.toMap)) else Map.empty))
    )

  /** A dataset's name, location, format and save mode, and its declared columns: an input's are the ones the package reads, with
    * their types, and an output's the ones whose required-ness the written file states. */
  private def dataset(d: Dataset): Map[String, Any] =
    Map("name" -> d.name, "location" -> d.location, "fields" -> d.schema.fields.map(f => Map("name" -> f.name, "type" -> f.fieldType, "required" -> f.required))) ++
      d.format.map("format" -> _) ++
      d.saveMode.map("saveMode" -> _) ++
      (if (d.partitionKeys.isEmpty) Map.empty else Map("partitionBy" -> d.partitionKeys, "partitionOverwriteMode" -> d.effectivePartitionOverwriteMode)) ++
      d.csv.map(_.declared).filter(_.nonEmpty).map(o => "csv" -> o.toMap)

  def refused(e: ContractViolationException): Map[String, Any] =
    Map("ok" -> false, "status" -> "refused", "violations" -> e.result.violations.map(_.violationType), "explanation" -> e.getMessage)

  def notChecked(reason: String): Map[String, Any] = Map("ok" -> false, "status" -> "not-checked", "reason" -> reason)
}

/** A run's sink, counting what it delivers for the run's `JobSummaryEvent`: validation events by verdict, with their
  * violations, and write events. One run, one thread: the package asks one request at a time. */
private[pythonioadapter] final class RunTally(delegate: NotificationSink) extends NotificationSink {
  private var passed = 0L
  private var failed = 0L
  private var violations = 0L
  private var writes = 0L

  override def configure(properties: Map[String, String]): Unit = delegate.configure(properties)

  override def flush(timeoutMs: Long): Unit = delegate.flush(timeoutMs)

  override def publish(event: NotificationEvent): Unit = {
    event match {
      case e: ContractValidationEvent =>
        if (e.status == "PASSED") passed += 1 else failed += 1
        violations += e.violations.size
      case _: WriteEvent => writes += 1
      case _             => ()
    }
    delegate.publish(event)
  }

  /** The run's summary: what this sink has delivered, over the time since the run opened. */
  def summary(job: JobInfo, metadata: Map[String, Any]): JobSummaryEvent = {
    val now = System.currentTimeMillis()
    JobSummaryEvent(writes, passed, failed, violations, job.startTimeMs.fold(0L)(now - _), now, metadata, job.runId, Some(job))
  }
}
