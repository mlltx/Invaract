// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.contract.{LogicalField, LogicalType}
import com.invaract.verification.Violation
import com.invaract.verification.notification._
import com.invaract.verification.notification.JsonText.{obj, Obj}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import scala.collection.mutable

/** Turns Invaract's events into OpenLineage run events (docs/OPENLINEAGE.md, section 2): one OpenLineage run per Invaract
  * run, `START` when its first event arrives, `RUNNING` for each check and each completed write, `COMPLETE` or `FAIL` when
  * it ends. Holds each open run's state, so it is used by one thread at a time (`OpenLineageSink`'s worker).
  *
  * Every event it returns is a `JsonText.Obj`, ready to render; nothing here does I/O. `now` and `workingDirectory` are
  * parameters so tests control them.
  */
private[openlineage] final class OpenLineageMapper(
    settings: OpenLineageSettings,
    now: () => Instant = () => Instant.now(),
    workingDirectory: String = System.getProperty("user.dir"),
    maxOpenRuns: Int = 256
) {
  import OpenLineageMapper._

  private val runs = mutable.LinkedHashMap.empty[String, RunState]

  /** Runs that have ended, most recent last (bounded), with the generation each was: how many runs before it had the same
    * key. A key comes back in two ways. A completed write reported after its run's end (Spark reports one from a listener
    * thread, which can trail the end) belongs to the run that ended, and is dropped: reopening the run would break
    * OpenLineage's one-terminal-event rule. A new check under the same key is a new run (a process whose engine gives its
    * runs no id of their own runs one after another under the same key), and gets an id of its own. */
  private val ended = mutable.LinkedHashMap.empty[String, Int]
  private val nextGeneration = mutable.Map.empty[String, Int]

  /** What `event` is published as: nothing, or one or more OpenLineage events, in order. */
  def map(event: NotificationEvent): List[Obj] =
    runKey(event).filter(ended.contains) match {
      case Some(_) if event.isInstanceOf[WriteEvent] => Nil
      case Some(key) =>
        nextGeneration(key) = ended.remove(key).get + 1
        mapLive(event)
      case None => mapLive(event)
    }

  /** The run an event belongs to, for the events that open or extend one. */
  private def runKey(event: NotificationEvent): Option[String] = event match {
    case e: ContractValidationEvent => Some(keyOf(e.job, e.runId))
    case e: WriteEvent              => Some(keyOf(e.job, e.runId))
    case e: ContractInferenceEvent  => Some(keyOf(Some(e.job), e.job.runId))
    case _                          => None
  }

  private def mapLive(event: NotificationEvent): List[Obj] = event match {
    case e: ContractValidationEvent =>
      // the datasets before the run, here and below: should naming one fail, no run is left begun without its START
      val (inputs, outputs) = e.lineage.fold((List.empty[Obj], List.empty[Obj]))(l => (l.inputs.map(input), List(checkedOutput(l, e))))
      val run = runOf(e.job, e.runId, e.contract)
      val failed = e.status != "PASSED"
      run.checksPassed += (if (failed) 0 else 1)
      run.checksFailed += (if (failed) 1 else 0)
      run.violations += e.violations.size
      val runFacets = if (failed) errorMessage(e).toList else Nil
      progress(run, inputs, outputs, runFacets)
    case e: WriteEvent =>
      val output = writtenOutput(e)
      val run = runOf(e.job, e.runId, e.contract.getOrElse("job"))
      run.writes += 1
      progress(run, Nil, List(output), Nil)
    case e: ContractInferenceEvent =>
      // a dry run's write publishes the lineage enforcement would: its inputs, and its output's schema and column
      // lineage, with the draft's status in place of a verdict
      val (inputs, outputs) = e.lineage match {
        case Some(l) => (l.inputs.map(input), List(outputOf(l, inferredFacet(e).toList, Nil)))
        case None    => (Nil, e.writeLocation.toList.map(inferredOutput(_, e)))
      }
      val run = runOf(Some(e.job), e.job.runId, "job")
      run.dryRun = true
      progress(run, inputs, outputs, Nil)
    case e: JobSummaryEvent =>
      runs.get(keyOf(e.job, e.runId)).toList.flatMap { run =>
        run.checksFailed = run.checksFailed max e.checksFailed
        end(run)
      }
    case e: DryRunSummaryEvent =>
      runs.get(keyOf(Some(e.job), e.job.runId)).toList.flatMap(end)
    case _: CodeScanEvent => Nil
  }

  /** The terminal event of the run `job` names, if it is open: the adapter saw the run end (`NotificationSink.endRun`). */
  def endRun(job: Option[JobInfo]): List[Obj] = runs.get(keyOf(job, None)).toList.flatMap(end)

  /** The terminal event of every run still open, for when the process ends: `FAIL` for a run with a refused write, else
    * `COMPLETE`. */
  def closeAll(): List[Obj] = runs.values.toList.flatMap(end)

  /** How many runs are open. */
  def openRuns: Int = runs.size

  // --- runs -------------------------------------------------------------------------------------------------------

  private def keyOf(job: Option[JobInfo], runId: Option[String]): String =
    job.flatMap(_.runId).orElse(runId).getOrElse("invaract-process")

  private def runOf(job: Option[JobInfo], runId: Option[String], fallbackName: String): RunState = {
    val key = keyOf(job, runId)
    runs.getOrElse(key, {
      // a process serves one run at a time in practice; bounded all the same, the oldest run giving way
      if (runs.size >= maxOpenRuns) runs.headOption.foreach { case (oldest, _) => runs.remove(oldest) }
      val name = settings.jobName.orElse(job.flatMap(_.jobId)).orElse(job.flatMap(_.name)).getOrElse(fallbackName)
      val generation = nextGeneration.remove(key).getOrElse(0)
      val uuid = runUuid(job.flatMap(_.engine).getOrElse("invaract"), if (generation == 0) key else s"$key#$generation")
      val state = new RunState(uuid, settings.namespace, name, job, generation)
      runs.put(key, state)
      state
    })
  }

  /** The run's `RUNNING` event with these datasets, preceded by its `START` if this is its first event; accumulated
    * instead when only the run's end is published (`endOfRun`, `failuresOnly`). */
  private def progress(run: RunState, inputs: List[Obj], outputs: List[Obj], runFacets: List[(String, Any)]): List[Obj] = {
    val start = if (run.started) Nil else { run.started = true; List(event("START", run, Nil, Nil, Nil, terminal = false)) }
    val running = List(event("RUNNING", run, inputs, outputs, runFacets, terminal = false))
    if (settings.emission == Emission.EndOfRun || settings.failuresOnly) {
      run.held ++= start
      if (settings.emission == Emission.EndOfRun) {
        inputs.foreach(d => run.inputs.update(idOf(d), d))
        outputs.foreach(d => run.outputs.update(idOf(d), d))
        run.endFacets ++= runFacets
      } else if (run.held.size < MaxHeldEvents) run.held ++= running
      Nil
    } else start ++ running
  }

  private def end(run: RunState): List[Obj] = {
    runs.find(_._2 eq run).foreach { case (key, _) =>
      runs.remove(key)
      ended(key) = run.generation
      if (ended.size > maxOpenRuns * 4) ended -= ended.head._1
    }
    val failed = run.checksFailed > 0
    val terminal = event(if (failed) "FAIL" else "COMPLETE", run, run.inputs.values.toList, run.outputs.values.toList, run.endFacets.toList, terminal = true)
    if (settings.failuresOnly && !failed) Nil else run.held.toList ++ List(terminal)
  }

  // --- the event --------------------------------------------------------------------------------------------------

  private def event(eventType: String, run: RunState, inputs: List[Obj], outputs: List[Obj], extraRunFacets: List[(String, Any)], terminal: Boolean): Obj = {
    val runFacets = List(
      facet("processing_engine", standard("ProcessingEngineRunFacet", "1-1-1"),
        "version" -> run.job.flatMap(_.engineVersion).getOrElse("unknown"),
        "name" -> run.job.flatMap(_.engine).getOrElse("invaract"),
        "openlineageAdapterVersion" -> InvaractVersion),
      settings.parent.flatMap(p => facet("parent", standard("ParentRunFacet", "1-2-0"),
        "run" -> obj("runId" -> p.runId),
        "job" -> obj("namespace" -> p.namespace, "name" -> p.name),
        "root" -> p.root.map(r => obj("run" -> obj("runId" -> r.runId), "job" -> obj("namespace" -> r.namespace, "name" -> r.name))))),
      facet("invaract_run", invaract("InvaractRunFacet"), invaractRun(run, terminal): _*)
    ).flatten ++ extraRunFacets.filter { case (name, _) => settings.enabled(name) }
    val jobFacets = List(
      facet("jobType", standard("JobTypeJobFacet", "2-0-4"),
        "processingType" -> "BATCH", "integration" -> "INVARACT", "jobType" -> "JOB")
    ).flatten
    obj(
      "eventType" -> eventType,
      "eventTime" -> now().toString,
      "run" -> obj("runId" -> run.uuid, "facets" -> Obj(runFacets)),
      "job" -> obj("namespace" -> run.namespace, "name" -> run.name, "facets" -> Obj(jobFacets)),
      "inputs" -> inputs,
      "outputs" -> outputs,
      "producer" -> Producer,
      "schemaURL" -> EventSchemaUrl
    )
  }

  private def invaractRun(run: RunState, terminal: Boolean): Seq[(String, Any)] =
    Seq("mode" -> (if (run.dryRun) "dryRun" else "enforce"), "invaractRunId" -> run.job.flatMap(_.runId).orNull) ++
      run.job.map(j => "jobId" -> j.jobId.orNull).toSeq ++
      run.job.filter(_.attributes.nonEmpty).map(j => "attributes" -> j.attributes).toSeq ++
      (if (terminal) Seq("checksPassed" -> run.checksPassed, "checksFailed" -> run.checksFailed, "violations" -> run.violations, "writes" -> run.writes)
       else Nil) filter { case (_, v) => v != null }

  // --- datasets ---------------------------------------------------------------------------------------------------

  private def input(d: LineageDataset): Obj = {
    val id = OpenLineageNaming.dataset(d.location, d.catalog, workingDirectory, d.format)
    obj("namespace" -> id.namespace, "name" -> id.name, "facets" -> Obj(datasetFacets(d, id)))
  }

  private def checkedOutput(l: WriteLineage, e: ContractValidationEvent): Obj =
    outputOf(l, contractFacet(e, l.output).toList, assertions(e, l.output).toList)

  /** A write's output dataset: its own facets and column lineage, then `facets` and `outputFacets` of the event. */
  private def outputOf(l: WriteLineage, facets: List[(String, Any)], outputFacets: List[(String, Any)]): Obj = {
    val d = l.output
    val id = OpenLineageNaming.dataset(d.location, d.catalog, workingDirectory, d.format)
    val lineage = if (settings.enabled("columnLineage")) columnLineage(l) else None
    val all = datasetFacets(d, id) ++ lineage.toList ++ lifecycle(l.saveMode).toList ++ facets
    obj("namespace" -> id.namespace, "name" -> id.name, "facets" -> Obj(all), "outputFacets" -> Obj(outputFacets))
  }

  private def writtenOutput(e: WriteEvent): Obj = {
    val d = LineageDataset(e.location, e.format, Some(com.invaract.contract.LogicalSchema(e.schema.map(f => LogicalField(f.name, LogicalType.OtherType(f.dataType), f.nullable)))), e.catalog)
    val id = OpenLineageNaming.dataset(e.location, e.catalog, workingDirectory, e.format)
    val version = e.deltaVersion.orElse(e.icebergSnapshotId).flatMap(v => facet("version", standard("DatasetVersionDatasetFacet", "1-0-1"), "datasetVersion" -> v.toString))
    val statistics = if (e.rowCount.isEmpty && e.bytesWritten.isEmpty && e.fileCount.isEmpty) None
      else facet("outputStatistics", standard("OutputStatisticsOutputDatasetFacet", "1-0-2"), "rowCount" -> e.rowCount, "size" -> e.bytesWritten, "fileCount" -> e.fileCount)
    obj(
      "namespace" -> id.namespace, "name" -> id.name,
      "facets" -> Obj(datasetFacets(d, id) ++ version.toList ++ lifecycle(e.saveMode).toList),
      "outputFacets" -> Obj(statistics.toList)
    )
  }

  private def inferredOutput(location: String, e: ContractInferenceEvent): Obj = {
    val id = OpenLineageNaming.dataset(location, None, workingDirectory)
    obj("namespace" -> id.namespace, "name" -> id.name, "facets" -> Obj(inferredFacet(e).toList))
  }

  private def inferredFacet(e: ContractInferenceEvent): Option[(String, Any)] =
    facet("invaract_contract", invaract("InvaractContractDatasetFacet"), "status" -> s"DRY_RUN_${e.status}", "dryRun" -> true)

  private def datasetFacets(d: LineageDataset, id: DatasetId): List[(String, Any)] =
    List(
      d.schema.map(_.fields.take(settings.schemaMaxFields)).filter(_.nonEmpty).flatMap(fs => facet("schema", standard("SchemaDatasetFacet", "1-2-0"), "fields" -> fields(fs))),
      d.format.flatMap(f => facet("storage", standard("StorageDatasetFacet", "1-0-1"), "storageLayer" -> storageLayer(f), "fileFormat" -> fileFormat(f))),
      d.catalog.flatMap(OpenLineageNaming.table).filter(_ != id).flatMap(t => facet("symlinks", standard("SymlinksDatasetFacet", "1-0-1"),
        "identifiers" -> List(obj("namespace" -> t.namespace, "name" -> t.name, "type" -> "TABLE")))),
      d.catalog.flatMap(c => c.table.flatMap(_ => facet("catalog", standard("CatalogDatasetFacet", "1-1-0"),
        "framework" -> c.technology.getOrElse("unknown"), "type" -> c.technology.getOrElse("unknown"),
        "name" -> c.catalogName.getOrElse(c.technology.getOrElse("unknown")), "warehouseUri" -> c.location)))
    ).flatten

  private def fields(fs: List[LogicalField]): List[Obj] = fs.zipWithIndex.map { case (f, i) =>
    val nested = f.dataType match {
      case LogicalType.StructType(children) => Some(fields(children))
      case _                                => None
    }
    obj("name" -> f.name, "type" -> f.dataType.catalogString, "ordinal_position" -> (i + 1), "fields" -> nested)
  }

  private def columnLineage(l: WriteLineage): Option[(String, Any)] = {
    val edges = l.columnLineage
    if (edges.isEmpty) None
    else {
      // one input field per source, with every way it reaches the field: a column can be a CASE WHEN's value and its
      // test at once (DIRECT and INDIRECT/CONDITIONAL)
      def inputFields(es: List[ColumnLineageEdge]): List[Obj] =
        es.groupBy(_.source).toList.sortBy { case (s, _) => (s.location, s.field) }.map { case (source, bySource) =>
          val known = l.inputs.find(_.location == source.location)
          val id = OpenLineageNaming.dataset(source.location, known.flatMap(_.catalog), workingDirectory, known.flatMap(_.format))
          obj("namespace" -> id.namespace, "name" -> id.name, "field" -> source.field,
            "transformations" -> bySource.map(_.transformation).distinct.map(t =>
              obj("type" -> t.kind, "subtype" -> t.subtype, "description" -> t.description, "masking" -> t.masking)))
        }
      val perField = edges.filter(_.field.isDefined).groupBy(_.field.get).toList.sortBy(_._1).take(settings.columnLineageMaxFields)
      val dataset = edges.filter(_.field.isEmpty)
      facet("columnLineage", standard("ColumnLineageDatasetFacet", "1-2-0"),
        "fields" -> Obj(perField.map { case (field, es) => field -> obj("inputFields" -> inputFields(es)) }),
        "dataset" -> (if (dataset.isEmpty) None else Some(inputFields(dataset))))
    }
  }

  private def lifecycle(saveMode: Option[String]): Option[(String, Any)] =
    saveMode.map(_.toLowerCase).collect {
      case "overwrite"                          => "OVERWRITE"
      case "create" | "errorifexists" | "error" => "CREATE"
    }.flatMap(change => facet("lifecycleStateChange", standard("LifecycleStateChangeDatasetFacet", "1-0-1"), "lifecycleStateChange" -> change))

  private def contractFacet(e: ContractValidationEvent, d: LineageDataset): Option[(String, Any)] =
    facet("invaract_contract", invaract("InvaractContractDatasetFacet"),
      "contract" -> e.contract,
      "dataset" -> d.declaredName,
      "status" -> e.status,
      "violations" -> e.violations.map(violation),
      "unverifiableInputs" -> e.unverifiableInputs.map(u => obj("name" -> u.inputName, "location" -> u.inputLocation)),
      "fingerprint" -> e.fingerprints.map(f => obj("algorithm" -> f.overall.algorithm, "value" -> f.overall.value)))

  private def violation(v: Violation): Obj =
    obj("type" -> v.violationType, "message" -> v.message, "remediation" -> v.remediation,
      "column" -> v.column, "location" -> v.location, "expected" -> v.expected, "actual" -> v.actual, "rule" -> v.rule)

  private def assertions(e: ContractValidationEvent, d: LineageDataset): Option[(String, Any)] = {
    val contract = obj("assertion" -> "invaract_contract", "success" -> (e.status == "PASSED"), "severity" -> "error",
      "name" -> e.contract, "description" -> s"the write ${if (e.status == "PASSED") "keeps to" else "breaks"} contract ${e.contract}")
    val violated = e.violations.map(v => obj("assertion" -> v.violationType.toLowerCase, "success" -> false, "severity" -> "error",
      "column" -> v.column, "description" -> v.message, "expected" -> v.expected, "actual" -> v.actual))
    val static = e.dataQuality.map(q => obj("assertion" -> q.constraint, "success" -> (q.verdict.toString != "Violated"),
      "column" -> q.field, "severity" -> "error", "description" -> s"statically ${q.verdict}"))
    facet("dataQualityAssertions", standard("DataQualityAssertionsDatasetFacet", "1-1-0"), "assertions" -> (contract :: violated ++ static))
  }

  private def errorMessage(e: ContractValidationEvent): Option[(String, Any)] =
    facet("errorMessage", standard("ErrorMessageRunFacet", "1-0-1"),
      "message" -> s"contract ${e.contract} refused the write: ${e.violations.map(v => s"[${v.violationType}] ${v.message}").mkString("; ")}",
      "programmingLanguage" -> "Scala")

  // --- facets -----------------------------------------------------------------------------------------------------

  private def facet(name: String, schemaUrl: String, fields: (String, Any)*): Option[(String, Any)] =
    if (!settings.enabled(name)) None
    else Some(name -> Obj(Seq("_producer" -> Producer, "_schemaURL" -> schemaUrl) ++ fields.filter { case (_, v) => v != None && v != null }))

  private def idOf(d: Obj): (String, String) = {
    val m = d.entries.toMap
    (m("namespace").toString, m("name").toString)
  }
}

private[openlineage] object OpenLineageMapper {

  /** Invaract's version, as `processing_engine.openlineageAdapterVersion` and in `producer`. Held equal to the build's
    * version by a test. */
  val InvaractVersion = "0.4.0"

  val Producer = s"https://github.com/mlltx/Invaract/tree/verification-core-$InvaractVersion/verification-core"
  val EventSchemaUrl = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
  val InvaractFacetsBase = "https://mlltx.github.io/Invaract/schemas/openlineage/1-0-0/"

  /** The most events a `failuresOnly` run holds back before its end decides whether they are published. */
  val MaxHeldEvents = 1000

  def standard(facet: String, version: String): String = s"https://openlineage.io/spec/facets/$version/$facet.json#/$$defs/$facet"
  def invaract(facet: String): String = s"$InvaractFacetsBase$facet.json#/$$defs/$facet"

  /** OpenLineage's run id for Invaract run `key` of `engine`: a name-based UUID (RFC 4122 version 5, SHA-1), so every
    * process publishing for one run gives it the same id. */
  def runUuid(engine: String, key: String): String = {
    val namespace = UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8") // RFC 4122's URL namespace
    val digest = MessageDigest.getInstance("SHA-1")
    val ns = java.nio.ByteBuffer.allocate(16).putLong(namespace.getMostSignificantBits).putLong(namespace.getLeastSignificantBits).array()
    digest.update(ns)
    digest.update(s"invaract:$engine:$key".getBytes(StandardCharsets.UTF_8))
    val hash = digest.digest()
    hash(6) = ((hash(6) & 0x0f) | 0x50).toByte
    hash(8) = ((hash(8) & 0x3f) | 0x80).toByte
    val bb = java.nio.ByteBuffer.wrap(hash, 0, 16)
    new UUID(bb.getLong, bb.getLong).toString
  }

  def storageLayer(format: String): String = format.toLowerCase match {
    case f @ ("delta" | "iceberg" | "hudi") => f
    case _                                  => "default"
  }

  def fileFormat(format: String): String = format.toLowerCase

  final class RunState(val uuid: String, val namespace: String, val name: String, val job: Option[JobInfo], val generation: Int = 0) {
    var started = false
    var dryRun = false
    var checksPassed = 0L
    var checksFailed = 0L
    var violations = 0L
    var writes = 0L
    val inputs = mutable.LinkedHashMap.empty[(String, String), Obj]
    val outputs = mutable.LinkedHashMap.empty[(String, String), Obj]
    val endFacets = mutable.ListBuffer.empty[(String, Any)]
    val held = mutable.ListBuffer.empty[Obj]
  }
}
