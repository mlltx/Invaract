// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.invaract.contract.{Contract, ContractParser, LogicalSchema}
import com.invaract.contract.LogicalType.{IntegerType, LongType, StringType, StructType}
import com.invaract.contract.LogicalField
import com.invaract.ir._
import com.invaract.verification.{CheckedWrite, ContractViolationException, VerificationOptions, VerificationPipeline}
import com.invaract.verification.notification._

import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.collection.JavaConverters._

/** What each Invaract event is published as (docs/OPENLINEAGE.md, section 2), every event held to the OpenLineage schema. */
class OpenLineageMapperSpec extends AnyFunSuite {
  import OpenLineageMapperSpec._

  private val json = new ObjectMapper()
  private val settings = OpenLineageSettings(TransportSettings.Console, namespace = "ns")

  private def mapper(s: OpenLineageSettings = settings) = new OpenLineageMapper(s, () => Instant.parse("2026-11-25T10:00:00Z"), "/work")

  /** The events, each validated against the OpenLineage schema, as JSON trees. */
  private def published(m: OpenLineageMapper, events: NotificationEvent*): List[JsonNode] =
    events.toList.flatMap(m.map).map { e =>
      val text = JsonText.render(e)
      val errors = OpenLineageSchema.errors(text)
      assert(errors.isEmpty, errors.mkString("\n") + "\n" + text)
      json.readTree(text)
    }

  private def closed(m: OpenLineageMapper): List[JsonNode] = m.closeAll().map(e => json.readTree(JsonText.render(e)))

  private def types(events: List[JsonNode]) = events.map(_.get("eventType").asText())
  private def facetNames(node: JsonNode): Set[String] = Option(node).toList.flatMap(_.fieldNames().asScala).toSet

  test("a passing check: START, then RUNNING with its inputs and output; the run's end COMPLETE") {
    val m = mapper()
    val events = published(m, check())
    assert(types(events) == List("START", "RUNNING"))
    val List(start, running) = events
    assert(start.get("inputs").size == 0 && start.get("outputs").size == 0)
    assert(start.path("run").get("runId") == running.path("run").get("runId"))
    assert(running.path("job").get("namespace").asText() == "ns" && running.path("job").get("name").asText() == "orders_daily")
    val input = running.get("inputs").get(0)
    assert((input.get("namespace").asText(), input.get("name").asText()) == (("file", "/work/in/orders")))
    val output = running.get("outputs").get(0)
    assert((output.get("namespace").asText(), output.get("name").asText()) == (("file", "/work/out/report")))
    assert(facetNames(output.get("facets")) == Set("schema", "storage", "columnLineage", "lifecycleStateChange", "invaract_contract"))
    assert(facetNames(output.get("outputFacets")) == Set("dataQualityAssertions"))
    assert(facetNames(running.path("run").get("facets")) == Set("processing_engine", "invaract_run"))
    assert(facetNames(running.path("job").get("facets")) == Set("jobType"))
    assert(m.openRuns == 1)
    val List(end) = closed(m)
    assert(end.get("eventType").asText() == "COMPLETE" && end.path("run").get("runId") == start.path("run").get("runId"))
    val totals = end.path("run").path("facets").get("invaract_run")
    assert((totals.get("checksPassed").asLong, totals.get("checksFailed").asLong, totals.get("violations").asLong, totals.get("writes").asLong) == ((1L, 0L, 0L, 0L)))
    assert(running.path("run").path("facets").get("invaract_run").get("checksPassed") == null, "totals only on the terminal event")
    assert(m.openRuns == 0)
  }

  test("the output's facets: schema with nested fields, storage, contract verdict, assertions, lifecycle") {
    val output = published(mapper(), check(outputSchema = nestedOutput))(1).get("outputs").get(0)
    val fields = output.path("facets").path("schema").get("fields")
    assert(fields.size == 3)
    assert(fields.get(0).get("name").asText() == "id" && fields.get(0).get("type").asText() == "bigint" && fields.get(0).get("ordinal_position").asInt == 1)
    val nested = fields.get(2)
    assert(nested.get("type").asText() == "struct<city:string>" && nested.get("fields").get(0).get("name").asText() == "city")
    assert(fields.get(0).get("fields") == null, "a scalar has no nested fields")
    assert(output.path("facets").path("storage").get("storageLayer").asText() == "default" && output.path("facets").path("storage").get("fileFormat").asText() == "parquet")
    val contract = output.path("facets").get("invaract_contract")
    assert(contract.get("contract").asText() == "pipe@1.0.0" && contract.get("status").asText() == "PASSED" && contract.get("dataset").asText() == "out")
    assert(contract.get("violations").size == 0)
    val assertions = output.path("outputFacets").path("dataQualityAssertions").get("assertions")
    assert(assertions.size == 1 && assertions.get(0).get("assertion").asText() == "invaract_contract" && assertions.get(0).get("success").asBoolean)
    assert(output.path("facets").path("lifecycleStateChange").get("lifecycleStateChange").asText() == "OVERWRITE")
  }

  test("column lineage: each output column's source column in its input dataset, how it is derived") {
    val output = published(mapper(), check())(1).get("outputs").get(0)
    val fields = output.path("facets").path("columnLineage").get("fields")
    assert(fields.fieldNames().asScala.toList == List("id", "total"))
    val id = fields.get("id").get("inputFields").get(0)
    assert((id.get("namespace").asText(), id.get("name").asText(), id.get("field").asText()) == (("file", "/work/in/orders", "id")))
    assert(id.get("transformations").get(0).get("type").asText() == "DIRECT" && id.get("transformations").get(0).get("subtype").asText() == "IDENTITY")
    val total = fields.get("total").get("inputFields").get(0)
    assert(total.get("field").asText() == "amount" && total.get("transformations").get(0).get("subtype").asText() == "TRANSFORMATION")
  }

  test("column lineage of a filter, a join and a grouping: INDIRECT on the whole dataset") {
    val l = WriteLineage(LineageDataset("out/x"), Nil, plan = Some(groupedJoinPlan))
    val edges = l.columnLineage
    assert(edges.exists(e => e.field.isEmpty && e.transformation == LineageTransformation("INDIRECT", "FILTER") && e.source == LineageSource("in/a", "flag")))
    assert(edges.exists(e => e.field.isEmpty && e.transformation == LineageTransformation("INDIRECT", "JOIN") && e.source == LineageSource("in/b", "k")))
    assert(edges.exists(e => e.field.isEmpty && e.transformation == LineageTransformation("INDIRECT", "GROUP_BY") && e.source == LineageSource("in/a", "region")))
    val summed = edges.filter(_.field.contains("total"))
    assert(summed.map(_.transformation).distinct == List(LineageTransformation("DIRECT", "AGGREGATION", Some("SUM"))))
    assert(edges == edges.distinct, "no edge twice")
  }

  test("column lineage leaves out a source it cannot place, and is empty for a plan it cannot see into") {
    // two reads, an unqualified column: the IR attributes it to every read it could come from (ir.Lineage's own rule), each placed
    val ambiguous = Write(DatasetRef("out/x"), Project(Join(Read(DatasetRef("in/a")), Read(DatasetRef("in/b")), JoinType.Inner), List(NamedExpr("v", ColumnReference(ColumnRef("v"))))))
    assert(WriteLineage(LineageDataset("out/x"), Nil, plan = Some(ambiguous)).columnLineage.map(_.source) == List(LineageSource("in/a", "v"), LineageSource("in/b", "v")))
    val single = Write(DatasetRef("out/x"), Project(Read(DatasetRef("in/a")), List(NamedExpr("v", ColumnReference(ColumnRef("v"))))))
    assert(WriteLineage(LineageDataset("out/x"), Nil, plan = Some(single)).columnLineage.map(_.source) == List(LineageSource("in/a", "v")), "one read: an unqualified column is its")
    val unknownScope = Write(DatasetRef("out/x"), Project(Read(DatasetRef("in/a")), List(NamedExpr("v", ColumnReference(ColumnRef("v", Some("elsewhere")))))))
    assert(WriteLineage(LineageDataset("out/x"), Nil, plan = Some(unknownScope)).columnLineage.isEmpty, "a qualifier that is no read's")
    val opaque = Write(DatasetRef("out/x"), UnknownPlan("job code", "Python", List(Read(DatasetRef("in/a")))))
    assert(WriteLineage(LineageDataset("out/x"), Nil, plan = Some(opaque)).columnLineage.isEmpty)
    assert(WriteLineage(LineageDataset("out/x"), Nil).columnLineage.isEmpty)
    val output = published(mapper(), check(plan = Write(DatasetRef("out/report"), UnknownPlan("job code", "Python", List(Read(DatasetRef(orders)))), Some("parquet"), Some("overwrite"))))(1).get("outputs").get(0)
    assert(!facetNames(output.get("facets")).contains("columnLineage"), "no column lineage is published rather than an empty one")
  }

  test("a UDF's output is a TRANSFORMATION said to be opaque") {
    val udf = Write(DatasetRef("out/x"), Project(Read(DatasetRef("in/a")), List(NamedExpr("h", UDF(Some("hash"), List(ColumnReference(ColumnRef("v", Some("in/a")))))))))
    assert(WriteLineage(LineageDataset("out/x"), Nil, plan = Some(udf)).columnLineage.map(_.transformation) ==
      List(LineageTransformation("DIRECT", "TRANSFORMATION", Some("opaque: a function Invaract cannot see into"))))
  }

  test("a refused check: RUNNING with the violation and errorMessage, and the run FAILs") {
    val m = mapper()
    val events = published(m, check(outputSchema = Cols(("id", LongType, false))))
    val running = events(1)
    val output = running.get("outputs").get(0)
    val contract = output.path("facets").get("invaract_contract")
    assert(contract.get("status").asText() == "FAILED" && contract.get("violations").size >= 1)
    val violation = contract.get("violations").get(0)
    assert(violation.get("type").asText() == "MISSING_OUTPUT_FIELD" && violation.get("column").asText() == "total" && violation.has("remediation"))
    val assertions = output.path("outputFacets").path("dataQualityAssertions").get("assertions").elements().asScala.toList
    assert(assertions.head.get("success").asBoolean == false)
    assert(assertions.exists(a => a.get("assertion").asText() == "missing_output_field" && a.get("column").asText() == "total" && !a.get("success").asBoolean))
    val error = running.path("run").path("facets").get("errorMessage")
    assert(error.get("message").asText().startsWith("contract pipe@1.0.0 refused the write: [MISSING_OUTPUT_FIELD]") && error.get("programmingLanguage").asText() == "Scala")
    val List(end) = closed(m)
    assert(end.get("eventType").asText() == "FAIL")
    assert(end.path("run").path("facets").get("invaract_run").get("checksFailed").asLong == 1)
  }

  test("a JobSummaryEvent ends the run it summarizes, FAIL when it counts a failed check; a summary of an unknown run is nothing") {
    val m = mapper()
    published(m, check())
    assert(published(m, JobSummaryEvent(1, 1, 0, 0, 10, 2L, Map.empty, job = Some(job.copy(runId = Some("other"))))).isEmpty)
    assert(m.openRuns == 1)
    val ended = published(m, JobSummaryEvent(1, 1, 1, 1, 10, 2L, Map.empty, job = Some(job)))
    assert(types(ended) == List("FAIL") && m.openRuns == 0)
    assert(published(mapper(), JobSummaryEvent(0, 0, 0, 0, 0, 1L, Map.empty, job = Some(job))).isEmpty)
  }

  test("a completed write: RUNNING with its statistics, version and lifecycle") {
    val write = WriteEvent(Some("pipe@1.0.0"), "s3a://bucket/out/report", Some("delta"), Some("overwrite"),
      List(WriteFieldInfo("id", "bigint", nullable = false)), 3L, Map.empty, rowCount = Some(10), bytesWritten = Some(2048), fileCount = Some(2),
      deltaVersion = Some(7), job = Some(job))
    val running = published(mapper(), write)(1)
    val output = running.get("outputs").get(0)
    assert((output.get("namespace").asText(), output.get("name").asText()) == (("s3://bucket", "out/report")))
    val stats = output.path("outputFacets").get("outputStatistics")
    assert((stats.get("rowCount").asLong, stats.get("size").asLong, stats.get("fileCount").asLong) == ((10L, 2048L, 2L)))
    assert(output.path("facets").path("version").get("datasetVersion").asText() == "7")
    assert(output.path("facets").path("storage").get("storageLayer").asText() == "delta")
    assert(output.path("facets").path("schema").get("fields").get(0).get("type").asText() == "bigint")
    val iceberg = published(mapper(), write.copy(format = Some("iceberg"), deltaVersion = None, icebergSnapshotId = Some(99L), rowCount = None, bytesWritten = None, fileCount = None, saveMode = Some("append")))(1)
    val out = iceberg.get("outputs").get(0)
    assert(out.path("facets").path("version").get("datasetVersion").asText() == "99" && !facetNames(out.get("outputFacets")).contains("outputStatistics"))
    assert(!facetNames(out.get("facets")).contains("lifecycleStateChange"), "an append changes no lifecycle state")
    val totals = closed({ val m = mapper(); published(m, write); m }).head.path("run").path("facets").get("invaract_run")
    assert(totals.get("writes").asLong == 1)
  }

  test("lifecycle: overwrite is OVERWRITE; create, error and errorIfExists are CREATE; others none") {
    def lifecycle(mode: String) = {
      val o = published(mapper(), check(plan = goodPlan.copy(saveMode = Some(mode))))(1).get("outputs").get(0)
      Option(o.path("facets").get("lifecycleStateChange")).map(_.get("lifecycleStateChange").asText())
    }
    assert(lifecycle("Overwrite").contains("OVERWRITE"))
    assert(List("create", "error", "errorIfExists").forall(lifecycle(_).contains("CREATE")))
    assert(lifecycle("append").isEmpty && lifecycle("ignore").isEmpty)
  }

  test("catalog tables: symlinks and catalog facets") {
    val catalog = CatalogIdentity(Some("iceberg"), Some("prod"), Some("s3://wh/db/t"), List("db"), Some("t"))
    val plan = goodPlan.copy(dataset = DatasetRef("s3://wh/db/t/data"), catalog = Some(catalog))
    val output = published(mapper(), check(plan = plan))(1).get("outputs").get(0)
    val link = output.path("facets").path("symlinks").get("identifiers").get(0)
    assert((link.get("namespace").asText(), link.get("name").asText(), link.get("type").asText()) == (("s3://wh", "db.t", "TABLE")))
    val c = output.path("facets").get("catalog")
    assert((c.get("framework").asText(), c.get("name").asText(), c.get("warehouseUri").asText()) == (("iceberg", "prod", "s3://wh/db/t")))
    val bare = published(mapper(), check(plan = goodPlan.copy(dataset = DatasetRef("db.t"), catalog = Some(catalog))))(1).get("outputs").get(0)
    assert(bare.get("name").asText() == "db.t" && !facetNames(bare.get("facets")).contains("symlinks"), "a dataset named by its table needs no symlink to itself")
  }

  test("run and job identity: a deterministic UUID per engine and run, the job's name from jobName, jobId, name or contract") {
    val a = published(mapper(), check())(0).path("run").get("runId").asText()
    assert(a == published(mapper(), check())(0).path("run").get("runId").asText(), "the same run is the same id, from any process")
    assert(a == OpenLineageMapper.runUuid("spark", "app-1"))
    assert(OpenLineageMapper.runUuid("spark", "app-2") != a && OpenLineageMapper.runUuid("beam", "app-1") != a)
    assert(java.util.UUID.fromString(a).version == 5 && java.util.UUID.fromString(a).variant == 2)
    // RFC 4122 version 5 of "invaract:<engine>:<run>" in the URL namespace, as Python's uuid.uuid5 computes it
    assert(OpenLineageMapper.runUuid("x", "y") == "c3cef2ba-db3b-5736-bfcc-1ec79ef4f4d7")
    assert(a == "a619078b-3773-5341-bab8-a20551d8b136")
    def name(s: OpenLineageSettings, j: Option[JobInfo]) = published(mapper(s), check(jobInfo = j))(0).path("job").get("name").asText()
    assert(name(settings.copy(jobName = Some("configured")), Some(job)) == "configured")
    assert(name(settings, Some(job)) == "orders_daily")
    assert(name(settings, Some(job.copy(jobId = None))) == "spark-app-name")
    assert(name(settings, Some(job.copy(jobId = None, name = None))) == "pipe@1.0.0")
    assert(name(settings, None) == "pipe@1.0.0")
  }

  test("processing_engine, invaract_run and parent run facets") {
    val parent = ParentRun("airflow", "dag.task", "01890b4c-bb6b-7a8c-8f5b-2a6c35dd2a0e")
    val facets = published(mapper(settings.copy(parent = Some(parent))), check())(0).path("run").get("facets")
    val engine = facets.get("processing_engine")
    assert((engine.get("name").asText(), engine.get("version").asText(), engine.get("openlineageAdapterVersion").asText()) == (("spark", "3.5.9", OpenLineageMapper.InvaractVersion)))
    val p = facets.get("parent")
    assert(p.path("run").get("runId").asText() == parent.runId && p.path("job").get("namespace").asText() == "airflow" && p.path("job").get("name").asText() == "dag.task")
    val run = facets.get("invaract_run")
    assert(run.get("mode").asText() == "enforce" && run.get("invaractRunId").asText() == "app-1" && run.get("jobId").asText() == "orders_daily")
    assert(run.get("attributes").get("team").asText() == "orders")
    val bare = published(mapper(), check(jobInfo = None))(0).path("run").get("facets")
    assert(bare.get("processing_engine").get("name").asText() == "invaract" && bare.get("processing_engine").get("version").asText() == "unknown")
    assert(!bare.get("invaract_run").has("attributes") && !bare.get("invaract_run").has("jobId") && !bare.get("invaract_run").has("invaractRunId"))
  }

  test("the run's totals are on its terminal event only: checks passed and failed, violations, writes") {
    val m = mapper()
    val events = published(m, check(), check(outputSchema = Cols(("id", LongType, false))))
    assert(events.forall(e => !e.path("run").path("facets").get("invaract_run").has("checksPassed")), "START and RUNNING carry no totals")
    val totals = closed(m).head.path("run").path("facets").get("invaract_run")
    assert((totals.get("checksPassed").asLong, totals.get("checksFailed").asLong, totals.get("writes").asLong) == ((1L, 1L, 0L)))
    assert(totals.get("violations").asLong >= 1)
  }

  test("the contract assertion says whether the write kept to the contract or broke it") {
    def assertion(e: ContractValidationEvent) = published(mapper(), e)(1).get("outputs").get(0).path("outputFacets").path("dataQualityAssertions").get("assertions").get(0)
    val kept = assertion(check())
    assert(kept.get("success").asBoolean && kept.get("description").asText().contains("keeps to"))
    val broken = assertion(check(outputSchema = Cols(("id", LongType, false))))
    assert(!broken.get("success").asBoolean && broken.get("description").asText().contains("breaks"))
  }

  test("only statistics a write reports are published, and one is enough for the facet") {
    val write = WriteEvent(None, "/out/x", None, None, Nil, 3L, Map.empty, job = Some(job))
    def stats(w: WriteEvent) = published(mapper(), w)(1).get("outputs").get(0).path("outputFacets").get("outputStatistics")
    assert(stats(write) == null)
    assert(stats(write.copy(rowCount = Some(5))).get("rowCount").asLong == 5 && !stats(write.copy(rowCount = Some(5))).has("size"))
    assert(stats(write.copy(bytesWritten = Some(9))).get("size").asLong == 9)
    assert(stats(write.copy(fileCount = Some(2))).get("fileCount").asLong == 2)
  }

  test("dataset-level lineage (filters, joins, groupings) is under columnLineage.dataset, only when there is some") {
    def lineage(plan: Plan) = published(mapper(), check(plan = plan))(1).get("outputs").get(0).path("facets").get("columnLineage")
    val grouped = lineage(groupedJoinPlan)
    assert(grouped.has("dataset") && grouped.get("dataset").size > 0 && grouped.get("fields").size > 0)
    val plain = WriteLineage(LineageDataset("out/x"), Nil, plan = Some(goodPlan)).columnLineage
    assert(lineage(goodPlan).has("dataset") == plain.exists(_.field.isEmpty))
    val onlyColumnLineage = published(mapper(settings.copy(disabledFacets = Set("columnLineage"))), check())(1).get("outputs").get(0).get("facets")
    assert(!facetNames(onlyColumnLineage).contains("columnLineage") && facetNames(onlyColumnLineage).contains("schema"))
  }

  test("failuresOnly holds at most MaxHeldEvents of a run; its terminal event is always published") {
    val m = mapper(settings.copy(failuresOnly = true))
    (1 to OpenLineageMapper.MaxHeldEvents + 5).foreach(i => assert(m.map(WriteEvent(None, s"/out/$i", None, None, Nil, 1L, Map.empty, job = Some(job))).isEmpty))
    val ended = m.map(JobSummaryEvent(0, 0, 1, 1, 0, 1L, Map.empty, job = Some(job)))
    assert(ended.size == OpenLineageMapper.MaxHeldEvents + 1)
    assert(types(ended.map(e => json.readTree(JsonText.render(e)))).last == "FAIL")
  }

  test("dry run: each inferred write is an output marked with the inference, and the dry-run summary ends the run") {
    val m = mapper()
    val inferred = ContractInferenceEvent(InferenceStatus.Inferred, None, Some("/data/out"), Some("id: x"), None, Nil, Nil, None, job, 1L, Map.empty)
    val events = published(m, inferred, inferred.copy(writeLocation = None))
    assert(types(events) == List("START", "RUNNING", "RUNNING"))
    val output = events(1).get("outputs").get(0)
    assert(output.get("name").asText() == "/data/out")
    val c = output.path("facets").get("invaract_contract")
    assert(c.get("status").asText() == "DRY_RUN_INFERRED" && c.get("dryRun").asBoolean)
    assert(events(1).path("run").path("facets").get("invaract_run").get("mode").asText() == "dryRun")
    assert(events(2).get("outputs").size == 0, "an inference with no location names no dataset")
    val end = published(m, DryRunSummaryEvent(Map("INFERRED" -> 1L), MergeStatus.Merged, None, Nil, job, 2L, Map.empty))
    assert(types(end) == List("COMPLETE") && m.openRuns == 0)
  }

  test("code scans publish nothing") {
    assert(mapper().map(CodeScanEvent(CodeScanStatus.Clean, CodeScanPhase.Start, "report", refused = false, "lint", Nil, Map.empty, Nil, 0L, Nil, 1L, 1L, None, Map.empty, job = Some(job))).isEmpty)
  }

  test("endOfRun: START, then the terminal event carrying every dataset seen, each once") {
    val m = mapper(settings.copy(emission = Emission.EndOfRun))
    assert(published(m, check(), check(), check(outputSchema = Cols(("id", LongType, false)))).isEmpty)
    val List(start, end) = closed(m)
    assert((start.get("eventType").asText(), end.get("eventType").asText()) == (("START", "FAIL")))
    assert(end.get("inputs").size == 1 && end.get("outputs").size == 1, "the same dataset is one node")
    assert(end.path("run").path("facets").has("errorMessage"))
    assert(OpenLineageSchema.errors(json.writeValueAsString(end)).isEmpty)
  }

  test("failuresOnly: a run with no refused write publishes nothing; one with a refusal publishes all of it") {
    val clean = mapper(settings.copy(failuresOnly = true))
    assert(published(clean, check(), check()).isEmpty && closed(clean).isEmpty)
    val refused = mapper(settings.copy(failuresOnly = true))
    assert(published(refused, check(), check(outputSchema = Cols(("id", LongType, false)))).isEmpty)
    assert(types(closed(refused)) == List("START", "RUNNING", "RUNNING", "FAIL"))
  }

  test("facets.disabled leaves facets out; schema.maxFields and columnLineage.maxFields bound them") {
    val m = mapper(settings.copy(disabledFacets = Set("schema", "columnLineage", "invaract_contract", "processing_engine", "jobType", "errorMessage", "dataQualityAssertions")))
    val running = published(m, check(outputSchema = Cols(("id", LongType, false))))(1)
    val output = running.get("outputs").get(0)
    assert(facetNames(output.get("facets")) == Set("storage", "lifecycleStateChange"))
    assert(facetNames(output.get("outputFacets")).isEmpty)
    assert(facetNames(running.path("run").get("facets")) == Set("invaract_run") && facetNames(running.path("job").get("facets")).isEmpty)
    val bounded = published(mapper(settings.copy(schemaMaxFields = 1, columnLineageMaxFields = 1)), check())(1).get("outputs").get(0)
    assert(bounded.path("facets").path("schema").get("fields").size == 1)
    assert(bounded.path("facets").path("columnLineage").get("fields").size == 1)
    val none = published(mapper(settings.copy(schemaMaxFields = 0)), check())(1).get("outputs").get(0)
    assert(!facetNames(none.get("facets")).contains("schema"), "no fields at all is no schema facet")
  }

  test("a completed write reported after its run's end is dropped; a new check under the same key is a new run with its own id") {
    val m = mapper()
    val late = WriteEvent(None, "/out/x", None, None, Nil, 1L, Map.empty, job = Some(job))
    def ids(events: List[JsonText.Obj]) = events.map(e => json.readTree(JsonText.render(e))).map(n => n.get("eventType").asText() -> n.path("run").get("runId").asText())
    val first = OpenLineageMapper.runUuid("spark", "app-1")
    assert(ids(m.map(check())) == List("START" -> first, "RUNNING" -> first))
    assert(ids(m.endRun(Some(job))) == List("COMPLETE" -> first))
    assert(m.map(late).isEmpty && m.openRuns == 0, "the ended run is not reopened by its own write's report")
    val second = OpenLineageMapper.runUuid("spark", "app-1#1")
    assert(ids(m.map(check())) == List("START" -> second, "RUNNING" -> second) && second != first)
    assert(ids(m.endRun(Some(job))) == List("COMPLETE" -> second))
    assert(ids(m.map(check())).head == ("START" -> OpenLineageMapper.runUuid("spark", "app-1#2")), "and a third")
    val inferred = ContractInferenceEvent(InferenceStatus.Inferred, None, Some("/out/x"), None, None, Nil, Nil, None, job.copy(runId = Some("dry")), 1L, Map.empty)
    m.map(inferred); m.endRun(Some(job.copy(runId = Some("dry"))))
    assert(ids(m.map(inferred)).head == ("START" -> OpenLineageMapper.runUuid("spark", "dry#1")), "a dry run's new inference is a new run too")
    assert(types(published(m, late.copy(job = Some(job.copy(runId = Some("another")))))) == List("START", "RUNNING"), "other runs go on")
  }

  test("the ended runs remembered are bounded, the oldest forgotten first") {
    val m = new OpenLineageMapper(settings, () => Instant.EPOCH, "/w", maxOpenRuns = 1)
    def runOf(id: String) = WriteEvent(None, "/out/x", None, None, Nil, 1L, Map.empty, job = Some(job.copy(runId = Some(id))))
    (1 to 5).foreach { i => m.map(runOf(s"r$i")); m.endRun(Some(job.copy(runId = Some(s"r$i")))) }
    assert(m.map(runOf("r1")).nonEmpty, "beyond four times maxOpenRuns, the oldest ended run is forgotten")
    assert(m.map(runOf("r5")).isEmpty, "a recent one is not")
  }

  test("runs are kept apart, and at most maxOpenRuns stay open, the oldest giving way") {
    val m = new OpenLineageMapper(settings, () => Instant.EPOCH, "/w", maxOpenRuns = 2)
    published(m, check(jobInfo = Some(job.copy(runId = Some("r1")))), check(jobInfo = Some(job.copy(runId = Some("r2")))))
    assert(m.openRuns == 2)
    published(m, check(jobInfo = Some(job.copy(runId = Some("r3")))))
    assert(m.openRuns == 2)
    val ids = closed(m).map(_.path("run").get("runId").asText()).toSet
    assert(ids == Set(OpenLineageMapper.runUuid("spark", "r2"), OpenLineageMapper.runUuid("spark", "r3")))
  }

  test("a check with no lineage (an invalid contract) still publishes the run, naming no dataset") {
    val event = ContractValidationEvent("bad@1", "FAILED", List(com.invaract.verification.Violation("INVALID_CONTRACT", "m", "r")), 1L, Map.empty, job = Some(job))
    val running = published(mapper(), event)(1)
    assert(running.get("outputs").size == 0 && running.path("run").path("facets").has("errorMessage"))
  }

  test("storage layer: table formats are their own layer, files the default") {
    assert(OpenLineageMapper.storageLayer("Delta") == "delta" && OpenLineageMapper.storageLayer("iceberg") == "iceberg" && OpenLineageMapper.storageLayer("hudi") == "hudi")
    assert(OpenLineageMapper.storageLayer("parquet") == "default" && OpenLineageMapper.fileFormat("PARQUET") == "parquet")
  }

  test("the version Invaract publishes is the build's") {
    val sbt = scala.io.Source.fromFile(EventSchema.repoFile("verification-core/build.sbt")).mkString
    assert(sbt.contains("ThisBuild / version := \"" + OpenLineageMapper.InvaractVersion + "\""))
  }

  /** A check run through the real pipeline, returning the event it published. */
  private def check(plan: Plan = goodPlan, outputSchema: LogicalSchema = goodOutput, jobInfo: Option[JobInfo] = Some(job)): ContractValidationEvent = {
    val sink = new TestNotificationSink
    try VerificationPipeline.verifyWrite(contract, CheckedWrite(plan, List(orders -> inputSchema), outputSchema, caseSensitive = false, None, Set.empty),
      VerificationOptions(), Some(sink), None, None, jobInfo)
    catch { case _: ContractViolationException => () }
    sink.events.collect { case e: ContractValidationEvent => e }.head
  }
}

object OpenLineageMapperSpec {
  val orders = "in/orders"

  val contract: Contract = ContractParser.parse(
    """id: pipe
      |version: "1.0.0"
      |inputs:
      |  - name: src
      |    location: in/orders
      |    format: parquet
      |    schema:
      |      fields:
      |        - { name: id, type: long, required: true }
      |        - { name: amount, type: integer }
      |outputs:
      |  - name: out
      |    location: out/report
      |    format: parquet
      |    schema:
      |      fields:
      |        - { name: id, type: long, required: true }
      |        - { name: total, type: integer, required: true }
      |""".stripMargin
  )

  def Cols(fields: (String, com.invaract.contract.LogicalType, Boolean)*): LogicalSchema =
    LogicalSchema(fields.toList.map { case (n, t, nullable) => LogicalField(n, t, nullable) })

  val inputSchema: LogicalSchema = Cols(("id", LongType, false), ("amount", IntegerType, true))
  val goodOutput: LogicalSchema = Cols(("id", LongType, false), ("total", IntegerType, false))
  val nestedOutput: LogicalSchema = LogicalSchema(goodOutput.fields :+ LogicalField("address", StructType(List(LogicalField("city", StringType))), nullable = true))

  private def col(name: String) = ColumnReference(ColumnRef(name, Some(orders)))

  val goodPlan: Write = Write(
    DatasetRef("out/report"),
    Project(Read(DatasetRef(orders)), List(NamedExpr("id", col("id")), NamedExpr("total", Arithmetic("+", List(col("amount"), Literal(1, "int")))))),
    Some("parquet"),
    Some("overwrite")
  )

  val groupedJoinPlan: Write = {
    val a = ColumnRef("region", Some("in/a"))
    Write(DatasetRef("out/x"),
      Aggregate(
        Filter(
          Join(Read(DatasetRef("in/a")), Read(DatasetRef("in/b")), JoinType.Inner, Some(Comparison("=", ColumnReference(ColumnRef("k", Some("in/a"))), ColumnReference(ColumnRef("k", Some("in/b")))))),
          ColumnReference(ColumnRef("flag", Some("in/a")))
        ),
        List(ColumnReference(a)),
        List(NamedExpr("region", ColumnReference(a)), NamedExpr("total", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("in/b"))))))
      ))
  }

  val job: JobInfo = JobInfo(runId = Some("app-1"), name = Some("spark-app-name"), jobId = Some("orders_daily"), engine = Some("spark"),
    engineVersion = Some("3.5.9"), attributes = Map("team" -> "orders"))
}
