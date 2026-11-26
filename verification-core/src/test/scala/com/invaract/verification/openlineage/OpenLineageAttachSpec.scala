// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.fasterxml.jackson.databind.ObjectMapper
import com.invaract.verification._
import com.invaract.verification.notification._
import com.invaract.contract.LogicalType.LongType

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** OpenLineage attached the way every adapter attaches it: `AttachSetup` from the neutral settings alone. */
class OpenLineageAttachSpec extends AnyFunSuite {
  import OpenLineageMapperSpec._

  private val json = new ObjectMapper()

  private def file(name: String, lines: String*): Path = {
    val f = Files.createTempDirectory("ol-attach").resolve(name)
    Files.write(f, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    f
  }

  private def events(path: Path): List[String] = if (Files.exists(path)) Files.readAllLines(path).asScala.toList else Nil

  private def contractFile: String = file("contract.yaml",
    """id: pipe""", """version: "1.0.0"""",
    """inputs:""", """  - { name: src, location: in/orders, format: parquet }""",
    """outputs:""", """  - { name: out, location: out/report, format: parquet }""").toString

  test("openLineageConfig alone attaches OpenLineage to an enforced job: a check publishes, valid, through the file transport") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val config = file("openlineage.properties", "transport=file", s"path=$out", "namespace=platform").toString
    val plan = AttachSetup.select(ConfigSource.fromMap(Map(InvaractConf.Contract -> contractFile, InvaractConf.OpenLineageConfig -> config)))
    val sink = plan.sink.get
    VerificationPipeline.verifyWrite(contract, CheckedWrite(goodPlan, List(orders -> inputSchema), goodOutput, false, None, Set.empty), VerificationOptions(), Some(sink), None, None, Some(job))
    sink.publish(JobSummaryEvent(0, 1, 0, 0, 1, 2L, Map.empty, job = Some(job)))
    sink.flush(5000)
    val published = events(out)
    assert(published.map(j => json.readTree(j).get("eventType").asText()) == List("START", "RUNNING", "COMPLETE"))
    published.foreach(j => assert(OpenLineageSchema.errors(j).isEmpty, OpenLineageSchema.errors(j)))
    assert(json.readTree(published.head).path("job").get("namespace").asText() == "platform")
  }

  test("it composes with notifyConfig: both sinks get every event, neither's failure reaching the other") {
    val out = Files.createTempDirectory("ol").resolve("ol.jsonl")
    val notified = Files.createTempDirectory("notify").resolve("events.jsonl")
    val notify = file("notify.properties", "sink.enabled=true", "sink.class=com.invaract.verification.notification.FileNotificationSink", s"sink.property.path=$notified")
    val config = file("openlineage.properties", "transport=file", s"path=$out")
    val sink = AttachSetup.strictSink(ConfigSource.fromMap(Map(InvaractConf.NotifyConfig -> notify.toString, InvaractConf.OpenLineageConfig -> config.toString))).get
    sink.publish(WriteEvent(None, "/x", None, None, Nil, 1L, Map.empty, job = Some(job.copy(runId = Some("compose")))))
    sink.flush(5000)
    assert(events(notified).size == 1 && events(out).size == 2)
  }

  test("the same settings give the same sink in a process (one run, one worker), different settings another") {
    val config = ConfigSource.fromMap(Map(InvaractConf.OpenLineageUrl -> "http://127.0.0.1:1/same"))
    assert(AttachSetup.openLineageSink(config).get eq AttachSetup.openLineageSink(config).get)
    assert(!(AttachSetup.openLineageSink(ConfigSource.fromMap(Map(InvaractConf.OpenLineageUrl -> "http://127.0.0.1:1/other"))).get eq AttachSetup.openLineageSink(config).get))
    assert(AttachSetup.openLineageSink(ConfigSource.empty).isEmpty)
  }

  test("enforcement refuses settings it cannot use, naming the setting; dry-run says so once and carries on without them") {
    val bad = ConfigSource.fromMap(Map(InvaractConf.Contract -> contractFile, InvaractConf.OpenLineageUrl -> "ftp://nope"))
    assert(intercept[IllegalArgumentException](AttachSetup.select(bad)).getMessage.contains("(openLineageUrl)"))
    val dryRun = AttachSetup.select(ConfigSource.fromMap(Map(InvaractConf.DryRun -> "true", InvaractConf.OpenLineageUrl -> "ftp://nope")))
    assert(dryRun == AttachPlan.DryRun(None))
  }

  test("without OpenLineage settings, nothing changes: the notifyConfig sink alone, or none") {
    assert(AttachSetup.strictSink(ConfigSource.empty).isEmpty)
    val notify = file("notify.properties", "sink.enabled=true", "sink.class=com.invaract.verification.notification.LoggingNotificationSink")
    assert(!AttachSetup.strictSink(ConfigSource.fromMap(Map(InvaractConf.NotifyConfig -> notify.toString))).get.isInstanceOf[TeeNotificationSink])
  }

  test("the sink can also be named as a notification sink class, its properties the configuration file's keys") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val sink = NotificationSinkFactory.create(NotificationConfig(enabled = true, Some(classOf[OpenLineageSink].getName), Map("transport" -> "file", "path" -> out.toString))).get
    sink.publish(WriteEvent(None, "/x", None, None, Nil, 1L, Map.empty, job = Some(job.copy(runId = Some("by-class")))))
    sink.flush(5000)
    assert(events(out).map(j => json.readTree(j).get("eventType").asText()) == List("START", "RUNNING"))
    intercept[IllegalArgumentException](new OpenLineageSink().configure(Map("transport" -> "nope")))
  }

  test("endRun ends the run it names, and only that run, once; the sink stays open for the next") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val config = file("openlineage.properties", "transport=file", s"path=$out")
    val sink = AttachSetup.strictSink(ConfigSource.fromMap(Map(InvaractConf.OpenLineageConfig -> config.toString))).get
    val (a, b) = (job.copy(runId = Some("run-a")), job.copy(runId = Some("run-b")))
    sink.endRun(Some(a)) // a run never started: nothing
    sink.publish(WriteEvent(None, "/x", None, None, Nil, 1L, Map.empty, job = Some(a)))
    sink.publish(WriteEvent(None, "/y", None, None, Nil, 1L, Map.empty, job = Some(b)))
    sink.endRun(Some(a))
    sink.endRun(Some(a)) // a second end of the same run publishes nothing more
    sink.publish(WriteEvent(None, "/z", None, None, Nil, 1L, Map.empty, job = Some(b)))
    sink.flush(5000)
    val seen = events(out).map(j => json.readTree(j)).map(n => n.get("eventType").asText() -> n.path("run").get("runId").asText())
    val (runA, runB) = (OpenLineageMapper.runUuid("spark", "run-a"), OpenLineageMapper.runUuid("spark", "run-b"))
    assert(seen == List("START" -> runA, "RUNNING" -> runA, "START" -> runB, "RUNNING" -> runB, "COMPLETE" -> runA, "RUNNING" -> runB), seen)
  }

  test("endRun after a refused write ends the run FAIL") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.File(out.toString)))
    val refused = job.copy(runId = Some("refused"))
    intercept[ContractViolationException](VerificationPipeline.verifyWrite(contract, CheckedWrite(goodPlan, List(orders -> inputSchema), Cols(("id", LongType, false)), false, None, Set.empty), VerificationOptions(), Some(sink), None, None, Some(refused)))
    sink.endRun(Some(refused))
    sink.flush(5000)
    assert(events(out).map(j => json.readTree(j).get("eventType").asText()) == List("START", "RUNNING", "FAIL"))
  }

  test("column lineage is published exactly when the adapter declares lineage.columnLevel supported or partial") {
    val toy = AdapterCapabilities.fromResource("capabilities-toy-fixture.yaml", getClass.getClassLoader).get.toOption.get
    def declaring(s: Support) = toy.copy(entries = toy.entries.updated(Capability.LineageColumnLevel, toy.entryOf(Capability.LineageColumnLevel).copy(support = s)))
    def hasColumns(caps: Option[AdapterCapabilities]): Boolean = {
      val sink = new TestNotificationSink
      VerificationPipeline.verifyWrite(contract, CheckedWrite(goodPlan, List(orders -> inputSchema), goodOutput, false, None, Set.empty), VerificationOptions(), Some(sink), None, caps, Some(job))
      sink.events.collect { case e: ContractValidationEvent => e }.head.lineage.get.columnLineage.nonEmpty
    }
    assert(hasColumns(None) && hasColumns(Some(declaring(Support.Supported))) && hasColumns(Some(declaring(Support.Partial))))
    assert(!hasColumns(Some(declaring(Support.Unsupported))) && !hasColumns(Some(declaring(Support.NotApplicable))))
  }

  test("a write refused before it runs (rejectWrite) carries its lineage, as a checked one does") {
    val sink = new TestNotificationSink
    val checked = CheckedWrite(goodPlan, List(orders -> inputSchema), Cols(("id", LongType, false)), false, None, Set.empty)
    val result = VerificationPipeline.evaluateWrite(contract, checked, VerificationOptions(), None)
    assert(!result.passed)
    val thrown = intercept[ContractViolationException](VerificationPipeline.rejectWrite(contract, result, checked, Some(sink), Some("r"), None, Some(job)))
    assert(thrown.result == result)
    val event = sink.events.collect { case e: ContractValidationEvent => e }.head
    assert(event.status == "FAILED" && event.runId.contains("r") && event.job.contains(job))
    assert(event.lineage.exists(l => l.output.location == "out/report" && l.inputs.map(_.location) == List(orders) && l.columnLineage.nonEmpty))
    intercept[ContractViolationException](VerificationPipeline.rejectWrite(contract, result, checked, None, None, None, None))
  }

  test("every wrapping sink passes endRun on; a failing one is contained") {
    final class Recording extends NotificationSink {
      val ended = new java.util.concurrent.ConcurrentLinkedQueue[Option[JobInfo]]()
      def publish(event: NotificationEvent): Unit = ()
      override def endRun(job: Option[JobInfo]): Unit = ended.add(job)
    }
    val failing = new NotificationSink {
      def publish(event: NotificationEvent): Unit = ()
      override def endRun(job: Option[JobInfo]): Unit = throw new IllegalStateException("boom")
    }
    val target = new Recording
    val wrapped: List[NotificationSink] = List(
      new SafeNotificationSink(target), new SummarizingNotificationSink(target), new FailureOnlyNotificationSink(target),
      new RetryingNotificationSink(target, failing), new TeeNotificationSink(List(failing, target)))
    wrapped.foreach(_.endRun(Some(job)))
    new SafeNotificationSink(failing).endRun(Some(job))
    assert(target.ended.size == wrapped.size && target.ended.asScala.forall(_.contains(job)))
    assert({ new LoggingNotificationSink().endRun(None); true }, "the default does nothing")
  }

  test("close ends the runs still open; nothing is published after") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.File(out.toString)))
    sink.publish(WriteEvent(None, "/x", None, None, Nil, 1L, Map.empty, job = Some(job.copy(runId = Some("closing")))))
    sink.close(5000)
    sink.publish(WriteEvent(None, "/y", None, None, Nil, 1L, Map.empty, job = Some(job.copy(runId = Some("after")))))
    sink.flush(1000)
    assert(events(out).map(j => json.readTree(j).get("eventType").asText()) == List("START", "RUNNING", "COMPLETE"))
    assert(sink.stats.exists(_.accepted == 1))
    assert(new OpenLineageSink().stats.isEmpty, "a sink never configured publishes nothing")
    new OpenLineageSink().publish(WriteEvent(None, "/z", None, None, Nil, 1L, Map.empty))
  }

  test("the shutdown hook closes runs a job left open: a real JVM, ended without a summary") {
    val out = Files.createTempDirectory("ol").resolve("events.jsonl")
    val java = Path.of(System.getProperty("java.home"), "bin", "java").toString
    val cp = System.getProperty("java.class.path")
    val process = new ProcessBuilder(java, "-cp", cp, classOf[OpenLineageShutdownMain].getName.stripSuffix("$"), out.toString).redirectErrorStream(true).start()
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    assert(process.waitFor() == 0, output)
    assert(events(out).map(j => json.readTree(j).get("eventType").asText()) == List("START", "RUNNING", "COMPLETE"), output)
  }
}

/** A job that publishes one write and ends without a summary: its run is closed by the shutdown hook. */
object OpenLineageShutdownMain {
  def main(args: Array[String]): Unit = {
    val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.File(args(0))))
    sink.publish(WriteEvent(None, "/x", None, None, Nil, 1L, Map.empty, job = Some(JobInfo(runId = Some("hooked"), engine = Some("test")))))
  }
}

final class OpenLineageShutdownMain
