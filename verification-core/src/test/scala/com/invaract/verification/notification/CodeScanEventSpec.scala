// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** A code scan's event: what it renders, what its findings add up to, and how the built-in sink wrappers treat it. */
class CodeScanEventSpec extends AnyFunSuite {
  private val mapper = new ObjectMapper()

  private def finding(category: String, allowed: Boolean, id: String = "f"): CodeFinding =
    EventSchemaSpec.finding.copy(id = id, category = category, allowed = allowed, reason = if (allowed) Some("kept") else None)

  private def scan(status: String): CodeScanEvent =
    CodeScanEvent(status, "START", "REPORT", refused = false, "invaract-lint 0.1.0", List("job.py"), Map.empty, Nil, 0L, Nil, 1L, 2L, None, Map.empty)

  test("toJson renders every field of a code scan, findings and errors as their own objects") {
    val event = CodeScanEvent(
      "FINDINGS", "FINISH", "ENFORCE", refused = true, "invaract-lint 0.1.0", List("jobs/a.py", "jobs/b.py"),
      Map("read" -> Map("findings" -> 1L, "exceptions" -> 0L)), List(EventSchemaSpec.finding), 4L,
      List(CodeScanError("jobs/c.py", "invalid syntax")), 15L, 99L, Some("orders@1.0.0"), Map("owner" -> "team-a"), Some("run-1"),
      Some(JobInfo(runId = Some("run-1"), engine = Some("pythonio")))
    )
    val json = mapper.readTree(NotificationJson.toJson(event))
    assert(json.get("eventType").asText == "CODE_SCAN" && json.get("timestamp").asLong == 99L)
    assert(json.get("status").asText == "FINDINGS" && json.get("phase").asText == "FINISH" && json.get("mode").asText == "ENFORCE")
    assert(json.get("refused").asBoolean && json.get("scanner").asText == "invaract-lint 0.1.0")
    assert(json.get("files").elements().asScala.map(_.asText).toList == List("jobs/a.py", "jobs/b.py"))
    assert(json.get("counts").get("read").get("findings").asLong == 1L && json.get("counts").get("read").get("exceptions").asLong == 0L)
    assert(json.get("findingsOmitted").asLong == 4L && json.get("durationMs").asLong == 15L)
    assert(json.get("errors").get(0).get("path").asText == "jobs/c.py" && json.get("errors").get(0).get("message").asText == "invalid syntax")
    assert(json.get("contract").asText == "orders@1.0.0" && json.get("metadata").get("owner").asText == "team-a")
    assert(json.get("runId").asText == "run-1" && json.get("job").get("engine").asText == "pythonio")
    val f = json.get("findings").get(0)
    assert(f.fieldNames().asScala.toList.sorted == List(
      "allowed", "call", "category", "code", "column", "detail", "endColumn", "endLine", "id", "line", "message", "path", "reason", "resolved",
      "rule", "scope", "snippet", "target"
    ))
    assert(f.get("id").asText == "0123456789abcdef" && f.get("code").asText == "IVR001" && f.get("category").asText == "read")
    assert(f.get("rule").asText == "pandas-read" && f.get("path").asText == "jobs/load.py")
    assert((f.get("line").asLong, f.get("column").asLong, f.get("endLine").asLong, f.get("endColumn").asLong) == ((4L, 14L, 4L, 37L)))
    assert(f.get("call").asText == "pd.read_csv" && f.get("resolved").asText == "pandas.read_csv" && f.get("scope").asText == "load")
    assert(f.get("snippet").asText == "pd.read_csv('in.csv')" && f.get("target").asText == "in.csv" && f.get("detail").asText == "mode 'w'")
    assert(!f.get("allowed").asBoolean && f.get("reason").isNull && f.get("message").asText.startsWith("`pd.read_csv`"))
    EventSchema.assertValid(event)
  }

  test("a finding's location is its file, line and column") {
    assert(EventSchemaSpec.finding.location == "jobs/load.py:4:14")
  }

  test("the status: FINDINGS for a call not explained; NOT_SCANNED unless every file was read; else EXCEPTIONS, else CLEAN") {
    val found = finding("read", allowed = false)
    val kept = finding("read", allowed = true)
    assert(CodeScan.status(1, 0, List(found, kept)) == CodeScanStatus.Findings)
    // a call not explained is FINDINGS whatever else is true: a file left unread does not hide it
    assert(CodeScan.status(1, 1, List(found)) == CodeScanStatus.Findings)
    assert(CodeScan.status(0, 0, List(finding("write", allowed = false))) == CodeScanStatus.Findings)
    assert(CodeScan.status(1, 0, List(kept)) == CodeScanStatus.Exceptions)
    assert(CodeScan.status(1, 0, Nil) == CodeScanStatus.Clean)
    // nothing more is claimed for a scan that could not read every file, or read none
    assert(CodeScan.status(1, 1, List(kept)) == CodeScanStatus.NotScanned)
    assert(CodeScan.status(1, 1, Nil) == CodeScanStatus.NotScanned)
    assert(CodeScan.status(0, 0, Nil) == CodeScanStatus.NotScanned)
    assert((CodeScanStatus.Clean, CodeScanStatus.Exceptions, CodeScanStatus.Findings, CodeScanStatus.NotScanned) ==
      (("CLEAN", "EXCEPTIONS", "FINDINGS", "NOT_SCANNED")))
  }

  test("counts lists every category, each split into findings and exceptions") {
    val found = List(finding("read", allowed = false), finding("read", allowed = false), finding("read", allowed = true), finding("sql", allowed = true))
    assert(CodeScan.counts(found) == Map(
      "read" -> Map("findings" -> 2L, "exceptions" -> 1L),
      "write" -> Map("findings" -> 0L, "exceptions" -> 0L),
      "connect" -> Map("findings" -> 0L, "exceptions" -> 0L),
      "sql" -> Map("findings" -> 0L, "exceptions" -> 1L)
    ))
    assert(CodeScan.Categories == List("read", "write", "connect", "sql"))
  }

  test("an event lists the calls not explained first, and at most MaxFindings") {
    val kept = finding("read", allowed = true, "kept")
    val found = finding("write", allowed = false, "found")
    assert(CodeScan.listed(List(kept, found)).map(_.id) == List("found", "kept"))
    assert(CodeScan.MaxFindings == 1000)
    val many = (1 to 1001).toList.map(i => finding("write", allowed = false, s"f$i"))
    assert(CodeScan.listed(many).size == 1000 && CodeScan.listed(many).last.id == "f1000")
    assert(CodeScan.listed((1 to 1000).toList.map(i => finding("write", allowed = false, s"f$i"))).size == 1000)
  }

  test("a summarizing sink forwards a code scan and does not count it as a check or a write") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)
    sink.publish(scan("FINDINGS"))
    sink.publishSummary()
    val summary = delegate.events.collect { case e: JobSummaryEvent => e }.head
    assert(delegate.events.head == scan("FINDINGS"))
    assert((summary.totalWrites, summary.checksPassed, summary.checksFailed, summary.totalViolations) == ((0L, 0L, 0L, 0L)))
  }

  test("a failure-only sink forwards a code scan that found something, and drops a clean one") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    List("CLEAN", "EXCEPTIONS", "FINDINGS", "NOT_SCANNED").foreach(s => sink.publish(scan(s)))
    assert(delegate.events.collect { case e: CodeScanEvent => e.status } == List("EXCEPTIONS", "FINDINGS", "NOT_SCANNED"))
  }

  test("a fan-out target's statuses filter applies to a code scan's status") {
    val target = FanOutNotificationSink.Target("t", new TestNotificationSink, Some(Set("FINDINGS")))
    assert(target.accepts(scan("FINDINGS")) && !target.accepts(scan("CLEAN")))
    assert(FanOutNotificationSink.Target("u", new TestNotificationSink, None).accepts(scan("CLEAN")))
  }
}
