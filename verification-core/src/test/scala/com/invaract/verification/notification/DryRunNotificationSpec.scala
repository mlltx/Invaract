// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import com.invaract.verification.Violation

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

/** The dry-run event types, and the wrapper sinks' handling of them and of `flush`. */
class DryRunNotificationSpec extends AnyFunSuite {

  private val job = JobInfo(
    applicationId = Some("app-1"),
    appName = Some("orders"),
    jobId = Some("nightly"),
    sparkVersion = Some("3.5.1"),
    master = Some("local[*]"),
    deployMode = Some("client"),
    user = Some("svc"),
    startTimeMs = Some(1700L),
    attributes = Map("team" -> "data-eng")
  )

  private def inference(status: String): ContractInferenceEvent =
    ContractInferenceEvent(status, None, None, None, None, Nil, Nil, None, job, 1L, Map.empty)
  private val summary = DryRunSummaryEvent(Map("INFERRED" -> 2L), "MERGED", Some("id: x"), Nil, job, 2L, Map.empty)

  private class FlushCounting extends TestNotificationSink {
    val flushes = mutable.ListBuffer.empty[Long]
    override def flush(timeoutMs: Long): Unit = synchronized(flushes += timeoutMs)
  }

  test("JobInfo.toMap carries every present field and omits absent ones; attributes is always there") {
    assert(
      job.toMap == Map(
        "applicationId" -> "app-1",
        "appName" -> "orders",
        "jobId" -> "nightly",
        "sparkVersion" -> "3.5.1",
        "master" -> "local[*]",
        "deployMode" -> "client",
        "user" -> "svc",
        "startTimeMs" -> 1700L,
        "attributes" -> Map("team" -> "data-eng")
      )
    )
    assert(JobInfo().toMap == Map("attributes" -> Map.empty))
  }

  test("InferenceStatus names are the stable strings consumers will match on") {
    assert(InferenceStatus.Inferred == "INFERRED")
    assert(InferenceStatus.InferredDegraded == "INFERRED_DEGRADED")
    assert(InferenceStatus.SkippedUnsupported == "SKIPPED_UNSUPPORTED")
    assert(InferenceStatus.SkippedUnrecognized == "SKIPPED_UNRECOGNIZED")
    assert(InferenceStatus.InferenceError == "INFERENCE_ERROR")
    assert(InferenceStatus.NoWritesObserved == "NO_WRITES_OBSERVED")
  }

  test("toJson renders a ContractInferenceEvent with every field") {
    val violation = Violation("OUTPUT_LOCATION_MISMATCH", "wrong place", "fix it", location = Some("/x"))
    val event = ContractInferenceEvent(
      status = "INFERRED_DEGRADED",
      reason = Some("why"),
      writeLocation = Some("/out"),
      contractYaml = Some("id: x\nversion: 0.1.0"),
      selfCheck = Some("FAILED"),
      selfCheckViolations = List(violation),
      diagnostics = List("d1", "d2"),
      fingerprints = None,
      job = job,
      timestamp = 99L,
      metadata = Map("team" -> "data-eng")
    )
    val json = NotificationJson.toJson(event)
    assert(json.contains("\"eventType\": \"CONTRACT_INFERENCE\""))
    assert(json.contains("\"timestamp\": 99"))
    assert(json.contains("\"status\": \"INFERRED_DEGRADED\""))
    assert(json.contains("\"reason\": \"why\""))
    assert(json.contains("\"writeLocation\": \"/out\""))
    assert(json.contains("\"contractYaml\": \"id: x\\nversion: 0.1.0\""), "multi-line YAML must be escaped, not break the line")
    assert(json.contains("\"selfCheck\": \"FAILED\""))
    assert(json.contains("\"selfCheckViolations\": [{"))
    assert(json.contains("OUTPUT_LOCATION_MISMATCH"))
    assert(json.contains("\"diagnostics\": [\"d1\", \"d2\"]"))
    assert(json.contains("\"fingerprints\": null"))
    assert(json.contains("\"job\": {"))
    assert(json.contains("\"jobId\": \"nightly\""))
    assert(json.contains("\"metadata\": {\"team\": \"data-eng\"}"))
    assert(!json.contains("\n"), "one event is one line, so a line-oriented sink stays valid")
  }

  test("toJson renders absent optionals of a ContractInferenceEvent as null") {
    val json = NotificationJson.toJson(inference("SKIPPED_UNSUPPORTED").copy(job = JobInfo()))
    assert(json.contains("\"reason\": null"))
    assert(json.contains("\"writeLocation\": null"))
    assert(json.contains("\"contractYaml\": null"))
    assert(json.contains("\"selfCheck\": null"))
    assert(json.contains("\"selfCheckViolations\": []"))
  }

  test("toJson renders a DryRunSummaryEvent with every field") {
    val json = NotificationJson.toJson(summary.copy(mergeConflicts = List("clash")))
    assert(json.contains("\"eventType\": \"DRY_RUN_SUMMARY\""))
    assert(json.contains("\"timestamp\": 2"))
    assert(json.contains("\"statusCounts\": {\"INFERRED\": 2}"))
    assert(json.contains("\"mergeStatus\": \"MERGED\""))
    assert(json.contains("\"mergedContractYaml\": \"id: x\""))
    assert(json.contains("\"mergeConflicts\": [\"clash\"]"))
    assert(json.contains("\"job\": {"))
    assert(json.contains("\"metadata\": {}"))
  }

  test("SummarizingNotificationSink forwards dry-run events but does not tally them as writes or checks") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)
    sink.publish(inference("INFERRED"))
    sink.publish(summary)
    assert(delegate.events == List(inference("INFERRED"), summary))

    sink.publishSummary()
    val published = delegate.events.last.asInstanceOf[JobSummaryEvent]
    assert(published.totalWrites == 0L && published.checksPassed == 0L && published.checksFailed == 0L)
  }

  test("FailureOnlyNotificationSink forwards every non-INFERRED inference status and drops only a clean INFERRED") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    sink.publish(inference("INFERRED"))
    assert(delegate.events.isEmpty)
    List("INFERRED_DEGRADED", "SKIPPED_UNSUPPORTED", "SKIPPED_UNRECOGNIZED", "INFERENCE_ERROR").foreach { status =>
      sink.publish(inference(status))
    }
    assert(delegate.events.size == 4)
  }

  test("FailureOnlyNotificationSink always forwards the end-of-job dry-run summary") {
    val delegate = new TestNotificationSink
    new FailureOnlyNotificationSink(delegate).publish(summary)
    assert(delegate.events == List(summary))
  }

  test("every wrapper sink passes flush through to what it wraps") {
    val a = new FlushCounting
    new SafeNotificationSinkForTest(a).flush(11L)
    assert(a.flushes.toList == List(11L))

    val b = new FlushCounting
    new SummarizingNotificationSink(b).flush(12L)
    assert(b.flushes.toList == List(12L))

    val c = new FlushCounting
    new FailureOnlyNotificationSink(c).flush(13L)
    assert(c.flushes.toList == List(13L))

    val delegate = new FlushCounting
    val deadLetter = new FlushCounting
    new RetryingNotificationSink(delegate, deadLetter).flush(14L)
    assert(delegate.flushes.toList == List(14L))
    assert(deadLetter.flushes.toList == List(14L), "the dead-letter sink may be asynchronous too")
  }

  test("flush defaults to a no-op, and SafeNotificationSink contains a flush that throws") {
    new LoggingNotificationSink().flush(1L) // default: nothing to do

    val throwing = new NotificationSink {
      override def publish(event: NotificationEvent): Unit = ()
      override def flush(timeoutMs: Long): Unit = throw new RuntimeException("nope")
    }
    new SafeNotificationSinkForTest(throwing).flush(1L) // must not throw
  }

  /** `SafeNotificationSink` is `private[invaract]`, reachable from here (same package tree). */
  private class SafeNotificationSinkForTest(delegate: NotificationSink)
      extends com.invaract.verification.notification.SafeNotificationSink(delegate)
}
