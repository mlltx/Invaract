// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.io.Source

class NotificationSinkSpec extends AnyFunSuite {

  private val sampleEvent = ContractValidationEvent(
    contract = "demo@1.0.0",
    status = "PASSED",
    violations = Nil,
    timestamp = 0L,
    metadata = Map.empty
  )

  private def readLines(path: Path): List[String] = {
    val source = Source.fromFile(path.toFile)
    try source.getLines().toList
    finally source.close()
  }

  test("FileNotificationSink.configure throws without a 'path' property") {
    val sink = new FileNotificationSink
    assertThrows[IllegalArgumentException] {
      sink.configure(Map.empty)
    }
  }

  test("FileNotificationSink.publish appends, one JSON line per call, rather than overwriting the file each time") {
    val file = Files.createTempFile("invaract-file-sink-test", ".jsonl")
    Files.delete(file) // FileWriter(path, append = true) must create it fresh too
    try {
      val sink = new FileNotificationSink
      sink.configure(Map("path" -> file.toString))

      sink.publish(sampleEvent.copy(contract = "first@1.0.0"))
      sink.publish(sampleEvent.copy(contract = "second@1.0.0"))

      val lines = readLines(file)
      assert(lines.size == 2, s"expected both publishes to append as separate lines, got: $lines")
      assert(lines(0).contains("\"first@1.0.0\""))
      assert(lines(1).contains("\"second@1.0.0\""), "the second publish must not have overwritten the first")
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("LoggingNotificationSink.publish does not throw") {
    val sink = new LoggingNotificationSink
    sink.publish(sampleEvent) // no assertion beyond "doesn't throw" - output goes to SLF4J, not captured here
  }

  private val sampleWrite = WriteEvent(
    contract = Some("demo@1.0.0"),
    location = "file:/tmp/out.parquet",
    format = Some("parquet"),
    saveMode = Some("append"),
    schema = Nil,
    timestamp = 0L,
    metadata = Map.empty
  )

  test("SummarizingNotificationSink forwards every event to its delegate unchanged") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)
    sink.publish(sampleWrite)
    sink.publish(sampleEvent)
    assert(delegate.events == List(sampleWrite, sampleEvent), "the wrapper must not swallow or alter the original events")
  }

  test("SummarizingNotificationSink.publishSummary tallies writes and both PASSED/FAILED checks independently") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)

    sink.publish(sampleWrite)
    sink.publish(sampleWrite)
    sink.publish(sampleEvent.copy(status = "PASSED"))
    sink.publish(sampleEvent.copy(status = "FAILED", violations = List(sampleViolation, sampleViolation)))
    sink.publish(sampleEvent.copy(status = "FAILED", violations = List(sampleViolation)))

    sink.publishSummary()

    val summary = delegate.events.collectFirst { case e: JobSummaryEvent => e }.getOrElse(fail("no JobSummaryEvent published"))
    assert(summary.totalWrites == 2L, s"expected 2 writes, got ${summary.totalWrites}")
    assert(summary.checksPassed == 1L, s"expected 1 passed check, got ${summary.checksPassed}")
    assert(summary.checksFailed == 2L, s"expected 2 failed checks, got ${summary.checksFailed}")
    assert(summary.totalViolations == 3L, s"expected 3 total violations (2 + 1), got ${summary.totalViolations}")
  }

  test("SummarizingNotificationSink.publishSummary with no events published yet produces an all-zero summary, not an exception") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)
    sink.publishSummary()
    val summary = delegate.events.collectFirst { case e: JobSummaryEvent => e }.getOrElse(fail("no JobSummaryEvent published"))
    assert(summary.totalWrites == 0L)
    assert(summary.checksPassed == 0L)
    assert(summary.checksFailed == 0L)
    assert(summary.totalViolations == 0L)
  }

  test("SummarizingNotificationSink resets its counters after publishSummary - a second summary reflects only what's new") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)

    sink.publish(sampleWrite)
    sink.publish(sampleEvent.copy(status = "FAILED", violations = List(sampleViolation)))
    sink.publishSummary()

    sink.publish(sampleWrite)
    sink.publishSummary()

    val summaries = delegate.events.collect { case e: JobSummaryEvent => e }
    assert(summaries.size == 2)
    assert(summaries(0).totalWrites == 1L && summaries(0).checksFailed == 1L)
    assert(
      summaries(1).totalWrites == 1L && summaries(1).checksFailed == 0L,
      s"expected the second summary to reflect only the one new write, not a cumulative total, got ${summaries(1)}"
    )
  }

  test("SummarizingNotificationSink.configure delegates to the wrapped sink") {
    val delegate = new TestNotificationSink
    val sink = new SummarizingNotificationSink(delegate)
    // TestNotificationSink.configure is the inherited no-op default - this
    // just confirms the call is forwarded, not swallowed, by observing it
    // doesn't throw even with arbitrary properties a stricter delegate
    // might reject.
    sink.configure(Map("anything" -> "goes"))
  }

  test("FailureOnlyNotificationSink drops a PASSED ContractValidationEvent") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    sink.publish(sampleEvent.copy(status = "PASSED"))
    assert(delegate.events.isEmpty, "a PASSED check must never reach the delegate")
  }

  test("FailureOnlyNotificationSink forwards a FAILED ContractValidationEvent") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    val failed = sampleEvent.copy(status = "FAILED")
    sink.publish(failed)
    assert(delegate.events == List(failed))
  }

  test("FailureOnlyNotificationSink always drops WriteEvent, regardless of contents") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    sink.publish(sampleWrite)
    assert(delegate.events.isEmpty, "a WriteEvent only exists because the write already succeeded - never forwarded")
  }

  test("FailureOnlyNotificationSink always forwards a JobSummaryEvent") {
    val delegate = new TestNotificationSink
    val sink = new FailureOnlyNotificationSink(delegate)
    val summary = JobSummaryEvent(0L, 0L, 0L, 0L, 0L, 0L, Map.empty)
    sink.publish(summary)
    assert(delegate.events == List(summary), "a single once-per-job summary is never noise, whatever it says")
  }

  private val sampleViolation = com.invaract.sparkadapter.Violation(
    violationType = "MISSING_OUTPUT_FIELD",
    message = "missing 'x'",
    remediation = "add 'x'",
    column = Some("x")
  )

  /** A delegate that fails the first `failuresBeforeSuccess` calls, then
    * succeeds on every call after that - the shape `RetryingNotificationSink`
    * is meant to recover from. `failuresBeforeSuccess = Int.MaxValue`
    * (effectively) models a sink that's simply down for the whole test.
    */
  private class FlakySink(failuresBeforeSuccess: Int) extends NotificationSink {
    var attempts: Int = 0
    var delivered: List[NotificationEvent] = Nil

    override def publish(event: NotificationEvent): Unit = {
      attempts += 1
      if (attempts <= failuresBeforeSuccess) throw new RuntimeException(s"simulated failure #$attempts")
      delivered = delivered :+ event
    }
  }

  /** `RetryingNotificationSink` with `sleep` overridden to record the
    * requested duration instead of actually blocking the test thread -
    * this both keeps the suite fast and makes the exact backoff sequence
    * directly assertable, which a real `Thread.sleep` call never would be.
    */
  private class RecordingRetryingSink(
    delegate: NotificationSink,
    deadLetterSink: NotificationSink,
    maxAttempts: Int = 3,
    initialBackoffMs: Long = 10L,
    backoffMultiplier: Double = 2.0
  ) extends RetryingNotificationSink(delegate, deadLetterSink, maxAttempts, initialBackoffMs, backoffMultiplier) {
    var sleeps: List[Long] = Nil
    override protected def sleep(ms: Long): Unit = sleeps = sleeps :+ ms
  }

  test("RetryingNotificationSink succeeding on the first attempt never retries, sleeps, or dead-letters") {
    val delegate = new FlakySink(failuresBeforeSuccess = 0)
    val deadLetter = new TestNotificationSink
    val sink = new RecordingRetryingSink(delegate, deadLetter)

    sink.publish(sampleWrite)

    assert(delegate.attempts == 1, s"expected exactly one delegate call, got ${delegate.attempts}")
    assert(delegate.delivered == List(sampleWrite))
    assert(sink.sleeps.isEmpty, "a first-try success must never sleep between attempts")
    assert(deadLetter.events.isEmpty, "a delivered event must never reach the dead-letter sink")
  }

  test("RetryingNotificationSink succeeding on a later attempt (within maxAttempts) does not dead-letter") {
    val delegate = new FlakySink(failuresBeforeSuccess = 2)
    val deadLetter = new TestNotificationSink
    val sink = new RecordingRetryingSink(delegate, deadLetter, maxAttempts = 3)

    sink.publish(sampleWrite)

    assert(delegate.attempts == 3, s"expected 2 failures then a successful 3rd attempt, got ${delegate.attempts} attempts")
    assert(delegate.delivered == List(sampleWrite))
    assert(sink.sleeps.size == 2, s"expected a sleep between each of the 2 failed attempts and the next, got ${sink.sleeps}")
    assert(deadLetter.events.isEmpty, "an eventually-delivered event must never reach the dead-letter sink")
  }

  test("RetryingNotificationSink exhausting every attempt publishes the original event to the dead-letter sink") {
    val delegate = new FlakySink(failuresBeforeSuccess = Int.MaxValue)
    val deadLetter = new TestNotificationSink
    val sink = new RecordingRetryingSink(delegate, deadLetter, maxAttempts = 3)

    sink.publish(sampleWrite)

    assert(delegate.attempts == 3, s"expected exactly maxAttempts (3) delegate calls, got ${delegate.attempts}")
    assert(delegate.delivered.isEmpty, "a delegate that always fails must never be recorded as having delivered anything")
    assert(deadLetter.events == List(sampleWrite), "the dead-letter sink must receive the exact original event")
  }

  test("RetryingNotificationSink with maxAttempts = 1 tries exactly once, never sleeps, and dead-letters on that single failure") {
    val delegate = new FlakySink(failuresBeforeSuccess = Int.MaxValue)
    val deadLetter = new TestNotificationSink
    val sink = new RecordingRetryingSink(delegate, deadLetter, maxAttempts = 1)

    sink.publish(sampleWrite)

    assert(delegate.attempts == 1, s"expected exactly one attempt, got ${delegate.attempts}")
    assert(sink.sleeps.isEmpty, "there is no next attempt to back off before with maxAttempts = 1")
    assert(deadLetter.events == List(sampleWrite))
  }

  test("RetryingNotificationSink backs off with the configured initial delay and multiplier, not a fixed or additive one") {
    val delegate = new FlakySink(failuresBeforeSuccess = Int.MaxValue)
    val deadLetter = new TestNotificationSink
    val sink = new RecordingRetryingSink(delegate, deadLetter, maxAttempts = 4, initialBackoffMs = 10L, backoffMultiplier = 3.0)

    sink.publish(sampleWrite)

    assert(sink.sleeps == List(10L, 30L, 90L), s"expected a 10 -> 30 -> 90 backoff sequence (x3 each time), got ${sink.sleeps}")
  }

  /** Records whether/with-what `configure` was called - `TestNotificationSink`
    * can't observe that (its `configure` is the inherited no-op), so this
    * exists purely to make "delegates to the wrapped sink, not the
    * dead-letter sink" a real, mutation-resistant assertion rather than
    * just "doesn't throw."
    */
  private class ConfigureRecordingSink extends NotificationSink {
    var received: Option[Map[String, String]] = None
    override def configure(properties: Map[String, String]): Unit = received = Some(properties)
    override def publish(event: NotificationEvent): Unit = ()
  }

  test("RetryingNotificationSink.configure delegates to the wrapped sink, not the dead-letter sink") {
    val delegate = new ConfigureRecordingSink
    val deadLetter = new ConfigureRecordingSink
    val sink = new RetryingNotificationSink(delegate, deadLetter)

    sink.configure(Map("anything" -> "goes"))

    assert(delegate.received.contains(Map("anything" -> "goes")), "configure must reach the wrapped (delegate) sink")
    assert(deadLetter.received.isEmpty, "configure must not also reach the dead-letter sink - it is configured separately, on its own")
  }
}
