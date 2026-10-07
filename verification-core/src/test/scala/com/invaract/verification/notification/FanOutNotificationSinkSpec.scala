// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

/** A target `FanOutNotificationSink` can build reflectively (public no-arg constructor): it
  * registers itself under its `name` property so a test can find the instance the factory made.
  */
class FanOutRecordingTarget extends NotificationSink {
  val events: mutable.ListBuffer[NotificationEvent] = mutable.ListBuffer.empty[NotificationEvent]
  val flushes: mutable.ListBuffer[Long] = mutable.ListBuffer.empty[Long]
  var properties: Map[String, String] = Map.empty

  override def configure(props: Map[String, String]): Unit = {
    properties = props
    FanOutRecordingTarget.byName(props("name")) = this
  }
  override def publish(event: NotificationEvent): Unit = events += event
  override def flush(timeoutMs: Long): Unit = flushes += timeoutMs
}

object FanOutRecordingTarget {
  val byName: mutable.Map[String, FanOutRecordingTarget] = mutable.Map.empty[String, FanOutRecordingTarget]
}

/** Always throws - from `publish` and from `flush`. */
class FanOutBrokenTarget extends NotificationSink {
  override def publish(event: NotificationEvent): Unit = throw new IllegalStateException("down")
  override def flush(timeoutMs: Long): Unit = throw new IllegalStateException("flush down")
}

class FanOutNotificationSinkSpec extends AnyFunSuite {
  private val recording = classOf[FanOutRecordingTarget].getName
  private val broken = classOf[FanOutBrokenTarget].getName

  private def inference(status: String): ContractInferenceEvent =
    ContractInferenceEvent(status, None, None, None, None, Nil, Nil, None, JobInfo(), 0L, Map.empty)
  private val validationPassed = ContractValidationEvent("c@1.0.0", "PASSED", Nil, 0L, Map.empty)
  private val validationFailed = validationPassed.copy(status = "FAILED")
  private val summary = DryRunSummaryEvent(Map.empty, "NO_DRAFTS", None, Nil, JobInfo(), 0L, Map.empty)
  private val write = WriteEvent(None, "x", None, None, Nil, 0L, Map.empty)

  private def configured(props: (String, String)*): FanOutNotificationSink = {
    val sink = new FanOutNotificationSink
    sink.configure(props.toMap)
    sink
  }

  test("every target receives every event, in the order configured, with its own sink.property.* only") {
    val sink = configured(
      "sinks" -> "first, second",
      "first.class" -> recording,
      "first.property.name" -> "t1-first",
      "first.property.url" -> "http://a",
      "second.class" -> recording,
      "second.property.name" -> "t1-second"
    )
    sink.publish(validationPassed)
    sink.publish(write)

    val first = FanOutRecordingTarget.byName("t1-first")
    val second = FanOutRecordingTarget.byName("t1-second")
    assert(first.events.toList == List(validationPassed, write))
    assert(second.events.toList == List(validationPassed, write))
    assert(first.properties == Map("name" -> "t1-first", "url" -> "http://a"))
    assert(second.properties == Map("name" -> "t1-second"), "a target must not see another target's properties")
  }

  test("statuses routes inference and validation events by status; every other event always passes") {
    val sink = configured(
      "sinks" -> "good,bad",
      "good.class" -> recording,
      "good.property.name" -> "t2-good",
      "good.statuses" -> "INFERRED, INFERRED_DEGRADED, PASSED",
      "bad.class" -> recording,
      "bad.property.name" -> "t2-bad",
      "bad.statuses" -> "SKIPPED_UNSUPPORTED,FAILED"
    )
    val events = List(
      inference("INFERRED"),
      inference("INFERRED_DEGRADED"),
      inference("SKIPPED_UNSUPPORTED"),
      inference("INFERENCE_ERROR"),
      validationPassed,
      validationFailed,
      summary,
      write
    )
    events.foreach(sink.publish)

    assert(FanOutRecordingTarget.byName("t2-good").events.toList ==
      List(inference("INFERRED"), inference("INFERRED_DEGRADED"), validationPassed, summary, write))
    assert(FanOutRecordingTarget.byName("t2-bad").events.toList ==
      List(inference("SKIPPED_UNSUPPORTED"), validationFailed, summary, write))
  }

  test("a blank statuses list means no filter") {
    val sink = configured("sinks" -> "all", "all.class" -> recording, "all.property.name" -> "t3", "all.statuses" -> " , ")
    sink.publish(inference("INFERENCE_ERROR"))
    assert(FanOutRecordingTarget.byName("t3").events.size == 1)
  }

  test("a target that throws is isolated: the others still receive the event, and flush still reaches them") {
    val sink = configured(
      "sinks" -> "down,up",
      "down.class" -> broken,
      "up.class" -> recording,
      "up.property.name" -> "t4-up"
    )
    sink.publish(summary) // must not throw
    sink.flush(777L) // must not throw

    val up = FanOutRecordingTarget.byName("t4-up")
    assert(up.events.toList == List(summary))
    assert(up.flushes.toList == List(777L))
  }

  test("flush reaches every target with the same timeout") {
    val sink = configured(
      "sinks" -> "a,b",
      "a.class" -> recording,
      "a.property.name" -> "t5-a",
      "b.class" -> recording,
      "b.property.name" -> "t5-b"
    )
    sink.flush(1234L)
    assert(FanOutRecordingTarget.byName("t5-a").flushes.toList == List(1234L))
    assert(FanOutRecordingTarget.byName("t5-b").flushes.toList == List(1234L))
  }

  test("configuration errors fail at setup with a message naming the problem") {
    val noSinks = intercept[IllegalArgumentException](configured())
    assert(noSinks.getMessage.contains("sinks"))
    intercept[IllegalArgumentException](configured("sinks" -> " , "))

    val noClass = intercept[IllegalArgumentException](configured("sinks" -> "a"))
    assert(noClass.getMessage.contains("'a'") && noClass.getMessage.contains("a.class"))

    val duplicate = intercept[IllegalArgumentException] {
      configured("sinks" -> "a,a", "a.class" -> recording, "a.property.name" -> "t6")
    }
    assert(duplicate.getMessage.contains("'a'"))

    intercept[IllegalArgumentException](configured("sinks" -> "a", "a.class" -> "com.example.DoesNotExist"))
  }

  test("a fan-out built by the factory from a properties-file-shaped config works end to end") {
    val config = NotificationConfig(
      enabled = true,
      sinkClassName = Some(classOf[FanOutNotificationSink].getName),
      properties = Map("sinks" -> "only", "only.class" -> recording, "only.property.name" -> "t7")
    )
    val sink = NotificationSinkFactory.create(config).get
    sink.publish(summary)
    sink.flush(5L)
    assert(FanOutRecordingTarget.byName("t7").events.toList == List(summary))
    assert(FanOutRecordingTarget.byName("t7").flushes.toList == List(5L))
  }
}
