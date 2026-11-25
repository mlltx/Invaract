// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.fasterxml.jackson.databind.ObjectMapper
import com.invaract.verification.notification._

import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.JavaConverters._

/** The delivery off the job's thread (docs/OPENLINEAGE.md, sections 5 and 6), with a scripted transport and clock. */
class OpenLineageDeliverySpec extends AnyFunSuite with org.scalatest.OneInstancePerTest {
  import OpenLineageMapperSpec.job

  private val json = new ObjectMapper()

  /** A transport that answers from `script` in turn (then `Delivered`), recording what it was sent. */
  private class Scripted(script: Outcome*) extends Transport {
    private val answers = new ConcurrentLinkedQueue[Outcome](script.asJava)
    val sent = new ConcurrentLinkedQueue[String]()
    val describe = "scripted"
    def send(json: String): Outcome = { sent.add(json); Option(answers.poll()).getOrElse(Outcome.Delivered) }
    def types: List[String] = sent.asScala.toList.map(j => json.readTree(j).get("eventType").asText())
  }

  private class Recorder extends Transport {
    val sent = new ConcurrentLinkedQueue[String]()
    val describe = "dead letter"
    def send(json: String): Outcome = { sent.add(json); Outcome.Delivered }
  }

  private val clock = new AtomicLong(0L)
  private val sleeps = new ConcurrentLinkedQueue[Long]()

  private def delivery(transport: Transport, s: OpenLineageSettings = OpenLineageSettings(TransportSettings.Console), deadLetter: Option[Transport] = None) =
    new OpenLineageDelivery(s, transport, new OpenLineageMapper(s, () => Instant.EPOCH, "/w"), deadLetter, ms => sleeps.add(ms), () => clock.get())

  private def write(n: Int = 0): WriteEvent =
    WriteEvent(None, s"/out/$n", Some("parquet"), Some("append"), List(WriteFieldInfo("id", "bigint", nullable = false)), 1L, Map.empty, job = Some(job))

  private def summary = JobSummaryEvent(1, 0, 0, 0, 1, 2L, Map.empty, job = Some(job))

  test("events are delivered in order off the caller's thread; flush waits for them") {
    val t = new Scripted
    val d = delivery(t)
    assert(d.submit(write(1)) && d.submit(write(2)) && d.submit(summary))
    assert(d.flush(5000))
    assert(t.types == List("START", "RUNNING", "RUNNING", "COMPLETE"))
    assert(d.stats == DeliveryStats(3, 4, 0, 0, 0, 0, 0))
  }

  test("a failure another try could fix is retried, with capped exponential backoff, and then delivered") {
    val t = new Scripted(Outcome.Retryable("503"), Outcome.Retryable("503"), Outcome.Retryable("timeout"))
    val s = OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 4, retryInitialBackoffMs = 100, retryMaxBackoffMs = 250)
    val d = delivery(t, s)
    d.submit(write())
    d.flush(5000)
    assert(sleeps.asScala.toList == List(100L, 200L, 250L))
    assert(t.sent.size == 5, "START tried four times, then RUNNING once")
    assert(d.stats.delivered == 2 && d.stats.failed == 0)
    assert(d.backoffMs(1) == 100 && d.backoffMs(2) == 200 && d.backoffMs(3) == 250 && d.backoffMs(30) == 250)
  }

  test("after maxAttempts a retryable failure is given up on, to the dead letter") {
    val t = new Scripted(Outcome.Retryable("a"), Outcome.Retryable("b"), Outcome.Retryable("c"))
    val dead = new Recorder
    val d = delivery(t, OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 3), Some(dead))
    d.submit(write())
    d.flush(5000)
    assert(t.sent.size == 4 && sleeps.size == 2, "three tries of START, two waits between them, then RUNNING")
    assert(dead.sent.size == 1 && json.readTree(dead.sent.peek()).get("eventType").asText() == "START")
    assert(d.stats.failed == 1 && d.stats.delivered == 1)
  }

  test("a refusal (4xx) is not retried, and goes to the dead letter") {
    val t = new Scripted(Outcome.Rejected("400"))
    val dead = new Recorder
    val d = delivery(t, OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 5), Some(dead))
    d.submit(write())
    d.flush(5000)
    assert(t.sent.size == 2 && sleeps.isEmpty && dead.sent.size == 1 && d.stats.failed == 1)
  }

  test("after circuitFailures events in a row fail, delivery pauses for circuitCooldownMs, then tries again") {
    val down = Seq.fill(3)(Outcome.Retryable("down"))
    val t = new Scripted(down: _*)
    val dead = new Recorder
    val d = delivery(t, OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 1, circuitFailures = 3, circuitCooldownMs = 1000), Some(dead))
    (1 to 5).foreach(i => d.submit(write(i)))
    d.flush(5000)
    // START + 5 RUNNING: the first three fail and open the circuit; the other three are skipped, not sent
    assert(t.sent.size == 3 && d.stats.failed == 3 && d.stats.skippedCircuitOpen == 3)
    assert(dead.sent.size == 6, "everything not delivered is in the dead letter")
    clock.set(TimeUnit.MILLISECONDS.toNanos(999))
    d.submit(write(6)); d.flush(5000)
    assert(t.sent.size == 3 && d.stats.skippedCircuitOpen == 4, "still paused just before the cooldown ends")
    clock.set(TimeUnit.MILLISECONDS.toNanos(1000))
    d.submit(write(7)); d.flush(5000)
    assert(t.sent.size == 4 && d.stats.delivered == 1, "tried again once the cooldown is over, and it worked")
  }

  test("a delivery, or a refusal, between failures resets the count: the circuit opens on failures in a row only") {
    val t = new Scripted(Outcome.Retryable("x"), Outcome.Retryable("x"), Outcome.Delivered, Outcome.Retryable("x"), Outcome.Retryable("x"),
      Outcome.Rejected("400"), Outcome.Retryable("x"), Outcome.Retryable("x"))
    val d = delivery(t, OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 1, circuitFailures = 3))
    (1 to 7).foreach(i => d.submit(write(i)))
    d.flush(5000)
    assert(d.stats.skippedCircuitOpen == 0 && t.sent.size == 8)
  }

  test("a full queue drops the newest event, counted, and never blocks the caller") {
    val gate = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    val blocking = new Transport {
      val describe = "blocking"
      def send(json: String): Outcome = { entered.countDown(); gate.await(); Outcome.Delivered }
    }
    val d = delivery(blocking, OpenLineageSettings(TransportSettings.Console, queueCapacity = 2))
    d.submit(write(0))
    assert(entered.await(5, TimeUnit.SECONDS), "the worker is busy delivering")
    val started = System.nanoTime()
    val results = (1 to 5).map(i => d.submit(write(i)))
    assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 1000, "submit never waits")
    assert(results == Seq(true, true, false, false, false))
    assert(d.stats.droppedQueueFull == 3 && d.stats.accepted == 3)
    gate.countDown()
    assert(d.flush(5000))
  }

  test("an event over maxEventBytes sheds its column lineage, then its schemas; one still too large is dropped") {
    val wide = OpenLineageMapperSpec.goodPlan
    val event = {
      val sink = new TestNotificationSink
      com.invaract.verification.VerificationPipeline.verifyWrite(OpenLineageMapperSpec.contract,
        com.invaract.verification.CheckedWrite(wide, List(OpenLineageMapperSpec.orders -> OpenLineageMapperSpec.inputSchema), OpenLineageMapperSpec.goodOutput, false, None, Set.empty),
        com.invaract.verification.VerificationOptions(), Some(sink), None, None, Some(job))
      sink.events.head
    }
    def runningOf(max: Int) = {
      val t = new Scripted
      val d = delivery(t, OpenLineageSettings(TransportSettings.Console, maxEventBytes = max))
      d.submit(event); d.flush(5000)
      (t.sent.asScala.toList.map(json.readTree).find(_.get("eventType").asText() == "RUNNING"), d.stats)
    }
    val (full, fullStats) = runningOf(1 << 20)
    val fullFacets = full.get.get("outputs").get(0).get("facets")
    assert(fullFacets.has("columnLineage") && fullFacets.has("schema") && fullStats.shed == 0)
    val fullSize = JsonText.render(json.convertValue(full.get, classOf[java.util.Map[String, Any]])).length
    val columnLineageSize = json.writeValueAsString(fullFacets.get("columnLineage")).length
    val (shedLineage, s1) = runningOf(fullSize - columnLineageSize / 2)
    val f1 = shedLineage.get.get("outputs").get(0).get("facets")
    assert(!f1.has("columnLineage") && f1.has("schema") && s1.shed == 1)
    val schemaSize = json.writeValueAsString(fullFacets.get("schema")).length
    val (shedSchema, s2) = runningOf(fullSize - columnLineageSize - schemaSize / 2)
    val f2 = shedSchema.get.get("outputs").get(0).get("facets")
    assert(!f2.has("columnLineage") && !f2.has("schema") && f2.has("invaract_contract") && s2.shed == 1)
    val (dropped, s3) = runningOf(1024)
    assert(dropped.isEmpty && s3.tooLarge >= 1, "the RUNNING event (and, this small, the START too) is not published")
  }

  test("without() removes the named facets from every dataset only") {
    val e = JsonText.Obj(Seq("inputs" -> List(JsonText.Obj(Seq("name" -> "a", "facets" -> JsonText.Obj(Seq("schema" -> 1, "storage" -> 2))))), "outputs" -> List("not a dataset"), "run" -> "kept"))
    assert(JsonText.render(OpenLineageDelivery.without(e, Set("schema"))) == """{"inputs":[{"name":"a","facets":{"storage":2}}],"outputs":["not a dataset"],"run":"kept"}""")
  }

  test("close ends every open run, delivers what is queued, and takes nothing after; it is idempotent") {
    val t = new Scripted
    val d = delivery(t)
    d.submit(write(1))
    assert(d.close(5000))
    assert(t.types == List("START", "RUNNING", "COMPLETE"))
    assert(!d.submit(write(2)) && d.close(5000))
    assert(t.sent.size == 3)
  }

  test("close is bounded: a transport that hangs costs at most the time given") {
    val hung = new CountDownLatch(1)
    val t = new Transport { val describe = "hung"; def send(json: String): Outcome = { hung.await(); Outcome.Delivered } }
    val d = delivery(t)
    d.submit(write())
    val started = System.nanoTime()
    assert(!d.close(300))
    assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 3000)
    hung.countDown()
  }

  test("a transport that throws costs that event only; the worker goes on") {
    val calls = new AtomicInteger
    val t = new Transport {
      val describe = "throwing"
      def send(json: String): Outcome = if (calls.incrementAndGet() == 1) throw new RuntimeException("boom") else Outcome.Delivered
    }
    val d = delivery(t)
    d.submit(write(1)); d.submit(write(2))
    assert(d.flush(5000) && calls.get >= 2)
  }

  test("closing during a retry's wait does not wait it out: one last try, then the rest, within the time given") {
    val tried = new CountDownLatch(1)
    val calls = new AtomicInteger
    val down = new Transport {
      val describe = "down"
      def send(json: String): Outcome = { calls.incrementAndGet(); tried.countDown(); Outcome.Retryable("down") }
    }
    val s = OpenLineageSettings(TransportSettings.Console, retryMaxAttempts = 5, retryInitialBackoffMs = 60000, retryMaxBackoffMs = 60000)
    // the real wait (no sleep seam): a minute per retry
    val d = new OpenLineageDelivery(s, down, new OpenLineageMapper(s, () => Instant.EPOCH, "/w"), None)
    d.submit(write())
    assert(tried.await(5, TimeUnit.SECONDS))
    val started = System.nanoTime()
    assert(d.close(10000), "closed within the time given")
    assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000, "not a minute's wait")
    // START: its first try, and the try its wait was for, woken by closing; RUNNING and the terminal event: one try each
    assert(calls.get == 4)
  }

  test("many threads submitting at once: every event is accepted or counted as dropped, and each accepted one delivered") {
    val t = new Scripted
    val d = delivery(t, OpenLineageSettings(TransportSettings.Console, queueCapacity = 100000))
    val threads = (1 to 8).map(i => new Thread(() => (1 to 500).foreach(n => d.submit(write(i * 1000 + n)))))
    threads.foreach(_.start()); threads.foreach(_.join())
    assert(d.flush(30000))
    val s = d.stats
    assert(s.accepted + s.droppedQueueFull == 4000 && s.accepted == 4000)
    assert(t.types.count(_ == "RUNNING") == 4000 && t.types.count(_ == "START") == 1)
  }
}
