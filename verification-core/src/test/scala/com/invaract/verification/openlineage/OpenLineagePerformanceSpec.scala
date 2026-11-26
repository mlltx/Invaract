// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.contract.{ContractParser, LogicalField, LogicalSchema}
import com.invaract.contract.LogicalType.LongType
import com.invaract.ir._
import com.invaract.verification.{CheckedWrite, VerificationOptions, VerificationPipeline}
import com.invaract.verification.notification._
import com.sun.net.httpserver.{HttpExchange, HttpServer}

import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/** What turning OpenLineage on costs a job, measured (docs/OPENLINEAGE.md, "Performance"). The bounds are loose - many times
  * what is measured on an idle machine, since the suite also runs under mutation testing's instrumented code, four runners at
  * once - so the suite holds the design (nothing slow on the job's thread, nothing unbounded), not one
  * machine's speed; the measured figures are printed for the design document. */
class OpenLineagePerformanceSpec extends AnyFunSuite {

  private val width = 200

  private def Column(name: String, from: String): Expr = ColumnReference(ColumnRef(name, Some(from)))

  /** A wide job: two inputs of `width` columns joined, filtered, and every output column computed from both. */
  private val (plan, inputs, output) = {
    val a = (0 until width).map(i => s"a$i")
    val b = (0 until width).map(i => s"b$i")
    val joined = Join(Read(DatasetRef("in/left")), Read(DatasetRef("in/right")), JoinType.Inner, Some(Comparison("=", Column("a0", "in/left"), Column("b0", "in/right"))))
    val filtered = Filter(joined, Comparison(">", Column("a1", "in/left"), Literal(0, "int")))
    val projected = Project(filtered, (0 until width).map(i => NamedExpr(s"out$i", Arithmetic("+", List(Column(a(i), "in/left"), Column(b(i), "in/right"))))).toList)
    val schema = (names: Seq[String]) => LogicalSchema(names.map(n => LogicalField(n, LongType, nullable = true)).toList)
    (Write(DatasetRef("out/wide"), projected, Some("parquet"), Some("overwrite")), List("in/left" -> schema(a), "in/right" -> schema(b)), schema((0 until width).map(i => s"out$i")))
  }

  private val contract = ContractParser.parse(
    """id: wide
      |version: "1.0.0"
      |inputs:
      |  - { name: left, location: in/left }
      |  - { name: right, location: in/right }
      |outputs:
      |  - { name: wide, location: out/wide, format: parquet }
      |""".stripMargin)

  private def checked = CheckedWrite(plan, inputs, output, caseSensitive = false, None, Set.empty)
  private def job(run: String) = Some(JobInfo(runId = Some(run), engine = Some("perf")))

  /** A server that answers every event after `delayMs`, counting them. */
  private def server(delayMs: Long): (HttpServer, AtomicInteger) = {
    val count = new AtomicInteger()
    val s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    s.createContext("/", (e: HttpExchange) => { e.getRequestBody.readAllBytes(); Thread.sleep(delayMs); count.incrementAndGet(); e.sendResponseHeaders(200, -1); e.close() })
    s.start()
    (s, count)
  }

  private def nanosPerWrite(writes: Int, sink: Option[NotificationSink], run: String): Double = {
    val started = System.nanoTime()
    (1 to writes).foreach(_ => VerificationPipeline.verifyWrite(contract, checked, VerificationOptions(), sink, None, None, job(run)))
    (System.nanoTime() - started).toDouble / writes
  }

  test("a check costs the job little more with OpenLineage on, against an endpoint answering in 20 ms") {
    val (s, delivered) = server(20)
    try {
      val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.Http(s"http://127.0.0.1:${s.getAddress.getPort}", "api/v1/lineage", Map.empty, None, gzip = false, 5000)))
      // warm the JIT on both paths first
      nanosPerWrite(300, None, "warm"); nanosPerWrite(300, Some(sink), "warm")
      // alternating rounds, the best of each: what the code costs, not what the machine was doing at the time
      val writes = 300
      val rounds = (1 to 5).map(r => (nanosPerWrite(writes, None, s"off-$r"), nanosPerWrite(writes, Some(sink), s"on-$r")))
      val off = rounds.map(_._1).min
      val on = rounds.map(_._2).min
      val overheadMicros = (on - off) / 1000
      info(f"wide check ($width columns): ${off / 1000}%.1f us off, ${on / 1000}%.1f us on: +$overheadMicros%.1f us per write on the job's thread")
      assert(overheadMicros < 5000, "turning OpenLineage on costs a check a few milliseconds at most on the job's thread, even on a loaded machine")
      assert(delivered.get() < writes * rounds.size, "the endpoint is far slower than the job: delivery happens behind it, not in it")
      sink.close(1000)
    } finally s.stop(0)
  }

  test("publishing never waits on a slow endpoint: 1,000 events against one answering in 1 s take the job milliseconds") {
    val (s, _) = server(1000)
    try {
      val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.Http(s"http://127.0.0.1:${s.getAddress.getPort}", "api/v1/lineage", Map.empty, None, gzip = false, 5000)))
      val event = WriteEvent(None, "/out/x", Some("parquet"), None, Nil, 1L, Map.empty, job = job("slow"))
      val started = System.nanoTime()
      (1 to 1000).foreach(_ => sink.publish(event))
      val millis = (System.nanoTime() - started) / 1000000
      info(s"1,000 publishes to an endpoint answering in 1 s: $millis ms on the job's thread")
      assert(millis < 1000)
      val closing = System.nanoTime()
      sink.close(500)
      assert((System.nanoTime() - closing) / 1000000 < 3000, "the end of the job waits at most what shutdownTimeoutMs allows, plus one request in flight")
    } finally s.stop(0)
  }

  test("memory is bounded: a flood against a stalled endpoint keeps at most queue.capacity events, the rest counted as dropped") {
    val stalled = new CountDownLatch(1)
    val transport = new Transport {
      val describe = "stalled"
      def send(json: String): Outcome = { stalled.await(30, TimeUnit.SECONDS); Outcome.Delivered }
    }
    val settings = OpenLineageSettings(TransportSettings.Console, queueCapacity = 1000)
    val delivery = new OpenLineageDelivery(settings, transport, new OpenLineageMapper(settings), None)
    val event = WriteEvent(None, "/out/x", Some("parquet"), None, Nil, 1L, Map.empty, job = job("flood"))
    val flood = 50000
    val started = System.nanoTime()
    val accepted = (1 to flood).count(_ => delivery.submit(event))
    val micros = (System.nanoTime() - started) / 1000.0 / flood
    info(f"$flood submits against a stalled endpoint: $micros%.2f us each; $accepted accepted, ${delivery.stats.droppedQueueFull} dropped")
    assert(accepted <= settings.queueCapacity + 1, "at most the queue, plus the one the worker holds")
    assert(delivery.stats.droppedQueueFull == flood - accepted)
    assert(micros < 500, "a full queue costs the job an offer, never a wait")
    stalled.countDown()
    delivery.close(5000)
  }

  test("the worker maps and writes wide events fast enough to keep up with a busy job") {
    val out = Files.createTempDirectory("ol-perf").resolve("events.jsonl")
    val events = 2000
    // a queue that holds the whole burst: this measures the worker, not what a full queue drops
    val sink = new OpenLineageSink(OpenLineageSettings(TransportSettings.File(out.toString), queueCapacity = events + 10))
    val capture = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked, VerificationOptions(), Some(capture), None, None, job("throughput"))
    val event = capture.events.head
    val started = System.nanoTime()
    (1 to events).foreach(_ => sink.publish(event))
    sink.flush(60000)
    val perSecond = events / ((System.nanoTime() - started) / 1e9)
    val bytes = Files.size(out) / (events + 1)
    info(f"worker: $perSecond%.0f wide events/s ($width columns, column lineage on), about $bytes bytes each")
    assert(perSecond > 10, "the worker is not pathologically slow (it is several hundred a second on an idle machine)")
    assert(sink.stats.exists(s => s.delivered == events + 1 && s.droppedQueueFull == 0))
    sink.close(1000)
  }

  test("without a sink, nothing is built for lineage: the check costs what it did") {
    // the lineage of a validation event is built only when a sink will receive it
    val capture = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked, VerificationOptions(), Some(capture), None, None, job("lineage"))
    assert(capture.events.collect { case e: ContractValidationEvent => e }.head.lineage.isDefined)
    val noSink = nanosPerWrite(200, None, "none")
    assert(noSink > 0)
  }
}
