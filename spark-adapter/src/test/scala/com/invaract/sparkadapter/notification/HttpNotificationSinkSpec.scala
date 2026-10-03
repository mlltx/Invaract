// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.sun.net.httpserver.HttpServer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.net.InetSocketAddress
import scala.collection.mutable

/** Against a real, local `com.sun.net.httpserver.HttpServer` (JDK-bundled,
  * the same "real thing over a mock" discipline this repo's Spark-facing
  * specs already use) rather than mocking `java.net.http.HttpClient`.
  */
class HttpNotificationSinkSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var server: HttpServer = _
  private val receivedBodies = mutable.ListBuffer.empty[String]
  @volatile private var responseCode = 200
  private val receivedHeaders = mutable.ListBuffer.empty[Map[String, String]]
  private val slowRequestsCompleted = new java.util.concurrent.atomic.AtomicInteger(0)
  @volatile private var slowMillis = 600L
  // /flaky answers each request with the next status queued here (200 once the queue is empty, or `flakyAlways`
  // if that is set), and counts every request it receives.
  private val flakyStatuses = new java.util.concurrent.ConcurrentLinkedQueue[Integer]()
  @volatile private var flakyAlways: Option[Int] = None
  private val flakyRequests = new java.util.concurrent.atomic.AtomicInteger(0)

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/events",
      exchange => {
        val body = new String(exchange.getRequestBody.readAllBytes(), "UTF-8")
        receivedBodies.synchronized(receivedBodies += body)
        exchange.sendResponseHeaders(responseCode, -1)
        exchange.close()
      }
    )
    server.createContext(
      "/headers",
      exchange => {
        val seen = exchange.getRequestHeaders.entrySet().iterator()
        var captured = Map.empty[String, String]
        while (seen.hasNext) {
          val entry = seen.next()
          captured += entry.getKey.toLowerCase -> entry.getValue.get(0)
        }
        receivedHeaders.synchronized(receivedHeaders += captured)
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      }
    )
    server.createContext(
      "/slow",
      exchange => {
        exchange.getRequestBody.readAllBytes()
        Thread.sleep(slowMillis)
        slowRequestsCompleted.incrementAndGet()
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      }
    )
    server.createContext(
      "/flaky",
      exchange => {
        exchange.getRequestBody.readAllBytes()
        flakyRequests.incrementAndGet()
        val queued = Option(flakyStatuses.poll()).map(_.intValue())
        exchange.sendResponseHeaders(queued.orElse(flakyAlways).getOrElse(200), -1)
        exchange.close()
      }
    )
    // The default executor is one thread: a deliberately slow handler would stall every other test's request.
    server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool())
    server.start()
  }

  override def afterAll(): Unit = server.stop(0)

  private def url: String = s"http://127.0.0.1:${server.getAddress.getPort}/events"

  private val sampleEvent = ContractValidationEvent(
    contract = "demo@1.0.0",
    status = "PASSED",
    violations = Nil,
    timestamp = 0L,
    metadata = Map.empty
  )

  test("configure throws without a 'url' property") {
    assertThrows[IllegalArgumentException] {
      new HttpNotificationSink().configure(Map.empty)
    }
  }

  test("configure throws for a non-numeric timeoutMs") {
    assertThrows[IllegalArgumentException] {
      new HttpNotificationSink().configure(Map("url" -> url, "timeoutMs" -> "not-a-number"))
    }
  }

  test("configure accepts a numeric timeoutMs without throwing") {
    new HttpNotificationSink().configure(Map("url" -> url, "timeoutMs" -> "1234")) // must not throw
  }

  test("publish POSTs the event's JSON, with a Content-Type header, to the configured url") {
    receivedBodies.synchronized(receivedBodies.clear())
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> url))
    sink.publish(sampleEvent.copy(contract = "http-test@1.0.0"))

    eventually(timeout(Span(5, Seconds))) {
      val bodies = receivedBodies.synchronized(receivedBodies.toList)
      assert(bodies.exists(_.contains("\"http-test@1.0.0\"")), s"expected a request body containing the contract ref, got: $bodies")
    }
  }

  test("publish does not throw when the server responds with a non-2xx status") {
    responseCode = 500
    try {
      val sink = new HttpNotificationSink
      sink.configure(Map("url" -> url))
      sink.publish(sampleEvent) // must not throw synchronously - the bad status is only logged
    } finally {
      responseCode = 200
    }
  }

  test("publish does not throw when the endpoint is unreachable") {
    val throwaway = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val deadPort = throwaway.getAddress.getPort
    throwaway.stop(0) // the port now refuses connections

    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> s"http://127.0.0.1:$deadPort/unreachable", "timeoutMs" -> "1000"))
    sink.publish(sampleEvent) // connection failure is delivered async - must not throw synchronously
  }

  // failureMessage is publish's async-callback decision, pulled out
  // specifically so these branches (a real behavioral choice - log or
  // don't, throwable vs. status code) are directly testable rather than
  // only reachable via a real async HTTP round trip whose only observable
  // effect (a log line) these tests have no way to assert on.
  test("failureMessage describes a connection failure via the throwable, without evaluating statusCode") {
    val ex = new RuntimeException("boom")
    val msg = HttpNotificationSink.failureMessage(ex, throw new AssertionError("statusCode must not be evaluated when throwable is set"), "WRITE", "http://x")
    assert(msg.exists(_.contains("WRITE")))
    assert(msg.exists(_.contains("http://x")))
  }

  test("failureMessage describes a non-2xx response when there is no throwable") {
    val msg = HttpNotificationSink.failureMessage(null, 500, "CONTRACT_VALIDATION", "http://x")
    assert(msg.exists(_.contains("500")))
    assert(msg.exists(_.contains("CONTRACT_VALIDATION")))
  }

  test("failureMessage returns None for a successful (2xx) response with no throwable") {
    assert(HttpNotificationSink.failureMessage(null, 200, "WRITE", "http://x").isEmpty)
    assert(HttpNotificationSink.failureMessage(null, 299, "WRITE", "http://x").isEmpty)
  }

  test("failureMessage boundary: 299 is success, 300 is not, 199 is not") {
    assert(HttpNotificationSink.failureMessage(null, 299, "x", "u").isEmpty)
    assert(HttpNotificationSink.failureMessage(null, 300, "x", "u").isDefined)
    assert(HttpNotificationSink.failureMessage(null, 199, "x", "u").isDefined)
  }

  private def headersUrl: String = s"http://127.0.0.1:${server.getAddress.getPort}/headers"
  private def slowUrl: String = s"http://127.0.0.1:${server.getAddress.getPort}/slow"

  private def lastHeaders(): Map[String, String] = {
    eventually(timeout(Span(5, Seconds))) {
      assert(receivedHeaders.synchronized(receivedHeaders.nonEmpty), "no request reached /headers")
    }
    receivedHeaders.synchronized(receivedHeaders.last)
  }

  test("configured header.<Name> properties are sent on every request, next to Content-Type") {
    receivedHeaders.synchronized(receivedHeaders.clear())
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> headersUrl, "header.X-Tenant" -> "acme", "header.X-Trace" -> "t-1", "timeoutMs" -> "5000"))
    sink.publish(sampleEvent)

    val headers = lastHeaders()
    assert(headers.get("x-tenant").contains("acme"))
    assert(headers.get("x-trace").contains("t-1"))
    assert(headers.get("content-type").contains("application/json"))
    assert(!headers.contains("authorization"), "no bearerTokenEnv configured: no Authorization header")
  }

  test("bearerTokenEnv sends Authorization: Bearer <value of that environment variable>") {
    receivedHeaders.synchronized(receivedHeaders.clear())
    val sink = new HttpNotificationSink {
      override protected def environmentVariable(name: String): Option[String] =
        if (name == "INVARACT_TOKEN") Some("s3cret") else None
    }
    sink.configure(Map("url" -> headersUrl, "bearerTokenEnv" -> "INVARACT_TOKEN"))
    sink.publish(sampleEvent)
    assert(lastHeaders().get("authorization").contains("Bearer s3cret"))
  }

  test("bearerTokenEnv naming an unset (or empty) variable fails at setup, without echoing any secret") {
    val unset = new HttpNotificationSink {
      override protected def environmentVariable(name: String): Option[String] = None
    }
    val e1 = intercept[IllegalArgumentException](unset.configure(Map("url" -> url, "bearerTokenEnv" -> "NOPE")))
    assert(e1.getMessage.contains("NOPE") && e1.getMessage.contains("not set"))

    val empty = new HttpNotificationSink {
      override protected def environmentVariable(name: String): Option[String] = Some("")
    }
    intercept[IllegalArgumentException](empty.configure(Map("url" -> url, "bearerTokenEnv" -> "EMPTY")))
  }

  test("a malformed or restricted header name fails at setup, naming the header") {
    val e = intercept[IllegalArgumentException] {
      new HttpNotificationSink().configure(Map("url" -> url, "header.Bad Name" -> "x"))
    }
    assert(e.getMessage.contains("Bad Name"))
    val restricted = intercept[IllegalArgumentException] {
      new HttpNotificationSink().configure(Map("url" -> url, "header.Host" -> "evil"))
    }
    assert(restricted.getMessage.contains("Host"))
  }

  test("flush waits for a request still in flight; without it the request would still be pending") {
    slowRequestsCompleted.set(0)
    slowMillis = 600L
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> slowUrl, "timeoutMs" -> "5000"))
    sink.publish(sampleEvent)
    assert(slowRequestsCompleted.get() == 0, "premise: the server is still sleeping when publish returns")

    sink.flush(5000L)
    assert(slowRequestsCompleted.get() == 1, "flush returned before the in-flight request finished")
  }

  test("flush is bounded: a timeout shorter than the request returns early, without throwing") {
    slowRequestsCompleted.set(0)
    slowMillis = 1500L
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> slowUrl, "timeoutMs" -> "5000"))
    sink.publish(sampleEvent)

    val started = System.nanoTime()
    sink.flush(100L)
    val elapsedMs = (System.nanoTime() - started) / 1000000L
    assert(elapsedMs < 1000L, s"flush(100) took ${elapsedMs}ms")
    assert(slowRequestsCompleted.get() == 0)
    sink.flush(5000L) // let the straggler finish so it cannot leak into another test
  }

  test("flush with nothing in flight, or after a failed request, returns immediately") {
    val idle = new HttpNotificationSink
    idle.configure(Map("url" -> url))
    idle.flush(1000L)

    val throwaway = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val deadPort = throwaway.getAddress.getPort
    throwaway.stop(0)
    val failing = new HttpNotificationSink
    failing.configure(Map("url" -> s"http://127.0.0.1:$deadPort/x", "timeoutMs" -> "1000"))
    failing.publish(sampleEvent)
    failing.flush(5000L) // the failure was already logged by publish's callback; flush only waits
  }

  test("a completed request is forgotten, so flush does not accumulate delivered requests") {
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> url))
    (1 to 3).foreach(_ => sink.publish(sampleEvent))
    sink.flush(5000L)
    val started = System.nanoTime()
    sink.flush(5000L)
    assert((System.nanoTime() - started) / 1000000L < 500L)
  }

  // ---- retry and dead letter ------------------------------------------------------------------

  private def flakyUrl: String = s"http://127.0.0.1:${server.getAddress.getPort}/flaky"

  private def resetFlaky(statuses: Int*): Unit = {
    flakyStatuses.clear()
    statuses.foreach(s => flakyStatuses.add(Int.box(s)))
    flakyAlways = None
    flakyRequests.set(0)
  }

  private def deadLetterFile(): java.nio.file.Path = {
    val file = java.nio.file.Files.createTempFile("invaract-http-dead-letter", ".jsonl")
    java.nio.file.Files.delete(file) // FileNotificationSink creates it on first append
    file
  }

  private def lines(file: java.nio.file.Path): List[String] =
    if (java.nio.file.Files.exists(file)) new String(java.nio.file.Files.readAllBytes(file), "UTF-8").split("\n").toList.filter(_.nonEmpty)
    else Nil

  private def fastRetry(extra: (String, String)*): Map[String, String] =
    Map("retry.initialBackoffMs" -> "10", "retry.maxBackoffMs" -> "50", "timeoutMs" -> "5000") ++ extra

  test("a 5xx is retried until it succeeds, and a delivered event is never dead-lettered") {
    resetFlaky(503, 500)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString) ++ fastRetry())
    sink.publish(sampleEvent)
    sink.flush(5000L)

    assert(flakyRequests.get() == 3, "two failures then the success")
    assert(lines(dead).isEmpty)
  }

  test("408 and 429 are retried too") {
    resetFlaky(408, 429)
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl) ++ fastRetry())
    sink.publish(sampleEvent)
    sink.flush(5000L)
    assert(flakyRequests.get() == 3)
  }

  test("any other non-2xx (400, 401, 404, 422) is a refusal a retry cannot fix: one request, straight to the dead letter") {
    Seq(400, 401, 404, 422).foreach { status =>
      resetFlaky(status)
      val dead = deadLetterFile()
      val sink = new HttpNotificationSink
      sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString) ++ fastRetry())
      sink.publish(sampleEvent)
      sink.flush(5000L)

      assert(flakyRequests.get() == 1, s"status $status must not be retried")
      assert(lines(dead).size == 1, s"status $status must be dead-lettered")
    }
  }

  test("retries are exhausted after retry.maxAttempts, then the exact event is dead-lettered with its eventId") {
    resetFlaky()
    flakyAlways = Some(503)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString) ++ fastRetry("retry.maxAttempts" -> "4"))
    val event = sampleEvent.copy(contract = "dead-letter-test@1.0.0")
    sink.publish(event)
    sink.flush(5000L)

    assert(flakyRequests.get() == 4)
    val saved = lines(dead)
    assert(saved.size == 1)
    assert(saved.head.contains("\"dead-letter-test@1.0.0\""))
    assert(saved.head.contains(NotificationJson.eventIdOf(event)), "a replay must carry the same eventId so the receiver can deduplicate")
    EventSchema.assertValid(saved.head)
  }

  test("retry.maxAttempts=1 means no retries: one request, then the dead letter") {
    resetFlaky()
    flakyAlways = Some(500)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString) ++ fastRetry("retry.maxAttempts" -> "1"))
    sink.publish(sampleEvent)
    sink.flush(5000L)
    assert(flakyRequests.get() == 1)
    assert(lines(dead).size == 1)
  }

  test("the default is three attempts, and with no deadLetter.path an exhausted event is dropped without error") {
    resetFlaky()
    flakyAlways = Some(500)
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "retry.initialBackoffMs" -> "10", "retry.maxBackoffMs" -> "20"))
    sink.publish(sampleEvent) // must not throw
    sink.flush(5000L)
    assert(flakyRequests.get() == 3)
  }

  test("a connection failure is retried, then dead-lettered") {
    val throwaway = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val deadPort = throwaway.getAddress.getPort
    throwaway.stop(0)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    val started = System.nanoTime()
    sink.configure(
      Map("url" -> s"http://127.0.0.1:$deadPort/x", "deadLetter.path" -> dead.toString, "retry.initialBackoffMs" -> "150", "retry.maxBackoffMs" -> "150", "timeoutMs" -> "1000")
    )
    sink.publish(sampleEvent)
    sink.flush(10000L)
    val elapsedMs = (System.nanoTime() - started) / 1000000L

    assert(lines(dead).size == 1)
    assert(elapsedMs >= 300L, s"two 150ms backoffs must have elapsed between three attempts, only $elapsedMs ms did")
  }

  test("flush waits across a retry's backoff, so a retried event is delivered before the job exits") {
    resetFlaky(503)
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "retry.initialBackoffMs" -> "400", "retry.maxBackoffMs" -> "400", "timeoutMs" -> "5000"))
    sink.publish(sampleEvent)
    assert(flakyRequests.get() <= 1, "the retry is still waiting out its backoff when publish returns")
    sink.flush(5000L)
    assert(flakyRequests.get() == 2)
  }

  test("flush that runs out of time hands undelivered events to the dead letter, once, and stops retrying them") {
    resetFlaky()
    flakyAlways = Some(503)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    sink.configure(
      Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString, "retry.maxAttempts" -> "5", "retry.initialBackoffMs" -> "800", "retry.maxBackoffMs" -> "800", "timeoutMs" -> "5000")
    )
    sink.publish(sampleEvent)
    eventually(timeout(Span(5, Seconds))) { assert(flakyRequests.get() == 1) }

    sink.flush(200L) // far less than the 800ms backoff
    assert(lines(dead).size == 1, "the straggler must be saved before the JVM can exit")

    Thread.sleep(1300L) // let the pending backoff elapse
    assert(flakyRequests.get() == 1, "an abandoned event must not be retried")
    assert(lines(dead).size == 1, "and must not be saved a second time")
  }

  test("an event given up on after flush already abandoned it is not dead-lettered again") {
    resetFlaky()
    flakyAlways = Some(400)
    val dead = deadLetterFile()
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dead.toString) ++ fastRetry())
    sink.publish(sampleEvent)
    sink.flush(5000L)
    sink.flush(5000L)
    assert(lines(dead).size == 1)
  }

  test("deadLetter.path with a scheme:// goes through Hadoop's FileSystem: one file per event") {
    resetFlaky()
    flakyAlways = Some(400)
    val dir = java.nio.file.Files.createTempDirectory("invaract-http-dead-letter-hadoop")
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> dir.toUri.toString) ++ fastRetry())
    sink.publish(sampleEvent)
    sink.flush(5000L)

    val written = dir.toFile.listFiles().filter(f => !f.getName.startsWith(".") && !f.getName.endsWith(".crc"))
    assert(written.length == 1, s"expected one dead-letter object, found ${dir.toFile.list().toList}")
    assert(new String(java.nio.file.Files.readAllBytes(written.head.toPath), "UTF-8").contains("\"CONTRACT_VALIDATION\""))
  }

  test("a dead letter that cannot be written is logged, never thrown") {
    resetFlaky()
    flakyAlways = Some(400)
    val unwritable = java.nio.file.Files.createTempDirectory("invaract-http-unwritable").resolve("no-such-dir").resolve("dead.jsonl")
    val sink = new HttpNotificationSink
    sink.configure(Map("url" -> flakyUrl, "deadLetter.path" -> unwritable.toString) ++ fastRetry())
    sink.publish(sampleEvent) // must not throw
    sink.flush(5000L) // must not throw
    assert(!java.nio.file.Files.exists(unwritable))
  }

  test("retry.* and deadLetter.* configuration is validated at setup") {
    def configure(extra: (String, String)*) = new HttpNotificationSink().configure(Map("url" -> url) ++ extra)
    configure("retry.maxAttempts" -> "1", "retry.initialBackoffMs" -> "0", "retry.backoffMultiplier" -> "1", "retry.maxBackoffMs" -> "0") // the boundaries are valid

    Seq(
      "retry.maxAttempts" -> "0",
      "retry.maxAttempts" -> "-1",
      "retry.maxAttempts" -> "two",
      "retry.maxAttempts" -> "2.5",
      "retry.initialBackoffMs" -> "-1",
      "retry.initialBackoffMs" -> "soon",
      "retry.backoffMultiplier" -> "0.99",
      "retry.backoffMultiplier" -> "double",
      "retry.maxBackoffMs" -> "-1",
      "retry.maxBackoffMs" -> "long"
    ).foreach { case (key, bad) =>
      val e = intercept[IllegalArgumentException](configure(key -> bad))
      assert(e.getMessage.contains(key) && e.getMessage.contains(bad), s"$key=$bad: ${e.getMessage}")
    }

    val noPath = intercept[IllegalArgumentException](configure("deadLetter.hadoop.fs.s3a.access.key" -> "k"))
    assert(noPath.getMessage.contains("deadLetter.path"))
  }

  test("RetryPolicy: defaults, and backoff grows geometrically, starts at the initial delay, and is capped") {
    val defaults = HttpNotificationSink.RetryPolicy()
    assert(defaults.maxAttempts == 3 && defaults.initialBackoffMs == 200L && defaults.backoffMultiplier == 2.0 && defaults.maxBackoffMs == 5000L)
    assert(HttpNotificationSink.RetryPolicy.from(Map.empty) == defaults)

    val policy = HttpNotificationSink.RetryPolicy(maxAttempts = 9, initialBackoffMs = 100L, backoffMultiplier = 3.0, maxBackoffMs = 1000L)
    assert(policy.backoffMs(1) == 100L)
    assert(policy.backoffMs(2) == 300L)
    assert(policy.backoffMs(3) == 900L)
    assert(policy.backoffMs(4) == 1000L, "capped")
    assert(policy.backoffMs(8) == 1000L)

    val parsed = HttpNotificationSink.RetryPolicy.from(
      Map("retry.maxAttempts" -> " 7 ", "retry.initialBackoffMs" -> "5", "retry.backoffMultiplier" -> "1.5", "retry.maxBackoffMs" -> "99")
    )
    assert(parsed == HttpNotificationSink.RetryPolicy(7, 5L, 1.5, 99L))
  }

  test("isRetryableStatus: 408, 429 and 500-599 only") {
    val retryable = (0 to 700).filter(HttpNotificationSink.isRetryableStatus)
    assert(retryable == Seq(408, 429) ++ (500 to 599))
  }

  test("failureOf: a connection failure is retryable without ever reading a status code; a 2xx is not a failure") {
    val ex = new RuntimeException("boom")
    val f = HttpNotificationSink.failureOf(ex, throw new AssertionError("statusCode must not be evaluated"), "WRITE", "http://x").get
    assert(f.retryable && (f.cause eq ex) && f.message.contains("WRITE"))

    assert(HttpNotificationSink.failureOf(null, 200, "WRITE", "http://x").isEmpty)
    assert(HttpNotificationSink.failureOf(null, 503, "WRITE", "http://x").exists(_.retryable))
    assert(HttpNotificationSink.failureOf(null, 429, "WRITE", "http://x").exists(_.retryable))
    val refused = HttpNotificationSink.failureOf(null, 404, "WRITE", "http://x").get
    assert(!refused.retryable && refused.cause == null && refused.message.contains("404"))
  }
}
