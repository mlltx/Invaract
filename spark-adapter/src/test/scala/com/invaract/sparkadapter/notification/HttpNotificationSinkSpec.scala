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
}
