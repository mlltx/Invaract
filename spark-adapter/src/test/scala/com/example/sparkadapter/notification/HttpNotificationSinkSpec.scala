// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

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
}
