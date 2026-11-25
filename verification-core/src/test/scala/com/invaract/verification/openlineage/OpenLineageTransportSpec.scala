// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayInputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters._

/** The transports, against a real HTTP server and real files. */
class OpenLineageTransportSpec extends AnyFunSuite with BeforeAndAfterAll {

  final case class Received(path: String, headers: Map[String, String], body: String)

  private val received = new ConcurrentLinkedQueue[Received]()
  @volatile private var status = 200
  @volatile private var delayMs = 0L
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  server.createContext("/", (e: HttpExchange) => {
    val raw = e.getRequestBody.readAllBytes()
    val headers = e.getRequestHeaders.asScala.map { case (k, v) => k.toLowerCase -> v.asScala.mkString(",") }.toMap
    val body = if (headers.get("content-encoding").contains("gzip")) new String(new GZIPInputStream(new ByteArrayInputStream(raw)).readAllBytes(), StandardCharsets.UTF_8) else new String(raw, StandardCharsets.UTF_8)
    received.add(Received(e.getRequestURI.getPath, headers, body))
    if (delayMs > 0) Thread.sleep(delayMs)
    if (status == 302) e.getResponseHeaders.add("Location", "http://127.0.0.1:1/elsewhere")
    e.sendResponseHeaders(status, -1)
    e.close()
  })
  server.start()
  private val base = s"http://127.0.0.1:${server.getAddress.getPort}"

  override def afterAll(): Unit = server.stop(0)

  private def http(apiKey: Option[String] = None, gzip: Boolean = false, headers: Map[String, String] = Map.empty, timeoutMs: Long = 2000, url: String = base) =
    new HttpTransport(TransportSettings.Http(url, "api/v1/lineage", headers, apiKey, gzip, timeoutMs))

  private def last(): Received = { val all = received.asScala.toList; all.last }

  test("HTTP: a 2xx is delivered; the event is POSTed as JSON to url/endpoint with the API key as a bearer token and the headers given") {
    status = 201
    assert(http(Some("k3y"), headers = Map("X-Team" -> "orders")).send("""{"a":1}""") == Outcome.Delivered)
    val r = last()
    assert(r.path == "/api/v1/lineage" && r.body == """{"a":1}""")
    assert(r.headers("content-type") == "application/json" && r.headers("authorization") == "Bearer k3y" && r.headers("x-team") == "orders")
    assert(!r.headers.contains("content-encoding"))
  }

  test("HTTP: gzip compresses the body and says so") {
    status = 200
    val text = "{\"x\":\"" + ("é" * 5000) + "\"}"
    assert(http(gzip = true).send(text) == Outcome.Delivered)
    assert(last().headers("content-encoding") == "gzip" && last().body == text)
    assert(HttpTransport.gzip(text.getBytes(StandardCharsets.UTF_8)).length < text.getBytes(StandardCharsets.UTF_8).length / 10)
  }

  test("HTTP: 408, 429 and 5xx are worth another try; other statuses are refusals; a redirect is not followed") {
    def outcome(code: Int) = { status = code; http().send("{}") }
    assert(List(408, 429, 500, 502, 503, 599).forall(c => outcome(c).isInstanceOf[Outcome.Retryable]))
    assert(List(400, 401, 403, 404, 413, 422, 302).forall(c => outcome(c).isInstanceOf[Outcome.Rejected]))
    status = 200
    assert(outcome(299) == Outcome.Delivered && outcome(200) == Outcome.Delivered)
    assert(outcome(500).asInstanceOf[Outcome.Retryable].message.endsWith("answered 500"))
    assert(outcome(404).asInstanceOf[Outcome.Rejected].message.endsWith("refused the event with 404"))
    assert(Transport.isRetryableStatus(408) && Transport.isRetryableStatus(429) && Transport.isRetryableStatus(500) && Transport.isRetryableStatus(599))
    assert(!Transport.isRetryableStatus(400) && !Transport.isRetryableStatus(499) && !Transport.isRetryableStatus(600) && !Transport.isRetryableStatus(200))
  }

  test("HTTP: an unreachable endpoint and a timeout are worth another try, and are said without credentials") {
    val unreachable = http(url = "http://user:secret@127.0.0.1:1").send("{}")
    assert(unreachable.isInstanceOf[Outcome.Retryable])
    val message = unreachable.asInstanceOf[Outcome.Retryable].message
    assert(message.contains("could not be reached") && !message.contains("secret") && message.contains("***@"))
    status = 200; delayMs = 1500
    try assert(http(timeoutMs = 200).send("{}").isInstanceOf[Outcome.Retryable])
    finally delayMs = 0
  }

  test("file: one line per event, appended, the folder made; concurrent writers interleave whole lines") {
    val dir = Files.createTempDirectory("ol-file")
    val path = dir.resolve("nested/events.jsonl")
    val transport = new FileTransport(path.toString)
    val other = new FileTransport(path.toString)
    val threads = (1 to 6).map(i => new Thread(() => (1 to 200).foreach(n => (if (i % 2 == 0) transport else other).send(s"""{"t":$i,"n":$n,"pad":"${"x" * 300}"}"""))))
    threads.foreach(_.start()); threads.foreach(_.join())
    val lines = Files.readAllLines(path).asScala.toList
    assert(lines.size == 1200)
    assert(lines.forall(l => l.startsWith("{\"t\":") && l.endsWith("\"}")), "every line whole")
    assert(transport.describe == s"file $path")
    val blocked = new FileTransport(dir.toString)
    assert(blocked.send("{}").isInstanceOf[Outcome.Retryable], "a folder cannot be written as a file")
  }

  test("console: delivered, to the log") {
    assert(ConsoleTransport.send("{}") == Outcome.Delivered && ConsoleTransport.describe == "the log")
  }

  test("Transport builds the transport its settings name") {
    assert(Transport(TransportSettings.File("/tmp/x")).isInstanceOf[FileTransport])
    assert(Transport(TransportSettings.Console) eq ConsoleTransport)
    assert(Transport(TransportSettings.Http(base, "e", Map.empty, None, false, 1)).describe == s"$base/e")
  }
}
