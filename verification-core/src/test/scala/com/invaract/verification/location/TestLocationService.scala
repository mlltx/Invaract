// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.location

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/** A location service on a free local port, answering each request path with the `(status, body)` `answers` gives it (a
  * 404 for any other path), after `delayMs`. It records every request: its path, and its `Authorization` header. */
final class TestLocationService(answers: Map[String, (Int, String)], delayMs: Long = 0L) extends AutoCloseable {
  @volatile private var seen = Vector.empty[(String, Option[String])]
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

  server.createContext("/", (exchange: HttpExchange) => {
    val path = exchange.getRequestURI.getRawPath
    synchronized(seen :+= path -> Option(exchange.getRequestHeaders.getFirst("Authorization")))
    if (delayMs > 0) Thread.sleep(delayMs)
    val (status, body) = answers.getOrElse(path, 404 -> "")
    if (status / 100 == 3) exchange.getResponseHeaders.add("Location", body)
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(status, if (bytes.isEmpty) -1 else bytes.length.toLong)
    if (bytes.nonEmpty) exchange.getResponseBody.write(bytes)
    exchange.close()
  })
  server.start()

  /** The service's base URL. */
  val url: String = s"http://127.0.0.1:${server.getAddress.getPort}"

  /** The paths asked for, in order. */
  def paths: Vector[String] = seen.map(_._1)

  /** Each request's `Authorization` header, in order. */
  def authorizations: Vector[Option[String]] = seen.map(_._2)

  override def close(): Unit = server.stop(0)
}

object TestLocationService {

  /** A service holding each `id -> location`, answering as the protocol says. */
  def holding(locations: (String, String)*): TestLocationService =
    new TestLocationService(locations.map { case (id, location) => s"/locations/$id" -> (200 -> s"""{"location": "$location"}""") }.toMap)
}
