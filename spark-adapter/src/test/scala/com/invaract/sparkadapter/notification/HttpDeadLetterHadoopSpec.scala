// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.invaract.verification.notification.{ContractValidationEvent, HttpNotificationSink}
import com.sun.net.httpserver.HttpServer
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress

/** `HttpNotificationSink`'s dead letter through Hadoop's `FileSystem`, named with
  * `deadLetter.class` - the one combination `verification-core` cannot test, since the
  * Hadoop-backed sink lives in this module.
  */
class HttpDeadLetterHadoopSpec extends AnyFunSuite {

  private val sampleEvent = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 1735689600000L, Map.empty)

  test("deadLetter.path with a scheme:// and deadLetter.class=HadoopFsNotificationSink writes one object per event") {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", exchange => { exchange.sendResponseHeaders(400, -1); exchange.close() })
    server.start()
    try {
      val dir = java.nio.file.Files.createTempDirectory("invaract-http-dead-letter-hadoop")
      val sink = new HttpNotificationSink
      sink.configure(
        Map(
          "url" -> s"http://127.0.0.1:${server.getAddress.getPort}/x",
          "deadLetter.path" -> dir.toUri.toString,
          "deadLetter.class" -> classOf[HadoopFsNotificationSink].getName,
          "retry.maxAttempts" -> "1",
          "retry.initialBackoffMs" -> "0"
        )
      )
      sink.publish(sampleEvent)
      sink.flush(5000L)

      val written = dir.toFile.listFiles().filter(f => !f.getName.startsWith(".") && !f.getName.endsWith(".crc"))
      assert(written.length == 1, s"expected one dead-letter object, found ${dir.toFile.list().toList}")
      assert(new String(java.nio.file.Files.readAllBytes(written.head.toPath), "UTF-8").contains("\"CONTRACT_VALIDATION\""))
    } finally server.stop(0)
  }
}
