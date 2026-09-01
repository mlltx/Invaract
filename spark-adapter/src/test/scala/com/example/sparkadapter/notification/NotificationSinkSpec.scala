// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

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
}
