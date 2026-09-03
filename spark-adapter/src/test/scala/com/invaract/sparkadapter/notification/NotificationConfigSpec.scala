// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class NotificationConfigSpec extends AnyFunSuite {

  private def withPropertiesFile(contents: String)(body: Path => Unit): Unit = {
    val file = Files.createTempFile("invaract-notify-test", ".properties")
    try {
      Files.write(file, contents.getBytes("UTF-8"))
      body(file)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("disabled is the zero-config default: no sink, no properties") {
    assert(!NotificationConfig.disabled.enabled)
    assert(NotificationConfig.disabled.sinkClassName.isEmpty)
    assert(NotificationConfig.disabled.properties.isEmpty)
  }

  test("load parses sink.enabled=true, sink.class, and sink.property.* with the prefix stripped") {
    withPropertiesFile(
      """sink.enabled=true
        |sink.class=com.invaract.sparkadapter.notification.FileNotificationSink
        |sink.property.path=/tmp/events.jsonl
        |sink.property.topic=invaract-events
        |""".stripMargin
    ) { file =>
      val config = NotificationConfig.load(file.toString)
      assert(config.enabled)
      assert(config.sinkClassName.contains("com.invaract.sparkadapter.notification.FileNotificationSink"))
      assert(config.properties == Map("path" -> "/tmp/events.jsonl", "topic" -> "invaract-events"))
    }
  }

  test("sink.enabled is case-insensitive") {
    withPropertiesFile("sink.enabled=TRUE\nsink.class=x.Y\n") { file =>
      assert(NotificationConfig.load(file.toString).enabled)
    }
  }

  test("sink.enabled defaults to false when absent, even with a sink.class present") {
    withPropertiesFile("sink.class=x.Y\n") { file =>
      val config = NotificationConfig.load(file.toString)
      assert(!config.enabled)
      assert(config.sinkClassName.contains("x.Y"), "sinkClassName is still parsed even when disabled")
    }
  }

  test("sink.enabled=false leaves enabled false regardless of other content") {
    withPropertiesFile("sink.enabled=false\nsink.class=x.Y\n") { file =>
      assert(!NotificationConfig.load(file.toString).enabled)
    }
  }

  test("a blank sink.class is treated as absent, not an empty-string class name") {
    withPropertiesFile("sink.enabled=true\nsink.class=   \n") { file =>
      assert(NotificationConfig.load(file.toString).sinkClassName.isEmpty)
    }
  }

  test("no sink.class key at all leaves sinkClassName empty") {
    withPropertiesFile("sink.enabled=true\n") { file =>
      assert(NotificationConfig.load(file.toString).sinkClassName.isEmpty)
    }
  }

  test("a key without the sink.property. prefix is not treated as a sink property") {
    withPropertiesFile("sink.enabled=true\nsink.class=x.Y\nunrelated.key=ignored\n") { file =>
      assert(NotificationConfig.load(file.toString).properties.isEmpty)
    }
  }

  test("no sink.property.* keys at all produces an empty properties map, not an error") {
    withPropertiesFile("sink.enabled=true\nsink.class=x.Y\n") { file =>
      assert(NotificationConfig.load(file.toString).properties.isEmpty)
    }
  }

  test("load throws when the file does not exist") {
    assertThrows[java.io.FileNotFoundException] {
      NotificationConfig.load("/does/not/exist/invaract-notify.properties")
    }
  }
}
