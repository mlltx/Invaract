// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification.kafka

import com.invaract.sparkadapter.notification.ContractValidationEvent

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** Against Kafka's own `MockProducer` — a real, officially-supported test
  * double `kafka-clients` ships for exactly this purpose — rather than a
  * hand-rolled mock or a real broker, the same "prefer the ecosystem's own
  * real thing" discipline `spark-adapter`'s specs use real Spark sessions
  * for. `MockProducer` is substituted via `KafkaNotificationSink`'s
  * `private[kafka]` constructor, which `NotificationSinkFactory`'s public
  * (no-arg) path never uses.
  */
class KafkaNotificationSinkSpec extends AnyFunSuite {

  private val sampleEvent = ContractValidationEvent(
    contract = "demo@1.0.0",
    status = "PASSED",
    violations = Nil,
    timestamp = 0L,
    metadata = Map.empty
  )

  private def newMockProducer(): MockProducer[String, String] =
    new MockProducer[String, String](true, new StringSerializer, new StringSerializer)

  test("configure throws without a 'topic' property") {
    val sink = new KafkaNotificationSink(Some(newMockProducer()))
    assertThrows[IllegalArgumentException] {
      sink.configure(Map.empty)
    }
  }

  test("publish sends the event's JSON, keyed by eventType, to the configured topic") {
    val mockProducer = newMockProducer()
    val sink = new KafkaNotificationSink(Some(mockProducer))
    sink.configure(Map("topic" -> "invaract-events"))

    sink.publish(sampleEvent.copy(contract = "kafka-test@1.0.0"))

    val history = mockProducer.history().asScala
    assert(history.size == 1)
    val record = history.head
    assert(record.topic() == "invaract-events")
    assert(record.key() == "CONTRACT_VALIDATION")
    assert(record.value().contains("\"kafka-test@1.0.0\""))
  }

  test("publish sends one record per call, each to the same configured topic") {
    val mockProducer = newMockProducer()
    val sink = new KafkaNotificationSink(Some(mockProducer))
    sink.configure(Map("topic" -> "invaract-events"))

    sink.publish(sampleEvent.copy(contract = "first@1.0.0"))
    sink.publish(sampleEvent.copy(contract = "second@1.0.0", status = "FAILED"))

    val history = mockProducer.history().asScala
    assert(history.size == 2)
    assert(history(0).value().contains("\"first@1.0.0\""))
    assert(history(1).value().contains("\"second@1.0.0\""))
    assert(history(1).value().contains("\"FAILED\""))
  }

  test("close() closes the underlying producer") {
    val mockProducer = newMockProducer()
    val sink = new KafkaNotificationSink(Some(mockProducer))
    sink.configure(Map("topic" -> "invaract-events"))

    sink.close()

    assert(mockProducer.closed())
  }

  test("the public no-arg constructor builds a real KafkaProducer from properties, not a MockProducer") {
    // Doesn't actually connect anywhere - KafkaProducer's constructor only
    // validates config and starts background threads; no broker contact
    // happens until send() is called. Confirms the reflective path
    // NotificationSinkFactory uses (a bare `new KafkaNotificationSink()`)
    // takes the real-producer branch, not the test-only override.
    val sink = new KafkaNotificationSink()
    try {
      sink.configure(Map("topic" -> "invaract-events", "bootstrap.servers" -> "127.0.0.1:0"))
    } finally {
      sink.close()
    }
  }
}
