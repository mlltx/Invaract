// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification.kafka

import com.example.sparkadapter.notification.{NotificationEvent, NotificationJson, NotificationSink}

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties

/** Publishes every event, as JSON keyed by its `eventType`, to a Kafka
  * topic. Ships as its own module/artifact (`invaract-notification-kafka`),
  * not part of `spark-adapter` itself — see this module's `build.sbt` for
  * why: `kafka-clients` is a real, sizeable dependency most users of the
  * verification engine don't need, so it stays out of `spark-adapter`'s
  * own dependency footprint entirely. A user who wants this sink adds
  * this module's assembled jar to their classpath and points
  * `sink.class` at `com.example.sparkadapter.notification.kafka.KafkaNotificationSink`
  * (the public no-arg constructor `NotificationSinkFactory` needs); a user
  * who doesn't never resolves `kafka-clients` at all.
  *
  * Configuration: `sink.property.topic` (required) names the destination
  * topic. Every *other* `sink.property.*` key is passed straight through
  * as a Kafka producer config (`sink.property.bootstrap.servers`,
  * `sink.property.security.protocol`, `sink.property.sasl.jaas.config`,
  * ... — exactly Kafka's own producer configuration keys, documented at
  * https://kafka.apache.org/documentation/#producerconfigs), rather than
  * this sink inventing and maintaining a second, parallel vocabulary for
  * settings Kafka already has names for. Key/value serializers are always
  * `StringSerializer` — the event is always published as its JSON
  * rendering, never left to the caller to choose.
  */
class KafkaNotificationSink private[kafka] (producerOverride: Option[Producer[String, String]]) extends NotificationSink {

  /** The public no-arg constructor `NotificationSinkFactory` requires. */
  def this() = this(None)

  private val logger = LoggerFactory.getLogger(classOf[KafkaNotificationSink])
  private var producer: Producer[String, String] = producerOverride.orNull
  private var topic: String = _

  override def configure(properties: Map[String, String]): Unit = {
    topic = properties.getOrElse(
      "topic",
      throw new IllegalArgumentException("KafkaNotificationSink requires a 'topic' property (sink.property.topic=...)")
    )
    // producerOverride is only ever set by the private[kafka] constructor
    // KafkaNotificationSinkSpec uses to substitute a real MockProducer
    // (Kafka's own test double, not a hand-rolled mock) - the public path
    // (NotificationSinkFactory, via the no-arg constructor) always builds
    // a real KafkaProducer here.
    if (producerOverride.isEmpty) {
      val producerProps = new Properties()
      (properties - "topic").foreach { case (k, v) => producerProps.put(k, v) }
      producerProps.put("key.serializer", classOf[StringSerializer].getName)
      producerProps.put("value.serializer", classOf[StringSerializer].getName)
      producer = new KafkaProducer[String, String](producerProps)
    }
  }

  override def publish(event: NotificationEvent): Unit = {
    val record = new ProducerRecord[String, String](topic, event.eventType, NotificationJson.toJson(event))
    // Fire-and-forget: NotificationSink.publish's own doc already warns
    // that a slow implementation adds real latency to whatever thread
    // called it, and this sink's whole purpose is talking to an external
    // broker whose latency is out of this module's control. A send
    // failure is delivered to the callback asynchronously, by which point
    // there's no longer a call stack for SafeNotificationSink to catch an
    // exception on - so this sink does its own equivalent logging for
    // that case, the same pattern HttpNotificationSink uses for its own
    // async response handling.
    producer.send(
      record,
      (_, exception) =>
        if (exception != null) {
          logger.warn(s"KafkaNotificationSink failed to publish a ${event.eventType} event to topic '$topic'", exception)
        }
    )
  }

  /** Not part of `NotificationSink` (no `close()` hook exists there — see
    * that trait's doc for why: most sinks, like `FileNotificationSink`,
    * need no persistent resource to release). Exposed so a caller holding
    * this sink directly — bypassing `NotificationSinkFactory`, e.g. a test
    * or a long-lived application shutting down cleanly — can release the
    * underlying producer's threads/connections deterministically rather
    * than relying on JVM exit.
    */
  def close(): Unit = if (producer != null) producer.close()
}
