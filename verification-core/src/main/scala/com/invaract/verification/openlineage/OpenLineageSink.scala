// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.notification.{NotificationEvent, NotificationSink}

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** Publishes what Invaract verified as OpenLineage run events (docs/OPENLINEAGE.md).
  *
  * Built by `AttachSetup` from the neutral settings (`openLineageUrl`, `openLineageConfig`, `openLineageNamespace`), so
  * every adapter publishes OpenLineage with no code of its own. It can also be named as a notification sink class
  * (`sink.class=com.invaract.verification.openlineage.OpenLineageSink`), its `sink.property.*` being the keys of the
  * OpenLineage configuration file.
  *
  * `publish` only hands the event to the worker (`OpenLineageDelivery`), so it costs the job one queue `offer`. The process
  * end closes the runs still open and delivers what is queued, waiting at most `shutdownTimeoutMs`.
  */
final class OpenLineageSink private[openlineage] (settings0: Option[OpenLineageSettings]) extends NotificationSink {
  import OpenLineageSink._

  /** For `NotificationSinkFactory`, which builds sinks by class name and then calls `configure`. */
  def this() = this(None)

  def this(settings: OpenLineageSettings) = this(Some(settings))

  @volatile private var delivery: Option[OpenLineageDelivery] = settings0.map(start)

  override def configure(properties: Map[String, String]): Unit =
    if (delivery.isEmpty) delivery = Some(start(OpenLineageSettings.parse(properties, None, None, "sink.property.*", env)))

  override def publish(event: NotificationEvent): Unit = delivery.foreach(_.submit(event))

  /** Waits (at most `timeoutMs`) for what is queued to be delivered. Runs stay open: `close` ends them. */
  override def flush(timeoutMs: Long): Unit = delivery.foreach(_.flush(timeoutMs))

  /** Ends every open run and delivers what is queued, at most `timeoutMs`; publishes nothing after. */
  def close(timeoutMs: Long): Unit = delivery.foreach(_.close(timeoutMs))

  private[openlineage] def stats: Option[DeliveryStats] = delivery.map(_.stats)
}

object OpenLineageSink {
  private val logger = LoggerFactory.getLogger(classOf[OpenLineageSink])

  private def env(name: String): Option[String] = Option(System.getenv(name))

  private def start(settings: OpenLineageSettings): OpenLineageDelivery = {
    val delivery = new OpenLineageDelivery(
      settings,
      Transport(settings.transport),
      new OpenLineageMapper(settings),
      settings.deadLetterPath.map(p => new FileTransport(p))
    )
    val hook = new Thread(() => delivery.close(settings.shutdownTimeoutMs), "invaract-openlineage-shutdown")
    try Runtime.getRuntime.addShutdownHook(hook)
    catch { case NonFatal(e) => logger.debug(s"OpenLineage: no shutdown hook ($e); runs still open when the process ends are not closed") }
    logger.info(s"Invaract publishes OpenLineage events to ${settings.transport} (namespace '${settings.namespace}')")
    delivery
  }
}
