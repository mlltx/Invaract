// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

import org.slf4j.LoggerFactory

/** A destination for `NotificationEvent`s. Implementations are looked up by
  * fully-qualified class name (see `NotificationConfig`/
  * `NotificationSinkFactory`) and must have a public no-arg constructor;
  * `configure` then receives whatever `sink.property.*` keys the
  * configuration file declared, before any event is ever published.
  *
  * A custom sink (e.g. one that calls out to a message queue or an HTTP
  * endpoint) is ordinary user code outside this module — nothing here
  * requires it to live in `com.example.sparkadapter`.
  */
trait NotificationSink {

  /** Called exactly once, right after construction, with this sink's
    * `sink.property.*` configuration (key with that prefix stripped — see
    * `NotificationConfig`). The default no-op suits a sink that needs no
    * configuration at all (`LoggingNotificationSink`).
    */
  def configure(properties: Map[String, String]): Unit = ()

  /** Publish one event. Called synchronously, on whatever thread the
    * engine itself runs on (Spark's analysis path for
    * `ContractValidationEvent`, Spark's listener thread for `WriteEvent`)
    * — a slow implementation adds real latency there. Any exception this
    * throws is caught and logged by the `SafeNotificationSink` wrapper
    * `NotificationSinkFactory.create` always returns, never allowed to
    * abort the write or check that triggered it — implementations don't
    * need their own defensive try/catch for that reason, though they may
    * still want one for their own diagnostics.
    */
  def publish(event: NotificationEvent): Unit
}

/** Wraps a sink so a broken or slow `publish` call (a network sink timing
  * out, an implementation bug) can never turn an otherwise-successful
  * contract check or write into a job failure — notification is a
  * best-effort side channel, not part of enforcement. Every
  * `NotificationSinkFactory.create` result is already wrapped in one of
  * these; a caller constructing a `NotificationSink` directly (e.g. in a
  * test) gets no such protection unless it wraps it itself.
  */
private[sparkadapter] class SafeNotificationSink(delegate: NotificationSink) extends NotificationSink {
  private val logger = LoggerFactory.getLogger(classOf[SafeNotificationSink])

  override def configure(properties: Map[String, String]): Unit = delegate.configure(properties)

  override def publish(event: NotificationEvent): Unit =
    try {
      delegate.publish(event)
    } catch {
      case e: Exception =>
        logger.warn(
          s"Notification sink ${delegate.getClass.getName} failed to publish a ${event.eventType} event; continuing without it.",
          e
        )
    }
}

/** Logs every event, at INFO, as a single JSON line via SLF4J — the
  * simplest possible sink, useful as a default/demonstration and for
  * environments where the enclosing application already aggregates its own
  * logs (e.g. into a log-shipping pipeline) rather than wanting a second,
  * separate delivery mechanism.
  */
class LoggingNotificationSink extends NotificationSink {
  private val logger = LoggerFactory.getLogger(classOf[LoggingNotificationSink])

  override def publish(event: NotificationEvent): Unit =
    logger.info(NotificationJson.toJson(event))
}

/** Appends every event, as a single JSON line, to a file — configured with
  * a required `path` property (i.e. `sink.property.path=...` in the
  * configuration file). Intended for local development, testing (this is
  * what `./dev/test`/`./dev/regression` use to prove real events are
  * published against a real Spark job — see `demo/notify.properties`), or
  * any environment already tailing a well-known file into wherever events
  * should ultimately land.
  *
  * Opens and closes the file on every `publish` call rather than holding a
  * writer open: this sink is invoked at most a handful of times per job
  * (once per checked write, once per completed write), so the per-call
  * open/close overhead is immaterial, and this avoids needing any
  * lifecycle hook (a `close()` this trait doesn't define) to flush a
  * held-open writer before the JVM exits.
  */
class FileNotificationSink extends NotificationSink {
  private var path: String = _

  override def configure(properties: Map[String, String]): Unit =
    path = properties.getOrElse(
      "path",
      throw new IllegalArgumentException("FileNotificationSink requires a 'path' property (sink.property.path=...)")
    )

  override def publish(event: NotificationEvent): Unit = {
    val writer = new java.io.FileWriter(path, /* append = */ true)
    try {
      writer.write(NotificationJson.toJson(event))
      writer.write("\n")
    } finally {
      writer.close()
    }
  }
}
