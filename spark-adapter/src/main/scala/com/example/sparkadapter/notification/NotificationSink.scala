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

/** POSTs every event, as JSON, to an HTTP endpoint — configured with a
  * required `url` property (`sink.property.url=...`) and an optional
  * `timeoutMs` property (`sink.property.timeoutMs=...`, default `5000`)
  * bounding how long a single request may run. Built on
  * `java.net.http.HttpClient`, part of the JDK since Java 11 — this needs
  * no new dependency, the same reasoning `FileNotificationSink` and
  * `NotificationConfig` already give for staying dependency-free.
  *
  * Requests are sent via `HttpClient.sendAsync`, not blocking `publish`'s
  * caller on the network round-trip: `NotificationSink.publish`'s own doc
  * already warns that a slow implementation adds real latency to whatever
  * thread called it (Spark's analysis path for a `ContractValidationEvent`,
  * its listener thread for a `WriteEvent`), and this sink's entire purpose
  * is calling an external service whose latency is out of this module's
  * control. A connection failure or non-2xx response is logged at WARN
  * once the async response arrives — by then there is no longer a call
  * stack for `SafeNotificationSink` to catch an exception on, so this sink
  * does its own equivalent logging for the async case.
  */
class HttpNotificationSink extends NotificationSink {
  private val logger = LoggerFactory.getLogger(classOf[HttpNotificationSink])
  private var url: String = _
  private var timeout: java.time.Duration = _
  private var client: java.net.http.HttpClient = _

  override def configure(properties: Map[String, String]): Unit = {
    url = properties.getOrElse(
      "url",
      throw new IllegalArgumentException("HttpNotificationSink requires a 'url' property (sink.property.url=...)")
    )
    val timeoutMs = properties.get("timeoutMs") match {
      case Some(v) =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"HttpNotificationSink's 'timeoutMs' property must be a number, got '$v'")
        }
      case None => 5000L
    }
    timeout = java.time.Duration.ofMillis(timeoutMs)
    client = java.net.http.HttpClient.newBuilder().connectTimeout(timeout).build()
  }

  override def publish(event: NotificationEvent): Unit = {
    val request = java.net.http.HttpRequest
      .newBuilder()
      .uri(java.net.URI.create(url))
      .timeout(timeout)
      .header("Content-Type", "application/json")
      .POST(java.net.http.HttpRequest.BodyPublishers.ofString(NotificationJson.toJson(event)))
      .build()

    client
      .sendAsync(request, java.net.http.HttpResponse.BodyHandlers.discarding())
      .whenComplete { (response, throwable) =>
        // A null throwable is a legal, documented SafeLogger#warn(String,
        // Throwable) argument (it just omits a stack trace) - passing it
        // through unconditionally, rather than branching on it here too,
        // removes an untested duplicate of failureMessage's own
        // throwable != null check instead of needing a second test to
        // cover it.
        HttpNotificationSink.failureMessage(throwable, response.statusCode(), event.eventType, url).foreach { msg =>
          logger.warn(msg, throwable)
        }
      }
  }
}

private[notification] object HttpNotificationSink {

  /** The decision of *whether* (and what) to log, pulled out of `publish`'s
    * async callback so it's directly unit-testable: a mutation on the
    * status-code/throwable branches there is a real behavioral bug (log a
    * genuine failure, or don't), not a `StringLiteral` mutant on message
    * text, so CLAUDE.md's mutation-testing bar requires it actually be
    * killed — asserting only "no exception, no crash" (the shape every
    * other async-callback test here was limited to before this existed)
    * never observes which branch ran. `statusCode` is by-name specifically
    * so a test can pass a literal that would blow up if evaluated (proving
    * the `throwable != null` branch never touches it, the same short-
    * circuit `publish`'s own callback relies on when the response is
    * `null` on failure).
    */
  private[notification] def failureMessage(throwable: Throwable, statusCode: => Int, eventType: String, url: String): Option[String] =
    if (throwable != null) {
      Some(s"HttpNotificationSink failed to publish a $eventType event to $url")
    } else if (statusCode / 100 != 2) {
      Some(s"HttpNotificationSink got HTTP $statusCode publishing a $eventType event to $url")
    } else {
      None
    }
}

/** Writes every event, as its own JSON object, under a configured path
  * prefix on any filesystem Hadoop's `FileSystem` API supports — `s3a://`
  * for S3, `gs://` for GCS (with that connector installed), `hdfs://`,
  * `abfs://` for Azure, or plain `file://`. One sink for all of them,
  * not one per vendor: this is exactly the same abstraction Spark's own
  * `DataFrameWriter` already dispatches through — the scheme picks the
  * concrete implementation at runtime via Hadoop's own configuration-driven
  * lookup, not compile-time linking to a vendor SDK.
  *
  * Needs no new dependency: `org.apache.hadoop.fs.{FileSystem, Path}` are
  * part of `hadoop-common`, already transitively present via this
  * module's existing `provided` `spark-core`/`spark-sql` dependencies. It
  * also needs no new dependency for a real *user* of this sink, for a
  * structural reason, not a coincidence — if a contract's own declared
  * `location:` already points at `s3a://.../gs://...`, the job's runtime
  * classpath already carries `hadoop-aws`/the GCS connector for that write
  * to work at all, and this sink piggybacks on exactly that, the same way
  * `FileNotificationSink` piggybacks on `java.io.FileWriter` already being
  * part of the JDK.
  *
  * Configuration: `sink.property.path` (required) is the destination
  * prefix — treated as a directory, not a single file. Every *other*
  * `sink.property.hadoop.<key>` is set on the `Configuration` this sink
  * builds (`sink.property.hadoop.fs.s3a.access.key`, etc.) — Hadoop's own
  * configuration keys, not a second vocabulary. Credentials/endpoint
  * config a real job already has in `core-site.xml` (or set on the
  * `SparkContext`'s own `hadoopConfiguration`, which this sink cannot see —
  * it only has whatever `sink.property.hadoop.*` supplies plus whatever
  * `core-site.xml`/`hdfs-site.xml` are visible on the classpath) apply the
  * normal Hadoop way; `sink.property.hadoop.*` is for overriding or
  * supplying config this sink specifically needs that isn't already
  * ambient.
  *
  * One file per event, not an appended log: `FileSystem.append` is not
  * reliably supported across implementations — S3A in particular has never
  * supported real append (S3 objects are immutable), so an append-based
  * design that works for `FileNotificationSink`'s local files would
  * silently misbehave or throw the moment `sink.property.path` pointed at
  * `s3a://`. Writing each event as its own object under the configured
  * prefix, named to avoid collisions, needs nothing more than `create`,
  * universally supported.
  */
class HadoopFsNotificationSink extends NotificationSink {
  private var basePath: org.apache.hadoop.fs.Path = _
  private var conf: org.apache.hadoop.conf.Configuration = _

  override def configure(properties: Map[String, String]): Unit = {
    val rawPath = properties.getOrElse(
      "path",
      throw new IllegalArgumentException("HadoopFsNotificationSink requires a 'path' property (sink.property.path=...)")
    )
    conf = new org.apache.hadoop.conf.Configuration()
    val hadoopPrefix = "hadoop."
    properties.foreach {
      case (k, v) if k.startsWith(hadoopPrefix) => conf.set(k.stripPrefix(hadoopPrefix), v)
      case _ => ()
    }
    basePath = new org.apache.hadoop.fs.Path(rawPath)
  }

  /** Exposed only so `HadoopFsNotificationSinkSpec` can assert the
    * `sink.property.hadoop.*` passthrough actually reached the
    * `Configuration` this sink builds, rather than only asserting the
    * absence of a crash — not part of `NotificationSink`.
    */
  private[notification] def configurationForTesting: org.apache.hadoop.conf.Configuration = conf

  /** Pulled out so a test can force a filename collision (by overriding
    * this in an anonymous subclass to return a fixed name) and observe
    * that the second `publish` throws rather than silently replacing the
    * first event — the only way to make `create`'s `overwrite = false`
    * argument actually matter under test, since the real UUID-based name
    * below is deliberately unpredictable from outside.
    */
  protected def newFileName(event: NotificationEvent): String =
    s"${event.timestamp}-${event.eventType}-${java.util.UUID.randomUUID()}.json"

  override def publish(event: NotificationEvent): Unit = {
    val fs = basePath.getFileSystem(conf)
    val target = new org.apache.hadoop.fs.Path(basePath, newFileName(event))
    val out = fs.create(target, /* overwrite = */ false)
    try {
      out.write(NotificationJson.toJson(event).getBytes("UTF-8"))
    } finally {
      out.close()
    }
  }
}

/** Wraps a sink to also tally `WriteEvent`/`ContractValidationEvent` traffic
  * and, on demand via `publishSummary()`, publish one `JobSummaryEvent`
  * aggregating everything seen since construction (or since the previous
  * `publishSummary()` call — each call atomically reads and resets every
  * counter, so a second call reports only what happened *since* the first,
  * not a running cumulative total). This makes the same sink work for a
  * batch job (call `publishSummary()` once, right before the job ends) and
  * a long-running one (call it periodically for a rolling summary).
  *
  * Deliberately a decorator around an ordinary `NotificationSink`, not a
  * built-in behavior of `ContractEnforcementRule`/`SparkAdapterListener`
  * themselves: they publish `ContractValidationEvent`/`WriteEvent` as each
  * one happens and have no natural "the job is over" moment of their own
  * to hook — the caller wiring a sink into their job already knows exactly
  * when that is (e.g. `DemoJobHarness` calling `publishSummary()` right
  * before `spark.stop()`), so it's cheaper and more flexible to let them
  * decide than to guess at a Spark lifecycle hook. Construct exactly one
  * instance and pass it as the `sink` to *both*
  * `ContractEnforcementRule.forContract` and `new SparkAdapterListener` -
  * two separate instances would each only see half the job's events.
  *
  * `applicationId`/`metadata` on the published summary come from whichever
  * event most recently supplied a non-empty value - `orElse`/plain
  * reassignment, not `getOrElse`, so an event that happens not to carry an
  * `applicationId` (there is no such real code path today, but nothing
  * enforces there never will be) doesn't blank out an already-known one.
  */
class SummarizingNotificationSink(delegate: NotificationSink) extends NotificationSink {
  private val writesPublished = new java.util.concurrent.atomic.AtomicLong(0L)
  private val checksPassed = new java.util.concurrent.atomic.AtomicLong(0L)
  private val checksFailed = new java.util.concurrent.atomic.AtomicLong(0L)
  private val violationsTotal = new java.util.concurrent.atomic.AtomicLong(0L)
  private val periodStart = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis())
  @volatile private var lastApplicationId: Option[String] = None
  @volatile private var lastMetadata: Map[String, Any] = Map.empty

  override def configure(properties: Map[String, String]): Unit = delegate.configure(properties)

  override def publish(event: NotificationEvent): Unit = {
    event match {
      case e: WriteEvent =>
        writesPublished.incrementAndGet()
        lastApplicationId = e.applicationId.orElse(lastApplicationId)
        lastMetadata = e.metadata
      case e: ContractValidationEvent =>
        if (e.status == "PASSED") checksPassed.incrementAndGet() else checksFailed.incrementAndGet()
        violationsTotal.addAndGet(e.violations.size.toLong)
        lastApplicationId = e.applicationId.orElse(lastApplicationId)
        lastMetadata = e.metadata
      case _: JobSummaryEvent => () // a summary isn't itself summarized
    }
    delegate.publish(event)
  }

  /** Builds and publishes one `JobSummaryEvent` from everything tallied
    * since construction or the last call, then resets every counter -
    * see this class's own doc for why that reset makes both a
    * once-per-job and a periodic call pattern work with the same sink.
    */
  def publishSummary(): Unit = {
    val now = System.currentTimeMillis()
    delegate.publish(
      JobSummaryEvent(
        totalWrites = writesPublished.getAndSet(0L),
        checksPassed = checksPassed.getAndSet(0L),
        checksFailed = checksFailed.getAndSet(0L),
        totalViolations = violationsTotal.getAndSet(0L),
        durationMs = now - periodStart.getAndSet(now),
        timestamp = now,
        metadata = lastMetadata,
        applicationId = lastApplicationId
      )
    )
  }
}

/** Wraps a sink to forward only the events a "just tell me when something's
  * wrong" consumer actually wants: a `ContractValidationEvent` whose
  * `status` isn't `"PASSED"`, and every `JobSummaryEvent` (a single
  * once-per-job message is never noise, whatever it says). Every
  * `WriteEvent` and every PASSED `ContractValidationEvent` is dropped
  * without reaching the delegate at all.
  *
  * For a team that wants PagerDuty or an on-call Slack channel to fire only
  * on real problems, without drowning it in the routine-success traffic a
  * job with many writes and checks otherwise produces. Compose it with any
  * other sink: `new FailureOnlyNotificationSink(new HttpNotificationSink)`,
  * or wrap a `SummarizingNotificationSink` the same way if only the
  * end-of-job summary (not every individual failure) should reach a given
  * destination.
  */
class FailureOnlyNotificationSink(delegate: NotificationSink) extends NotificationSink {
  override def configure(properties: Map[String, String]): Unit = delegate.configure(properties)

  override def publish(event: NotificationEvent): Unit = event match {
    case e: ContractValidationEvent => if (e.status != "PASSED") delegate.publish(e)
    case e: JobSummaryEvent => delegate.publish(e)
    case _: WriteEvent => ()
  }
}
