// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.invaract.verification.notification.{NotificationEvent, NotificationJson, NotificationSink}

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
