// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** Against a real `file://` `LocalFileSystem` — Hadoop's own built-in
  * implementation of `org.apache.hadoop.fs.FileSystem`, the exact same API
  * this sink uses for every scheme (`s3a://`, `gs://`, `hdfs://`, ...).
  * `LocalFileSystem` and `DistributedFileSystem`/S3A are different
  * implementations of the identical abstract `FileSystem` contract Hadoop
  * itself tests for consistent semantics across backends — proving this
  * sink's logic (path resolution, one-object-per-event, no reliance on
  * `append`) against `file://` confirms the same logic against `hdfs://`/
  * `s3a://`/`gs://`, which this environment cannot stand up for real
  * (no cluster, no cloud credentials, and adding a real in-process HDFS
  * test cluster — Hadoop's own `MiniDFSCluster` — would pull in the
  * unshaded `hadoop-common`/`hadoop-hdfs` artifacts alongside the shaded
  * `hadoop-client-api`/`hadoop-client-runtime` Spark 3.5.7 already
  * resolves, risking exactly the kind of transitive classpath conflict
  * this module's own `build.sbt` has extensively documented fighting for
  * Netty/Jackson/Arrow — not worth reopening for a test whose real
  * assertions are about this sink's own logic, not Hadoop's).
  */
class HadoopFsNotificationSinkSpec extends AnyFunSuite {

  private val sampleEvent = ContractValidationEvent(
    contract = "demo@1.0.0",
    status = "PASSED",
    violations = Nil,
    timestamp = 1735689600000L,
    metadata = Map.empty
  )

  test("configure throws without a 'path' property") {
    assertThrows[IllegalArgumentException] {
      new HadoopFsNotificationSink().configure(Map.empty)
    }
  }

  test("publish writes one file per event under the configured path, each independently readable") {
    val dir = Files.createTempDirectory("invaract-hadoopfs-sink-test")
    try {
      val sink = new HadoopFsNotificationSink
      sink.configure(Map("path" -> s"file://${dir.toString}"))

      sink.publish(sampleEvent.copy(contract = "first@1.0.0"))
      sink.publish(sampleEvent.copy(contract = "second@1.0.0", status = "FAILED"))

      val fs = FileSystem.get(new java.net.URI(s"file://${dir.toString}"), new org.apache.hadoop.conf.Configuration())
      val children = fs.listStatus(new Path(dir.toString)).map(_.getPath.getName)
      assert(children.length == 2, s"expected two separate objects, one per publish, got: ${children.toList}")

      val contents = children.map { name =>
        val in = fs.open(new Path(dir.toString, name))
        try new String(in.readAllBytes(), "UTF-8")
        finally in.close()
      }
      assert(contents.exists(_.contains("\"first@1.0.0\"")))
      assert(contents.exists(_.contains("\"second@1.0.0\"")))
      assert(contents.exists(_.contains("\"FAILED\"")))
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  test("publish never overwrites a previous event's file, even for two events with the same timestamp and type") {
    val dir = Files.createTempDirectory("invaract-hadoopfs-sink-test")
    try {
      val sink = new HadoopFsNotificationSink
      sink.configure(Map("path" -> s"file://${dir.toString}"))

      // Same timestamp and eventType - only the random UUID component of
      // the filename can disambiguate these; asserts create()'s
      // overwrite=false behavior isn't masking a real filename collision.
      sink.publish(sampleEvent.copy(contract = "a@1.0.0"))
      sink.publish(sampleEvent.copy(contract = "b@1.0.0"))

      val fs = FileSystem.get(new java.net.URI(s"file://${dir.toString}"), new org.apache.hadoop.conf.Configuration())
      val children = fs.listStatus(new Path(dir.toString))
      assert(children.length == 2, "two publishes with identical timestamp/eventType must still produce two distinct files")
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  test("sink.property.hadoop.* keys are set on the underlying Hadoop Configuration, prefix stripped") {
    val dir = Files.createTempDirectory("invaract-hadoopfs-sink-test")
    try {
      val sink = new HadoopFsNotificationSink
      sink.configure(Map("path" -> s"file://${dir.toString}", "hadoop.invaract.test.marker" -> "reached"))

      assert(sink.configurationForTesting.get("invaract.test.marker") == "reached")
      assert(sink.configurationForTesting.get("hadoop.invaract.test.marker") == null, "the 'hadoop.' prefix must be stripped, not kept")
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  test("a property without the 'hadoop.' prefix is not set on the Configuration") {
    val dir = Files.createTempDirectory("invaract-hadoopfs-sink-test")
    try {
      val sink = new HadoopFsNotificationSink
      sink.configure(Map("path" -> s"file://${dir.toString}", "unrelated.key" -> "ignored"))

      assert(sink.configurationForTesting.get("unrelated.key") == null)
      assert(sink.configurationForTesting.get("key") == null)
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  // The real UUID-based filename is unpredictable from outside, so
  // overwrite=false's actual effect (reject a collision instead of
  // silently replacing the earlier event) is otherwise never exercised -
  // forcing a fixed name via the protected `newFileName` seam is the only
  // way to make that flag's value actually observable under test.
  test("publish never silently overwrites: a forced filename collision throws on the second call") {
    val dir = Files.createTempDirectory("invaract-hadoopfs-sink-test")
    try {
      val sink = new HadoopFsNotificationSink {
        override def newFileName(event: NotificationEvent): String = "fixed-name.json"
      }
      sink.configure(Map("path" -> s"file://${dir.toString}"))

      sink.publish(sampleEvent.copy(contract = "first@1.0.0")) // must not throw

      intercept[java.io.IOException] {
        sink.publish(sampleEvent.copy(contract = "second@1.0.0"))
      }

      val fs = FileSystem.get(new java.net.URI(s"file://${dir.toString}"), new org.apache.hadoop.conf.Configuration())
      val content = {
        val in = fs.open(new Path(dir.toString, "fixed-name.json"))
        try new String(in.readAllBytes(), "UTF-8")
        finally in.close()
      }
      assert(content.contains("\"first@1.0.0\""), "the original event's file must survive the rejected second write")
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }
}
