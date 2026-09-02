// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.sparkadapter.notification.{TestNotificationSink, WriteEvent}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.nio.file.{Files, Path}

/** `SparkAdapterListener`'s `icebergSnapshotId` publishing - kept in its own
  * file/session rather than folded into `SparkAdapterListenerSpec`, the
  * same separation `IcebergConnectorSpec`/`ContractEnforcementRuleSpec`
  * already use: Iceberg's own `spark.sql.extensions`/catalog configuration
  * can't coexist in one session with Delta's.
  */
class SparkAdapterListenerIcebergSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-listener-iceberg-notify-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAdapterListenerIcebergSpec")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def awaitEvent(sink: TestNotificationSink): WriteEvent =
    eventually(timeout(Span(5, Seconds))) {
      sink.events.collectFirst { case e: WriteEvent => e }.getOrElse(fail("listener has not published a WriteEvent yet"))
    }

  test("a real Iceberg write publishes a WriteEvent with a populated icebergSnapshotId and no deltaVersion") {
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("CREATE TABLE local.db.listener_probe_tbl (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO local.db.listener_probe_tbl VALUES (1), (2), (3)")

    val event = awaitEvent(sink)
    assert(event.format.contains("iceberg"))
    assert(event.icebergSnapshotId.isDefined, "expected a real Iceberg snapshot ID")
    assert(event.deltaVersion.isEmpty, s"expected no deltaVersion for an Iceberg write, got ${event.deltaVersion}")

    // Ground truth: the snapshot ID this listener reported must match the
    // table's own real, independently-loaded current snapshot - not just
    // "some Long," but the actual committed one.
    val trueSnapshotId = spark
      .sql("SELECT snapshot_id FROM local.db.listener_probe_tbl.snapshots ORDER BY committed_at DESC LIMIT 1")
      .collect()
      .head
      .getLong(0)
    assert(event.icebergSnapshotId.contains(trueSnapshotId), s"expected snapshot $trueSnapshotId, got ${event.icebergSnapshotId}")
  }

  test("a second Iceberg write to the same table reports a different (newer) snapshot ID") {
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("CREATE TABLE local.db.listener_probe_tbl_2 (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO local.db.listener_probe_tbl_2 VALUES (1)")
    val firstEvent = awaitEvent(sink)

    spark.sql("INSERT INTO local.db.listener_probe_tbl_2 VALUES (2)")
    val secondEvent = eventually(timeout(Span(5, Seconds))) {
      sink.events.collect { case e: WriteEvent => e }.lastOption.filter(_ ne firstEvent).getOrElse(fail("second WriteEvent not published yet"))
    }

    assert(firstEvent.icebergSnapshotId.isDefined)
    assert(secondEvent.icebergSnapshotId.isDefined)
    assert(
      secondEvent.icebergSnapshotId != firstEvent.icebergSnapshotId,
      "a second, independent commit must report a different snapshot ID, not a stale/cached one"
    )
  }
}
