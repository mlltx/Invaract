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

  /** Like `awaitEvent`, but for a row-level DML (MERGE/UPDATE/DELETE)
    * assertion specifically: `sink` is created and registered right after
    * this test's own `writeTo(...).append()` setup call, but that setup
    * write's own `WriteEvent` (`operation = None`, `saveMode =
    * Some("append")`) can - confirmed empirically, the hard way, via a
    * real flaky test failure, not assumed - still arrive at the
    * newly-registered listener *after* registration, racing with the DML
    * statement that follows: Iceberg's own write-completion notification
    * isn't guaranteed to have fully reached every registered
    * `QueryExecutionListener` by the time `.append()` returns to the
    * calling thread. A plain "first `WriteEvent` in the sink" assertion
    * (`awaitEvent`) is therefore order-dependent and intermittently picks
    * up that unrelated setup event instead of the DML's own - filtering on
    * `operation.isDefined` (true only for row-level DML, never for a
    * plain append) sidesteps the race entirely rather than depending on
    * arrival order.
    */
  private def awaitOperationEvent(sink: TestNotificationSink): WriteEvent =
    eventually(timeout(Span(5, Seconds))) {
      sink.events
        .collectFirst { case e: WriteEvent if e.operation.isDefined => e }
        .getOrElse(fail("listener has not published a row-level-DML WriteEvent yet"))
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
    assert(event.operation.isEmpty, s"expected no operation for a plain append, got ${event.operation}")
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

  test("an Iceberg MERGE INTO publishes a WriteEvent with operation = Some(\"merge\")") {
    val tableName = "local.db.listener_merge_tbl"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", org.apache.spark.sql.functions.col("id") * 2).writeTo(tableName).append()

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark
      .sql(s"""MERGE INTO $tableName t
              |USING (SELECT 99L as id, 198L as doubled) s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET *
              |WHEN NOT MATCHED THEN INSERT *
              |""".stripMargin)
      .collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("merge"), s"expected operation 'merge', got ${event.operation}")
    assert(event.saveMode.isEmpty, "in-place mutation isn't append/overwrite/ignore/error")
  }

  test("an Iceberg UPDATE publishes a WriteEvent with operation = Some(\"update\")") {
    val tableName = "local.db.listener_update_tbl"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", org.apache.spark.sql.functions.col("id") * 2).writeTo(tableName).append()

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("update"), s"expected operation 'update', got ${event.operation}")
  }

  test("an Iceberg DELETE publishes a WriteEvent with operation = Some(\"delete\")") {
    val tableName = "local.db.listener_delete_tbl"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", org.apache.spark.sql.functions.col("id") * 2).writeTo(tableName).append()

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("delete"), s"expected operation 'delete', got ${event.operation}")
  }
}
