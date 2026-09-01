// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser
import com.example.sparkadapter.notification.{TestNotificationSink, WriteEvent}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.nio.file.{Files, Path}

/** `SparkAdapterListener`'s `WriteEvent` publishing — a different moment
  * than `ContractEnforcementRuleSpec`'s `ContractValidationEvent` tests:
  * this listener only observes *after* Spark reports a write actually
  * completed (`onSuccess`), not at analysis time. See
  * `com.example.sparkadapter.notification.WriteEvent`'s doc for why that
  * distinction matters.
  */
class SparkAdapterListenerSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-listener-notify-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAdapterListenerSpec")
      // Delta's session extension/catalog only activate for `.format("delta")`
      // usage - confirmed harmless to every other test in this suite by
      // the full suite still passing with this enabled (see
      // ContractEnforcementRuleSpec's own beforeAll for the same pattern).
      // Needed for the rowCount/bytesWritten/fileCount-are-None Delta test
      // below.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

  test("a real write publishes a WriteEvent with location/format/saveMode/schema, once Spark reports success") {
    val outputPath = scratchDir.resolve("listener_write.parquet").toString
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    df.write.mode("overwrite").parquet(outputPath)

    val event = awaitEvent(sink)
    assert(event.location.contains(outputPath))
    assert(event.format.contains("parquet"))
    assert(event.saveMode.contains("overwrite"))
    assert(event.schema.exists(f => f.name == "id" && f.dataType == "long"))
    assert(event.schema.exists(f => f.name == "doubled" && f.dataType == "long"))
    // rowCount/bytesWritten/fileCount come from Spark's own SQLMetrics on
    // the executed plan - confirmed empirically present for this exact
    // write shape (InsertIntoHadoopFsRelationCommand / DataWritingCommandExec).
    assert(event.rowCount.contains(5L), s"expected 5 rows, got ${event.rowCount}")
    assert(event.bytesWritten.exists(_ > 0L), s"expected a positive byte count, got ${event.bytesWritten}")
    assert(event.fileCount.exists(_ > 0L), s"expected a positive file count, got ${event.fileCount}")
    assert(event.durationMs >= 0L)
    assert(event.applicationId.contains(spark.sparkContext.applicationId))
  }

  // Confirmed empirically (see docs/SPARK_ADAPTER.md's "Notification
  // sinks" section): Delta's write shapes never populate Spark's own
  // SQLMetric keys on the executed plan node SparkAdapterListener
  // observes - None is the honest, correct value here, not a bug.
  test("a Delta write publishes a WriteEvent with rowCount/bytesWritten/fileCount all None") {
    val outputPath = scratchDir.resolve("listener_delta_write").toString
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.range(5).write.format("delta").mode("overwrite").save(outputPath)

    val event = awaitEvent(sink)
    assert(event.format.contains("delta"))
    assert(event.rowCount.isEmpty, s"expected None for a Delta write, got ${event.rowCount}")
    assert(event.bytesWritten.isEmpty, s"expected None for a Delta write, got ${event.bytesWritten}")
    assert(event.fileCount.isEmpty, s"expected None for a Delta write, got ${event.fileCount}")
    assert(event.applicationId.contains(spark.sparkContext.applicationId))
  }

  test("no sink configured: onSuccess still captures lastWrite, publishing nothing (no crash either)") {
    val outputPath = scratchDir.resolve("listener_no_sink.parquet").toString
    val listener = new SparkAdapterListener // the original zero-arg constructor
    spark.listenerManager.register(listener)

    val df = spark.range(3)
    df.write.mode("overwrite").parquet(outputPath)

    val translation = eventually(timeout(Span(5, Seconds))) {
      listener.lastWrite.getOrElse(fail("listener has not captured the write yet"))
    }
    assert(translation.plan.isInstanceOf[com.example.ir.Write])
  }

  test("a query that isn't a write (a plain .count()) publishes no WriteEvent") {
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.range(10).count() // triggers onSuccess, but analyzes to no write command

    // Give any (incorrect) async publish a moment to have happened before
    // asserting its absence - eventually's own polling would otherwise
    // pass instantly on an empty buffer regardless of timing.
    Thread.sleep(200)
    assert(sink.events.collect { case e: WriteEvent => e }.isEmpty)
  }

  test("the (sink, contract) convenience constructor derives contract ref and metadata from the Contract") {
    val outputPath = scratchDir.resolve("listener_with_contract.parquet").toString
    val yaml =
      s"""id: listener_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |extensions:
         |  team: data-platform
         |""".stripMargin
    val contract = ContractParser.parse(yaml)
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), Some(contract))
    spark.listenerManager.register(listener)

    spark.range(5).write.mode("overwrite").parquet(outputPath)

    val event = awaitEvent(sink)
    assert(event.contract.contains("listener_demo@1.0.0"))
    assert(event.metadata.get("team").contains("data-platform"))
  }
}
