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

  /** Like `awaitEvent`, but for a row-level DML (MERGE/UPDATE/DELETE)
    * assertion specifically: `sink` is created and registered right after
    * this test's own setup `.write.format("delta")...save(...)` call, but
    * that setup write's own `WriteEvent` (`operation = None`, `saveMode =
    * Some("overwrite")`) can - confirmed empirically, the hard way, via a
    * real flaky failure on the equivalent Iceberg tests in
    * `SparkAdapterListenerIcebergSpec` - still arrive at the
    * newly-registered listener *after* registration, racing with the DML
    * statement that follows: a write's completion notification isn't
    * guaranteed to have fully reached every registered
    * `QueryExecutionListener` by the time the write call returns to the
    * calling thread. A plain "first `WriteEvent` in the sink" assertion
    * (`awaitEvent`) is therefore order-dependent and could intermittently
    * pick up that unrelated setup event instead of the DML's own -
    * filtering on `operation.isDefined` (true only for row-level DML,
    * never for a plain overwrite) sidesteps the race entirely rather than
    * depending on arrival order.
    */
  private def awaitOperationEvent(sink: TestNotificationSink): WriteEvent =
    eventually(timeout(Span(5, Seconds))) {
      sink.events
        .collectFirst { case e: WriteEvent if e.operation.isDefined => e }
        .getOrElse(fail("listener has not published a row-level-DML WriteEvent yet"))
    }

  test("a real write publishes a WriteEvent with location/format/saveMode/schema, once Spark reports success") {
    val outputPath = scratchDir.resolve("listener_write.parquet").toString
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    df.write.mode("overwrite").parquet(outputPath)

    val event = awaitEvent(sink)
    // event.location is Spark's own resolved file: URI, which always uses
    // forward slashes (e.g. "file:/C:/Users/.../listener_write.parquet" on
    // Windows) - outputPath, built from java.nio.file.Path.toString, uses
    // the platform's native separator (backslashes on Windows). Normalizing
    // before comparing avoids a real, confirmed Windows CI failure (a
    // false negative, not a translation bug) rather than asserting on
    // OS-specific string formatting.
    assert(event.location.contains(outputPath.replace('\\', '/')))
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
    // Neither connector-specific field applies to a plain Parquet write.
    assert(event.deltaVersion.isEmpty, s"expected no deltaVersion for a Parquet write, got ${event.deltaVersion}")
    assert(event.icebergSnapshotId.isEmpty, s"expected no icebergSnapshotId for a Parquet write, got ${event.icebergSnapshotId}")
    // A plain append already has its operation conveyed by saveMode -
    // operation is reserved for the three shapes where saveMode is None.
    assert(event.operation.isEmpty, s"expected no operation for a plain append, got ${event.operation}")
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
    // The first-ever write to a brand new Delta table always commits as
    // version 0 - confirmed empirically (see SparkAdapterListener's own
    // doc) that this plain, unforced read already reflects the
    // just-committed version at this exact callback.
    assert(event.deltaVersion.contains(0L), s"expected deltaVersion 0 for the first write to a new Delta table, got ${event.deltaVersion}")
    assert(event.icebergSnapshotId.isEmpty, s"expected no icebergSnapshotId for a Delta write, got ${event.icebergSnapshotId}")
  }

  test("a second Delta write to the same table reports an incremented deltaVersion, not a stale/constant one") {
    val outputPath = scratchDir.resolve("listener_delta_second_write").toString
    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.range(5).write.format("delta").mode("overwrite").save(outputPath)
    val firstEvent = awaitEvent(sink)
    assert(firstEvent.deltaVersion.contains(0L))

    spark.range(5, 10).write.format("delta").mode("append").save(outputPath)
    val secondEvent = eventually(timeout(Span(5, Seconds))) {
      sink.events.collect { case e: WriteEvent => e }.lastOption.filter(_ ne firstEvent).getOrElse(fail("second WriteEvent not published yet"))
    }
    assert(secondEvent.deltaVersion.contains(1L), s"expected deltaVersion 1 after a second write, got ${secondEvent.deltaVersion}")
  }

  test("a Delta MERGE INTO publishes a WriteEvent with operation = Some(\"merge\")") {
    val tablePath = scratchDir.resolve("listener_delta_merge").toString
    val tableName = "listener_merge_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark
      .sql(s"""MERGE INTO $tableName t
              |USING (SELECT 99L as id, 198L as doubled) s
              |ON t.id = s.id
              |WHEN NOT MATCHED THEN INSERT *
              |""".stripMargin)
      .collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("merge"), s"expected operation 'merge', got ${event.operation}")
    assert(event.saveMode.isEmpty, "in-place mutation isn't append/overwrite/ignore/error")
  }

  test("a Delta UPDATE publishes a WriteEvent with operation = Some(\"update\")") {
    val tablePath = scratchDir.resolve("listener_delta_update").toString
    val tableName = "listener_update_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("update"), s"expected operation 'update', got ${event.operation}")
  }

  test("a Delta DELETE publishes a WriteEvent with operation = Some(\"delete\")") {
    val tablePath = scratchDir.resolve("listener_delta_delete").toString
    val tableName = "listener_delete_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val sink = new TestNotificationSink
    val listener = new SparkAdapterListener(Some(sink), None, Map.empty)
    spark.listenerManager.register(listener)

    spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect()

    val event = awaitOperationEvent(sink)
    assert(event.operation.contains("delete"), s"expected operation 'delete', got ${event.operation}")
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

  test("SparkAdapterListener.deltaVersionOf returns None (not a thrown exception) for an unparseable location") {
    // new org.apache.hadoop.fs.Path("") throws IllegalArgumentException
    // ("Can not create a Path from an empty string") - a real, reachable
    // failure this reflective lookup's Try must swallow, not just a
    // ClassNotFoundException from Delta being absent (which the write
    // tests above can't exercise, since Delta genuinely is on this
    // module's test classpath).
    assert(SparkAdapterListener.deltaVersionOf(spark, "").isEmpty)
  }

  test("SparkAdapterListener.icebergSnapshotIdOfTable's SparkTable guard actually prevents reflection from being attempted at all") {
    // A plain "returns None" assertion alone can't tell "the guard rejected
    // this" apart from "reflection was attempted and merely failed/threw" -
    // both produce None. This Table has a table() method shaped just like
    // SparkTable's, so if the guard were mutated away (e.g. always false),
    // reflection would actually call it - proven here via a side effect
    // (the flag), not the return value, which the mutant can't fake.
    val reflectivePathEntered = new java.util.concurrent.atomic.AtomicBoolean(false)
    val lookalike = new org.apache.spark.sql.connector.catalog.Table {
      override def name(): String = "not-iceberg"
      override def schema(): org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType(Nil)
      override def capabilities(): java.util.Set[org.apache.spark.sql.connector.catalog.TableCapability] =
        java.util.Collections.emptySet()
      def table(): AnyRef = {
        reflectivePathEntered.set(true)
        throw new RuntimeException("must never be called: the SparkTable guard should reject this Table by class name first")
      }
    }
    assert(SparkAdapterListener.icebergSnapshotIdOfTable(lookalike).isEmpty)
    assert(!reflectivePathEntered.get(), "the guard must short-circuit before ever calling table()")
  }

  test("SparkAdapterListener.icebergSnapshotIdOf returns None immediately when the catalog isn't a TableCatalog") {
    val notATableCatalog = new org.apache.spark.sql.connector.catalog.CatalogPlugin {
      override def initialize(name: String, options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit = ()
      override def name(): String = "not-a-table-catalog"
    }
    val identifier = org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "irrelevant")
    assert(SparkAdapterListener.icebergSnapshotIdOf(notATableCatalog, identifier).isEmpty)
  }
}
