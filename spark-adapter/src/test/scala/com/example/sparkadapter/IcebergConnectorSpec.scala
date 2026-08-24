// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Iceberg-specific coverage, separate from `ContractEnforcementRuleSpec`
  * (which is Delta-configured - a session's `spark.sql.extensions`/
  * `spark.sql.catalog.*` config can't mix both). Covers exactly the write
  * shapes/fail-closed behaviors this connector's investigation found:
  * `CreateTableAsSelect`, `OverwritePartitionsDynamic`, and
  * `ReplaceData`/`WriteDelta` row-level DML are new, connector-agnostic
  * `WriteCommandSupport` cases (see that file); `AppendData`/
  * `OverwriteByExpression`/`ReplaceTableAsSelect`/`WriteToStream` were
  * already generic and are confirmed here to cover Iceberg "for free",
  * not just assumed from the Delta precedent. See
  * docs/SPARK_ADAPTER.md's Iceberg section for the full investigation.
  */
class IcebergConnectorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  @volatile private var activeContract: Option[com.example.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invariant-iceberg-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("IcebergConnectorSpec")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", scratchDir.resolve("warehouse").toString)
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          capturedPlans += plan
          activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions))
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def parseContract(yaml: String) = ContractParser.parse(yaml)

  private def withContract[T](yaml: String, options: VerificationOptions = VerificationOptions())(body: => T): T = {
    activeContract = Some(parseContract(yaml))
    activeOptions = options
    try body
    finally activeContract = None
  }

  // --- Read: batch DataSourceV2Relation (SparkPlanAdapter's new case) ---

  test("translates a batch Iceberg catalog read (DataSourceV2Relation) with a precise location, no fallback needed") {
    val tableName = "local.db.read_test_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    // A bare .load(...) (unlike spark.table(...), confirmed empirically -
    // see the Phase 2 probe, since deleted) analyzes directly to a
    // DataSourceV2Relation with no SubqueryAlias wrapper.
    val df = spark.read.format("iceberg").load(tableName)
    val translated = SparkPlanAdapter.translate(df.queryExecution.analyzed)
    translated.plan match {
      case r: com.example.ir.Read =>
        assert(r.dataset.location.nonEmpty)
        assert(!r.dataset.location.contains("DataSourceV2Relation"), "must be a real location, not a toString fallback")
      case other => fail(s"expected ir.Read, got $other")
    }
    assert(translated.diagnostics.isEmpty, "a resolved catalog table read should need no fallback diagnostic")
  }

  // --- Write: AppendData/OverwriteByExpression/ReplaceTableAsSelect - confirmed generic, not Iceberg-specific ---

  test("PASS: appending to an existing Iceberg table satisfying its contract executes normally") {
    val tableName = "local.db.append_pass_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("append_pass_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    format: iceberg
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: doubled
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append() // must not throw
    }

    assert(spark.table(tableName).count() == 6)
  }

  test("FAIL: appending to an Iceberg table violating its contract is aborted before anything is written") {
    val tableName = "local.db.append_fail_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("append_fail_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val beforeRows = spark.table(tableName).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table(tableName).collect().toSet == beforeRows, "the append must be aborted before touching the table")
  }

  // --- Write: CreateTableAsSelect (new case, previously fell through unrecognized+unsafe -> failed closed) ---

  test("PASS: .writeTo(...).create() (explicit V2 CTAS) satisfying its contract executes normally") {
    val tableName = "local.db.ctas_pass_tbl"
    // The table doesn't exist until this write runs, so - same reasoning
    // as ReplaceTableAsSelect's own new-table PASS test - the qualified
    // catalog identifier is the location, not a physical path.
    val expectedLocation = tableName

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: doubled
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).create() // must not throw
    }

    assert(spark.catalog.tableExists(tableName.stripPrefix("local.")) || spark.table(tableName).count() == 5)
  }

  test("FAIL: .writeTo(...).create() violating its contract is aborted before any table is created") {
    val tableName = "local.db.ctas_fail_tbl"
    val expectedLocation = tableName

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).create()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(scala.util.Try(spark.table(tableName)).isFailure, "the new table must never be created, not merely reported as failed")
  }

  // Direct-inspection companion, same pattern as the path-based-DML test
  // elsewhere in this codebase: CreateTableAsSelect.ignoreIfExists picks
  // between "error" (a bare CREATE TABLE ... AS SELECT, fails if the
  // table exists) and "ignore" (CREATE TABLE IF NOT EXISTS ... AS SELECT,
  // silently skips) - proving createTableAsSelect actually distinguishes
  // the two, not just that a write happens either way.
  test("CreateTableAsSelect's saveMode reflects ignoreIfExists: 'error' for a bare CTAS, 'ignore' for IF NOT EXISTS") {
    capturedPlans.clear()
    spark.range(3).withColumn("doubled", col("id") * 2).writeTo("local.db.ctas_savemode_error_tbl").create()
    val errorCtas = capturedPlans.collectFirst {
      case p: org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect => p
    }.getOrElse(fail("no CreateTableAsSelect plan observed for the bare CTAS"))
    assert(WriteCommandSupport.combined.lift(errorCtas).flatMap(_.saveMode).contains("error"))

    capturedPlans.clear()
    spark.sql(
      "CREATE TABLE IF NOT EXISTS local.db.ctas_savemode_ignore_tbl USING iceberg AS SELECT 1L AS id, 2L AS doubled")
    val ignoreCtas = capturedPlans.collectFirst {
      case p: org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect => p
    }.getOrElse(fail("no CreateTableAsSelect plan observed for CREATE TABLE IF NOT EXISTS"))
    assert(WriteCommandSupport.combined.lift(ignoreCtas).flatMap(_.saveMode).contains("ignore"))
  }

  // --- Write: OverwritePartitionsDynamic (new case) ---

  test("PASS: .writeTo(...).overwritePartitions() satisfying its contract executes normally") {
    val tableName = "local.db.dynpart_pass_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("dynpart_pass_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg PARTITIONED BY (id)")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: doubled
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.range(0, 1).withColumn("doubled", lit(0L)).writeTo(tableName).overwritePartitions() // must not throw
    }

    assert(spark.table(tableName).count() == 5, "id=0's partition was overwritten in place, not appended")
  }

  // --- Write: row-level DML via ReplaceData/WriteDelta (new, connector-agnostic case) ---
  //
  // Confirmed empirically (not assumed): unlike AppendData's `table`
  // above, `RowLevelWrite.table`'s resolved `Table` handle doesn't carry
  // a `"location"` property here - `namedRelationLocationAndFormat` falls
  // through to the qualified catalog identifier, the same fallback tier
  // `ReplaceTableAsSelect`'s new-table case uses (with the same
  // `V2Write` diagnostic explaining why). So these tests' contracts
  // declare the table's qualified name as the location, not a physical
  // path.

  test("PASS: a MERGE INTO an Iceberg table satisfying its contract's declared output executes normally") {
    val tableName = "local.db.merge_pass_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("FAIL: a MERGE INTO an Iceberg table whose target violates its contract's declared output schema is aborted before touching the table") {
    val tableName = "local.db.merge_fail_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val beforeRows = spark.table(tableName).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled) s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table(tableName).collect().toSet == beforeRows, "the MERGE must be aborted before touching the table")
  }

  test("PASS: an UPDATE against an Iceberg table satisfying its contract executes normally") {
    val tableName = "local.db.update_pass_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).where("id = 3").collect().head.getLong(1) == 7)
  }

  test("PASS: a DELETE against an Iceberg table satisfying its contract executes normally") {
    val tableName = "local.db.delete_pass_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  // --- Fail-closed: Call (deliberately unmodeled) ---

  test("FAIL: a CALL system procedure is rejected by the fail-closed policy, since it's deliberately unmodeled") {
    val tableName = "local.db.call_fail_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("call_fail_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    // Any active contract - CALL's fail-closed rejection doesn't depend on
    // the contract's content, only on the command being unrecognized and
    // not on FailClosedCommands' safe list.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"CALL local.system.rewrite_data_files('db.call_fail_tbl')").collect()
      }
    }
  }

  // --- Regression: safe-listed Iceberg metadata commands aren't blocked ---

  test("safe-listed Iceberg metadata commands (branch/tag/partition-spec) are not blocked under an active, unrelated-checking contract") {
    val tableName = "local.db.metadata_safe_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("metadata_safe_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg PARTITIONED BY (id)")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"ALTER TABLE $tableName CREATE BRANCH test_branch").collect() // must not throw
      spark.sql(s"ALTER TABLE $tableName CREATE TAG test_tag").collect() // must not throw
      spark.sql(s"ALTER TABLE $tableName ADD PARTITION FIELD doubled").collect() // must not throw
      spark.sql(s"ALTER TABLE $tableName DROP BRANCH test_branch").collect() // must not throw
    }
  }

  // --- Streaming: WriteToStream/StreamingRelationV2 - confirmed generic, not Iceberg-specific ---

  test("PASS: a streaming Iceberg .toTable() write satisfying its contract starts and writes normally") {
    val sourceTable = "local.db.stream_source_tbl"
    val sinkTable = "local.db.stream_sink_tbl"
    // Confirmed empirically: streamSinkLocationAndFormat's catalogTable
    // tier resolves for Iceberg's streaming sink, but (like the row-level
    // write case above) without a physical "location", landing on the
    // qualified identifier.
    val expectedLocation = sinkTable
    spark.sql(s"CREATE TABLE $sourceTable (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(sourceTable).append()
    spark.sql(s"CREATE TABLE $sinkTable (id BIGINT, doubled BIGINT) USING iceberg")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    val checkpointDir = scratchDir.resolve("stream_checkpoint").toString
    withContract(yaml) {
      val query = spark.readStream.format("iceberg").load(sourceTable).writeStream
        .format("iceberg")
        .option("checkpointLocation", checkpointDir)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .toTable(sinkTable) // must not throw
      query.awaitTermination()
    }

    assert(spark.table(sinkTable).count() == 5)
  }
}
