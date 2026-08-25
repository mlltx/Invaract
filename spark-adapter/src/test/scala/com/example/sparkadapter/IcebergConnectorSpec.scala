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
      // See ContractEnforcementRuleSpec's beforeAll for why - same
      // reasoning, and this suite's MERGE/UPDATE/DELETE tests shuffle too.
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
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

  // --- Feature surface: deletion vectors (format-version=3 merge-on-read deletes) ---
  //
  // Confirmed empirically (throwaway probe, since deleted) that a real
  // format-version=3 table (Iceberg's deletion-vector spec, the successor
  // to position-delete files for merge-on-read deletes) still produces a
  // plain ReplaceData node for a DELETE - the same class the existing
  // dsv2RowLevelWrite case already matches via the shared RowLevelWrite
  // trait. No new code was needed: the storage mechanism behind a
  // merge-on-read delete (position-delete file vs. deletion vector) isn't
  // visible at the LogicalPlan level Invariant operates on at all.

  test("PASS: a DELETE against a format-version=3 (deletion vector) Iceberg table satisfying its contract executes normally") {
    val tableName = "local.db.dv_delete_pass_tbl"
    val expectedLocation = tableName
    spark.sql(
      s"""CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg
         |TBLPROPERTIES ('format-version' = '3')
         |""".stripMargin)
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

  // --- Write: target-only fields (outputSchemaWithTargetOnlyFields) ---
  //
  // A real bug, found while investigating Iceberg's own schema-evolution
  // mechanism (throwaway probes, since deleted, not assumed): with
  // 'write.spark.accept-any-schema' = 'true', Iceberg accepts a narrower
  // append - a write whose DataFrame is missing a column the target
  // already has - silently NULL-filling the omitted column. Confirmed
  // empirically that Spark's own analyzer only allows this when the
  // property is set (a plain narrower append against a table without it
  // is rejected with AnalysisException before ever reaching a check
  // rule) - so by the time a resolved AppendData/OverwritePartitionsDynamic/
  // OverwriteByExpression reaches WriteCommandSupport at all, the
  // target's own connector has already endorsed the field's absence as
  // valid. Previously, outputSchema (from query.schema alone) omitted
  // that field entirely, so a contract requiring it would be wrongly
  // MISSING_OUTPUT_FIELD-rejected for a write that actually satisfies it.
  // Fixed by generalizing what was previously a Delta-specific
  // "generated columns" fix (outputSchemaWithGeneratedColumns, reflecting
  // into DeltaTableV2/Snapshot metadata) into a connector-agnostic one
  // (outputSchemaWithTargetOnlyFields, plain cmd.table.schema() - no
  // reflection at all) once this investigation confirmed the field NAME
  // was already present there all along, just not its Delta-specific
  // generation metadata - see docs/SPARK_ADAPTER.md's Iceberg section.
  test("PASS: an append narrower than an Iceberg table's schema, under accept-any-schema, satisfies a contract requiring the omitted column") {
    val tableName = "local.db.narrow_evo_pass_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("narrow_evo_pass_tbl").toString
    spark.sql(
      s"""CREATE TABLE $tableName (id BIGINT, doubled BIGINT, extra_col STRING) USING iceberg
         |TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
         |""".stripMargin)
    spark.range(5).withColumn("doubled", col("id") * 2).withColumn("extra_col", lit("x")).writeTo(tableName).append()

    // required: false throughout - Iceberg reports every column nullable
    // on read-back (the same quirk already documented for Delta), and
    // extra_col specifically must be nullable for Iceberg to NULL-fill it.
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
         |        - name: extra_col
         |          type: string
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      // No extra_col supplied - Iceberg NULL-fills it, but the contract
      // requires it - must not throw, since it's still genuinely committed.
      spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    }

    val written = spark.table(tableName).where("id = 5").collect()
    assert(written.length == 1)
    assert(written.head.isNullAt(2), "extra_col must actually have been NULL-filled by Iceberg, not supplied")
  }

  test("FAIL: a narrower append is still rejected by Spark itself without accept-any-schema, before reaching Invariant") {
    val tableName = "local.db.narrow_no_evo_fail_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT, extra_col STRING) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).withColumn("extra_col", lit("x")).writeTo(tableName).append()

    // No active contract - this must fail on Spark's own analysis, not
    // Invariant's enforcement, proving outputSchemaWithTargetOnlyFields's
    // safety argument: a genuinely-missing field is still caught upstream.
    intercept[org.apache.spark.sql.AnalysisException] {
      spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    }
  }

  // --- Feature surface: identity/generated columns ---
  //
  // Confirmed empirically (throwaway probes, since deleted): unlike
  // Delta, this Iceberg catalog integration rejects both Spark's
  // GENERATED ALWAYS AS syntax and column DEFAULT values outright -
  // UNSUPPORTED_FEATURE.TABLE_OPERATION, thrown by Spark's own analyzer
  // before a plan is ever produced, regardless of 'write.spark.accept-
  // any-schema'. So there's no Iceberg analog to Delta's generated
  // columns reachable through Spark SQL with this connector: nothing for
  // Invariant to translate or verify, and no gap the outputSchemaWith-
  // TargetOnlyFields mechanism needs to cover for this case.

  test("GENERATED ALWAYS AS is rejected outright by this Iceberg catalog integration, before any plan is produced") {
    val tableName = "local.db.gen_col_unsupported_tbl"
    intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(
        s"""CREATE TABLE $tableName (id BIGINT, doubled BIGINT GENERATED ALWAYS AS (id * 2)) USING iceberg
           |""".stripMargin)
    }
  }

  test("a column DEFAULT value is rejected outright by this Iceberg catalog integration, even under accept-any-schema") {
    val tableName = "local.db.default_col_unsupported_tbl"
    intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(
        s"""CREATE TABLE $tableName (id BIGINT, doubled BIGINT, status STRING DEFAULT 'active') USING iceberg
           |TBLPROPERTIES ('format-version' = '3', 'write.spark.accept-any-schema' = 'true')
           |""".stripMargin)
    }
  }

  // --- StateChangingCallSupport: six procedures genuinely verified (not
  // just fail-closed) - see that file's own doc for the reflection
  // mechanism, the per-procedure argument-shape findings, and why this is
  // a separate registry from WriteCommandSupport.

  // Corrected design (see StateChangingCallSupport's own doc for the full
  // finding): none of the six can revert current-schema-id (confirmed
  // empirically per procedure, and for rollback_to_snapshot specifically
  // independently corroborated by apache/iceberg#15165) - so what's
  // checked is the table's CURRENT schema (unaffected by any of them)
  // plus location, not any snapshot's own historical schema.

  test("PASS: rollback_to_snapshot on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.rollback_pass_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val firstSnapshotId =
      spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at").collect().head.getLong(0)
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

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
      spark.sql(s"CALL local.system.rollback_to_snapshot('db.rollback_pass_tbl', $firstSnapshotId)").collect() // must not throw
    }

    // A CALL-driven rollback doesn't go through Spark's normal write path,
    // which is what usually auto-invalidates the session's cached table
    // metadata - refresh explicitly rather than assume spark.table(...)
    // reflects the post-rollback state.
    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 5, "the rollback must actually have taken effect (data reverted)")
  }

  test("FAIL: rollback_to_snapshot on a table whose current schema violates the contract is aborted before touching the table") {
    val tableName = "local.db.rollback_fail_tbl"
    val expectedLocation = tableName
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val firstSnapshotId =
      spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at").collect().head.getLong(0)
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val beforeRows = spark.table(tableName).collect().toSet

    // Contract requires 'missing_col', which the table's CURRENT schema
    // doesn't have (and never will, regardless of which snapshot the
    // rollback targets) - the operation must be rejected.
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
         |        - name: missing_col
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"CALL local.system.rollback_to_snapshot('db.rollback_fail_tbl', $firstSnapshotId)").collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table(tableName).collect().toSet == beforeRows, "the rollback must be aborted before touching the table")
  }

  test("PASS: rollback_to_snapshot on a table the active contract doesn't govern is allowed, not swept up by an unrelated contract") {
    val governedTable = "local.db.rollback_unrelated_governed_tbl"
    val unrelatedTable = "local.db.rollback_unrelated_target_tbl"
    val expectedLocation = governedTable
    spark.sql(s"CREATE TABLE $governedTable (id BIGINT) USING iceberg")
    spark.range(3).writeTo(governedTable).append()

    spark.sql(s"CREATE TABLE $unrelatedTable (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(unrelatedTable).append()
    val firstSnapshotId =
      spark.sql(s"SELECT snapshot_id FROM $unrelatedTable.snapshots ORDER BY committed_at").collect().head.getLong(0)
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(unrelatedTable).append()

    // A contract that would genuinely reject unrelatedTable's current
    // schema if it applied there (requires 'missing_col', which
    // unrelatedTable doesn't have) - proving location-scoping actually
    // skips the schema check, not just that a lenient contract happens to
    // pass either way (a real mutation-testing survivor caught this test
    // not actually distinguishing the two - see StructuralVerifier.
    // verifyStateChange's own doc).
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
         |        - name: missing_col
         |          type: string
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      // Targets unrelatedTable, not governedTable - must not throw, since
      // this contract doesn't govern unrelatedTable at all.
      spark.sql(s"CALL local.system.rollback_to_snapshot('db.rollback_unrelated_target_tbl', $firstSnapshotId)").collect()
    }

    spark.catalog.refreshTable(unrelatedTable)
    assert(spark.table(unrelatedTable).count() == 5, "the rollback on the unrelated table must actually have taken effect")
  }

  // --- StateChangingCallSupport's other five procedures. Each pairs with
  // a genuinely different way of moving what's current (see that file's
  // own doc, "The six procedures"), confirmed via a real probe (since
  // deleted) before being relied on here - one PASS test per procedure is
  // enough to kill a mutant swapping its entry in
  // currentStateChangingProcedureClasses, since verifyStateChange's own
  // PASS/FAIL logic is already exhaustively covered by rollback_to_snapshot's
  // tests above and doesn't change per procedure.

  test("PASS: rollback_to_timestamp on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.rollback_ts_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    Thread.sleep(50)
    val ts = new java.sql.Timestamp(System.currentTimeMillis())
    Thread.sleep(50)
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
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
      spark.sql(s"CALL local.system.rollback_to_timestamp('db.rollback_ts_pass_tbl', TIMESTAMP '$ts')").collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 5, "the rollback must actually have taken effect (data reverted)")
  }

  test("FAIL: rollback_to_timestamp on a table whose current schema violates the contract is aborted before touching the table") {
    val tableName = "local.db.rollback_ts_fail_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    Thread.sleep(50)
    val ts = new java.sql.Timestamp(System.currentTimeMillis())
    Thread.sleep(50)
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    val beforeRows = spark.table(tableName).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: missing_col
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"CALL local.system.rollback_to_timestamp('db.rollback_ts_fail_tbl', TIMESTAMP '$ts')").collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table(tableName).collect().toSet == beforeRows, "the rollback must be aborted before touching the table")
  }

  test("PASS: cherrypick_snapshot on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.cherrypick_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg")
    spark.range(3).writeTo(tableName).append()
    spark.sql(s"ALTER TABLE $tableName CREATE BRANCH audit")
    spark.range(3, 5).writeTo(s"$tableName.branch_audit").append()
    val branchSnapshotId =
      spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at DESC LIMIT 1").collect().head.getLong(0)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"CALL local.system.cherrypick_snapshot('db.cherrypick_pass_tbl', $branchSnapshotId)").collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 5, "the cherry-pick must actually have taken effect on main")
  }

  test("PASS: publish_changes on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.publish_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.range(3).writeTo(tableName).append()
    spark.conf.set("spark.wap.id", "wap-invariant-pass")
    spark.range(3, 5).writeTo(tableName).append() // staged under the WAP id, invisible on main until published
    spark.conf.unset("spark.wap.id")
    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 3, "a staged WAP write must not be visible on main before publishing")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"CALL local.system.publish_changes('db.publish_pass_tbl', 'wap-invariant-pass')").collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 5, "the publish must actually have taken effect on main")
  }

  test("PASS: set_current_snapshot (by snapshot_id) on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.set_current_id_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg")
    spark.range(3).writeTo(tableName).append()
    val firstSnapshotId =
      spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at").collect().head.getLong(0)
    spark.range(3, 5).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      // Named args, table + snapshot_id only - the args array is still
      // 3-wide (ref bound to null), confirmed via probe.
      spark.sql(s"CALL local.system.set_current_snapshot(table => 'db.set_current_id_pass_tbl', snapshot_id => $firstSnapshotId)")
        .collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 3, "the set-current must actually have taken effect")
  }

  test("PASS: set_current_snapshot (by ref) on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.set_current_ref_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg")
    spark.range(3).writeTo(tableName).append()
    spark.sql(s"ALTER TABLE $tableName CREATE TAG snap1")
    spark.range(3, 5).writeTo(tableName).append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      // Named args, table + ref this time - a different pair of the 3
      // declared parameters bound than the snapshot_id test above,
      // confirmed via probe to resolve to the same args(0)-is-the-table
      // shape the extraction relies on.
      spark.sql(s"CALL local.system.set_current_snapshot(table => 'db.set_current_ref_pass_tbl', ref => 'snap1')").collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 3, "the set-current must actually have taken effect")
  }

  test("PASS: fast_forward('main', ...) on a table whose current schema satisfies the contract executes normally") {
    val tableName = "local.db.fast_forward_main_pass_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg")
    spark.range(3).writeTo(tableName).append()
    spark.sql(s"ALTER TABLE $tableName CREATE BRANCH feature")
    spark.range(3, 5).writeTo(s"$tableName.branch_feature").append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"CALL local.system.fast_forward('db.fast_forward_main_pass_tbl', 'main', 'feature')").collect() // must not throw
    }

    spark.catalog.refreshTable(tableName)
    assert(spark.table(tableName).count() == 5, "fast-forwarding main must actually have taken effect on the default read")
  }

  // The one genuinely different behavior among the six (see
  // StateChangingCallSupport's own doc): fast-forwarding a *non*-"main"
  // branch doesn't touch the table's default read at all. The check
  // still applies uniformly regardless - proving that concretely, not
  // just asserting it in a comment: a fast_forward on branch "b1" is
  // still rejected when the table's CURRENT (main) schema violates the
  // contract, even though this specific call's own effect is scoped to
  // "b1" and would never touch main either way.
  test("FAIL: fast_forward on a non-'main' branch is still checked against the table's current schema, and rejected") {
    val tableName = "local.db.fast_forward_branch_fail_tbl"
    spark.sql(s"CREATE TABLE $tableName (id BIGINT) USING iceberg")
    spark.range(3).writeTo(tableName).append()
    spark.sql(s"ALTER TABLE $tableName CREATE BRANCH b1")
    spark.sql(s"ALTER TABLE $tableName CREATE BRANCH b2")
    spark.range(3, 5).writeTo(s"$tableName.branch_b2").append()
    val mainRowsBefore = spark.table(tableName).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tableName
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: missing_col
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"CALL local.system.fast_forward('db.fast_forward_branch_fail_tbl', 'b1', 'b2')").collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table(tableName).collect().toSet == mainRowsBefore, "main must be untouched (it always was) and b1 must never have moved")
  }

  // --- Fail-closed: Call, for procedures still classified as genuinely
  // data-affecting and unmodeled (see FailClosedCommands'
  // safeIcebergProcedureClasses for the full classification - the six
  // procedures above are the exceptions that now have real support via
  // StateChangingCallSupport; every other state-changing procedure still
  // fails closed unconditionally) ---

  test("FAIL: add_files (still unmodeled - no verification mechanism built for it yet) is rejected by the fail-closed policy") {
    val tableName = "local.db.call_fail_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("call_fail_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    // Any active contract - CALL's fail-closed rejection doesn't depend on
    // the contract's content, only on the command being unrecognized and
    // not on FailClosedCommands' safe list or StateChangingCallSupport.
    // add_files never even reaches Iceberg's own execution (which would
    // fail anyway - 'db.nonexistent_source' doesn't exist) since
    // Invariant's check rule throws first, during analysis.
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
        spark.sql(s"CALL local.system.add_files('db.call_fail_tbl', 'db.nonexistent_source')").collect()
      }
    }
    // Nothing was imported - still just the one write's rows.
    assert(spark.table(tableName).count() == 5)
  }

  // --- Regression: Iceberg system procedures classified safe aren't blocked ---
  //
  // A representative sample, not all 10 (same precedent as the pre-existing
  // metadata-safe-commands test below, which samples 4 of its 13) - one per
  // reasoning category in FailClosedCommands' safeIcebergProcedureClasses
  // comment: storage/metadata compaction, GC of unreferenced
  // files/snapshots, and read-only introspection.

  test("Iceberg system procedures classified safe (compaction, GC, introspection) are not blocked under an active, unrelated-checking contract") {
    val tableName = "local.db.call_safe_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve("db").resolve("call_safe_tbl").toString
    spark.sql(s"CREATE TABLE $tableName (id BIGINT, doubled BIGINT) USING iceberg")
    spark.range(5).withColumn("doubled", col("id") * 2).writeTo(tableName).append()
    spark.range(5, 10).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    // A real, checking contract (not a trivial always-pass one) - proves
    // these calls actually reach FailClosedCommands' safe-list check
    // rather than merely not encountering any contract at all.
    // required: false throughout - Iceberg reports every column nullable
    // on read-back (the same quirk documented elsewhere in this file).
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
      spark.sql(s"CALL local.system.rewrite_data_files(table => 'db.call_safe_tbl')").collect() // must not throw
      spark.sql(s"CALL local.system.rewrite_manifests(table => 'db.call_safe_tbl')").collect() // must not throw
      spark.sql(s"CALL local.system.expire_snapshots(table => 'db.call_safe_tbl')").collect() // must not throw
      spark.sql(s"CALL local.system.remove_orphan_files(table => 'db.call_safe_tbl')").collect() // must not throw
      spark.sql(s"CALL local.system.ancestors_of('db.call_safe_tbl')").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 10, "none of these procedures should have changed the table's row count")
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
