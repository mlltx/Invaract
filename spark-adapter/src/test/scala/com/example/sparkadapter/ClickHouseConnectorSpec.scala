// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** ClickHouse-specific coverage, added via the `add-spark-connector`
  * skill's process (docs/ADDING_A_SPARK_CONNECTOR.md). Unlike every prior
  * connector, ClickHouse needs a real, separate *server* process to test
  * against, not just a session extension or embedded metastore - provided
  * here by `ClickHouseTestServer`, a real standalone `clickhouse` binary
  * launched as a subprocess (no Docker/testcontainers - see that file's
  * doc comment for why). Linux/macOS only; excluded on Windows in
  * `build.sbt` (ClickHouse has no supported native Windows server build).
  *
  * A real reflective jar scan of `clickhouse-spark-runtime` found zero
  * `Command`-shaped classes (same as Avro's finding) - this connector's
  * entire write surface routes through Spark's own generic DSv2 command
  * family. `AppendData`/`OverwriteByExpression`/`CreateTableAsSelect`/
  * `ReplaceTableAsSelect` were already generic (from Iceberg's pass) and
  * are confirmed here to cover ClickHouse "for free." `DeleteFromTable`
  * is a genuinely new, connector-agnostic `WriteCommandSupport` case
  * added this pass: confirmed empirically that a real predicate-based
  * `DELETE FROM ... WHERE ...` executes successfully against ClickHouse
  * (unlike Parquet/CSV/Avro's plain tables), via Spark's `SupportsDelete`
  * mechanism rather than the `SupportsRowLevelOperations`/
  * `RewriteRowLevelOperation` path Iceberg's MERGE/UPDATE/DELETE use - a
  * structurally different, simpler write shape. See
  * docs/connectors/clickhouse.md for the full coverage ledger.
  */
class ClickHouseConnectorSpec extends ConnectorSpecBase {
  private var server: ClickHouseTestServer = _
  private val httpPort = 28423
  private val tcpPort = 29300

  override def beforeAll(): Unit = {
    server = new ClickHouseTestServer(httpPort, tcpPort)
    server.start()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ClickHouseConnectorSpec")
      .config("spark.sql.catalog.ch", "com.clickhouse.spark.ClickHouseCatalog")
      .config("spark.sql.catalog.ch.host", "127.0.0.1")
      .config("spark.sql.catalog.ch.protocol", "http")
      .config("spark.sql.catalog.ch.http_port", httpPort.toString)
      .config("spark.sql.catalog.ch.user", "default")
      .config("spark.sql.catalog.ch.password", "")
      .config("spark.sql.catalog.ch.database", "default")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE DATABASE IF NOT EXISTS ch.probe_db")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (server != null) server.stop()
  }

  private def createTable(name: String): Unit =
    spark.sql(s"CREATE TABLE IF NOT EXISTS ch.probe_db.$name (id BIGINT NOT NULL, value BIGINT) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id')")

  // --- .insertInto(...)/.writeTo(...).append(): AppendData, already
  // generic from Iceberg's pass, confirmed to cover ClickHouse via a real
  // PASS/FAIL pair. Location resolves to the qualified catalog identifier
  // (ch.db.table) - ClickHouseTable exposes no physical filesystem path
  // via Table.properties(), confirmed empirically. ---

  test("PASS: appending to an existing ClickHouse table satisfying its contract executes normally") {
    createTable("append_pass_tbl")
    df().writeTo("ch.probe_db.append_pass_tbl").append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.append_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      df().writeTo("ch.probe_db.append_pass_tbl").append() // must not throw
    }
    assert(spark.table("ch.probe_db.append_pass_tbl").count() == 4)
  }

  test("FAIL: appending to a ClickHouse table violating its contract is aborted before anything is written") {
    createTable("append_fail_tbl")
    df().writeTo("ch.probe_db.append_fail_tbl").append()
    val rowCountBefore = spark.table("ch.probe_db.append_fail_tbl").count()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.append_fail_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        df().writeTo("ch.probe_db.append_fail_tbl").append()
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("ch.probe_db.append_fail_tbl").count() == rowCountBefore, "the append must be aborted before touching the table")
  }

  test("translates .insertInto() against a ClickHouse catalog table via the generic AppendData case") {
    createTable("insertinto_translate_tbl")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.insertInto("ch.probe_db.insertinto_translate_tbl")

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .insertInto() write yet"))
    }
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, _, saveMode) =>
        assert(location == "ch.probe_db.insertinto_translate_tbl")
        assert(saveMode.contains("append"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  // --- .writeTo(...).create()/.createOrReplace(): CreateTableAsSelect/
  // ReplaceTableAsSelect, already generic from Iceberg's pass. Every new
  // ClickHouse table needs an explicit ORDER BY/PRIMARY KEY (a real
  // ClickHouse constraint, confirmed via the "ORDER BY or PRIMARY KEY
  // clause is missing" server error otherwise) - passed via
  // .tableProperty("order_by", ...), not a contract concern. ---

  test("PASS: .writeTo(...).create() on a new ClickHouse table satisfying its contract executes normally") {
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.create_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      df().writeTo("ch.probe_db.create_pass_tbl").using("ClickHouse")
        .tableProperty("engine", "MergeTree()").tableProperty("order_by", "id")
        .tableProperty("settings.allow_nullable_key", "1").create() // must not throw
    }
    assert(spark.table("ch.probe_db.create_pass_tbl").count() == 2)
  }

  test("FAIL: .writeTo(...).create() on a new ClickHouse table violating its contract is aborted before any table is created") {
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.create_fail_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin
    withContract(yaml) {
      intercept[ContractViolationException] {
        df().writeTo("ch.probe_db.create_fail_tbl").using("ClickHouse")
          .tableProperty("engine", "MergeTree()").tableProperty("order_by", "id").create()
      }
    }
    assert(!spark.catalog.tableExists("ch.probe_db.create_fail_tbl"), "the table must never have been created")
  }

  test("PASS: .writeTo(...).createOrReplace() on a ClickHouse table satisfying its contract executes normally") {
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.createorreplace_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      df().writeTo("ch.probe_db.createorreplace_pass_tbl").using("ClickHouse")
        .tableProperty("engine", "MergeTree()").tableProperty("order_by", "id")
        .tableProperty("settings.allow_nullable_key", "1").createOrReplace() // must not throw
    }
    assert(spark.table("ch.probe_db.createorreplace_pass_tbl").count() == 2)
  }

  // --- .writeTo(...).overwrite(cond): OverwriteByExpression, already
  // generic from Iceberg's pass. ---

  test("PASS: .writeTo(...).overwrite(...) against a ClickHouse table satisfying its contract executes normally") {
    createTable("overwrite_pass_tbl")
    df().writeTo("ch.probe_db.overwrite_pass_tbl").append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.overwrite_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      df().writeTo("ch.probe_db.overwrite_pass_tbl").overwrite(org.apache.spark.sql.functions.lit(true)) // must not throw
    }
    assert(spark.table("ch.probe_db.overwrite_pass_tbl").count() == 2)
  }

  // --- DELETE FROM ... WHERE ...: DeleteFromTable, a genuinely new
  // WriteCommandSupport case added this pass - confirmed via a real
  // PASS/FAIL pair, structural (target location/schema) verification only,
  // matching Delta/Iceberg's row-level DML precedent. The delete
  // predicate itself has no IR representation and isn't checked. ---

  test("PASS: DELETE FROM a ClickHouse table whose target satisfies its contract executes normally") {
    createTable("delete_pass_tbl")
    df().writeTo("ch.probe_db.delete_pass_tbl").append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.delete_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      spark.sql("DELETE FROM ch.probe_db.delete_pass_tbl WHERE id = 1") // must not throw
    }
    assert(spark.table("ch.probe_db.delete_pass_tbl").count() == 1, "exactly the matching row should be deleted")
  }

  test("FAIL: DELETE FROM a ClickHouse table whose target violates its contract is aborted before anything is deleted") {
    createTable("delete_fail_tbl")
    df().writeTo("ch.probe_db.delete_fail_tbl").append()
    val rowCountBefore = spark.table("ch.probe_db.delete_fail_tbl").count()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.delete_fail_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin
    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql("DELETE FROM ch.probe_db.delete_fail_tbl WHERE id = 1")
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("ch.probe_db.delete_fail_tbl").count() == rowCountBefore, "the delete must be aborted before touching the table")
  }

  // --- UPDATE/MERGE: confirmed empirically to be rejected before ever
  // producing an executable write - UPDATE by Spark's own generic V2
  // rejection ("UPDATE TABLE is not supported temporarily"), MERGE by
  // AnalysisException before even resolving to a Command-shaped plan.
  // Neither is a real Phase-4 case-3 (data-mutating, unmodeled, left to
  // fail closed) operation for this connector specifically - both are
  // N/A the same way Parquet/CSV/Avro's own DML rejections are, not
  // something Invariant's fail-closed policy needs to catch. ---

  test("UPDATE/MERGE against a ClickHouse table are rejected before any write occurs, nothing reaches Invariant's fail-closed policy") {
    createTable("dml_reject_target")
    df().writeTo("ch.probe_db.dml_reject_target").append()
    createTable("dml_reject_source")
    df().writeTo("ch.probe_db.dml_reject_source").append()
    val rowCountBefore = spark.table("ch.probe_db.dml_reject_target").count()

    // SparkUnsupportedOperationException is package-private to org.apache.spark;
    // caught as the public Exception supertype, same convention as the
    // Avro/CSV specs' equivalent UPDATE-rejection tests.
    val updateEx = intercept[Exception] {
      spark.sql("UPDATE ch.probe_db.dml_reject_target SET value = 0 WHERE id = 1").collect()
    }
    assert(updateEx.getMessage.contains("UPDATE TABLE is not supported"))
    intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(
        "MERGE INTO ch.probe_db.dml_reject_target t USING ch.probe_db.dml_reject_source s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT *"
      ).collect()
    }
    assert(spark.table("ch.probe_db.dml_reject_target").count() == rowCountBefore, "no rejected DML should have changed the table")
  }

  // --- Streaming write: confirmed empirically that ClickHouseTable
  // itself doesn't implement SupportsStreamingWrite - a genuine connector
  // limitation, not an Invariant gap, rejected by Spark before any
  // Command-shaped write plan is ever produced. ---

  test("streaming write to a ClickHouse table is rejected by the connector itself, not by Invariant") {
    createTable("stream_reject_tbl")
    val memStream = new org.apache.spark.sql.execution.streaming.MemoryStream[(Long, Long)](500, spark.sqlContext)(org.apache.spark.sql.Encoders.product)
    memStream.addData((1L, 10L))
    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      memStream.toDF().toDF("id", "value").writeStream.format("ClickHouse")
        .option("checkpointLocation", java.nio.file.Files.createTempDirectory("ch-ckpt-reject").toString)
        .toTable("ch.probe_db.stream_reject_tbl")
    }
    assert(ex.getMessage.contains("doesn't support streaming write"))
  }

  // --- Read: catalog table reference and TableProvider format-based
  // load, both via the existing generic DataSourceV2Relation case
  // (Iceberg's pass). ---

  // Read-side location format is genuinely different from the write side
  // for the same table - confirmed empirically, not assumed. Writes
  // resolve to the computed 3-part qualified identifier
  // (catalog.namespace.table, e.g. "ch.probe_db.t") via
  // WriteCommandSupport's own qualifiedIdentifier helper, since
  // ClickHouseTable exposes no "location" property either side. Reads go
  // through DataSourceV2Relation.name (ClickHouseTable's own name()
  // implementation), which returns a backtick-quoted *2-part*
  // `namespace`.`table` with no catalog prefix at all - a real,
  // connector-specific inconsistency between the read and write location
  // conventions for the identical table, not an Invariant bug (the same
  // "does Table.properties() expose 'location'?" fallback chain other
  // DSv2 connectors already use, ClickHouse just answers differently on
  // each side). See docs/connectors/clickhouse.md for the next step (a
  // possible future unification, out of scope for this pass).
  test("PASS: a contract's declared input schema is satisfied by a catalog table reference read") {
    createTable("read_catalog_tbl")
    df().writeTo("ch.probe_db.read_catalog_tbl").append()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: "`probe_db`.`read_catalog_tbl`"
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |outputs:
         |  - name: out
         |    location: ch.probe_db.read_catalog_out_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      spark.table("ch.probe_db.read_catalog_tbl").writeTo("ch.probe_db.read_catalog_out_tbl")
        .using("ClickHouse").tableProperty("engine", "MergeTree()").tableProperty("order_by", "id")
        .tableProperty("settings.allow_nullable_key", "1").create() // must not throw
    }
  }

  test("PASS: TableProvider format-based read/write round-trips real data") {
    createTable("format_rw_tbl")
    df().write.format("clickhouse")
      .option("host", "127.0.0.1")
      .option("protocol", "http")
      .option("http_port", httpPort.toString)
      .option("user", "default")
      .option("password", "")
      .option("database", "probe_db")
      .option("table", "format_rw_tbl")
      .mode("append")
      .save()
    val readBack = spark.read.format("clickhouse")
      .option("host", "127.0.0.1")
      .option("protocol", "http")
      .option("http_port", httpPort.toString)
      .option("user", "default")
      .option("password", "")
      .option("database", "probe_db")
      .option("table", "format_rw_tbl")
      .load()
    assert(readBack.collect().length == 2)
  }

  // --- Feature surface: ClickHouse's own ORDER BY/sorting-key nullability
  // constraint. Confirmed genuinely orthogonal to Invariant: a source
  // DataFrame column correctly reports nullable=false, but
  // DataFrameWriterV2's .create() path doesn't propagate that into the
  // generated ClickHouse DDL, so ClickHouse itself rejects a nullable
  // sorting key regardless of the Spark-side schema - independently of
  // whatever a contract declares. Invariant's own structural checks don't
  // interact with this at all; ClickHouse enforces it itself, the same
  // "confirmed orthogonal" pattern as Delta's CHECK constraints. ---

  test("feature surface: ClickHouse's own nullable-sorting-key constraint is orthogonal to Invariant, not something it checks") {
    val ex = intercept[com.clickhouse.spark.exception.CHServerException] {
      df().writeTo("ch.probe_db.nullable_key_feature_tbl").using("ClickHouse")
        .tableProperty("engine", "MergeTree()").tableProperty("order_by", "id").create()
      // deliberately omitting settings.allow_nullable_key=1, unlike every
      // other .create() test above
    }
    assert(ex.getMessage.contains("allow_nullable_key"))
    // Invariant itself never rejects or reports on this - Delete/Append
    // tests elsewhere in this file already prove a *satisfying* write
    // (with the workaround applied) is unaffected either way.
  }

  // --- Regression: ClickHouse's own safe-listed DDL isn't blocked under
  // a contract that would reject anything it actually checked. All four
  // classes here were already safe-listed generically from prior DSv2
  // connector passes - confirmed to cover ClickHouse with zero new
  // FailClosedCommands entries. ---

  test("regression: CREATE DATABASE/TABLE, ANALYZE TABLE, SHOW TABLES for ClickHouse tables aren't blocked under an active contract") {
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: nonexistent_location
         |    schema:
         |      fields:
         |        - name: whatever
         |          type: string
         |          required: true
         |""".stripMargin
    withContract(yaml) {
      spark.sql("CREATE DATABASE IF NOT EXISTS ch.regression_db").collect()
      spark.sql("CREATE TABLE IF NOT EXISTS ch.regression_db.t (id BIGINT NOT NULL) USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id')").collect()
      intercept[org.apache.spark.sql.AnalysisException] {
        spark.sql("ANALYZE TABLE ch.regression_db.t COMPUTE STATISTICS").collect()
      } // ANALYZE TABLE is rejected by Spark itself for v2 tables, not by Invariant - confirmed, not assumed
      spark.sql("SHOW TABLES IN ch.regression_db").collect()
    }
  }
}
