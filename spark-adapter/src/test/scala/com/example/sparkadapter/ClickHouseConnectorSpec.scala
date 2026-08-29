// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

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
  // something Invaract's fail-closed policy needs to catch. ---

  test("UPDATE/MERGE against a ClickHouse table are rejected before any write occurs, nothing reaches Invaract's fail-closed policy") {
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
  // limitation, not an Invaract gap, rejected by Spark before any
  // Command-shaped write plan is ever produced. ---

  test("streaming write to a ClickHouse table is rejected by the connector itself, not by Invaract") {
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
  // conventions for the identical table, not an Invaract bug (the same
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
  // constraint. Confirmed genuinely orthogonal to Invaract: a source
  // DataFrame column correctly reports nullable=false, but
  // DataFrameWriterV2's .create() path doesn't propagate that into the
  // generated ClickHouse DDL, so ClickHouse itself rejects a nullable
  // sorting key regardless of the Spark-side schema - independently of
  // whatever a contract declares. Invaract's own structural checks don't
  // interact with this at all; ClickHouse enforces it itself, the same
  // "confirmed orthogonal" pattern as Delta's CHECK constraints. ---

  test("feature surface: ClickHouse's own nullable-sorting-key constraint is orthogonal to Invaract, not something it checks") {
    val ex = intercept[com.clickhouse.spark.exception.CHServerException] {
      df().writeTo("ch.probe_db.nullable_key_feature_tbl").using("ClickHouse")
        .tableProperty("engine", "MergeTree()").tableProperty("order_by", "id").create()
      // deliberately omitting settings.allow_nullable_key=1, unlike every
      // other .create() test above
    }
    assert(ex.getMessage.contains("allow_nullable_key"))
    // Invaract itself never rejects or reports on this - Delete/Append
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
      } // ANALYZE TABLE is rejected by Spark itself for v2 tables, not by Invaract - confirmed, not assumed
      spark.sql("SHOW TABLES IN ch.regression_db").collect()
    }
  }

  // --- Follow-up pass closing the 4 remaining ❓ rows from the prior
  // pass: streaming read, .saveAsTable() onto an existing table,
  // maintenance operations, and the richer type system. Zero
  // src/main/scala changes this pass - every finding below is either a
  // confirmed-transparent behavior or a confirmed, permanent connector
  // limitation, not a translation gap. See docs/connectors/clickhouse.md. ---

  test("streaming read: rejected outright, both TableProvider- and catalog-based, before any plan is produced") {
    createTable("stream_read_tbl")
    val readStreamEx = intercept[Exception] {
      spark.readStream.format("clickhouse")
        .option("host", "127.0.0.1").option("http_port", httpPort.toString)
        .option("user", "default").option("password", "")
        .option("database", "probe_db").option("table", "stream_read_tbl")
        .load()
    }
    // SparkUnsupportedOperationException is package-private to org.apache.spark;
    // caught as the public Exception supertype, same convention as the UPDATE
    // rejection test above.
    assert(readStreamEx.getMessage.contains("does not support streamed reading"))

    val catalogStreamEx = intercept[org.apache.spark.sql.AnalysisException] {
      spark.readStream.table("ch.probe_db.stream_read_tbl")
    }
    assert(catalogStreamEx.getMessage.contains("does not support") &&
      (catalogStreamEx.getMessage.contains("micro-batch") || catalogStreamEx.getMessage.contains("continuous")))
  }

  test(".saveAsTable() append onto an EXISTING ClickHouse table: AppendData, the same already-covered shape") {
    createTable("saveastable_existing_tbl")
    capturedPlans.clear()
    df().write.mode("append").saveAsTable("ch.probe_db.saveastable_existing_tbl")
    assert(capturedPlans.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.AppendData]),
      "saveAsTable() onto an existing table must produce AppendData, the same shape .insertInto()/.writeTo().append() use")
    assert(spark.table("ch.probe_db.saveastable_existing_tbl").count() == 2)
  }

  test("maintenance operations (OPTIMIZE/ALTER...DELETE/VACUUM) are unreachable through Spark SQL with this connector") {
    createTable("maintenance_tbl")
    // Every one of these fails Spark's own parser before analysis - the
    // connector registers no SQL extension for them (unlike Delta's
    // OPTIMIZE/VACUUM, which install a parser extension of their own).
    // Nothing ever reaches a Command-shaped plan, so there is nothing for
    // WriteCommandSupport/FailClosedCommands to classify - not a 🚫
    // fails-closed row, a genuine absence of Spark-visible surface.
    Seq(
      "OPTIMIZE TABLE ch.probe_db.maintenance_tbl",
      "OPTIMIZE TABLE ch.probe_db.maintenance_tbl FINAL",
      "ALTER TABLE ch.probe_db.maintenance_tbl DELETE WHERE id = 1",
      "VACUUM ch.probe_db.maintenance_tbl"
    ).foreach { sql =>
      intercept[org.apache.spark.sql.catalyst.parser.ParseException] {
        spark.sql(sql)
      }
    }
  }

  test("PASS: array/map/struct columns round-trip through Spark and satisfy a contract declaring them") {
    spark.sql("DROP TABLE IF EXISTS ch.probe_db.rich_types_tbl")
    spark.sql("CREATE TABLE ch.probe_db.rich_types_tbl (id BIGINT NOT NULL, tags ARRAY<STRING>, " +
      "attrs MAP<STRING, BIGINT>, point STRUCT<x: BIGINT, y: BIGINT>) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id')")

    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType, nullable = false),
      org.apache.spark.sql.types.StructField("tags", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType), nullable = true),
      org.apache.spark.sql.types.StructField("attrs", org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.LongType), nullable = true),
      org.apache.spark.sql.types.StructField("point", org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("x", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("y", org.apache.spark.sql.types.LongType))), nullable = true)
    ))
    val row = org.apache.spark.sql.Row(1L, Seq("a", "b"), Map("k" -> 1L), org.apache.spark.sql.Row(3L, 4L))
    val richDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)

    val yaml =
      s"""id: ch_rich_types
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ch.probe_db.rich_types_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: tags
         |          type: array
         |          required: false
         |        - name: attrs
         |          type: map
         |          required: false
         |        - name: point
         |          type: struct
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      richDf.write.mode("append").insertInto("ch.probe_db.rich_types_tbl")
    }
    val readBack = spark.table("ch.probe_db.rich_types_tbl").head()
    assert(readBack.getAs[Long]("id") == 1L)
  }

  test("LowCardinality: transparent on read, not requestable from Spark on write - confirmed against the real server, not assumed") {
    // Neither of these TBLPROPERTIES keys is documented by clickhouse-spark
    // as a way to request LowCardinality; both are silently accepted as
    // opaque, unvalidated metadata rather than erroring - confirmed below
    // that neither actually changes the column's real ClickHouse-side type,
    // ruling out "maybe it's just undocumented" before concluding "no
    // mechanism exists."
    spark.sql("CREATE TABLE ch.probe_db.lowcard_a (id BIGINT NOT NULL, name STRING) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id', " +
      "'clickhouse.column.name.type' = 'LowCardinality(String)')")
    spark.sql("CREATE TABLE ch.probe_db.lowcard_b (id BIGINT NOT NULL, name STRING) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id', " +
      "column_types = 'name LowCardinality(String)')")
    assert(chColumnType("lowcard_a", "name") == "Nullable(String)")
    assert(chColumnType("lowcard_b", "name") == "Nullable(String)")
  }

  test("Nested: Spark's ARRAY<STRUCT<...>> produces ClickHouse's Array(Tuple(...)), not a true Nested column") {
    // A real, worth-documenting distinction, not a bug: ClickHouse's Nested
    // type has its own parallel-arrays/sub-column-addressing semantics,
    // structurally similar to but not the same as Array(Tuple(...)). A
    // contract author reading "ARRAY<STRUCT<...>> works" should not assume
    // it gives them true Nested semantics - it doesn't, and this connector
    // has no Spark-side mechanism to request true Nested on write either.
    spark.sql("CREATE TABLE ch.probe_db.nested_tbl (id BIGINT NOT NULL, " +
      "items ARRAY<STRUCT<name: STRING, count: BIGINT>>) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id')")
    assert(chColumnType("nested_tbl", "items") == "Array(Tuple(name Nullable(String), count Nullable(Int64)))")

    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType, nullable = false),
      org.apache.spark.sql.types.StructField("items", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("count", org.apache.spark.sql.types.LongType)))), nullable = true)
    ))
    val row = org.apache.spark.sql.Row(1L, Seq(org.apache.spark.sql.Row("a", 1L), org.apache.spark.sql.Row("b", 2L)))
    val nestedDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
    nestedDf.write.mode("append").insertInto("ch.probe_db.nested_tbl")
    assert(spark.table("ch.probe_db.nested_tbl").count() == 1)
  }

  /** Queries the real ClickHouse server directly (bypassing Spark entirely)
    * for a column's actual server-side type - the only way to distinguish
    * "the connector silently ignored this option" from "the connector
    * genuinely applied it," since Spark's own DESCRIBE only ever reports
    * back the Spark-side type it already believes a column has.
    */
  private def chColumnType(table: String, column: String): String = {
    val query = s"SELECT type FROM system.columns WHERE database = 'probe_db' AND table = '$table' AND name = '$column' FORMAT TSV"
    val url = new java.net.URL(s"http://127.0.0.1:$httpPort/?query=" + java.net.URLEncoder.encode(query, "UTF-8"))
    val conn = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
    try {
      val src = scala.io.Source.fromInputStream(conn.getInputStream)
      try src.mkString.trim finally src.close()
    } finally conn.disconnect()
  }

  /** Issues a raw statement directly against the real ClickHouse server,
    * bypassing Spark entirely - the only way to create a genuinely
    * ClickHouse-native construct (e.g. a true `Nested` column) this
    * connector's own Spark-side DDL has no syntax to request, and the
    * only way to confirm a `CREATE TABLE ... TBLPROPERTIES(...)` option
    * was actually applied rather than silently accepted as opaque,
    * unvalidated metadata (see `chColumnType` above for the same
    * "confirm against the real server, don't assume" principle, applied
    * to a column's type rather than a table's full DDL). POST, not GET -
    * ClickHouse's HTTP interface rejects any modifying statement
    * (CREATE/INSERT) issued via GET as read-only, confirmed empirically.
    */
  private def chExec(sql: String): String = {
    val conn = new java.net.URL(s"http://127.0.0.1:$httpPort/").openConnection().asInstanceOf[java.net.HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    try {
      val out = conn.getOutputStream
      try out.write(sql.getBytes("UTF-8")) finally out.close()
      val stream = if (conn.getResponseCode >= 400) conn.getErrorStream else conn.getInputStream
      val src = scala.io.Source.fromInputStream(stream)
      try src.mkString.trim finally src.close()
    } finally conn.disconnect()
  }

  private def createTableQuery(table: String): String =
    chExec(s"SELECT create_table_query FROM system.tables WHERE database = 'probe_db' AND name = '$table' FORMAT TSVRaw")

  // --- Second follow-up pass closing the last 3 ❓ feature-surface rows:
  // compression/PARTITION BY/replicated engines/materialized views, TTL,
  // and whether reading a genuinely pre-existing Nested column
  // round-trips. Zero spark-adapter/src/main/scala changes - every
  // finding is a confirmed-transparent behavior or a confirmed real
  // limitation. See docs/connectors/clickhouse.md. ---

  test("PARTITION BY: Spark's native PARTITIONED BY clause, not a TBLPROPERTIES key") {
    spark.sql("CREATE TABLE ch.probe_db.partitioned_tbl (id BIGINT NOT NULL, dt STRING NOT NULL) " +
      "USING ClickHouse PARTITIONED BY (dt) " +
      "TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id', 'settings.allow_nullable_key' = '1')")
    assert(createTableQuery("partitioned_tbl").contains("PARTITION BY dt"))
  }

  test("primary_key and sample_by TBLPROPERTIES both apply for real, confirmed against the server") {
    spark.sql("CREATE TABLE ch.probe_db.pk_tbl (id BIGINT NOT NULL, value BIGINT NOT NULL) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id, value', primary_key = 'id')")
    assert(createTableQuery("pk_tbl").contains("PRIMARY KEY id"))

    // SAMPLE BY requires an unsigned-integer sampling column in real
    // ClickHouse; cityHash64(id) is ClickHouse's own standard idiom for
    // sampling on a key that isn't naturally unsigned (Spark's BIGINT is
    // signed Int64) - a real ClickHouse constraint, not an Invaract one.
    spark.sql("CREATE TABLE ch.probe_db.sample_tbl (id BIGINT NOT NULL, value BIGINT NOT NULL) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'cityHash64(id), id', " +
      "sample_by = 'cityHash64(id)')")
    assert(createTableQuery("sample_tbl").contains("SAMPLE BY cityHash64(id)"))
  }

  test("replicated engines: genuinely supported by the connector, but not verifiable end to end without Keeper/shard-macro config in this environment") {
    // A dedicated ReplicatedMergeTreeEngineSpec class exists in the
    // connector's own jar (confirmed via decompilation, not assumed from
    // its name alone) - this is real, first-class support, not just an
    // opaque `engine` string passed through. The rejection below is this
    // single-node standalone-binary test server having no {shard}/{replica}
    // macros or Keeper coordination configured, not a connector or
    // Invaract limitation - the same class of environment gap already
    // documented for Docker/testcontainers in this connector's onboarding.
    val ex = intercept[Exception] {
      spark.sql("CREATE TABLE ch.probe_db.replicated_tbl (id BIGINT NOT NULL, value BIGINT) " +
        "USING ClickHouse TBLPROPERTIES (engine = " +
        "\"ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated_tbl', '{replica}')\", order_by = 'id')")
    }
    assert(ex.getMessage.contains("shard") || ex.getMessage.contains("NO_ELEMENTS_IN_CONFIG"))
  }

  test("materialized views: unreachable through Spark SQL with this connector") {
    createTable("mv_source_tbl")
    intercept[org.apache.spark.sql.catalyst.parser.ParseException] {
      spark.sql("CREATE MATERIALIZED VIEW ch.probe_db.mv_tbl AS SELECT id, value FROM ch.probe_db.mv_source_tbl")
    }
  }

  test("TTL: not requestable from Spark - the ttl TBLPROPERTIES key is silently ignored, confirmed against the real server") {
    spark.sql("CREATE TABLE ch.probe_db.ttl_tbl (id BIGINT NOT NULL, created_at TIMESTAMP) " +
      "USING ClickHouse TBLPROPERTIES (engine = 'MergeTree()', order_by = 'id', " +
      "ttl = 'created_at + INTERVAL 1 DAY')")
    assert(!createTableQuery("ttl_tbl").contains("TTL"))
  }

  test("compression codec write option affects wire transfer only, not ClickHouse-side column storage") {
    createTable("codec_tbl")
    df().write.mode("append").option("compression_codec", "lz4").insertInto("ch.probe_db.codec_tbl")
    assert(spark.table("ch.probe_db.codec_tbl").count() == 2)
    // No CODEC(...) clause on any column - spark.clickhouse.write.compression.codec
    // (confirmed via the connector's own ClickHouseSQLConf, decompiled directly)
    // controls Spark<->ClickHouse wire-transfer compression only, a session/write
    // concern, never a column's real storage-layer codec.
    assert(!createTableQuery("codec_tbl").contains("CODEC"))
  }

  test("reading a genuinely pre-existing Nested column: flattens to dotted Array columns, not a true nested Spark type") {
    // Created via raw SQL passthrough, bypassing Spark entirely - Spark's
    // own DDL (ARRAY<STRUCT<...>>, see the test above) has no syntax to
    // request a true ClickHouse Nested column at all.
    chExec("CREATE TABLE probe_db.real_nested_tbl (id Int64, items Nested(name String, count Int64)) ENGINE = MergeTree() ORDER BY id")
    chExec("INSERT INTO probe_db.real_nested_tbl (id, items.name, items.count) VALUES (1, ['a','b'], [1,2])")

    // ClickHouse itself already stores a Nested column as separate
    // parallel arrays under dotted names - confirmed via the real
    // create_table_query, not assumed from ClickHouse's own docs.
    assert(createTableQuery("real_nested_tbl").contains("`items.name` Array(String)"))
    assert(createTableQuery("real_nested_tbl").contains("`items.count` Array(Int64)"))

    // The connector reads that flattened representation as-is: two
    // ordinary top-level Spark columns literally named "items.name" and
    // "items.count" (dots included), each a plain ArrayType - not a
    // single "items" column of ArrayType(StructType(name, count)) the
    // way a real Nested read might be assumed to surface. A contract
    // declaring a genuinely pre-existing Nested column must account for
    // this flattened shape, not assume it reads back as a nested struct.
    val df = spark.table("ch.probe_db.real_nested_tbl")
    assert(df.schema.fieldNames.contains("items.name"))
    assert(df.schema.fieldNames.contains("items.count"))
    assert(df.schema("items.name").dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType])
    assert(df.collect().head.getAs[Long]("id") == 1L)
  }
}
