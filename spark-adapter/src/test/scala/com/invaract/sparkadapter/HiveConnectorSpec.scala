// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.sparkadapter.notification.FileNotificationSink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.nio.file.{Files, Path}

/** Hive support, added via the `add-spark-connector` skill's process
  * (docs/ADDING_A_SPARK_CONNECTOR.md). Unlike Delta/Iceberg, Hive is not an
  * external connector library - it's Spark's own first-party integration
  * module (`spark-hive`, `enableHiveSupport()`), tested here against a real
  * embedded-Derby metastore (no external Hive install needed). See
  * docs/connectors/hive.md for the full investigation and coverage
  * ledger.
  *
  * Two real findings drove this suite, neither assumed from documentation:
  *
  *  1. `HiveTableRelation` (the read-side shape for a genuinely Hive-native
  *     table - non-Parquet/ORC, or with metastore conversion disabled) is a
  *     plain public `spark-catalyst` class, not `LogicalRelation`-wrapped
  *     the way Delta's read shape turned out to be - and, before this
  *     pass, had NO translation case at all, falling to the fully generic
  *     `Unsupported` fallback (worse than an imprecise location - not
  *     recognized as a read at all).
  *  2. A Hive static-partition INSERT (`INSERT INTO t PARTITION(dt='...')
  *     SELECT ...`) omits the partition column from the query's own
  *     schema entirely - a real false-rejection bug, the same class as
  *     Delta's generated columns/DSv2's target-only fields, fixed in
  *     `WriteCommandSupport.insertIntoHiveTable`.
  */
class HiveConnectorSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-hive-test")
    System.setProperty("derby.stream.error.file", scratchDir.resolve("derby.log").toString)

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveConnectorSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${scratchDir.resolve("metastore_db")};create=true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      // Delta's own extension/catalog, added for this suite's external-table
      // pass (see "External tables" below) - harmless alongside plain Hive
      // SerDe tables (every existing test above STORED AS ... is unaffected;
      // DeltaCatalog only intercepts USING DELTA/delta-provider tables).
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .enableHiveSupport()
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  /** `SparkAdapterListener.onSuccess` fires asynchronously on Spark's own
    * listener-bus thread, with no ordering guarantee relative to when a
    * *different* test's own writes finish dispatching to whichever
    * listeners are registered at dispatch time (registered listeners are
    * never unregistered between tests in this suite, matching every other
    * `*ConnectorSpec` in this module). A real, found flakiness risk in
    * this specific suite (not a production bug): a test with multiple
    * nested writes queued right before another test that also registers a
    * fresh listener can have that next test's listener race a still-
    * in-flight event from the PRIOR test, capturing a write for the wrong
    * table. Filtering on the expected location, not just "any write
    * captured yet", makes the wait robust to that race the same way a
    * real caller polling for a specific outcome would.
    */
  private def awaitWriteTo(
    listener: SparkAdapterListener,
    expectedLocationFragment: String,
    extra: com.invaract.ir.Write => Boolean = _ => true
  ): TranslationResult =
    org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite match {
        case Some(r @ TranslationResult(w @ com.invaract.ir.Write(com.invaract.ir.DatasetRef(loc), _, _, _, _), _))
          if loc.contains(expectedLocationFragment) && extra(w) => r
        case Some(other) => fail(s"listener's last captured write doesn't match yet: $other")
        case None => fail(s"listener has not captured a write targeting '$expectedLocationFragment' yet")
      }
    }

  /** The exact same field `WriteCommandSupport`'s Hive cases and
    * `SparkPlanAdapter.hiveTableRelationLocationOf` read to resolve a
    * table's location - used here to build contracts that are guaranteed
    * to agree with what the production code itself will resolve, rather
    * than guessing at Spark's warehouse-path convention. Strips the
    * `file:` scheme, matching every other spec in this module's own
    * convention of writing a contract's `location:` field as a bare
    * filesystem path (`StructuralVerifier.locationsMatch` only strips
    * `file:` from the *actual* side, not the declared side, so a raw
    * `URI.toString` here - which keeps the scheme - would never match).
    */
  private def tableLocation(name: String): String =
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(name)).storage.locationUri
      .getOrElse(fail(s"table $name has no resolved storage location")).toString.stripPrefix("file:")

  // --- Read: a genuinely Hive-native (TEXTFILE) catalog table - the primary
  // real bug this pass found: HiveTableRelation had no translation case at
  // all, unlike Delta's read shape (a LogicalRelation-wrapped HadoopFsRelation
  // subclass). ---

  test("translates a catalog read of a Hive TEXTFILE table via HiveTableRelation") {
    spark.sql("CREATE TABLE hive_text_read_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_text_read_tbl VALUES (1, 10)")

    val result = SparkPlanAdapter.translate(spark.table("hive_text_read_tbl").queryExecution.analyzed)
    result.plan match {
      case com.invaract.ir.Read(com.invaract.ir.DatasetRef(location), _, _) =>
        assert(location.stripPrefix("file:") == tableLocation("hive_text_read_tbl"))
      case other => fail(s"expected a Read, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty, s"expected a precise translation, got diagnostics: ${result.diagnostics}")
  }

  test("PASS: a Hive TEXTFILE table satisfies a contract's declared input schema") {
    spark.sql("CREATE TABLE hive_text_input_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_text_input_tbl VALUES (1, 10)")
    val outPath = scratchDir.resolve("hive_input_out").toString

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: ${tableLocation("hive_text_input_tbl")}
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
         |    location: $outPath
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
      spark.table("hive_text_input_tbl").write.mode("overwrite").parquet(outPath) // must not throw MISSING_INPUT
    }
  }

  // --- Read: a Hive PARQUET table with metastore conversion ON (the
  // default) - confirmed to resolve to LogicalRelation/HadoopFsRelation,
  // already covered by the pre-existing generic case, NOT HiveTableRelation.
  // A confirmation test, not a new code path. ---

  test("a Hive PARQUET table with conversion ON translates via the existing LogicalRelation/HadoopFsRelation case, not HiveTableRelation") {
    spark.sql("CREATE TABLE hive_parquet_conv_tbl (id BIGINT, value BIGINT) STORED AS PARQUET")
    spark.sql("INSERT INTO hive_parquet_conv_tbl VALUES (1, 10)")

    val result = SparkPlanAdapter.translate(spark.table("hive_parquet_conv_tbl").queryExecution.analyzed)
    result.plan match {
      case com.invaract.ir.Read(com.invaract.ir.DatasetRef(location), _, _) =>
        assert(location.stripPrefix("file:") == tableLocation("hive_parquet_conv_tbl"))
      case other => fail(s"expected a Read, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty)
  }

  // --- Read: a Hive PARQUET table with metastore conversion OFF - confirmed
  // to take the SAME HiveTableRelation path as a TEXTFILE table, not the
  // LogicalRelation path - the format-conversion setting, not the on-disk
  // file format, is what actually determines which case applies. ---

  test("a Hive PARQUET table with conversion OFF translates via HiveTableRelation") {
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    try {
      spark.sql("CREATE TABLE hive_parquet_noconv_tbl (id BIGINT, value BIGINT) STORED AS PARQUET")
      spark.sql("INSERT INTO hive_parquet_noconv_tbl VALUES (1, 10)")
      val result = SparkPlanAdapter.translate(spark.table("hive_parquet_noconv_tbl").queryExecution.analyzed)
      result.plan match {
        case com.invaract.ir.Read(com.invaract.ir.DatasetRef(location), _, _) =>
          assert(location.stripPrefix("file:") == tableLocation("hive_parquet_noconv_tbl"))
        case other => fail(s"expected a Read, got ${com.invaract.ir.PlanPrinter.render(other)}")
      }
    } finally spark.conf.set("spark.sql.hive.convertMetastoreParquet", "true")
  }

  // --- Write: .format("hive").saveAsTable(...) - a real structural trap,
  // the same class as Parquet/Delta/Iceberg's nested-write pitfall: a
  // single call produces TWO Command-shaped plans
  // (CreateHiveTableAsSelectCommand, then a nested InsertIntoHiveTable),
  // both independently checked.
  //
  // Unlike Parquet/Delta/Iceberg's version of this pitfall, the two sides
  // do NOT always agree here - a real, found (not fixed - see below)
  // asymmetry: confirmed empirically that `CreateHiveTableAsSelectCommand.
  // tableDesc.storage.locationUri` is populated with the real physical
  // path when appending onto an EXISTING table (the analyzer resolves
  // `tableDesc` by looking up the existing catalog entry), but is `None` -
  // falling back to the qualified catalog identifier - both for a
  // genuinely NEW table AND, surprisingly, for `.mode("overwrite")` onto
  // an EXISTING table (overwrite is treated as replace-like, so the
  // analyzer builds `tableDesc` fresh rather than consulting the existing
  // entry). The nested `InsertIntoHiveTable`, built during `run()` after
  // the table has been created/verified, always has the real physical
  // path. So APPEND mode is the one case confirmed to agree by
  // construction (both PASS below); NEW-table and OVERWRITE-onto-existing
  // are the same "no explicit path given" class of gap
  // `ParquetConnectorSpec`'s own `.saveAsTable()`-on-a-brand-new-table test
  // already documents and leaves out of scope (there, a V1 `.option("path",
  // ...)` sidesteps it; Hive's `.saveAsTable()` has no equivalent knob) -
  // see docs/connectors/hive.md for the full writeup and next step. ---

  test("PASS: .format(hive).saveAsTable() append onto an existing table - both nested Command plans see a satisfying write") {
    df().write.format("hive").saveAsTable("hive_ctas_append_pass_tbl")
    val loc = tableLocation("hive_ctas_append_pass_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      df().write.format("hive").mode("append").saveAsTable("hive_ctas_append_pass_tbl")
    }
    assert(spark.table("hive_ctas_append_pass_tbl").count() == 4)
  }

  test("FAIL: .format(hive).saveAsTable() append onto an existing table is rejected, nested insert never runs") {
    df().write.format("hive").saveAsTable("hive_ctas_append_fail_tbl")
    val loc = tableLocation("hive_ctas_append_fail_tbl")
    val rowCountBefore = spark.table("hive_ctas_append_fail_tbl").count()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        df().write.format("hive").mode("append").saveAsTable("hive_ctas_append_fail_tbl")
      }
    }
    assert(ex.result.violations.exists(v => v.violationType == ViolationType.MissingOutputField && v.message.contains("missing_field")))
    assert(spark.table("hive_ctas_append_fail_tbl").count() == rowCountBefore, "a rejected append must never have committed, and the nested insert must never run")
  }

  // --- Known limitation (not fixed - see the comment above): a genuinely
  // NEW table via .format("hive").saveAsTable(...) has the same "outer
  // command has no explicit physical path to resolve" gap
  // ParquetConnectorSpec's own new-table CTAS test already documents for
  // the V1 CreateDataSourceTableAsSelectCommand case - confirmed here to
  // also apply to Hive's CreateHiveTableAsSelectCommand, and confirmed to
  // ALSO apply to .mode("overwrite") onto an EXISTING table (a real,
  // Hive-specific extension of the same gap: overwrite is treated as
  // replace-like, so the analyzer never consults the existing catalog
  // entry for tableDesc's location). Translation-only, not a PASS/FAIL
  // enforcement pair - a contract targeting the outer command's own
  // resolved location for either scenario would need to use the qualified
  // catalog identifier (e.g. "spark_catalog.default.<table>"), not the
  // physical path, which the nested InsertIntoHiveTable then independently
  // resolves to the physical path instead - the two commands don't agree
  // unless the physical path happens to already be known before the
  // command is built (append mode, tested above). ---

  test("known limitation: OVERWRITE-mode .saveAsTable() onto an EXISTING table has no resolved location in the outer command") {
    df().write.format("hive").saveAsTable("hive_ctas_overwrite_gap_tbl")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.format("hive").mode("overwrite").saveAsTable("hive_ctas_overwrite_gap_tbl")

    // SparkAdapterListener's QueryExecutionListener only observes the
    // top-level query (the outer CreateHiveTableAsSelectCommand), unlike
    // injectCheckRule which sees both nested commands - so this captures
    // exactly the command whose location resolution is the known gap:
    // the qualified catalog identifier, not the table's real physical
    // path (confirmed via `tableLocation` below, which IS the real path).
    // Filtered on the exact expected qualified identifier, not just
    // location-contains: the setup write just above (a genuinely NEW
    // table, its own top-level query) is exactly the same "no resolved
    // path yet" shape and can still have its own async onSuccess event
    // in flight when this listener registers - without this filter,
    // `eventually` can accept that stray event instead of the actual
    // target write's.
    val result = awaitWriteTo(listener, "hive_ctas_overwrite_gap_tbl", w => w.dataset.location == "spark_catalog.default.hive_ctas_overwrite_gap_tbl")
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, _, _, _) =>
        assert(location == "spark_catalog.default.hive_ctas_overwrite_gap_tbl",
          s"expected the outer command's qualified-identifier fallback, got '$location'")
        assert(location.stripPrefix("file:") != tableLocation("hive_ctas_overwrite_gap_tbl"),
          "this is exactly the known gap: the outer command's location does NOT match the table's real physical path")
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Write: .insertInto(...) / .saveAsTable() append onto an EXISTING
  // Hive table - InsertIntoHiveTable, confirmed to also appear nested
  // inside a CreateHiveTableAsSelectCommand for the append-via-saveAsTable
  // form. ---

  test("translates .insertInto() with saveMode=append (not overwrite)") {
    spark.sql("CREATE TABLE hive_insertinto_append_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.insertInto("hive_insertinto_append_tbl")

    val result = awaitWriteTo(listener, "hive_insertinto_append_tbl", w => w.saveMode.contains("append"))
    result.plan match {
      case com.invaract.ir.Write(_, _, format, saveMode, _) =>
        assert(format.contains("hive"))
        assert(saveMode.contains("append"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  test("PASS: .insertInto() an existing Hive table") {
    spark.sql("CREATE TABLE hive_insertinto_pass_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val loc = tableLocation("hive_insertinto_pass_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      df().write.insertInto("hive_insertinto_pass_tbl") // must not throw
    }
    assert(spark.table("hive_insertinto_pass_tbl").count() == 2)
  }

  test("FAIL: .insertInto() an existing Hive table is rejected and the table is left unchanged") {
    spark.sql("CREATE TABLE hive_insertinto_fail_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_insertinto_fail_tbl VALUES (99, 990)")
    val loc = tableLocation("hive_insertinto_fail_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        df().write.insertInto("hive_insertinto_fail_tbl")
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("hive_insertinto_fail_tbl").count() == 1, "a rejected insert must never have committed")
  }

  // --- Write: INSERT OVERWRITE - confirms the overwrite flag maps to the
  // contract's "overwrite" saveMode, not "append". ---

  test("translates INSERT OVERWRITE TABLE with saveMode=overwrite") {
    spark.sql("CREATE TABLE hive_overwrite_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_overwrite_tbl VALUES (1, 10)")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    spark.sql("INSERT OVERWRITE TABLE hive_overwrite_tbl VALUES (2, 20)")

    // Filters on saveMode too, not just location: Spark's listener bus is
    // asynchronous, and the earlier INSERT INTO's own onSuccess event can
    // still be in flight when this listener is registered - without this,
    // `eventually` can stop as soon as it sees THAT (append-mode) event,
    // never waiting for the INSERT OVERWRITE event that follows it in the
    // bus's FIFO order to actually arrive and overwrite `lastWrite`.
    val result = awaitWriteTo(listener, "hive_overwrite_tbl", w => w.saveMode.contains("overwrite"))
    result.plan match {
      case com.invaract.ir.Write(_, _, format, saveMode, _) =>
        assert(format.contains("hive"))
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Feature surface: static-partition INSERT - the real false-rejection
  // bug this pass found and fixed. The partition column, supplied as a
  // literal in the PARTITION clause, never appears in the query's own
  // schema - without unioning it in from the table's own schema, a
  // contract requiring it would be falsely rejected. `dt` is declared
  // required: false here, not true: a second, related feature-surface
  // finding is that the unioned field comes from CatalogTable.schema,
  // whose partition columns are always nullable (Hive has no NOT NULL
  // column constraint to preserve in the first place - the same "every
  // field nullable" finding the dedicated nullability tests below confirm
  // for data columns), so a required: true declaration on a partition
  // column would always spuriously fail OUTPUT_FIELD_NULLABILITY_MISMATCH,
  // independently of the MISSING_OUTPUT_FIELD bug this test targets. ---

  test("PASS: static-partition INSERT satisfies a contract requiring the partition column (real bug, fixed)") {
    spark.sql("CREATE TABLE hive_static_part_tbl (id BIGINT, value BIGINT) PARTITIONED BY (dt STRING) STORED AS TEXTFILE")
    val loc = tableLocation("hive_static_part_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |        - name: dt
         |          type: string
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      // must not throw MISSING_OUTPUT_FIELD for 'dt', even though the
      // query itself (SELECT 1, 10) never selects it - the pre-fix
      // behavior used query.schema alone and would have rejected this.
      spark.sql("INSERT INTO hive_static_part_tbl PARTITION(dt='2024-01-01') SELECT 1, 10")
    }
    assert(spark.table("hive_static_part_tbl").count() == 1)
  }

  test("PASS: dynamic-partition INSERT satisfies a contract requiring the partition column (already correct, no fix needed)") {
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sql("CREATE TABLE hive_dynamic_part_tbl (id BIGINT, value BIGINT) PARTITIONED BY (dt STRING) STORED AS TEXTFILE")
    val loc = tableLocation("hive_dynamic_part_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |        - name: dt
         |          type: string
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO hive_dynamic_part_tbl SELECT 1, 10, '2024-01-01'") // must not throw
    }
    assert(spark.table("hive_dynamic_part_tbl").count() == 1)
  }

  test("FAIL: static-partition INSERT missing a genuinely-required data field is still correctly rejected") {
    spark.sql("CREATE TABLE hive_static_part_fail_tbl (id BIGINT, value BIGINT) PARTITIONED BY (dt STRING) STORED AS TEXTFILE")
    val loc = tableLocation("hive_static_part_fail_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |        - name: dt
         |          type: string
         |          required: false
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql("INSERT INTO hive_static_part_fail_tbl PARTITION(dt='2024-01-01') SELECT 1, 10")
      }
    }
    assert(ex.result.violations.exists(v => v.violationType == ViolationType.MissingOutputField && v.message.contains("missing_field")))
    assert(spark.table("hive_static_part_fail_tbl").count() == 0, "a rejected write must never have committed")
  }

  // --- Write: INSERT ... DIRECTORY - a real, previously-unknown write shape
  // found only via the reflective jar-scan (Phase 3), not by trying the
  // standard .save/.saveAsTable/.insertInto operations. Writes to an
  // arbitrary filesystem path, outside the catalog entirely. ---

  test("translates INSERT OVERWRITE DIRECTORY via InsertIntoHiveDirCommand") {
    // Forward-slash form for the SQL string literal: on Windows, Hive's
    // INSERT OVERWRITE DIRECTORY location parsing (unlike a plain
    // java.nio.file.Path) can't round-trip a raw backslash-separated path
    // embedded in a single-quoted SQL string, which surfaces downstream as
    // Hadoop's RawLocalFileSystem deriving an empty-string Path and
    // throwing "Can not create a Path from an empty string" - the same
    // convention already used everywhere else in this module a location is
    // interpolated into a SQL string (see StructuralVerifier.scala,
    // ContractEnforcementRuleSpec).
    val outDir = scratchDir.resolve("hive_insert_dir_out").toString.replace('\\', '/')
    df().createOrReplaceTempView("hive_insert_dir_src")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    spark.sql(s"INSERT OVERWRITE DIRECTORY '$outDir' STORED AS TEXTFILE SELECT * FROM hive_insert_dir_src")

    val result = awaitWriteTo(listener, outDir)
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, format, saveMode, _) =>
        assert(location.contains(outDir) || location == outDir, s"expected the real directory path, got '$location'")
        assert(format.contains("hive"))
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  test("FAIL: INSERT OVERWRITE DIRECTORY at a location that doesn't match the contract is rejected") {
    // See the sibling PASS test above for why this needs forward slashes.
    val outDir = scratchDir.resolve("hive_insert_dir_fail_out").toString.replace('\\', '/')
    df().createOrReplaceTempView("hive_insert_dir_fail_src")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${scratchDir.resolve("somewhere_else_entirely")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"INSERT OVERWRITE DIRECTORY '$outDir' STORED AS TEXTFILE SELECT * FROM hive_insert_dir_fail_src")
      }
    }
    assert(!Files.exists(java.nio.file.Paths.get(outDir)) || Files.list(java.nio.file.Paths.get(outDir)).count() == 0,
      "a rejected INSERT DIRECTORY must never have committed any data")
  }

  // insertIntoHiveDir's own "no resolved storage location" fallback -
  // reached only when storage.locationUri is None, a shape real Spark SQL
  // can't produce (INSERT ... DIRECTORY '<path>' always supplies a literal
  // path), so - like deleteFromTable's own "no NamedRelation found"
  // fallback in ContractEnforcementRuleSpec - this is exercised by
  // constructing the real Catalyst node directly (spark-hive is already a
  // test-scope dependency of this module, so InsertIntoHiveDirCommand is
  // directly importable here, unlike WriteCommandSupport's own reflective,
  // no-compile-time-dependency access to it).
  //
  // This is the regression test for the same instability class already
  // fixed for deltaRowLevelDml/deleteFromTable: this fallback used to
  // report `plan.toString` (raw LogicalPlan.toString, which renders any
  // attribute reference as "name#<exprId>", a per-session counter) as the
  // write's location. Fixed by switching to `plan.canonicalized.toString`.
  test("WriteCommandSupport's insertIntoHiveDir fallback location is stable across separate exprId allocations") {
    def commandWithNoStorageLocation(): LogicalPlan = {
      val query = org.apache.spark.sql.catalyst.plans.logical.LocalRelation(
        Seq(org.apache.spark.sql.catalyst.expressions.AttributeReference("id", org.apache.spark.sql.types.LongType)())
      )
      org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand(
        isLocal = false,
        storage = org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat.empty,
        query = query,
        overwrite = true,
        outputColumnNames = Seq("id")
      )
    }

    def insertIntoHiveDirInfo(): WriteCommandInfo =
      WriteCommandSupport.combined.lift(commandWithNoStorageLocation())
        .getOrElse(fail("InsertIntoHiveDirCommand must always be recognized, even with no resolved storage location"))

    val info1 = insertIntoHiveDirInfo()
    val info2 = insertIntoHiveDirInfo()
    assert(info1.diagnostic.isDefined, "the no-storage-location fallback must report a diagnostic, not resolve a clean location silently")
    assert(
      info1.location == info2.location,
      s"the fallback location must be stable across separate exprId allocations for the identical command shape, " +
        s"got '${info1.location}' vs '${info2.location}'"
    )
  }

  // --- Fail-closed: LOAD DATA INPATH - a genuinely data-mutating Hive
  // operation Invaract deliberately doesn't translate (already documented
  // generically in FailClosedCommands' exclusion list; this is the first
  // real confirmation against an actual Hive table, not just a theoretical
  // classification). ---

  test("fails closed: LOAD DATA INPATH is rejected, the table is left unchanged") {
    spark.sql("CREATE TABLE hive_load_data_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val srcDir = scratchDir.resolve("load_data_src")
    Files.createDirectories(srcDir)
    Files.write(srcDir.resolve("data.txt"), "1\t10\n".getBytes("UTF-8"))

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${tableLocation("hive_load_data_tbl")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        spark.sql(s"LOAD DATA LOCAL INPATH '${srcDir.toString}/data.txt' INTO TABLE hive_load_data_tbl")
      }
      assert(ex.result.violations.exists(_.violationType == ViolationType.UnverifiableWrite))
    }
    assert(spark.table("hive_load_data_tbl").count() == 0, "a rejected LOAD DATA must never have committed")
  }

  // --- Fail-closed: TRUNCATE TABLE - genuinely data-mutating (deletes every
  // row), already generically excluded from FailClosedCommands' safe list;
  // confirmed here for real against a Hive table with actual data. ---

  test("fails closed: TRUNCATE TABLE is rejected, the table's data is left unchanged") {
    spark.sql("CREATE TABLE hive_truncate_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_truncate_tbl VALUES (1, 10)")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${tableLocation("hive_truncate_tbl")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql("TRUNCATE TABLE hive_truncate_tbl")
      }
    }
    assert(spark.table("hive_truncate_tbl").count() == 1, "a rejected TRUNCATE must never have removed data")
  }

  // --- Fail-closed: MERGE INTO / UPDATE against a plain Hive table -
  // confirmed via probing (Phase 2) that Spark analyzes these to its own
  // generic MergeIntoTable/UpdateTable nodes (real Command-shaped plans,
  // unlike DELETE which Spark rejects before producing any plan at all) -
  // already generically excluded from FailClosedCommands' safe list (see
  // its own doc comment's exclusion list). Confirmed here for real,
  // through actual enforcement, not just probing the plan shape. ---

  test("fails closed: MERGE INTO against a plain Hive table is rejected by Invaract before Spark's own rejection ever runs") {
    spark.sql("CREATE TABLE hive_merge_target_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_merge_target_tbl VALUES (1, 10)")
    spark.sql("CREATE TABLE hive_merge_source_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${tableLocation("hive_merge_target_tbl")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        spark.sql(
          "MERGE INTO hive_merge_target_tbl t USING hive_merge_source_tbl s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT *"
        ).collect()
      }
      assert(ex.result.violations.exists(_.violationType == ViolationType.UnverifiableWrite))
    }
    assert(spark.table("hive_merge_target_tbl").count() == 1, "a rejected MERGE must never have committed")
  }

  // --- N/A, confirmed by Spark itself, not an Invaract gap: DataFrameWriterV2
  // and streaming writes against a Hive table. ---

  test(".writeTo() against a Hive table is rejected by Spark itself, not by Invaract") {
    spark.sql("CREATE TABLE hive_writeto_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val ex = intercept[org.apache.spark.sql.AnalysisException](df().writeTo("hive_writeto_tbl").append())
    assert(ex.getMessage.contains("Cannot write into v1 table"))
    assert(spark.table("hive_writeto_tbl").count() == 0)
  }

  test("streaming .toTable() against a Hive table is rejected by Spark itself, not by Invaract") {
    spark.sql("CREATE TABLE hive_stream_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val inputDir = scratchDir.resolve("hive_stream_in")
    df().write.parquet(inputDir.toString)
    val schema = spark.read.parquet(inputDir.toString).schema

    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      spark.readStream.schema(schema).parquet(inputDir.toString).writeStream
        .option("checkpointLocation", scratchDir.resolve("hive_stream_ckpt").toString)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .toTable("hive_stream_tbl")
    }
    assert(ex.getMessage.contains("data source provider"))
    assert(spark.table("hive_stream_tbl").count() == 0)
  }

  // --- Regression: Hive's own non-data-mutating DDL/maintenance commands
  // must not be blocked by the fail-closed policy, under a contract that
  // would reject anything it actually checked. All of these route through
  // Spark's already-classified generic commands (CreateTableCommand/
  // AnalyzeTableCommand/ShowTables/RepairTableCommand/
  // AlterTableAddPartitionCommand) - confirmed here specifically for Hive,
  // not just assumed to carry over from the Delta/Iceberg precedent. ---

  test("regression: Hive DDL/maintenance commands are never blocked by the fail-closed policy") {
    val strictYaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: /nonexistent/should/never/match/anything
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(strictYaml) {
      spark.sql("CREATE TABLE hive_ddl_regression_tbl (id BIGINT, value BIGINT) PARTITIONED BY (dt STRING) STORED AS TEXTFILE")
      spark.sql("ANALYZE TABLE hive_ddl_regression_tbl COMPUTE STATISTICS")
      spark.sql("SHOW TABLES").collect()
      spark.sql("MSCK REPAIR TABLE hive_ddl_regression_tbl")
      spark.sql("ALTER TABLE hive_ddl_regression_tbl ADD IF NOT EXISTS PARTITION (dt='2024-01-01')")
      spark.sql("DESCRIBE TABLE hive_ddl_regression_tbl").collect()
      // must not throw for any of the above
    }
  }

  // --- Feature surface: bucketed tables - confirmed transparent, no fix
  // needed. Bucketing metadata lives entirely in BucketSpec, never affects
  // the translated schema/location/format. ---

  test("feature surface: a bucketed Hive table is recognized identically to a non-bucketed one") {
    df().createOrReplaceTempView("hive_bucket_src")
    spark.sql("CREATE TABLE hive_bucket_feature_tbl (id BIGINT, value BIGINT) CLUSTERED BY (id) INTO 4 BUCKETS STORED AS TEXTFILE")
    val loc = tableLocation("hive_bucket_feature_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $loc
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO hive_bucket_feature_tbl SELECT * FROM hive_bucket_src") // must not throw
    }
    assert(spark.table("hive_bucket_feature_tbl").count() == 2)

    // Read side too: HiveTableRelation for a bucketed table translates the
    // same way as an unbucketed one.
    val result = SparkPlanAdapter.translate(spark.table("hive_bucket_feature_tbl").queryExecution.analyzed)
    result.plan match {
      case com.invaract.ir.Read(com.invaract.ir.DatasetRef(location), _, _) => assert(location.stripPrefix("file:") == loc)
      case other => fail(s"expected a Read, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Feature surface: a real Hive UDF (not Spark's own ScalaUDF) -
  // confirmed transparent: isOpaqueUdf's existing "endsWith HiveGenericUDF"
  // check (already in the codebase, previously untested against a real
  // Hive UDF for lack of a metastore) correctly recognizes it. ---

  test("feature surface: a real Hive UDF is translated as an explicit UDF node with a diagnostic") {
    spark.sql("CREATE TEMPORARY FUNCTION hive_udf_feature_upper AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper'")
    df().createOrReplaceTempView("hive_udf_feature_src")
    val analyzed = spark.sql("SELECT id, hive_udf_feature_upper(CAST(value AS STRING)) AS v FROM hive_udf_feature_src").queryExecution.analyzed

    val result = SparkPlanAdapter.translate(analyzed)
    assert(result.diagnostics.exists(_.nodeType.contains("HiveGenericUDF")), s"expected a HiveGenericUDF diagnostic, got ${result.diagnostics}")
    result.plan match {
      case p: com.invaract.ir.Project =>
        assert(p.columns.exists {
          case com.invaract.ir.NamedExpr("v", _: com.invaract.ir.UDF) => true
          case _ => false
        }, s"expected 'v' translated as an explicit UDF node, got ${com.invaract.ir.PlanPrinter.render(p)}")
      case other => fail(s"expected a Project, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Feature surface: nullability - a Hive table (like plain Parquet)
  // reports every column nullable on read-back regardless of the CREATE
  // TABLE DDL, since classic Hive DDL (unlike some other formats) has no
  // NOT NULL column constraint at all to preserve in the first place. ---

  test("feature surface: a Hive table read-back reports every field nullable") {
    spark.sql("CREATE TABLE hive_nullability_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_nullability_tbl VALUES (1, 10)")
    val readBack = spark.table("hive_nullability_tbl")
    assert(readBack.schema("id").nullable, "Hive catalog reads should report every field nullable")
    assert(readBack.schema("value").nullable)

    val outPath = scratchDir.resolve("hive_nullability_out").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outPath
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
      readBack.write.mode("overwrite").parquet(outPath) // must not throw
    }
  }

  test("feature surface: a required:true field sourced from a Hive read is correctly rejected as a nullability mismatch") {
    spark.sql("CREATE TABLE hive_nullability_fail_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    spark.sql("INSERT INTO hive_nullability_fail_tbl VALUES (1, 10)")
    val readBack = spark.table("hive_nullability_fail_tbl")

    val outPath = scratchDir.resolve("hive_nullability_fail_out").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin
    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        readBack.write.mode("overwrite").parquet(outPath)
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.OutputFieldNullabilityMismatch))
  }

  // --- External tables ---------------------------------------------------
  //
  // Everything above uses a plain managed table (default warehouse path).
  // None of it exercises an EXTERNAL table or a LOCATION outside the
  // warehouse dir - a real, previously-uninvestigated gap, closed here
  // against a real embedded-Derby session for both Hive-SerDe and
  // datasource-provider (Parquet/Delta) formats, and both the raw-SQL
  // `CREATE EXTERNAL TABLE` and `.saveAsTable()` write paths. See
  // docs/connectors/hive.md's "External tables" section for the full
  // writeup this pass produced.

  test("translates a read/write of a Hive SerDe EXTERNAL table (STORED AS PARQUET, conversion ON) via the datasource path, not InsertIntoHiveTable") {
    val loc = extScratchDir("hive_ext_serde_parquet")
    spark.sql(s"CREATE EXTERNAL TABLE hive_ext_serde_parquet_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")
    assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier("hive_ext_serde_parquet_tbl")).tableType == CatalogTableType.EXTERNAL)

    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    spark.sql("INSERT INTO hive_ext_serde_parquet_tbl SELECT 1, 10")
    // Conversion ON (the default) means even a Hive-SerDe EXTERNAL Parquet
    // table writes through the plain datasource command, not
    // InsertIntoHiveTable - the write-side counterpart of this suite's
    // existing read-side "conversion ON" confirmation above, now checked
    // against an EXTERNAL table specifically (a genuinely different
    // location-resolution path than a managed one).
    val result = awaitWriteTo(listener, loc.stripPrefix("file:"))
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, format, _, _) =>
        assert(location.stripPrefix("file:") == loc.stripPrefix("file:"))
        assert(format.contains("parquet"), s"expected a plain parquet write (not Hive), got format=$format")
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  test("PASS: an EXTERNAL Hive SerDe table's real physical LOCATION (outside the warehouse dir) satisfies a contract declaring that exact location") {
    val loc = extScratchDir("hive_ext_serde_pass")
    spark.sql(s"CREATE EXTERNAL TABLE hive_ext_serde_pass_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO hive_ext_serde_pass_tbl SELECT 1, 10") // must not throw
    }
    assert(spark.table("hive_ext_serde_pass_tbl").count() == 1)
  }

  test("PASS: a datasource-provider EXTERNAL table (CREATE TABLE ... USING PARQUET LOCATION, no EXTERNAL keyword) is fully verified") {
    val loc = extScratchDir("ds_ext_parquet")
    spark.sql(s"CREATE TABLE ds_ext_parquet_tbl (id BIGINT, value BIGINT) USING PARQUET LOCATION '$loc'")
    // Confirms Spark's own documented behavior: a bare LOCATION implies
    // EXTERNAL even with no EXTERNAL keyword in the DDL at all.
    assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier("ds_ext_parquet_tbl")).tableType == CatalogTableType.EXTERNAL)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO ds_ext_parquet_tbl SELECT 1, 10") // must not throw
    }
    assert(spark.table("ds_ext_parquet_tbl").count() == 1)
  }

  test("PASS: a Delta EXTERNAL table registered in the Hive metastore (CREATE TABLE ... USING DELTA LOCATION) is fully verified") {
    val loc = extScratchDir("delta_ext")
    spark.sql(s"CREATE TABLE delta_ext_tbl (id BIGINT, value BIGINT) USING DELTA LOCATION '$loc'")
    assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_ext_tbl")).tableType == CatalogTableType.EXTERNAL)
    assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_ext_tbl")).provider.contains("delta"))

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO delta_ext_tbl SELECT 1, 10") // must not throw - real AppendData, already-covered DSv2 path
    }
    assert(spark.read.format("delta").load(loc).count() == 1)
  }

  // A real, previously-documented limitation ("Known limitation: CTAS with
  // no pre-existing physical path" in docs/connectors/hive.md) says a
  // genuinely NEW table's outer CreateHiveTableAsSelectCommand has no
  // resolved physical path, disagreeing with the nested InsertIntoHiveTable.
  // Confirmed here that supplying an explicit .option("path", ...) - the
  // exact ".saveToTable()"-style external-table case - sidesteps that gap
  // entirely: tableDesc.storage.locationUri is populated immediately, the
  // same reason CreateDataSourceTableAsSelectCommand's own path-option case
  // already doesn't have this problem either. A standing regression test
  // for a real, working case, not just a probe's remembered output.
  test("PASS: .format(hive).option(path, ...).saveAsTable() on a NEW table resolves the outer command to the real physical path, not the known CTAS gap") {
    val loc = extScratchDir("hive_saveastable_ext")
    withContract(
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin
    ) {
      df().write.format("hive").option("path", loc).saveAsTable("hive_saveastable_ext_tbl") // must not throw
    }
    val t = spark.sessionState.catalog.getTableMetadata(TableIdentifier("hive_saveastable_ext_tbl"))
    assert(t.tableType == CatalogTableType.EXTERNAL)
    assert(t.storage.locationUri.get.toString.stripPrefix("file:") == loc.stripPrefix("file:"))
    assert(spark.table("hive_saveastable_ext_tbl").count() == 2)
  }

  // Unlike Hive's own V1 CreateHiveTableAsSelectCommand (see the PASS test
  // above), an explicit .option("path", ...) does NOT change how a new
  // table's V2 CreateTableAsSelect resolves its location - confirmed here
  // (not assumed) to be the same, already-documented behavior
  // docs/connectors/delta.md's own new-table `.saveAsTable()` row already
  // describes for the default in-memory catalog: a not-yet-committed
  // StagedTable's location is never trusted (see
  // WriteCommandSupport.namedRelationLocationAndFormat's own doc), so the
  // outer CTAS always resolves to the qualified catalog identifier
  // regardless of any path option - the CREATE step below is exercised
  // with no contract active (matching how every other new-table CTAS setup
  // in this suite/ContractEnforcementRuleSpec is written), and only the
  // APPEND - against the now-resolved, committed table - is checked
  // against a contract declaring the real physical path.
  test("PASS: .format(delta).option(path, ...).saveAsTable() append onto an existing EXTERNAL table verifies against the real physical path") {
    val loc = extScratchDir("delta_saveastable_ext")
    df().write.format("delta").option("path", loc).saveAsTable("delta_saveastable_ext_tbl") // new table, no contract active yet

    withContract(
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin
    ) {
      df().write.format("delta").mode("append").option("path", loc).saveAsTable("delta_saveastable_ext_tbl") // must not throw
    }
    assert(spark.read.format("delta").load(loc).count() == 4)
  }

  test("known limitation (shared with the default in-memory catalog, not Hive-specific): a NEW external Delta table's CTAS resolves to the qualified identifier, not its physical path") {
    val loc = extScratchDir("delta_saveastable_ctas_gap")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.format("delta").option("path", loc).saveAsTable("delta_saveastable_ctas_gap_tbl")

    val result = awaitWriteTo(listener, "delta_saveastable_ctas_gap_tbl")
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, format, _, _) =>
        assert(location == "spark_catalog.default.delta_saveastable_ctas_gap_tbl",
          s"expected the qualified-identifier fallback (unaffected by .option(path, ...)), got '$location'")
        assert(format.contains("delta"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Fail-closed: DROP TABLE on an EXTERNAL table -----------------------
  //
  // Confirmed (not assumed): dropping an EXTERNAL table leaves its data
  // files on disk - only the metastore entry is removed. Invaract's
  // fail-closed policy nonetheless rejects DropTable unconditionally
  // (it's neither a recognized write nor on FailClosedCommands' safe
  // list), regardless of table type. This is a deliberate, accepted false
  // rejection, not a bug: the class-name-only check can't distinguish "this
  // specific DROP is harmless" from "this one deletes a managed table's
  // real data" without inspecting the instance, and FailClosedCommands'
  // own asymmetry (a safe command wrongly missing costs one rejection; one
  // wrongly present could silently defeat the feature) already accepts that
  // tradeoff. See docs/connectors/hive.md's "External tables" section.
  test("fails closed: DROP TABLE on an EXTERNAL table is rejected, even though the underlying data would have survived") {
    val loc = extScratchDir("drop_ext")
    spark.sql(s"CREATE EXTERNAL TABLE hive_drop_ext_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")
    spark.sql("INSERT INTO hive_drop_ext_tbl SELECT 1, 10")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        spark.sql("DROP TABLE hive_drop_ext_tbl")
      }
      assert(ex.result.violations.exists(_.violationType == ViolationType.UnverifiableWrite))
    }
    assert(spark.sessionState.catalog.tableExists(TableIdentifier("hive_drop_ext_tbl")), "a rejected DROP must never have removed the catalog entry")
    assert(
      Files.list(java.nio.file.Paths.get(loc.stripPrefix("file:"))).count() > 0,
      "an EXTERNAL table's data is never deleted by DROP TABLE in the first place - confirming what the rejection is (over-)protecting against"
    )
  }

  // --- Catalog registration checks (docs/CONTRACT_MODEL.md's `catalog`
  // field, ROADMAP.md's catalog-registration addendum) -----------------
  //
  // The real gap this closes: before this feature, a contract had no way
  // to *require* that an output (or input) be registered in a catalog at
  // all - a bare `.parquet(path)`/`.save(path)` write with a perfectly
  // correct schema passed cleanly, with no way for an org to mandate "every
  // job's output must have a real catalog entry so downstream tools can
  // discover it" (the user's own original ask). Note what this does NOT
  // close: `CREATE EXTERNAL TABLE ... LOCATION` itself is schema-only DDL,
  // never checked against anything (see "External tables" in
  // docs/connectors/hive.md) - a table registered with a schema that
  // doesn't match its underlying physical data still succeeds silently.
  // That's a distinct, still-open gap; these tests are about catalog
  // *identity* (is there a registration, and does it match what's
  // declared), not validating a table's declared schema against its
  // physical files.

  test("PASS: an EXTERNAL Hive table write satisfies a contract requiring catalog registration") {
    val loc = extScratchDir("catalog_required_pass")
    spark.sql(s"CREATE EXTERNAL TABLE hive_catalog_required_pass_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: hive
         |      catalogName: spark_catalog
         |      table: hive_catalog_required_pass_tbl
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO hive_catalog_required_pass_tbl SELECT 1, 10") // must not throw
    }
    assert(spark.table("hive_catalog_required_pass_tbl").count() == 1)
  }

  test("FAIL: a bare-path parquet write with a correct schema is rejected (MISSING_OUTPUT_CATALOG_REGISTRATION) when the contract mandates catalog registration") {
    // This is the concrete fix for the org-wide policy the user asked for:
    // "all spark jobs in their ecosystem [must] have a catalog entry ...
    // downstream technologies can interoperate more easily." Before this
    // feature, this exact write - correct schema, correct location -
    // passed with zero violations, because nothing in the contract format
    // could express "this output must be catalog-registered" at all.
    val loc = extScratchDir("catalog_required_fail_bare")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: hive
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        df().write.mode("overwrite").parquet(loc) // correct schema, but no catalog registration at all
      }
      assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputCatalogRegistration))
    }
    assert(
      !Files.exists(java.nio.file.Paths.get(loc.stripPrefix("file:"))) ||
        Files.list(java.nio.file.Paths.get(loc.stripPrefix("file:"))).count() == 0,
      "a rejected write must never have committed any data"
    )
  }

  test("FAIL: an EXTERNAL Hive table registered under the wrong technology is rejected (OUTPUT_CATALOG_MISMATCH)") {
    // A real Hive registration exists - just not the one the contract
    // declares. Confirms the check compares identity, not just presence.
    val loc = extScratchDir("catalog_mismatch_fail")
    spark.sql(s"CREATE EXTERNAL TABLE hive_catalog_mismatch_fail_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: iceberg
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        spark.sql("INSERT INTO hive_catalog_mismatch_fail_tbl SELECT 1, 10")
      }
      assert(ex.result.violations.exists(v =>
        v.violationType == ViolationType.OutputCatalogMismatch &&
          v.expected.exists(_.contains("technology=iceberg")) &&
          v.actual.exists(_.contains("technology=hive"))
      ))
    }
    assert(spark.table("hive_catalog_mismatch_fail_tbl").count() == 0, "a rejected insert must never have committed")
  }

  // The specific, concrete question this whole check exists to answer:
  // can a job silently write through a DIFFERENT Hive metastore than the
  // contract declares? `location` (CatalogIdentitySupport.hiveMetastoreLocation)
  // is a real, session-derived value - hive.metastore.uris, or an
  // "embedded:<jdbc-url>" fallback for a local/embedded metastore, exactly
  // this test session's own setup - not a Spark-local alias like
  // catalogName. These two tests prove it's actually compared, not just
  // present in the data model: a wrong declared location is rejected, and
  // the real one (read the same way CatalogIdentitySupport itself does,
  // not hand-typed) passes.

  test("FAIL: a write through the wrong Hive metastore location is rejected (OUTPUT_CATALOG_MISMATCH names 'location')") {
    val loc = extScratchDir("catalog_location_mismatch")
    spark.sql(s"CREATE EXTERNAL TABLE hive_catalog_location_mismatch_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: hive
         |      location: thrift://not-the-real-metastore.example.com:9083
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val ex = intercept[ContractViolationException] {
        spark.sql("INSERT INTO hive_catalog_location_mismatch_tbl SELECT 1, 10")
      }
      val violation = ex.result.violations.find(_.violationType == ViolationType.OutputCatalogMismatch)
        .getOrElse(fail(s"expected an OUTPUT_CATALOG_MISMATCH violation, got: ${ex.result.violations}"))
      assert(violation.expected.exists(_.contains("location=thrift://not-the-real-metastore.example.com:9083")))
      assert(!violation.actual.exists(_.contains("thrift://not-the-real-metastore.example.com:9083")), "the actual side must report this session's REAL metastore location, not echo the wrong declared one")
    }
    assert(spark.table("hive_catalog_location_mismatch_tbl").count() == 0, "a rejected insert must never have committed")
  }

  test("PASS: a write against the contract's correctly-declared Hive metastore location is accepted") {
    val loc = extScratchDir("catalog_location_pass")
    spark.sql(s"CREATE EXTERNAL TABLE hive_catalog_location_pass_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")

    // The real value this session's own writes will actually report -
    // read the identical way CatalogIdentitySupport.hiveMetastoreLocation
    // does, not hand-typed, so this test can't pass by coincidentally
    // matching a guess.
    val realMetastoreLocation = CatalogIdentitySupport.hiveMetastoreLocation
      .getOrElse(fail("expected this test session to report a real Hive metastore location (embedded or thrift)"))

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: hive
         |      location: $realMetastoreLocation
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      spark.sql("INSERT INTO hive_catalog_location_pass_tbl SELECT 1, 10") // must not throw
    }
    assert(spark.table("hive_catalog_location_pass_tbl").count() == 1)
  }

  // The disclosed, real counterpart to the Hive tests above: for a DSv2
  // catalog (Delta/Iceberg/JDBC), `location` is ALWAYS None -
  // CatalogIdentitySupport.fromV2's own doc explains why (no reflective
  // accessor exists in any of these connectors' public API to read a
  // catalog plugin's own network endpoint without a compile-time
  // dependency this module deliberately doesn't take). This means a
  // contract declaring `catalog.location` against a Delta/Iceberg/JDBC
  // output can never be verified today - only `technology`/`catalogName`/
  // `namespace`/`table` are actually checked for those. Confirmed here
  // rather than left as an assumption: declaring a location the actual
  // write plainly does NOT satisfy still passes cleanly, because the
  // location sub-field is never compared when the actual side is None -
  // same "no false rejection on unknown information" rule format/saveMode
  // already follow.
  test("known limitation: a Delta output's declared catalog.location is never checked (DSv2 catalogs report no location at all)") {
    val loc = extScratchDir("catalog_location_delta_unchecked")
    df().write.format("delta").option("path", loc).saveAsTable("delta_catalog_location_unchecked_tbl") // new table, no contract active yet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: delta
         |      location: this-value-is-never-actually-checked-for-delta
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: value
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      df().write.format("delta").mode("append").saveAsTable("delta_catalog_location_unchecked_tbl") // must not throw
    }
    assert(spark.read.format("delta").load(loc).count() == 4)
  }

  test("PASS: a real Hive-registered input satisfies a contract requiring input-side catalog registration") {
    // Input-side mirror of the output tests above, against a real Hive
    // read - confirms the check applies symmetrically per the user's own
    // explicit answer ("both inputs and outputs").
    val loc = extScratchDir("catalog_input_required")
    spark.sql(s"CREATE EXTERNAL TABLE hive_catalog_input_required_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$loc'")
    spark.sql("INSERT INTO hive_catalog_input_required_tbl SELECT 1, 10")

    val outLoc = extScratchDir("catalog_input_required_out")
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: ${loc.stripPrefix("file:")}
         |    catalog:
         |      required: true
         |      technology: hive
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |        - name: value
         |          type: long
         |outputs:
         |  - name: out
         |    location: ${outLoc.stripPrefix("file:")}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    // Output fields are `required: false` (not `required: true`) because
    // the write reads back through a Hive table, which - per the "feature
    // surface: a Hive table read-back reports every field nullable" test
    // above - always reports every field nullable regardless of the
    // underlying DDL; this test is about the catalog check, not schema
    // nullability, so it avoids that unrelated, already-documented quirk.
    withContract(yaml) {
      spark.table("hive_catalog_input_required_tbl").write.mode("overwrite").parquet(outLoc) // must not throw: read is catalog-registered
    }
    assert(spark.read.parquet(outLoc).count() == 1)
  }

  // --- Real notification messages, captured against an external table ----
  //
  // Everything above proves enforcement decides PASS/FAIL correctly.
  // This proves the *published* ContractValidationEvent/WriteEvent JSON -
  // the messages an external NotificationSink actually receives - are
  // correct for an external table too: real location (not a warehouse
  // path), the contract ref, and (for WriteEvent) the real row/format
  // detail. Captured via FileNotificationSink, the same sink
  // demo/notify.properties and ./dev/test's own real-message proof use -
  // not a mock, not a hand-built JSON string.
  /** One end-to-end scenario's real captured notification traffic: how many
    * of each event type a fresh `FileNotificationSink` actually received,
    * plus the raw lines - built once, reused by both halves of the
    * comparison test below so neither has to duplicate the
    * sink-wiring/eventually-wait boilerplate.
    */
  private case class CapturedEvents(validationLines: Seq[String], writeLines: Seq[String])

  /** `expectedWriteEvents` matters here in a way it wouldn't for a simpler
    * "wait for at least one" check: `SparkAdapterListener.onSuccess` fires
    * asynchronously on Spark's own listener-bus thread (documented
    * elsewhere in this file - see `awaitWriteTo`'s own doc), and a
    * `.saveAsTable()` call that triggers it *twice* (once per nested
    * Command plan - see the comparison test below) has no ordering/timing
    * guarantee that both have landed by the time a generic "any WriteEvent
    * yet" check would already return - confirmed the hard way by this
    * exact test flaking between 1 and 2 observed WriteEvents before this
    * fix. Waiting for the *exact expected count* (not just "at least one")
    * is what `awaitWriteTo`'s own location/saveMode filters achieve for a
    * single expected write; this generalizes that to N.
    */
  private def captureNotifications(
    contractYaml: String,
    sink: FileNotificationSink,
    eventsFile: Path,
    expectedWriteEvents: Int
  )(body: => Unit): CapturedEvents = {
    val contract = parseContract(contractYaml)
    val listener = new SparkAdapterListener(Some(sink), Some(contract))
    spark.listenerManager.register(listener)
    withSink(sink) {
      withContract(contractYaml) {
        body
      }
    }
    val lines = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      val ls = if (Files.exists(eventsFile)) Files.readAllLines(eventsFile).toArray.toIndexedSeq.map(_.toString) else IndexedSeq.empty
      val writeCount = ls.count(_.contains("\"eventType\": \"WRITE\""))
      assert(writeCount == expectedWriteEvents, s"expected $expectedWriteEvents WriteEvent(s) by now, have $writeCount so far")
      ls
    }
    CapturedEvents(
      lines.filter(_.contains("\"eventType\": \"CONTRACT_VALIDATION\"")),
      lines.filter(_.contains("\"eventType\": \"WRITE\""))
    )
  }

  private def contractFor(location: String): String =
    s"""id: enforcement_demo
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: $location
       |    format: parquet
       |    schema:
       |      fields:
       |        - name: id
       |          type: long
       |          required: true
       |        - name: value
       |          type: long
       |          required: true
       |""".stripMargin

  // --- Comparing the two write paths the user actually asked about -------
  //
  // Not ".saveAsTable() vs. a single CREATE EXTERNAL TABLE ... AS SELECT",
  // but the more realistic pairing: (A) write plain parquet data first,
  // then separately point a Hive EXTERNAL table at the already-written
  // data (a common real pattern - an ETL job writes files, a later
  // metadata-registration step catalogs them) vs. (B) .saveAsTable() doing
  // both the write AND the table registration in one call. Captured with a
  // real FileNotificationSink for both, to see how the actual published
  // traffic differs, not just whether each individually passes.
  test("notifications: write-then-register (raw SQL) vs. saveAsTable() (write+register in one call) - real captured message counts differ") {
    // --- (A) raw SQL: write parquet directly, THEN register an EXTERNAL
    // Hive table over the already-existing data (schema-only DDL, no AS
    // SELECT - no data is written by this second statement). ---
    val locA = extScratchDir("compare_sql")
    val eventsFileA = scratchDir.resolve("compare_sql_events.jsonl")
    val sinkA = new FileNotificationSink
    sinkA.configure(Map("path" -> eventsFileA.toString))

    val contractYamlA = contractFor(locA.stripPrefix("file:"))
    val capturedA = captureNotifications(contractYamlA, sinkA, eventsFileA, expectedWriteEvents = 1) {
      df().write.mode("overwrite").parquet(locA) // the ONLY write in this scenario
    }
    val linesAfterStep1 = Files.readAllLines(eventsFileA).toArray.toIndexedSeq.map(_.toString)

    // scalastyle:off println
    println("=" * 100)
    println("STEP 1: df.write.mode(\"overwrite\").parquet(locA)")
    println("Contract active during this step:")
    println(contractYamlA)
    println(s"Events file after step 1 (${linesAfterStep1.size} line(s)):")
    linesAfterStep1.foreach(l => println(s"  $l"))
    // scalastyle:on println

    // Register the external table over the data that's already there - no
    // active contract even needed to prove the point, since this must
    // publish nothing regardless: CREATE EXTERNAL TABLE with no AS SELECT
    // is CreateTableCommand, safe-listed DDL, never reaches verifyOrThrow's
    // ir.Write branch at all.
    spark.sql(s"CREATE EXTERNAL TABLE hive_compare_sql_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$locA'")
    Thread.sleep(500) // let any (unexpected) async event a chance to land before asserting its absence
    val linesAfterStep2 = Files.readAllLines(eventsFileA).toArray.toIndexedSeq.map(_.toString)

    // scalastyle:off println
    println("STEP 2: spark.sql(\"CREATE EXTERNAL TABLE hive_compare_sql_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '...'\")")
    println("(no contract was even re-activated for this step - proving the point regardless of whether one is active)")
    println(s"Events file after step 2 (${linesAfterStep2.size} line(s) - should be unchanged from step 1):")
    linesAfterStep2.foreach(l => println(s"  $l"))
    println("=" * 100)
    // scalastyle:on println

    val countAfterWrite = linesAfterStep1.size
    val countAfterCreate = linesAfterStep2.size
    assert(
      countAfterCreate == countAfterWrite,
      "CREATE EXTERNAL TABLE over already-written data is metadata-only and must publish NO additional event " +
        s"(had $countAfterWrite lines after the write, $countAfterCreate after the CREATE)"
    )
    assert(spark.table("hive_compare_sql_tbl").count() == 2, "the external table must see the data written before it existed")

    // --- (B) .saveAsTable(): a single call both creates the table (a new
    // one) and writes the data, analyzing to TWO Command-shaped plans
    // (CreateDataSourceTableAsSelectCommand + a nested
    // InsertIntoHadoopFsRelationCommand) - the same "one call, two writes"
    // shape already documented elsewhere in this file for Hive/Delta's own
    // CTAS. ---
    val locB = extScratchDir("compare_saveastable")
    val eventsFileB = scratchDir.resolve("compare_saveastable_events.jsonl")
    val sinkB = new FileNotificationSink
    sinkB.configure(Map("path" -> eventsFileB.toString))

    val capturedB = captureNotifications(contractFor(locB.stripPrefix("file:")), sinkB, eventsFileB, expectedWriteEvents = 2) {
      df().write.format("parquet").option("path", locB).saveAsTable("hive_compare_saveastable_tbl")
    }

    // The real, previously-unstated difference this test exists to show -
    // and a real correction to an assumption carried over from Hive's own
    // CreateHiveTableAsSelectCommand write-up (whose QueryExecutionListener
    // genuinely does only fire once): CreateDataSourceTableAsSelectCommand
    // is different. injectCheckRule sees BOTH nested Command plans
    // .saveAsTable() produces and verifies each independently, so ONE
    // .saveAsTable() call publishes TWO ContractValidationEvents - expected.
    // But confirmed empirically (not assumed from the Hive precedent) that
    // SparkAdapterListener.onSuccess ALSO fires twice here, not once: Spark
    // executes CreateDataSourceTableAsSelectCommand's inner write as its
    // own separate QueryExecution, so this specific V1 CTAS command
    // produces TWO WriteEvents for one logical call - one for the real
    // physical write (real rowCount/bytesWritten/fileCount, saveMode
    // reported as Spark's own internal "overwrite" for the fresh table),
    // and one for the outer command itself (saveMode "error" - the
    // .saveAsTable() default for a brand-new table with no explicit
    // .mode() - and no SQLMetrics at all, so rowCount/bytesWritten/
    // fileCount are null). Scenario (A)'s explicit two-statement form has
    // no such doubling on either channel - exactly one of each.
    assert(capturedA.validationLines.size == 1, s"expected exactly 1 ContractValidationEvent for the explicit write+register form, got ${capturedA.validationLines.size}")
    assert(capturedA.writeLines.size == 1, s"expected exactly 1 WriteEvent for the explicit write+register form, got ${capturedA.writeLines.size}")
    assert(capturedB.validationLines.size == 2, s"expected exactly 2 ContractValidationEvents for .saveAsTable() (one per nested Command plan), got ${capturedB.validationLines.size}")
    assert(capturedB.writeLines.size == 2, s"expected exactly 2 WriteEvents for .saveAsTable() (the outer CTAS command AND its inner physical write each trigger onSuccess separately), got ${capturedB.writeLines.size}")
    capturedB.validationLines.foreach(l => assert(l.contains("\"status\": \"PASSED\"")))

    // Real captured catalog identity, per scenario: (A)'s write happens
    // BEFORE the EXTERNAL TABLE is ever registered, so its one WriteEvent
    // has no catalog at all - an honest "not registered at write time,"
    // not a bug. (B)'s saveAsTable() registers the table as part of the
    // very same call, so its WriteEvent(s) do carry a real Hive catalog
    // identity - confirmed against the actual captured JSON, not assumed.
    assert(capturedA.writeLines.forall(_.contains("\"catalog\": null")), s"scenario A's write predates catalog registration entirely: ${capturedA.writeLines}")
    assert(capturedB.writeLines.exists(l => l.contains("\"technology\": \"hive\"")), s"expected at least one of scenario B's WriteEvents to carry a real Hive catalog identity: ${capturedB.writeLines}")

    // Surfaced for the human reading test output/docs, not asserted on
    // beyond the counts above - this is exactly the real captured JSON
    // docs/connectors/hive.md's "External tables" section quotes.
    // scalastyle:off println
    println(s"[scenario A: write, then CREATE EXTERNAL TABLE] ${capturedA.validationLines.size} ContractValidationEvent(s), ${capturedA.writeLines.size} WriteEvent(s)")
    capturedA.validationLines.foreach(l => println(s"[A ContractValidationEvent] $l"))
    capturedA.writeLines.foreach(l => println(s"[A WriteEvent] $l"))
    println(s"[scenario B: .saveAsTable()] ${capturedB.validationLines.size} ContractValidationEvent(s), ${capturedB.writeLines.size} WriteEvent(s)")
    capturedB.validationLines.foreach(l => println(s"[B ContractValidationEvent] $l"))
    capturedB.writeLines.foreach(l => println(s"[B WriteEvent] $l"))
    // scalastyle:on println
  }

  // A path under scratchDir but a SIBLING of "warehouse" (not nested under
  // it) - genuinely outside the Hive warehouse directory, the same
  // property a real EXTERNAL table's LOCATION has in production, without
  // needing a second temp-directory tree.
  private def extScratchDir(name: String): String = scratchDir.resolve(s"external_$name").toString
}
