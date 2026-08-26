// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Hive support, added via the `add-spark-connector` skill's process
  * (docs/ADDING_A_SPARK_CONNECTOR.md). Unlike Delta/Iceberg, Hive is not an
  * external connector library - it's Spark's own first-party integration
  * module (`spark-hive`, `enableHiveSupport()`), tested here against a real
  * embedded-Derby metastore (no external Hive install needed). See
  * docs/SPARK_ADAPTER.md's "Hive support" section for the full
  * investigation and coverage ledger.
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
class HiveConnectorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  @volatile private var activeContract: Option[com.example.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invariant-hive-test")
    System.setProperty("derby.stream.error.file", scratchDir.resolve("derby.log").toString)

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveConnectorSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${scratchDir.resolve("metastore_db")};create=true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
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

  private def df() = spark.createDataFrame(Seq((1L, 10L), (2L, 20L))).toDF("id", "value")

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
    extra: com.example.ir.Write => Boolean = _ => true
  ): TranslationResult =
    org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite match {
        case Some(r @ TranslationResult(w @ com.example.ir.Write(com.example.ir.DatasetRef(loc), _, _, _), _))
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
      case com.example.ir.Read(com.example.ir.DatasetRef(location), _) =>
        assert(location.stripPrefix("file:") == tableLocation("hive_text_read_tbl"))
      case other => fail(s"expected a Read, got ${com.example.ir.PlanPrinter.render(other)}")
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
      case com.example.ir.Read(com.example.ir.DatasetRef(location), _) =>
        assert(location.stripPrefix("file:") == tableLocation("hive_parquet_conv_tbl"))
      case other => fail(s"expected a Read, got ${com.example.ir.PlanPrinter.render(other)}")
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
        case com.example.ir.Read(com.example.ir.DatasetRef(location), _) =>
          assert(location.stripPrefix("file:") == tableLocation("hive_parquet_noconv_tbl"))
        case other => fail(s"expected a Read, got ${com.example.ir.PlanPrinter.render(other)}")
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
  // see docs/SPARK_ADAPTER.md's "Hive support" section for the full
  // writeup and next step. ---

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
    val result = awaitWriteTo(listener, "hive_ctas_overwrite_gap_tbl")
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, _, _) =>
        assert(location == "spark_catalog.default.hive_ctas_overwrite_gap_tbl",
          s"expected the outer command's qualified-identifier fallback, got '$location'")
        assert(location.stripPrefix("file:") != tableLocation("hive_ctas_overwrite_gap_tbl"),
          "this is exactly the known gap: the outer command's location does NOT match the table's real physical path")
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Write: .insertInto(...) / .saveAsTable() append onto an EXISTING
  // Hive table - InsertIntoHiveTable, confirmed to also appear nested
  // inside a CreateHiveTableAsSelectCommand for the append-via-saveAsTable
  // form. ---

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
      case com.example.ir.Write(_, _, format, saveMode) =>
        assert(format.contains("hive"))
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
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
    val outDir = scratchDir.resolve("hive_insert_dir_out").toString
    df().createOrReplaceTempView("hive_insert_dir_src")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    spark.sql(s"INSERT OVERWRITE DIRECTORY '$outDir' STORED AS TEXTFILE SELECT * FROM hive_insert_dir_src")

    val result = awaitWriteTo(listener, outDir)
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, format, saveMode) =>
        assert(location.contains(outDir) || location == outDir, s"expected the real directory path, got '$location'")
        assert(format.contains("hive"))
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  test("FAIL: INSERT OVERWRITE DIRECTORY at a location that doesn't match the contract is rejected") {
    val outDir = scratchDir.resolve("hive_insert_dir_fail_out").toString
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

  // --- Fail-closed: LOAD DATA INPATH - a genuinely data-mutating Hive
  // operation Invariant deliberately doesn't translate (already documented
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

  test("fails closed: MERGE INTO against a plain Hive table is rejected by Invariant before Spark's own rejection ever runs") {
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

  // --- N/A, confirmed by Spark itself, not an Invariant gap: DataFrameWriterV2
  // and streaming writes against a Hive table. ---

  test(".writeTo() against a Hive table is rejected by Spark itself, not by Invariant") {
    spark.sql("CREATE TABLE hive_writeto_tbl (id BIGINT, value BIGINT) STORED AS TEXTFILE")
    val ex = intercept[org.apache.spark.sql.AnalysisException](df().writeTo("hive_writeto_tbl").append())
    assert(ex.getMessage.contains("Cannot write into v1 table"))
    assert(spark.table("hive_writeto_tbl").count() == 0)
  }

  test("streaming .toTable() against a Hive table is rejected by Spark itself, not by Invariant") {
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
      case com.example.ir.Read(com.example.ir.DatasetRef(location), _) => assert(location.stripPrefix("file:") == loc)
      case other => fail(s"expected a Read, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  // --- Feature surface: a real Hive UDF (not Spark's own ScalaUDF) -
  // confirmed transparent: isOpaqueUdf's existing "endsWith HiveGenericUDF"
  // check (already in the codebase, previously untested against a real
  // Hive UDF for lack of a metastore) correctly recognizes it. ---

  test("feature surface: a real Hive UDF is translated as an opaque FunctionCall with a diagnostic") {
    spark.sql("CREATE TEMPORARY FUNCTION hive_udf_feature_upper AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper'")
    df().createOrReplaceTempView("hive_udf_feature_src")
    val analyzed = spark.sql("SELECT id, hive_udf_feature_upper(CAST(value AS STRING)) AS v FROM hive_udf_feature_src").queryExecution.analyzed

    val result = SparkPlanAdapter.translate(analyzed)
    assert(result.diagnostics.exists(_.nodeType.contains("HiveGenericUDF")), s"expected a HiveGenericUDF diagnostic, got ${result.diagnostics}")
    result.plan match {
      case p: com.example.ir.Project =>
        assert(p.columns.exists {
          case com.example.ir.NamedExpr("v", _: com.example.ir.FunctionCall) => true
          case _ => false
        }, s"expected 'v' translated as an opaque FunctionCall, got ${com.example.ir.PlanPrinter.render(p)}")
      case other => fail(s"expected a Project, got ${com.example.ir.PlanPrinter.render(other)}")
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
}
