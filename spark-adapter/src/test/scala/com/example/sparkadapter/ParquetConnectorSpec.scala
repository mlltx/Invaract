// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Parquet-specific coverage, added via the `add-spark-connector` skill
  * (docs/ADDING_A_SPARK_CONNECTOR.md). Unlike Delta/Iceberg, Parquet is not
  * a separate connector library: `ParquetFileFormat` is Spark's own
  * built-in `FileFormat`, already on this module's `provided` Spark
  * dependency, registers no catalog of its own, and adds no SQL-extension
  * `Command` classes (confirmed: a reflective survey of spark-sql's own
  * `Command` hierarchy - the same one Delta/Iceberg's onboarding already
  * enumerated - found nothing Parquet-specific; every Parquet operation
  * routes through generic Spark commands already recognized by
  * `WriteCommandSupport`/`FailClosedCommands`). So this suite is mostly a
  * confirmation pass - real evidence that those generic mechanisms cover
  * Parquet specifically, not just assumed from the Delta/Iceberg
  * precedent - plus permanent tests for the real, Parquet-specific
  * findings the investigation surfaced (see docs/SPARK_ADAPTER.md's
  * "Parquet support" section for the full coverage ledger).
  */
class ParquetConnectorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  @volatile private var activeContract: Option[com.example.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invariant-parquet-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ParquetConnectorSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
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

  private def df() = spark.createDataFrame(Seq((1L, 10L), (2L, 20L))).toDF("id", "value")

  // --- .insertInto(...) against an existing table: same InsertIntoHadoopFsRelationCommand
  // shape .save(path) uses, confirmed empirically (not assumed) rather than
  // inferred from the class name alone. ---

  test("translates .insertInto() against an existing table via InsertIntoHadoopFsRelationCommand") {
    spark.sql("CREATE TABLE IF NOT EXISTS parquet_insert_into_tbl (id BIGINT, value BIGINT) USING parquet")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.insertInto("parquet_insert_into_tbl")

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .insertInto() write yet"))
    }
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, format, saveMode) =>
        assert(location.contains("parquet_insert_into_tbl"))
        assert(format.contains("parquet"))
        assert(saveMode.contains("append"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  // --- .saveAsTable() append onto an EXISTING table: a real, newly-found structural
  // trap - a single call produces TWO Command-shaped plans through injectCheckRule,
  // not one. Confirmed empirically: CreateDataSourceTableAsSelectCommand.run()
  // itself detects the table already exists and, rather than creating a new
  // table, internally executes a second, nested InsertIntoHadoopFsRelationCommand
  // to perform the actual insert - both visible to ContractEnforcementRule,
  // meaning it runs TWICE for one logical write. The same general shape as the
  // Delta/Iceberg "atomic CTAS/RTAS issues a second, nested write" pitfall
  // (docs/SPARK_ADAPTER.md), but via a different mechanism (a V1 command's own
  // internal delegation, not Spark's StagedTable protocol) - and, unlike that
  // case, this one needed no fix: both plans resolve to the identical location
  // (the existing table's real storage path), so a satisfying write passes both
  // checks and a violating write is rejected at the first (outer) one, before
  // the nested write ever runs. ---

  test("PASS: .saveAsTable() append onto an existing table - both nested Command plans see a satisfying write") {
    val tblPath = scratchDir.resolve("nested_pass").toString
    df().write.option("path", tblPath).mode("overwrite").saveAsTable("nested_pass_tbl")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tblPath
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
      df().write.mode("append").saveAsTable("nested_pass_tbl") // must not throw
    }
  }

  test("FAIL: .saveAsTable() append onto an existing table - rejected at the outer command, nested insert never runs") {
    val tblPath = scratchDir.resolve("nested_fail").toString
    df().write.option("path", tblPath).mode("overwrite").saveAsTable("nested_fail_tbl")
    val rowCountBefore = spark.table("nested_fail_tbl").count()

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tblPath
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
        df().write.mode("append").saveAsTable("nested_fail_tbl")
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("nested_fail_tbl").count() == rowCountBefore, "the nested insert must never have run")
  }

  // --- .writeTo(...) DataFrameWriterV2 against a plain parquet table under the
  // default (non-Hive) session catalog: confirmed empirically to be entirely
  // rejected by Spark ITSELF, not an Invariant gap. append/overwrite/
  // overwritePartitions against an EXISTING plain-parquet table always fail
  // ("Cannot write into v1 table") - a permanent constraint of Spark's V1/V2
  // architecture (parquet never implements the SupportsWrite V2 capability
  // under the default useV1SourceList, by design), independent of Hive
  // support. createOrReplace/replace fail too, for both new and existing
  // tables, even with the format made explicit ("does not support REPLACE
  // TABLE AS SELECT") - the default spark_catalog is not a StagingTableCatalog
  // for V1-provider-backed tables the way Delta's/Iceberg's own catalogs are.
  // Only .create() on a genuinely NEW table, with the format made explicit via
  // .using("parquet"), succeeds - and it resolves through the exact same
  // CreateDataSourceTableAsSelectCommand + nested InsertIntoHadoopFsRelationCommand
  // path .saveAsTable() already uses, confirmed via injectCheckRule (no new
  // plan shape at all). None of this needs new WriteCommandSupport code: the
  // rejected sub-ops never produce an analyzable plan for Invariant to see,
  // and the one that succeeds was already covered. ---

  test(".writeTo() against an existing plain-parquet table is rejected by Spark itself, not by Invariant") {
    spark.sql("CREATE TABLE IF NOT EXISTS writeto_existing_tbl (id BIGINT, value BIGINT) USING parquet")
    val ex1 = intercept[org.apache.spark.sql.AnalysisException](df().writeTo("writeto_existing_tbl").append())
    assert(ex1.getMessage.contains("Cannot write into v1 table"))
    val ex2 = intercept[org.apache.spark.sql.AnalysisException](
      df().writeTo("writeto_existing_tbl").overwrite(org.apache.spark.sql.functions.lit(true))
    )
    assert(ex2.getMessage.contains("Cannot write into v1 table"))
    assert(spark.table("writeto_existing_tbl").count() == 0, "no rejected write should have committed any data")
  }

  // Translation-only, not a full PASS/FAIL enforcement pair: a genuinely
  // new table created with NO explicit path (unlike every existing
  // .saveAsTable() precedent test, which always supplies .option("path", ...))
  // hits a pre-existing, CreateDataSourceTableAsSelectCommand-general
  // (not Parquet-specific) nuance - the outer command's location falls
  // back to the qualified catalog identifier (no physical path exists yet
  // at plan-construction time, confirmed via this file's own
  // "No storage location on new table" diagnostic), while the nested
  // InsertIntoHadoopFsRelationCommand's location is the real physical
  // path - the two don't match unless an explicit path is given, the same
  // "shared pitfall" class already documented for Delta/Iceberg's
  // StagedTable case, just not attempted here (out of scope for this
  // pass; every enforcement PASS/FAIL pair in this file uses an explicit
  // path, sidestepping it, matching prior precedent).
  test(".writeTo(...).using(\"parquet\").create() on a new table reuses the existing CreateDataSourceTableAsSelectCommand translation") {
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().writeTo("writeto_create_tbl").using("parquet").create()

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .writeTo().create() write yet"))
    }
    result.plan match {
      case com.example.ir.Write(_, _, format, _) => assert(format.contains("parquet"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
    assert(spark.table("writeto_create_tbl").count() == 2)
  }

  // --- Format-specific DML (MERGE/UPDATE/DELETE): confirmed empirically that
  // Spark ITSELF refuses all three against a plain parquet table - real
  // SparkUnsupportedOperationException/AnalysisException, before any
  // analyzable Command-shaped plan is ever produced for injectCheckRule to
  // see. Genuinely N/A for this format (not a 🚫 "Invariant hasn't
  // translated this yet"): plain Parquet is not a row-level-DML-capable
  // table format at all in vanilla Spark, unlike Delta/Iceberg. ---

  test("MERGE/UPDATE/DELETE against a plain parquet table are rejected by Spark itself, nothing reaches Invariant") {
    spark.sql("CREATE TABLE IF NOT EXISTS dml_target_tbl (id BIGINT, value BIGINT) USING parquet")
    spark.sql("INSERT INTO dml_target_tbl VALUES (1, 10)")
    spark.sql("CREATE TABLE IF NOT EXISTS dml_source_tbl (id BIGINT, value BIGINT) USING parquet")

    // SparkUnsupportedOperationException is private[spark] - not
    // accessible from this module - so these are asserted by message
    // content on the common Exception supertype instead.
    val mergeEx = intercept[Exception] {
      spark.sql(
        "MERGE INTO dml_target_tbl t USING dml_source_tbl s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT *"
      ).collect()
    }
    assert(mergeEx.getMessage.contains("MERGE INTO TABLE is not supported"))
    val updateEx = intercept[Exception] {
      spark.sql("UPDATE dml_target_tbl SET value = 0 WHERE id = 1").collect()
    }
    assert(updateEx.getMessage.contains("UPDATE TABLE is not supported"))
    assertThrows[org.apache.spark.sql.AnalysisException] {
      spark.sql("DELETE FROM dml_target_tbl WHERE id = 1").collect()
    }
    assert(spark.table("dml_target_tbl").count() == 1, "no rejected DML should have changed the table")
  }

  // --- Streaming write: WriteCommandSupport's WriteToStream case, the same
  // one built for Delta - but a REAL BUG was found and fixed testing it
  // against Parquet's own FileStreamSink (a legacy V1 Sink wrapper distinct
  // from DeltaSink): unlike DeltaSink, FileStreamSink.name() does not
  // throw, so streamSinkLocationAndFormat's tier-2 check ("doesn't throw ⟹
  // trustworthy") took it at face value - but it returns a *descriptive*
  // "FileSink[<path>]" string, not a bare path, so every plain
  // FileFormat-based streaming write's location was this wrapper string,
  // never matching a contract's real declared path. Fixed in
  // WriteCommandSupport.streamSinkLocationAndFormat/streamSinkFormatOf (see
  // that file's own doc comment) by skipping tier 2 for FileStreamSink
  // specifically and reflecting into its private path/fileFormat fields
  // instead - this test is the enforcement-level proof the fix works, not
  // just a translation-level check. ---

  test("PASS: streaming write to parquet resolves to the real physical path, not FileStreamSink's descriptive name()") {
    val inputDir = scratchDir.resolve("stream_write_in")
    val outDir = scratchDir.resolve("stream_write_out").toString
    val ckpt = scratchDir.resolve("stream_write_ckpt").toString
    df().write.parquet(inputDir.toString)
    val schema = spark.read.parquet(inputDir.toString).schema

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outDir
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
      val streamDf = spark.readStream.schema(schema).parquet(inputDir.toString)
      val query = streamDf.writeStream
        .format("parquet")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw
    }
    assert(spark.read.parquet(outDir).count() == 2)
  }

  // Direct-construction test (same technique as SparkPlanAdapterSpec's
  // fallback-diagnostic test) rather than a live streaming query: a real
  // FileStreamSink + WriteToStream built by hand, translated synchronously
  // via SparkPlanAdapter.translate - no QueryExecutionListener race, no
  // micro-batch scheduling involved. Directly exercises both
  // streamSinkLocationAndFormat's FileStreamSink guard (must resolve the
  // real physical path, not "FileSink[<path>]") and streamSinkFormatOf's
  // FileStreamSink/DeltaSink guards (must resolve Some("parquet"), not
  // None or "delta") in one deterministic assertion.
  test("translates a directly-constructed FileStreamSink write with the real path and format=parquet") {
    val outPath = scratchDir.resolve("direct_stream_sink").toString
    val sink = new org.apache.spark.sql.execution.streaming.FileStreamSink(
      spark,
      outPath,
      new org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat(),
      Seq.empty,
      Map.empty[String, String]
    )
    val writeToStream = new org.apache.spark.sql.catalyst.streaming.WriteToStream(
      "direct-construction-test",
      scratchDir.resolve("direct_stream_ckpt").toString,
      sink,
      org.apache.spark.sql.streaming.OutputMode.Append(),
      false,
      df().queryExecution.analyzed,
      None,
      None
    )

    val result = SparkPlanAdapter.translate(writeToStream)
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, format, _) =>
        assert(location == outPath, s"expected the real physical path, got '$location' (the pre-fix bug reported FileStreamSink's descriptive name() instead)")
        assert(format.contains("parquet"), s"expected format Some(parquet), got $format")
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
    // Not asserting result.diagnostics.isEmpty here: df()'s LocalRelation
    // input has no translatePlan case of its own (an incidental fact about
    // this test's fixture, not about the FileStreamSink write being
    // tested), so it always carries its own "no translation for this plan
    // node" diagnostic regardless of whether the fix above is correct.
  }

  test("FAIL: streaming write to parquet at a location that doesn't match the contract is rejected") {
    val inputDir = scratchDir.resolve("stream_write_fail_in")
    val outDir = scratchDir.resolve("stream_write_fail_out").toString
    val ckpt = scratchDir.resolve("stream_write_fail_ckpt").toString
    df().write.parquet(inputDir.toString)
    val schema = spark.read.parquet(inputDir.toString).schema

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: ${scratchDir.resolve("somewhere_else").toString}
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      intercept[ContractViolationException] {
        val streamDf = spark.readStream.schema(schema).parquet(inputDir.toString)
        val query = streamDf.writeStream
          .format("parquet")
          .option("path", outDir)
          .option("checkpointLocation", ckpt)
          .trigger(Trigger.AvailableNow())
          .start()
        query.awaitTermination()
      }
    }
    // Spark's FileStreamSink eagerly creates the output directory (and its
    // _spark_metadata subdirectory) as part of query initialization, before
    // the first micro-batch's plan ever reaches injectCheckRule - so
    // directory existence alone doesn't prove data was committed. No actual
    // data file is the real assertion.
    val dataFiles = Option(new java.io.File(outDir).listFiles())
      .map(_.toSeq).getOrElse(Seq.empty)
      .filter(f => f.getName.endsWith(".parquet"))
    assert(dataFiles.isEmpty, "the rejected streaming write must never have committed any data file")
  }

  // --- Streaming read as a declared contract input: the same generic
  // StreamingRelation handling built for Delta, confirmed here for Parquet's
  // own FileSource (a plain public spark-sql class, unlike Delta's - so this
  // is, if anything, a simpler case than the one already covered). ---

  test("PASS: a streaming Parquet source satisfies a contract's declared input schema") {
    val inputDir = scratchDir.resolve("stream_read_in")
    df().write.parquet(inputDir.toString)
    val schema = spark.read.parquet(inputDir.toString).schema
    val outDir = scratchDir.resolve("stream_read_out").toString
    val ckpt = scratchDir.resolve("stream_read_ckpt").toString

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: ${inputDir.toString}
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
         |    location: $outDir
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
      val streamDf = spark.readStream.schema(schema).parquet(inputDir.toString)
      val query = streamDf.writeStream
        .format("parquet")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw MISSING_INPUT
    }
  }

  // --- Feature surface: Parquet always reports every column nullable on
  // read-back, regardless of what was written. This is NOT a Delta-specific
  // quirk (docs/SPARK_ADAPTER.md previously documented it only under "Delta
  // Lake reads") - confirmed here directly against plain Parquet, with no
  // Delta/Iceberg involved at all, meaning Delta's (and Iceberg's) own
  // instance of this behavior was inherited from their underlying Parquet
  // storage layer all along, not something either connector does itself. ---

  test("feature surface: Parquet read-back reports every field nullable regardless of source nullability") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType, nullable = false), StructField("value", LongType, nullable = false)))
    val data = spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(1L, 10L)))
    val written = spark.createDataFrame(data, schema)
    assert(!written.schema("id").nullable && !written.schema("value").nullable)

    val p = scratchDir.resolve("nullability_feature").toString
    written.write.mode("overwrite").parquet(p)
    val readBack = spark.read.parquet(p)
    assert(readBack.schema("id").nullable, "Parquet read-back should report id nullable regardless of the original schema")
    assert(readBack.schema("value").nullable, "Parquet read-back should report value nullable regardless of the original schema")

    // The practical consequence for a contract author: a field sourced from
    // a Parquet read needs required: false (an ODCS "not-null enforced"
    // claim can't be verified from the file's own schema) - required: true
    // would always spuriously fail with OUTPUT_FIELD_NULLABILITY_MISMATCH,
    // covered by the FAIL test below.
    val outPath = scratchDir.resolve("nullability_feature_out").toString
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

  test("feature surface: a required:true field sourced from a Parquet read is correctly rejected as a nullability mismatch") {
    val p = scratchDir.resolve("nullability_fail_src").toString
    df().write.mode("overwrite").parquet(p)
    val readBack = spark.read.parquet(p)

    val outPath = scratchDir.resolve("nullability_fail_out").toString
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

  // --- Feature surface: schema merging (mergeSchema=true) across
  // heterogeneous Parquet files - the analyzed plan's schema genuinely
  // includes the merged (union) column set, confirmed via a real contract
  // that would MISSING_OUTPUT_FIELD-reject if the extra column weren't
  // present. Confirmed transparent: no WriteCommandSupport/StructuralVerifier
  // change needed. ---

  test("feature surface: mergeSchema=true across heterogeneous files is reflected in the write's verified schema") {
    val dir = scratchDir.resolve("merge_schema_feature")
    Files.createDirectories(dir)
    spark.createDataFrame(Seq((1L, 10L))).toDF("id", "value").write.mode("append").parquet(dir.toString)
    spark.createDataFrame(Seq((2L, 20L, "x"))).toDF("id", "value", "extra").write.mode("append").parquet(dir.toString)
    val merged = spark.read.option("mergeSchema", "true").parquet(dir.toString)
    assert(merged.columns.toSet == Set("id", "value", "extra"))

    val outPath = scratchDir.resolve("merge_schema_feature_out").toString
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
         |        - name: extra
         |          type: string
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      merged.write.mode("overwrite").parquet(outPath) // must not throw MISSING_OUTPUT_FIELD for 'extra'
    }
  }

  // --- Feature surface: partitionBy columns are genuinely present (name and
  // type) in both the read-back and the write's own verified schema - no
  // "generated columns"-style gap the way Delta's onboarding found, since a
  // partition column is always part of query.output for a FileFormat write
  // (Spark needs its value to route rows to the right directory). Confirmed
  // transparent. ---

  test("feature surface: partitionBy columns are present with the correct name/type in the verified write schema") {
    val dir = scratchDir.resolve("part_disc_feature").toString
    spark.createDataFrame(Seq((1L, 10L, "a"), (2L, 20L, "b"))).toDF("id", "value", "part")
      .write.mode("overwrite").partitionBy("part").parquet(dir)
    val readBack = spark.read.parquet(dir)
    assert(readBack.columns.toSet == Set("id", "value", "part"))

    val outPath = scratchDir.resolve("part_disc_feature_out").toString
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
         |        - name: part
         |          type: string
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readBack.write.mode("overwrite").parquet(outPath) // must not throw
    }
  }

  // --- Feature surface: a corrupt/malformed file in the read path fails
  // entirely within Spark's own Parquet-reading machinery - confirmed
  // empirically to happen at schema-inference (footer-reading) time for a
  // multi-file directory, which can occur either while the DataFrame is
  // being constructed or once a job actually runs, depending on file
  // layout and footer-reading order (non-deterministic, so this test
  // doesn't assert exactly which phase). Either way, the failure is
  // orthogonal to Invariant's structural verification: a write whose
  // source can't be read never produces an analyzable write-command plan
  // for ContractEnforcementRule to see at all, and nothing ever commits.
  // No fix needed. ---

  test("feature surface: a directory with a corrupt file fails entirely within Spark, never committing output or reaching Invariant") {
    val dir = scratchDir.resolve("corrupt_feature")
    Files.createDirectories(dir)
    df().write.mode("overwrite").parquet(dir.resolve("good").toString)
    Files.write(dir.resolve("good").resolve("not_really_parquet.parquet"), "not a real parquet file".getBytes("UTF-8"))

    val outPath = scratchDir.resolve("corrupt_feature_out").toString
    val plansBefore = capturedPlans.size
    intercept[Throwable] {
      spark.read.parquet(dir.resolve("good").toString).write.mode("overwrite").parquet(outPath)
    }
    assert(!Files.exists(java.nio.file.Paths.get(outPath)), "a write whose source can't be read must never produce output")
    assert(capturedPlans.size == plansBefore, "a source Spark itself can't read must never reach injectCheckRule as a write command")
  }
}
