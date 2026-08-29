// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Path}

/** CSV-specific coverage, added via the `add-spark-connector` skill
  * (docs/ADDING_A_SPARK_CONNECTOR.md). Like Parquet, CSV is Spark's own
  * built-in `FileFormat` (`org.apache.spark.sql.execution.datasources.csv.CSVFileFormat`),
  * not a separate library — no dependency added, not even test-scoped.
  * Its operation surface is confirmed to route through the exact same
  * generic mechanisms Parquet's pass already confirmed (`InsertIntoHadoopFsRelationCommand`,
  * `CreateDataSourceTableAsSelectCommand`, `WriteToStream`/`FileStreamSink`
  * — including the private-field-reflection fix, since `FileStreamSink`
  * is shared by every `FileFormat`, not Parquet-specific). What's
  * genuinely different from Parquet is the feature surface: CSV is a
  * text format with no native schema, so schema inference, header
  * handling, malformed-record modes, and text-based date/timestamp
  * parsing are all real, CSV-specific behaviors with no Parquet analog.
  * See docs/connectors/csv.md for the full coverage ledger.
  */
class CsvConnectorSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-csv-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CsvConnectorSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  // --- .insertInto(...): same InsertIntoHadoopFsRelationCommand shape as
  // Parquet, confirmed empirically for CSV specifically. ---

  test("translates .insertInto() against an existing table via InsertIntoHadoopFsRelationCommand") {
    spark.sql("CREATE TABLE IF NOT EXISTS csv_insert_into_tbl (id BIGINT, value BIGINT) USING csv")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.insertInto("csv_insert_into_tbl")

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .insertInto() write yet"))
    }
    result.plan match {
      case com.example.ir.Write(com.example.ir.DatasetRef(location), _, format, saveMode) =>
        assert(location.contains("csv_insert_into_tbl"))
        assert(format.contains("csv"))
        assert(saveMode.contains("append"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  // --- .saveAsTable() append onto an existing table: confirmed to be the
  // same nested-double-write pattern found for Parquet
  // (CreateDataSourceTableAsSelectCommand delegating to a nested
  // InsertIntoHadoopFsRelationCommand), not assumed to generalize -
  // verified directly for CSV. ---

  test("PASS: .saveAsTable() append onto an existing table - both nested Command plans see a satisfying write") {
    val tblPath = scratchDir.resolve("nested_pass").toString
    df().write.format("csv").option("path", tblPath).mode("overwrite").saveAsTable("csv_nested_pass_tbl")

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
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      df().write.format("csv").mode("append").saveAsTable("csv_nested_pass_tbl") // must not throw
    }
  }

  test("FAIL: .saveAsTable() append onto an existing table - rejected at the outer command, nested insert never runs") {
    val tblPath = scratchDir.resolve("nested_fail").toString
    df().write.format("csv").option("path", tblPath).mode("overwrite").saveAsTable("csv_nested_fail_tbl")
    val rowCountBefore = spark.table("csv_nested_fail_tbl").count()

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
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |        - name: missing_field
         |          type: long
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        df().write.format("csv").mode("append").saveAsTable("csv_nested_fail_tbl")
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("csv_nested_fail_tbl").count() == rowCountBefore, "the nested insert must never have run")
  }

  // --- .writeTo(...) DataFrameWriterV2: confirmed empirically (not
  // assumed from the Parquet precedent) to be rejected by Spark itself
  // against an existing plain-CSV table, and to reuse the existing
  // CreateDataSourceTableAsSelectCommand translation for a genuinely new
  // table with the format made explicit. ---

  test(".writeTo() against an existing plain-CSV table is rejected by Spark itself, not by Invaract") {
    spark.sql("CREATE TABLE IF NOT EXISTS csv_writeto_existing_tbl (id BIGINT, value BIGINT) USING csv")
    val ex1 = intercept[org.apache.spark.sql.AnalysisException](df().writeTo("csv_writeto_existing_tbl").append())
    assert(ex1.getMessage.contains("Cannot write into v1 table"))
    val ex2 = intercept[org.apache.spark.sql.AnalysisException](
      df().writeTo("csv_writeto_existing_tbl").overwrite(org.apache.spark.sql.functions.lit(true))
    )
    assert(ex2.getMessage.contains("Cannot write into v1 table"))
    assert(spark.table("csv_writeto_existing_tbl").count() == 0, "no rejected write should have committed any data")
  }

  test(".writeTo(...).createOrReplace()/.replace() against plain csv tables are rejected by Spark itself, new or existing table alike") {
    spark.sql("CREATE TABLE IF NOT EXISTS csv_replace_existing_tbl (id BIGINT, value BIGINT) USING csv")
    val ex1 = intercept[org.apache.spark.sql.AnalysisException](
      df().writeTo("csv_replace_existing_tbl").using("csv").replace()
    )
    assert(ex1.getMessage.contains("does not support REPLACE TABLE AS SELECT") || ex1.getMessage.contains("REPLACE TABLE"))
    val ex2 = intercept[org.apache.spark.sql.AnalysisException](
      df().writeTo("csv_replace_new_tbl").using("csv").createOrReplace()
    )
    assert(ex2.getMessage.contains("does not support REPLACE TABLE AS SELECT") || ex2.getMessage.contains("REPLACE TABLE"))
    assert(spark.table("csv_replace_existing_tbl").count() == 0, "no rejected replace should have committed any data")
  }

  test(".writeTo(...).using(\"csv\").create() on a new table reuses the existing CreateDataSourceTableAsSelectCommand translation") {
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().writeTo("csv_writeto_create_tbl").using("csv").create()

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .writeTo().create() write yet"))
    }
    result.plan match {
      case com.example.ir.Write(_, _, format, _) => assert(format.contains("csv"))
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
    assert(spark.table("csv_writeto_create_tbl").count() == 2)
  }

  // --- Format-specific DML: confirmed empirically that Spark itself
  // refuses MERGE/UPDATE/DELETE against a plain CSV table, the exact same
  // rejection messages as Parquet's - a generic V1-table architectural
  // constraint, not a CSV-specific finding, but verified directly rather
  // than assumed. ---

  test("MERGE/UPDATE/DELETE against a plain csv table are rejected by Spark itself, nothing reaches Invaract") {
    spark.sql("CREATE TABLE IF NOT EXISTS csv_dml_target_tbl (id BIGINT, value BIGINT) USING csv")
    spark.sql("INSERT INTO csv_dml_target_tbl VALUES (1, 10)")
    spark.sql("CREATE TABLE IF NOT EXISTS csv_dml_source_tbl (id BIGINT, value BIGINT) USING csv")

    val mergeEx = intercept[Exception] {
      spark.sql(
        "MERGE INTO csv_dml_target_tbl t USING csv_dml_source_tbl s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT *"
      ).collect()
    }
    assert(mergeEx.getMessage.contains("MERGE INTO TABLE is not supported"))
    val updateEx = intercept[Exception] {
      spark.sql("UPDATE csv_dml_target_tbl SET value = 0 WHERE id = 1").collect()
    }
    assert(updateEx.getMessage.contains("UPDATE TABLE is not supported"))
    assertThrows[org.apache.spark.sql.AnalysisException] {
      spark.sql("DELETE FROM csv_dml_target_tbl WHERE id = 1").collect()
    }
    assert(spark.table("csv_dml_target_tbl").count() == 1, "no rejected DML should have changed the table")
  }

  // --- Streaming write/read: WriteToStream/StreamingRelation, generic
  // since Delta and already fixed connector-agnostically during the
  // Parquet pass (FileStreamSink's private path/fileFormat fields).
  // Direct-construction test (no live streaming query, matching
  // ParquetConnectorSpec's technique) confirms this generalizes to CSV
  // specifically, not assumed. ---

  test("translates a directly-constructed FileStreamSink write with the real path and format=csv") {
    val outPath = scratchDir.resolve("direct_stream_sink").toString
    val sink = new org.apache.spark.sql.execution.streaming.FileStreamSink(
      spark,
      outPath,
      new org.apache.spark.sql.execution.datasources.csv.CSVFileFormat(),
      Seq.empty,
      Map.empty[String, String]
    )
    val writeToStream = new org.apache.spark.sql.catalyst.streaming.WriteToStream(
      "direct-construction-test-csv",
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
        assert(location == outPath, s"expected the real physical path, got '$location'")
        assert(format.contains("csv"), s"expected format Some(csv), got $format")
      case other => fail(s"expected a Write, got ${com.example.ir.PlanPrinter.render(other)}")
    }
  }

  test("PASS: streaming write to csv satisfying its contract executes normally") {
    val inputDir = scratchDir.resolve("stream_write_in")
    val outDir = scratchDir.resolve("stream_write_out").toString
    val ckpt = scratchDir.resolve("stream_write_ckpt").toString
    df().write.option("header", "true").csv(inputDir.toString)
    val schema = spark.read.option("header", "true").option("inferSchema", "true").csv(inputDir.toString).schema

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outDir
         |    schema:
         |      fields:
         |        - name: id
         |          type: integer
         |          required: false
         |        - name: value
         |          type: integer
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.schema(schema).option("header", "true").csv(inputDir.toString)
      val query = streamDf.writeStream
        .format("csv")
        .option("header", "true")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw
    }
    assert(spark.read.option("header", "true").option("inferSchema", "true").csv(outDir).count() == 2)
  }

  test("PASS: a streaming CSV source satisfies a contract's declared input schema") {
    val inputDir = scratchDir.resolve("stream_read_in")
    df().write.option("header", "true").csv(inputDir.toString)
    val schema = spark.read.option("header", "true").option("inferSchema", "true").csv(inputDir.toString).schema
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
         |          type: integer
         |          required: false
         |        - name: value
         |          type: integer
         |          required: false
         |outputs:
         |  - name: out
         |    location: $outDir
         |    schema:
         |      fields:
         |        - name: id
         |          type: integer
         |          required: false
         |        - name: value
         |          type: integer
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.schema(schema).option("header", "true").csv(inputDir.toString)
      val query = streamDf.writeStream
        .format("csv")
        .option("header", "true")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw MISSING_INPUT
    }
  }

  // --- Feature surface: without inferSchema or an explicit schema, CSV
  // defaults every column to StringType - genuinely different from
  // Parquet (self-describing, no such default exists). Real, CSV-specific
  // gotcha: a contract declaring a numeric/date/etc. type for a field
  // sourced from a plain (no-inferSchema) CSV read is correctly rejected
  // as a type mismatch, not silently passed. ---

  test("feature surface: without inferSchema, CSV defaults every column to StringType") {
    val p = scratchDir.resolve("no_infer_schema").toString
    df().write.mode("overwrite").option("header", "true").csv(p)
    val readBack = spark.read.option("header", "true").csv(p) // no inferSchema
    assert(readBack.schema("id").dataType == org.apache.spark.sql.types.StringType)
    assert(readBack.schema("value").dataType == org.apache.spark.sql.types.StringType)

    val outPath = scratchDir.resolve("no_infer_schema_out").toString
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
         |""".stripMargin
    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        readBack.write.mode("overwrite").option("header", "true").csv(outPath)
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.OutputFieldTypeMismatch))
  }

  test("feature surface: inferSchema=true resolves real types, satisfying a contract declaring them") {
    val p = scratchDir.resolve("infer_schema").toString
    df().write.mode("overwrite").option("header", "true").csv(p)
    val readBack = spark.read.option("header", "true").option("inferSchema", "true").csv(p)
    assert(readBack.schema("id").dataType == org.apache.spark.sql.types.IntegerType)

    val outPath = scratchDir.resolve("infer_schema_out").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: integer
         |          required: false
         |        - name: value
         |          type: integer
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readBack.write.mode("overwrite").option("header", "true").csv(outPath) // must not throw
    }
  }

  // --- Feature surface: header handling. Confirmed transparent: with
  // header=true the real column names are used; without it, CSV falls
  // back to positional _c0/_c1/... names - both are just ordinary column
  // names as far as translation/verification are concerned, no special
  // handling needed. ---

  test("feature surface: header=false falls back to positional _c0/_c1 column names, contract can still be satisfied") {
    val p = scratchDir.resolve("no_header").toString
    df().write.mode("overwrite").csv(p) // no header option
    val readBack = spark.read.schema(
      org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("_c0", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("_c1", org.apache.spark.sql.types.LongType)
      ))
    ).csv(p)
    assert(readBack.columns.toSeq == Seq("_c0", "_c1"))

    val outPath = scratchDir.resolve("no_header_out").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outPath
         |    schema:
         |      fields:
         |        - name: _c0
         |          type: long
         |          required: false
         |        - name: _c1
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readBack.write.mode("overwrite").csv(outPath) // must not throw
    }
  }

  // --- Feature surface: malformed-record modes. FAILFAST confirmed to
  // fail only at execution (task/job failure reading the bad record),
  // never at analysis - the same "orthogonal to Invaract's structural
  // check" pattern already confirmed for Parquet's corrupt-file case.
  // DROPMALFORMED confirmed to silently exclude bad rows from what's
  // written, with the analyzed schema unaffected. ---

  test("feature surface: FAILFAST mode fails only at execution, never at analysis") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType), StructField("value", LongType)))
    val dir = scratchDir.resolve("failfast_dir")
    Files.createDirectories(dir)
    Files.write(dir.resolve("data.csv"), "1,10\nnotanumber,20\n".getBytes("UTF-8"))

    val readDf = spark.read.schema(schema).option("mode", "FAILFAST").csv(dir.toString)
    noException should be thrownBy readDf.queryExecution.analyzed
    assertThrows[org.apache.spark.SparkException](readDf.collect())
  }

  test("feature surface: DROPMALFORMED silently excludes bad rows, satisfies a contract on the remaining ones") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType), StructField("value", LongType)))
    val dir = scratchDir.resolve("dropmalformed_dir")
    Files.createDirectories(dir)
    Files.write(dir.resolve("data.csv"), "1,10\nnotanumber,20\n3,30\n".getBytes("UTF-8"))

    val readDf = spark.read.schema(schema).option("mode", "DROPMALFORMED").csv(dir.toString)
    assert(readDf.collect().length == 2, "the malformed row must be dropped, not the other two")

    val outPath = scratchDir.resolve("dropmalformed_out").toString
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
      readDf.write.mode("overwrite").csv(outPath) // must not throw
    }
  }

  // --- Feature surface: the corrupt-record column (columnNameOfCorruptRecord).
  // Confirmed transparent: declaring it just adds an ordinary extra
  // StringType column to the schema - Invaract sees it like any other
  // field, no special handling needed. ---

  test("feature surface: columnNameOfCorruptRecord is an ordinary extra column in the analyzed schema") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("value", LongType),
      StructField("_corrupt_record", StringType)
    ))
    val dir = scratchDir.resolve("corrupt_col_dir")
    Files.createDirectories(dir)
    Files.write(dir.resolve("data.csv"), "1,10\nnotanumber,20\n".getBytes("UTF-8"))

    val readDf = spark.read.schema(schema).option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record").csv(dir.toString)
    assert(readDf.schema.fieldNames.contains("_corrupt_record"))

    val outPath = scratchDir.resolve("corrupt_col_out").toString
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
         |        - name: _corrupt_record
         |          type: string
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readDf.write.mode("overwrite").csv(outPath) // must not throw
    }
  }

  // --- Feature surface: nullability on read-back. Confirmed for CSV
  // independently (not assumed from Parquet's finding): every field
  // reports nullable=true after a write+read round-trip, even when an
  // explicit schema declares otherwise. Same practical consequence for
  // contract authors as Parquet's. ---

  test("feature surface: CSV read-back reports every field nullable regardless of source nullability") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType, nullable = false), StructField("value", LongType, nullable = false)))
    val data = spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(1L, 10L)))
    val written = spark.createDataFrame(data, schema)
    assert(!written.schema("id").nullable)

    val p = scratchDir.resolve("nullability_feature").toString
    written.write.mode("overwrite").option("header", "true").csv(p)
    val readBack = spark.read.option("header", "true").schema(schema).csv(p)
    assert(readBack.schema("id").nullable, "CSV read-back should report id nullable regardless of the declared schema")
    assert(readBack.schema("value").nullable, "CSV read-back should report value nullable regardless of the declared schema")
  }

  // --- Feature surface: text-based date parsing with a custom dateFormat.
  // Confirmed transparent under PERMISSIVE mode: an unparseable date
  // becomes null in the row, not a thrown exception or an analysis-time
  // failure - the schema stays DateType either way. ---

  test("feature surface: an unparseable date under a custom dateFormat becomes null, not a failure") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType), StructField("d", DateType)))
    val dir = scratchDir.resolve("date_format_dir")
    Files.createDirectories(dir)
    Files.write(dir.resolve("data.csv"), "id,d\n1,2024-01-15\n2,not-a-date\n".getBytes("UTF-8"))

    val readDf = spark.read.schema(schema).option("header", "true").option("dateFormat", "yyyy-MM-dd")
      .option("mode", "PERMISSIVE").csv(dir.toString)
    val rows = readDf.collect()
    assert(rows.length == 2, "PERMISSIVE mode must not drop the row with the unparseable date")
    assert(readDf.schema("d").dataType == DateType)

    val outPath = scratchDir.resolve("date_format_out").toString
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
         |        - name: d
         |          type: date
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readDf.write.mode("overwrite").option("header", "true").csv(outPath) // must not throw
    }
  }

  // --- Regression: CSV's own safe-listed DDL isn't blocked under a
  // contract that would reject anything it actually checked. ---

  test("regression: CREATE TABLE, ANALYZE TABLE, SHOW TABLES for csv tables aren't blocked under an active contract") {
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
      spark.sql("CREATE TABLE IF NOT EXISTS csv_regression_tbl (id INT) USING csv").collect()
      spark.sql("ANALYZE TABLE csv_regression_tbl COMPUTE STATISTICS").collect()
      spark.sql("SHOW TABLES").collect()
    }
  }
}
