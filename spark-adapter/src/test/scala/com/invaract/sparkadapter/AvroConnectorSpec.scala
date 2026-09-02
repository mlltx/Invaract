// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.{Files, Path}

/** Avro-specific coverage, added via the `add-spark-connector` skill
  * (docs/ADDING_A_SPARK_CONNECTOR.md). Unlike Parquet/CSV (Spark's own
  * bundled `FileFormat`s), Avro (`org.apache.spark.sql.avro.AvroFileFormat`)
  * ships in a separate first-party artifact (`spark-avro`) - the first real
  * `% "test"` dependency addition since Delta/Iceberg/Hive. A real
  * reflective jar scan (docs/ADDING_A_SPARK_CONNECTOR.md's Phase 3) found
  * zero `Command`-shaped classes in `spark-avro` at all - no SQL-extension
  * commands, unlike Delta/Iceberg/Hive - so this connector's operation
  * surface routes entirely through the exact same generic mechanisms
  * Parquet/CSV's passes already confirmed (`InsertIntoHadoopFsRelationCommand`,
  * `CreateDataSourceTableAsSelectCommand`, `WriteToStream`/`FileStreamSink`).
  * What's genuinely different from Parquet/CSV is the feature surface:
  * Avro is a schema-carrying binary format with logical types, explicit
  * external-schema support (`avroSchema` option), and union-type-based
  * nullability representation. See docs/connectors/avro.md for the full
  * coverage ledger.
  *
  * This pass also closed a real, previously-documented-but-unfixed gap
  * found via Avro's own path-less new-table `.saveAsTable()`: see the
  * "path-less new-table location fix" tests below and
  * `WriteCommandSupport.createDataSourceTableAsSelect`'s updated doc
  * comment.
  */
class AvroConnectorSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-avro-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("AvroConnectorSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  // --- .save(path), all save modes: same InsertIntoHadoopFsRelationCommand
  // shape as Parquet/CSV, confirmed empirically for Avro specifically. ---

  test("translates .save(path) with format=avro via InsertIntoHadoopFsRelationCommand") {
    val p = scratchDir.resolve("save_translate").toString
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.mode("overwrite").format("avro").save(p)

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .save() write yet"))
    }
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, format, saveMode) =>
        // Spark resolves the raw path through Hadoop's Path/FileSystem
        // machinery, which prepends the local filesystem's "file:" scheme
        // and always uses forward slashes - `p` is `scratchDir.resolve(...)
        // .toString`, the OS-native form (backslashes on Windows), so a raw
        // substring check fails there even though the paths are the same
        // location. Normalize separators the same way
        // StructuralVerifier.locationsMatch does before comparing.
        assert(location.replace('\\', '/').contains(p.replace('\\', '/')))
        assert(format.contains("avro"))
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  test("PASS: .save(path) satisfying its contract executes normally, all four save modes") {
    val p = scratchDir.resolve("save_modes").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $p
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
      df().write.mode("overwrite").format("avro").save(p) // must not throw
      df().write.mode("append").format("avro").save(p) // must not throw
      df().write.mode("ignore").format("avro").save(p) // must not throw
    }
  }

  test("FAIL: .save(path) violating its contract is rejected, nothing committed") {
    val p = scratchDir.resolve("save_fail").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $p
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
        df().write.mode("overwrite").format("avro").save(p)
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(!Files.exists(java.nio.file.Paths.get(p)), "a rejected write must not have committed any data")
  }

  // --- .insertInto(...): same InsertIntoHadoopFsRelationCommand shape as
  // Parquet/CSV, confirmed empirically for Avro specifically. ---

  test("translates .insertInto() against an existing table via InsertIntoHadoopFsRelationCommand") {
    spark.sql("CREATE TABLE IF NOT EXISTS avro_insert_into_tbl (id BIGINT, value BIGINT) USING avro")
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().write.insertInto("avro_insert_into_tbl")

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .insertInto() write yet"))
    }
    result.plan match {
      case com.invaract.ir.Write(com.invaract.ir.DatasetRef(location), _, format, saveMode) =>
        assert(location.contains("avro_insert_into_tbl"))
        assert(format.contains("avro"))
        assert(saveMode.contains("append"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
  }

  // --- .saveAsTable(): new table (explicit path) and existing table
  // (append) - the same nested-double-write pattern Parquet/CSV already
  // confirmed (CreateDataSourceTableAsSelectCommand delegating to a nested
  // InsertIntoHadoopFsRelationCommand). ---

  test("PASS: .saveAsTable() append onto an existing table - both nested Command plans see a satisfying write") {
    val tblPath = scratchDir.resolve("nested_pass").toString
    df().write.format("avro").option("path", tblPath).mode("overwrite").saveAsTable("avro_nested_pass_tbl")

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
      df().write.format("avro").mode("append").saveAsTable("avro_nested_pass_tbl") // must not throw
    }
  }

  test("FAIL: .saveAsTable() append onto an existing table - rejected at the outer command, nested insert never runs") {
    val tblPath = scratchDir.resolve("nested_fail").toString
    df().write.format("avro").option("path", tblPath).mode("overwrite").saveAsTable("avro_nested_fail_tbl")
    val rowCountBefore = spark.table("avro_nested_fail_tbl").count()

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
        df().write.format("avro").mode("append").saveAsTable("avro_nested_fail_tbl")
      }
    }
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    assert(spark.table("avro_nested_fail_tbl").count() == rowCountBefore, "the nested insert must never have run")
  }

  // --- Path-less new-table location fix: real bug found and fixed this
  // pass (see WriteCommandSupport.createDataSourceTableAsSelect's updated
  // doc comment). Before the fix, a brand-new table created with no
  // explicit `.option("path", ...)` produced two Command-shaped plans
  // (the outer CreateDataSourceTableAsSelectCommand and a nested
  // InsertIntoHadoopFsRelationCommand) whose translated locations could
  // never agree - the outer fell back to the bare qualified identifier
  // (`spark_catalog.default.t`), the nested resolved the real physical
  // warehouse path (`file:/.../t`) - so no single contract `location`
  // value could satisfy both checks for what is, logically, one write.
  // Confirmed empirically via a direct WriteCommandSupport.combined
  // comparison before writing this fix (not assumed from Parquet's own
  // "just not attempted here" note). ---

  test("path-less new-table .saveAsTable(): outer and nested WriteCommandInfo.location now agree") {
    // Reuses the shared spec-level `spark` session (already wired to
    // `capturedPlans` via injectCheckRule in beforeAll) rather than
    // constructing a second SparkSession - SparkSession.builder().getOrCreate()
    // silently returns the already-active session when one exists in this
    // JVM (ignoring a second .withExtensions() call entirely), and a second
    // session sharing the same underlying SparkContext would have its
    // .stop() call tear down the shared context out from under every other
    // test in this suite - confirmed the hard way, not assumed.
    val startIndex = capturedPlans.size
    df().write.format("avro").mode("overwrite").saveAsTable("avro_locfix_tbl")
    val newPlans = capturedPlans.drop(startIndex)

    val outer = newPlans.collectFirst { case c: org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand => c }
      .getOrElse(fail("expected a CreateDataSourceTableAsSelectCommand"))
    val nested = newPlans.collectFirst { case c: org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand => c }
      .getOrElse(fail("expected a nested InsertIntoHadoopFsRelationCommand"))

    val outerInfo = WriteCommandSupport.combined.lift(outer).getOrElse(fail("outer not recognized"))
    val nestedInfo = WriteCommandSupport.combined.lift(nested).getOrElse(fail("nested not recognized"))

    assert(outerInfo.location == nestedInfo.location,
      s"outer (${outerInfo.location}) and nested (${nestedInfo.location}) locations must agree")
    assert(outerInfo.diagnostic.isEmpty, "the default-table-path resolution must not need a fallback diagnostic")
    assert(outerInfo.location.startsWith("file:"), "must be the real physical path, not the bare identifier")
  }

  test("PASS: path-less new-table .saveAsTable() satisfies a contract declared at the resolved default warehouse path") {
    // StructuralVerifier.locationsMatch strips "file:" only from the
    // *actual* plan-reported location, never from the *declared* side - a
    // contract's `location` must be given as a plain path, matching every
    // other test in this file/repo (see docs/connectors/; none embed a
    // "file:" scheme in a YAML `location:` value).
    //
    // The expected location is derived from a real WriteCommandSupport
    // resolution for a throwaway probe table, not hand-built via
    // java.net.URI/File.toURI - confirmed the hard way (a real Windows/macOS
    // CI failure) that hand-constructing this path doesn't reliably match
    // what Hadoop's own Path/FileSystem machinery reports on every OS
    // (drive-letter/separator/symlink-canonicalization differences), the
    // same class of platform variance locationsMatch's own doc comment
    // already warns about for the *declared* side.
    val probeIndex = capturedPlans.size
    df().write.format("avro").mode("overwrite").saveAsTable("avro_locfix_discover_tbl")
    val probeOuter = capturedPlans.drop(probeIndex)
      .collectFirst { case c: org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand => c }
      .getOrElse(fail("expected a CreateDataSourceTableAsSelectCommand"))
    val probeLocation = WriteCommandSupport.combined.lift(probeOuter).getOrElse(fail("probe not recognized")).location
    val prefix = probeLocation.stripSuffix("avro_locfix_discover_tbl")
    val expectedLocation = (prefix + "avro_locfix_pass_tbl").stripPrefix("file:")
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
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      df().write.format("avro").mode("overwrite").saveAsTable("avro_locfix_pass_tbl") // must not throw
    }
  }

  // --- .writeTo(...) DataFrameWriterV2: confirmed empirically to be
  // rejected by Spark itself against an existing plain-Avro table, and to
  // reuse the existing CreateDataSourceTableAsSelectCommand translation for
  // a genuinely new table with the format made explicit. ---

  test(".writeTo() against an existing plain-avro table is rejected by Spark itself, not by Invaract") {
    spark.sql("CREATE TABLE IF NOT EXISTS avro_writeto_existing_tbl (id BIGINT, value BIGINT) USING avro")
    val ex1 = intercept[org.apache.spark.sql.AnalysisException](df().writeTo("avro_writeto_existing_tbl").append())
    assert(ex1.getMessage.contains("Cannot write into v1 table"))
    val ex2 = intercept[org.apache.spark.sql.AnalysisException](
      df().writeTo("avro_writeto_existing_tbl").overwrite(org.apache.spark.sql.functions.lit(true))
    )
    assert(ex2.getMessage.contains("Cannot write into v1 table"))
    assert(spark.table("avro_writeto_existing_tbl").count() == 0, "no rejected write should have committed any data")
  }

  test(".writeTo(...).using(\"avro\").create() on a new table reuses the existing CreateDataSourceTableAsSelectCommand translation") {
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df().writeTo("avro_writeto_create_tbl").using("avro").create()

    val result = org.scalatest.concurrent.Eventually.eventually(
      org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
    ) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .writeTo().create() write yet"))
    }
    result.plan match {
      case com.invaract.ir.Write(_, _, format, _) => assert(format.contains("avro"))
      case other => fail(s"expected a Write, got ${com.invaract.ir.PlanPrinter.render(other)}")
    }
    assert(spark.table("avro_writeto_create_tbl").count() == 2)
  }

  // --- Format-specific DML: confirmed empirically that Spark itself
  // refuses MERGE/UPDATE/DELETE against a plain Avro table, the exact same
  // rejection messages as Parquet/CSV's - a generic V1-table architectural
  // constraint, not an Avro-specific finding, but verified directly. ---

  test("MERGE/UPDATE/DELETE against a plain avro table are rejected by Spark itself, nothing reaches Invaract") {
    spark.sql("CREATE TABLE IF NOT EXISTS avro_dml_target_tbl (id BIGINT, value BIGINT) USING avro")
    spark.sql("INSERT INTO avro_dml_target_tbl VALUES (1, 10)")
    spark.sql("CREATE TABLE IF NOT EXISTS avro_dml_source_tbl (id BIGINT, value BIGINT) USING avro")

    val mergeEx = intercept[Exception] {
      spark.sql(
        "MERGE INTO avro_dml_target_tbl t USING avro_dml_source_tbl s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT *"
      ).collect()
    }
    assert(mergeEx.getMessage.contains("MERGE INTO TABLE is not supported"))
    val updateEx = intercept[Exception] {
      spark.sql("UPDATE avro_dml_target_tbl SET value = 0 WHERE id = 1").collect()
    }
    assert(updateEx.getMessage.contains("UPDATE TABLE is not supported"))
    assertThrows[org.apache.spark.sql.AnalysisException] {
      spark.sql("DELETE FROM avro_dml_target_tbl WHERE id = 1").collect()
    }
    assert(spark.table("avro_dml_target_tbl").count() == 1, "no rejected DML should have changed the table")
  }

  // --- Streaming write/read: WriteToStream/StreamingRelation, generic
  // since Delta and already fixed connector-agnostically during the
  // Parquet pass. Confirmed for Avro specifically. A per-microbatch
  // WriteToMicroBatchDataSourceV1 node was also observed reaching
  // injectCheckRule during investigation - confirmed NOT Command-shaped
  // (isInstanceOf[Command] == false), so it's inert with respect to
  // ContractEnforcementRule's fail-closed net; the one real enforcement
  // point is the outer WriteToStream node, already covered below. ---

  test("PASS: streaming write to avro satisfying its contract executes normally") {
    val inputDir = scratchDir.resolve("stream_write_in")
    val outDir = scratchDir.resolve("stream_write_out").toString
    val ckpt = scratchDir.resolve("stream_write_ckpt").toString
    df().write.format("avro").save(inputDir.toString)
    val schema = spark.read.format("avro").load(inputDir.toString).schema

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
      val streamDf = spark.readStream.schema(schema).format("avro").load(inputDir.toString)
      val query = streamDf.writeStream
        .format("avro")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw
    }
    assert(spark.read.format("avro").load(outDir).count() == 2)
  }

  test("PASS: a streaming avro source satisfies a contract's declared input schema") {
    val inputDir = scratchDir.resolve("stream_read_in")
    df().write.format("avro").save(inputDir.toString)
    val schema = spark.read.format("avro").load(inputDir.toString).schema
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
      val streamDf = spark.readStream.schema(schema).format("avro").load(inputDir.toString)
      val query = streamDf.writeStream
        .format("avro")
        .option("path", outDir)
        .option("checkpointLocation", ckpt)
        .trigger(Trigger.AvailableNow())
        .start()
      query.awaitTermination() // must not throw MISSING_INPUT
    }
  }

  // --- Feature surface: avroSchema option (explicit external reader
  // schema). Confirmed transparent: Invaract sees exactly the schema
  // Spark reports for the read (the avroSchema-declared shape, including
  // an extra field the underlying data doesn't carry, read back as null),
  // no special handling needed. ---

  test("feature surface: avroSchema option's declared schema is what Invaract sees on read, including an extra field") {
    val p = scratchDir.resolve("avro_schema_feature").toString
    df().write.mode("overwrite").format("avro").save(p)
    val explicitSchema =
      """{
        |  "type": "record",
        |  "name": "topLevelRecord",
        |  "fields": [
        |    {"name": "id", "type": "long"},
        |    {"name": "value", "type": "long"},
        |    {"name": "extra", "type": ["null", "string"], "default": null}
        |  ]
        |}""".stripMargin
    val readDf = spark.read.format("avro").option("avroSchema", explicitSchema).load(p)
    assert(readDf.schema.fieldNames.toSeq == Seq("id", "value", "extra"))
    assert(readDf.collect().forall(_.isNullAt(2)), "the extra field must read back null, not fail")

    val outPath = scratchDir.resolve("avro_schema_feature_out").toString
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
      readDf.write.mode("overwrite").format("avro").save(outPath) // must not throw
    }
  }

  // --- Feature surface: logical types (decimal, date, timestamp). Round
  // trip confirmed exact - a real, Avro-specific behavior worth a
  // permanent test since Avro represents these via its own logical-type
  // annotations over primitive Avro types (bytes/int/long), not native
  // container types the way Parquet does. date/timestamp satisfy a
  // contract declaring them normally. ---

  test("feature surface: decimal/date/timestamp logical types round-trip exactly; date/timestamp satisfy a declaring contract") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("amount", DecimalType(10, 2)),
      StructField("d", DateType),
      StructField("ts", TimestampType)
    ))
    val data = spark.sparkContext.parallelize(Seq(
      org.apache.spark.sql.Row(
        1L,
        new java.math.BigDecimal("123.45"),
        java.sql.Date.valueOf("2024-01-15"),
        java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
      )
    ))
    val written = spark.createDataFrame(data, schema)
    val p = scratchDir.resolve("logical_types_feature").toString
    written.write.mode("overwrite").format("avro").save(p)
    val readBack = spark.read.format("avro").load(p)
    assert(readBack.schema("amount").dataType == DecimalType(10, 2))
    assert(readBack.schema("d").dataType == DateType)
    assert(readBack.schema("ts").dataType == TimestampType)
    val row = readBack.collect().head
    assert(row.getDecimal(1).compareTo(new java.math.BigDecimal("123.45")) == 0)

    // 'amount' deliberately left undeclared here (rejectUndeclaredFields
    // defaults false) - see the dedicated test below for why a contract
    // can't actually declare a matching type for it today.
    val outPath = scratchDir.resolve("logical_types_feature_out").toString
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
         |        - name: ts
         |          type: timestamp
         |          required: false
         |""".stripMargin
    withContract(yaml) {
      readBack.write.mode("overwrite").format("avro").save(outPath) // must not throw
    }
  }

  // --- Feature surface: a real, pre-existing gap found via Avro's decimal
  // logical type - NOT Avro-specific, and out of scope to fix in this
  // connector pass. `ContractValidator.KnownTypes` accepts the bare literal
  // "decimal" as a valid `type:` value, but `StructuralVerifier.checkSchema`
  // compares against Spark's own `DataType.typeName` (`StructuralVerifier.scala`,
  // `actualField.dataType.typeName`), which for `DecimalType` always
  // includes precision/scale (e.g. "decimal(10,2)") - so a contract
  // declaring the bare "decimal" keyword can never match *any* decimal
  // field, from any connector, and `KnownTypes` has no parametrized
  // "decimal(p,s)" form to declare instead. Confirmed empirically this
  // pass (not previously exercised anywhere in this suite - no existing
  // Parquet/CSV/Delta/Iceberg/Hive test declares a decimal-typed contract
  // field either). This is a `contract`/`StructuralVerifier` type-vocabulary
  // gap, not a spark-adapter connector-translation bug - fixing it needs
  // `ContractValidator`/`StructuralVerifier` to accept and compare a
  // parametrized decimal type, a `contract` module design change outside
  // this connector pass's scope. Documented here, not silently worked
  // around, so it's discoverable rather than rediscovered from scratch. ---

  test("feature surface: a contract cannot declare a matching type for any decimal field today (pre-existing gap, not Avro-specific)") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType), StructField("amount", DecimalType(10, 2))))
    val data = spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(1L, new java.math.BigDecimal("123.45"))))
    val written = spark.createDataFrame(data, schema)
    val p = scratchDir.resolve("decimal_gap_feature").toString
    written.write.mode("overwrite").format("avro").save(p)
    val readBack = spark.read.format("avro").load(p)

    val outPath = scratchDir.resolve("decimal_gap_feature_out").toString
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
         |        - name: amount
         |          type: decimal
         |          required: false
         |""".stripMargin
    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        readBack.write.mode("overwrite").format("avro").save(outPath)
      }
    }
    assert(ex.result.violations.exists(v =>
      v.violationType == ViolationType.OutputFieldTypeMismatch && v.message.contains("decimal(10,2)")
    ), "expected the bare 'decimal' declaration to mismatch against Spark's typeName-including-precision-scale")
  }

  // --- Feature surface: nullability on read-back (Avro represents an
  // optional field as a ["null", T] union). Confirmed for Avro
  // independently (not assumed from Parquet/CSV/Delta's finding): every
  // field reports nullable=true after a write+read round-trip, even when
  // the written DataFrame's schema declared non-nullable columns. Same
  // practical consequence for contract authors as the other connectors. ---

  test("feature surface: avro read-back reports every field nullable regardless of source nullability") {
    import org.apache.spark.sql.types._
    val schema = StructType(Seq(StructField("id", LongType, nullable = false), StructField("value", LongType, nullable = false)))
    val data = spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(1L, 10L)))
    val written = spark.createDataFrame(data, schema)
    assert(!written.schema("id").nullable)

    val p = scratchDir.resolve("nullability_feature").toString
    written.write.mode("overwrite").format("avro").save(p)
    val readBack = spark.read.format("avro").load(p)
    assert(readBack.schema("id").nullable, "avro read-back should report id nullable regardless of the declared schema")
    assert(readBack.schema("value").nullable, "avro read-back should report value nullable regardless of the declared schema")
  }

  // --- Feature surface: recordName/recordNamespace write options (Avro's
  // own record-identity metadata). Confirmed transparent: purely a
  // writer-side detail of the emitted Avro schema's `name`/`namespace`
  // fields, with zero effect on the DataFrame-facing schema Invaract sees
  // on read-back. ---

  test("feature surface: recordName/recordNamespace write options don't affect the read-back schema") {
    val p = scratchDir.resolve("record_name_feature").toString
    df().write.mode("overwrite").format("avro")
      .option("recordName", "InvaractProbeRecord")
      .option("recordNamespace", "com.invaract.probe")
      .save(p)
    val readBack = spark.read.format("avro").load(p)
    assert(readBack.schema.fieldNames.toSeq == Seq("id", "value"))

    val outPath = scratchDir.resolve("record_name_feature_out").toString
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
      readBack.write.mode("overwrite").format("avro").save(outPath) // must not throw
    }
  }

  // --- Feature surface: compression codec options. Confirmed transparent:
  // purely a storage-representation detail, zero effect on the read-back
  // schema or row content. ---

  test("feature surface: compression codec options don't affect the read-back schema or data") {
    val p = scratchDir.resolve("compression_feature").toString
    df().write.mode("overwrite").format("avro").option("compression", "deflate").save(p)
    val readBack = spark.read.format("avro").load(p)
    assert(readBack.schema.fieldNames.toSeq == Seq("id", "value"))
    assert(readBack.collect().length == 2)
  }

  // --- Feature surface: ignoreExtension (Avro-specific: by default every
  // file in a directory is read regardless of its extension; set to false,
  // only .avro-suffixed files are). Confirmed for real, not assumed: a
  // non-.avro-named copy of a real Avro file is included by default and
  // excluded when ignoreExtension=false. ---

  test("feature surface: ignoreExtension controls whether a non-.avro-named file is read") {
    val dir = scratchDir.resolve("ignore_extension_feature")
    Files.createDirectories(dir)
    // repartition(1) so the write produces exactly one .avro part file
    // carrying both rows - otherwise local[*] may split the 2 rows across
    // multiple part files, making the row counts below unpredictable.
    df().repartition(1).write.mode("overwrite").format("avro").save(dir.toString)
    val avroFile = Files.list(dir).filter(_.toString.endsWith(".avro")).findFirst().get()
    val renamed = dir.resolve("renamed_no_extension")
    Files.copy(avroFile, renamed)

    val defaultRead = spark.read.format("avro").load(dir.toString)
    assert(defaultRead.count() == 4, "default ignoreExtension=true must include the renamed file too")

    val strictRead = spark.read.format("avro").option("ignoreExtension", "false").load(dir.toString)
    assert(strictRead.count() == 2, "ignoreExtension=false must exclude the renamed file")
  }

  // --- Regression: Avro's own safe-listed DDL isn't blocked under a
  // contract that would reject anything it actually checked. ---

  test("regression: CREATE TABLE, ANALYZE TABLE, SHOW TABLES for avro tables aren't blocked under an active contract") {
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
      spark.sql("CREATE TABLE IF NOT EXISTS avro_regression_tbl (id INT) USING avro").collect()
      spark.sql("ANALYZE TABLE avro_regression_tbl COMPUTE STATISTICS").collect()
      spark.sql("SHOW TABLES").collect()
    }
  }

  // --- Read: .load(path) and catalog table reference. Confirmed to
  // translate precisely via the existing generic LogicalRelation case,
  // same as every other file-backed format. ---

  test("PASS: a contract's declared input schema is satisfied by a plain avro .load(path) read") {
    val p = scratchDir.resolve("read_load_feature").toString
    df().write.mode("overwrite").format("avro").save(p)
    val outPath = scratchDir.resolve("read_load_feature_out").toString

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: $p
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
      spark.read.format("avro").load(p).write.mode("overwrite").format("avro").save(outPath) // must not throw
    }
  }

  test("PASS: a contract's declared input schema is satisfied by a catalog table reference read") {
    spark.sql("CREATE TABLE IF NOT EXISTS avro_read_catalog_tbl (id BIGINT, value BIGINT) USING avro")
    spark.sql("INSERT INTO avro_read_catalog_tbl VALUES (1, 10)")
    // DESCRIBE FORMATTED reports Location as a full "file:..." URI;
    // locationsMatch only strips that scheme from the *actual* side, so
    // the declared side needs it stripped here too (see the path-less
    // new-table test above for the same convention).
    val tblLocation = spark.sql("DESCRIBE FORMATTED avro_read_catalog_tbl")
      .filter("col_name = 'Location'").collect().head.getString(1).stripPrefix("file:")
    val outPath = scratchDir.resolve("read_catalog_feature_out").toString

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: in
         |    location: $tblLocation
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
      spark.table("avro_read_catalog_tbl").write.mode("overwrite").format("avro").save(outPath) // must not throw
    }
  }
}
