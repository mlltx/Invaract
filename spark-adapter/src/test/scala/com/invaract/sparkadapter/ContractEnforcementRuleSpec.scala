// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class ContractEnforcementRuleSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  // The check rule is fixed at SparkSession construction time, but which
  // contract it enforces needs to vary per test. injectCheckRule's function
  // is invoked fresh on every analyzed plan, so a mutable cell it reads at
  // call time — not a value captured once at registration — lets one
  // session serve every test in this suite without the overhead of
  // stopping and rebuilding a SparkSession (and its SparkContext) per case.
  @volatile private var activeContract: Option[com.invaract.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()

  // Not read by any enforcement test above — a raw capture of every
  // analyzed plan the session produces, so a test can inspect
  // WriteCommandSupport's translation directly (e.g. its format
  // detection) without going through StructuralVerifier, which only
  // checks format when the contract *also* declares one and both sides
  // are known, and so wouldn't surface a wrong-format bug as a test
  // failure on its own.
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-enforcement-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleSpec")
      // See SparkPlanAdapterSpec's beforeAll for why this is safe to add
      // to a session every other test in this suite also shares.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      // Spark's default (200) is tuned for real clusters; every shuffle
      // (a MERGE's join included) against these few-row local fixtures
      // would otherwise spin up 200 tasks for no benefit - real,
      // measured overhead in a suite this size. Purely a physical-
      // execution parallelism knob, invisible to query results.
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

  private val passingContractYaml =
    """id: enforcement_demo
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: OUTPUT_PATH
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |        - name: doubled
      |          type: long
      |          required: true
      |""".stripMargin

  test("PASS: a write satisfying its contract executes normally, output file created") {
    val outputPath = scratchDir.resolve("pass.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_missing_column.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // Found via the ClickHouse connector pass's Phase 8, but not
  // ClickHouse-specific - reproduces with any connector, since it's a
  // contract/spark-adapter boundary issue, not a translation one. A
  // contract YAML missing its top-level 'outputs:' key used to parse
  // without error (ContractParser.parse never validates on its own) and
  // then crash verifyOrThrow with an unguarded
  // NoSuchElementException("head of empty list") at contract.outputs.head,
  // instead of a clean, actionable rejection - even though
  // ContractValidator already has an "outputs must be non-empty" check
  // that would have caught it. Fixed by validating the contract first.
  test("FAIL: a contract missing 'outputs' is rejected cleanly, not with an unguarded crash") {
    val outputPath = scratchDir.resolve("invalid_contract_no_outputs.parquet").toString
    val yaml =
      s"""id: invalid_contract
         |version: "1.0.0"
         |""".stripMargin // deliberately no 'outputs:' key at all

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(v =>
      v.violationType == ViolationType.InvalidContract && v.message.contains("outputs")))
  }

  // Closes the enforcement half of the gap SparkPlanAdapterSpec's Delta
  // translation test documents: before SparkPlanAdapter recognized
  // SaveIntoDataSourceCommand, a Delta write translated to Unsupported,
  // and ContractEnforcementRule only gates plans that translate to
  // ir.Write — meaning a Delta write passed through completely
  // unverified, silently, contract or no contract. These two tests are
  // the same PASS/FAIL pair as the Parquet tests above, proving real
  // enforcement now applies to a real Delta write end to end, not just
  // that translation produces the right IR shape in isolation.
  test("PASS: a Delta write satisfying its contract executes normally, output written") {
    val outputPath = scratchDir.resolve("pass_delta").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("overwrite").save(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a Delta write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_delta").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.format("delta").mode("overwrite").save(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the Delta write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // Closes the same kind of gap as the Delta pair above, for a third write
  // shape: `.saveAsTable(...)` against a *new* table analyzes to
  // CreateDataSourceTableAsSelectCommand, not InsertIntoHadoopFsRelationCommand
  // or SaveIntoDataSourceCommand — confirmed empirically, see
  // docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable writes" section.
  test("PASS: a .saveAsTable() write satisfying its contract executes normally, output written") {
    val outputPath = scratchDir.resolve("pass_saveAsTable").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.option("path", outputPath).mode("overwrite").saveAsTable("pass_save_as_table_tbl") // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a .saveAsTable() write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_saveAsTable").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.option("path", outputPath).mode("overwrite").saveAsTable("fail_save_as_table_tbl")
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the .saveAsTable() write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // Delta as an INPUT, not an output — a different code path from every
  // Delta test above. verifyOrThrow collects input schemas via its own
  // `plan.collect { case lr: LogicalRelation => ... }`, independent of
  // SparkPlanAdapter.translate; investigated empirically (see
  // docs/SPARK_ADAPTER.md's "Delta Lake reads" section) that this needs no
  // change: Delta's read relation (`DeltaLog$$anon$2`) is an anonymous
  // subclass of Spark's own HadoopFsRelation, not a distinct type, so both
  // this collection and translation already match it as a normal
  // HadoopFsRelation via ordinary subtyping. This PASS/FAIL pair proves
  // that through real enforcement, not just translation in isolation — a
  // contract's declared input schema is genuinely checked against a real
  // Delta read's actual schema, both when it matches and when it doesn't.
  test("PASS: a contract's declared Delta input schema is genuinely checked against a real Delta read") {
    val deltaInputPath = scratchDir.resolve("delta_input_pass").toString
    val outputPath = scratchDir.resolve("pass_delta_input").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(deltaInputPath)

    // required: false throughout - Delta reports every column nullable on
    // read-back regardless of what was written (a real, separate Delta
    // behavior, not something this test is about); nullability itself
    // already has its own dedicated coverage in StructuralVerifierSpec.
    // This test is specifically about field existence/type, checked
    // against a real Delta read's real schema.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: $deltaInputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val df = spark.read.format("delta").load(deltaInputPath).select("id")
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a contract requiring an input field genuinely absent from a real Delta read is rejected") {
    val deltaInputPath = scratchDir.resolve("delta_input_fail").toString
    val outputPath = scratchDir.resolve("fail_delta_input").toString
    // Only id/doubled actually exist in this Delta table.
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(deltaInputPath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: $deltaInputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.read.format("delta").load(deltaInputPath).select("id")
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
  }

  // Closes the last coverage-ledger gap: a streaming source is not a
  // LogicalRelation, so a contract declaring it as a required `input`
  // used to always report MISSING_INPUT even though data was genuinely
  // being read - not a silent-pass risk (a MISSING_INPUT rejection is
  // safe, just wrong), but a real false-positive gap. This PASS/FAIL pair
  // proves it through real enforcement of a real streaming Delta source,
  // not just that the false MISSING_INPUT stops firing.
  test("PASS: a contract declaring a streaming Delta source as its input is genuinely recognized") {
    val sourcePath = scratchDir.resolve("stream_input_pass_source").toString
    val sinkPath = scratchDir.resolve("stream_input_pass_sink").toString
    val checkpointPath = scratchDir.resolve("stream_input_pass_checkpoint").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(sourcePath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |outputs:
         |  - name: out
         |    location: $sinkPath
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
      val streamDf = spark.readStream.format("delta").load(sourcePath)
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .start(sinkPath) // must not throw MISSING_INPUT
      query.awaitTermination()
    }

    assert(Files.exists(java.nio.file.Paths.get(sinkPath)))
  }

  test("FAIL: a contract requiring an input field genuinely absent from a real streaming Delta source is rejected") {
    val sourcePath = scratchDir.resolve("stream_input_fail_source").toString
    val sinkPath = scratchDir.resolve("stream_input_fail_sink").toString
    val checkpointPath = scratchDir.resolve("stream_input_fail_checkpoint").toString
    // Only id/doubled actually exist in this Delta table.
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(sourcePath)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("delta").load(sourcePath).select("id")
      intercept[ContractViolationException] {
        val query = streamDf.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointPath)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start(sinkPath)
        query.awaitTermination()
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(sinkPath)), "the streaming write must be aborted before the query ever starts, not merely reported as failed")
    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
  }

  // Delta's row-level DML - MERGE INTO / UPDATE / DELETE - used to be a
  // real, concrete example of the fail-closed policy itself: MERGE INTO
  // analyzes to org.apache.spark.sql.delta.commands.MergeIntoCommand,
  // previously neither a recognized write nor on the known-safe list, so
  // it was rejected outright (safely, but unverified). Now
  // WriteCommandSupport's deltaRowLevelDml case recognizes all three
  // Delta-internal DML commands by reflection, checking the operation's
  // *target* against the contract's declared output location and current
  // schema - not the row-level merge/update/delete logic itself, which
  // there is no contract vocabulary to check yet (see that case's own doc
  // comment, and ROADMAP.md's "Full semantic DML verification" item).
  // This PASS/FAIL trio proves that structural check through real
  // enforcement: a satisfying MERGE actually executes (rows genuinely
  // merged, not just "didn't throw"), a schema-violating one is aborted
  // before touching the table, and - proving the "source is a contract
  // input" claim through enforcement, not just code reading - a MERGE
  // whose *source* doesn't satisfy a declared input schema is also
  // aborted, with no special-casing needed for that in this case at all
  // (ContractEnforcementRule's input-schema collection already walks the
  // whole analyzed plan, target and source alike).
  test("PASS: a MERGE INTO satisfying its contract's declared output executes normally, rows genuinely merged") {
    val tablePath = scratchDir.resolve("merge_pass_target").toString
    val tableName = "merge_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false throughout - Delta reports every column nullable on
    // read-back regardless of what was written (see the existing Delta
    // input-read PASS test above for the same, already-documented
    // behavior) - this case's outputSchema comes from the *target's*
    // read-back schema (target.schema), not a freshly-written query's
    // pre-write schema the way every other write shape's does, so it hits
    // this quirk where those don't.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("FAIL: a MERGE INTO whose target violates its contract's declared output schema is aborted before touching the table") {
    val tablePath = scratchDir.resolve("merge_fail_target").toString
    val tableName = "merge_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  test("FAIL: a MERGE INTO whose source violates a contract's declared input schema is aborted before touching the table") {
    val tablePath = scratchDir.resolve("merge_fail_input_target").toString
    val tableName = "merge_fail_input_tbl"
    val sourcePath = scratchDir.resolve("merge_fail_input_source").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    // A real file-backed source, not a temp view over an in-memory
    // DataFrame - a temp view resolves to whatever underlying plan it
    // wraps (here, not a LogicalRelation at all), so it would never be
    // recognized as a read to begin with, reporting MISSING_INPUT (the
    // declared input was never read) rather than the MISSING_INPUT_FIELD
    // this test is actually about (recognized as read, but missing a
    // required field) - confirmed the hard way by a real test failure.
    spark.createDataFrame(Seq((99L, 198L))).toDF("id", "doubled").write.mode("overwrite").parquet(sourcePath)
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |inputs:
         |  - name: merge_source
         |    location: $sourcePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING parquet.`${sourcePath.replace('\\', '/')}` s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_name")),
      s"expected a MISSING_INPUT_FIELD violation naming 'customer_name', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  // A real bug, found by writing this test rather than assumed away: a
  // contract requiring a field that a schema-evolving MERGE is about to
  // add would previously be rejected with MISSING_OUTPUT_FIELD, even
  // though the merge would have satisfied it - because outputSchema came
  // from target.schema at analysis time (pre-merge), confirmed empirically
  // to not yet include columns schema evolution is about to add.
  // deltaRowLevelDml now detects MergeIntoCommand.schemaEvolutionEnabled()
  // and unions in the source's new fields as a best-effort approximation.
  test("PASS: a MERGE INTO with schema evolution enabled, satisfying a contract requiring the newly-added column") {
    val tablePath = scratchDir.resolve("merge_schema_evo_target").toString
    val tableName = "merge_schema_evo_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
         |          required: true
         |""".stripMargin

    // The session (and its SQL conf) is shared across this whole spec, so
    // this must not leak "enabled = true" to later tests even if the MERGE
    // itself throws unexpectedly.
    withContract(yaml) {
      spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
      try {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled, 'new' as extra_col) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect() // must not throw - extra_col is about to be added by evolution
      } finally {
        spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
      }
    }

    assert(spark.table(tableName).schema.fieldNames.contains("extra_col"), "the merge must actually have evolved the schema")
    assert(spark.table(tableName).count() == 6)
  }

  // Confirmed empirically (MergeNoEvoExtraFieldProbeSpec, since deleted):
  // with autoMerge disabled, a MERGE's source can carry a column the target
  // doesn't have - INSERT * silently drops it, the commit succeeds, and the
  // table's schema never gains it. So when schemaEvolutionEnabled() is
  // false, outputSchema must come from target.schema alone; unioning in the
  // source's extra fields regardless (as if evolution were always active)
  // would report a column as written that never actually was. This is
  // exactly the case rejectUndeclaredFields is designed to catch, so it's
  // used here as the tripwire: real code passes (extra_col was never
  // written, contract doesn't mention it); a build that ignored
  // schemaEvolutionEnabled()'s actual value would wrongly add extra_col to
  // outputSchema and abort this write.
  test("PASS: a MERGE INTO without schema evolution enabled ignores the source's extra column, not just the target's") {
    val tablePath = scratchDir.resolve("merge_no_evo_extra_target").toString
    val tableName = "merge_no_evo_extra_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml, VerificationOptions(rejectUndeclaredFields = true)) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled, 'new' as extra_col) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw - extra_col is silently dropped, never evolution-added
    }

    assert(!spark.table(tableName).schema.fieldNames.contains("extra_col"), "extra_col must not actually have been written")
    assert(spark.table(tableName).count() == 6)
  }

  // A real bug, the same class as the MERGE schema-evolution one above,
  // found the same way (real probes, not assumed): Delta generated columns
  // (GENERATED ALWAYS AS (...)) are computed by Delta itself at commit
  // time, never supplied by the writer, so AppendData's outputSchema
  // (previously always cmd.query.schema) never included them - a contract
  // requiring a generated column would be wrongly MISSING_OUTPUT_FIELD-
  // rejected for an append that would actually satisfy it once Delta
  // computed the column. Confirmed empirically that this can't be detected
  // from any DataFrame-facing schema (read-back, catalog table, or the
  // DSv2 Table handle's own .schema()) - only Delta's internal
  // Snapshot.schema() carries the delta.generationExpression metadata
  // GeneratedColumn.isGeneratedColumn actually checks -
  // outputSchemaWithGeneratedColumns/deltaGeneratedFields now read that
  // reflectively and union in the target's generated-only columns.
  test("PASS: appending to a Delta table with a generated column, satisfying a contract requiring it") {
    val tablePath = scratchDir.resolve("gen_col_target").toString
    val tableName = "gen_col_append_tbl"
    DeltaTable.create(spark)
      .tableName(tableName)
      .location(tablePath)
      .addColumn("id", org.apache.spark.sql.types.LongType)
      .addColumn("event_time", TimestampType)
      .addColumn(
        DeltaTable.columnBuilder(spark, "event_date")
          .dataType(DateType)
          .generatedAlwaysAs("CAST(event_time AS DATE)")
          .build()
      )
      .execute()

    // id/event_time: required: false - Delta reports every column nullable
    // on read-back (see the existing Delta input-read PASS test for the
    // same documented quirk). event_date: deliberately required: true -
    // this is the field under test, never supplied by this write's own
    // DataFrame (id, event_time only); required: false here would make
    // this test pass identically whether or not the generated-column fix
    // exists (a missing non-required field isn't flagged at all - see
    // "an absent field is only flagged when the contract marks it
    // required" in StructuralVerifierSpec), silently proving nothing.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: event_time
         |          type: timestamp
         |          required: false
         |        - name: event_date
         |          type: date
         |          required: true
         |          nullable: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.createDataFrame(Seq((1L, java.sql.Timestamp.valueOf("2024-01-01 00:00:00"))))
        .toDF("id", "event_time")
      df.writeTo(tableName).append() // must not throw - event_date is about to be Delta-computed
    }

    val written = spark.table(tableName).collect()
    assert(written.length == 1)
    assert(written.head.getAs[java.sql.Date]("event_date") != null, "event_date must actually have been Delta-computed")
  }

  // Direct-inspection companion to the PASS test above, same pattern as
  // the path-based DML test elsewhere in this file: an AppendData against
  // a Delta table with NO generated columns must resolve outputSchema
  // via the ordinary query.schema path, with no diagnostic attached -
  // proving outputSchemaWithGeneratedColumns's "nothing found" branch
  // stays silent, not just that the schema value happens to come out the
  // same either way.
  test("WriteCommandSupport reports no diagnostic for AppendData into a Delta table with no generated columns") {
    val tablePath = scratchDir.resolve("no_gen_col_target").toString
    val tableName = "no_gen_col_append_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    capturedPlans.clear()

    spark.range(5, 6).withColumn("doubled", col("id") * 2).writeTo(tableName).append()

    val append = capturedPlans.collectFirst { case p: org.apache.spark.sql.catalyst.plans.logical.AppendData => p }
      .getOrElse(fail("no AppendData plan observed"))
    val info = WriteCommandSupport.combined.lift(append).getOrElse(fail("AppendData should be recognized"))
    assert(info.outputSchema.fieldNames.toSet == Set("id", "doubled"))
    assert(info.diagnostic.isEmpty, "a table with no generated columns must not get a generated-columns diagnostic")
  }

  test("PASS: an UPDATE satisfying its contract's declared output executes normally") {
    val tablePath = scratchDir.resolve("update_pass_target").toString
    val tableName = "update_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false - see the MERGE PASS test above for why (Delta
    // reports every column nullable on read-back, and this case's
    // outputSchema comes from the target's read-back schema).
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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

    assert(spark.table(tableName).count() == 5, "the UPDATE must actually have run against all 5 original rows")
  }

  test("PASS: a DELETE satisfying its contract's declared output executes normally") {
    val tablePath = scratchDir.resolve("delete_pass_target").toString
    val tableName = "delete_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    // required: false - see the MERGE PASS test above for why.
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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

  // The four tests below lock in findings from a real probing pass over
  // Delta features not otherwise exercised by this file (throwaway probes,
  // since deleted, not assumed from documentation): each is confirmed
  // transparent to Invaract - a real write against a table with the
  // feature enabled is recognized exactly the same way as one without it,
  // no special-casing needed in WriteCommandSupport. Unlike schema
  // evolution and generated columns above, none of these needed a fix.

  test("PASS: a DELETE against a table with deletion vectors enabled executes normally") {
    val tablePath = scratchDir.resolve("dv_target").toString
    val tableName = "dv_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id <= 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  test("PASS: writes and DML against a table with column mapping mode 'name' execute normally") {
    val tablePath = scratchDir.resolve("colmap_target").toString
    val tableName = "colmap_pass_tbl"
    spark.sql(
      s"""CREATE TABLE $tableName (id LONG, doubled LONG) USING delta
         |LOCATION '${tablePath.replace('\\', '/')}'
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.minReaderVersion' = '2',
         |  'delta.minWriterVersion' = '5'
         |)
         |""".stripMargin)
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("append").saveAsTable(tableName)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id = 5").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6)
  }

  test("PASS: appending to a table with liquid clustering (CLUSTER BY) executes normally") {
    val tablePath = scratchDir.resolve("cluster_target").toString
    val tableName = "cluster_pass_tbl"
    spark.sql(
      s"""CREATE TABLE $tableName (id LONG, doubled LONG) USING delta
         |CLUSTER BY (id)
         |LOCATION '${tablePath.replace('\\', '/')}'
         |""".stripMargin)

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
    }

    assert(spark.table(tableName).count() == 5)
  }

  // CHECK constraints are enforced independently by Delta itself, at
  // commit time - Invaract's structural checks and Delta's own constraint
  // enforcement operate orthogonally, with no interaction/gap: a violating
  // write is recognized by Invaract identically to a satisfying one (no
  // diagnostic, no violation - Invaract has no vocabulary for row-level
  // constraints, only schema/location/format/save-mode), but Delta itself
  // then rejects it before commit. Confirmed empirically, not assumed.
  test("PASS: a write satisfying a Delta CHECK constraint executes normally; a violating one is rejected by Delta itself, not Invaract") {
    val tablePath = scratchDir.resolve("check_target").toString
    val tableName = "check_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    spark.sql(s"ALTER TABLE $tableName ADD CONSTRAINT id_positive CHECK (id >= 0)")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
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
      val satisfying = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      satisfying.write.format("delta").mode("append").saveAsTable(tableName) // must not throw - Invaract passes, constraint satisfied

      val violating = spark.createDataFrame(Seq((-2L, -4L))).toDF("id", "doubled")
      intercept[org.apache.spark.sql.delta.schema.DeltaInvariantViolationException] {
        violating.write.format("delta").mode("append").saveAsTable(tableName)
        // Invaract itself raises nothing here (no ContractViolationException) - the row that
        // gets rejected is rejected by Delta's own commit-time constraint enforcement, not by
        // Invaract, which has no rule vocabulary for a CHECK constraint's condition.
      }
    }

    assert(spark.table(tableName).count() == 6, "only the satisfying row (plus the original 5) was ever committed")
  }

  // Every DML PASS/FAIL pair above targets a catalog table, where
  // catalogTable is always populated - real, but not the only shape.
  // `UPDATE delta.\`path\`` operates directly on a path with no catalog
  // entry at all (confirmed empirically: catalogTable is None, not just
  // missing a location), exercising deltaRowLevelDml's fallback branch -
  // no active contract needed, direct inspection instead, the same
  // pattern as the streaming format-detection test above.
  test("WriteCommandSupport falls back to the target plan's toString for a path-based DML op with no catalog table") {
    val tablePath = scratchDir.resolve("path_dml_target").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    capturedPlans.clear()

    spark.sql(s"UPDATE delta.`${tablePath.replace('\\', '/')}` SET doubled = doubled + 1 WHERE id > 2").collect()

    val upd = capturedPlans.collectFirst { case p if p.getClass.getSimpleName == "UpdateCommand" => p }
      .getOrElse(fail("no UpdateCommand plan observed"))
    val info = WriteCommandSupport.combined.lift(upd).getOrElse(fail("path-based UpdateCommand should still be recognized"))
    assert(info.format.contains("delta"))
    assert(info.diagnostic.isDefined, "no catalog table at all should report a fallback diagnostic, not resolve a clean location silently")
  }

  // RuleVerifier: the three DML rule types (com.invaract.contract.RuleType)
  // checked against RowMutationSupport's extraction, per PASS/FAIL pair -
  // exercised against real Delta MERGE/UPDATE/DELETE, the same "must
  // actually execute, or must be aborted before touching the table"
  // discipline as every other DML test in this file.

  test("PASS: a MERGE INTO satisfying its contract's merge_condition rule executes normally") {
    val tablePath = scratchDir.resolve("rule_merge_pass_target").toString
    val tableName = "rule_merge_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as id, 198L as doubled) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("FAIL: a MERGE INTO whose ON condition doesn't match its contract's merge_condition rule is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_merge_fail_target").toString
    val tableName = "rule_merge_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id, region]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleMergeConditionViolation && v.message.contains("region")),
      s"expected a RULE_MERGE_CONDITION_VIOLATION naming 'region', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  // A real regression test for the predicate-logic upgrade: before it, a
  // declared column that was merely *referenced* anywhere in the ON
  // condition satisfied merge_condition, even via a range check that
  // never actually matches target against source on it. This MERGE's
  // condition references 'region' (in a `>` comparison) without an
  // equality pairing for it at all - this must now be rejected, where it
  // would previously have wrongly passed.
  test("FAIL: a MERGE INTO whose ON condition only range-checks a declared column, never equality-matching it, is aborted") {
    val tablePath = scratchDir.resolve("rule_merge_range_check_target").toString
    val tableName = "rule_merge_range_check_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).withColumn("region", lit("us")).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |        - name: region
         |          type: string
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id, region]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(
          s"""MERGE INTO $tableName t
             |USING (SELECT 99L as id, 198L as doubled, 'us' as region) s
             |ON t.id = s.id AND t.region > 'a'
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin).collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleMergeConditionViolation && v.message.contains("region")),
      s"expected a RULE_MERGE_CONDITION_VIOLATION naming 'region', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the MERGE must be aborted before touching the table, not merely reported as failed")
  }

  test("PASS: a MERGE INTO satisfying merge_condition via a differently-named source column executes normally") {
    val tablePath = scratchDir.resolve("rule_merge_crossname_target").toString
    val tableName = "rule_merge_crossname_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: merge_condition
         |    columns: [id]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(
        s"""MERGE INTO $tableName t
           |USING (SELECT 99L as source_id, 198L as doubled) s
           |ON t.id = s.source_id
           |WHEN NOT MATCHED THEN INSERT (id, doubled) VALUES (s.source_id, s.doubled)
           |""".stripMargin).collect() // must not throw
    }

    assert(spark.table(tableName).count() == 6, "the MERGE must actually have run: 5 original rows + 1 inserted")
  }

  test("PASS: a DELETE with a filtering predicate satisfies its contract's forbid_unconditional_delete rule") {
    val tablePath = scratchDir.resolve("rule_delete_pass_target").toString
    val tableName = "rule_delete_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: forbid_unconditional_delete
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"DELETE FROM $tableName WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the DELETE must actually have run, leaving only id <= 2")
  }

  test("FAIL: an unconditional DELETE violates its contract's forbid_unconditional_delete rule and is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_delete_fail_target").toString
    val tableName = "rule_delete_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: forbid_unconditional_delete
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"DELETE FROM $tableName").collect()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.RuleUnconditionalDelete))
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the DELETE must be aborted before touching the table, not merely reported as failed")
  }

  test("PASS: an UPDATE assigning only allowed columns satisfies its contract's allowed_update_columns rule") {
    val tablePath = scratchDir.resolve("rule_update_pass_target").toString
    val tableName = "rule_update_pass_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: allowed_update_columns
         |    columns: [doubled]
         |""".stripMargin

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET doubled = doubled + 1 WHERE id > 2").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 5, "the UPDATE must actually have run against all 5 original rows")
  }

  test("FAIL: an UPDATE assigning a disallowed column violates its contract's allowed_update_columns rule and is aborted before touching the table") {
    val tablePath = scratchDir.resolve("rule_update_fail_target").toString
    val tableName = "rule_update_fail_tbl"
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $tablePath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: false
         |        - name: doubled
         |          type: long
         |          required: false
         |rules:
         |  - type: allowed_update_columns
         |    columns: [doubled]
         |""".stripMargin

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"UPDATE $tableName SET id = id + 100 WHERE id > 2").collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.RuleDisallowedUpdateColumn && v.message.contains("id")),
      s"expected a RULE_DISALLOWED_UPDATE_COLUMN naming 'id', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the UPDATE must be aborted before touching the table, not merely reported as failed")
  }

  // Closes the "operation surface" gaps docs/ADDING_A_SPARK_CONNECTOR.md's
  // coverage ledger flagged: .format("delta").saveAsTable() on a NEW
  // table, .saveAsTable()/.insertInto() appending to an EXISTING table,
  // and DataFrameWriterV2 (.writeTo()) all analyze to V2 write commands
  // (ReplaceTableAsSelect/AppendData/OverwriteByExpression) - confirmed
  // empirically via injectCheckRule, not assumed. These used to only
  // fail closed (safely rejected, but never actually checked against a
  // contract) - now WriteCommandSupport recognizes all three, so they're
  // genuinely verified, the same as every other write shape. Location
  // differs between the two, confirmed empirically: ReplaceTableAsSelect
  // (a table that doesn't exist yet) has no physical path to resolve, so
  // it uses the qualified catalog identifier ("spark_catalog.default.
  // <table>"); AppendData/OverwriteByExpression target an *existing*
  // table, whose resolved DataSourceV2 Table reports a physical
  // warehouse path via properties() - the same "prefer a real path,
  // fall back to the identifier" asymmetry createDataSourceTableAsSelect
  // already has for V1 new-table writes.
  test("PASS: .format(\"delta\").saveAsTable() on a new table, satisfying its contract, executes normally") {
    val tableName = "pass_rtas_new_tbl"
    val expectedLocation = s"spark_catalog.default.$tableName"
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    format: delta
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("overwrite").saveAsTable(tableName) // must not throw
    }

    assert(spark.catalog.tableExists(tableName))
  }

  test("FAIL: .format(\"delta\").saveAsTable() on a new table, violating its contract, is aborted before any table is created") {
    val tableName = "fail_rtas_new_tbl"
    val expectedLocation = s"spark_catalog.default.$tableName"
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.format("delta").mode("overwrite").saveAsTable(tableName)
      }
    }

    assert(!spark.catalog.tableExists(tableName), "the new table must never be created, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  test("PASS: appending to an existing Delta table via .saveAsTable()/.insertInto()/.writeTo(), satisfying its contract, executes normally") {
    val tableName = "pass_append_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").saveAsTable(tableName)

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
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)
      df.write.format("delta").mode("append").saveAsTable(tableName) // must not throw
      df.write.insertInto(tableName) // must not throw
      df.writeTo(tableName).append() // must not throw
      df.writeTo(tableName).overwrite(org.apache.spark.sql.functions.lit(true)) // must not throw
    }

    assert(spark.table(tableName).count() > 0)
  }

  test("FAIL: appending to an existing Delta table via .saveAsTable()/.insertInto()/.writeTo(), violating its contract, is aborted before anything is written") {
    val tableName = "fail_append_tbl"
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.format("delta").saveAsTable(tableName)
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

    withContract(yaml) {
      val df = spark.range(5, 6).withColumn("doubled", col("id") * 2)

      val exSaveAsTable = intercept[ContractViolationException](df.write.format("delta").mode("append").saveAsTable(tableName))
      assert(exSaveAsTable.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exInsertInto = intercept[ContractViolationException](df.write.insertInto(tableName))
      assert(exInsertInto.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exWriteToAppend = intercept[ContractViolationException](df.writeTo(tableName).append())
      assert(exWriteToAppend.result.violations.exists(_.violationType == ViolationType.MissingOutputField))

      val exWriteToOverwrite = intercept[ContractViolationException](df.writeTo(tableName).overwrite(org.apache.spark.sql.functions.lit(true)))
      assert(exWriteToOverwrite.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
    }

    val afterRows = spark.table(tableName).collect().toSet
    assert(beforeRows == afterRows, "none of the rejected attempts may have actually written anything")
  }

  // Closes the most significant coverage-ledger gap found investigating
  // Delta support: a streaming write's top-level plan (WriteToStream)
  // isn't Command-shaped, so it was invisible to the fail-closed policy
  // entirely - not "fails closed, unverified" like the V2 write commands
  // above, but genuinely unenforced (confirmed via a real probe: zero of
  // the plans injectCheckRule saw during a real streaming Delta write were
  // Command-shaped, and via javap confirming WriteToStream doesn't
  // implement Command - see docs/SPARK_ADAPTER.md's "Streaming writes"
  // section). Recognizing WriteToStream in WriteCommandSupport - the same
  // registry every other write shape goes through, rather than a
  // special-cased check here - means it's genuinely verified, not merely
  // allowed or blocked wholesale. This PASS/FAIL pair proves that through
  // real enforcement of a real streaming Delta write, not just translation
  // in isolation.
  test("PASS: a streaming Delta write satisfying its contract starts and writes normally") {
    val sinkPath = scratchDir.resolve("pass_stream").toString
    val checkpointPath = scratchDir.resolve("pass_stream_checkpoint").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .start(sinkPath) // must not throw
      query.awaitTermination()
    }

    assert(Files.exists(java.nio.file.Paths.get(sinkPath)))
  }

  test("FAIL: a streaming Delta write violating its contract is aborted before the query starts, nothing written") {
    val sinkPath = scratchDir.resolve("fail_stream").toString
    val checkpointPath = scratchDir.resolve("fail_stream_checkpoint").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $sinkPath
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      intercept[ContractViolationException] {
        streamDf.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointPath)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start(sinkPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(sinkPath)), "the streaming write must be rejected before the query ever starts, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  // A second, distinct WriteToStream shape: `.toTable(...)` resolves a
  // real `catalogTable` (confirmed empirically - unlike the path-based
  // `.start(path)` pair above, where it's None and the sink's own
  // reflectively-read `path()` is used instead). Exercises the other half
  // of `streamSinkLocationAndFormat`'s branching, the same way
  // `createDataSourceTableAsSelect`'s catalog-table path is exercised
  // separately from `saveIntoDataSource`'s options-map path above.
  test("PASS: a streaming Delta .toTable() write satisfying its contract starts and writes normally") {
    val tableName = "pass_stream_to_table_tbl"
    val checkpointPath = scratchDir.resolve("pass_stream_to_table_checkpoint").toString
    val expectedLocation = scratchDir.resolve("warehouse").resolve(tableName).toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $expectedLocation
         |    schema:
         |      fields:
         |        - name: timestamp
         |          type: timestamp
         |          required: false
         |        - name: value
         |          type: long
         |          required: false
         |""".stripMargin

    withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      val query = streamDf.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .toTable(tableName) // must not throw
      query.awaitTermination()
    }

    assert(spark.catalog.tableExists(tableName))
  }

  // The PASS/FAIL pair above proves end-to-end enforcement, but never
  // directly inspects the format WriteCommandSupport detected for a
  // streaming Delta write - StructuralVerifier only compares format when
  // the contract also declares one and both sides are known, so a bug in
  // `streamSinkFormatOf` (returning the wrong format, or "delta" for a
  // non-Delta sink) wouldn't necessarily surface as a PASS/FAIL test
  // failure on its own. Inspects WriteCommandInfo directly instead, no
  // active contract needed - the write completes normally either way.
  test("WriteCommandSupport detects format \"delta\" for a real streaming Delta write") {
    val sinkPath = scratchDir.resolve("format_detection_stream").toString
    val checkpointPath = scratchDir.resolve("format_detection_stream_checkpoint").toString
    capturedPlans.clear()

    val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    val query = streamDf.writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
      .start(sinkPath)
    query.awaitTermination()

    val ws = capturedPlans.collectFirst { case w: org.apache.spark.sql.catalyst.streaming.WriteToStream => w }
      .getOrElse(fail("no WriteToStream plan observed"))
    val info = WriteCommandSupport.combined.lift(ws).getOrElse(fail("WriteToStream should be recognized by WriteCommandSupport"))
    assert(info.format.contains("delta"), s"expected format 'delta' for a real Delta streaming sink, got ${info.format}")
    assert(info.location.contains("format_detection_stream"))
  }

  // Before WriteCommandSupport recognized WriteToStream, this test
  // documented the accidental consequence of streaming being entirely
  // invisible to enforcement: starting ANY streaming query while a
  // mismatched contract was active never threw, because WriteToStream fell
  // into the untranslated-and-not-Command-shaped silent no-op. Now that a
  // streaming write is a real, recognized write, it's checked against
  // whatever contract is active the same way every batch write always has
  // been - `forContract`'s own doc says "verifies any write this session
  // performs against contract", not "any write whose location happens to
  // match". A streaming write to an unrelated location under an active,
  // unrelated contract is therefore correctly rejected
  // (OUTPUT_LOCATION_MISMATCH), consistent with batch writes, not silently
  // allowed through the way it used to be. Uses the "memory" sink
  // (a genuine V2 Table, unlike Delta's legacy-wrapped one - confirmed via
  // javap) to exercise the other branch of location resolution:
  // `sink.name()` succeeding directly, no reflection needed.
  test("a streaming write to a location unrelated to the active contract is rejected, consistent with batch writes") {
    val yaml =
      """id: enforcement_demo
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: some/other/contract/output
        |    schema:
        |      fields:
        |        - name: id
        |          type: long
        |          required: true
        |""".stripMargin

    val ex = withContract(yaml) {
      val streamDf = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
      intercept[ContractViolationException] {
        val query = streamDf.writeStream
          .format("memory")
          .queryName("unrelated_stream_q")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .start()
        query.awaitTermination()
      }
    }

    assert(ex.result.violations.exists(_.violationType == ViolationType.OutputLocationMismatch))
  }

  // Regression guard for the fail-closed policy's biggest risk: it must
  // NOT reject ordinary catalog/DDL operations just because they're
  // Command-shaped and untranslated — only FailClosedCommands' known-safe
  // list stands between "legitimate DDL" and "rejected as unverifiable",
  // so this proves that list actually works for real commands, not just
  // that it type-checks.
  test("non-data DDL commands (CREATE TABLE, ANALYZE TABLE) are never blocked by the fail-closed policy") {
    val yaml =
      """id: would_always_fail
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: nonexistent/location
        |    schema:
        |      fields:
        |        - name: impossible_field
        |          type: string
        |          required: true
        |""".stripMargin

    withContract(yaml) {
      spark.sql("CREATE TABLE IF NOT EXISTS ddl_regression_tbl (id INT) USING parquet").collect()
      spark.sql("ANALYZE TABLE ddl_regression_tbl COMPUTE STATISTICS").collect()
      spark.sql("SHOW TABLES").collect()
    }
    // no exception means every DDL/administrative statement completed
    succeed
  }

  test("the abort exception explains what/what/why/how — all four, not just a bare violation code") {
    val outputPath = scratchDir.resolve("fail_explain.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "2.1.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
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
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    val message = ex.getMessage

    // What the contract expected
    assert(message.contains("What the contract expects"))
    assert(message.contains("customer_name"))
    assert(message.contains("enforcement_demo@2.1.0"))

    // What the plan contains
    assert(message.contains("What the plan contains"))
    assert(message.contains("Write("))

    // Why it violates
    assert(message.contains("Why it violates the contract"))
    assert(message.contains("MISSING_OUTPUT_FIELD"))

    // How to correct it
    assert(message.contains("How to correct it"))
    assert(message.contains("Add a 'customer_name' column"))
  }

  test("the same violation produces a byte-identical explanation every time (deterministic)") {
    val outputPath = scratchDir.resolve("fail_deterministic.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: missing_a
         |          type: string
         |          required: true
         |        - name: missing_b
         |          type: string
         |          required: true
         |""".stripMargin

    def attempt(): String = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }.getMessage
    }

    val first = attempt()
    val second = attempt()
    val third = attempt()

    assert(first == second)
    assert(second == third)
  }

  test("non-write queries (reads, counts) never trigger verification, even against a contract they'd violate") {
    // A contract that would fail immediately if applied to *any* plan --
    // proves the check rule really does gate on "is this a Write", not on
    // "is a contract active".
    val yaml =
      """id: would_always_fail
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: nonexistent/location
        |    schema:
        |      fields:
        |        - name: impossible_field
        |          type: string
        |          required: true
        |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.count() // triggers analysis of Range/Project plans, not a Write
      df.collect()
    }
    // no exception means both actions completed successfully
    succeed
  }

  test("VerificationOptions thread through to enforcement: rejectUndeclaredFields turns an extra column into an abort") {
    val outputPath = scratchDir.resolve("fail_undeclared.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2) // 'doubled' is undeclared
      df.write.mode("overwrite").parquet(outputPath) // permissive default: passes
    }
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))

    val strictOutputPath = scratchDir.resolve("fail_undeclared_strict.parquet").toString
    val strictYaml = yaml.replace(outputPath, strictOutputPath)
    withContract(strictYaml, VerificationOptions(rejectUndeclaredFields = true)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(strictOutputPath)
      }
    }
    assert(!Files.exists(java.nio.file.Paths.get(strictOutputPath)))
  }

  test("forContract builds a usable check-rule function directly") {
    val outputPath = scratchDir.resolve("for_contract.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val contract = parseContract(yaml)
    val rule = ContractEnforcementRule.forContract(contract)

    // Exercise the built function directly against a translated plan,
    // independent of SparkSession wiring: same shape injectCheckRule wants.
    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    rule(spark)(df.queryExecution.analyzed) // no write command in this plan -> no-op, must not throw
  }

  // Added while raising the module's mutation-testing score (see
  // ROADMAP.md Phase 1c / CLAUDE.md): explain() is private[sparkadapter],
  // so it can be exercised directly with a synthetic result — no need to
  // provoke a real Spark abort just to check its text formatting.
  test("explain pluralizes the violation count and marks optional fields distinctly") {
    val contractYaml =
      """id: explain_demo
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |        - name: note
        |          type: string
        |          required: false
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |""".stripMargin
    val contract = parseContract(contractYaml)
    val plan = com.invaract.ir.Write(com.invaract.ir.DatasetRef("gold.out"), com.invaract.ir.Read(com.invaract.ir.DatasetRef("raw.orders")))

    val oneViolation = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(Violation(ViolationType.MissingOutputField, "msg", "remediation", column = Some("id")))
    )
    val oneText = ContractEnforcementRule.explain(contract, plan, oneViolation)
    assert(oneText.contains("(1 violation):"), oneText)
    assert(!oneText.contains("(1 violations):"), oneText)

    val twoViolations = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(
        Violation(ViolationType.MissingOutputField, "msg1", "remediation1", column = Some("id")),
        Violation(ViolationType.MissingInputField, "msg2", "remediation2", column = Some("note"))
      )
    )
    val twoText = ContractEnforcementRule.explain(contract, plan, twoViolations)
    assert(twoText.contains("(2 violations):"), twoText)

    // "note" is declared optional (required: false); "id" is required.
    // Only the optional field's description should carry "(optional)".
    assert(oneText.contains("id: integer") && !oneText.contains("id: integer (optional)"), oneText)
    assert(oneText.contains("note: string (optional)"), oneText)
  }
}
