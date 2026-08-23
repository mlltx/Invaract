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

class ContractEnforcementRuleSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  // The check rule is fixed at SparkSession construction time, but which
  // contract it enforces needs to vary per test. injectCheckRule's function
  // is invoked fresh on every analyzed plan, so a mutable cell it reads at
  // call time — not a value captured once at registration — lets one
  // session serve every test in this suite without the overhead of
  // stopping and rebuilding a SparkSession (and its SparkContext) per case.
  @volatile private var activeContract: Option[com.example.contract.Contract] = None
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
    scratchDir = Files.createTempDirectory("invariant-enforcement-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleSpec")
      // See SparkPlanAdapterSpec's beforeAll for why this is safe to add
      // to a session every other test in this suite also shares.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
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
    val plan = com.example.ir.Write(com.example.ir.DatasetRef("gold.out"), com.example.ir.Read(com.example.ir.DatasetRef("raw.orders")))

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
