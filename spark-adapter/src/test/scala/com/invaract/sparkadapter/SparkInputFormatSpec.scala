// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import java.nio.file.{Files, Path}

/** `check.inputFormat` on a real Spark session: the format of each kind of read Spark plans, named as a write to the same
  * dataset is named (`SparkPlanAdapter.readFormatOf`), and a contract's declared input format enforced against it
  * (docs/SPARK_ADAPTER.md, "Input formats"). Hive tables are in `HiveConnectorSpec`, Iceberg tables in `IcebergConnectorSpec`. */
class SparkInputFormatSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-input-format")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkInputFormatSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def at(name: String): String = scratchDir.resolve(name).toString

  private val schema = StructType(Seq(StructField("id", LongType), StructField("value", LongType)))

  /** The formats of every read in `frame`'s analyzed plan, in plan order. */
  private def formatsOf(frame: DataFrame): List[String] =
    frame.queryExecution.analyzed.collect(Function.unlift(SparkPlanAdapter.readFormatOf)).toList

  /** `name` written in `format` (by path), once. */
  private def written(name: String, format: String): String = {
    val path = at(name)
    if (!Files.exists(scratchDir.resolve(name))) df().write.format(format).save(path)
    path
  }

  test("a file read is named by its file format") {
    for (format <- List("parquet", "csv", "json", "orc", "avro")) {
      assert(formatsOf(spark.read.format(format).schema(schema).load(written(s"files_$format", format))) == List(format), format)
    }
    df().select(org.apache.spark.sql.functions.col("id").cast("string")).write.text(at("files_text"))
    assert(formatsOf(spark.read.text(at("files_text"))) == List("text"))
  }

  test("a catalog table is named by its provider, as a write to it is") {
    spark.sql(s"CREATE TABLE parquet_table (id BIGINT, value BIGINT) USING parquet LOCATION '${scratchDir.resolve("parquet_table").toUri}'")
    assert(formatsOf(spark.table("parquet_table")) == List("parquet"))
    spark.sql("CREATE TABLE delta_table (id BIGINT, value BIGINT) USING delta")
    assert(formatsOf(spark.table("delta_table")) == List("delta"))
  }

  test("a Delta table read by its path is delta, though Spark reads its files as Parquet") {
    assert(formatsOf(spark.read.format("delta").load(written("delta_path", "delta"))) == List("delta"))
  }

  test("a JDBC read is jdbc") {
    val url = s"jdbc:h2:mem:invaract_input_format_${System.nanoTime()};DB_CLOSE_DELAY=-1"
    val conn = java.sql.DriverManager.getConnection(url)
    try {
      conn.createStatement().execute("CREATE TABLE orders (id BIGINT, val BIGINT)")
      assert(formatsOf(spark.read.format("jdbc").option("url", url).option("dbtable", "orders").load()) == List("jdbc"))
    } finally conn.close()
  }

  test("a streaming source is named by its data source, whether the job named it or its class") {
    val csv = written("stream_csv", "csv")
    assert(formatsOf(spark.readStream.format("csv").schema(schema).load(csv)) == List("csv"))
    assert(formatsOf(spark.readStream.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").schema(schema).load(csv)) == List("csv"))
    assert(formatsOf(spark.readStream.format("delta").load(written("stream_delta", "delta"))) == List("delta"))
    assert(formatsOf(spark.readStream.format("rate").load()) == List("rate"))
  }

  test("a plan that reads nothing has no read format") {
    assert(formatsOf(spark.range(3).toDF()) == Nil)
    assert(formatsOf(df()) == Nil)
  }

  private def contract(inputFormat: String, input: String, output: String): String =
    s"""id: input_format
       |version: "1.0.0"
       |inputs:
       |  - name: orders
       |    location: $input
       |    format: $inputFormat
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |outputs:
       |  - name: report
       |    location: $output
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |""".stripMargin

  test("an input read in the format its contract declares passes; in another, the write is refused before anything is written") {
    val input = written("enforced_in", "parquet")
    withContract(contract("parquet", input, at("enforced_ok"))) {
      spark.read.parquet(input).write.mode("overwrite").parquet(at("enforced_ok"))
    }
    assert(Files.exists(scratchDir.resolve("enforced_ok")))
    val refused = withContract(contract("csv", input, at("enforced_refused"))) {
      intercept[ContractViolationException](spark.read.parquet(input).write.mode("overwrite").parquet(at("enforced_refused")))
    }
    assert(refused.result.violations.map(_.violationType) == List(ViolationType.InputFormatMismatch))
    assert(refused.result.violations.head.message.contains("contract declares input format 'csv'") &&
      refused.result.violations.head.actual.contains("parquet"))
    assert(!Files.exists(scratchDir.resolve("enforced_refused")))
  }

  test("a dry run's draft declares the format each input was read in") {
    val input = written("draft_in", "json")
    // a CTAS analyzed, not run: the write the draft describes
    val ctas = spark.sessionState.executePlan(
      spark.sessionState.sqlParser.parsePlan(s"CREATE TABLE draft_out USING parquet AS SELECT * FROM json.`$input`")
    ).analyzed
    val inputs = ContractEnforcementRule.inferOutcome(ctas) match {
      case Some(inferred: ContractEnforcementRule.InferenceOutcome.Inferred) => inferred.contract.inputs
      case other                                    => fail(s"no draft: $other")
    }
    assert(inputs.map(_.format) == List(Some("json")))
  }
}
