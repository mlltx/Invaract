// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.invaract.verification.ContractViolationException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** OpenLineage on a real Spark application attached with configuration alone (`spark.sql.extensions` and
  * `spark.invaract.*`): one run per application, ended when the application stops, the ordinary way (`spark.stop()`). */
class SparkOpenLineageSpec extends AnyFunSuite {
  private val json = new ObjectMapper()

  private def run(contractOutput: String => String, write: (SparkSession, Path) => Unit): (List[JsonNode], Path) = {
    val dir = Files.createTempDirectory("spark-openlineage")
    val input = dir.resolve("orders").toString
    val output = dir.resolve("report").toString
    val contract = dir.resolve("contract.yaml")
    Files.write(contract,
      s"""id: lineage_demo
         |version: "1.0.0"
         |inputs:
         |  - { name: orders, location: $input, format: parquet }
         |outputs:
         |  - { name: report, location: ${contractOutput(output)}, format: parquet, schema: { fields: [ { name: id, type: long }, { name: doubled, type: long } ] } }
         |""".stripMargin.getBytes(StandardCharsets.UTF_8))
    val events = dir.resolve("lineage.jsonl")
    val config = dir.resolve("openlineage.properties")
    Files.write(config, s"transport=file\npath=$events\nnamespace=spark-test\n".getBytes(StandardCharsets.UTF_8))
    val setup = SparkSession.builder().master("local[1]").appName("setup").config("spark.ui.enabled", "false").getOrCreate()
    try setup.range(3).write.parquet(input) finally setup.stop()
    val spark = SparkSession.builder().master("local[1]").appName("SparkOpenLineageSpec").config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[InvaractSparkSessionExtension].getName)
      .config("spark.invaract.contract", contract.toString)
      .config("spark.invaract.openLineageConfig", config.toString)
      .getOrCreate()
    try write(spark, dir) finally spark.stop()
    // the application's end is delivered by Spark's listener bus, then by the sink's worker: wait for the terminal event
    val deadline = System.nanoTime() + 30L * 1000000000L
    def read() = if (Files.exists(events)) Files.readAllLines(events).asScala.toList.map(json.readTree) else Nil
    while (!read().exists(e => Set("COMPLETE", "FAIL")(e.get("eventType").asText())) && System.nanoTime() < deadline) Thread.sleep(50)
    (read(), dir)
  }

  private def doubled(spark: SparkSession, dir: Path, to: String): Unit =
    spark.read.parquet(dir.resolve("orders").toString).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(to)

  test("an allowed write: START, the write with its input, schema and verdict, then COMPLETE when the application stops") {
    val (events, dir) = run(identity, (spark, dir) => doubled(spark, dir, dir.resolve("report").toString))
    val types = events.map(_.get("eventType").asText())
    assert(types.head == "START" && types.last == "COMPLETE" && types.count(t => t == "COMPLETE" || t == "FAIL") == 1, types)
    assert(events.map(_.path("run").get("runId").asText()).distinct.size == 1)
    assert(events.forall(_.path("job").get("namespace").asText() == "spark-test"))
    val outputs = events.flatMap(_.get("outputs").elements().asScala).filter(_.get("name").asText() == dir.resolve("report").toString)
    assert(outputs.nonEmpty && outputs.forall(_.get("namespace").asText() == "file"))
    val checked = outputs.find(_.path("facets").has("invaract_contract")).get
    assert(checked.path("facets").path("invaract_contract").get("status").asText() == "PASSED")
    assert(checked.path("facets").path("schema").get("fields").elements().asScala.map(_.get("name").asText()).toList == List("id", "doubled"))
    val lineage = checked.path("facets").path("columnLineage").path("fields").path("doubled").get("inputFields").get(0)
    assert(lineage.get("name").asText() == dir.resolve("orders").toString && lineage.get("field").asText() == "id", "Spark traces columns: lineage.columnLevel is partial")
    val inputs = events.flatMap(_.get("inputs").elements().asScala).map(_.get("name").asText())
    assert(inputs.contains(dir.resolve("orders").toString))
    val engine = events.head.path("run").path("facets").get("processing_engine")
    assert(engine.get("name").asText() == "spark" && engine.get("version").asText() == org.apache.spark.SPARK_VERSION)
  }

  test("a refused write: the verdict FAILED, and the run ends FAIL with the reason when the application stops") {
    val (events, _) = run(_ => "/somewhere/else", (spark, dir) =>
      intercept[ContractViolationException](doubled(spark, dir, dir.resolve("report").toString)))
    val types = events.map(_.get("eventType").asText())
    assert(types.head == "START" && types.last == "FAIL", types)
    val failed = events.flatMap(_.get("outputs").elements().asScala).flatMap(o => Option(o.path("facets").get("invaract_contract")))
    assert(failed.exists(_.get("status").asText() == "FAILED"))
    val message = events.flatMap(e => Option(e.path("run").path("facets").get("errorMessage"))).head.get("message").asText()
    assert(message.contains("OUTPUT_LOCATION_MISMATCH"), message)
  }
}
