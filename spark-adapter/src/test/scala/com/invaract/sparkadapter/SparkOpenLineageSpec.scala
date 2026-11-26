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

  /** A path as a .properties file holds it and as OpenLineage names it: forward slashes. */
  private def slashes(path: Path): String = path.toString.replace('\\', '/')

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
    Files.write(config, s"transport=file\npath=${slashes(events)}\nnamespace=spark-test\n".getBytes(StandardCharsets.UTF_8))
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
    assert(types.count(_ == "START") == 1, "one sink: nothing published twice")
    assert(events.map(_.path("run").get("runId").asText()).distinct.size == 1)
    assert(events.forall(_.path("job").get("namespace").asText() == "spark-test"))
    val outputs = events.flatMap(_.get("outputs").elements().asScala).filter(_.get("name").asText() == slashes(dir.resolve("report")))
    assert(outputs.nonEmpty && outputs.forall(_.get("namespace").asText() == "file"))
    val checked = outputs.find(_.path("facets").has("invaract_contract")).get
    assert(checked.path("facets").path("invaract_contract").get("status").asText() == "PASSED")
    assert(checked.path("facets").path("schema").get("fields").elements().asScala.map(_.get("name").asText()).toList == List("id", "doubled"))
    val lineage = checked.path("facets").path("columnLineage").path("fields").path("doubled").get("inputFields").get(0)
    assert(lineage.get("name").asText() == slashes(dir.resolve("orders")) && lineage.get("field").asText() == "id", "Spark traces columns: lineage.columnLevel is partial")
    val inputs = events.flatMap(_.get("inputs").elements().asScala).map(_.get("name").asText())
    assert(inputs.contains(slashes(dir.resolve("orders"))))
    val engine = events.head.path("run").path("facets").get("processing_engine")
    assert(engine.get("name").asText() == "spark" && engine.get("version").asText() == org.apache.spark.SPARK_VERSION)
  }

  test("a real job's wall time with OpenLineage on stays within noise of it off, against an endpoint answering in 20 ms") {
    import com.sun.net.httpserver.{HttpExchange, HttpServer}
    val received = new java.util.concurrent.atomic.AtomicInteger()
    val server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", (e: HttpExchange) => { e.getRequestBody.readAllBytes(); Thread.sleep(20); received.incrementAndGet(); e.sendResponseHeaders(200, -1); e.close() })
    server.start()
    try {
      val dir = Files.createTempDirectory("spark-openlineage-timing")
      val input = dir.resolve("orders").toString
      val contract = dir.resolve("contract.yaml")
      val outputs = (0 until 20).map(i => s"""  - { name: report$i, location: ${dir.resolve(s"report$i")}, format: parquet }""").mkString("\n")
      Files.write(contract, (s"""id: timing
          |version: "1.0.0"
          |inputs:
          |  - { name: orders, location: $input, format: parquet }
          |outputs:
          |""".stripMargin + outputs + "\n").getBytes(StandardCharsets.UTF_8))
      val setup = SparkSession.builder().master("local[1]").appName("setup").config("spark.ui.enabled", "false").getOrCreate()
      try setup.range(1000).write.parquet(input) finally setup.stop()
      def job(lineage: Boolean): Long = {
        val builder = SparkSession.builder().master("local[2]").appName("timing").config("spark.ui.enabled", "false")
          .config("spark.sql.extensions", classOf[InvaractSparkSessionExtension].getName)
          .config("spark.invaract.contract", contract.toString)
        if (lineage) builder.config("spark.invaract.openLineageUrl", s"http://127.0.0.1:${server.getAddress.getPort}")
        val spark = builder.getOrCreate()
        try {
          val started = System.nanoTime()
          (0 until 20).foreach(i => spark.read.parquet(input).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(dir.resolve(s"report$i").toString))
          (System.nanoTime() - started) / 1000000
        } finally spark.stop()
      }
      job(lineage = false); job(lineage = true) // warm up
      val rounds = (1 to 3).map(_ => (job(lineage = false), job(lineage = true)))
      val (off, on) = (rounds.map(_._1).min, rounds.map(_._2).min)
      info(s"20 writes: $off ms off, $on ms on (${on - off} ms); ${received.get()} events delivered")
      assert(on - off < math.max(2000L, off / 2), "OpenLineage adds no more than noise to a real job")
      assert(received.get() > 0)
    } finally server.stop(0)
  }

  test("a job that installs Invaract in code (forContract) gets OpenLineage from configuration alone, with its own sink too") {
    val dir = Files.createTempDirectory("spark-openlineage-code")
    val output = dir.resolve("report").toString
    val contract = com.invaract.contract.ContractParser.parse(
      s"""id: in_code
         |version: "1.0.0"
         |outputs:
         |  - { name: report, location: $output, format: parquet }
         |""".stripMargin)
    val events = dir.resolve("lineage.jsonl")
    val config = dir.resolve("openlineage.properties")
    Files.write(config, List("transport=file", s"path=${slashes(events)}").mkString("\n").getBytes(StandardCharsets.UTF_8))
    val own = new com.invaract.verification.notification.NotificationSink {
      val seen = new java.util.concurrent.atomic.AtomicInteger()
      def publish(event: com.invaract.verification.notification.NotificationEvent): Unit = seen.incrementAndGet()
    }
    for (withOwn <- List(false, true)) {
      Files.deleteIfExists(events)
      val spark = SparkSession.builder().master("local[1]").appName("in-code").config("spark.ui.enabled", "false")
        .config("spark.invaract.openLineageConfig", config.toString)
        .withExtensions(_.injectCheckRule(s =>
          if (withOwn) ContractEnforcementRule.forContract(contract, com.invaract.verification.VerificationOptions(), own)(s)
          else ContractEnforcementRule.forContract(contract)(s)))
        .getOrCreate()
      try spark.range(3).write.mode("overwrite").parquet(output) finally spark.stop()
      val deadline = System.nanoTime() + 30L * 1000000000L
      def types = if (Files.exists(events)) Files.readAllLines(events).asScala.toList.map(l => json.readTree(l).get("eventType").asText()) else Nil
      while (!types.contains("COMPLETE") && System.nanoTime() < deadline) Thread.sleep(50)
      assert(types.headOption.contains("START") && types.last == "COMPLETE" && types.count(_ == "START") == 1, s"withOwn=$withOwn: $types")
    }
    assert(own.seen.get() > 0, "the job's own sink still gets its events")
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

  /** A job attached by configuration alone, with `extra` settings, writing `doubled` of its input; its events. */
  private def runWith(extra: Map[String, String], contract: Option[String => String]): List[JsonNode] = {
    val dir = Files.createTempDirectory("spark-openlineage-attach")
    val input = dir.resolve("orders").toString
    val output = dir.resolve("report").toString
    val events = dir.resolve("lineage.jsonl")
    val config = dir.resolve("openlineage.properties")
    Files.write(config, s"transport=file\npath=${slashes(events)}\n".getBytes(StandardCharsets.UTF_8))
    val setup = SparkSession.builder().master("local[1]").appName("setup").config("spark.ui.enabled", "false").getOrCreate()
    try setup.range(3).write.parquet(input) finally setup.stop()
    val builder = SparkSession.builder().master("local[1]").appName("attach").config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[InvaractSparkSessionExtension].getName)
      .config("spark.invaract.openLineageConfig", config.toString)
    contract match {
      case Some(yaml) =>
        val file = dir.resolve("contract.yaml")
        Files.write(file, yaml(output).getBytes(StandardCharsets.UTF_8))
        builder.config("spark.invaract.contract", file.toString)
      case None => builder.config("spark.invaract.dryRun", "true")
    }
    val spark = extra.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }.getOrCreate()
    try doubled(spark, dir, output) finally spark.stop()
    val deadline = System.nanoTime() + 30L * 1000000000L
    def read() = if (Files.exists(events)) Files.readAllLines(events).asScala.toList.map(json.readTree) else Nil
    while (!read().exists(e => Set("COMPLETE", "FAIL")(e.get("eventType").asText())) && System.nanoTime() < deadline) Thread.sleep(50)
    read()
  }

  private val outputOnly: String => String = output =>
    s"""id: attach
       |version: "1.0.0"
       |outputs:
       |  - { name: report, location: $output, format: parquet }
       |""".stripMargin

  test("the parent and root runs an orchestrator injects (spark.openlineage.parent*, rootParent*) make the run their child") {
    val parent = "01890b4c-bb6b-7a8c-8f5b-2a6c35dd2a0e"
    val root = "01890b4c-bb6b-7a8c-8f5b-2a6c35dd2a0f"
    val events = runWith(Map(
      "spark.openlineage.parentJobNamespace" -> "airflow", "spark.openlineage.parentJobName" -> "dag.task", "spark.openlineage.parentRunId" -> parent,
      "spark.openlineage.rootParentJobNamespace" -> "airflow", "spark.openlineage.rootParentJobName" -> "dag", "spark.openlineage.rootParentRunId" -> root
    ), Some(outputOnly))
    assert(events.nonEmpty)
    for (e <- events) {
      val facet = e.path("run").path("facets").path("parent")
      assert(facet.path("run").get("runId").asText() == parent && facet.path("job").get("name").asText() == "dag.task", e.get("eventType").asText())
      assert(facet.path("root").path("run").get("runId").asText() == root && facet.path("root").path("job").get("name").asText() == "dag")
    }
    val partial = runWith(Map("spark.openlineage.parentJobNamespace" -> "airflow", "spark.openlineage.parentRunId" -> parent), Some(outputOnly))
    assert(partial.forall(e => !e.path("run").path("facets").has("parent")), "a parent missing its job name is no parent")
  }

  test("a dry run publishes the lineage enforcement would: the write's input, its output's schema and column lineage") {
    val events = runWith(Map.empty, None)
    val types = events.map(_.get("eventType").asText())
    assert(types.head == "START" && types.last == "COMPLETE", types)
    val written = events.find(_.get("outputs").elements().asScala.exists(_.path("facets").has("columnLineage"))).getOrElse(fail(s"no column lineage in $events"))
    val output = written.get("outputs").get(0)
    assert(output.path("facets").path("invaract_contract").get("status").asText().startsWith("DRY_RUN_"))
    assert(output.path("facets").path("schema").get("fields").elements().asScala.map(_.get("name").asText()).toList == List("id", "doubled"))
    val source = output.path("facets").path("columnLineage").path("fields").path("doubled").get("inputFields").get(0)
    assert(source.get("field").asText() == "id" && source.get("name").asText().endsWith("/orders"))
    assert(written.get("inputs").elements().asScala.exists(_.get("name").asText().endsWith("/orders")))
  }
}
