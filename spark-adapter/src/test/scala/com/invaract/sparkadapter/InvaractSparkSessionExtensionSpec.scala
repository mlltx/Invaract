// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Proves `InvaractSparkSessionExtension`'s actual claim — that installing
  * Invaract via `spark.sql.extensions` + `spark.invaract.*` conf keys alone
  * genuinely works, with zero code beyond `SparkSession.builder()...` — so
  * this suite builds its own real `SparkSession`s the ordinary
  * `spark-submit`/end-user way (`.config("spark.sql.extensions", ...)`),
  * never touching `ContractEnforcementRule`/`withExtensions` directly the
  * way every other spec in this module does. Each test that needs a
  * session with different conf builds and stops its own — this class's
  * whole point is exercised at session-construction time (see its own
  * doc), so, unlike `ContractEnforcementRuleSpec`'s shared session, one
  * session per conf combination is unavoidable, not merely a convenience.
  */
class InvaractSparkSessionExtensionSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-extension-test")
  }

  override def afterAll(): Unit = {
    // Best-effort: individual tests already stop their own sessions.
  }

  private def buildSession(confs: (String, String)*): SparkSession = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName("InvaractSparkSessionExtensionSpec")
      .config("spark.sql.extensions", classOf[InvaractSparkSessionExtension].getName)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
    val withConfs = confs.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }
    val session = withConfs.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  private val passingContractYaml =
    """id: extension_demo
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

  test("a write satisfying its conf-attached contract executes normally, with zero code beyond building the session") {
    val outputPath = scratchDir.resolve("pass.parquet").toString
    val contractFile = Files.createTempFile(scratchDir, "contract", ".yaml")
    Files.write(contractFile, passingContractYaml.replace("OUTPUT_PATH", outputPath).getBytes("UTF-8"))

    val spark = buildSession(
      InvaractSparkSessionExtension.ContractConfKey -> contractFile.toString
    )
    try {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw
      assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally {
      spark.stop()
    }
  }

  test("a write violating its conf-attached contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail.parquet").toString
    val yaml =
      s"""id: extension_demo
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
    val contractFile = Files.createTempFile(scratchDir, "contract", ".yaml")
    Files.write(contractFile, yaml.getBytes("UTF-8"))

    val spark = buildSession(
      InvaractSparkSessionExtension.ContractConfKey -> contractFile.toString
    )
    try {
      val df = spark.range(5).withColumn("doubled", col("id") * 2) // no customer_name
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
      assert(!Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally {
      spark.stop()
    }
  }

  test("spark.invaract.dryRun=true never blocks a write, with no contract conf set at all") {
    val outputPath = scratchDir.resolve("dry_run.parquet").toString

    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true"
    )
    try {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw - nothing to violate
      assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally {
      spark.stop()
    }
  }

  test("neither spark.invaract.contract nor spark.invaract.dryRun set fails closed with a clear message") {
    val spark = buildSession() // no Invaract conf keys at all
    try {
      val ex = intercept[IllegalStateException] {
        spark.range(5).count() // any analyzed plan triggers the check rule
      }
      assert(ex.getMessage.contains(InvaractSparkSessionExtension.ContractConfKey))
      assert(ex.getMessage.contains(InvaractSparkSessionExtension.DryRunConfKey))
    } finally {
      spark.stop()
    }
  }

  test("spark.invaract.notifyConfig attaches a real sink and listener purely via conf") {
    val outputPath = scratchDir.resolve("with_sink.parquet").toString
    val contractFile = Files.createTempFile(scratchDir, "contract", ".yaml")
    Files.write(contractFile, passingContractYaml.replace("OUTPUT_PATH", outputPath).getBytes("UTF-8"))

    val eventsFile = scratchDir.resolve("events.jsonl")
    val notifyPropsFile = Files.createTempFile(scratchDir, "notify", ".properties")
    // java.util.Properties.load treats a bare backslash as the start of an
    // escape sequence, so interpolating eventsFile's raw path into the file
    // bytes corrupts it on Windows (a Windows temp path is full of them) -
    // the sink then silently writes somewhere other than eventsFile, and
    // this test's own read of eventsFile never finds it. Properties.store
    // escapes it correctly, the same fix applied to
    // ContractEnforcementRuleSpec's location-map fixtures for the same
    // reason.
    val notifyProps = new java.util.Properties()
    notifyProps.setProperty("sink.enabled", "true")
    notifyProps.setProperty("sink.class", "com.invaract.sparkadapter.notification.FileNotificationSink")
    notifyProps.setProperty("sink.property.path", eventsFile.toString)
    val notifyPropsOut = new java.io.FileOutputStream(notifyPropsFile.toFile)
    try notifyProps.store(notifyPropsOut, null)
    finally notifyPropsOut.close()

    val spark = buildSession(
      InvaractSparkSessionExtension.ContractConfKey -> contractFile.toString,
      InvaractSparkSessionExtension.NotifyConfigConfKey -> notifyPropsFile.toString
    )
    try {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath)

      // QueryExecutionListener callbacks (the WriteEvent below) run
      // asynchronously on Spark's own listener thread - same brief poll
      // DemoJobHarness's own waitForTranslation uses for the same reason.
      val deadline = System.currentTimeMillis() + 15000
      while ((!Files.exists(eventsFile) || !new String(Files.readAllBytes(eventsFile), "UTF-8").contains("\"eventType\": \"WRITE\"")) && System.currentTimeMillis() < deadline) {
        Thread.sleep(100)
      }

      val events = new String(Files.readAllBytes(eventsFile), "UTF-8")
      assert(events.contains("\"eventType\": \"CONTRACT_VALIDATION\""), "sink attached purely via conf must receive the check's event")
      assert(events.contains("\"eventType\": \"WRITE\""), "the listener registered purely via conf must observe the completed write")
    } finally {
      spark.stop()
    }
  }

  test("checkRuleFor: a contract satisfied still runs cleanly when accessed directly (no SparkSessionExtensions indirection)") {
    val outputPath = scratchDir.resolve("direct.parquet").toString
    val contractFile = Files.createTempFile(scratchDir, "contract", ".yaml")
    Files.write(contractFile, passingContractYaml.replace("OUTPUT_PATH", outputPath).getBytes("UTF-8"))

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("InvaractSparkSessionExtensionSpec-direct")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config(InvaractSparkSessionExtension.ContractConfKey, contractFile.toString)
      .getOrCreate()
    try {
      spark.sparkContext.setLogLevel("ERROR")
      val rule = InvaractSparkSessionExtension.checkRuleFor
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // unchecked - no extension installed on this session
      rule(spark)(df.queryExecution.analyzed) // no write node in this captured plan -> no-op, must not throw
    } finally {
      spark.stop()
    }
  }

  private def writeNotifyProps(entries: (String, String)*): Path = {
    val file = Files.createTempFile(scratchDir, "notify", ".properties")
    val props = new java.util.Properties()
    entries.foreach { case (k, v) => props.setProperty(k, v) }
    val out = new java.io.FileOutputStream(file.toFile)
    try props.store(out, null)
    finally out.close()
    file
  }

  private def fileSinkProps(eventsFile: Path): Path =
    writeNotifyProps(
      "sink.enabled" -> "true",
      "sink.class" -> "com.invaract.sparkadapter.notification.FileNotificationSink",
      "sink.property.path" -> eventsFile.toString
    )

  private def eventLines(eventsFile: Path): List[String] =
    if (Files.exists(eventsFile)) new String(Files.readAllBytes(eventsFile), "UTF-8").split("\n").toList.filter(_.nonEmpty)
    else Nil

  test("dry-run + spark.invaract.notifyConfig reports every write and a job summary at application end, purely via conf") {
    val outputPath = scratchDir.resolve("dry_run_reporting.parquet").toString
    val eventsFile = scratchDir.resolve("dry_run_events.jsonl")

    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> fileSinkProps(eventsFile).toString,
      InvaractSparkSessionExtension.JobIdConfKey -> "e2e_job",
      InvaractSparkSessionExtension.JobMetadataConfPrefix + "team" -> "orders"
    )
    try {
      spark.range(5).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(outputPath)
      assert(Files.exists(java.nio.file.Paths.get(outputPath)), "dry-run never blocks the write")
    } finally {
      spark.stop() // posts SparkListenerApplicationEnd: this is what publishes the summary
    }

    val lines = eventLines(eventsFile)
    val inference = lines.filter(_.contains("\"eventType\": \"CONTRACT_INFERENCE\""))
    val summary = lines.filter(_.contains("\"eventType\": \"DRY_RUN_SUMMARY\""))
    assert(inference.size == 1, s"expected one inference event, got: $lines")
    assert(inference.head.contains("\"status\": \"INFERRED\""))
    assert(inference.head.contains("\"selfCheck\": \"PASSED\""))
    assert(inference.head.contains("\"jobId\": \"e2e_job\""))
    assert(inference.head.contains("\"team\": \"orders\""))
    assert(inference.head.contains("dry_run_reporting.parquet"))
    assert(summary.size == 1, s"expected exactly one summary at application end, got: $lines")
    assert(summary.head.contains("\"mergeStatus\": \"MERGED\""))
    assert(summary.head.contains("\"statusCounts\": {\"INFERRED\": 1}"))
    assert(lines.indexWhere(_.contains("CONTRACT_INFERENCE")) < lines.indexWhere(_.contains("DRY_RUN_SUMMARY")))
  }

  test("dry-run + notifyConfig on a job that writes nothing still reports NO_WRITES_OBSERVED at application end") {
    val eventsFile = scratchDir.resolve("dry_run_no_writes.jsonl")
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> fileSinkProps(eventsFile).toString
    )
    try assert(spark.range(5).count() == 5L)
    finally spark.stop()

    val lines = eventLines(eventsFile)
    assert(lines.size == 1, s"only the summary expected, got: $lines")
    assert(lines.head.contains("\"eventType\": \"DRY_RUN_SUMMARY\""))
    assert(lines.head.contains("\"NO_WRITES_OBSERVED\": 1"))
    assert(lines.head.contains("\"mergeStatus\": \"NO_DRAFTS\""))
  }

  test("dry-run with a disabled notifyConfig keeps the log-only behavior: no events, write still goes through") {
    val outputPath = scratchDir.resolve("dry_run_disabled_sink.parquet").toString
    val eventsFile = scratchDir.resolve("dry_run_disabled_events.jsonl")
    val disabled = writeNotifyProps("sink.enabled" -> "false", "sink.property.path" -> eventsFile.toString)
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> disabled.toString
    )
    try spark.range(3).write.mode("overwrite").parquet(outputPath)
    finally spark.stop()
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    assert(eventLines(eventsFile).isEmpty)
  }

  test("a fan-out sink named in notifyConfig delivers dry-run events to every target, purely via conf") {
    val outputPath = scratchDir.resolve("dry_run_fanout.parquet").toString
    val everything = scratchDir.resolve("fanout_everything.jsonl")
    val problems = scratchDir.resolve("fanout_problems.jsonl")
    val notify = writeNotifyProps(
      "sink.enabled" -> "true",
      "sink.class" -> "com.invaract.sparkadapter.notification.FanOutNotificationSink",
      "sink.property.sinks" -> "all,problems",
      "sink.property.all.class" -> "com.invaract.sparkadapter.notification.FileNotificationSink",
      "sink.property.all.property.path" -> everything.toString,
      "sink.property.problems.class" -> "com.invaract.sparkadapter.notification.FileNotificationSink",
      "sink.property.problems.property.path" -> problems.toString,
      "sink.property.problems.statuses" -> "INFERRED_DEGRADED,SKIPPED_UNSUPPORTED,SKIPPED_UNRECOGNIZED,INFERENCE_ERROR"
    )
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> notify.toString
    )
    try spark.range(3).write.mode("overwrite").parquet(outputPath)
    finally spark.stop()

    val all = eventLines(everything)
    assert(all.exists(_.contains("CONTRACT_INFERENCE")) && all.exists(_.contains("DRY_RUN_SUMMARY")))
    val onlyProblems = eventLines(problems)
    assert(!onlyProblems.exists(_.contains("CONTRACT_INFERENCE")), "a clean INFERRED draft is not a problem")
    assert(onlyProblems.exists(_.contains("DRY_RUN_SUMMARY")), "the end-of-job summary always passes")
  }

  test("dry-run with a notifyConfig that cannot be loaded warns and falls back to log-only instead of failing the job") {
    val outputPath = scratchDir.resolve("dry_run_bad_config.parquet").toString
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> scratchDir.resolve("does-not-exist.properties").toString
    )
    try {
      spark.range(3).write.mode("overwrite").parquet(outputPath) // must not throw
      assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally spark.stop()
  }

  test("dry-run with a sink.class that cannot be built falls back the same way") {
    val outputPath = scratchDir.resolve("dry_run_bad_class.parquet").toString
    val badClass = writeNotifyProps("sink.enabled" -> "true", "sink.class" -> "com.example.NoSuchSink")
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> badClass.toString
    )
    try {
      spark.range(3).write.mode("overwrite").parquet(outputPath) // must not throw
      assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally spark.stop()
  }

  test("dry-run with a sink whose own configuration is invalid (a missing required property) falls back too") {
    val outputPath = scratchDir.resolve("dry_run_bad_sink_props.parquet").toString
    val noPath = writeNotifyProps(
      "sink.enabled" -> "true",
      "sink.class" -> "com.invaract.sparkadapter.notification.FileNotificationSink" // requires sink.property.path
    )
    val spark = buildSession(
      InvaractSparkSessionExtension.DryRunConfKey -> "true",
      InvaractSparkSessionExtension.NotifyConfigConfKey -> noPath.toString
    )
    try {
      spark.range(3).write.mode("overwrite").parquet(outputPath) // must not throw
      assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally spark.stop()
  }

  test("dryRunSink: none when notifyConfig is unset or disabled, a sink when it is valid, none when it is not") {
    val spark = buildSession(InvaractSparkSessionExtension.DryRunConfKey -> "true")
    try {
      assert(InvaractSparkSessionExtension.dryRunSink(spark).isEmpty)

      spark.conf.set(InvaractSparkSessionExtension.NotifyConfigConfKey, writeNotifyProps("sink.enabled" -> "false").toString)
      assert(InvaractSparkSessionExtension.dryRunSink(spark).isEmpty)

      spark.conf.set(InvaractSparkSessionExtension.NotifyConfigConfKey, fileSinkProps(scratchDir.resolve("sink_probe.jsonl")).toString)
      assert(InvaractSparkSessionExtension.dryRunSink(spark).isDefined)

      spark.conf.set(InvaractSparkSessionExtension.NotifyConfigConfKey, scratchDir.resolve("missing.properties").toString)
      assert(InvaractSparkSessionExtension.dryRunSink(spark).isEmpty)
    } finally spark.stop()
  }

  test("enforcement mode still fails loudly on a notifyConfig that cannot be loaded (only dry-run is lenient)") {
    val outputPath = scratchDir.resolve("enforce_bad_config.parquet").toString
    val contractFile = Files.createTempFile(scratchDir, "contract", ".yaml")
    Files.write(contractFile, passingContractYaml.replace("OUTPUT_PATH", outputPath).getBytes("UTF-8"))
    val spark = buildSession(
      InvaractSparkSessionExtension.ContractConfKey -> contractFile.toString,
      InvaractSparkSessionExtension.NotifyConfigConfKey -> scratchDir.resolve("does-not-exist.properties").toString
    )
    try {
      intercept[Exception] {
        spark.range(3).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(outputPath)
      }
      assert(!Files.exists(java.nio.file.Paths.get(outputPath)))
    } finally spark.stop()
  }
}
