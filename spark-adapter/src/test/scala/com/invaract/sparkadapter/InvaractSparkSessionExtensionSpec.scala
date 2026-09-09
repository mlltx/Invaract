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
    Files.write(
      notifyPropsFile,
      s"""sink.enabled=true
         |sink.class=com.invaract.sparkadapter.notification.FileNotificationSink
         |sink.property.path=$eventsFile
         |""".stripMargin.getBytes("UTF-8")
    )

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
}
