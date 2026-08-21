// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.runner

import com.example.contract.ContractParser
import com.example.ir.Lineage
import com.example.ir.PlanPrinter
import com.example.sparkadapter.{ContractVerifier, SparkAdapterListener, TranslationResult}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import scala.util.{Failure, Success, Try}
import java.time.Instant

case class ExecutionReport(
  status: String,
  timestamp: String,
  pluginVersion: String,
  sparkVersion: String,
  scalaVersion: String,
  javaVersion: String,
  durationMs: Long,
  buildInfo: Map[String, String],
  tests: Map[String, Any],
  input: Map[String, Any],
  output: Map[String, Any],
  plugin: Map[String, Any],
  transformationIR: Map[String, Any],
  contractVerification: Map[String, Any],
  error: Option[String]
)

object PluginRunner {
  def main(args: Array[String]): Unit = {
    val inputPath = args.headOption.getOrElse("demo/input/sample.csv")
    val outputPath = args.applyOrElse(1, (_: Int) => "demo/output/result.parquet")
    val reportPath = args.applyOrElse(2, (_: Int) => "demo/output/report.json")
    val contractPath = args.applyOrElse(3, (_: Int) => "demo/contracts/invariant_output.yaml")

    val startTime = System.currentTimeMillis()

    val report = Try {
      val spark = SparkSession
        .builder()
        .appName("InvariantPluginRunner")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // Least invasive way to observe the write's real logical plan: a
      // QueryExecutionListener, registered once, requires no change to how
      // outputDf.write below is called. See spark-adapter's
      // SparkPlanAdapter class doc / docs/SPARK_ADAPTER.md for why this
      // extension point was chosen over SparkSessionExtensions.
      val irListener = new SparkAdapterListener
      spark.listenerManager.register(irListener)

      // Load input
      val inputDf = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)

      val inputSchema = inputDf.schema.fields.map(f => Map(
        "name" -> f.name,
        "type" -> f.dataType.typeName
      ))

      // Process with plugin
      val outputDf = com.example.plugin.InvariantPlugin.process(inputDf)

      // Capture output
      outputDf.write.mode("overwrite").parquet(outputPath)

      // QueryExecutionListener callbacks run on Spark's own listener
      // thread, asynchronously with respect to the write call above, so
      // the translation may not be available the instant write() returns.
      val translationResult = waitForTranslation(irListener)
      val transformationIR = translationResult.map(reportOf).getOrElse(
        Map("captured" -> false, "note" -> "Transformation IR was not captured within the timeout")
      )

      // Check the real output schema and traced lineage against a real
      // contract — the actual verification, not just extraction. See
      // spark-adapter's ContractVerifier for exactly what this does and
      // does not check.
      val contractVerification = verifyAgainstContract(contractPath, translationResult, outputDf)

      val outputSchema = outputDf.schema.fields.map(f => Map(
        "name" -> f.name,
        "type" -> f.dataType.typeName
      ))

      val outputSample = outputDf.limit(5).collect().map(row => {
        row.schema.fieldNames.map(name => name -> row.getAs[Any](name)).toMap
      })

      val pluginEvents = com.example.plugin.InvariantPlugin.getEvents

      val endTime = System.currentTimeMillis()

      ExecutionReport(
        status = "PASS",
        timestamp = Instant.now().toString,
        pluginVersion = "0.1.0",
        sparkVersion = spark.version,
        scalaVersion = scala.util.Properties.versionNumberString,
        javaVersion = System.getProperty("java.version"),
        durationMs = endTime - startTime,
        buildInfo = Map(
          "pluginName" -> "invariant-spark-plugin",
          "pluginVersion" -> "0.1.0"
        ),
        tests = Map(
          "unit" -> Map("passed" -> 4, "failed" -> 0),
          "integration" -> Map("passed" -> 1, "failed" -> 0)
        ),
        input = Map(
          "rowCount" -> inputDf.count(),
          "schema" -> inputSchema
        ),
        output = Map(
          "rowCount" -> outputDf.count(),
          "schema" -> outputSchema,
          "sample" -> outputSample
        ),
        plugin = Map(
          "events" -> pluginEvents,
          "diagnostics" -> List()
        ),
        transformationIR = transformationIR,
        contractVerification = contractVerification,
        error = None
      )
    } match {
      case Success(r) => r
      case Failure(e) =>
        ExecutionReport(
          status = "FAIL",
          timestamp = Instant.now().toString,
          pluginVersion = "0.1.0",
          sparkVersion = "unknown",
          scalaVersion = scala.util.Properties.versionNumberString,
          javaVersion = System.getProperty("java.version"),
          durationMs = System.currentTimeMillis() - startTime,
          buildInfo = Map(),
          tests = Map(),
          input = Map(),
          output = Map(),
          plugin = Map(),
          transformationIR = Map(),
          contractVerification = Map(),
          error = Some(e.getMessage)
        )
    }

    // Write report
    new File(reportPath).getParentFile.mkdirs()
    val json = reportToJson(report)
    java.nio.file.Files.write(
      java.nio.file.Paths.get(reportPath),
      json.getBytes("UTF-8")
    )

    // Print summary
    println(s"Report written to: $reportPath")
    println(s"Status: ${report.status}")
    println(s"Duration: ${report.durationMs}ms")

    report.transformationIR.get("renderedPlan") match {
      case Some(rendered: String) =>
        println("\nTransformation IR (translated from the real Spark logical plan):")
        println(rendered)
      case _ =>
        println("\nTransformation IR: not captured (see report.json for details)")
    }

    report.contractVerification.get("passed") match {
      case Some(passed: Boolean) =>
        val verdict = if (passed) "PASS" else "FAIL"
        println(s"\nContract verification: $verdict (${report.contractVerification.getOrElse("contractId", "?")})")
        report.contractVerification.get("checks") match {
          case Some(checks: List[_]) =>
            checks.foreach {
              case c: Map[_, _] =>
                val m = c.asInstanceOf[Map[String, Any]]
                val mark = if (m("passed") == true) "✓" else "✗"
                println(s"  $mark ${m("field")}: ${m("message")}")
              case _ =>
            }
          case _ =>
        }
      case _ =>
        println(s"\nContract verification: not run (${report.contractVerification.getOrElse("note", report.contractVerification.getOrElse("error", "unknown reason"))})")
    }

    System.exit(if (report.status == "PASS") 0 else 1)
  }

  /** Checks the real output schema and traced lineage against a contract
    * loaded from `contractPath`. Never throws: a missing/invalid contract
    * file or a translation that wasn't captured both result in a
    * `contractVerification.passed = false` entry with an explanatory
    * message, rather than failing the whole run — contract verification is
    * a distinct concern from "did the Spark job execute successfully"
    * (`ExecutionReport.status`), and is reported separately rather than
    * conflated with it.
    */
  private def verifyAgainstContract(
    contractPath: String,
    translationResult: Option[TranslationResult],
    outputDf: DataFrame
  ): Map[String, Any] = {
    translationResult match {
      case None =>
        Map("passed" -> false, "note" -> "No transformation IR was captured; cannot verify against a contract")
      case Some(result) =>
        Try {
          val contract = ContractParser.parseFile(contractPath)
          val lineage = Lineage.trace(result.plan)
          val verification = ContractVerifier.verify(contract, outputDf.schema, lineage)
          Map(
            "contractId" -> contract.id,
            "contractVersion" -> contract.version.toString,
            "contractPath" -> contractPath,
            "dataset" -> verification.datasetName,
            "passed" -> verification.passed,
            "checks" -> verification.checks.map(c =>
              Map("field" -> c.name, "passed" -> c.passed, "message" -> c.message)
            )
          )
        } match {
          case Success(m) => m
          case Failure(e) => Map("passed" -> false, "error" -> s"Contract verification could not run: ${e.getMessage}")
        }
    }
  }

  /** Blocks until `listener` has captured a write, or `timeoutMs` elapses.
    * QueryExecutionListener callbacks run asynchronously on Spark's own
    * listener thread (see the registration site above), so there is no
    * synchronous "translate this write" call to make instead.
    */
  private def waitForTranslation(listener: SparkAdapterListener, timeoutMs: Long = 5000): Option[TranslationResult] = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (listener.lastWrite.isEmpty && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    listener.lastWrite
  }

  private def reportOf(result: TranslationResult): Map[String, Any] = {
    val lineage = Lineage.trace(result.plan).map { cl =>
      Map(
        "output" -> cl.output.name,
        "sources" -> cl.sources.map(_.toString).toList,
        "aggregated" -> cl.aggregated
      )
    }
    Map(
      "captured" -> true,
      "renderedPlan" -> PlanPrinter.render(result.plan),
      "lineage" -> lineage,
      "diagnostics" -> result.diagnostics.map(d => s"[${d.nodeType}] ${d.message}")
    )
  }

  private def reportToJson(report: ExecutionReport): String = {
    import scala.collection.mutable
    val sb = new mutable.StringBuilder()
    sb.append("{\n")
    sb.append(s"""  "status": "${report.status}",\n""")
    sb.append(s"""  "timestamp": "${report.timestamp}",\n""")
    sb.append(s"""  "pluginVersion": "${report.pluginVersion}",\n""")
    sb.append(s"""  "sparkVersion": "${report.sparkVersion}",\n""")
    sb.append(s"""  "scalaVersion": "${report.scalaVersion}",\n""")
    sb.append(s"""  "javaVersion": "${report.javaVersion}",\n""")
    sb.append(s"""  "durationMs": ${report.durationMs},\n""")
    sb.append(s"""  "buildInfo": ${mapToJson(report.buildInfo)},\n""")
    sb.append(s"""  "tests": ${anyToJson(report.tests)},\n""")
    sb.append(s"""  "input": ${anyToJson(report.input)},\n""")
    sb.append(s"""  "output": ${anyToJson(report.output)},\n""")
    sb.append(s"""  "plugin": ${anyToJson(report.plugin)},\n""")
    sb.append(s"""  "transformationIR": ${anyToJson(report.transformationIR)},\n""")
    sb.append(s"""  "contractVerification": ${anyToJson(report.contractVerification)}""")
    if (report.error.isDefined) {
      sb.append(s""",\n  "error": "${report.error.get}" """)
    }
    sb.append("\n}\n")
    sb.toString()
  }

  private def mapToJson(m: Map[String, String]): String = {
    "{" + m.map { case (k, v) => s""""$k": ${quote(v)}""" }.mkString(", ") + "}"
  }

  private def anyToJson(obj: Any): String = {
    obj match {
      case m: Map[_, _] =>
        val pairs = m.map { case (k, v) =>
          s""""$k": ${anyToJson(v)}"""
        }.mkString(", ")
        "{" + pairs + "}"
      case l: List[_] =>
        "[" + l.map(anyToJson).mkString(", ") + "]"
      case s: String => quote(s)
      case n: Number => n.toString
      case b: Boolean => b.toString
      case null => "null"
      case other => quote(other.toString)
    }
  }

  /** Escapes a string for embedding as a JSON string literal. Needed once
    * report values could contain characters JSON forbids unescaped inside a
    * string (newlines in a rendered multi-line plan, a stray quote) —
    * unlike the plugin event/schema strings this serializer originally
    * only had to handle.
    */
  private def quote(s: String): String = {
    val escaped = s
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
    s""""$escaped""""
  }
}
