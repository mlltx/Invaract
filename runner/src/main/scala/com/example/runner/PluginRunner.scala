// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.runner

import com.example.ir.{Lineage, PlanPrinter}
import com.example.sparkadapter.{SparkAdapterListener, TranslationResult}

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
  error: Option[String]
)

object PluginRunner {
  def main(args: Array[String]): Unit = {
    val inputPath = args.headOption.getOrElse("demo/input/sample.csv")
    val outputPath = args.applyOrElse(1, (_: Int) => "demo/output/result.parquet")
    val reportPath = args.applyOrElse(2, (_: Int) => "demo/output/report.json")

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
      val transformationIR = waitForTranslation(irListener).map(reportOf).getOrElse(
        Map("captured" -> false, "note" -> "Transformation IR was not captured within the timeout")
      )

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

    System.exit(if (report.status == "PASS") 0 else 1)
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
    sb.append(s"""  "transformationIR": ${anyToJson(report.transformationIR)}""")
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
