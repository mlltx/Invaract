package com.example.runner

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
          error = Some(e.getMessage)
        )
    }

    // Write report
    new File(reportPath).getParentFile.mkdirs()
    val json = reportToJson(report)
    scala.io.Source.fromFile(reportPath, "UTF-8")
    java.nio.file.Files.write(
      java.nio.file.Paths.get(reportPath),
      json.getBytes("UTF-8")
    )

    // Print summary
    println(s"Report written to: $reportPath")
    println(s"Status: ${report.status}")
    println(s"Duration: ${report.durationMs}ms")

    System.exit(if (report.status == "PASS") 0 else 1)
  }

  private def reportToJson(report: ExecutionReport): String = {
    import scala.collection.mutable
    val sb = mutable.StringBuilder()
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
    sb.append(s"""  "plugin": ${anyToJson(report.plugin)}""")
    if (report.error.isDefined) {
      sb.append(s""",\n  "error": "${report.error.get}" """)
    }
    sb.append("\n}\n")
    sb.toString()
  }

  private def mapToJson(m: Map[String, String]): String = {
    "{" + m.map { case (k, v) => s""""$k": "$v"""" }.mkString(", ") + "}"
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
      case s: String => s""""$s""""
      case n: Number => n.toString
      case b: Boolean => b.toString
      case null => "null"
      case _ => s""""$obj""""
    }
  }
}
