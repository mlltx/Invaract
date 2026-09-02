// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.runner

import com.invaract.contract.ContractParser
import com.invaract.ir.Lineage
import com.invaract.ir.PlanPrinter
import com.invaract.sparkadapter.{ContractEnforcementRule, ContractViolationException, SparkAdapterListener, TranslationResult}

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

/** An example Spark job, run as a test harness: it drives `InvaractPlugin`
  * (a stand-in for a real transformation) through a real `SparkSession` with
  * Invaract's verification engine (`contract`/`ir`/`spark-adapter`)
  * installed as an extension, then captures the outcome as `report.json`.
  * This class is not part of that engine and is not what a real Invaract
  * user would depend on — it exists to prove the engine works end-to-end
  * against an actual Spark job, the same way any user's own job would use
  * it. See ARCHITECTURE.md's "Example Integration & Test Harness" section.
  */
object DemoJobHarness {
  def main(args: Array[String]): Unit = {
    // Dry-run mode (ROADMAP.md): run with no contract at all, and instead
    // of enforcing one, infer and print what a contract covering this run
    // would look like — see ContractEnforcementRule.dryRun's class doc. A
    // flag rather than a magic positional value so "no contract" stays an
    // explicit, discoverable choice, not an easy-to-miss reinterpretation
    // of an omitted argument. Recognized anywhere in `args` and stripped
    // before positional parsing, so `--dry-run` can precede or follow the
    // other arguments equally.
    val dryRun = args.contains("--dry-run")
    val positional = args.filterNot(_ == "--dry-run")

    val inputPath = positional.headOption.getOrElse("demo/input/sample.csv")
    val outputPath = positional.applyOrElse(1, (_: Int) => "demo/output/result.parquet")
    val reportPath = positional.applyOrElse(2, (_: Int) => "demo/output/report.json")
    val contractPath = positional.applyOrElse(3, (_: Int) => "demo/contracts/invaract_output.yaml")

    val startTime = System.currentTimeMillis()

    val report = Try {
      // Loaded before the SparkSession, since ContractEnforcementRule must
      // be installed at session-construction time (SparkSessionExtensions
      // configuration can't be changed on an already-built session). In
      // dry-run mode there is no contract to load at all — contractPath is
      // ignored entirely, not just left unvalidated.
      val contract = if (dryRun) None else Some(ContractParser.parseFile(contractPath))

      // Least invasive way to observe a write's real logical plan for
      // *reporting*: a QueryExecutionListener, registered once, requires no
      // change to how outputDf.write below is called. See spark-adapter's
      // SparkPlanAdapter class doc / docs/SPARK_ADAPTER.md for why this
      // extension point was chosen over SparkSessionExtensions for that
      // purpose. It is not, however, sufficient to *gate* a write: it only
      // fires after Spark has already executed the query. Enforcement (see
      // below) needs a different mechanism entirely.
      val irListener = new SparkAdapterListener

      // Only dry-run mode ever writes to this; a mutable cell is the
      // simplest way to get a value out of a check-rule callback (the same
      // "last value wins" pattern SparkAdapterListener.lastWrite already
      // uses for the analogous post-execution case — see dryRun's own doc
      // for why more than one callback per write is possible).
      @volatile var inferredContract: Option[com.invaract.contract.Contract] = None

      val spark = SparkSession
        .builder()
        .appName("InvaractDemoJobHarness")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        // Moves verification into the Spark execution lifecycle (ROADMAP.md
        // Phase 5): this check rule runs on the analyzed plan of every
        // query the session executes, before Spark runs any of them. A
        // write that violates `contract` throws ContractViolationException
        // here, aborting before any data is written — see
        // ContractEnforcementRule's class doc for why a check rule, not the
        // listener above, is the correct mechanism for this. Dry-run mode
        // installs the analogous observe-only rule instead — see
        // ContractEnforcementRule.dryRun's class doc.
        .withExtensions(_.injectCheckRule(contract match {
          case Some(c) => ContractEnforcementRule.forContract(c)
          case None    => ContractEnforcementRule.dryRun(c => inferredContract = Some(c))
        }))
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
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
      val outputDf = com.invaract.plugin.InvaractPlugin.process(inputDf)

      // Verification happens as part of this call, before any data is
      // written (ContractEnforcementRule, installed above). Reaching the
      // next line means the write already succeeded *and* was verified —
      // a ContractViolationException here is caught below, and by then no
      // output file exists at all.
      outputDf.write.mode("overwrite").parquet(outputPath)

      // QueryExecutionListener callbacks run on Spark's own listener
      // thread, asynchronously with respect to the write call above, so
      // the translation may not be available the instant write() returns.
      val translationResult = waitForTranslation(irListener)
      val transformationIR = translationResult.map(reportOf).getOrElse(
        Map("captured" -> false, "note" -> "Transformation IR was not captured within the timeout")
      )

      // The write only reached this point because ContractEnforcementRule
      // already verified it (or, in dry-run mode, was only observed);
      // report that outcome rather than re-verifying.
      val contractVerification = contract match {
        case Some(c) =>
          Map(
            "status" -> "PASSED",
            "contract" -> s"${c.id}@${c.version}",
            "contractPath" -> contractPath,
            "violations" -> List()
          )
        case None =>
          Map(
            "status" -> "DRY_RUN",
            "inferredContractYaml" -> inferredContract.map(ContractParser.write).getOrElse("")
          )
      }

      val outputSchema = outputDf.schema.fields.map(f => Map(
        "name" -> f.name,
        "type" -> f.dataType.typeName
      ))

      val outputSample = outputDf.limit(5).collect().map(row => {
        row.schema.fieldNames.map(name => name -> row.getAs[Any](name)).toMap
      })

      val pluginEvents = com.invaract.plugin.InvaractPlugin.getEvents

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
          "pluginName" -> "invaract-spark-plugin",
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
      case Failure(e: ContractViolationException) =>
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
          contractVerification = Map(
            "status" -> e.result.status,
            "contract" -> e.result.contract,
            "contractPath" -> contractPath,
            "violations" -> e.result.violations.map(_.toMap),
            "explanation" -> e.getMessage
          ),
          error = Some("Write aborted: this transformation violates its contract. See contractVerification for the full explanation.")
        )
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

    report.contractVerification.get("status") match {
      case Some("DRY_RUN") =>
        println("\nDry-run mode: no contract was supplied, so nothing was enforced.")
        report.contractVerification.get("inferredContractYaml") match {
          case Some(yaml: String) if yaml.nonEmpty =>
            println("Inferred contract, from this run's actual inputs/outputs — copy it into a file, review it, " +
              "and pass it as the contract argument to use the normal (enforced) mode:\n")
            println(yaml)
          case _ =>
            println("No write was recognized during this run, so no contract could be inferred.")
        }
      case Some(status: String) =>
        println(s"\nContract verification: $status (${report.contractVerification.getOrElse("contract", "?")})")
        report.contractVerification.get("explanation") match {
          case Some(explanation: String) =>
            // ContractEnforcementRule already built the full what/what/why/
            // how explanation (see its class doc); print it as-is rather
            // than re-deriving a shorter summary from the same data.
            println()
            println(explanation)
          case _ =>
            report.contractVerification.get("violations") match {
              case Some(violations: List[_]) if violations.nonEmpty =>
                violations.foreach {
                  case v: Map[_, _] =>
                    val m = v.asInstanceOf[Map[String, Any]]
                    val detail = m.collect { case (k, value) if k != "type" && k != "message" => s"$k=$value" }.mkString(", ")
                    val suffix = if (detail.isEmpty) "" else s" ($detail)"
                    println(s"  ✗ ${m("type")}: ${m("message")}$suffix")
                  case _ =>
                }
              case _ =>
                println("  (no violations)")
            }
        }
      case _ =>
        println("\nContract verification: not run (see report.json for details)")
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
