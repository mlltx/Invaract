// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.verification.{VerificationOptions, ViolationType}
import com.invaract.verification.notification.{ContractValidationEvent, NotificationEvent, NotificationSink}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.funsuite.AnyFunSuite

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

/** A declared output a Spark job never writes (`DeclaredOutputs`): reported when the application ends, and with
  * `spark.invaract.failOnMissingOutput=true` the driver's exit status. */
class DeclaredOutputsSpec extends AnyFunSuite {

  private final class Capture extends NotificationSink {
    val events = ListBuffer.empty[NotificationEvent]
    override def publish(event: NotificationEvent): Unit = synchronized(events += event)
    def validations: List[ContractValidationEvent] = synchronized(events.toList.collect { case v: ContractValidationEvent => v })
  }

  private def contractYaml(out: String): String =
    s"""id: two_outputs
       |version: "1.0.0"
       |outputs:
       |  - { name: report, location: $out/report, schema: { fields: [ { name: id, type: long } ] } }
       |  - { name: audit, location: $out/audit, schema: { fields: [ { name: id, type: long } ] } }
       |""".stripMargin

  private def contract(out: Path): Contract = ContractParser.parse(contractYaml(out.toString))

  /** A session of its own (each test stops it, which is what ends the application), enforcing `c` with `sink`. */
  private def session(c: Contract, sink: Capture): SparkSession = {
    val s = SparkSession
      .builder()
      .master("local[1]")
      .appName("DeclaredOutputsSpec")
      .config("spark.ui.enabled", "false")
      .withExtensions(_.injectCheckRule(s => ContractEnforcementRule.forContract(c, VerificationOptions(), sink)(s)))
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  test("a job that writes every declared output reports nothing more when it ends") {
    val out = Files.createTempDirectory("declared-outputs-all")
    val sink = new Capture
    val spark = session(contract(out), sink)
    spark.range(3).write.parquet(s"$out/report")
    spark.range(3).write.parquet(s"$out/audit")
    spark.stop()
    assert(sink.validations.map(_.status) == List("PASSED", "PASSED"))
  }

  test("a declared output the job never writes is reported once, as MISSING_OUTPUT, when the application ends") {
    val out = Files.createTempDirectory("declared-outputs-missing")
    val sink = new Capture
    val spark = session(contract(out), sink)
    spark.range(3).write.parquet(s"$out/report")
    assert(sink.validations.map(_.status) == List("PASSED"), "nothing is known to be missing while the job runs")
    spark.stop()
    val Seq(passed, failed) = sink.validations
    assert(passed.status == "PASSED" && failed.status == "FAILED")
    assert(failed.violations.map(v => (v.violationType, v.location)) == List((ViolationType.MissingOutput, Some(s"$out/audit"))))
  }

  test("a job's record is decided once: a second ending publishes nothing more and gives the same answer") {
    val out = Files.createTempDirectory("declared-outputs-once")
    val sink = new Capture
    val outputs = new DeclaredOutputs(contract(out), Some(sink), None, None)
    outputs.record(s"file:$out/report")
    outputs.record(s"file:$out/report")
    assert(outputs.locations == List(s"file:$out/report"), "each location once")
    val first = outputs.finish()
    assert(first.exists(_.result.violations.map(_.location) == List(Some(s"$out/audit"))))
    assert(outputs.finish() eq first)
    assert(sink.validations.map(_.status) == List("FAILED"))
    assert(first.get.getMessage.contains(s"the job's writes: file:$out/report)"), "the plan names the writes")
    val none = new DeclaredOutputs(contract(out), None, None, None).finish()
    assert(none.get.getMessage.contains("the job's writes: none)"), "and says so when there were none")
  }

  test("an exit hook waits on the job only under failOnMissingOutput=true") {
    def attached(setting: Option[String]): Boolean = {
      val out = Files.createTempDirectory("declared-outputs-conf")
      val c = contract(out)
      val builder = SparkSession.builder().master("local[1]").appName("DeclaredOutputsSpec").config("spark.ui.enabled", "false")
      setting.foreach(v => builder.config("spark.invaract.failOnMissingOutput", v))
      val spark = builder.withExtensions(_.injectCheckRule(s => ContractEnforcementRule.forContract(c, VerificationOptions())(s))).getOrCreate()
      try {
        spark.range(1).write.parquet(s"$out/report")
        DeclaredOutputs.exitHookAttached(spark.sparkContext)
      } finally {
        DeclaredOutputs.discardAll(spark.sparkContext)
        spark.stop()
      }
    }
    assert(attached(Some("true")))
    assert(attached(Some(" TRUE ")))
    assert(!attached(Some("false")))
    assert(!attached(None))
  }

  test("the shutdown hook exits with status 1 only when a declared output was never written") {
    val out = Files.createTempDirectory("declared-outputs-hook")
    val halted = ListBuffer.empty[Int]
    val missing = new DeclaredOutputs(contract(out), None, None, None)
    missing.record(s"$out/report")
    DeclaredOutputs.exitHook(missing, halted += _).run()
    assert(halted.toList == List(DeclaredOutputs.ExitStatus) && DeclaredOutputs.ExitStatus == 1)

    halted.clear()
    val complete = new DeclaredOutputs(contract(out), None, None, None)
    complete.record(s"$out/report")
    complete.record(s"$out/audit")
    DeclaredOutputs.exitHook(complete, halted += _).run()
    assert(halted.isEmpty)
  }

  test("finishAll ends and forgets every contract an application enforces; discardAll forgets them unjudged") {
    val out = Files.createTempDirectory("declared-outputs-finish")
    val sink = new Capture
    val spark = session(contract(out), sink)
    try {
      spark.range(3).write.parquet(s"$out/report")
      val Seq(missing) = DeclaredOutputs.finishAll(spark.sparkContext)
      assert(missing.result.violations.map(_.violationType) == List(ViolationType.MissingOutput))
      assert(DeclaredOutputs.finishAll(spark.sparkContext).isEmpty, "forgotten once finished")
      spark.range(3).write.parquet(s"$out/audit")
      DeclaredOutputs.discardAll(spark.sparkContext)
      assert(DeclaredOutputs.finishAll(spark.sparkContext).isEmpty)
    } finally spark.stop()
    assert(sink.validations.map(_.status).count(_ == "FAILED") == 1, "nothing is reported for the forgotten records at the end")
  }

  // --- the exit status of a real job, in a JVM of its own ---

  private def runJob(failOnMissing: Boolean, writeAudit: Boolean): (Int, String) = {
    val out = Files.createTempDirectory("declared-outputs-job")
    val contractFile = out.resolve("contract.yaml")
    Files.write(contractFile, contractYaml(out.toString).getBytes("UTF-8"))
    val java = s"${System.getProperty("java.home")}/bin/java"
    // the same JVM flags this test JVM has (Spark needs its --add-opens), minus any agent
    val flags = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.filterNot(_.startsWith("-agent")).toList
    val args = flags ++ List("-cp", System.getProperty("java.class.path"), MissingOutputJob.getClass.getName.stripSuffix("$"),
      contractFile.toString, out.toString, failOnMissing.toString, writeAudit.toString)
    // in an argument file: the class path is longer than a Windows command line may be
    val argFile = out.resolve("job.args")
    Files.write(argFile, args.map(a => "\"" + a.replace("\\", "\\\\").replace("\"", "\\\"") + "\"").mkString("\n").getBytes("UTF-8"))
    val command = List(java, s"@$argFile")
    val log = out.resolve("job.log").toFile
    val process = new ProcessBuilder(command.asJava).redirectErrorStream(true).redirectOutput(log).start()
    assert(process.waitFor(5, TimeUnit.MINUTES), "the job did not end")
    (process.exitValue(), Source.fromFile(log).mkString)
  }

  test("a real job that never writes a declared output exits with status 1 under failOnMissingOutput") {
    val (status, log) = runJob(failOnMissing = true, writeAudit = false)
    assert(status == 1, log)
    assert(log.contains("MISSING_OUTPUT") && log.contains("failOnMissingOutput"), log)
  }

  test("without failOnMissingOutput the same job only reports it, and exits as it would have") {
    val (status, log) = runJob(failOnMissing = false, writeAudit = false)
    assert(status == 0, log)
    assert(log.contains("MISSING_OUTPUT"), log)
  }

  test("a real job that writes every declared output exits normally under failOnMissingOutput") {
    val (status, log) = runJob(failOnMissing = true, writeAudit = true)
    assert(status == 0, log)
    assert(!log.contains("MISSING_OUTPUT"), log)
  }
}
