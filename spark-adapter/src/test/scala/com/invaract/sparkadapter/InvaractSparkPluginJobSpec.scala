// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.scalatest.funsuite.AnyFunSuite

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.io.Source

/** A real job attached by `spark.plugins`, with settings pinned by `INVARACT_PINNED_CONF`, in a JVM of its own (the
  * environment variable and the plugin both act when the application starts) - H-07. */
class InvaractSparkPluginJobSpec extends AnyFunSuite {

  private def runJob(plugins: String): (Int, String, Path) = {
    val dir = Files.createTempDirectory("invaract-plugin-job")
    val contract = dir.resolve("contract.yaml")
    Files.write(
      contract,
      s"""id: pinned
         |version: "1.0.0"
         |outputs:
         |  - { name: report, location: $dir/report, schema: { fields: [ { name: id, type: long } ] } }
         |""".stripMargin.getBytes("UTF-8")
    )
    val pins = dir.resolve("pins.properties")
    val properties = new java.util.Properties()
    properties.setProperty("spark.invaract.contract", contract.toString)
    properties.setProperty("spark.invaract.dryRun", "false")
    val pinsOut = Files.newOutputStream(pins)
    try properties.store(pinsOut, null)
    finally pinsOut.close()

    val out = dir.resolve("undeclared")
    val javaBin = s"${System.getProperty("java.home")}/bin/java"
    val flags = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.filterNot(_.startsWith("-agent")).toList
    val args = flags ++ List("-cp", System.getProperty("java.class.path"), PinnedAttachJob.getClass.getName.stripSuffix("$"), plugins, out.toString)
    val argFile = dir.resolve("job.args")
    Files.write(argFile, args.map(a => "\"" + a.replace("\\", "\\\\").replace("\"", "\\\"") + "\"").mkString("\n").getBytes("UTF-8"))
    val log = dir.resolve("job.log").toFile
    val builder = new ProcessBuilder(List(javaBin, s"@$argFile").asJava).redirectErrorStream(true).redirectOutput(log)
    builder.environment().put(PinnedSettings.EnvVar, pins.toString)
    val process = builder.start()
    assert(process.waitFor(5, TimeUnit.MINUTES), "the job did not end")
    (process.exitValue(), Source.fromFile(log).mkString, out)
  }

  test("a job that sets its own spark.sql.extensions and dryRun is still enforced: the plugin re-adds Invaract, the pins win") {
    val (status, log, out) = runJob(classOf[InvaractSparkPlugin].getName)
    assert(status == 3, log)
    assert(log.contains("PinnedAttachJob: REFUSED OUTPUT_LOCATION_MISMATCH"), log)
    assert(log.contains("spark.sql.extensions did not name Invaract"), log)
    assert(!Files.exists(out), "the refused write never landed")
  }

  test("a plugin class that cannot be loaded stops the application before any write") {
    val (status, log, out) = runJob("com.invaract.sparkadapter.NoSuchPlugin")
    assert(status != 0 && status != 3, log)
    assert(!log.contains("PinnedAttachJob: WRITTEN"), log)
    assert(!Files.exists(out))
  }
}
