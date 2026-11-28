// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** What the three tools reported about one job's write. */
final case class Observed(invaract: Captured, openLineageSpark: Captured, spline: Captured, refused: Option[String], dir: String)

/** Runs one corpus job in a fresh SparkSession with Invaract, openlineage-spark and Spline's agent all capturing it,
  * each attached the way a platform would attach it (configuration only), and reads back what each reported. */
object Harness {

  def run(job: Job, fixture: Fixture): Observed = {
    val dir = Files.createTempDirectory(fixture.root, job.id)
    val out = Out(dir)
    val contract = dir.resolve("contract.yaml")
    Files.write(contract, contractFor(job, out).getBytes(StandardCharsets.UTF_8))
    val olConfig = dir.resolve("openlineage.properties")
    Files.write(olConfig, s"transport=file\npath=${slashes(dir.resolve("invaract.jsonl"))}\nnamespace=crosscheck\n".getBytes(StandardCharsets.UTF_8))
    val builder = SparkSession.builder().master("local[1]").appName(job.id)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", out.warehouse)
      .config("spark.sql.extensions", ("com.invaract.sparkadapter.InvaractSparkSessionExtension" :: job.extensions).mkString(","))
      .config("spark.invaract.contract", contract.toString)
      .config("spark.invaract.openLineageConfig", olConfig.toString)
      .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
      .config("spark.openlineage.transport.type", "file")
      .config("spark.openlineage.transport.location", dir.resolve("openlineage-spark.jsonl").toString)
      .config("spark.openlineage.namespace", "crosscheck")
      .config("spark.openlineage.columnLineage.datasetLineageEnabled", "true")
      .config("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
      .config("spark.spline.lineageDispatcher", "capture")
      .config("spark.spline.lineageDispatcher.capture.className", classOf[SplineCapture].getName)
      .config("spark.spline.lineageDispatcher.capture.path", dir.resolve("spline.jsonl").toString)
    val withConf = job.conf(out).foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }
    val spark = withConf.getOrCreate()
    val refused =
      try { job.run(spark, fixture, out); None }
      catch { case e: Exception if e.getClass.getSimpleName == "ContractViolationException" => Some(e.getMessage) }
      finally spark.stop()
    // Invaract delivers off the job's thread: its run's end is the last thing it writes
    val invaract = dir.resolve("invaract.jsonl")
    val deadline = System.nanoTime() + 30L * 1000000000L
    def ended = Files.exists(invaract) && Files.readAllLines(invaract).asScala.exists(l => l.contains("\"COMPLETE\"") || l.contains("\"FAIL\""))
    while (!ended && System.nanoTime() < deadline) Thread.sleep(50)
    // every job writes one dataset, never one of the shared inputs: each tool's outputs are that write, however it
    // names it (a difference in naming is then a difference the comparison reports, not a write the harness fails to
    // find). openlineage-spark also reports a CREATE TABLE over existing files as an output of that location.
    val inputs = slashes(fixture.root.resolve("inputs"))
    def isOutput(d: Ds) = !Option(d.name).exists(_.startsWith(inputs))
    Observed(
      Captured.fromOpenLineage(invaract, isOutput),
      Captured.fromOpenLineage(dir.resolve("openlineage-spark.jsonl"), isOutput),
      Captured.fromSpline(dir.resolve("spline.jsonl"), isOutput),
      refused,
      slashes(dir)
    )
  }

  /** A contract that declares only the job's output, where it writes: every read is undeclared, as in a job whose
    * contract names only what it produces. Lineage is published for a checked write, so the write must pass. */
  private def contractFor(job: Job, out: Out): String =
    s"""id: ${job.id.replace('-', '_')}
       |version: "1.0.0"
       |outputs:
       |  - { name: out, location: "${job.contractLocation(out)}" }
       |""".stripMargin

  def slashes(path: Path): String = path.toString.replace('\\', '/')
}

/** Where one job writes: its own directory, and a warehouse for the catalog tables it creates. */
final case class Out(dir: Path) {
  def at(name: String): String = Harness.slashes(dir.resolve(name))
  def warehouse: String = at("warehouse")
}
