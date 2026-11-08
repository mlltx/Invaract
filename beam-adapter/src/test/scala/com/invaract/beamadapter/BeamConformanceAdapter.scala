// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.Contract
import com.invaract.ir.Plan
import com.invaract.testkit.{AttachedRun, ConformanceAdapter, EnvironmentUnavailable, RecordingSink, ScenarioJob, ScenarioOutcome}
import com.invaract.verification.{AdapterCapabilities, ConfigSource, ContractViolationException, InvaractConf, VerificationOptions}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.io.gcp.testing.{FakeBigQueryServices, FakeDatasetService, FakeJobService}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}

import java.nio.file.{Files, Path}
import scala.util.Try
import scala.util.control.NonFatal

/** Beam as an adapter for the conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4).
  *
  * `run` turns a neutral `ScenarioJob` into a real Beam pipeline (`BeamScenarioJobs`: Parquet in, Beam transforms, Parquet or
  * CSV out), constructs it, and runs it through `InvaractRunner` - the adapter's real enforcement path, the one a job gets
  * when its command line names `--runner=...InvaractRunner` - which checks the pipeline before handing it to Beam's
  * in-process `DirectRunner`. A rejected pipeline never reaches the `DirectRunner`; an allowed one really runs, over empty data.
  */
class BeamConformanceAdapter extends ConformanceAdapter {

  override def capabilities: AdapterCapabilities =
    BeamCapabilities.declared.getOrElse(throw new IllegalStateException("Beam's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    val sink = new RecordingSink
    val root = Files.createTempDirectory(s"beam-conformance-")
    try {
      val beamOptions = PipelineOptionsFactory.create()
      beamOptions.setTempLocation(root.resolve("tmp").toString)
      val pipeline = Pipeline.create(beamOptions)
      BeamScenarioJobs.build(pipeline, job, root, BeamConformanceAdapter.targetFor(contract, job))
      val check = InvaractBeam.enforce(contract, options, Some(sink), ConfigSource.empty, InvaractBeam.jobInfoOf(beamOptions))
      val runner = InvaractRunner.enforcing(check, DirectRunner.fromOptions(beamOptions).asInstanceOf[PipelineRunner[PipelineResult]])
      runner.run(pipeline).waitUntilFinish()
      sink.passed
    } catch {
      case e: ContractViolationException => sink.rejected(e.result.violations.map(_.violationType).toSet)
    } finally BeamConformanceAdapter.deleteQuietly(root)
  }

  /** Beam's plan lineage is not available (the functions in a Beam job are opaque), and the lineage and sensitivity
    * capabilities are declared unsupported, so the kit never asks. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = None

  /** The job launched the way a platform launches one: the settings as `--invaract<Name>` flags beside `--runner` naming
    * `InvaractRunner`, parsed into the pipeline's own options, and the pipeline created and run with those options alone. Beam
    * builds the runner `--runner` names when the pipeline is created, so a contract or location that cannot be resolved stops
    * the job there, with the message its user sees. */
  override def attached(scenarioId: String, contract: Contract, job: ScenarioJob, settings: Map[String, String]): Option[AttachedRun] = {
    val root = Files.createTempDirectory("beam-conformance-")
    try
      Some(
        try {
          val beamOptions = PipelineOptionsFactory
            .fromArgs(BeamConformanceAdapter.flags(settings) ++ List(s"--runner=${classOf[InvaractRunner].getName}", "--invaractDelegate=DirectRunner"): _*)
            .create()
          beamOptions.setTempLocation(root.resolve("tmp").toString)
          val pipeline = Pipeline.create(beamOptions)
          BeamScenarioJobs.build(pipeline, job, root, BeamConformanceAdapter.targetFor(contract, job))
          pipeline.run().waitUntilFinish()
          AttachedRun.Ran
        } catch {
          // blocked: the verdict is in the events the configured sink wrote
          case _: ContractViolationException => AttachedRun.Ran
          case NonFatal(e)                   => AttachedRun.Stopped(Option(e.getMessage).getOrElse(e.getClass.getName))
        }
      )
    finally BeamConformanceAdapter.deleteQuietly(root)
  }

  /** Beam's users add the registry client to the job's classpath themselves, as a dependency of their launcher, so the kit
    * names its own, as they name theirs. */
  override def bundlesRegistryClient: Boolean = false

  /** The settings as `--invaract<Name>` flags, parsed into pipeline options and read as the runner reads them. */
  override def configSource(settings: Map[String, String]): Option[ConfigSource] =
    Some(new BeamConfigSource(PipelineOptionsFactory.fromArgs(BeamConformanceAdapter.flags(settings): _*).create().as(classOf[InvaractBeamOptions])))

  override def inapplicableSettings: Map[String, String] =
    Map(InvaractConf.FailOnMissingOutput -> "Beam sees a job's whole graph before it runs, so a declared output it never writes is refused then")
}

private[beamadapter] object BeamConformanceAdapter {

  /** The project of the in-memory BigQuery the scenario jobs write to. */
  val Project = "conformance"

  /** Files, unless the scenario is about something only a BigQuery write states in Beam - a save mode, or a catalog table - in
    * which case the job writes a BigQuery table, to a fresh in-memory BigQuery (Beam's own `FakeBigQueryServices`) so it runs. */
  def targetFor(contract: Contract, job: ScenarioJob): BeamScenarioJobs.Target =
    if (job.output.registeredAs.isEmpty && !contract.outputs.exists(_.saveMode.isDefined)) BeamScenarioJobs.ToFiles
    else {
      FakeDatasetService.setUp()
      FakeJobService.setUp()
      val datasets = new FakeDatasetService()
      datasets.createDataset(Project, job.output.location.split('/').init.mkString("_"), "", "", null)
      BeamScenarioJobs.BigQuery(new FakeBigQueryServices().withDatasetService(datasets).withJobService(new FakeJobService()), Project)
    }

  /** The settings as a Beam command line spells them: `--invaract<Name>=<value>`. */
  def flags(settings: Map[String, String]): List[String] =
    settings.toList.sortBy(_._1).map { case (name, value) => s"${BeamConfigSource.spelling(name)}=$value" }

  /** Removes `root` and everything under it; a file the platform still holds open is left rather than failing the run. */
  def deleteQuietly(root: Path): Unit =
    Try {
      val walk = Files.walk(root)
      try walk.sorted(java.util.Comparator.reverseOrder[Path]()).forEach(p => Try(Files.deleteIfExists(p)))
      finally walk.close()
    }
}
