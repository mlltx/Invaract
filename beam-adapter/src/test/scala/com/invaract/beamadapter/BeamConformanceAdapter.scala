// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.Contract
import com.invaract.ir.Plan
import com.invaract.testkit.{ConformanceAdapter, EnvironmentUnavailable, RecordingSink, ScenarioJob, ScenarioOutcome}
import com.invaract.verification.{AdapterCapabilities, ConfigSource, ContractViolationException, VerificationOptions}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}

import java.nio.file.{Files, Path}
import scala.util.Try

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
      val pipeline = Pipeline.create(beamOptions)
      BeamScenarioJobs.build(pipeline, job, root)
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
}

private[beamadapter] object BeamConformanceAdapter {

  /** Removes `root` and everything under it; a file the platform still holds open is left rather than failing the run. */
  def deleteQuietly(root: Path): Unit =
    Try {
      val walk = Files.walk(root)
      try walk.sorted(java.util.Comparator.reverseOrder[Path]()).forEach(p => Try(Files.deleteIfExists(p)))
      finally walk.close()
    }
}
