// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.Contract
import com.invaract.verification.notification.{JobInfo, NotificationSink}
import com.invaract.verification.{AttachPlan, AttachSetup, ConfigSource, VerificationOptions, VerificationSetup}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions

import scala.util.Try

/** The Beam adapter's entry points.
  *
  * Switching it on without touching a job's source is `--runner=com.invaract.beamadapter.InvaractRunner` plus the
  * `--invaract...` options (`InvaractBeamOptions`): Beam then builds the runner through `InvaractRunner.fromOptions`, which calls
  * `fromOptions` here. A job that picks its own runner in code, or that never reads its command line, is not reached that
  * way; for those a job author calls `InvaractBeam.verify(pipeline)` once, which reads the same options off the pipeline.
  */
object InvaractBeam {

  /** The engine name every event this adapter publishes carries (`job.engine`). */
  val Engine = "beam"

  /** The check the `--invaract...` options of `options` describe: enforcement of the contract named there, or dry-run mode.
    * Organizational policy is enforced here, once, eagerly - before any pipeline is looked at. */
  def fromOptions(options: PipelineOptions): BeamCheck = {
    val config = new BeamConfigSource(options.as(classOf[InvaractBeamOptions]))
    val job = jobInfoOf(options)
    AttachSetup.select(config) match {
      case AttachPlan.Enforce(contract, sink) => enforce(contract, VerificationOptions(), sink, config, job)
      case AttachPlan.DryRun(sink)            => new BeamCheck(None, VerificationOptions(), sink, job, BeamCapabilities.declared, VerificationSetup.trustedSteps(config))
    }
  }

  /** Enforcement of `contract` under `options`, resolving `ref://` locations, overlaying the settings in `config` and
    * applying organizational policy exactly as every adapter does (`VerificationSetup`). */
  def enforce(
      contract: Contract,
      options: VerificationOptions = VerificationOptions(),
      sink: Option[NotificationSink] = None,
      config: ConfigSource = ConfigSource.empty,
      job: JobInfo = JobInfo(engine = Some(Engine))
  ): BeamCheck = {
    val located = VerificationSetup.resolveContractLocations(contract, config)
    val resolved = VerificationSetup.resolveVerificationOptions(options, config)
    val (governed, governedOptions) = VerificationSetup.enforceOrgPolicy(located, resolved, config, sink, job.runId, Some(job))
    new BeamCheck(Some(governed), governedOptions, sink, job, BeamCapabilities.declared, VerificationSetup.trustedSteps(config))
  }

  /** Checks `pipeline` as its own options say (`fromOptions(pipeline.getOptions)`). One line a job author can add when the
    * command-line route cannot reach the job. */
  def verify(pipeline: Pipeline): Unit = fromOptions(pipeline.getOptions).verify(pipeline)

  /** Which engine and job a check's events come from. */
  private[beamadapter] def jobInfoOf(options: PipelineOptions): JobInfo = {
    val jobName = Option(options.getJobName)
    JobInfo(
      runId = jobName,
      name = jobName,
      engine = Some(Engine),
      engineVersion = Try(org.apache.beam.sdk.util.ReleaseInfo.getReleaseInfo.getSdkVersion).toOption,
      engineDetails = Option(options.getRunner).map(r => "runner" -> r.getName).toMap,
      user = Option(options.getUserAgent).filter(_.nonEmpty).flatMap(_ => Try(System.getProperty("user.name")).toOption),
      startTimeMs = Some(System.currentTimeMillis())
    )
  }
}
