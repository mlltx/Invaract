// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.Contract
import com.invaract.verification.notification.{JobInfo, NotificationSink}
import com.invaract.verification.{ConfigSource, VerificationOptions}
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
  def fromOptions(options: PipelineOptions): BeamCheck =
    BeamFrontEnd.Java.check(new BeamConfigSource(options.as(classOf[InvaractBeamOptions])), jobInfoOf(options))

  /** Enforcement of `contract` under `options`, resolving `ref://` locations, overlaying the settings in `config` and
    * applying organizational policy exactly as every adapter does (`VerificationSetup`). */
  def enforce(
      contract: Contract,
      options: VerificationOptions = VerificationOptions(),
      sink: Option[NotificationSink] = None,
      config: ConfigSource = ConfigSource.empty,
      job: JobInfo = JobInfo(engine = Some(Engine))
  ): BeamCheck = BeamFrontEnd.Java.enforcing(contract, options, sink, config, job)

  /** Checks `pipeline` as its own options say (`fromOptions(pipeline.getOptions)`). One line a job author can add when the
    * command-line route cannot reach the job. */
  def verify(pipeline: Pipeline): Unit = fromOptions(pipeline.getOptions).verify(pipeline)

  /** Which engine and job a check's events come from. */
  private[beamadapter] def jobInfoOf(options: PipelineOptions): JobInfo =
    BeamFrontEnd.Java.jobInfo(
      Option(options.getJobName),
      Try(org.apache.beam.sdk.util.ReleaseInfo.getReleaseInfo.getSdkVersion).toOption,
      Option(options.getRunner).map(_.getName),
      InvaractBeamOptions.checkOnly(options)
    )
}
