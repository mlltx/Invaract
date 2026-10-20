// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.Contract
import com.invaract.verification.notification.{JobInfo, NotificationSink}
import com.invaract.verification.{AdapterCapabilities, AttachPlan, AttachSetup, ConfigSource, VerificationOptions, VerificationSetup}

/** What differs between the two ways a pipeline reaches the adapter - a Java pipeline constructed in this JVM (`Java`) and a
  * Python one read from its portable form (`Python`, docs/BEAM_PYTHON_ADAPTER.md): the engine its events name, the catalogue of
  * steps its pipelines are made of, what it declares it can check, and the advice a refusal gives. Everything else (the
  * translation, the checks, the events) is the same, so these four always travel together and never get mixed.
  */
private[beamadapter] final case class BeamFrontEnd(
    engine: String,
    steps: StepCatalogue,
    capabilities: Option[AdapterCapabilities],
    advice: BeamAdvice
) {

  /** The check the settings in `config` describe: enforcement of the contract named there, or dry-run mode. Organizational policy
    * is enforced here, once, eagerly - before any pipeline is looked at. */
  def check(config: ConfigSource, job: JobInfo): BeamCheck =
    AttachSetup.select(config) match {
      case AttachPlan.Enforce(contract, sink) => enforcing(contract, VerificationOptions(), sink, config, job)
      case AttachPlan.DryRun(sink)            => new BeamCheck(None, VerificationOptions(), sink, job, capabilities, VerificationSetup.trustedSteps(config), advice)
    }

  /** Enforcement of `contract` under `options`, resolving `ref://` locations, overlaying the settings in `config` and applying
    * organizational policy exactly as every adapter does (`VerificationSetup`). */
  def enforcing(contract: Contract, options: VerificationOptions, sink: Option[NotificationSink], config: ConfigSource, job: JobInfo): BeamCheck = {
    val located = VerificationSetup.resolveContractLocations(contract, config)
    val resolved = VerificationSetup.resolveVerificationOptions(options, config)
    val (governed, governedOptions) = VerificationSetup.enforceOrgPolicy(located, resolved, config, sink, job.runId, Some(job))
    new BeamCheck(Some(governed), governedOptions, sink, job, capabilities, VerificationSetup.trustedSteps(config), advice)
  }

  /** Which job a check's events come from: the job's name (also its run id: Beam gives a job no other), the SDK release that
    * built it, the runner it is handed to, and the user submitting it. */
  def jobInfo(jobName: Option[String], sdkVersion: Option[String], runner: Option[String]): JobInfo =
    JobInfo(
      runId = jobName,
      name = jobName,
      engine = Some(engine),
      engineVersion = sdkVersion,
      engineDetails = runner.map("runner" -> _).toMap,
      user = Option(System.getProperty("user.name")),
      startTimeMs = Some(System.currentTimeMillis())
    )
}

private[beamadapter] object BeamFrontEnd {
  lazy val Java: BeamFrontEnd = BeamFrontEnd(InvaractBeam.Engine, JavaSteps, BeamCapabilities.declared, BeamAdvice.Java)
  lazy val Python: BeamFrontEnd = BeamFrontEnd(PortableCheck.Engine, PythonSteps, BeamCapabilities.python, BeamAdvice.Python)
}
