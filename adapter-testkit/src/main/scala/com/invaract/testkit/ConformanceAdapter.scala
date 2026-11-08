// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.Contract
import com.invaract.ir.Plan
import com.invaract.verification.{AdapterCapabilities, ConfigSource, VerificationOptions}

/** What an engine adapter implements to be run through the conformance scenarios: its own
  * capability declaration, and a way to run a neutral job on its engine under enforcement.
  */
trait ConformanceAdapter {

  /** The adapter's declaration - the same one that produces its docs-site matrix entry. */
  def capabilities: AdapterCapabilities

  /** Runs `job` on the engine, under the adapter's real enforcement of `contract` with `options`,
    * and reports what happened. Must go through the same code path a real job takes - the engine's
    * own hook, not a direct call to the verifier - or the scenario proves nothing about the adapter.
    * An adapter must not catch anything but the engine's own rejection: an unexpected exception
    * is a test failure, not a verdict.
    */
  def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome

  /** The engine-neutral plan the adapter's own translation produces for `job` - the `ir.Plan` its
    * enforcement would check, rooted at the write. Lineage and sensitivity propagation are computed from
    * a plan, so the only way to check them is to look at what the adapter translated.
    *
    * Return `None` only if the adapter cannot expose it, and declare `lineage.columnLevel` and
    * `analysis.sensitivityPropagation` `unsupported` or `not-applicable`: the kit reports a claim it cannot
    * look at as a divergence, not a pass. The job's inputs are materialized the way `run` does, but nothing
    * is written and no contract is enforced.
    */
  def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = None

  /** Runs `job` on the engine with Invaract attached the way a platform attaches it to a job whose source it does not own:
    * through the engine's own configuration alone, with nothing handed over in code. `settings` are given by their neutral
    * names (`InvaractConf`: `contract`, `registryUrl`, `locationMap`, `locationUrl`, `notifyConfig`, ...), and the adapter
    * spells each one its engine's way (Spark: `--conf spark.invaract.<name>`; Beam: `--invaract<Name>`; a Python job:
    * `INVARACT_<NAME>`) and installs Invaract through its real, configuration-driven path. `notifyConfig` names the file sink
    * the kit reads the run's validation events from, so the adapter installs no sink of its own.
    *
    * `contract` is the contract those settings lead to, with its `ref://` locations already resolved, given only so an
    * adapter whose jobs name datasets by the contract's names (not by location) can name them. It must not be enforced
    * from: the contract enforced is the one the adapter reaches through `settings`.
    *
    * Returns `AttachedRun.Ran` when the job ran (whether its write was allowed or blocked by the engine's own rejection,
    * which the kit reads from the events), or `AttachedRun.Stopped` with the message the job was told when anything else
    * stopped it before it ran. `None`: the adapter cannot attach a job through its configuration in a test, and the kit
    * reports its `config.contractRegistry` and `config.locationRefs` claims, which only such a job can show, as divergences.
    */
  def attached(scenarioId: String, contract: Contract, job: ScenarioJob, settings: Map[String, String]): Option[AttachedRun] = None

  /** Whether the adapter's shipped form carries the default contract registry client (`registry-client`'s
    * `HttpContractRegistryClient`), so a `registry://` contract works with nothing more installed. The kit then gives it no
    * `registryClientClass`, and a scenario fetching a contract fails if the client is not really there.
    *
    * `false` only for an engine whose users add the client themselves, as their own job's dependency (Spark's `--jars`, a
    * JVM classpath they control), and whose user documentation says so: the kit then names a client of its own that speaks
    * the same protocol (`KitRegistryClient`), as such a user names theirs. An adapter whose users cannot add a jar (a program
    * compiled ahead of time, a closed runtime) must carry the client, and leave this `true`.
    */
  def bundlesRegistryClient: Boolean = true

  /** The configuration the adapter would read if a platform set `settings` (by neutral name) through the engine's own
    * configuration surface, spelled the engine's way: the `ConfigSource` its attach path reads. The kit sets every neutral
    * setting and checks that each reaches it, so a setting added to `InvaractConf` cannot be left unreachable on one engine.
    * `None` fails that check, for every adapter that claims any of the `config.*` capabilities (a newly generated one, claiming
    * none, is not asked).
    */
  def configSource(settings: Map[String, String]): Option[ConfigSource] = None

  /** The neutral settings that do not apply to this engine, each with why (a setting for something the engine does not
    * have): the reachability check leaves them out. */
  def inapplicableSettings: Map[String, String] = Map.empty
}
