// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.Contract
import com.invaract.ir.Plan
import com.invaract.verification.{AdapterCapabilities, VerificationOptions}

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
}
