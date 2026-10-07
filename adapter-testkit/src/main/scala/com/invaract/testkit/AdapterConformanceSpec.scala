// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import org.scalatest.funsuite.AnyFunSuite

/** Mix this into an adapter's test to run every conformance scenario on it:
  *
  * {{{
  * class MyEngineConformanceSpec extends AdapterConformanceSpec {
  *   override protected def adapter: ConformanceAdapter = new MyEngineConformanceAdapter
  * }
  * }}}
  *
  * One test per scenario, so a failure names the scenario. A scenario the adapter's own declaration
  * says does not apply is *canceled* with that reason (visible in the report, not a silent pass).
  * `adapter` is read lazily, per test, so an adapter that starts an engine does so once it is needed.
  */
trait AdapterConformanceSpec extends AnyFunSuite {

  /** The adapter under test. Called repeatedly: return the same instance. */
  protected def adapter: ConformanceAdapter

  Scenarios.all.foreach { scenario =>
    test(s"conformance: ${scenario.id} - ${scenario.description}") {
      Conformance.evaluateOne(adapter, adapter.capabilities, scenario) match {
        case ScenarioVerdict.Conforms         => succeed
        case ScenarioVerdict.Skipped(reason)  => cancel(s"does not apply to this adapter: $reason")
        case ScenarioVerdict.Diverges(reason) => fail(reason)
      }
    }
  }

  test("conformance: the adapter's claims the kit cannot yet verify are listed, not hidden") {
    val unverified = Conformance.unverifiedClaims(adapter.capabilities)
    info(
      if (unverified.isEmpty) "every capability this adapter claims is covered by a scenario"
      else "claimed but not covered by any scenario: " + unverified.map(_.id).mkString(", ")
    )
    // A claim on something the kit has no scenario for must at least be one the kit knows it lacks.
    assert(unverified.forall(Scenarios.notCovered.contains))
  }
}
