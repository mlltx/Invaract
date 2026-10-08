// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability, CapabilityCheck, Support, ViolationType}

/** How one scenario went on one adapter. */
sealed trait ScenarioVerdict

object ScenarioVerdict {

  /** The adapter did what its declaration promises. */
  case object Conforms extends ScenarioVerdict

  /** The scenario cannot be run on this adapter, for a reason the adapter itself declared. */
  final case class Skipped(reason: String) extends ScenarioVerdict

  /** The adapter did something its declaration does not allow - the thing the kit exists to find. */
  final case class Diverges(reason: String) extends ScenarioVerdict
}

final case class ScenarioResult(scenario: Scenario, verdict: ScenarioVerdict)

/** @param unverifiedClaims capabilities the adapter declares `supported` or `partial` that no scenario
  *   is evidence for (`Scenarios.notCovered`) - claims the kit cannot yet check, never hidden.
  */
final case class ConformanceReport(adapter: String, results: List[ScenarioResult], unverifiedClaims: List[Capability]) {
  def failures: List[ScenarioResult] = results.filter(_.verdict.isInstanceOf[ScenarioVerdict.Diverges])
  def skipped: List[ScenarioResult] = results.filter(_.verdict.isInstanceOf[ScenarioVerdict.Skipped])
  def conforming: List[ScenarioResult] = results.filter(_.verdict == ScenarioVerdict.Conforms)

  /** The unverified claims no job could check by their nature (`Scenarios.attested`): the adapter's own tests carry them. */
  def attestedClaims: List[Capability] = unverifiedClaims.filter(Scenarios.attested.contains)

  /** The unverified claims the kit should be able to check but cannot yet (`Scenarios.gaps`). */
  def gapClaims: List[Capability] = unverifiedClaims.filter(Scenarios.gaps.contains)
}

/** What a scenario must produce on an adapter, given what that adapter declares.
  *
  * The rule is the contract's own: an adapter must do what it claims, and say so when it does not.
  * So a scenario's expected verdict is its neutral one, unless the adapter declares unsupported a
  * capability the scenario's contract relies on - then the right answer is the loud rejection
  * `UNSUPPORTED_CONTRACT_FEATURE`, never a quiet pass; or declares something not applicable to the
  * engine (or a needed operation unsupported) - then the scenario does not apply.
  */
object Conformance {

  /** What the adapter must do with `scenario`, or why the scenario is skipped for it. */
  def expectationFor(scenario: Scenario, capabilities: AdapterCapabilities): Either[String, Expected] = {
    def unavailable(c: Capability): Option[String] = capabilities.supportOf(c) match {
      case Support.NotApplicable => Some(s"'${c.id}' does not apply to this engine${note(capabilities, c)}")
      case Support.Unsupported if !c.enforcesContract => Some(s"'${c.id}' is declared unsupported${note(capabilities, c)}")
      case _ => None
    }

    val needed = scenario.operations.toList ++ scenario.focus.toList ++ CapabilityCheck.required(scenario.contract, scenario.options).map(_._1)
    needed.distinct.flatMap(unavailable).headOption match {
      case Some(reason) => Left(reason)
      case None =>
        if (CapabilityCheck.violations(scenario.contract, scenario.options, capabilities).nonEmpty) Right(Expected.RejectedAsUnsupported)
        else
          Right(scenario.expect match {
            case Expectation.Pass          => Expected.Pass
            case Expectation.Reject(types) => Expected.RejectExactly(types)
          })
    }
  }

  /** What an adapter is held to for one scenario. */
  sealed trait Expected
  object Expected {
    case object Pass extends Expected
    final case class RejectExactly(types: Set[String]) extends Expected

    /** The adapter declared a capability the contract needs unsupported: it must reject with
      * `UNSUPPORTED_CONTRACT_FEATURE` (other violations may accompany it). */
    case object RejectedAsUnsupported extends Expected
  }

  /** Compares what the adapter did with what it must have done. */
  def judge(
      expected: Expected,
      outcome: ScenarioOutcome,
      capabilities: AdapterCapabilities,
      expectNonDeterministic: Option[Set[String]] = None
  ): ScenarioVerdict = {
    val verdict: Option[String] = (expected, outcome) match {
      case (Expected.Pass, _: ScenarioOutcome.Passed) => None
      case (Expected.Pass, r: ScenarioOutcome.Rejected) =>
        Some(s"expected the write to be allowed, but it was blocked with ${show(r.violationTypes)}")
      case (Expected.RejectExactly(types), r: ScenarioOutcome.Rejected) =>
        if (r.violationTypes == types) None
        else Some(s"expected the write to be blocked with exactly ${show(types)}, got ${show(r.violationTypes)}")
      case (Expected.RejectExactly(types), _: ScenarioOutcome.Passed) =>
        Some(s"expected the write to be blocked with ${show(types)}, but it was allowed - a requirement went unchecked")
      case (Expected.RejectedAsUnsupported, r: ScenarioOutcome.Rejected) =>
        if (r.violationTypes.contains(ViolationType.UnsupportedContractFeature)) None
        else Some(s"the adapter declares a needed capability unsupported, so it must block with UNSUPPORTED_CONTRACT_FEATURE; got ${show(r.violationTypes)}")
      case (Expected.RejectedAsUnsupported, _: ScenarioOutcome.Passed) =>
        Some("the adapter declares a capability the contract needs unsupported, yet the write was allowed - it must fail closed")
    }
    verdict match {
      case Some(reason) => ScenarioVerdict.Diverges(reason)
      case None =>
        val reports = Set[Support](Support.Supported, Support.Partial).contains(capabilities.supportOf(Capability.ReportingNotifications))
        val wanted = if (expected == Expected.Pass) List("PASSED") else List("FAILED")
        if (reports && outcome.statuses != wanted) {
          ScenarioVerdict.Diverges(
            s"the adapter declares reporting.notifications ${capabilities.supportOf(Capability.ReportingNotifications).id} and must publish exactly " +
              s"${wanted.mkString("[", ", ", "]")} validation event(s); it published ${outcome.statuses.mkString("[", ", ", "]")}"
          )
        } else
          expectNonDeterministic match {
            case Some(columns) if expected == Expected.Pass && outcome.nonDeterministicColumns != columns =>
              ScenarioVerdict.Diverges(
                s"the fingerprint must report exactly ${show(columns)} non-deterministic; it reported ${show(outcome.nonDeterministicColumns)} - an engine function name did not reach the catalog"
              )
            case _ => ScenarioVerdict.Conforms
          }
    }
  }

  /** Runs every scenario on `adapter` and compares it with what its declaration promises. An
    * exception out of `adapter.run` is a divergence, not a verdict: only the engine's own
    * rejection is one.
    */
  def evaluate(adapter: ConformanceAdapter, scenarios: List[Scenario] = Scenarios.all): ConformanceReport = {
    val caps = adapter.capabilities
    val results = scenarios.map(s => ScenarioResult(s, evaluateOne(adapter, caps, s)))
    ConformanceReport(caps.adapter, results, unverifiedClaims(caps))
  }

  def evaluateOne(adapter: ConformanceAdapter, caps: AdapterCapabilities, scenario: Scenario): ScenarioVerdict =
    expectationFor(scenario, caps) match {
      case Left(reason) => ScenarioVerdict.Skipped(reason)
      case Right(expected) =>
        try judge(expected, adapter.run(scenario.id, scenario.contract, scenario.job, scenario.options), caps, scenario.expectNonDeterministic)
        catch { case e: Exception => ScenarioVerdict.Diverges(s"the adapter threw ${e.getClass.getName}: ${e.getMessage}") }
    }

  /** The capabilities the adapter claims (`supported`/`partial`) that no scenario is evidence for. */
  def unverifiedClaims(caps: AdapterCapabilities): List[Capability] =
    Capability.all.filter(c => !Scenarios.covered.contains(c) && Set[Support](Support.Supported, Support.Partial).contains(caps.supportOf(c)))

  private def show(types: Set[String]): String = types.toList.sorted.mkString("{", ", ", "}")

  private def note(caps: AdapterCapabilities, c: Capability): String = caps.entryOf(c).note.map(n => s": $n").getOrElse("")
}
