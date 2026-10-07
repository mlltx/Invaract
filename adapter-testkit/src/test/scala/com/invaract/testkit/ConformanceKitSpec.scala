// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability, CapabilityCheck, ViolationType}

import org.scalatest.funsuite.AnyFunSuite

/** The kit tested against itself: the reference adapter (built on the SPI, no engine) must pass
  * every scenario - which proves the scenarios are consistent - and deliberately dishonest adapters
  * must be caught, which proves the kit can fail.
  */
class ConformanceKitSpec extends AnyFunSuite {

  // --- the catalogue itself -----------------------------------------------------------------------

  test("scenario ids are unique") {
    val ids = Scenarios.all.map(_.id)
    assert(ids.distinct == ids)
  }

  test("every capability is either the focus of a scenario or listed, with a reason, as not covered - never both, never neither") {
    val everything = Capability.all.toSet
    val covered = Scenarios.covered
    val uncovered = Scenarios.notCovered.keySet
    assert(covered.intersect(uncovered).isEmpty, s"both covered and listed as not covered: ${covered.intersect(uncovered).map(_.id)}")
    assert((covered ++ uncovered) == everything, s"neither: ${(everything -- covered -- uncovered).map(_.id)}")
    assert(Scenarios.notCovered.values.forall(_.trim.nonEmpty))
  }

  test("a scenario's focus is something its own contract or operations actually exercise") {
    Scenarios.all.foreach { s =>
      val relied = CapabilityCheck.required(s.contract, s.options).map(_._1).toSet
      s.focus.foreach { c =>
        assert(
          relied.contains(c) || s.operations.contains(c) || c == Capability.ReportingNotifications,
          s"scenario '${s.id}' names '${c.id}' as its focus, but nothing in it relies on that capability"
        )
      }
    }
  }

  // --- the reference adapter: no engine, the SPI only --------------------------------------------

  private val reference = new ReferenceAdapter

  test("the reference adapter conforms to every scenario, skipping none") {
    val report = Conformance.evaluate(reference)
    assert(report.failures.map(r => r.scenario.id -> r.verdict) == Nil)
    assert(report.skipped == Nil)
    assert(report.conforming.size == Scenarios.all.size)
  }

  test("the reference adapter's declaration parses and names itself") {
    assert(ReferenceAdapter.declared.adapter == "reference")
  }

  test("the reference adapter's claims the kit cannot verify are exactly its supported capabilities outside the catalogue") {
    // It declares only what the scenarios cover as supported, so nothing is left unverified.
    assert(Conformance.unverifiedClaims(reference.capabilities) == Nil)
  }

  // --- the kit must be able to fail --------------------------------------------------------------

  /** Wraps an adapter, changing what it is handed or what it reports - a stand-in for a real adapter's bug. */
  private class Tampered(
      val delegate: ConformanceAdapter,
      contractOf: com.invaract.contract.Contract => com.invaract.contract.Contract = identity,
      outcomeOf: ScenarioOutcome => ScenarioOutcome = identity
  ) extends ConformanceAdapter {
    override def capabilities: AdapterCapabilities = delegate.capabilities
    override def run(id: String, c: com.invaract.contract.Contract, j: ScenarioJob, o: com.invaract.verification.VerificationOptions): ScenarioOutcome =
      outcomeOf(delegate.run(id, contractOf(c), j, o))
  }

  private def failingIds(adapter: ConformanceAdapter): Set[String] = Conformance.evaluate(adapter).failures.map(_.scenario.id).toSet

  test("an adapter that claims check.format but never checks it is caught on exactly the format scenario that needs it") {
    val liar = new Tampered(reference, contractOf = c => c.copy(outputs = c.outputs.map(_.copy(format = None))))
    assert(failingIds(liar) == Set("output-format-mismatch"))
  }

  test("an adapter that claims check.saveMode but never checks it is caught") {
    val liar = new Tampered(reference, contractOf = c => c.copy(outputs = c.outputs.map(_.copy(saveMode = None))))
    assert(failingIds(liar) == Set("output-save-mode-mismatch"))
  }

  test("an adapter that lets everything through is caught on every scenario that expects a rejection") {
    val permissive = new Tampered(reference, outcomeOf = o => ScenarioOutcome.Passed(o.statuses))
    val expectingReject = Scenarios.all.filter(_.expect.isInstanceOf[Expectation.Reject]).map(_.id).toSet
    assert(failingIds(permissive) == expectingReject)
  }

  test("an adapter that blocks everything is caught on every scenario that expects a pass") {
    val paranoid = new Tampered(reference, outcomeOf = o => ScenarioOutcome.Rejected(Set(ViolationType.MissingOutput), o.statuses))
    val expectingPass = Scenarios.all.filter(_.expect == Expectation.Pass).map(_.id).toSet
    val expectingOtherReject = Scenarios.all.filter(s => s.expect == Expectation.Reject(Set(ViolationType.MissingOutput))).map(_.id).toSet
    assert(failingIds(paranoid) == Scenarios.all.map(_.id).toSet -- expectingOtherReject)
    assert(expectingPass.subsetOf(failingIds(paranoid)))
  }

  test("an adapter that claims notifications but publishes none is caught on every scenario") {
    val silent = new Tampered(reference, outcomeOf = {
      case p: ScenarioOutcome.Passed   => p.copy(statuses = Nil)
      case r: ScenarioOutcome.Rejected => r.copy(statuses = Nil)
    })
    assert(failingIds(silent) == Scenarios.all.map(_.id).toSet)
  }

  test("an adapter that throws instead of reporting a verdict is a divergence, not a crash of the kit") {
    val crashing = new ConformanceAdapter {
      override def capabilities: AdapterCapabilities = reference.capabilities
      override def run(id: String, c: com.invaract.contract.Contract, j: ScenarioJob, o: com.invaract.verification.VerificationOptions): ScenarioOutcome =
        throw new IllegalStateException("engine exploded")
    }
    val report = Conformance.evaluate(crashing)
    assert(report.failures.size == Scenarios.all.size)
    assert(report.failures.forall(_.verdict.asInstanceOf[ScenarioVerdict.Diverges].reason.contains("engine exploded")))
  }

  // --- honest declarations are held to what they say -----------------------------------------------

  private def declaring(overrides: Map[String, (String, Option[String])]): AdapterCapabilities = {
    val base = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    val lines = base.split("\n").toList
    val rewritten = new StringBuilder
    var skip = false
    lines.foreach { line =>
      val key = line.trim.stripSuffix(":")
      if (line.startsWith("  ") && !line.startsWith("    ") && line.trim.endsWith(":")) {
        overrides.get(key) match {
          case Some((status, note)) =>
            rewritten.append(s"  $key:\n    status: $status\n")
            note.foreach(n => rewritten.append(s"    note: $n\n"))
            skip = true
          case None => rewritten.append(line + "\n"); skip = false
        }
      } else if (!skip) rewritten.append(line + "\n")
    }
    AdapterCapabilities.parse(rewritten.toString).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }

  test("an adapter that honestly declares check.format unsupported must block a contract that declares a format, with UNSUPPORTED_CONTRACT_FEATURE") {
    val honest = new ReferenceAdapter(declaring(Map("check.format" -> (("unsupported", Some("this engine has no format concept"))))))
    val report = Conformance.evaluate(honest)
    assert(report.failures == Nil)
    // The two format scenarios now expect the loud rejection, not the structural verdict.
    val formatResults = report.results.filter(_.scenario.focus.contains(Capability.CheckFormat))
    assert(formatResults.size == 2 && formatResults.forall(_.verdict == ScenarioVerdict.Conforms))
  }

  test("declaring a capability unsupported does not excuse an adapter that quietly passes anyway") {
    val declared = declaring(Map("check.format" -> (("unsupported", Some("no formats")))))
    val permissive = new Tampered(new ReferenceAdapter(declared), outcomeOf = o => ScenarioOutcome.Passed(o.statuses))
    assert(failingIds(permissive).contains("output-format-mismatch"))
    assert(failingIds(permissive).contains("output-format-matches"))
  }

  test("a scenario that needs something the engine does not have is skipped with the adapter's own reason, not failed") {
    val noFilter = new ReferenceAdapter(declaring(Map("write.batch" -> (("not-applicable", Some("the engine only streams"))))))
    val report = Conformance.evaluate(noFilter)
    assert(report.failures == Nil)
    assert(report.conforming == Nil)
    assert(report.skipped.size == Scenarios.all.size)
    assert(report.skipped.forall(_.verdict.asInstanceOf[ScenarioVerdict.Skipped].reason.contains("the engine only streams")))
  }

  test("an unsupported capability that does not enforce a contract requirement skips the scenario rather than demanding a rejection") {
    val noRead = new ReferenceAdapter(declaring(Map("read.batch" -> (("unsupported", Some("sources are push-only"))))))
    val report = Conformance.evaluate(noRead)
    assert(report.failures == Nil)
    assert(report.skipped.size == Scenarios.all.size)
  }

  test("an adapter claiming a capability the kit has no scenario for has it listed as unverified") {
    val claims = declaring(Map("analysis.fingerprint" -> (("supported", None))))
    assert(Conformance.unverifiedClaims(claims) == List(Capability.AnalysisFingerprint))
  }
}
