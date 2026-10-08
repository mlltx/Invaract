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

  test("attested and gaps are disjoint, and together are exactly notCovered") {
    assert(Scenarios.attested.keySet.intersect(Scenarios.gaps.keySet).isEmpty)
    assert(Scenarios.notCovered == (Scenarios.attested ++ Scenarios.gaps))
    assert(Scenarios.attested.nonEmpty)
  }

  test("a report lists an adapter's unchecked claims as attested (a job cannot check them) or gaps (the kit cannot yet)") {
    val claims = declaring(Map("config.zeroCodeInstall" -> (("supported", None)), "config.locationRefs" -> (("partial", Some("only maps"))))) 
    val report = Conformance.evaluate(new ReferenceAdapter(claims))
    assert(report.unverifiedClaims.toSet == Set(Capability.ConfigZeroCodeInstall, Capability.ConfigLocationRefs))
    assert(report.attestedClaims.toSet == Set(Capability.ConfigZeroCodeInstall, Capability.ConfigLocationRefs))
    // The kit has no gap today: every capability a job can show is shown by a scenario.
    assert(Scenarios.gaps.isEmpty && report.gapClaims.isEmpty)
  }

  // Capabilities no contract "relies on" (they describe what the engine reports, not what a contract asks for).
  // Lineage, sensitivity and boundary resolution describe what an adapter's translation preserves, not what a contract asks for.
  private val notDerivable = Set(
    Capability.ReportingNotifications, Capability.AnalysisFunctionCatalog, Capability.FailClosedUnverifiableWrites,
    Capability.LineageColumnLevel, Capability.AnalysisSensitivityPropagation, Capability.LineageBoundaryResolution
  )

  test("a scenario's focus is something its own contract or operations actually exercise") {
    Scenarios.all.foreach { s =>
      val relied = CapabilityCheck.required(s.contract, s.options).map(_._1).toSet
      s.focus.foreach { c =>
        assert(
          relied.contains(c) || s.operations.contains(c) || notDerivable.contains(c),
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
    override def translation(id: String, j: ScenarioJob): Option[com.invaract.ir.Plan] = delegate.translation(id, j)
  }

  private def failingIds(adapter: ConformanceAdapter): Set[String] = Conformance.evaluate(adapter).failures.map(_.scenario.id).toSet

  test("an adapter that claims check.format but never checks it is caught on exactly the format scenario that needs it") {
    val liar = new Tampered(reference, contractOf = c => c.copy(outputs = c.outputs.map(_.copy(format = None))))
    assert(failingIds(liar) == Set("output-format-mismatch"))
  }

  test("an adapter that lets an untranslatable write through unchecked is caught on exactly the fail-closed scenario") {
    val letsItThrough = new Tampered(
      reference,
      outcomeOf = {
        case r: ScenarioOutcome.Rejected if r.violationTypes == Set(com.invaract.verification.ViolationType.UnverifiableWrite) => ScenarioOutcome.Passed(Nil)
        case other => other
      }
    )
    assert(failingIds(letsItThrough) == Set("untranslatable-write-fails-closed"))
  }

  test("an adapter that declares fail-closed unsupported has the scenario skipped, not failed") {
    val caps = declaring(Map("failClosed.unverifiableWrites" -> (("unsupported", Some("not implemented")))))
    val report = Conformance.evaluate(new ReferenceAdapter(caps))
    assert(report.skipped.map(_.scenario.id) == List("untranslatable-write-fails-closed"))
    assert(report.failures.isEmpty)
  }

  test("an adapter that claims check.saveMode but never checks it is caught") {
    val liar = new Tampered(reference, contractOf = c => c.copy(outputs = c.outputs.map(_.copy(saveMode = None))))
    assert(failingIds(liar) == Set("output-save-mode-mismatch"))
  }

  test("an adapter that lets everything through is caught on every scenario that expects a rejection") {
    val permissive = new Tampered(reference, outcomeOf = o => ScenarioOutcome.Passed(o.statuses, o.nonDeterministicColumns, o.dataQuality, o.roles, o.unverifiableInputs))
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

  test("an adapter whose engine function names never reach the catalog is caught: its generated id is not reported non-deterministic") {
    val forgetsAliases = new Tampered(reference, outcomeOf = {
      case p: ScenarioOutcome.Passed   => p.copy(nonDeterministicColumns = Set.empty)
      case r: ScenarioOutcome.Rejected => r.copy(nonDeterministicColumns = Set.empty)
    })
    assert(failingIds(forgetsAliases) == Set("fingerprint-flags-a-generated-id"))
  }

  test("an adapter that flags everything non-deterministic is caught on the deterministic job too") {
    val overeager = new Tampered(reference, outcomeOf = {
      case p: ScenarioOutcome.Passed   => p.copy(nonDeterministicColumns = Set("id", "total", "token"))
      case r: ScenarioOutcome.Rejected => r
    })
    assert(failingIds(overeager) == Set("fingerprint-flags-nothing-for-a-deterministic-job", "fingerprint-flags-a-generated-id"))
  }

  test("an adapter that exposes no translation diverges on exactly the scenarios that read the plan, and nowhere else") {
    val noPlan = new Tampered(reference) {
      override def translation(id: String, j: ScenarioJob): Option[com.invaract.ir.Plan] = None
    }
    assert(failingIds(noPlan) == Scenarios.all.filter(_.analysis.needsTranslation).map(_.id).toSet)
    assert(Scenarios.all.exists(_.analysis.needsTranslation))
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
    // the row-level DML scenarios need a different operation than a batch write, so they still run, and conform
    val needsBatchWrite = Scenarios.all.filter(_.operations.contains(Capability.WriteBatch))
    assert(report.skipped.map(_.scenario.id).toSet == needsBatchWrite.map(_.id).toSet)
    assert(report.conforming.map(_.scenario.id).toSet == Scenarios.all.filterNot(_.operations.contains(Capability.WriteBatch)).map(_.id).toSet)
    assert(report.conforming.nonEmpty)
    assert(report.skipped.forall(_.verdict.asInstanceOf[ScenarioVerdict.Skipped].reason.contains("the engine only streams")))
  }

  test("an unsupported capability that does not enforce a contract requirement skips the scenario rather than demanding a rejection") {
    val noRead = new ReferenceAdapter(declaring(Map("read.batch" -> (("unsupported", Some("sources are push-only"))))))
    val report = Conformance.evaluate(noRead)
    assert(report.failures == Nil)
    // every scenario that reads is skipped; those that need no batch read (fail-closed, row-level DML, streaming) still run, and conform
    assert(report.skipped.map(_.scenario.id).toSet == Scenarios.all.filter(_.operations.contains(Capability.ReadBatch)).map(_.id).toSet)
    assert(
      report.conforming.map(_.scenario.id).toSet ==
        Set(
          "untranslatable-write-fails-closed", "row-level-delete-checked-as-write", "row-level-delete-own-table-is-not-an-input",
          "unconditional-delete-forbidden", "filtered-delete-allowed",
          "streaming-write-is-checked-like-a-batch-write", "streaming-write-to-the-wrong-location"
        )
    )
  }

  test("an adapter claiming a capability the kit has no scenario for has it listed as unverified") {
    val claims = declaring(Map("config.contractRegistry" -> (("supported", None))))
    assert(Conformance.unverifiedClaims(claims) == List(Capability.ConfigContractRegistry))
  }

  // --- the row-level DML and catalog scenarios must be able to fail too ------------------------

  test("an adapter that lets an unconditional DELETE through is caught on exactly the DELETE rule scenario") {
    val letsItThrough = new Tampered(
      reference,
      outcomeOf = {
        case r: ScenarioOutcome.Rejected if r.violationTypes == Set(ViolationType.RuleUnconditionalDelete) => ScenarioOutcome.Passed(r.statuses)
        case other => other
      }
    )
    assert(failingIds(letsItThrough) == Set("unconditional-delete-forbidden"))
  }

  test("an adapter that wrongly rejects a filtered DELETE is caught on exactly that scenario") {
    val rejectsFilteredDelete = new ConformanceAdapter {
      override def capabilities: AdapterCapabilities = reference.capabilities
      override def run(id: String, c: com.invaract.contract.Contract, j: ScenarioJob, o: com.invaract.verification.VerificationOptions): ScenarioOutcome =
        if (id == "filtered-delete-allowed") ScenarioOutcome.Rejected(Set(ViolationType.RuleUnconditionalDelete), List("FAILED"))
        else reference.run(id, c, j, o)
      override def translation(id: String, j: ScenarioJob): Option[com.invaract.ir.Plan] = reference.translation(id, j)
    }
    assert(failingIds(rejectsFilteredDelete) == Set("filtered-delete-allowed"))
  }

  test("an adapter that never checks catalog registration is caught on exactly the scenario that requires it") {
    val liar = new Tampered(reference, contractOf = c => c.copy(outputs = c.outputs.map(_.copy(catalog = None))))
    assert(failingIds(liar) == Set("catalog-required-output-unregistered"))
  }

  test("an adapter that declares catalog registration unsupported conforms by failing closed on both catalog scenarios") {
    val caps = declaring(Map("check.catalogRegistration" -> (("unsupported", Some("no catalogs")))))
    val honest = Conformance.evaluate(new ReferenceAdapter(caps))
    assert(honest.failures == Nil)
    assert(Set("catalog-required-output-unregistered", "catalog-required-output-registered").subsetOf(honest.conforming.map(_.scenario.id).toSet))
    // ...and one that says so but lets the registered write through unchecked is caught on both.
    val lying = new Tampered(new ReferenceAdapter(caps), outcomeOf = o => ScenarioOutcome.Passed(o.statuses))
    assert(Set("catalog-required-output-unregistered", "catalog-required-output-registered").subsetOf(failingIds(lying)))
  }

  // --- the reference adapter's own translation --------------------------------------------------

  private def scenario(id: String): Scenario = Scenarios.all.find(_.id == id).getOrElse(fail(s"no scenario '$id'"))

  test("a row-level change reads no declared input: the job's inputs are not offered as read, so rejectUndeclaredInputs does not trip") {
    val s = scenario("filtered-delete-allowed")
    val strict = com.invaract.verification.VerificationOptions(rejectUndeclaredInputs = true)
    assert(reference.run(s.id, s.contract, s.job, strict).isInstanceOf[ScenarioOutcome.Passed])
  }

  test("a transform job does offer its inputs' schemas: a declared input type that differs from the dataset's is blocked") {
    val s = scenario("conforming-write")
    val wrongInputType = s.contract.copy(inputs = s.contract.inputs.map(d =>
      d.copy(schema = d.schema.copy(fields = d.schema.fields.map(f => if (f.name == "id") f.copy(fieldType = "string") else f)))
    ))
    reference.run(s.id, wrongInputType, s.job, s.options) match {
      case r: ScenarioOutcome.Rejected => assert(r.violationTypes == Set(ViolationType.InputFieldTypeMismatch))
      case other                       => fail(s"expected a rejection, got $other")
    }
  }

  test("the reference adapter compares names case-insensitively, as Spark does by default") {
    val s = scenario("conforming-write")
    val upperCased = s.contract.copy(outputs = s.contract.outputs.map(d => d.copy(schema = d.schema.copy(fields = d.schema.fields.map(f => f.copy(name = f.name.toUpperCase))))))
    assert(reference.run(s.id, upperCased, s.job, s.options).isInstanceOf[ScenarioOutcome.Passed])
  }

  test("an adapter that declares a capability unsupported but blocks for some other reason is a divergence naming UNSUPPORTED_CONTRACT_FEATURE") {
    val caps = declaring(Map("check.nestedTypes" -> (("unsupported", Some("flat schemas only")))))
    val wrongReason = new Tampered(
      new ReferenceAdapter(caps),
      outcomeOf = o => ScenarioOutcome.Rejected(Set(ViolationType.OutputFieldTypeMismatch), o.statuses)
    )
    val nested = Conformance.evaluate(wrongReason).failures.filter(_.scenario.id.startsWith("nested-type"))
    assert(nested.nonEmpty)
    nested.foreach(r => assert(r.verdict.asInstanceOf[ScenarioVerdict.Diverges].reason.contains("UNSUPPORTED_CONTRACT_FEATURE"), r.scenario.id))
  }
}
