// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.Contract
import com.invaract.verification.{AdapterCapabilities, VerificationOptions}

import org.scalatest.{Args, DoNotDiscover}
import org.scalatest.events.{Event, TestCanceled, TestFailed, TestSucceeded}
import org.scalatest.funsuite.AnyFunSuite

/** An adapter whose engine's environment is missing: every run says so (or, for `unavailableFor`, only those scenarios). */
class EnvironmentlessAdapter(unavailableFor: String => Boolean = _ => true) extends ConformanceAdapter {
  private val reference = new ReferenceAdapter
  override def capabilities: AdapterCapabilities = reference.capabilities
  override def run(id: String, c: Contract, j: ScenarioJob, o: VerificationOptions): ScenarioOutcome =
    if (unavailableFor(id)) throw new EnvironmentUnavailable(s"the emulator for $id would not start")
    else reference.run(id, c, j, o)
  override def translation(id: String, j: ScenarioJob): Option[com.invaract.ir.Plan] =
    if (unavailableFor(id)) throw new EnvironmentUnavailable(s"no connection to translate $id") else reference.translation(id, j)
}

@DoNotDiscover
class LenientEnvironmentSpec extends AdapterConformanceSpec {
  override protected def adapter: ConformanceAdapter = new EnvironmentlessAdapter()
  override protected def strictEnvironment: Boolean = false
}

@DoNotDiscover
class StrictEnvironmentSpec extends AdapterConformanceSpec {
  override protected def adapter: ConformanceAdapter = new EnvironmentlessAdapter()
  override protected def strictEnvironment: Boolean = true
}

@DoNotDiscover
class ExposesStrictness extends AdapterConformanceSpec {
  override protected def adapter: ConformanceAdapter = new ReferenceAdapter
  def strict: Boolean = strictEnvironment
}

class EnvironmentUnavailableTest extends AnyFunSuite {

  private val first = Scenarios.all.head
  private val translated = Scenarios.all.find(_.analysis.needsTranslation).get

  private def events(suite: org.scalatest.Suite): List[Event] = {
    val seen = scala.collection.mutable.ListBuffer.empty[Event]
    suite.run(None, Args(reporter = (e: Event) => seen.synchronized { seen += e }))
    seen.toList
  }

  test("an adapter that cannot reach its engine is Unavailable with its reason - not a divergence, and not a pass") {
    val adapter = new EnvironmentlessAdapter
    val verdict = Conformance.evaluateOne(adapter, adapter.capabilities, first)
    assert(verdict == ScenarioVerdict.Unavailable(s"the emulator for ${first.id} would not start"))
    assert(verdict != ScenarioVerdict.Conforms)
  }

  test("it is Unavailable when the environment is missing during translation too") {
    val adapter = new EnvironmentlessAdapter(_ => false) {
      override def translation(id: String, j: ScenarioJob): Option[com.invaract.ir.Plan] = throw new EnvironmentUnavailable("no connection")
    }
    assert(Conformance.evaluateOne(adapter, adapter.capabilities, translated) == ScenarioVerdict.Unavailable("no connection"))
  }

  test("any other exception is still a divergence, so only an adapter that says it is the environment is excused") {
    val broken = new ConformanceAdapter {
      override def capabilities: AdapterCapabilities = new ReferenceAdapter().capabilities
      override def run(id: String, c: Contract, j: ScenarioJob, o: VerificationOptions): ScenarioOutcome =
        throw new java.net.ConnectException("connection refused")
    }
    val verdict = Conformance.evaluateOne(broken, broken.capabilities, first)
    assert(verdict.isInstanceOf[ScenarioVerdict.Diverges])
    assert(verdict.asInstanceOf[ScenarioVerdict.Diverges].reason.contains("connection refused"))
  }

  test("a bounded run reports an unavailable environment the same way") {
    import scala.concurrent.duration._
    val adapter = new EnvironmentlessAdapter
    assert(Conformance.evaluateWithin(adapter, adapter.capabilities, first, 1.minute) == Conformance.evaluateOne(adapter, adapter.capabilities, first))
  }

  test("the report lists unavailable scenarios apart from failures, skips and passes") {
    val adapter = new EnvironmentlessAdapter(_ == first.id)
    val report = Conformance.evaluate(adapter)
    assert(report.unavailable.map(_.scenario.id) == List(first.id))
    assert(report.failures == Nil)
    assert(!report.conforming.exists(_.scenario.id == first.id))
    assert(report.conforming.size + report.skipped.size + report.unavailable.size == Scenarios.all.size)
  }

  test("by default the suite cancels each unavailable scenario with the reason, so the build shows it and still passes") {
    val reported = events(new LenientEnvironmentSpec)
    val scenarioTests = reported.collect { case c: TestCanceled if c.testName.startsWith("conformance: ") => c }
    assert(scenarioTests.size == Scenarios.all.size)
    assert(scenarioTests.forall(c => c.message.contains("environment is unavailable") && c.message.contains("would not start")))
    assert(!reported.exists(_.isInstanceOf[TestFailed]))
  }

  test("in strict mode each unavailable scenario fails, so a missing environment cannot pass a build that requires it") {
    val reported = events(new StrictEnvironmentSpec)
    val failed = reported.collect { case f: TestFailed if f.testName.startsWith("conformance: ") => f }
    assert(failed.size == Scenarios.all.size)
    assert(failed.forall(f => f.message.contains("requires it") && f.message.contains("would not start")))
    assert(!reported.exists(e => e.isInstanceOf[TestSucceeded] && e.asInstanceOf[TestSucceeded].testName.startsWith("conformance: " + first.id)))
  }

  test("strict mode is read from INVARACT_CONFORMANCE_STRICT and is off when it is not set") {
    val spec = new ExposesStrictness
    assert(spec.strict == sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true"))
  }
}
