// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.duration._

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

  /** How long one scenario may take before it is reported as a divergence naming it, instead of holding the build
    * hostage. Generous by default (a scenario starts engine jobs); raise it for a slow engine. `None` runs a scenario
    * on the test's own thread with no limit, for an adapter that needs thread affinity (a thread-local engine session):
    * the scenario then runs exactly as `Conformance.evaluateOne` does, and a hang is the build's to time out.
    */
  protected def scenarioTimeLimit: Option[FiniteDuration] = Some(10.minutes)

  /** Whether a scenario the engine's environment could not run (`EnvironmentUnavailable`) fails instead of being
    * canceled. Off by default, so a developer without the engine's service still gets a run; set
    * `INVARACT_CONFORMANCE_STRICT=true` where the environment is supposed to be there (CI), so a build whose
    * environment silently went missing cannot pass by skipping everything.
    */
  protected def strictEnvironment: Boolean = sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true")

  /** Where this run's results are written as JSON (`<adapter>.json`), or `None` to write nothing. */
  protected def resultsDirectory: Option[File] = Some(new File("target/conformance"))

  private val verdicts = new ConcurrentHashMap[String, ScenarioVerdict]()

  Scenarios.all.foreach { scenario =>
    test(s"conformance: ${scenario.id} - ${scenario.description}") {
      val verdict = scenarioTimeLimit match {
        case Some(limit) => Conformance.evaluateWithin(adapter, adapter.capabilities, scenario, limit)
        case None        => Conformance.evaluateOne(adapter, adapter.capabilities, scenario)
      }
      verdicts.put(scenario.id, verdict)
      verdict match {
        case ScenarioVerdict.Conforms         => succeed
        case ScenarioVerdict.Skipped(reason)  => cancel(s"does not apply to this adapter: $reason")
        case ScenarioVerdict.Diverges(reason) => fail(reason)
        case ScenarioVerdict.Unavailable(reason) =>
          if (strictEnvironment) fail(s"the engine's environment is unavailable, and this run requires it: $reason")
          else cancel(s"the engine's environment is unavailable, so nothing was checked: $reason")
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

  test("baseline: every scenario on the checked-in baseline still conforms, and every one that conforms is on it") {
    val recorded = verdicts.asScala.toMap
    if (recorded.size < Scenarios.all.size) cancel(s"only ${recorded.size} of ${Scenarios.all.size} scenarios ran, so the run is not compared with the baseline")
    val name = adapter.capabilities.adapter
    resultsDirectory.foreach { dir =>
      Files.createDirectories(dir.toPath)
      Files.write(new File(dir, s"$name.json").toPath, ConformanceBaseline.resultsJson(name, recorded).getBytes(StandardCharsets.UTF_8))
    }
    ConformanceBaseline.load(name, getClass.getClassLoader) match {
      case Left(why) =>
        fail(s"$why Create it with:\n${ConformanceBaseline.body(name, recorded)}")
      case Right(baseline) =>
        val problems = ConformanceBaseline.problems(baseline, recorded)
        assert(problems.isEmpty, problems.mkString("\n") + s"\nThe baseline for this run would be:\n${ConformanceBaseline.body(name, recorded)}")
    }
  }
}
