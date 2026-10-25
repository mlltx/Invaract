// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.AdapterCapabilities

import org.scalatest.{Args, DoNotDiscover}
import org.scalatest.events.{Event, TestFailed, TestSucceeded}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** The reference adapter with one capability declared away, so scenarios that conformed on its baseline are skipped. */
@DoNotDiscover
class RegressedReferenceSpec extends AdapterConformanceSpec {
  override protected def resultsDirectory: Option[java.io.File] = None
  private val caps = {
    val base = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    // write.batch not-applicable skips every scenario that needs a batch write
    val rewritten = base.replaceAll("(?s)(  write\\.batch:\\n    status: )\\w[\\w-]*", "$1not-applicable\n    note: the engine only streams")
    AdapterCapabilities.parse(rewritten).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }
  override protected def adapter: ConformanceAdapter = new ReferenceAdapter(caps)
}

/** An adapter with no baseline among the test resources. */
@DoNotDiscover
class UnbaselinedSpec extends AdapterConformanceSpec {
  override protected def resultsDirectory: Option[java.io.File] = None
  private val caps = {
    val base = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    AdapterCapabilities.parse(base.replace("adapter: reference", "adapter: nobaseline")).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }
  override protected def adapter: ConformanceAdapter = new ReferenceAdapter(caps)
}

/** Writes its results where the test says. */
@DoNotDiscover
class ResultsWritingSpec(dir: java.io.File) extends AdapterConformanceSpec {
  override protected def resultsDirectory: Option[java.io.File] = Some(dir)
  override protected def adapter: ConformanceAdapter = new ReferenceAdapter
}

class ConformanceBaselineTest extends AnyFunSuite {

  private val ids = Scenarios.all.map(_.id)
  private val allConform: Map[String, ScenarioVerdict] = ids.map(_ -> (ScenarioVerdict.Conforms: ScenarioVerdict)).toMap

  private def events(suite: org.scalatest.Suite): List[Event] = {
    val seen = scala.collection.mutable.ListBuffer.empty[Event]
    suite.run(None, Args(reporter = (e: Event) => seen.synchronized { seen += e }))
    seen.toList
  }

  test("a baseline is one scenario id per line; blank lines and # comments are not ids") {
    assert(ConformanceBaseline.parse("# why\n\n  a  \nb\n   # indented comment\n") == Set("a", "b"))
    assert(ConformanceBaseline.parse("") == Set.empty[String])
  }

  test("the baseline is found by the adapter's name among the test resources, and a missing one says how to make it") {
    val reference = ConformanceBaseline.load("reference", getClass.getClassLoader)
    assert(reference == Right(ids.toSet))
    val missing = ConformanceBaseline.load("no-such-adapter", getClass.getClassLoader)
    assert(missing.left.exists(m => m.contains("conformance-baseline-no-such-adapter.txt") && m.contains("one id per line")))
  }

  test("a run that matches its baseline has no problems") {
    assert(ConformanceBaseline.problems(ids.toSet, allConform) == Nil)
  }

  test("a scenario on the baseline that is now skipped is a regression naming the scenario and why it was skipped") {
    val verdicts = allConform.updated(ids.head, ScenarioVerdict.Skipped("declared unsupported: no longer implemented"))
    val problems = ConformanceBaseline.problems(ids.toSet, verdicts)
    assert(problems.size == 1)
    assert(problems.head.startsWith(ids.head) && problems.head.contains("skipped") && problems.head.contains("no longer implemented"))
  }

  test("a scenario on the baseline that now diverges is reported too") {
    val verdicts = allConform.updated(ids(1), ScenarioVerdict.Diverges("it let the write through"))
    val problems = ConformanceBaseline.problems(ids.toSet, verdicts)
    assert(problems.size == 1 && problems.head.startsWith(ids(1)) && problems.head.contains("diverges") && problems.head.contains("let the write through"))
  }

  test("a scenario that conforms but is not on the baseline must be added to it, all of them listed") {
    val baseline = ids.toSet - ids(2) - ids(3)
    val problems = ConformanceBaseline.problems(baseline, allConform)
    assert(problems.size == 1 && problems.head.contains(s"${ids(2)}, ${ids(3)}") && problems.head.contains("add them"))
  }

  test("a scenario that is not on the baseline and does not conform is nobody's business") {
    val baseline = ids.toSet - ids(2)
    val verdicts = allConform.updated(ids(2), ScenarioVerdict.Skipped("not applicable"))
    assert(ConformanceBaseline.problems(baseline, verdicts) == Nil)
  }

  test("a scenario the environment could not run is judged neither way") {
    val verdicts = allConform.updated(ids.head, ScenarioVerdict.Unavailable("emulator down"))
    assert(ConformanceBaseline.problems(ids.toSet, verdicts) == Nil)
    assert(ConformanceBaseline.problems(ids.toSet - ids.head, verdicts) == Nil)
  }

  test("a baseline line that names no scenario is reported, so a renamed scenario cannot leave a dead entry") {
    val problems = ConformanceBaseline.problems(ids.toSet + "renamed-away", allConform)
    assert(problems == List("the baseline names 'renamed-away', which is not a scenario"))
  }

  test("the body to paste lists exactly the conforming scenarios, in the catalogue's order, under a comment") {
    val verdicts = allConform.updated(ids(1), ScenarioVerdict.Skipped("x")).updated(ids(2), ScenarioVerdict.Diverges("y"))
    val body = ConformanceBaseline.body("acme", verdicts)
    assert(body.startsWith("# The scenarios acme conforms to"))
    assert(ConformanceBaseline.parse(body) == ids.toSet - ids(1) - ids(2))
    assert(body.split("\n").filterNot(_.startsWith("#")).toList == ids.filterNot(Set(ids(1), ids(2))))
    assert(body.endsWith("\n"))
  }

  test("results are JSON with every scenario's verdict and the reason where there is one, escaped") {
    val verdicts = allConform
      .updated(ids(0), ScenarioVerdict.Skipped("not \"applicable\"\nhere"))
      .updated(ids(1), ScenarioVerdict.Diverges("bad\\path\ttab"))
      .updated(ids(2), ScenarioVerdict.Unavailable("emulator down"))
    val json = ConformanceBaseline.resultsJson("acme", verdicts)
    assert(json.contains(""""adapter": "acme""""))
    assert(json.contains(s""""scenario": "${ids(0)}", "verdict": "skipped", "reason": "not \\"applicable\\"\\nhere""""))
    assert(json.contains(s""""scenario": "${ids(1)}", "verdict": "diverges", "reason": "bad\\\\path\\ttab""""))
    assert(json.contains(s""""scenario": "${ids(2)}", "verdict": "unavailable", "reason": "emulator down""""))
    assert(json.contains(s""""scenario": "${ids(3)}", "verdict": "conforms"}"""))
    assert(""""scenario":""".r.findAllIn(json.replace("\": ", "\":")).size == ids.size)
    assert(!json.contains(s""""scenario": "${ids(3)}", "verdict": "conforms", "reason""""))
  }

  test("a verdict missing from a partial run is left out of the results rather than invented") {
    val json = ConformanceBaseline.resultsJson("acme", Map(ids.head -> ScenarioVerdict.Conforms))
    assert(json.contains(ids.head) && !json.contains(ids(1)))
  }

  test("the suite's baseline test fails an adapter whose scenarios fell out of conformance, naming them and showing the new baseline") {
    val failed = events(new RegressedReferenceSpec).collect { case f: TestFailed if f.testName.startsWith("baseline:") => f }
    assert(failed.size == 1)
    assert(failed.head.message.contains("conformed on the baseline and is now skipped"))
    assert(failed.head.message.contains("The baseline for this run would be"))
  }

  test("the suite's baseline test fails an adapter with no baseline file, with the body to create it") {
    val failed = events(new UnbaselinedSpec).collect { case f: TestFailed if f.testName.startsWith("baseline:") => f }
    assert(failed.size == 1)
    assert(failed.head.message.contains("conformance-baseline-nobaseline.txt") && failed.head.message.contains("Create it with"))
    assert(failed.head.message.contains(ids.head))
  }

  test("an adapter that matches its baseline passes it, and its results are written where the spec says") {
    val dir = Files.createTempDirectory("conformance-results").toFile
    try {
      val reported = events(new ResultsWritingSpec(dir))
      assert(reported.collect { case s: TestSucceeded if s.testName.startsWith("baseline:") => s }.size == 1)
      assert(!reported.exists(_.isInstanceOf[TestFailed]))
      val written = new java.io.File(dir, "reference.json")
      assert(written.exists)
      val json = new String(Files.readAllBytes(written.toPath), "UTF-8")
      assert(json == ConformanceBaseline.resultsJson("reference", allConform))
    } finally {
      Option(dir.listFiles()).toList.flatten.foreach(_.delete())
      dir.delete()
    }
  }

  test("a partial run is not compared with the baseline: the test is canceled, not failed") {
    import org.scalatest.events.TestCanceled
    val reported = {
      val seen = scala.collection.mutable.ListBuffer.empty[Event]
      new RegressedReferenceSpec().run(Some("baseline: every scenario on the checked-in baseline still conforms, and every one that conforms is on it"), Args(reporter = (e: Event) => seen += e))
      seen.toList
    }
    assert(reported.collect { case c: TestCanceled => c }.size == 1)
    assert(!reported.exists(_.isInstanceOf[TestFailed]))
  }
}
