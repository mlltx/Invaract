// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** The conformance suite as a table a person can read, and - given an adapter's capability declaration, even a
  * draft for an adapter that does not exist yet - what the kit will expect of that adapter on every scenario.
  *
  * The expectation is not guessed: it is `Conformance.expectationFor`, the same function the kit uses when it runs a
  * real adapter, so a forecast made before any code is written and the verdict on the finished adapter use one rule.
  * What this cannot say is whether the adapter will manage to do what is expected; that is the author's judgement,
  * which is why every row has a column left to fill in (`Forecast`, `Why`).
  *
  * `./dev/adapter-survey` runs `main` for the survey that precedes a new adapter
  * (`.claude/skills/survey-engine-adapter`).
  */
object ScenarioCatalogue {

  /** What the kit will expect of an adapter on one scenario, given its declaration. */
  sealed trait Forecast
  object Forecast {

    /** The adapter does the checks and the write goes through. */
    case object Allows extends Forecast

    /** The adapter does the checks and blocks the write, with exactly these violation types. */
    final case class Blocks(violationTypes: Set[String]) extends Forecast

    /** The adapter declared something the scenario's contract relies on unsupported, so it must refuse the write with
      * `UNSUPPORTED_CONTRACT_FEATURE` rather than let it through unchecked. */
    case object RefusesAsUnsupported extends Forecast

    /** The scenario does not apply to this adapter, for the reason its own declaration gives. */
    final case class Skipped(reason: String) extends Forecast
  }

  def forecast(scenario: Scenario, capabilities: AdapterCapabilities): Forecast =
    Conformance.expectationFor(scenario, capabilities) match {
      case Left(reason)                                     => Forecast.Skipped(reason)
      case Right(Conformance.Expected.Pass)                 => Forecast.Allows
      case Right(Conformance.Expected.RejectExactly(types)) => Forecast.Blocks(types)
      case Right(Conformance.Expected.RejectedAsUnsupported) => Forecast.RefusesAsUnsupported
    }

  /** The neutral result: what the scenario expects of an adapter that supports everything it needs. */
  def neutral(scenario: Scenario): String = scenario.expect match {
    case Expectation.Pass          => "allowed"
    case Expectation.Reject(types) => describeBlock(types)
  }

  private[testkit] def describeBlock(types: Set[String]): String = "blocked: " + types.toList.sorted.map(t => s"`$t`").mkString(", ")

  private def describe(e: Forecast): String = e match {
    case Forecast.Allows               => "does the checks, write allowed"
    case Forecast.Blocks(types)        => "does the checks, write " + describeBlock(types)
    case Forecast.RefusesAsUnsupported => "must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`)"
    case Forecast.Skipped(reason)      => s"skipped: ${cell(reason)}"
  }

  /** A markdown table cell: one line, `|` escaped. */
  private def cell(text: String): String = text.trim.replaceAll("\\s+", " ").replace("|", "\\|")

  private def focusOf(scenario: Scenario): String = scenario.focus.toList.sortBy(_.id).map(c => s"`${c.id}`").mkString(", ")

  /** One row per scenario, in the catalogue's order. With a declaration the table says what the kit will expect of that
    * adapter; either way it ends with `Forecast` and `Why` columns for the author to fill in. */
  def render(declaration: Option[AdapterCapabilities]): String = {
    val sb = new StringBuilder
    val header = List("#", "Scenario", "What it means for a user", "What it checks", "Evidence for", "Neutral result") ++
      declaration.map(d => s"What the kit expects of `${d.adapter}`") ++ List("Forecast", "Why")
    sb.append(header.mkString("| ", " | ", " |")).append('\n')
    sb.append(header.map(_ => "---").mkString("| ", " | ", " |")).append('\n')
    Scenarios.all.zipWithIndex.foreach { case (s, i) =>
      val cells = List(
        (i + 1).toString,
        s"`${s.id}`",
        cell(s.userStory),
        cell(s.description),
        focusOf(s),
        neutral(s)
      ) ++ declaration.map(d => describe(forecast(s, d))) ++ List("", "")
      sb.append(cells.mkString("| ", " | ", " |")).append('\n')
    }
    sb.toString()
  }

  /** The capabilities the catalogue is the evidence for (a scenario's focus), then the ones no job can check. */
  def coverageSummary: String = {
    val verified = Scenarios.all.flatMap(_.focus).toSet
    val attested = Scenarios.attested.keySet
    val rest = Capability.all.filterNot(c => verified.contains(c) || attested.contains(c))
    s"${Scenarios.all.size} scenarios; ${verified.size} capabilities are the focus of at least one; ${attested.size} are attested " +
      s"(no job can check them: the adapter's own tests carry them); ${rest.size} are exercised only as operations a job needs."
  }

  /** `args`: the output file, then (optionally) a capability declaration, e.g. a draft for an adapter being surveyed. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 1 && args.length <= 2, "usage: ScenarioCatalogue <output.md> [invaract-capabilities-*.yaml]")
    val declaration = args.lift(1).map { path =>
      val text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
      AdapterCapabilities.parse(text).fold(errors => throw new IllegalArgumentException(s"$path is not a valid declaration: ${errors.mkString("; ")}"), identity)
    }
    Files.write(Paths.get(args.head), render(declaration).getBytes(StandardCharsets.UTF_8))
    println(coverageSummary)
  }
}
