// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Renders the engine capability matrix - what every adapter verifies and where it can stop a bad
  * write - from the adapters' own `AdapterCapabilities` declarations, so the documentation can never
  * disagree with them: it is generated, and a test fails if the committed page differs from what this
  * produces (see `CapabilityMatrixSpec`).
  *
  * Run it through `./dev/capabilities`, which runs `adapter-testkit`'s `CapabilityMatrixPage` (the same
  * rendering with the conformance suite's coverage folded in) over every adapter's declaration file.
  */
/** What the conformance suite does for each capability, so the page can say how far a declaration is checked:
  * `verified` capabilities have scenarios (the ids) that run as real jobs on every adapter, `attested` ones
  * cannot be checked by a job by their nature (the reason says why), `gaps` could be but are not yet.
  * Supplied by `adapter-testkit` (this module does not know the scenarios).
  */
final case class SuiteCoverage(verified: Map[Capability, List[String]], attested: Map[Capability, String], gaps: Map[Capability, String])

object CapabilityMatrix {

  val GeneratedNotice: String =
    "GENERATED from each adapter's invaract-capabilities-<adapter>.yaml and the conformance scenarios (adapter-testkit). " +
      "Do not edit by hand: change the declaration and run ./dev/capabilities."

  private val GitHubDocs = "https://github.com/mlltx/Invaract/blob/main/docs/"

  /** The docs-site page (Starlight markdown) for `adapters`, in adapter-name order, without suite coverage. */
  def render(adapters: List[AdapterCapabilities]): String = render(adapters, None)

  /** The docs-site page for `adapters`; with `suite` it also says, per capability, how the conformance suite checks it. */
  def render(adapters: List[AdapterCapabilities], suite: Option[SuiteCoverage]): String = {
    val sorted = adapters.sortBy(_.adapter)
    val sb = new StringBuilder

    sb.append("---\n")
    sb.append("title: Engine Capabilities\n")
    sb.append("description: What each engine adapter verifies, and where it can stop a bad write.\n")
    sb.append("sidebar:\n  order: 7\n")
    sb.append("---\n\n")
    sb.append(s"<!-- $GeneratedNotice -->\n\n")

    sb.append("Invaract checks a data transformation against a contract through an *engine adapter*. Every adapter\n")
    sb.append("declares, for every capability below, whether it is supported, partly supported, unsupported, or does\n")
    sb.append("not apply to its engine — an adapter cannot leave a gap undeclared. If a contract relies on something\n")
    sb.append("an adapter declares **unsupported** (a catalog requirement, a rule, a nested type, a format, ...), the\n")
    sb.append("write is rejected with `UNSUPPORTED_CONTRACT_FEATURE` rather than passed as if it had been checked.\n")
    sb.append("A **partial** capability never blocks on its own; the notes below say which shapes are not covered.\n\n")
    sb.append("A declaration is a claim, and a shared conformance suite checks it: the same engine-neutral scenarios run\n")
    sb.append("as real jobs on every adapter, and each must come out the way that adapter's declaration promises. A\n")
    sb.append("capability the suite has no scenario for yet is listed by the suite as unverified rather than assumed.\n\n")
    sb.append("✅ supported · ◐ partial · ✖ unsupported · — not applicable\n\n")

    sb.append("## Where each adapter enforces\n\n")
    sb.append("| Adapter | Engine | Enforcement point |\n|---|---|---|\n")
    sorted.foreach { a =>
      sb.append(s"| `${a.adapter}` | ${cell(a.engine)} | ${a.enforcement.description}${a.enforcementNote.map(n => s" — ${cell(n)}").getOrElse("")} |\n")
    }
    sb.append("\n")

    sb.append("## Capability matrix\n\n")
    CapabilityCategory.all.foreach { category =>
      val capabilities = Capability.all.filter(_.category == category)
      if (capabilities.nonEmpty) {
        sb.append(s"### ${category.title}\n\n")
        val suiteHeader = if (suite.isDefined) " Suite check |" else ""
        val suiteRule = if (suite.isDefined) "---|" else ""
        sb.append("| Capability | What it means |" + suiteHeader + sorted.map(a => s" ${a.adapter} |").mkString + "\n")
        sb.append("|---|---|" + suiteRule + sorted.map(_ => "---|").mkString + "\n")
        capabilities.foreach { c =>
          val blocking = if (c.enforcesContract) " *(blocks if unsupported)*" else ""
          val suiteCell = suite.map(sc => s" ${suiteCheck(sc, c)} |").getOrElse("")
          sb.append(s"| `${c.id}` | ${cell(c.description)}$blocking |" + suiteCell + sorted.map(a => s" ${symbol(a.supportOf(c))} |").mkString + "\n")
        }
        sb.append("\n")
      }
    }

    suite.foreach { sc =>
      sb.append("## What the conformance suite checks\n\n")
      sb.append("The *Suite check* column says how far a declaration is verified rather than taken on trust.\n\n")
      sb.append("- **verified** — scenarios run as real jobs on every adapter that declares the capability, and each must come out as the declaration promises.\n")
      sb.append("- **attested** — no job can check it by its nature; the adapter's own tests carry it.\n")
      sb.append("- **gap** — a job could check it, but the suite cannot yet; a declaration of support is a claim only.\n\n")
      if (sc.attested.nonEmpty) {
        sb.append("Attested:\n\n")
        Capability.all.filter(sc.attested.contains).foreach(c => sb.append(s"- `${c.id}` — ${cell(sc.attested(c))}\n"))
        sb.append("\n")
      }
      if (sc.gaps.nonEmpty) {
        sb.append("Gaps:\n\n")
        Capability.all.filter(sc.gaps.contains).foreach(c => sb.append(s"- `${c.id}` — ${cell(sc.gaps(c))}\n"))
        sb.append("\n")
      }
    }

    sb.append("## Notes\n\n")
    sorted.foreach { a =>
      val noted = Capability.all.filter(c => a.supportOf(c) != Support.Supported || a.entryOf(c).note.isDefined)
      sb.append(s"### ${a.adapter}\n\n")
      if (noted.isEmpty) sb.append("Every capability is supported without qualification.\n\n")
      else {
        noted.foreach { c =>
          val e = a.entryOf(c)
          val link = e.docs.map(d => s" ([details]($GitHubDocs$d))").getOrElse("")
          sb.append(s"- `${c.id}` — ${e.support.id}${e.note.map(n => s": ${cell(n)}").getOrElse("")}$link\n")
        }
        sb.append("\n")
      }
    }
    sb.toString()
  }

  private def suiteCheck(suite: SuiteCoverage, c: Capability): String =
    suite.verified.get(c) match {
      case Some(scenarios) => s"verified (${scenarios.size})"
      case None if suite.attested.contains(c) => "attested"
      case None if suite.gaps.contains(c)     => "gap"
      // Not a gap in the suite's own bookkeeping but not exercised either: say so rather than imply coverage.
      case None => "none"
    }

  private def symbol(s: Support): String = s match {
    case Support.Supported     => "✅"
    case Support.Partial       => "◐"
    case Support.Unsupported   => "✖"
    case Support.NotApplicable => "—"
  }

  /** A table cell / list item: one line, with `|` escaped. */
  private def cell(text: String): String = text.trim.replaceAll("\\s+", " ").replace("|", "\\|")

  /** `render` of the declarations in `paths`; throws with every problem if any file is invalid. */
  def renderFiles(paths: List[String]): String = renderFiles(paths, None)

  /** `render` of the declarations in `paths` with `suite` coverage; throws with every problem if any file is invalid. */
  def renderFiles(paths: List[String], suite: Option[SuiteCoverage]): String = {
    val parsed = paths.map { p =>
      val text = new String(Files.readAllBytes(Paths.get(p)), StandardCharsets.UTF_8)
      AdapterCapabilities.parse(text).left.map(errors => s"$p:\n  - ${errors.mkString("\n  - ")}")
    }
    val errors = parsed.collect { case Left(e) => e }
    if (errors.nonEmpty) throw new IllegalArgumentException(errors.mkString("\n"))
    render(parsed.collect { case Right(a) => a }, suite)
  }

  /** `args`: the output page, then each adapter's declaration file. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: CapabilityMatrix <output.md> <invaract-capabilities-*.yaml>...")
    Files.write(Paths.get(args.head), renderFiles(args.tail.toList).getBytes(StandardCharsets.UTF_8))
  }
}
