// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability, CapabilityCategory, Support}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/** The user-facing "What Each Engine Checks" page, written from each adapter's declaration and `CapabilityGuide`: for every
  * engine, what it checks, what it checks only in part, and what it does not, each in plain words, with what that means
  * for a job. The capability matrix (`CapabilityMatrixPage`) is the same facts for contributors.
  *
  * A declaration gives the plain words for its own gaps: every capability it declares anything but `supported` needs a
  * `summary` (one sentence for a user, beside the contributor-facing `note`), and the file may name its install guide as
  * `guide`. `render` refuses a declaration that leaves a summary out, so a gap never reaches the page unexplained.
  */
object EngineGuidePage {

  /** One adapter as the page shows it. */
  final case class Engine(declaration: AdapterCapabilities, guide: Option[String], summaries: Map[Capability, String])

  val Path = "docs-site/src/content/docs/reference/what-each-engine-checks.md"

  /** Reads a declaration file: what `AdapterCapabilities.parse` reads, plus the `guide` and each entry's `summary`. */
  def engine(yaml: String): Engine = {
    val declaration = AdapterCapabilities.parse(yaml).fold(problems => throw new IllegalArgumentException(problems.mkString("; ")), identity)
    val root = new Yaml(new SafeConstructor(new LoaderOptions)).load[AnyRef](yaml).asInstanceOf[JMap[String, AnyRef]].asScala
    val declared = root.get("capabilities").collect { case m: JMap[_, _] => m.asInstanceOf[JMap[String, AnyRef]].asScala.toMap }.getOrElse(Map.empty)
    val summaries = Capability.all.flatMap { c =>
      declared.get(c.id).collect { case m: JMap[_, _] => m.asInstanceOf[JMap[String, AnyRef]].get("summary") }.flatMap(text).map(c -> _)
    }.toMap
    Engine(declaration, root.get("guide").flatMap(text), summaries)
  }

  private def text(value: AnyRef): Option[String] = value match {
    case s: String if s.trim.nonEmpty => Some(s.trim.replaceAll("\\s+", " "))
    case _                            => None
  }

  /** The words a cell of the overview shows for each status. */
  def statusWords(support: Support): String = support match {
    case Support.Supported     => "✅ Yes"
    case Support.Partial       => "◐ Partly"
    case Support.Unsupported   => "✖ No"
    case Support.NotApplicable => "— Doesn't apply"
  }

  /** The anchor Starlight gives a heading: lower case, letters, digits, `-` and `_` kept, spaces made `-`. */
  def anchor(heading: String): String =
    heading.toLowerCase.filter(c => c.isLetterOrDigit || c == ' ' || c == '-' || c == '_').replace(' ', '-')

  private def link(capability: Capability): String = {
    val title = CapabilityGuide.of(capability).title
    s"[$title](#${anchor(title)})"
  }

  def render(engines: List[Engine]): String = {
    require(engines.nonEmpty, "no adapter declarations")
    val missing = for {
      e <- engines
      (c, entry) <- e.declaration.entries.toList.sortBy(p => Capability.all.indexOf(p._1))
      if entry.support != Support.Supported && !e.summaries.contains(c)
    } yield s"${e.declaration.adapter}: ${c.id}"
    require(
      missing.isEmpty,
      "every capability a declaration does not fully support needs a `summary:` (one sentence for a user, beside its `note`): " + missing.mkString(", ")
    )
    val sorted = engines.sortBy(_.declaration.engine)
    val out = new StringBuilder
    out.append(
      """---
        |title: What Each Engine Checks
        |description: What Invaract checks on each engine, in plain words, and what you lose where an engine cannot make a check.
        |sidebar:
        |  order: 0.5
        |---
        |
        |<!-- GENERATED from each adapter's invaract-capabilities-<adapter>.yaml and adapter-testkit's CapabilityGuide. Do not edit by hand: change those and run ./dev/capabilities. -->
        |
        |Invaract works with more than one engine, and an engine can only be checked as far as it describes its own jobs.
        |This page says what Invaract checks on each one, and what it means for a job where it cannot.
        |
        |One rule holds everywhere: if your contract relies on a check an engine cannot make, the job is **refused before it
        |writes anything** (`UNSUPPORTED_CONTRACT_FEATURE`). It is never run with part of the contract quietly unchecked.
        |A check that only reports, such as a fingerprint, is simply not produced.
        |
        |For the same facts in more detail, with the notes each engine's adapter keeps, see
        |[Engine Capabilities](/reference/engine-capabilities/).
        |
        |## How each engine stops a bad write
        |
        |""".stripMargin
    )
    out.append("| Engine | How it stops a bad write | Set it up |\n|---|---|---|\n")
    sorted.foreach { e =>
      val setUp = e.guide.fold("")(g => s"[Guide]($g)")
      out.append(s"| ${e.declaration.engine} | ${CapabilityGuide.enforcement(e.declaration.enforcement)} | $setUp |\n")
    }
    out.append("\n## At a glance\n\n")
    out.append("✅ checked · ◐ checked in part (see below) · ✖ not checked · — does not apply to that engine\n\n")
    CapabilityCategory.all.foreach { category =>
      val capabilities = Capability.all.filter(_.category == category)
      out.append(s"### ${CapabilityGuide.categoryTitles(category)}\n\n")
      out.append("| Check | " + sorted.map(_.declaration.engine).mkString(" | ") + " |\n")
      out.append("|---|" + sorted.map(_ => "---").mkString("|") + "|\n")
      capabilities.foreach { c =>
        out.append(s"| ${link(c)} | " + sorted.map(e => statusWords(e.declaration.supportOf(c))).mkString(" | ") + " |\n")
      }
      out.append("\n")
    }
    out.append("## Where each engine falls short\n\n")
    sorted.foreach { e =>
      out.append(s"### ${e.declaration.engine}\n\n")
      val bySupport = Capability.all.groupBy(e.declaration.supportOf)
      if (bySupport.keySet == Set(Support.Supported)) out.append("Everything above is checked.\n\n")
      else {
        out.append(s"Everything not listed here is checked in full.\n\n")
        List(Support.Partial -> "Checked in part", Support.Unsupported -> "Not checked", Support.NotApplicable -> "Does not apply").foreach {
          case (support, heading) =>
            bySupport.get(support).foreach { caps =>
              out.append(s"**$heading**\n\n")
              caps.foreach { c =>
                out.append(s"- ${link(c)}: ${e.summaries(c)}")
                if (support == Support.Unsupported) out.append(s" ${CapabilityGuide.shortWithoutIt(c)}")
                out.append("\n")
              }
              out.append("\n")
            }
        }
      }
    }
    out.append("## What each check means\n\n")
    Capability.all.foreach { c =>
      val explained = CapabilityGuide.of(c)
      out.append(s"### ${explained.title}\n\n${explained.what}\n\n")
      out.append(s"Where an engine does not do it: ${CapabilityGuide.withoutIt(c)}")
      explained.guide.foreach(g => out.append(s" [Read more]($g)"))
      out.append("\n\n")
    }
    out.toString.stripSuffix("\n")
  }

  def renderFiles(paths: List[String]): String =
    render(paths.map(p => engine(new String(Files.readAllBytes(Paths.get(p)), StandardCharsets.UTF_8))))

  /** `args`: the output page, then each adapter's declaration file. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: EngineGuidePage <output.md> <invaract-capabilities-*.yaml>...")
    Files.write(Paths.get(args.head), renderFiles(args.tail.toList).getBytes(StandardCharsets.UTF_8))
  }
}
