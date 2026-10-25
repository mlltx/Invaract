// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{Capability, Support}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class EngineGuidePageSpec extends AnyFunSuite {

  /** A declaration with every capability supported, but for `overrides` (id -> status line and any further lines). */
  private def yaml(
      adapter: String = "toy",
      engine: String = "Toy Engine",
      point: String = "pre-submit-gate",
      guide: Option[String] = Some("/guides/toy/"),
      overrides: Map[String, List[String]] = Map.empty
  ): String = {
    val caps = Capability.all.map { c =>
      val lines = overrides.getOrElse(c.id, List("status: supported"))
      s"  ${c.id}:\n" + lines.map("    " + _).mkString("\n")
    }
    (List(s"adapter: $adapter", s"engine: $engine") ++ guide.map("guide: " + _) ++
      List("enforcement:", s"  point: $point", "capabilities:") ++ caps).mkString("\n") + "\n"
  }

  private def page(engines: String*): String = EngineGuidePage.render(engines.map(EngineGuidePage.engine).toList)

  private val gap = List("status: unsupported", "summary: Not on this engine.", "note: contributor detail")
  private val part = List("status: partial", "summary: Only some shapes.", "note: contributor detail")
  private val none = List("status: not-applicable", "summary: The engine has no such thing.", "note: contributor detail")

  // --- reading a declaration --------------------------------------------------------------------------------------

  test("a declaration gives its guide and a summary per entry, whitespace folded; a blank or absent one is no summary") {
    val e = EngineGuidePage.engine(yaml(overrides = Map(
      "check.format" -> List("status: partial", "summary: \"  Two   words \"", "note: n"),
      "check.saveMode" -> List("status: partial", "summary: \"  \"", "note: n")
    )))
    assert(e.guide.contains("/guides/toy/"))
    assert(e.summaries == Map(Capability.CheckFormat -> "Two words"))
    assert(EngineGuidePage.engine(yaml(guide = None)).guide.isEmpty)
  }

  test("a declaration the parser refuses is refused here too, with its problems") {
    val broken = yaml().replace("  read.batch:\n    status: supported\n", "")
    val ex = intercept[IllegalArgumentException](EngineGuidePage.engine(broken))
    assert(ex.getMessage.contains("read.batch"))
  }

  // --- what must be explained -----------------------------------------------------------------------------------

  test("every capability not fully supported needs a summary, and the error names each one missing; a supported one needs none") {
    val missing = yaml(adapter = "lazy", overrides = Map(
      "check.format" -> List("status: partial", "note: n"),
      "rules.custom" -> List("status: unsupported", "note: n"),
      "write.stateChange" -> List("status: not-applicable", "note: n")
    ))
    val ex = intercept[IllegalArgumentException](page(missing))
    assert(ex.getMessage.contains("lazy: write.stateChange, lazy: check.format, lazy: rules.custom"), ex.getMessage)
    page(yaml()) // all supported: nothing to explain
  }

  test("no declarations, no page") {
    intercept[IllegalArgumentException](EngineGuidePage.render(Nil))
  }

  // --- the page -------------------------------------------------------------------------------------------------

  test("status words, one per status") {
    assert(Support.all.map(EngineGuidePage.statusWords) == List("✅ Yes", "◐ Partly", "✖ No", "— Doesn't apply"))
  }

  test("anchors follow the docs site's headings: lower case, punctuation dropped, spaces as hyphens") {
    assert(EngineGuidePage.anchor("MERGE, UPDATE and DELETE") == "merge-update-and-delete")
    assert(EngineGuidePage.anchor("The transformation's shape") == "the-transformations-shape")
    assert(EngineGuidePage.anchor("a-b c_d 2") == "a-b-c_d-2")
  }

  test("engines are columns in name order, each with how it stops a write and its guide when it names one") {
    val p = page(yaml(adapter = "zed", engine = "Zed Engine", point = "observe-only", guide = None), yaml(adapter = "abc", engine = "Abc Engine"))
    assert(p.contains("| Check | Abc Engine | Zed Engine |"))
    assert(p.contains(s"| Abc Engine | ${CapabilityGuide.enforcement(com.invaract.verification.EnforcementPoint.PreSubmitGate)} | [Guide](/guides/toy/) |"))
    assert(p.contains(s"| Zed Engine | ${CapabilityGuide.enforcement(com.invaract.verification.EnforcementPoint.ObserveOnly)} |  |"))
    assert(p.indexOf("### Abc Engine") < p.indexOf("### Zed Engine"))
  }

  test("each row of the overview links to the check's explanation and gives each engine's status in words") {
    val p = page(yaml(overrides = Map("check.format" -> part, "rules.custom" -> gap, "write.stateChange" -> none)))
    val title = CapabilityGuide.of(Capability.CheckFormat).title
    assert(p.contains(s"| [$title](#${EngineGuidePage.anchor(title)}) | ◐ Partly |"))
    assert(p.contains(s"| [${CapabilityGuide.of(Capability.RulesCustom).title}](#"))
    assert(p.contains("| ✖ No |") && p.contains("| — Doesn't apply |") && p.contains("| ✅ Yes |"))
    CapabilityGuide.categoryTitles.values.foreach(t => assert(p.contains(s"### $t\n"), t))
  }

  test("an engine's shortfalls are listed by kind; only a missing check says what you lose") {
    val p = page(yaml(overrides = Map("check.format" -> part, "rules.custom" -> gap, "write.stateChange" -> none)))
    val section = p.substring(p.indexOf("### Toy Engine"), p.indexOf("## What each check means"))
    assert(section.contains("Everything not listed here is checked in full."))
    assert(section.indexOf("**Checked in part**") < section.indexOf("**Not checked**"))
    assert(section.indexOf("**Not checked**") < section.indexOf("**Does not apply**"))
    assert(section.contains(s": Only some shapes.\n"), "a partial check gives its summary alone")
    assert(section.contains(s": Not on this engine. ${CapabilityGuide.shortWithoutIt(Capability.RulesCustom)}\n"))
    assert(section.contains(s": The engine has no such thing.\n"))
    assert(!section.contains("contributor detail"), "the contributor note stays on the capability matrix")
  }

  test("an engine with no shortfall says everything is checked, and lists none") {
    val p = page(yaml())
    val section = p.substring(p.indexOf("### Toy Engine"), p.indexOf("## What each check means"))
    assert(section.contains("Everything above is checked."))
    assert(!section.contains("**Checked in part**") && !section.contains("**Not checked**") && !section.contains("**Does not apply**"))
  }

  test("every check is explained once, with what you lose without it and its guide when it has one") {
    val p = page(yaml())
    Capability.all.foreach { c =>
      val e = CapabilityGuide.of(c)
      assert(p.contains(s"### ${e.title}\n\n${e.what}\n\nWhere an engine does not do it: ${CapabilityGuide.withoutIt(c)}"), c.id)
      e.guide.foreach(g => assert(p.contains(s"${CapabilityGuide.withoutIt(c)} [Read more]($g)"), c.id))
    }
    val noGuide = CapabilityGuide.of(Capability.WriteStateChange)
    assert(p.contains(s"Where an engine does not do it: ${CapabilityGuide.withoutIt(Capability.WriteStateChange)}\n"), noGuide.title)
    assert(p.startsWith("---\ntitle: What Each Engine Checks\n") && p.endsWith("\n") && !p.endsWith("\n\n"))
  }

  // --- the committed page ------------------------------------------------------------------------------------------

  private val repoRoot: File = {
    def isRoot(d: File) = new File(d, "docs-site").isDirectory && new File(d, "spark-adapter").isDirectory
    Iterator.iterate(new File(".").getCanonicalFile)(_.getParentFile).takeWhile(_ != null).find(isRoot)
      .getOrElse(fail("could not find the repository root above " + new File(".").getCanonicalPath))
  }

  private def declarations: List[String] =
    repoRoot.listFiles().toList.filter(_.isDirectory).sortBy(_.getName).flatMap { module =>
      Option(new File(module, "src/main/resources").listFiles()).toList.flatten
        .filter(f => f.getName.startsWith("invaract-capabilities-") && f.getName.endsWith(".yaml"))
    }.map(_.getPath)

  private def read(path: String) = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)

  test("the committed what-each-engine-checks page is exactly what the declarations generate - run ./dev/capabilities") {
    val committed = new File(repoRoot, EngineGuidePage.Path)
    assert(committed.exists(), s"missing ${EngineGuidePage.Path} - run ./dev/capabilities")
    assert(
      read(committed.getPath) == EngineGuidePage.renderFiles(declarations),
      s"${EngineGuidePage.Path} is out of date with the adapters' invaract-capabilities-*.yaml or CapabilityGuide - it is generated: " +
        "run ./dev/capabilities and commit the result"
    )
  }

  test("main writes the page the renderer produces, and needs an output path and at least one declaration") {
    val dir = Files.createTempDirectory("engine-guide-test")
    try {
      val out = dir.resolve("page.md")
      EngineGuidePage.main((out.toString :: declarations).toArray)
      assert(read(out.toString) == EngineGuidePage.renderFiles(declarations))
      intercept[IllegalArgumentException](EngineGuidePage.main(Array(out.toString)))
      // one declaration is enough
      EngineGuidePage.main(Array(out.toString, declarations.head))
      assert(read(out.toString) == EngineGuidePage.renderFiles(List(declarations.head)))
    } finally {
      Files.list(dir).forEach(p => Files.delete(p))
      Files.delete(dir)
    }
  }
}
