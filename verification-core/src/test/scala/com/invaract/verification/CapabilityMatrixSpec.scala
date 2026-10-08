// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class CapabilityMatrixSpec extends AnyFunSuite {
  import CapabilityFixtures.{parsed, yaml}

  private def read(path: String): String = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)

  // --- rendering ---------------------------------------------------------------------------------

  private val toy = parsed(
    Map(
      "rules.dml" -> (("partial", Some("only DELETE | and nothing else"))),
      "check.catalogRegistration" -> (("unsupported", Some("no catalogs   in this engine"))),
      "read.streaming" -> (("not-applicable", Some("batch only")))
    ),
    adapter = "toy"
  )

  private val plain = parsed(adapter = "alpha", point = "in-engine-blocking")

  private val page = CapabilityMatrix.render(List(toy, plain))

  test("the page is Starlight markdown carrying the generated notice") {
    assert(page.startsWith("---\ntitle: Engine Capabilities\ndescription: "))
    assert(page.contains("sidebar:\n  order: 7\n---\n"))
    assert(page.contains(s"<!-- ${CapabilityMatrix.GeneratedNotice} -->"))
    assert(CapabilityMatrix.GeneratedNotice.contains("./dev/capabilities"))
  }

  test("adapters are ordered by name whatever order they are given, one column each") {
    assert(CapabilityMatrix.render(List(toy, plain)) == CapabilityMatrix.render(List(plain, toy)))
    assert(page.contains("| Capability | What it means | alpha | toy |"))
    assert(page.indexOf("| `alpha` |") < page.indexOf("| `toy` |"))
  }

  test("the enforcement table says where each adapter can stop a bad write") {
    assert(page.contains("| `alpha` | Toy Engine 1.0 | Blocks inside the engine, before the operation executes |"))
    assert(page.contains("| `toy` | Toy Engine 1.0 | Gates before the job is submitted |"))
    val withNote = CapabilityMatrix.render(List(parsedWithEnforcementNote))
    assert(withNote.contains("Observes only; cannot block — reports after the fact"))
  }

  private def parsedWithEnforcementNote: AdapterCapabilities =
    AdapterCapabilities.parse(yaml(adapter = "obs", point = "observe-only", enforcementNote = Some("reports after the fact"))).fold(e => fail(e.mkString), identity)

  test("each capability is a row with a symbol per adapter, grouped under its category") {
    assert(page.contains("### Structural checks"))
    assert(page.contains("### Fail-closed behaviour"))
    assert(page.contains("| `rules.dml` | DML rules: merge_condition, forbid_unconditional_delete, allowed_update_columns. *(blocks if unsupported)* | ✅ | ◐ |"))
    assert(page.contains("| `check.catalogRegistration` | A declared catalog requirement is checked for inputs and outputs. *(blocks if unsupported)* | ✅ | ✖ |"))
    assert(page.contains("| `read.streaming` | A streaming read is recognized as one of the contract's inputs. | ✅ | — |"))
    // a capability that does not enforce the contract is not marked as blocking
    assert(page.contains("| `analysis.fingerprint` | A semantic fingerprint of the transformation is computed (opt-in). | ✅ | ✅ |"))
    Capability.all.foreach(c => assert(page.contains(s"| `${c.id}` |"), c.id))
  }

  test("categories appear in the vocabulary's order") {
    val positions = CapabilityCategory.all.map(c => page.indexOf(s"### ${c.title}"))
    assert(positions.forall(_ >= 0))
    assert(positions == positions.sorted)
  }

  test("notes list every capability that is not plainly supported, with its reason on one line (runs of whitespace collapsed) and pipes escaped") {
    assert(page.contains("### toy\n"))
    assert(page.contains("- `rules.dml` — partial: only DELETE \\| and nothing else"))
    assert(page.contains("- `check.catalogRegistration` — unsupported: no catalogs in this engine"))
    assert(page.contains("- `read.streaming` — not-applicable: batch only"))
    assert(!page.contains("- `check.schema` — supported"))
  }

  test("an adapter with nothing to qualify says so") {
    assert(page.contains("### alpha\n\nEvery capability is supported without qualification.\n"))
  }

  test("a docs path becomes a link to the design doc, and a supported note is listed too") {
    val withDocs = AdapterCapabilities.parse(
      yaml(adapter = "d").replace("  check.schema:\n    status: supported\n", "  check.schema:\n    status: supported\n    note: names follow the case rule\n    docs: SPARK_ADAPTER.md\n")
    ).fold(e => fail(e.mkString), identity)
    val text = CapabilityMatrix.render(List(withDocs))
    assert(text.contains("- `check.schema` — supported: names follow the case rule ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))"))
  }

  // --- files -------------------------------------------------------------------------------------

  test("renderFiles reads declaration files, and names the file and every problem when one is invalid") {
    val dir = Files.createTempDirectory("capability-matrix-test")
    try {
      val good = dir.resolve("good.yaml"); Files.write(good, yaml(adapter = "good").getBytes(StandardCharsets.UTF_8))
      val bad = dir.resolve("bad.yaml"); Files.write(bad, yaml(adapter = "bad", omit = Set("rules.dml")).getBytes(StandardCharsets.UTF_8))
      assert(CapabilityMatrix.renderFiles(List(good.toString)).contains("| `good` |"))
      val ex = intercept[IllegalArgumentException] { CapabilityMatrix.renderFiles(List(good.toString, bad.toString)) }
      assert(ex.getMessage.contains(bad.toString) && ex.getMessage.contains("'rules.dml' is not declared"))
      assert(!ex.getMessage.contains(good.toString))
      // main writes the page
      val out = dir.resolve("out.md")
      CapabilityMatrix.main(Array(out.toString, good.toString))
      assert(read(out.toString) == CapabilityMatrix.renderFiles(List(good.toString)))
      intercept[IllegalArgumentException] { CapabilityMatrix.main(Array(out.toString)) }
    } finally {
      Files.list(dir).forEach(p => Files.delete(p))
      Files.delete(dir)
    }
  }

  // --- drift: the real declarations and the committed page ---------------------------------------

  /** The repository root: `..` from a normal test run, but a mutation run executes from a copy of this
    * module under `target/`, so walk up until the directory holding both the docs site and the adapters.
    */
  private val repoRoot: File = {
    def isRoot(d: File) = new File(d, "docs-site").isDirectory && new File(d, "spark-adapter").isDirectory
    Iterator.iterate(new File(".").getCanonicalFile)(_.getParentFile).takeWhile(_ != null).find(isRoot).getOrElse(fail("could not find the repository root above " + new File(".").getCanonicalPath))
  }

  private def declarations: List[File] =
    repoRoot.listFiles().toList.filter(_.isDirectory).sortBy(_.getName).flatMap { module =>
      val resources = new File(module, "src/main/resources")
      Option(resources.listFiles()).toList.flatten.filter(f => f.getName.startsWith("invaract-capabilities-") && f.getName.endsWith(".yaml"))
    }

  test("every adapter in the repository declares every capability, under a file named for it") {
    val files = declarations
    assert(files.nonEmpty, "expected at least the Spark adapter's declaration")
    files.foreach { f =>
      val caps = AdapterCapabilities.parse(read(f.getPath)).fold(e => fail(s"${f.getPath}:\n  - ${e.mkString("\n  - ")}"), identity)
      assert(f.getName == s"invaract-capabilities-${caps.adapter}.yaml", s"${f.getPath} declares adapter '${caps.adapter}'")
    }
  }

  // The committed page is generated with the conformance suite's coverage, which only adapter-testkit knows;
  // its drift test is adapter-testkit's CapabilityMatrixPageSpec.

  // --- suite coverage ----------------------------------------------------------------------------

  private val coverage = SuiteCoverage(
    verified = Map(Capability.CheckSchema -> List("a", "b"), Capability.CheckLocation -> List("c")),
    attested = Map(Capability.ConfigZeroCodeInstall -> "how an adapter attaches   to a job | is engine-specific"),
    gaps = Map(Capability.ReadStreaming -> "needs a streaming job shape")
  )

  private val withSuite = CapabilityMatrix.render(List(toy, plain), Some(coverage))

  test("without suite coverage the page has no suite column or section (the plain render is unchanged)") {
    assert(CapabilityMatrix.render(List(toy, plain), None) == page)
    assert(!page.contains("Suite check") && !page.contains("What the conformance suite checks"))
  }

  test("with suite coverage each capability row says verified (n), attested, gap or none, before the adapter columns") {
    assert(withSuite.contains("| Capability | What it means | Suite check | alpha | toy |"))
    assert(withSuite.contains("|---|---|---|---|---|"))
    assert(withSuite.contains("| `check.schema` | Field presence, type and nullability of inputs and outputs. *(blocks if unsupported)* | verified (2) | ✅ | ✅ |"))
    assert(withSuite.contains("| `check.location` ") && withSuite.contains("| verified (1) |"))
    assert(withSuite.contains("| attested | ✅ | ✅ |"))
    assert(withSuite.contains("| gap | ✅ | — |"))
    assert(withSuite.contains("| none | ✅ | ✅ |"), "a capability in no tier is shown as none, not as covered")
    Capability.all.foreach(c => assert(withSuite.contains(s"| `${c.id}` |"), c.id))
  }

  test("the suite section lists attested and gap reasons on one line each, pipes escaped, in vocabulary order") {
    assert(withSuite.contains("## What the conformance suite checks"))
    assert(withSuite.contains("Attested:\n\n- `config.zeroCodeInstall` — how an adapter attaches to a job \\| is engine-specific\n"))
    assert(withSuite.contains("Gaps:\n\n- `read.streaming` — needs a streaming job shape\n"))
    assert(withSuite.indexOf("## What the conformance suite checks") < withSuite.indexOf("## Notes"))
  }

  test("an empty tier is not given a heading") {
    val onlyVerified = CapabilityMatrix.render(List(plain), Some(SuiteCoverage(Map(Capability.CheckSchema -> List("a")), Map.empty, Map.empty)))
    assert(onlyVerified.contains("## What the conformance suite checks"))
    assert(!onlyVerified.contains("Attested:") && !onlyVerified.contains("Gaps:"))
  }

  test("renderFiles with suite coverage renders the suite column too") {
    val dir = Files.createTempDirectory("capability-matrix-suite-test")
    try {
      val good = dir.resolve("good.yaml"); Files.write(good, yaml(adapter = "good").getBytes(StandardCharsets.UTF_8))
      assert(CapabilityMatrix.renderFiles(List(good.toString), Some(coverage)).contains("| Suite check |"))
      assert(!CapabilityMatrix.renderFiles(List(good.toString)).contains("Suite check"))
    } finally {
      Files.list(dir).forEach(p => Files.delete(p))
      Files.delete(dir)
    }
  }
}
