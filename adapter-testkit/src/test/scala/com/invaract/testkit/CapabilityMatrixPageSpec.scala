// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.Capability
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class CapabilityMatrixPageSpec extends AnyFunSuite {

  private def read(path: String): String = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)

  test("a capability is in exactly one of verified, attested, gap") {
    val c = CapabilityMatrixPage.coverage
    Capability.all.foreach { cap =>
      val tiers = List(c.verified.contains(cap), c.attested.contains(cap), c.gaps.contains(cap)).count(identity)
      assert(tiers == 1, s"${cap.id} is in $tiers tiers")
    }
  }

  test("the row-level DML capabilities are verified by their scenarios") {
    val c = CapabilityMatrixPage.coverage
    assert(c.verified(Capability.WriteRowLevelDml).toSet == Set(
      "row-level-delete-checked-as-write", "row-level-delete-own-table-is-not-an-input", "unconditional-delete-forbidden", "filtered-delete-allowed"
    ))
    assert(c.verified(Capability.RulesDml).toSet == Set("unconditional-delete-forbidden", "filtered-delete-allowed"))
  }

  /** The repository root: `..` from a normal test run, but a mutation run executes from a copy of the module under
    * `target/`, so walk up until the directory holding both the docs site and the adapters.
    */
  private val repoRoot: File = {
    def isRoot(d: File) = new File(d, "docs-site").isDirectory && new File(d, "spark-adapter").isDirectory
    Iterator
      .iterate(new File(".").getCanonicalFile)(_.getParentFile)
      .takeWhile(_ != null)
      .find(isRoot)
      .getOrElse(fail("could not find the repository root above " + new File(".").getCanonicalPath))
  }

  private def declarations: List[File] =
    repoRoot.listFiles().toList.filter(_.isDirectory).sortBy(_.getName).flatMap { module =>
      Option(new File(module, "src/main/resources").listFiles()).toList.flatten
        .filter(f => f.getName.startsWith("invaract-capabilities-") && f.getName.endsWith(".yaml"))
    }

  test("the committed engine-capabilities page is exactly what the declarations and the suite generate - run ./dev/capabilities") {
    val committed = new File(repoRoot, "docs-site/src/content/docs/reference/engine-capabilities.md")
    assert(committed.exists(), "missing docs-site/src/content/docs/reference/engine-capabilities.md - run ./dev/capabilities")
    assert(
      read(committed.getPath) == CapabilityMatrixPage.renderFiles(declarations.map(_.getPath)),
      "docs-site/src/content/docs/reference/engine-capabilities.md is out of date with the adapters' invaract-capabilities-*.yaml " +
        "or the conformance scenarios - it is generated: run ./dev/capabilities and commit the result"
    )
  }

  test("main writes the page the renderer produces, and needs an output path and at least one declaration") {
    val dir = Files.createTempDirectory("capability-page-test")
    try {
      val out = dir.resolve("page.md")
      val files = declarations.map(_.getPath)
      CapabilityMatrixPage.main((out.toString :: files).toArray)
      assert(read(out.toString) == CapabilityMatrixPage.renderFiles(files))
      intercept[IllegalArgumentException](CapabilityMatrixPage.main(Array(out.toString)))
      intercept[IllegalArgumentException](CapabilityMatrixPage.main(Array.empty[String]))
    } finally {
      Files.list(dir).forEach(p => Files.delete(p))
      Files.delete(dir)
    }
  }

  // --- the conformance results section, from each adapter's checked-in baseline ---------------------------------

  private def withModule(baselineIds: Option[List[String]], declarationName: String = "invaract-capabilities-reference.yaml")(check: String => Unit): Unit = {
    val root = Files.createTempDirectory("matrix-results")
    try {
      val resources = Files.createDirectories(root.resolve("mod").resolve("src").resolve("main").resolve("resources"))
      val declaration = resources.resolve(declarationName)
      Files.copy(Paths.get(repoRoot.getPath, "adapter-testkit", "src", "main", "resources", "reference", "invaract-capabilities-reference.yaml"), declaration)
      baselineIds.foreach { ids =>
        val tests = Files.createDirectories(root.resolve("mod").resolve("src").resolve("test").resolve("resources"))
        Files.write(tests.resolve("conformance-baseline-reference.txt"), ("# held\n" + ids.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
      }
      check(CapabilityMatrixPage.renderFiles(List(declaration.toString)))
    } finally {
      Files.walk(root).sorted(java.util.Comparator.reverseOrder[java.nio.file.Path]()).forEach(p => Files.delete(p))
    }
  }

  test("an adapter with a checked-in baseline gets a conformance-results row counting it against the whole suite, before the notes") {
    val ids = Scenarios.all.take(3).map(_.id)
    withModule(Some(ids)) { page =>
      assert(page.contains(s"The suite has ${Scenarios.all.size} scenarios."))
      assert(page.contains(s"| `reference` | 3 | ${Scenarios.all.size} |"))
      assert(page.indexOf("## Conformance results") > 0 && page.indexOf("## Conformance results") < page.indexOf("## Notes"))
    }
  }

  test("an empty baseline is a row of zero, not an absent adapter") {
    withModule(Some(Nil)) { page => assert(page.contains(s"| `reference` | 0 | ${Scenarios.all.size} |")) }
  }

  test("an adapter with no baseline file gets no results section at all") {
    withModule(None) { page => assert(!page.contains("## Conformance results")) }
  }

  test("a declaration not named invaract-capabilities-<adapter>.yaml has no baseline to find, and is not an error") {
    withModule(Some(List("conforming-write")), declarationName = "capabilities-reference.yaml") { page => assert(!page.contains("## Conformance results")) }
    withModule(Some(List("conforming-write")), declarationName = "invaract-capabilities-reference.txt") { page => assert(!page.contains("## Conformance results")) }
  }

  test("the committed page's results section matches the baselines in the repository") {
    val page = read(new File(repoRoot, "docs-site/src/content/docs/reference/engine-capabilities.md").getPath)
    val spark = scala.io.Source.fromFile(new File(repoRoot, "spark-adapter/src/test/resources/conformance-baseline-spark.txt"), "UTF-8")
    val held = try ConformanceBaseline.parse(spark.mkString).size finally spark.close()
    assert(page.contains(s"| `spark` | $held | ${Scenarios.all.size} |"))
  }
}
