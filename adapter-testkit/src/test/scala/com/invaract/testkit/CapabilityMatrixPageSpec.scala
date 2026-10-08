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

  test("every capability is verified, attested or a gap - never silently absent from the page's suite column") {
    val c = CapabilityMatrixPage.coverage
    Capability.all.foreach { cap =>
      assert(c.verified.contains(cap) || c.attested.contains(cap) || c.gaps.contains(cap), cap.id)
    }
  }

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
}
