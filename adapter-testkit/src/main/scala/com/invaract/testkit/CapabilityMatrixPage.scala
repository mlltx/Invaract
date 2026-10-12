// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{CapabilityMatrix, SuiteCoverage}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** The engine-capabilities docs page with the conformance suite's own coverage folded in: for each capability, whether
  * scenarios verify it, it is attested, or it is a known gap. `verification-core` renders the page but does not know the
  * scenarios; this is the one place that does, so it is the generator (`./dev/capabilities` runs `main`) and its
  * drift test lives with the kit (`CapabilityMatrixPageSpec`).
  */
object CapabilityMatrixPage {

  /** What the catalogue of scenarios says about every capability. */
  def coverage: SuiteCoverage =
    SuiteCoverage(
      verified = Scenarios.all
        .flatMap(s => (s.focus ++ s.operations).map(_ -> s.id))
        .groupBy(_._1)
        .map { case (capability, pairs) => capability -> pairs.map(_._2).distinct.sorted },
      attested = Scenarios.attested,
      gaps = Scenarios.gaps
    )

  def renderFiles(paths: List[String]): String = withResults(CapabilityMatrix.renderFiles(paths, Some(coverage)), paths)

  /** Each declaration is `<module>/src/main/resources/invaract-capabilities-<adapter>.yaml`; the adapter's checked-in
    * baseline (`ConformanceBaseline`) is `<module>/src/test/resources/conformance-baseline-<adapter>.txt`. */
  private def baselineOf(declaration: String): Option[(String, Set[String])] = {
    val file = Paths.get(declaration).toAbsolutePath.normalize
    val name = file.getFileName.toString
    val prefix = "invaract-capabilities-"
    if (!name.startsWith(prefix) || !name.endsWith(".yaml") || file.getNameCount < 5) None
    else {
      val adapter = name.stripPrefix(prefix).stripSuffix(".yaml")
      val module = file.getParent.getParent.getParent.getParent
      val baseline = module.resolve("src").resolve("test").resolve("resources").resolve(ConformanceBaseline.resource(adapter))
      if (Files.exists(baseline)) Some(adapter -> ConformanceBaseline.parse(new String(Files.readAllBytes(baseline), StandardCharsets.UTF_8)))
      else None
    }
  }

  /** The page with a section saying, for each adapter that has a baseline, how many of the suite's scenarios it is held
    * to conform to. A declaration says what an adapter promises; this says how much of that a real run has shown. */
  private def withResults(page: String, paths: List[String]): String = {
    val baselines = paths.flatMap(baselineOf).sortBy(_._1)
    if (baselines.isEmpty) page
    else {
      val total = Scenarios.all.size
      val section = new StringBuilder("## Conformance results\n\n")
      section.append(s"The suite has $total scenarios. For each adapter, the scenarios it is held to conform to: its checked-in baseline, ")
      section.append("which its own test run must match exactly (a scenario that stops conforming fails the build, and one that starts must be added).\n\n")
      section.append("| Adapter | Scenarios conforming | Of |\n|---|---|---|\n")
      baselines.foreach { case (adapter, ids) => section.append(s"| `$adapter` | ${ids.size} | $total |\n") }
      section.append("\n")
      val at = page.indexOf("## Notes\n")
      if (at < 0) page + section else page.substring(0, at) + section + page.substring(at)
    }
  }

  /** `args`: the output page, then each adapter's declaration file. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: CapabilityMatrixPage <output.md> <invaract-capabilities-*.yaml>...")
    Files.write(Paths.get(args.head), renderFiles(args.tail.toList).getBytes(StandardCharsets.UTF_8))
  }
}
