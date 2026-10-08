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

  def renderFiles(paths: List[String]): String = CapabilityMatrix.renderFiles(paths, Some(coverage))

  /** `args`: the output page, then each adapter's declaration file. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: CapabilityMatrixPage <output.md> <invaract-capabilities-*.yaml>...")
    Files.write(Paths.get(args.head), renderFiles(args.tail.toList).getBytes(StandardCharsets.UTF_8))
  }
}
