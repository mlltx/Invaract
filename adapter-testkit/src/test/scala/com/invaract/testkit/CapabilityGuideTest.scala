// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{Capability, CapabilityCategory, EnforcementPoint}
import org.scalatest.funsuite.AnyFunSuite

class CapabilityGuideTest extends AnyFunSuite {

  test("every capability has plain words, and nothing else does: a new capability fails here until it is explained") {
    assert(CapabilityGuide.entries.keySet == Capability.all.toSet)
    Capability.all.foreach { c =>
      val e = CapabilityGuide.of(c)
      assert(e.title.nonEmpty && e.what.nonEmpty, c.id)
    }
  }

  test("every category and every enforcement point has its plain words") {
    assert(CapabilityGuide.categoryTitles.keySet == CapabilityCategory.all.toSet)
    assert(CapabilityGuide.enforcement.keySet == EnforcementPoint.all.toSet)
  }

  test("titles, and the anchors the page links to them by, are unique") {
    val titles = Capability.all.map(CapabilityGuide.of(_).title)
    assert(titles.distinct.size == titles.size)
    val anchors = titles.map(EngineGuidePage.anchor)
    assert(anchors.distinct.size == anchors.size)
  }

  test("a check that guards the contract says the job is refused, and when; any other says what goes missing") {
    Capability.all.foreach { c =>
      val (without, short) = (CapabilityGuide.withoutIt(c), CapabilityGuide.shortWithoutIt(c))
      val when = CapabilityGuide.of(c).refusedWhen
      if (c.enforcesContract) {
        assert(without == s"If $when, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked.", c.id)
        assert(short == s"Refused if $when.", c.id)
      } else {
        assert(without == CapabilityGuide.of(c).withoutIt && short == without, c.id)
        assert(without.nonEmpty && !without.contains("UNSUPPORTED_CONTRACT_FEATURE"), c.id)
      }
    }
    // an opt-in analysis is refused when it is turned on, not because the contract mentions it
    assert(CapabilityGuide.of(Capability.AnalysisStaticDataQuality).refusedWhen == "you turn on `staticDataQuality`")
    assert(CapabilityGuide.of(Capability.AnalysisRoleConsistency).refusedWhen == "you turn on `roleConsistency`")
    assert(CapabilityGuide.of(Capability.CheckSchema).refusedWhen == "your contract relies on it")
  }

  test("guide links are root-relative pages, as the docs site writes them") {
    Capability.all.flatMap(c => CapabilityGuide.of(c).guide.map(c.id -> _)).foreach { case (id, g) =>
      assert(g.startsWith("/") && (g.endsWith("/") || g.contains("/#")), s"$id: $g")
    }
    assert(CapabilityGuide.of(Capability.WriteStateChange).guide.isEmpty, "a capability with no guide links nowhere")
  }
}
