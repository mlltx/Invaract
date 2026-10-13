// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{Capability, Support}

import org.scalatest.funsuite.AnyFunSuite

/** The declaration shipped in the jar parses and covers every capability the vocabulary has. */
class BeamCapabilitiesSpec extends AnyFunSuite {

  test("the shipped declaration loads and names this adapter") {
    val declared = BeamCapabilities.declared
    assert(declared.isDefined, "invaract-capabilities-beam.yaml must be on the classpath and valid")
    assert(declared.get.adapter == "beam")
  }

  test("every capability in the vocabulary has a declared status") {
    val declared = BeamCapabilities.declared.get
    Capability.all.foreach(c => assert(Set[Support](Support.Supported, Support.Partial, Support.Unsupported, Support.NotApplicable).contains(declared.supportOf(c)), c.id))
  }

  test("a missing declaration yields None instead of throwing") {
    assert(BeamCapabilities.load("no-such-declaration.yaml", getClass.getClassLoader).isEmpty)
  }
}
