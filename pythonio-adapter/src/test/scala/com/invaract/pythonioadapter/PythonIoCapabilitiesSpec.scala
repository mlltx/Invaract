// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.verification.{Capability, Support}

import org.scalatest.funsuite.AnyFunSuite

/** The declaration shipped in the jar parses and covers every capability the vocabulary has. */
class PythonIoCapabilitiesSpec extends AnyFunSuite {

  test("the shipped declaration loads and names this adapter") {
    val declared = PythonIoCapabilities.declared
    assert(declared.isDefined, "invaract-capabilities-pythonio.yaml must be on the classpath and valid")
    assert(declared.get.adapter == "pythonio")
  }

  test("every capability in the vocabulary has a declared status") {
    val declared = PythonIoCapabilities.declared.get
    Capability.all.foreach(c => assert(Set[Support](Support.Supported, Support.Partial, Support.Unsupported, Support.NotApplicable).contains(declared.supportOf(c)), c.id))
  }

  test("a missing declaration yields None instead of throwing") {
    assert(PythonIoCapabilities.load("no-such-declaration.yaml", getClass.getClassLoader).isEmpty)
  }
}
