// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.scalatest.funsuite.AnyFunSuite

class TrustedStepsSpec extends AnyFunSuite {

  test("a pattern names the whole step, not a part of it") {
    assert(TrustedSteps.matches("Notify", "Notify"))
    assert(!TrustedSteps.matches("Notify", "NotifyTeam"))
    assert(!TrustedSteps.matches("Notify", "Pipeline/Notify"))
    assert(!TrustedSteps.matches("Notify", "notify"), "names are compared as written")
  }

  test("* stands for any run of characters, none included, wherever it is") {
    assert(TrustedSteps.matches("Notify*", "Notify"))
    assert(TrustedSteps.matches("Notify*", "NotifyTeam/ParMultiDo(Send)"))
    assert(TrustedSteps.matches("*/LogCounts", "Report/LogCounts"))
    assert(!TrustedSteps.matches("*/LogCounts", "LogCounts"))
    assert(TrustedSteps.matches("Report*Log", "Report/Counts/Log"))
    assert(TrustedSteps.matches("*", "anything at all"))
    assert(!TrustedSteps.matches("Notify*", "PreNotify"))
  }

  test("everything other than * stands for itself, characters a regular expression gives meaning to included") {
    assert(TrustedSteps.matches("Log (counts).v2", "Log (counts).v2"))
    assert(!TrustedSteps.matches("Log.v2", "LogXv2"))
    assert(!TrustedSteps.matches("Log[ab]", "Loga"))
  }

  test("the setting is a comma-separated list, blanks and surrounding spaces ignored") {
    val trusted = VerificationSetup.trustedSteps(ConfigSource.fromMap(Map(InvaractConf.TrustedSteps -> " Notify* , ,*/LogCounts ,")))
    assert(trusted.patterns == List("Notify*", "*/LogCounts"))
    assert(trusted.trusts("NotifyTeam") && trusted.trusts("Report/LogCounts"))
    assert(!trusted.trusts("PushToService"))
  }

  test("nothing configured trusts nothing") {
    assert(TrustedSteps.from(ConfigSource.empty) == TrustedSteps.Nobody)
    assert(!TrustedSteps.Nobody.trusts("Notify") && !TrustedSteps.Nobody.trusts(""))
    assert(TrustedSteps.from(ConfigSource.fromMap(Map(InvaractConf.TrustedSteps -> " , "))).patterns.isEmpty)
  }

  test("a step is trusted when any one pattern names it") {
    val two = TrustedSteps(List("A", "B"))
    assert(two.trusts("A") && two.trusts("B") && !two.trusts("C"))
  }
}
