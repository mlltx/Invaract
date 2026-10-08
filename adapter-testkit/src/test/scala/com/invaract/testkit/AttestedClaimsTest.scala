// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability}

import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

/** A suite whose test names the attestation tests point at. */
class AttestedFixtureSuite extends AnyFunSuite {
  test("shows the thing") { succeed }
}

/** A "suite" whose construction fails, as a suite that needs something unavailable would. */
@DoNotDiscover
class UnconstructableSuite extends AnyFunSuite {
  throw new IllegalStateException("needs an engine that is not here")
}

class AttestedClaimsTest extends AnyFunSuite {

  private val fixture = "com.invaract.testkit.AttestedFixtureSuite"
  private val loader = getClass.getClassLoader
  private val good = Attestation(fixture, "shows the thing")

  /** The reference declaration with every attested capability set to `status`. */
  private def declaringAttested(status: String): AdapterCapabilities = {
    val base = scala.io.Source.fromInputStream(loader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    val ids = Scenarios.attested.keys.map(_.id).toSet
    val out = new StringBuilder
    var skip = false
    base.split("\n").foreach { line =>
      val key = line.trim.stripSuffix(":")
      if (line.startsWith("  ") && !line.startsWith("    ") && line.trim.endsWith(":")) {
        if (ids.contains(key)) { out.append(s"  $key:\n    status: $status\n" + (if (status == "supported") "" else "    note: a test fixture\n")); skip = true } else { out.append(line + "\n"); skip = false }
      } else if (!skip) out.append(line + "\n")
    }
    AdapterCapabilities.parse(out.toString).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }

  private def allAttested(a: Attestation): Map[Capability, Attestation] = Scenarios.attested.keys.map(_ -> a).toMap

  test("every attested capability says what its test must show, and nothing else does") {
    assert(AttestedClaims.requirements.keySet == Scenarios.attested.keySet)
    assert(AttestedClaims.requirements.values.forall(_.trim.nonEmpty))
  }

  test("an attestation naming a real test of a real suite is accepted") {
    assert(AttestedClaims.exists(good, loader) == Right(()))
  }

  test("an attestation naming a test the suite does not have, a missing suite, or a non-suite is rejected with the reason") {
    assert(AttestedClaims.exists(good.copy(test = "renamed"), loader).left.exists(_.contains("no test named 'renamed'")))
    assert(AttestedClaims.exists(Attestation("com.example.NoSuchSuite", "x"), loader).left.exists(_.contains("no suite named")))
    assert(AttestedClaims.exists(Attestation("java.lang.String", "x"), loader).left.exists(_.contains("not a ScalaTest suite")))
    assert(AttestedClaims.exists(Attestation("com.invaract.testkit.UnconstructableSuite", "x"), loader).left.exists(_.contains("could not inspect")))
  }

  test("an adapter claiming every attested capability with a real test has no problems") {
    assert(AttestedClaims.problems(declaringAttested("supported"), allAttested(good), loader) == Nil)
    assert(AttestedClaims.problems(declaringAttested("partial"), allAttested(good), loader) == Nil)
  }

  test("a claim with no attestation names the capability and what its test must show") {
    val problems = AttestedClaims.problems(declaringAttested("supported"), Map.empty, loader)
    assert(problems.size == Scenarios.attested.size)
    Scenarios.attested.keys.foreach { c =>
      assert(problems.exists(p => p.startsWith(c.id) && p.contains(AttestedClaims.requirements(c))), c.id)
    }
  }

  test("an attestation for a capability the adapter does not claim is a problem, so a stale one cannot linger") {
    Seq("unsupported", "not-applicable").foreach { status =>
      val problems = AttestedClaims.problems(declaringAttested(status), allAttested(good), loader)
      assert(problems.size == Scenarios.attested.size, status)
      assert(problems.forall(_.contains("remove it")), status)
    }
  }

  test("an attestation that points at a missing test is reported against its capability") {
    val problems = AttestedClaims.problems(declaringAttested("supported"), allAttested(good.copy(test = "gone")), loader)
    assert(problems.size == Scenarios.attested.size)
    assert(problems.forall(_.contains("no test named 'gone'")))
  }

  test("an attestation for a capability a scenario checks is a problem: there is nothing to attest") {
    val problems = AttestedClaims.problems(declaringAttested("unsupported"), Map(Capability.CheckFormat -> good), loader)
    assert(problems.exists(p => p.startsWith("check.format") && p.contains("not an attested capability")))
  }

  test("nothing is attested when nothing is claimed") {
    assert(AttestedClaims.problems(declaringAttested("unsupported"), Map.empty, loader) == Nil)
  }
}

/** Records which of the classes below ran any code; they are named by string, never referenced. */
object AttestedFlags { @volatile var staticInitRan = false }

/** Not a suite, with a static initializer that would record itself if the class were initialized. */
object NotASuiteWithStaticInit { AttestedFlags.staticInitRan = true }

/** The mixin the adapters use, driven by hand so its pass, fail and cancel paths are all seen. */
class AttestedClaimsMixinTest extends AnyFunSuite {
  import org.scalatest.{Args, Reporter}
  import org.scalatest.events.{Event, TestCanceled, TestFailed, TestSucceeded}

  private class Recorder extends Reporter {
    val events = scala.collection.mutable.ListBuffer.empty[Event]
    override def apply(event: Event): Unit = events += event
    def succeeded: List[String] = events.toList.collect { case e: TestSucceeded => e.testName }
    def failed: List[String] = events.toList.collect { case e: TestFailed => e.testName }
    def canceled: List[String] = events.toList.collect { case e: TestCanceled => e.testName }
  }

  private val fixture = "com.invaract.testkit.AttestedFixtureSuite"
  private def declared(status: String): AdapterCapabilities = {
    val base = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    val ids = Scenarios.attested.keys.map(_.id).toSet
    val out = new StringBuilder
    var skip = false
    base.split("\n").foreach { line =>
      val key = line.trim.stripSuffix(":")
      if (line.startsWith("  ") && !line.startsWith("    ") && line.trim.endsWith(":")) {
        if (ids.contains(key)) { out.append(s"  $key:\n    status: $status\n" + (if (status == "supported") "" else "    note: a test fixture\n")); skip = true }
        else { out.append(line + "\n"); skip = false }
      } else if (!skip) out.append(line + "\n")
    }
    AdapterCapabilities.parse(out.toString).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }

  private def runSpec(status: String, named: Map[Capability, Attestation]): Recorder = {
    val recorder = new Recorder
    new AttestedClaimsSpec {
      override protected def capabilities: AdapterCapabilities = declared(status)
      override protected def attestations: Map[Capability, Attestation] = named
    }.run(None, Args(recorder))
    recorder
  }

  private val perCapability = Scenarios.attested.size

  test("every claim naming a real test passes, and the spec adds one check that nothing scenario-checked is attested") {
    val all = Scenarios.attested.keys.map(_ -> Attestation(fixture, "shows the thing")).toMap
    val r = runSpec("supported", all)
    assert(r.failed == Nil && r.canceled == Nil)
    assert(r.succeeded.size == perCapability + 1)
  }

  test("claims with no test fail one test per capability, naming it") {
    val r = runSpec("supported", Map.empty)
    assert(r.failed.size == perCapability)
    assert(r.succeeded.size == 1)
  }

  test("capabilities the adapter does not claim are canceled, not failed or passed") {
    val r = runSpec("unsupported", Map.empty)
    assert(r.canceled.size == perCapability && r.failed == Nil)
  }

  test("an attestation for something a scenario checks fails the last check") {
    val r = runSpec("unsupported", Map(Capability.CheckFormat -> Attestation(fixture, "shows the thing")))
    assert(r.failed.exists(_.contains("nothing is attested that the kit checks with a scenario")))
  }

  test("naming a class that is not a suite does not initialize it") {
    AttestedFlags.staticInitRan = false
    val result = AttestedClaims.exists(Attestation("com.invaract.testkit.NotASuiteWithStaticInit$", "x"), getClass.getClassLoader)
    assert(result.left.exists(_.contains("not a ScalaTest suite")))
    assert(!AttestedFlags.staticInitRan, "naming a class must not initialize it")
  }
}
