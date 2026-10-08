// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability, Support}

import org.scalatest.Suite
import org.scalatest.funsuite.AnyFunSuite

/** The adapter's own test that demonstrates an attested claim: a ScalaTest suite and the name of one test in it. */
final case class Attestation(suite: String, test: String)

/** Some capabilities cannot be shown by running a neutral job (`Scenarios.attested`): how an adapter attaches to
  * a job, what it applies before any job exists, a mode of installing. The adapter's own tests carry those claims.
  * Left to each adapter's judgement that is where claims go unchecked, so this makes the shape standard: for every
  * attested capability an adapter declares `supported` or `partial`, it names the test that demonstrates it, and
  * the kit checks the test exists. `requirements` says what that test must show - the review checklist for the
  * named test, which the kit cannot judge.
  */
object AttestedClaims {

  /** What a test attesting each capability must demonstrate. Keyed exactly by `Scenarios.attested`. */
  val requirements: Map[Capability, String] = Map(
    Capability.PolicyOrganizational ->
      "a contract that breaks an organizational policy is rejected when enforcement is set up, before any job runs; one that satisfies it installs",
    Capability.ConfigZeroCodeInstall ->
      "enforcement is installed on a job through the engine's own configuration alone, with no change to the job's source, and then blocks a violating write",
    Capability.ConfigLocationRefs ->
      "a ref://<id> location in a contract is resolved through configuration, and a real write to the resolved location is checked against it",
    Capability.ConfigContractRegistry ->
      "a registry://<id>@<version> contract reference is fetched from the registry named in configuration and enforced",
    Capability.ReportingDryRun ->
      "dry-run mode lets a real write through and reports a contract inferred from it",
    Capability.WriteStateChange ->
      "an operation that commits a schema change without writing rows is checked against the contract's output, and blocked when it violates it",
    Capability.RulesCustom ->
      "a custom rule type named in the contract resolves, and its verdict blocks a write that breaks it"
  )

  /** Whether `attestation` names a test that exists: the suite loads, is a ScalaTest `Suite`, and has a test of that exact name. */
  def exists(attestation: Attestation, loader: ClassLoader): Either[String, Unit] =
    try {
      val cls = Class.forName(attestation.suite, false, loader)
      if (!classOf[Suite].isAssignableFrom(cls)) Left(s"${attestation.suite} is not a ScalaTest suite")
      else {
        val suite = cls.getDeclaredConstructor().newInstance().asInstanceOf[Suite]
        if (suite.testNames.contains(attestation.test)) Right(())
        else Left(s"${attestation.suite} has no test named '${attestation.test}'")
      }
    } catch {
      case _: ClassNotFoundException => Left(s"no suite named ${attestation.suite}")
      case e: Exception              => Left(s"could not inspect ${attestation.suite}: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  /** What is wrong with `attestations` for an adapter declared as `capabilities`; empty when nothing is. */
  def problems(capabilities: AdapterCapabilities, attestations: Map[Capability, Attestation], loader: ClassLoader): List[String] = {
    val notAttestable = attestations.keySet.filterNot(Scenarios.attested.contains).toList.sortBy(_.id).map { c =>
      s"${c.id} is not an attested capability: the kit checks it with a scenario, so it has nothing to attest"
    }
    val perCapability = Scenarios.attested.keys.toList.sortBy(_.id).flatMap { c =>
      val claimed = Set[Support](Support.Supported, Support.Partial).contains(capabilities.supportOf(c))
      attestations.get(c) match {
        case None if claimed => List(s"${c.id} is declared ${capabilities.supportOf(c).id} but names no test that demonstrates it; it must show: ${requirements(c)}")
        case Some(_) if !claimed => List(s"${c.id} is declared ${capabilities.supportOf(c).id} but has an attestation: remove it, or declare the capability")
        case Some(a) => exists(a, loader).left.toOption.map(why => s"${c.id}: $why").toList
        case None => Nil
      }
    }
    notAttestable ++ perCapability
  }
}

/** Mix this into an adapter's tests so every attested claim it makes names a real test:
  *
  * {{{
  * class MyEngineAttestedClaimsSpec extends AttestedClaimsSpec {
  *   override protected def capabilities = MyEngineCapabilities.declared
  *   override protected def attestations = Map(
  *     Capability.ConfigZeroCodeInstall -> Attestation("com.example.MyInstallSpec", "a violating write is blocked")
  *   )
  * }
  * }}}
  *
  * One test per attested capability, so a failure names it. A capability the adapter does not claim is
  * canceled with that reason.
  */
trait AttestedClaimsSpec extends AnyFunSuite {

  /** The adapter's declaration. */
  protected def capabilities: AdapterCapabilities

  /** For each attested capability the adapter claims, the test that demonstrates it. */
  protected def attestations: Map[Capability, Attestation]

  Scenarios.attested.keys.toList.sortBy(_.id).foreach { capability =>
    test(s"attested: ${capability.id} - ${AttestedClaims.requirements(capability)}") {
      val problems = AttestedClaims.problems(capabilities, attestations, getClass.getClassLoader).filter(_.startsWith(capability.id))
      assert(problems.isEmpty, problems.mkString("; "))
      val support = capabilities.supportOf(capability)
      if (!Set[Support](Support.Supported, Support.Partial).contains(support)) cancel(s"declared ${support.id}: nothing to attest")
    }
  }

  test("attested: nothing is attested that the kit checks with a scenario") {
    val extra = attestations.keySet.filterNot(Scenarios.attested.contains)
    assert(extra.isEmpty, extra.map(_.id).toList.sorted.mkString(", "))
  }
}
