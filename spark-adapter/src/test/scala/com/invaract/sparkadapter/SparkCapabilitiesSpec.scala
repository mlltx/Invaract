// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{AdapterCapabilities, Capability, ContractViolationException, EnforcementPoint, Support, VerificationOptions, ViolationType}
import com.invaract.contract.{Contract, ContractParser}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** The Spark adapter's own capability declaration: that the file shipped in the jar parses and says
  * what it should, and that the declaration really does reach the verification pipeline - a contract
  * relying on something declared unsupported is rejected by a real Spark write before it executes.
  */
class SparkCapabilitiesSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratch: Path = _

  @volatile private var active: Option[(Contract, Option[AdapterCapabilities])] = None

  override def beforeAll(): Unit = {
    scratch = Files.createTempDirectory("invaract-capabilities-test")
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkCapabilitiesSpec")
      .config("spark.sql.warehouse.dir", scratch.resolve("warehouse").toString)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          active.foreach { case (contract, capabilities) =>
            ContractEnforcementRule.verifyOrThrow(contract, plan, VerificationOptions(), capabilities = capabilities)
          }
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  // --- the shipped declaration ---------------------------------------------------------------------

  test("the declaration shipped in the jar parses, declares every capability, and says Spark blocks inside the engine") {
    val caps = SparkCapabilities.declared.getOrElse(fail("invaract-capabilities-spark.yaml did not load"))
    assert(caps.adapter == "spark")
    assert(caps.engine.startsWith("Apache Spark"))
    assert(caps.enforcement == EnforcementPoint.InEngineBlocking)
    assert(caps.entries.keySet == Capability.all.toSet)
    assert(SparkCapabilities.ResourcePath == "invaract-capabilities-spark.yaml")
  }

  test("what the Spark adapter checks is declared supported, and its known limits are declared partial with a reason") {
    val caps = SparkCapabilities.declared.get
    List(Capability.CheckSchema, Capability.CheckCatalogRegistration, Capability.RulesPlanShape, Capability.FailClosedUnverifiableWrites)
      .foreach(c => assert(caps.supportOf(c) == Support.Supported, c.id))
    List(Capability.RulesDml, Capability.LineageBoundaryResolution, Capability.CheckLocation).foreach { c =>
      assert(caps.supportOf(c) == Support.Partial, c.id)
      assert(caps.entryOf(c).note.exists(_.nonEmpty), c.id)
    }
  }

  test("load returns None, not an exception, for a declaration that is missing or invalid") {
    val loader = getClass.getClassLoader
    assert(SparkCapabilities.load("invaract-capabilities-spark.yaml", loader).isDefined)
    assert(SparkCapabilities.load("capabilities-broken.yaml", loader).isEmpty)
    assert(SparkCapabilities.load("no-such-declaration.yaml", loader).isEmpty)
  }

  // --- through a real Spark write -------------------------------------------------------------------

  private def contractFor(outputPath: String): Contract = ContractParser.parse(
    s"""id: capability_demo
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: $outputPath
       |    format: parquet
       |    schema:
       |      fields:
       |        - name: id
       |          type: long
       |          required: true
       |""".stripMargin
  )

  private def writeTo(outputPath: String): Unit = spark.range(3).write.mode("overwrite").parquet(outputPath)

  test("a write passes under the shipped declaration, which supports everything this contract relies on") {
    val out = scratch.resolve("pass").toString
    active = Some(contractFor(out) -> SparkCapabilities.declared)
    try writeTo(out)
    finally active = None
    assert(Files.exists(scratch.resolve("pass")))
  }

  test("a contract relying on a capability the declaration marks unsupported is rejected before anything is written") {
    val out = scratch.resolve("blocked").toString
    val unsupported = AdapterCapabilities.parse(
      scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("invaract-capabilities-spark.yaml"), "UTF-8").mkString
        .replace("  check.format:\n    status: supported\n", "  check.format:\n    status: unsupported\n    note: pretend this engine cannot see formats\n")
    ).fold(e => fail(e.mkString("; ")), identity)
    active = Some(contractFor(out) -> Some(unsupported))
    val ex =
      try intercept[ContractViolationException](writeTo(out))
      finally active = None
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UnsupportedContractFeature))
    assert(ex.result.violations.head.expected.contains("check.format"))
    assert(ex.getMessage.contains("pretend this engine cannot see formats"))
    assert(!Files.exists(scratch.resolve("blocked")), "the write must be aborted before it executes")
  }

  test("with no declaration (resource unavailable) the capability check is skipped and enforcement is unchanged") {
    val out = scratch.resolve("nodecl").toString
    active = Some(contractFor(out) -> None)
    try writeTo(out)
    finally active = None
    assert(Files.exists(scratch.resolve("nodecl")))
  }
}
