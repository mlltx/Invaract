// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Covers `ContractEnforcementRule.dryRun`/`ContractInference` - dry-run
  * mode's "no contract supplied, infer what one would look like" path,
  * the mirror image of `ContractEnforcementRuleSpec`'s "a contract is
  * supplied, enforce it" coverage. Installed via the same `injectCheckRule`
  * mechanism as `forContract` (see `dryRun`'s own doc for why), so this
  * spec builds its session the same way `ContractEnforcementRuleSpec` does.
  */
class ContractInferenceSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  // dryRun's callback fires once per analyzed plan that translates to a
  // recognized write (see its own doc for why that can be more than once
  // per DataFrame-level write call) - captured in order so a test can
  // assert on "the last one inferred" the same way a real caller would.
  private val inferred = scala.collection.mutable.ListBuffer.empty[com.example.contract.Contract]

  // Every analyzed plan the session produces, alongside `inferred` above -
  // lets a test re-run real enforcement (verifyOrThrow) against the exact
  // plan an inferred contract came from, the strongest possible check that
  // an inferred contract is actually usable, not just plausible-looking.
  private val capturedPlans = scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-inference-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractInferenceSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          capturedPlans += plan
          ContractEnforcementRule.inferOrIgnore(plan, c => inferred += c)
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  /** Runs `action` (expected to trigger exactly one recognized write) and
    * returns the last contract `dryRun`'s callback inferred from it - the
    * `inferred.clear()` / act / `inferred.last` shape every test below
    * needs, factored out since it was otherwise repeated verbatim five
    * times with only `action` varying.
    */
  private def lastInferredAfter(action: => Unit): com.example.contract.Contract = {
    inferred.clear()
    action
    inferred.last
  }

  /** `scratchDir.resolve(...).toString` renders OS-native separators
    * (backslashes on Windows), but `ContractInference` always normalizes an
    * inferred location to forward slashes (via
    * `StructuralVerifier.normalizeSparkLocation`, matching Spark's own
    * platform-independent reporting) - confirmed the hard way by a real
    * CI failure on windows-latest, not assumed, when these tests compared
    * against the raw, backslash-containing `Path.toString()` value
    * directly. Every exact-equality location assertion below normalizes
    * its expected value the same way first.
    */
  private def normalizedPath(path: String): String = path.replace('\\', '/')

  test("dry-run infers an output dataset matching a real write's actual location, format, save mode, and schema") {
    val outputPath = scratchDir.resolve("infer_output.parquet").toString

    val contract = lastInferredAfter {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath)
    }
    val output = contract.output("output").get
    // Exact equality, not just endsWith: InsertIntoHadoopFsRelationCommand's
    // own outputPath.toString (what WriteCommandInfo.location actually
    // carries - see WriteCommandSupport) renders a local path with a
    // "file:" scheme prefix, which ContractInference must strip so the
    // inferred location matches the bare-path form a hand-authored
    // contract declares (see ContractInference.normalizeLocation's doc).
    assert(output.location == normalizedPath(outputPath), s"expected location '$outputPath', got '${output.location}'")
    assert(output.format.contains("parquet"))
    assert(output.saveMode.contains("overwrite"))
    assert(output.schema.fields.map(_.name) == List("id", "doubled"))
    assert(output.schema.fields.forall(_.required), "every observed output field should be inferred as required")
    assert(output.schema.field("id").get.fieldType == "long")
  }

  test("dry-run never throws, even though the write's shape would violate a hypothetical strict contract") {
    val outputPath = scratchDir.resolve("infer_never_throws.parquet").toString
    inferred.clear()

    // No contract exists in dry-run mode to violate - this must just work,
    // proving dryRun genuinely never gates anything, unlike forContract.
    val df = spark.range(3)
    df.write.mode("overwrite").parquet(outputPath) // must not throw
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
    assert(inferred.nonEmpty)
  }

  test("dry-run infers input datasets from a real read the write depends on") {
    val inputPath = scratchDir.resolve("infer_input_source.parquet").toString
    val outputPath = scratchDir.resolve("infer_input_output.parquet").toString
    spark.range(5).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(inputPath)

    val contract = lastInferredAfter {
      val df = spark.read.parquet(inputPath).select("id")
      df.write.mode("overwrite").parquet(outputPath)
    }
    assert(contract.inputs.size == 1)
    val input = contract.input("input").get
    assert(input.location == normalizedPath(inputPath), s"expected location '$inputPath', got '${input.location}'")
    assert(input.schema.fields.map(_.name).toSet == Set("id", "doubled"))
  }

  test("dry-run names multiple inputs input_1/input_2 when a write depends on more than one source") {
    val leftPath = scratchDir.resolve("infer_multi_left.parquet").toString
    val rightPath = scratchDir.resolve("infer_multi_right.parquet").toString
    val outputPath = scratchDir.resolve("infer_multi_output.parquet").toString
    spark.range(3).write.mode("overwrite").parquet(leftPath)
    spark.range(3).write.mode("overwrite").parquet(rightPath)

    val contract = lastInferredAfter {
      val left = spark.read.parquet(leftPath).withColumnRenamed("id", "left_id")
      val right = spark.read.parquet(rightPath).withColumnRenamed("id", "right_id")
      val joined = left.crossJoin(right)
      joined.write.mode("overwrite").parquet(outputPath)
    }
    assert(contract.inputs.size == 2)
    assert(contract.inputs.map(_.name).toSet == Set("input_1", "input_2"))
    assert(contract.inputs.map(_.location).toSet == Set(normalizedPath(leftPath), normalizedPath(rightPath)))
  }

  test("dry-run infers a contract with no rules and no extensions - never fabricates business intent") {
    val outputPath = scratchDir.resolve("infer_no_rules.parquet").toString

    val contract = lastInferredAfter {
      spark.range(5).write.mode("overwrite").parquet(outputPath)
    }
    assert(contract.rules.isEmpty)
    assert(contract.extensions.isEmpty)
    assert(contract.status == "draft")
  }

  test("dry-run ignores a plain read/transformation with no write at all") {
    inferred.clear()
    spark.range(5).withColumn("doubled", col("id") * 2).count() // triggers analyzed plans, but no write

    assert(inferred.isEmpty, "a count() with no write must not produce an inferred contract")
  }

  test("an inferred contract round-trips through ContractParser.write/parse and passes real enforcement of the write it came from") {
    val outputPath = scratchDir.resolve("infer_roundtrip.parquet").toString
    capturedPlans.clear()

    val contract = lastInferredAfter {
      spark.range(5).withColumn("doubled", col("id") * 2).write.mode("overwrite").parquet(outputPath)
    }
    val roundTripped = ContractParser.parse(ContractParser.write(contract))
    assert(roundTripped == contract)

    // The inferred contract, unmodified, must describe the exact write that
    // produced it precisely enough to pass real enforcement - proving this
    // is a genuinely usable starting point (usable with forContract as-is),
    // not just a plausible-looking document. Re-verifying against the same
    // captured plan (rather than a hand-built IR/schema) is the strongest
    // check available: any mismatch between what was inferred and what
    // StructuralVerifier actually expects would show up as a real
    // violation here.
    val writePlan = capturedPlans.find(WriteCommandSupport.combined.isDefinedAt)
      .getOrElse(fail("no recognized write plan was captured"))
    ContractEnforcementRule.verifyOrThrow(roundTripped, writePlan, VerificationOptions()) // must not throw
  }
}
