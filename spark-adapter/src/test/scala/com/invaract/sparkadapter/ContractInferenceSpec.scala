// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser

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
  private val inferred = scala.collection.mutable.ListBuffer.empty[com.invaract.contract.Contract]

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
  private def lastInferredAfter(action: => Unit): com.invaract.contract.Contract = {
    inferred.clear()
    action
    inferred.last
  }

  /** Whether `actual` (an inferred location) and `expectedRaw`
    * (`scratchDir.resolve(...).toString`, in OS-native form) refer to the
    * same location - the exact same tolerance
    * `StructuralVerifier.locationsMatch` itself applies for a real
    * contract's declared location against a plan's actual one, deliberately
    * reused here rather than a hand-rolled byte-equality check. A plain
    * backslash swap isn't enough on Windows: Hadoop's own `Path` (the type
    * Spark's `InsertIntoHadoopFsRelationCommand.outputPath` actually is)
    * additionally renders a local drive-letter absolute path with a
    * leading slash (`C:\Users\x` becomes `/C:/Users/x`) that
    * `scratchDir.resolve(...).toString` never has - confirmed the hard way
    * by a real CI failure on windows-latest that a backslash-only fix
    * still didn't account for. Asserting the same tolerance
    * `locationsMatch` already applies, instead of trying to replicate
    * Hadoop's own path-parsing quirks here, is the right level to test at:
    * it's the actual acceptance criterion a real contract's location gets
    * checked against, and it's immune to whichever exact string Hadoop's
    * `Path` produces on a given OS.
    */
  private def sameLocation(actual: String, expectedRaw: String): Boolean = {
    val expected = expectedRaw.replace('\\', '/')
    actual == expected || actual.endsWith("/" + expected)
  }

  test("dry-run infers an output dataset matching a real write's actual location, format, save mode, and schema") {
    val outputPath = scratchDir.resolve("infer_output.parquet").toString

    val contract = lastInferredAfter {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath)
    }
    val output = contract.output("output").get
    // Not raw string equality: InsertIntoHadoopFsRelationCommand's own
    // outputPath.toString (what WriteCommandInfo.location actually carries
    // - see WriteCommandSupport) renders a local path with a "file:" scheme
    // prefix (which ContractInference strips) and, on Windows, an
    // OS-specific drive-letter form `sameLocation` already tolerates - see
    // its own doc.
    assert(sameLocation(output.location, outputPath), s"expected location matching '$outputPath', got '${output.location}'")
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
    assert(sameLocation(input.location, inputPath), s"expected location matching '$inputPath', got '${input.location}'")
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
    val actualLocations = contract.inputs.map(_.location)
    assert(
      Set(leftPath, rightPath).forall(expected => actualLocations.exists(sameLocation(_, expected))),
      s"expected locations matching '$leftPath' and '$rightPath', got $actualLocations"
    )
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
