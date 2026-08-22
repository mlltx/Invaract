// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class ContractEnforcementRuleSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  // The check rule is fixed at SparkSession construction time, but which
  // contract it enforces needs to vary per test. injectCheckRule's function
  // is invoked fresh on every analyzed plan, so a mutable cell it reads at
  // call time — not a value captured once at registration — lets one
  // session serve every test in this suite without the overhead of
  // stopping and rebuilding a SparkSession (and its SparkContext) per case.
  @volatile private var activeContract: Option[com.example.contract.Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invariant-enforcement-test")

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleSpec")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions))
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def parseContract(yaml: String) = ContractParser.parse(yaml)

  private def withContract[T](yaml: String, options: VerificationOptions = VerificationOptions())(body: => T): T = {
    activeContract = Some(parseContract(yaml))
    activeOptions = options
    try body
    finally activeContract = None
  }

  private val passingContractYaml =
    """id: enforcement_demo
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: OUTPUT_PATH
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |        - name: doubled
      |          type: long
      |          required: true
      |""".stripMargin

  test("PASS: a write satisfying its contract executes normally, output file created") {
    val outputPath = scratchDir.resolve("pass.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.write.mode("overwrite").parquet(outputPath) // must not throw
    }

    assert(Files.exists(java.nio.file.Paths.get(outputPath)))
  }

  test("FAIL: a write violating its contract is aborted before any data is written") {
    val outputPath = scratchDir.resolve("fail_missing_column.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(outputPath)), "the write must be aborted, not merely reported as failed")
    assert(ex.result.violations.exists(_.violationType == ViolationType.MissingOutputField))
  }

  test("the abort exception explains what/what/why/how — all four, not just a bare violation code") {
    val outputPath = scratchDir.resolve("fail_explain.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "2.1.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: customer_name
         |          type: string
         |          required: true
         |""".stripMargin

    val ex = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }
    }

    val message = ex.getMessage

    // What the contract expected
    assert(message.contains("What the contract expects"))
    assert(message.contains("customer_name"))
    assert(message.contains("enforcement_demo@2.1.0"))

    // What the plan contains
    assert(message.contains("What the plan contains"))
    assert(message.contains("Write("))

    // Why it violates
    assert(message.contains("Why it violates the contract"))
    assert(message.contains("MISSING_OUTPUT_FIELD"))

    // How to correct it
    assert(message.contains("How to correct it"))
    assert(message.contains("Add a 'customer_name' column"))
  }

  test("the same violation produces a byte-identical explanation every time (deterministic)") {
    val outputPath = scratchDir.resolve("fail_deterministic.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |        - name: missing_a
         |          type: string
         |          required: true
         |        - name: missing_b
         |          type: string
         |          required: true
         |""".stripMargin

    def attempt(): String = withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(outputPath)
      }.getMessage
    }

    val first = attempt()
    val second = attempt()
    val third = attempt()

    assert(first == second)
    assert(second == third)
  }

  test("non-write queries (reads, counts) never trigger verification, even against a contract they'd violate") {
    // A contract that would fail immediately if applied to *any* plan --
    // proves the check rule really does gate on "is this a Write", not on
    // "is a contract active".
    val yaml =
      """id: would_always_fail
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: nonexistent/location
        |    schema:
        |      fields:
        |        - name: impossible_field
        |          type: string
        |          required: true
        |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      df.count() // triggers analysis of Range/Project plans, not a Write
      df.collect()
    }
    // no exception means both actions completed successfully
    succeed
  }

  test("VerificationOptions thread through to enforcement: rejectUndeclaredFields turns an extra column into an abort") {
    val outputPath = scratchDir.resolve("fail_undeclared.parquet").toString
    val yaml =
      s"""id: enforcement_demo
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: $outputPath
         |    schema:
         |      fields:
         |        - name: id
         |          type: long
         |          required: true
         |""".stripMargin

    withContract(yaml) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2) // 'doubled' is undeclared
      df.write.mode("overwrite").parquet(outputPath) // permissive default: passes
    }
    assert(Files.exists(java.nio.file.Paths.get(outputPath)))

    val strictOutputPath = scratchDir.resolve("fail_undeclared_strict.parquet").toString
    val strictYaml = yaml.replace(outputPath, strictOutputPath)
    withContract(strictYaml, VerificationOptions(rejectUndeclaredFields = true)) {
      val df = spark.range(5).withColumn("doubled", col("id") * 2)
      intercept[ContractViolationException] {
        df.write.mode("overwrite").parquet(strictOutputPath)
      }
    }
    assert(!Files.exists(java.nio.file.Paths.get(strictOutputPath)))
  }

  test("forContract builds a usable check-rule function directly") {
    val outputPath = scratchDir.resolve("for_contract.parquet").toString
    val yaml = passingContractYaml.replace("OUTPUT_PATH", outputPath)
    val contract = parseContract(yaml)
    val rule = ContractEnforcementRule.forContract(contract)

    // Exercise the built function directly against a translated plan,
    // independent of SparkSession wiring: same shape injectCheckRule wants.
    val df = spark.range(5).withColumn("doubled", col("id") * 2)
    rule(spark)(df.queryExecution.analyzed) // no write command in this plan -> no-op, must not throw
  }

  // Added while raising the module's mutation-testing score (see
  // ROADMAP.md Phase 1c / CLAUDE.md): explain() is private[sparkadapter],
  // so it can be exercised directly with a synthetic result — no need to
  // provoke a real Spark abort just to check its text formatting.
  test("explain pluralizes the violation count and marks optional fields distinctly") {
    val contractYaml =
      """id: explain_demo
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |        - name: note
        |          type: string
        |          required: false
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |""".stripMargin
    val contract = parseContract(contractYaml)
    val plan = com.example.ir.Write(com.example.ir.DatasetRef("gold.out"), com.example.ir.Read(com.example.ir.DatasetRef("raw.orders")))

    val oneViolation = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(Violation(ViolationType.MissingOutputField, "msg", "remediation", column = Some("id")))
    )
    val oneText = ContractEnforcementRule.explain(contract, plan, oneViolation)
    assert(oneText.contains("(1 violation):"), oneText)
    assert(!oneText.contains("(1 violations):"), oneText)

    val twoViolations = VerificationResult.of(
      s"${contract.id}@${contract.version}",
      List(
        Violation(ViolationType.MissingOutputField, "msg1", "remediation1", column = Some("id")),
        Violation(ViolationType.MissingInputField, "msg2", "remediation2", column = Some("note"))
      )
    )
    val twoText = ContractEnforcementRule.explain(contract, plan, twoViolations)
    assert(twoText.contains("(2 violations):"), twoText)

    // "note" is declared optional (required: false); "id" is required.
    // Only the optional field's description should carry "(optional)".
    assert(oneText.contains("id: integer") && !oneText.contains("id: integer (optional)"), oneText)
    assert(oneText.contains("note: string (optional)"), oneText)
  }
}
