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

/** Tests `Contract.customRuleTypes`'s wiring through `ContractEnforcementRule`
  * — see docs/SPARK_ADAPTER.md's "Custom rule types" section for the design.
  * `RuleVerifier`'s own dispatch logic (built-in-wins, unresolvable-is-inert,
  * exceptions propagate) is already exhaustively covered in `RuleVerifierSpec`
  * — these tests are about the wiring: does a real UPDATE actually get
  * blocked by a reflectively-resolved `CustomRuleVerifier`, and does a
  * contract naming an unresolvable class fail loudly at validation time,
  * before any plan is even checked (mirroring `requireValidContract`'s
  * existing "outputs must be non-empty" coverage in `ContractEnforcementRuleSpec`).
  * Kept in its own file, the same way `ContractEnforcementRuleOrgPolicySpec`
  * is kept separate from the (very large) main spec.
  */
class ContractEnforcementRuleCustomRuleTypesSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  @volatile private var activeContract: Option[com.invaract.contract.Contract] = None

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-custom-rule-types-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractEnforcementRuleCustomRuleTypesSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, VerificationOptions(), None))
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def parseContract(yaml: String) = ContractParser.parse(yaml)

  private def withContract[T](yaml: String)(body: => T): T = {
    activeContract = Some(parseContract(yaml))
    try body
    finally activeContract = None
  }

  private val forbidPasswordClassName = classOf[ForbidPasswordColumnUpdateVerifier].getName

  private def contractYaml(tablePath: String, customRuleTypesYaml: String, rulesYaml: String): String =
    s"""id: custom_rule_types_demo
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: $tablePath
       |    schema:
       |      fields:
       |        - name: id
       |          type: long
       |          required: false
       |        - name: password
       |          type: string
       |          required: false
       |$customRuleTypesYaml
       |$rulesYaml
       |""".stripMargin

  // -- a real DML write, blocked or allowed by a reflectively-resolved
  // CustomRuleVerifier, exactly the way a built-in rule type already is ---

  test("PASS: an UPDATE not touching the disallowed column satisfies a custom rule type's own check") {
    val tablePath = scratchDir.resolve("custom_rule_pass_target").toString
    val tableName = "custom_rule_pass_tbl"
    spark.range(3).withColumn("password", lit("hunter2")).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml = contractYaml(
      tablePath,
      s"""customRuleTypes:
         |  forbid_password_update: $forbidPasswordClassName
         |""".stripMargin,
      """rules:
        |  - type: forbid_password_update
        |""".stripMargin
    )

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET id = id + 100 WHERE id = 0").collect() // must not throw
    }

    assert(spark.table(tableName).count() == 3, "the UPDATE must actually have run")
  }

  test("FAIL: an UPDATE assigning the disallowed column is aborted before touching the table") {
    val tablePath = scratchDir.resolve("custom_rule_fail_target").toString
    val tableName = "custom_rule_fail_tbl"
    spark.range(3).withColumn("password", lit("hunter2")).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")
    val beforeRows = spark.read.format("delta").load(tablePath).collect().toSet

    val yaml = contractYaml(
      tablePath,
      s"""customRuleTypes:
         |  forbid_password_update: $forbidPasswordClassName
         |""".stripMargin,
      """rules:
        |  - type: forbid_password_update
        |""".stripMargin
    )

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.sql(s"UPDATE $tableName SET password = 'leaked' WHERE id = 0").collect()
      }
    }

    assert(
      ex.result.violations.exists(v => v.violationType == ViolationType.InvalidContract && v.message.contains("password")),
      s"expected a violation naming 'password', got ${ex.result.violations}"
    )
    val afterRows = spark.read.format("delta").load(tablePath).collect().toSet
    assert(beforeRows == afterRows, "the UPDATE must be aborted before touching the table, not merely reported as failed")
  }

  // -- eager resolution: a contract naming an unresolvable custom rule
  // type's class fails loudly, before any plan is ever checked --------------

  test("a contract whose customRuleTypes entry names an unresolvable class throws before touching the table") {
    val tablePath = scratchDir.resolve("custom_rule_unresolvable_target").toString
    val yaml = contractYaml(
      tablePath,
      """customRuleTypes:
        |  totally_unrecognized_type: com.invaract.sparkadapter.NoSuchClassAtAll
        |""".stripMargin,
      """rules:
        |  - type: totally_unrecognized_type
        |""".stripMargin
    )

    val ex = withContract(yaml) {
      intercept[ContractViolationException] {
        spark.range(2).withColumn("password", lit("hunter2")).write.mode("overwrite").parquet(tablePath)
      }
    }

    assert(!Files.exists(java.nio.file.Paths.get(tablePath)), "the write must be aborted, not merely reported as failed")
    assert(
      ex.result.violations.exists(v =>
        v.violationType == ViolationType.InvalidContract &&
          v.message.contains("totally_unrecognized_type") &&
          v.message.contains("NoSuchClassAtAll")
      ),
      s"expected a violation naming the unresolvable class, got ${ex.result.violations}"
    )
  }

  test("a contract whose customRuleTypes entry names a real, resolvable class installs without incident") {
    val tablePath = scratchDir.resolve("custom_rule_resolvable_target").toString
    val tableName = "custom_rule_resolvable_tbl"
    spark.range(2).withColumn("password", lit("hunter2")).write.format("delta").mode("overwrite").save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING delta LOCATION '${tablePath.replace('\\', '/')}'")

    val yaml = contractYaml(
      tablePath,
      s"""customRuleTypes:
         |  forbid_password_update: $forbidPasswordClassName
         |""".stripMargin,
      """rules:
        |  - type: forbid_password_update
        |""".stripMargin
    )

    withContract(yaml) {
      spark.sql(s"UPDATE $tableName SET id = id + 1 WHERE id = 0").collect() // must not throw
    }
  }
}
