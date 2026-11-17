// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.nio.file.{Files, Path}

/** Under `rejectUndeclaredInputs`, a write that reads a DataFrame built from an RDD is refused: Invaract cannot see what
  * the RDD was computed from, so it cannot show the write reads only declared inputs (H-04 of
  * docs/SPARK_ADAPTER_HARDENING_PLAN.md). Without the option nothing changes.
  */
class RddBackedReadsSpec extends ConnectorSpecBase {
  private var dir: Path = _
  private def orders = dir.resolve("orders").toString
  private def secret = dir.resolve("secret").toString
  private def out = dir.resolve("out").toString
  // One registry for the session, as `forContract` installs it, so a resolved checkpoint is seen through.
  private val registry = new CheckpointRegistry

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-rdd-backed")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("RddBackedReadsSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(_.injectCheckRule { _ => (plan: LogicalPlan) =>
        activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions, None, checkpointRegistry = Some(registry)))
      })
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(dir.resolve("checkpoints").toString)
    val s = spark
    import s.implicits._
    Seq((1L, 10L), (2L, 20L)).toDF("id", "value").write.parquet(orders)
    Seq((1L, "s")).toDF("id", "ssn").write.parquet(secret)
  }

  private val contract =
    s"""id: rdd_backed
       |version: "1.0.0"
       |inputs:
       |  - { name: orders, location: ORDERS, schema: { fields: [ { name: id, type: long } ] } }
       |outputs:
       |  - { name: out, location: OUT, schema: { fields: [ { name: id, type: long } ] } }
       |""".stripMargin
  private def yaml = contract.replace("ORDERS", orders).replace("OUT", out)

  private val strict = VerificationOptions(rejectUndeclaredInputs = true)

  private def joinedWithRddOfSecret: DataFrame = {
    val sec = spark.read.parquet(secret)
    spark.read.parquet(orders).join(spark.createDataFrame(sec.rdd, sec.schema), "id").select("id")
  }

  test("under rejectUndeclaredInputs, a write reading an RDD-backed DataFrame is refused, and nothing is written") {
    val e = intercept[ContractViolationException](withContract(yaml, strict)(joinedWithRddOfSecret.write.mode("overwrite").parquet(out)))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("built from an RDD") && e.getMessage.contains("spark.read or spark.table"))
    assert(!Files.exists(dir.resolve("out")))
  }

  test("without rejectUndeclaredInputs the same write passes, as before") {
    withContract(yaml)(joinedWithRddOfSecret.write.mode("overwrite").parquet(out)) // must not throw
  }

  test("under rejectUndeclaredInputs, a write with no RDD-backed source passes") {
    withContract(yaml, strict)(spark.read.parquet(orders).select("id").write.mode("overwrite").parquet(out))
  }

  test("a lazy checkpoint the registry resolves is not an RDD-backed source: it is seen through to its reads") {
    // Not yet materialized, a lazy checkpoint is a plain RDD to Spark (`isCheckpointed` is false): unresolved, it would be
    // the RDD-backed source this refuses.
    // Built while the rule is active, as under `forContract`, which sees every plan: the registry records the plan the
    // checkpoint is made from, and binds the checkpoint to it.
    withContract(yaml, strict) {
      val checkpointed = spark.read.parquet(orders).select("id").checkpoint(eager = false)
      assert(SparkPlanAdapter.translate(checkpointed.queryExecution.analyzed).plan.toString.contains(SparkPlanAdapter.RddRelationSourceType))
      checkpointed.write.mode("overwrite").parquet(out) // must not throw
    }
  }
}
