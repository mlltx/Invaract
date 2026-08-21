// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser
import com.example.ir.{ColumnLineage, ColumnRef, Lineage}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractVerifierSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("ContractVerifierSpec").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def realDemoContract() =
    ContractParser.parseFile(new File("../demo/contracts/invariant_output.yaml"))

  private def realDemoOutput() = {
    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../demo/input/sample.csv")
    orders.withColumn("value_squared", col("value") * col("value"))
  }

  test("passes against the real demo pipeline's actual output: schema and lineage both satisfy the contract") {
    val contract = realDemoContract()
    val outputDf = realDemoOutput()

    val plan = SparkPlanAdapter.translateAsWrite(
      outputDf.queryExecution.analyzed,
      com.example.ir.DatasetRef("demo/output/result.parquet")
    ).plan
    val lineage = Lineage.trace(plan)

    val result = ContractVerifier.verify(contract, outputDf.schema, lineage)

    assert(result.passed, s"expected PASS, got failing checks: ${result.checks.filterNot(_.passed)}")
    assert(result.checks.map(_.name) == List("id", "value", "value_squared"))
    assert(result.checks.forall(_.passed))
  }

  test("fails when the contract requires a field the actual output does not have") {
    val contract = realDemoContract()
    // Simulate a contract that (incorrectly) also requires a customer_id
    // column this pipeline never produces.
    val brokenContract = contract.copy(
      outputs = contract.outputs.map { ds =>
        ds.copy(schema = com.example.contract.Schema(
          ds.schema.fields :+ com.example.contract.Field("customer_id", "string", required = true, nullable = true)
        ))
      }
    )
    val outputDf = realDemoOutput()
    val lineage = Lineage.trace(
      SparkPlanAdapter.translateAsWrite(
        outputDf.queryExecution.analyzed,
        com.example.ir.DatasetRef("demo/output/result.parquet")
      ).plan
    )

    val result = ContractVerifier.verify(brokenContract, outputDf.schema, lineage)

    assert(!result.passed)
    val failure = result.checks.find(_.name == "customer_id").get
    assert(!failure.passed)
    assert(failure.message.contains("absent from the actual output"))
  }

  test("fails when the contract declares the wrong type for a field the actual output does have") {
    val contract = realDemoContract()
    val wrongTypeContract = contract.copy(
      outputs = contract.outputs.map { ds =>
        ds.copy(schema = com.example.contract.Schema(ds.schema.fields.map {
          case f if f.name == "value_squared" => f.copy(fieldType = "string")
          case f                              => f
        }))
      }
    )
    val outputDf = realDemoOutput()
    val lineage = Lineage.trace(
      SparkPlanAdapter.translateAsWrite(
        outputDf.queryExecution.analyzed,
        com.example.ir.DatasetRef("demo/output/result.parquet")
      ).plan
    )

    val result = ContractVerifier.verify(wrongTypeContract, outputDf.schema, lineage)

    assert(!result.passed)
    val failure = result.checks.find(_.name == "value_squared").get
    assert(failure.message.contains("declares type 'string'"))
    assert(failure.message.contains("actual output has type 'integer'"))
  }

  test("passing checks report the traced lineage source, not just a boolean") {
    val contract = realDemoContract()
    val outputDf = realDemoOutput()
    val lineage = Lineage.trace(
      SparkPlanAdapter.translateAsWrite(
        outputDf.queryExecution.analyzed,
        com.example.ir.DatasetRef("demo/output/result.parquet")
      ).plan
    )

    val result = ContractVerifier.verify(contract, outputDf.schema, lineage)
    val valueSquared = result.checks.find(_.name == "value_squared").get
    assert(valueSquared.message.contains("sample.csv.value"))
  }
}
