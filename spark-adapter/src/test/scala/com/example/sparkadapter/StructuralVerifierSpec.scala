// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.ContractParser
import com.example.ir.{DatasetRef, Lineage, Read}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class StructuralVerifierSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("StructuralVerifierSpec").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def realDemoContract() =
    ContractParser.parseFile(new File("../demo/contracts/invariant_output.yaml"))

  private def realDemoInput() =
    spark.read.option("header", "true").option("inferSchema", "true").csv("../demo/input/sample.csv")

  private def realDemoPlan(outputDf: org.apache.spark.sql.DataFrame, outputLocation: String = "demo/output/result.parquet") =
    SparkPlanAdapter.translateAsWrite(outputDf.queryExecution.analyzed, DatasetRef(outputLocation)).plan

  test("PASSES against the real demo pipeline: real inputs, real output, real lineage, real contract") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val plan = realDemoPlan(outputDf)

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )

    assert(result.passed, s"expected PASSED, got violations: ${result.violations}")
    assert(result.status == "PASSED")
    assert(result.contract == "invariant_demo_output@1.0.0")
    assert(result.violations.isEmpty)
  }

  test("MISSING_INPUT: contract declares an input the plan never reads") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    // Build the plan from a Read at a location that does not match the
    // contract's declared input at all.
    val plan = com.example.ir.Write(
      DatasetRef("demo/output/result.parquet"),
      SparkPlanAdapter.translate(outputDf.queryExecution.analyzed).plan match {
        case p @ com.example.ir.Project(_: Read, _) =>
          p.copy(input = Read(DatasetRef("demo/input/other_source.csv")))
        case other => other
      }
    )

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = outputDf.schema)

    assert(!result.passed)
    assert(result.violations.exists(v => v.violationType == ViolationType.MissingInput && v.location.contains("demo/input/sample.csv")))
  }

  test("UNDECLARED_INPUT is reported only when rejectUndeclaredInputs is enabled") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val extraRead = Read(DatasetRef("demo/input/extra_lookup.csv"))
    val joined = com.example.ir.Join(
      Read(DatasetRef("demo/input/sample.csv")),
      extraRead,
      com.example.ir.JoinType.Inner
    )
    val plan = com.example.ir.Write(DatasetRef("demo/output/result.parquet"), joined)

    val permissive = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = inputDf.schema)
    assert(!permissive.violations.exists(_.violationType == ViolationType.UndeclaredInput))

    val strict = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = Nil,
      outputSchema = inputDf.schema,
      options = VerificationOptions(rejectUndeclaredInputs = true)
    )
    assert(strict.violations.exists(v => v.violationType == ViolationType.UndeclaredInput && v.location.contains("demo/input/extra_lookup.csv")))
  }

  test("MISSING_OUTPUT: the plan does not produce a write at all") {
    val contract = realDemoContract()
    val bareProject = com.example.ir.Project(
      Read(DatasetRef("demo/input/sample.csv")),
      List(com.example.ir.NamedExpr("id", com.example.ir.ColumnReference(com.example.ir.ColumnRef("id"))))
    )

    val result = StructuralVerifier.verify(
      contract,
      bareProject,
      inputSchemas = Nil,
      outputSchema = new StructType().add("id", IntegerType)
    )

    assert(!result.passed)
    assert(result.violations.exists(_.violationType == ViolationType.MissingOutput))
  }

  test("OUTPUT_LOCATION_MISMATCH: the plan writes somewhere other than the contract's declared location") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val plan = realDemoPlan(outputDf, outputLocation = "demo/output/somewhere_else.parquet")

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = outputDf.schema)

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.OutputLocationMismatch).get
    assert(violation.expected.contains("demo/output/result.parquet"))
    assert(violation.actual.contains("demo/output/somewhere_else.parquet"))
  }

  test("MISSING_OUTPUT_FIELD: a required output field the contract declares is absent from the actual output") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf // no value_squared column added
    val plan = realDemoPlan(outputDf)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = outputDf.schema)

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.MissingOutputField).get
    assert(violation.column.contains("value_squared"))
  }

  test("golden example: UNDECLARED_OUTPUT_COLUMN, matching the spec's exact violation shape") {
    // Reproduces the worked example from the Phase 4 spec: a customer_orders
    // contract, an actual output with an extra undeclared 'country' column.
    val contractYaml =
      """id: customer_orders
        |version: "1.2.0"
        |outputs:
        |  - name: customer_orders
        |    location: gold.customer_orders
        |    schema:
        |      fields:
        |        - name: customer_id
        |          type: string
        |          required: true
        |          nullable: false
        |""".stripMargin
    val contract = ContractParser.parse(contractYaml)

    val plan = com.example.ir.Write(
      DatasetRef("gold.customer_orders"),
      com.example.ir.Project(
        Read(DatasetRef("raw.orders")),
        List(
          com.example.ir.NamedExpr("customer_id", com.example.ir.ColumnReference(com.example.ir.ColumnRef("customer_id"))),
          com.example.ir.NamedExpr("country", com.example.ir.ColumnReference(com.example.ir.ColumnRef("country")))
        )
      )
    )
    val actualOutputSchema = new StructType()
      .add("customer_id", StringType, nullable = false)
      .add("country", StringType, nullable = true)

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = Nil,
      outputSchema = actualOutputSchema,
      options = VerificationOptions(rejectUndeclaredFields = true)
    )

    assert(result.status == "FAILED")
    assert(result.contract == "customer_orders@1.2.0")
    assert(
      result.violations == List(
        Violation(
          ViolationType.UndeclaredOutputColumn,
          result.violations.head.message,
          result.violations.head.remediation,
          column = Some("country")
        )
      )
    )
    assert(result.violations.head.toMap("type") == "UNDECLARED_OUTPUT_COLUMN")
    assert(result.violations.head.toMap("column") == "country")
    assert(result.violations.head.remediation.nonEmpty)
  }

  test("UNDECLARED_OUTPUT_COLUMN is not reported when rejectUndeclaredFields is left at the default") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf
      .withColumn("value_squared", col("value") * col("value"))
      .withColumn("extra_column", lit("unexpected"))
    val plan = realDemoPlan(outputDf)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = outputDf.schema)

    assert(result.passed)
  }

  test("OUTPUT_FIELD_TYPE_MISMATCH: actual output has the wrong type for a declared field") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", (col("value") * col("value")).cast("string"))
    val plan = realDemoPlan(outputDf)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = outputDf.schema)

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.OutputFieldTypeMismatch).get
    assert(violation.column.contains("value_squared"))
    assert(violation.expected.contains("integer"))
    assert(violation.actual.contains("string"))
  }

  test("OUTPUT_FIELD_NULLABILITY_MISMATCH: contract requires non-null but the actual schema permits nulls") {
    val contract = ContractParser.parse(
      """id: strict_nulls
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |          nullable: false
        |""".stripMargin
    )
    val plan = com.example.ir.Write(
      DatasetRef("gold.out"),
      Read(DatasetRef("raw.in"))
    )
    val actualSchema = new StructType().add("id", IntegerType, nullable = true)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = actualSchema)

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.OutputFieldNullabilityMismatch).get
    assert(violation.column.contains("id"))
  }

  test("a stricter-than-required actual nullability (non-null where contract allows null) is not a violation") {
    val contract = ContractParser.parse(
      """id: lenient_nulls
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |          nullable: true
        |""".stripMargin
    )
    val plan = com.example.ir.Write(DatasetRef("gold.out"), Read(DatasetRef("raw.in")))
    val actualSchema = new StructType().add("id", IntegerType, nullable = false)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = actualSchema)

    assert(result.passed)
  }

  test("input schema checks (MISSING_INPUT_FIELD, INPUT_FIELD_TYPE_MISMATCH) mirror the output checks") {
    val contract = ContractParser.parse(
      """id: input_schema_check
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |        - name: customer_id
        |          type: string
        |          required: true
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |""".stripMargin
    )
    val plan = com.example.ir.Write(DatasetRef("gold.out"), Read(DatasetRef("raw.orders")))
    // actual input schema: id is a string (type mismatch), customer_id absent (missing field)
    val actualInputSchema = new StructType().add("id", StringType)
    val outputSchema = new StructType().add("id", IntegerType)

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("raw.orders" -> actualInputSchema),
      outputSchema = outputSchema
    )

    assert(!result.passed)
    assert(result.violations.exists(v => v.violationType == ViolationType.MissingInputField && v.column.contains("customer_id")))
    assert(result.violations.exists(v => v.violationType == ViolationType.InputFieldTypeMismatch && v.column.contains("id")))
  }

  test("location matching bridges a contract's relative path and Spark's absolute file: URI") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    // realDemoPlan translates via SparkPlanAdapter against the real, absolute
    // file: URI Spark reports for a locally-read CSV — the contract declares
    // the relative "demo/input/sample.csv" / "demo/output/result.parquet".
    val plan = realDemoPlan(outputDf)

    val reads = {
      def collect(p: com.example.ir.Plan): List[Read] = p match {
        case r: Read => List(r)
        case other    => other.children.flatMap(collect)
      }
      collect(plan)
    }
    assert(reads.head.dataset.location.startsWith("file:"), "sanity check: Spark does report an absolute file: URI")

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )
    assert(result.passed, s"expected the relative contract location to match the absolute runtime path: ${result.violations}")
  }
}
