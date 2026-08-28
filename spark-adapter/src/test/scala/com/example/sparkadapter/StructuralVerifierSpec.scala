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
    spark = SparkSession.builder().master("local[*]").appName("StructuralVerifierSpec")
      // See ContractEnforcementRuleSpec's beforeAll for why.
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
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

  // Previously a KNOWN GAP (see git history / ROADMAP.md Phase 1c): a
  // contract's declared `format` was parsed into the object model but
  // never checked against what the plan actually did — `ir.Write` didn't
  // even carry a format field to compare against. SparkPlanAdapter now
  // populates it (via Spark's DataSourceRegister.shortName() on the
  // write's FileFormat) and StructuralVerifier checks it; these tests
  // replace the old characterization test that documented the gap.

  test("OUTPUT_FORMAT_MISMATCH: contract declares one format but the plan writes in another") {
    val contract = realDemoContract()
    assert(contract.outputs.head.format.contains("parquet"), "sanity check: the real demo contract does declare a format")

    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)
    val plan = bareWrite.asInstanceOf[com.example.ir.Write].copy(format = Some("csv"))

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.OutputFormatMismatch)
      .getOrElse(fail(s"expected an OUTPUT_FORMAT_MISMATCH violation, got: ${result.violations}"))
    assert(violation.expected.contains("parquet"))
    assert(violation.actual.contains("csv"))
  }

  test("format matches (case-insensitively): no violation when contract and actual format agree") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)
    // Contract declares "parquet"; Spark's own shortName() for the format
    // this real demo actually writes is lowercase too, but the check is
    // explicitly case-insensitive — prove that, not just the exact-match case.
    val plan = bareWrite.asInstanceOf[com.example.ir.Write].copy(format = Some("PARQUET"))

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )

    assert(result.passed, s"expected matching formats to pass: ${result.violations}")
  }

  test("format is not checked when the contract doesn't declare one, or the actual format is unknown") {
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)

    // Contract declares no format at all.
    val contractNoFormat = realDemoContract().copy(
      outputs = realDemoContract().outputs.map(_.copy(format = None))
    )
    val planWithFormat = bareWrite.asInstanceOf[com.example.ir.Write].copy(format = Some("csv"))
    val resultA = StructuralVerifier.verify(
      contractNoFormat,
      planWithFormat,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )
    assert(resultA.passed, s"a contract with no declared format shouldn't check it at all: ${resultA.violations}")

    // Contract declares a format, but the adapter couldn't determine the
    // actual one (format = None, same as realDemoPlan's default synthetic wrap).
    val resultB = StructuralVerifier.verify(
      realDemoContract(),
      bareWrite,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )
    assert(resultB.passed, s"an unknown actual format shouldn't be treated as a mismatch: ${resultB.violations}")
  }

  // Previously a KNOWN GAP (see ROADMAP.md Phase 1c): SaveMode
  // (append/overwrite/ignore/error) wasn't captured by `ir.Write` at all —
  // a contract couldn't express or verify "this output must be
  // overwritten, not appended to." SparkPlanAdapter now populates it (via
  // InsertIntoHadoopFsRelationCommand.mode) and StructuralVerifier checks
  // it, mirroring the format check above exactly.

  test("OUTPUT_SAVE_MODE_MISMATCH: contract declares one save mode but the plan writes with another") {
    val contract = realDemoContract()
    assert(contract.outputs.head.saveMode.contains("overwrite"), "sanity check: the real demo contract does declare a saveMode")

    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)
    val plan = bareWrite.asInstanceOf[com.example.ir.Write].copy(saveMode = Some("append"))

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )

    assert(!result.passed)
    val violation = result.violations.find(_.violationType == ViolationType.OutputSaveModeMismatch)
      .getOrElse(fail(s"expected an OUTPUT_SAVE_MODE_MISMATCH violation, got: ${result.violations}"))
    assert(violation.expected.contains("overwrite"))
    assert(violation.actual.contains("append"))
  }

  test("save mode matches (case-insensitively): no violation when contract and actual save mode agree") {
    val contract = realDemoContract()
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)
    val plan = bareWrite.asInstanceOf[com.example.ir.Write].copy(saveMode = Some("OVERWRITE"))

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )

    assert(result.passed, s"expected matching save modes to pass: ${result.violations}")
  }

  test("save mode is not checked when the contract doesn't declare one, or the actual save mode is unknown") {
    val inputDf = realDemoInput()
    val outputDf = inputDf.withColumn("value_squared", col("value") * col("value"))
    val bareWrite = realDemoPlan(outputDf)

    // Contract declares no saveMode at all.
    val contractNoSaveMode = realDemoContract().copy(
      outputs = realDemoContract().outputs.map(_.copy(saveMode = None))
    )
    val planWithSaveMode = bareWrite.asInstanceOf[com.example.ir.Write].copy(saveMode = Some("append"))
    val resultA = StructuralVerifier.verify(
      contractNoSaveMode,
      planWithSaveMode,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )
    assert(resultA.passed, s"a contract with no declared saveMode shouldn't check it at all: ${resultA.violations}")

    // Contract declares a saveMode, but the adapter couldn't determine the
    // actual one (saveMode = None, same as realDemoPlan's default synthetic wrap).
    val resultB = StructuralVerifier.verify(
      realDemoContract(),
      bareWrite,
      inputSchemas = List("demo/input/sample.csv" -> inputDf.schema),
      outputSchema = outputDf.schema
    )
    assert(resultB.passed, s"an unknown actual saveMode shouldn't be treated as a mismatch: ${resultB.violations}")
  }

  // The tests below were added while raising the module's mutation-testing
  // score (see ROADMAP.md Phase 1c and CLAUDE.md's mutation-score
  // requirement): each targets a specific predicate that was previously
  // *covered* by other tests but not *pinned down* by their assertions.

  test("MISSING_INPUT and UNDECLARED_INPUT check each location independently, not requiring universal agreement") {
    // Existing tests only ever declare/read a single input, which can't
    // distinguish `exists` (correct: at least one match) from `forall`
    // (wrong: every actual location must match every declared one, or
    // vice versa) — both give the same answer when there's only one item
    // to compare. This uses two declared inputs and two actual reads,
    // overlapping on exactly one location, which the two quantifiers
    // disagree about.
    val contract = ContractParser.parse(
      """id: multi_input_demo
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |  - name: customers
        |    location: raw.customers
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |""".stripMargin
    )
    // Reads raw.orders (declared) and raw.extra (undeclared); never reads
    // raw.customers (declared).
    val joined = com.example.ir.Join(Read(DatasetRef("raw.orders")), Read(DatasetRef("raw.extra")), com.example.ir.JoinType.Inner)
    val plan = com.example.ir.Write(DatasetRef("gold.out"), joined)

    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = Nil,
      outputSchema = new StructType().add("id", IntegerType),
      options = VerificationOptions(rejectUndeclaredInputs = true)
    )

    assert(
      !result.violations.exists(v => v.violationType == ViolationType.MissingInput && v.location.contains("raw.orders")),
      s"raw.orders is declared AND read, so it must not be flagged missing: ${result.violations}"
    )
    assert(
      result.violations.exists(v => v.violationType == ViolationType.MissingInput && v.location.contains("raw.customers")),
      s"raw.customers is declared but never read, so it must be flagged missing: ${result.violations}"
    )
    assert(
      result.violations.exists(v => v.violationType == ViolationType.UndeclaredInput && v.location.contains("raw.extra")),
      s"raw.extra is read but undeclared, so it must be flagged: ${result.violations}"
    )
    assert(
      !result.violations.exists(v => v.violationType == ViolationType.UndeclaredInput && v.location.contains("raw.orders")),
      s"raw.orders is read AND declared, so it must not be flagged undeclared: ${result.violations}"
    )
  }

  test("an absent field is only flagged when the contract marks it required") {
    val contract = ContractParser.parse(
      """id: optional_field_demo
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |        - name: notes
        |          type: string
        |          required: false
        |""".stripMargin
    )
    val plan = com.example.ir.Write(DatasetRef("gold.out"), Read(DatasetRef("raw.in")))
    // "notes" is absent from the actual schema, but it's optional. "id" is
    // explicitly non-nullable to match the contract's required = true (which
    // implies nullable = false) - otherwise this would also trip an
    // unrelated OUTPUT_FIELD_NULLABILITY_MISMATCH.
    val actualSchema = new StructType().add("id", IntegerType, nullable = false)

    val result = StructuralVerifier.verify(contract, plan, inputSchemas = Nil, outputSchema = actualSchema)

    assert(result.passed, s"an absent optional field should not be a violation: ${result.violations}")
  }

  test("MISSING_INPUT_FIELD and MISSING_OUTPUT_FIELD remediations name the correct side") {
    val contract = ContractParser.parse(
      """id: side_naming_demo
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: raw.orders
        |    schema:
        |      fields:
        |        - name: id
        |          type: integer
        |          required: true
        |outputs:
        |  - name: out
        |    location: gold.out
        |    schema:
        |      fields:
        |        - name: total
        |          type: integer
        |          required: true
        |""".stripMargin
    )
    val plan = com.example.ir.Write(DatasetRef("gold.out"), Read(DatasetRef("raw.orders")))
    val result = StructuralVerifier.verify(
      contract,
      plan,
      inputSchemas = List("raw.orders" -> new StructType()), // "id" absent from the actual input
      outputSchema = new StructType() // "total" absent from the actual output
    )

    // `datasetNoun` ("input"/"output", lowercase) only appears in the
    // remediation text, not `message` (which uses `contextPrefix`,
    // "INPUT"/"OUTPUT" uppercase, directly).
    val inputViolation = result.violations.find(_.violationType == ViolationType.MissingInputField).get
    assert(inputViolation.remediation.contains("to the input,"), inputViolation.remediation)
    val outputViolation = result.violations.find(_.violationType == ViolationType.MissingOutputField).get
    assert(outputViolation.remediation.contains("to the output,"), outputViolation.remediation)
  }

  // A contract with no declared outputs is now rejected before it ever
  // reaches this method at all - see ContractEnforcementRule.verifyOrThrow's
  // ContractValidator.validate guard, and ContractEnforcementRuleSpec's
  // "FAIL: a contract missing 'outputs' is rejected cleanly..." test for
  // the real, end-to-end regression test. StructuralVerifier.verify itself
  // still assumes a structurally valid contract on input, by design -
  // validating one is ContractEnforcementRule's responsibility, not this
  // method's, the same single-responsibility split every other caller of
  // this method (StateChangingCallSupport's tests included) already relies on.
}
