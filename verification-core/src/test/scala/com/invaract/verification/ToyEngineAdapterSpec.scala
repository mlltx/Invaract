// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{ColumnRef, ColumnReference, DatasetRef, Filter, Comparison, Literal, NamedExpr, Plan, Project, Read, Write}
import com.invaract.verification.notification.{ContractValidationEvent, TestNotificationSink}

import org.scalatest.funsuite.AnyFunSuite

/** A complete, deliberately tiny engine adapter - "ToyEngine" - written against nothing but the
  * public SPI, to show (and keep proving) what an adapter for a non-Spark engine consists of:
  *
  *   1. recognize an operation in the engine's own representation (`ToyJob`);
  *   2. translate it into `ir.Plan`;
  *   3. map the engine's own column types into `LogicalType` (a `TypeMapper`);
  *   4. spell the neutral `InvaractConf` names its own way (a `ConfigSource`);
  *   5. hand all of that to `VerificationSetup` once and `VerificationPipeline` per operation.
  *
  * Everything else - every checker, the DML rules, fingerprinting, the event, the rejection text -
  * is the core's. This is also the seed of the conformance kit: the same contract, checked through a
  * second engine, gives the same verdicts.
  */
class ToyEngineAdapterSpec extends AnyFunSuite {

  // --- the toy engine's own world ---------------------------------------------------------------

  /** The engine's native column types (a BigQuery-flavoured naming on purpose, to show the mapping). */
  private sealed trait ToyType
  private case object Int64 extends ToyType
  private case object Str extends ToyType
  private case object Numeric extends ToyType
  private case object Geography extends ToyType // no logical equivalent

  private case class ToyColumn(name: String, tpe: ToyType, notNull: Boolean = false)

  /** One toy "job": read a table, optionally filter on a column, write a table. */
  private case class ToyJob(source: String, sourceCols: List[ToyColumn], filterOn: Option[String], target: String, outCols: List[ToyColumn])

  // --- the adapter -------------------------------------------------------------------------------

  private object ToyAdapter {

    /** 3. the engine's types -> logical types. A type with no logical equivalent must not be guessed. */
    def toLogical(t: ToyType): LogicalType = t match {
      case Int64     => LogicalType.LongType
      case Str       => LogicalType.StringType
      case Numeric   => LogicalType.DecimalType(38, 9)
      case Geography => LogicalType.OtherType("geography")
    }

    def schemaOf(cols: List[ToyColumn]): LogicalSchema =
      LogicalSchema(cols.map(c => LogicalField(c.name, toLogical(c.tpe), nullable = !c.notNull)))

    /** 2. the engine's job -> ir.Plan. */
    def translate(job: ToyJob): Plan = {
      def ref(c: String) = ColumnReference(ColumnRef(c, Some(job.source)))
      val read: Plan = Read(DatasetRef(job.source))
      val filtered = job.filterOn.fold(read)(c => Filter(read, Comparison(">", ref(c), Literal(0, "long"))))
      Write(DatasetRef(job.target), Project(filtered, job.outCols.map(c => NamedExpr(c.name, ref(c.name)))), format = Some("table"), saveMode = Some("append"))
    }

    /** 4. the engine's configuration surface, spelled its own way (here: `TOY_INVARACT_<NAME>` env-style keys). */
    def configFrom(env: Map[String, String]): ConfigSource =
      ConfigSource.prefixed("TOY_INVARACT_")(key => env.get(key.toUpperCase))

    /** 5. enforce one job. */
    def enforce(contract: Contract, job: ToyJob, config: ConfigSource, sink: Option[TestNotificationSink] = None): Unit = {
      val governedOptions = VerificationSetup.resolveVerificationOptions(VerificationOptions(), config)
      VerificationPipeline.verifyWrite(
        contract,
        CheckedWrite(
          plan = translate(job),
          inputSchemas = List(job.source -> schemaOf(job.sourceCols)),
          outputSchema = schemaOf(job.outCols),
          caseSensitive = true, // the toy engine's identifiers are case-sensitive
          rowMutation = None,
          lineageBoundaryTypes = Set.empty // the toy engine has no checkpoints
        ),
        governedOptions,
        sink
      )
    }
  }

  private val contract: Contract = ContractParser.parse(
    """id: toy
      |version: "1.0.0"
      |inputs:
      |  - name: events
      |    location: warehouse.events
      |    schema:
      |      fields:
      |        - name: user_id
      |          type: long
      |          required: true
      |        - name: amount
      |          type: decimal(38,9)
      |outputs:
      |  - name: totals
      |    location: warehouse.totals
      |    format: table
      |    saveMode: append
      |    schema:
      |      fields:
      |        - name: user_id
      |          type: long
      |          required: true
      |""".stripMargin
  )

  private val job = ToyJob(
    source = "warehouse.events",
    sourceCols = List(ToyColumn("user_id", Int64, notNull = true), ToyColumn("amount", Numeric)),
    filterOn = Some("amount"),
    target = "warehouse.totals",
    outCols = List(ToyColumn("user_id", Int64, notNull = true))
  )

  // --- the same contract, a second engine, the same verdicts -------------------------------------

  test("a conforming toy job passes, and the PASSED event is the one every engine publishes") {
    val sink = new TestNotificationSink
    ToyAdapter.enforce(contract, job, ConfigSource.empty, Some(sink))
    val events = sink.events.collect { case e: ContractValidationEvent => e }
    assert(events.map(_.status) == List("PASSED"))
    assert(events.head.contract == "toy@1.0.0")
  }

  test("a wrong column type is the same OUTPUT_FIELD_TYPE_MISMATCH a Spark job would get, stated in the contract's vocabulary") {
    val bad = job.copy(outCols = List(ToyColumn("user_id", Str, notNull = true)))
    val ex = intercept[ContractViolationException] { ToyAdapter.enforce(contract, bad, ConfigSource.empty) }
    val v = ex.result.violations.head
    assert(v.violationType == ViolationType.OutputFieldTypeMismatch)
    assert(v.expected.contains("long") && v.actual.contains("string"))
  }

  test("a missing declared input is MISSING_INPUT (the toy engine has no lineage boundaries to excuse it)") {
    val ex = intercept[ContractViolationException] {
      ToyAdapter.enforce(contract, job.copy(source = "warehouse.other"), ConfigSource.empty)
    }
    assert(ex.result.violations.map(_.violationType).contains(ViolationType.MissingInput))
  }

  test("a native type with no logical equivalent never satisfies a declared standard type") {
    val geo = job.copy(outCols = List(ToyColumn("user_id", Geography, notNull = true)))
    val ex = intercept[ContractViolationException] { ToyAdapter.enforce(contract, geo, ConfigSource.empty) }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.OutputFieldTypeMismatch))
    assert(ex.result.violations.head.actual.contains("geography"))
  }

  test("the engine's own case rule is honoured: caseSensitive = true makes 'USER_ID' a different column") {
    val upper = job.copy(outCols = List(ToyColumn("USER_ID", Int64, notNull = true)))
    val ex = intercept[ContractViolationException] { ToyAdapter.enforce(contract, upper, ConfigSource.empty) }
    assert(ex.result.violations.map(_.violationType).contains(ViolationType.MissingOutputField))
  }

  // --- external attachability: the platform turns a capability on without touching the job ----------

  test("a platform turns on rejectUndeclaredFields through the engine's own config surface, with no change to the job") {
    val extra = job.copy(outCols = List(ToyColumn("user_id", Int64, notNull = true), ToyColumn("debug", Str)))
    ToyAdapter.enforce(contract, extra, ConfigSource.empty) // permitted by default
    val ex = intercept[ContractViolationException] {
      ToyAdapter.enforce(contract, extra, ToyAdapter.configFrom(Map("TOY_INVARACT_REJECTUNDECLAREDFIELDS" -> "true")))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UndeclaredOutputColumn))
  }

  test("the toy engine's fail-closed path uses the same rejection every engine does") {
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.rejectUnverifiableWrite(contract, "ToyBulkLoad", ToyAdapter.translate(job))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
  }
}
