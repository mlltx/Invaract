// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{Cast, ColumnRef, ColumnReference, Comparison, DatasetRef, Expr, Filter, Join, JoinType, Literal, NamedExpr, Plan, Project, Read, Write}
import com.invaract.verification.{AdapterCapabilities, CheckedWrite, ContractViolationException, VerificationOptions, VerificationPipeline}

/** The smallest possible adapter: it "runs" a `ScenarioJob` by translating it straight into the
  * engine-neutral plan and handing it to `VerificationPipeline`, with no engine at all.
  *
  * It is here for two reasons. It proves the scenarios are consistent with the SPI before any real
  * engine is involved (if the reference adapter disagrees with a scenario, the scenario is wrong,
  * not an engine). And it is a worked example of the whole adapter surface in about forty lines:
  * schemas in, plan in, `CheckedWrite` handed over, the rejection read back out.
  *
  * @param capabilities the declaration to enforce - defaults to the reference adapter's own; a test
  *   can pass another to show how the kit judges an adapter that claims less (or more) than it does.
  */
class ReferenceAdapter(override val capabilities: AdapterCapabilities = ReferenceAdapter.declared) extends ConformanceAdapter {

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    val sink = new RecordingSink
    try {
      VerificationPipeline.verifyWrite(
        contract,
        CheckedWrite(
          plan = ReferenceAdapter.translate(job),
          inputSchemas = job.inputs.map(i => i.location -> i.schema),
          outputSchema = ReferenceAdapter.outputSchema(job),
          caseSensitive = false,
          rowMutation = None,
          lineageBoundaryTypes = Set.empty
        ),
        options,
        Some(sink),
        None,
        Some(capabilities)
      )
      ScenarioOutcome.Passed(sink.statuses)
    } catch {
      case e: ContractViolationException => ScenarioOutcome.Rejected(e.result.violations.map(_.violationType).toSet, sink.statuses)
    }
  }
}

object ReferenceAdapter {

  /** The reference adapter's own declaration (`reference/invaract-capabilities-reference.yaml`). */
  lazy val declared: AdapterCapabilities =
    AdapterCapabilities
      .fromResource("reference/invaract-capabilities-reference.yaml", getClass.getClassLoader)
      .getOrElse(throw new IllegalStateException("reference/invaract-capabilities-reference.yaml is missing from the testkit jar"))
      .fold(errors => throw new IllegalStateException(errors.mkString("; ")), identity)

  private def ref(job: ScenarioJob, input: Int, column: String): Expr =
    ColumnReference(ColumnRef(column, Some(job.inputs(input).location)))

  def translate(job: ScenarioJob): Plan = {
    val reads = job.inputs.map(i => Read(DatasetRef(i.location)))
    val joined: Plan = job.join.fold[Plan](reads.head)(j =>
      Join(reads(0), reads(1), JoinType.Inner, Some(Comparison("=", ref(job, 0, j.leftColumn), ref(job, 1, j.rightColumn))))
    )
    val filtered: Plan = job.filterColumn.fold(joined)(c => Filter(joined, Comparison(">", ref(job, 0, c), Literal(0L, "long"))))
    val columns = job.columns.map { c =>
      NamedExpr(
        c.name,
        c.source match {
          case ColumnSource.FromInput(i, col)    => ref(job, i, col)
          case ColumnSource.CastInput(i, col, t) => Cast(ref(job, i, col), t.typeName)
          case ColumnSource.NonNullLong(v)       => Literal(v, "long")
        }
      )
    }
    Write(DatasetRef(job.output.location), Project(filtered, columns), Some(job.output.format), Some(job.output.saveMode))
  }

  def outputSchema(job: ScenarioJob): LogicalSchema = {
    def source(i: Int, column: String): LogicalField =
      job.inputs(i).schema.fields.find(_.name == column).getOrElse(throw new IllegalArgumentException(s"input $i has no column '$column'"))
    LogicalSchema(job.columns.map { c =>
      c.source match {
        case ColumnSource.FromInput(i, col)    => source(i, col).copy(name = c.name)
        case ColumnSource.CastInput(i, col, t) => LogicalField(c.name, t, source(i, col).nullable)
        case ColumnSource.NonNullLong(_)       => LogicalField(c.name, LogicalType.LongType, nullable = false)
      }
    })
  }
}
