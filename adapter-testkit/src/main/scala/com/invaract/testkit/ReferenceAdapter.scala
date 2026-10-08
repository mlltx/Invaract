// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{CatalogIdentity, DeleteScope, RowMutation, FunctionCatalog, Cast, ColumnRef, ColumnReference, Comparison, DatasetRef, Expr, Filter, Join, JoinType, Literal, NamedExpr, Plan, Project, Read, Write}
import com.invaract.verification.{AdapterCapabilities, CheckedWrite, ContractViolationException, MutationClassification, MutationKind, VerificationOptions, VerificationPipeline}

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
      if (job.untranslatableWrite)
        VerificationPipeline.rejectUnverifiableWrite(
          contract,
          "ReferenceTruncate",
          com.invaract.ir.UnknownPlan(s"an operation on ${job.output.location} the reference adapter has no translation for"),
          Some(sink)
        )
      VerificationPipeline.verifyWrite(contract, checkedWrite(job), options, Some(sink), None, Some(capabilities))
      sink.passed
    } catch {
      case e: ContractViolationException => sink.rejected(e.result.violations.map(_.violationType).toSet)
    }
  }

  /** What the reference adapter hands the pipeline for `job` - the whole of an adapter's engine-specific work:
    * the translated plan, the schemas mapped to `LogicalSchema`, and the row-level classification. The conformance
    * kit's own tests override this to break one thing at a time. */
  protected def checkedWrite(job: ScenarioJob): CheckedWrite =
    CheckedWrite(
      plan = ReferenceAdapter.translate(job),
      inputSchemas = if (job.rowChange.isDefined) Nil else job.inputs.map(i => i.location -> i.schema),
      outputSchema = ReferenceAdapter.outputSchema(job),
      caseSensitive = false,
      rowMutation = job.rowChange.map(ReferenceAdapter.mutationOf),
      lineageBoundaryTypes = Set.empty
    )

  /** The reference engine has no checkpoint or cache, so a job run through a boundary translates to the same
    * plan as one without: there is nothing behind it to lose. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = Some(checkedWrite(job).plan)
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

  /** What a row-level change looks like to the rule verifiers: the engine-neutral extraction every adapter makes. */
  def mutationOf(change: RowChange): MutationClassification = {
    val scope: DeleteScope = change match {
      case RowChange.UnconditionalDelete => DeleteScope.Unconditional
      case RowChange.FilteredDelete      => DeleteScope.Conditional(Comparison(">", ColumnReference(ColumnRef("id", None)), Literal(0L, "long")))
    }
    MutationClassification.Extracted(MutationKind.Delete, RowMutation(delete = scope))
  }

  private def catalogOf(output: ScenarioOutput): Option[CatalogIdentity] =
    output.registeredAs.map(table => CatalogIdentity(table = Some(table)))

  def translate(job: ScenarioJob): Plan = job.rowChange match {
    // A row-level change in place is a write whose input is the table it changes - the shape Spark's
    // translation of a Delta DELETE has too.
    case Some(_) =>
      Write(DatasetRef(job.output.location), Read(DatasetRef(job.output.location)), Some(job.output.format), None, catalogOf(job.output))
    case None => translateTransform(job)
  }

  private def translateTransform(job: ScenarioJob): Plan = {
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
          case ColumnSource.UniqueId             => com.invaract.ir.Function(FunctionCatalog.Uuid.name, Nil)
        }
      )
    }
    Write(DatasetRef(job.output.location), Project(filtered, columns), Some(job.output.format), Some(job.output.saveMode), catalogOf(job.output))
  }

  def outputSchema(job: ScenarioJob): LogicalSchema = {
    def source(i: Int, column: String): LogicalField =
      job.inputs(i).schema.fields.find(_.name == column).getOrElse(throw new IllegalArgumentException(s"input $i has no column '$column'"))
    LogicalSchema(job.columns.map { c =>
      c.source match {
        case ColumnSource.FromInput(i, col)    => source(i, col).copy(name = c.name)
        case ColumnSource.CastInput(i, col, t) => LogicalField(c.name, t, source(i, col).nullable)
        case ColumnSource.NonNullLong(_)       => LogicalField(c.name, LogicalType.LongType, nullable = false)
        case ColumnSource.UniqueId             => LogicalField(c.name, LogicalType.StringType, nullable = false)
      }
    })
  }
}
