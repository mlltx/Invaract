// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.Contract
import com.example.ir.PlanPrinter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}

/** Thrown by `ContractEnforcementRule` to abort a Spark write that violates
  * its contract, before Spark executes it. `result` carries the full
  * `VerificationResult`; `getMessage` is a complete, human-readable
  * explanation (see `ContractEnforcementRule.explain`) — a developer
  * reading only the exception text, with no other context, should be able
  * to answer all four of: what the contract expected, what the plan
  * contains, why it violates the contract, and how to correct it.
  */
class ContractViolationException(val result: VerificationResult, message: String) extends RuntimeException(message)

/** Gates a Spark write on contract verification, per ROADMAP.md Phase 5:
  *
  * {{{
  * Spark application → Logical plan → Invariant → PASS → execute
  *                                             └─→ FAIL → abort
  * }}}
  *
  * ## Why a check rule, not the `SparkAdapterListener` used elsewhere
  *
  * `SparkAdapterListener` (see `SparkPlanAdapter`'s class doc) observes a
  * query via `QueryExecutionListener.onSuccess` — which, as the name says,
  * fires only *after* Spark has already executed the query successfully.
  * By the time that callback runs, a write's output file already exists on
  * disk. That is exactly backwards for this requirement: verification has
  * to run, and be able to reject, *before* Spark performs a destructive
  * output operation.
  *
  * Spark's `SparkSessionExtensions.injectCheckRule` is built for this: a
  * function invoked on every analyzed `LogicalPlan` the session produces,
  * whose only role is to validate and optionally throw to reject the
  * query — a purpose-built pre-execution gate, not a repurposed observer.
  * Confirmed empirically (not assumed) that throwing inside a check rule
  * aborts a `DataFrame.write` call before any data is written: a probe
  * against a fresh `SparkSession` with a check rule that unconditionally
  * throws on the write command showed the exception propagating out of
  * `.write.parquet(...)` unwrapped, and the target file never created.
  *
  * This is real leverage over `SparkAdapterListener`, not a strictly better
  * replacement for it: a check rule can only approve or reject, mid-call,
  * inside whatever action triggered it — it has no equivalent of the
  * listener's "give me the finished result to report on afterward." The
  * two serve different moments in the same pipeline: this one decides
  * whether the write happens at all; the listener (still used for
  * `demo/output/report.json`'s human-facing summary) reports on it once it
  * has.
  *
  * ## What triggers verification
  *
  * The check rule fires on *every* analyzed plan the session produces —
  * schema-inference reads, `.count()`, intermediate transformations, not
  * just the final write. Only a plan that `SparkPlanAdapter.translate`s to
  * an `ir.Write` is checked; everything else is a silent no-op, so this
  * imposes no overhead or risk of false rejection on non-write queries.
  */
object ContractEnforcementRule {

  /** Builds a Spark check rule (pass to
    * `SparkSession.Builder.withExtensions(_.injectCheckRule(...))`) that
    * verifies any write this session performs against `contract`, throwing
    * `ContractViolationException` to abort it if verification fails.
    */
  def forContract(contract: Contract, options: VerificationOptions = VerificationOptions()): SparkSession => LogicalPlan => Unit =
    _ => (plan: LogicalPlan) => verifyOrThrow(contract, plan, options)

  /** The check logic itself, exposed directly for tests and for callers
    * that want to verify without going through `SparkSession` construction
    * (`forContract` is a thin adapter to the shape `injectCheckRule` wants).
    */
  private[sparkadapter] def verifyOrThrow(contract: Contract, plan: LogicalPlan, options: VerificationOptions): Unit = {
    val translated = SparkPlanAdapter.translate(plan)
    translated.plan match {
      case _: com.example.ir.Write =>
        val inputSchemas = plan.collect { case lr: LogicalRelation => SparkPlanAdapter.locationOf(lr) -> lr.schema }.toList
        // Both branches use the underlying query's schema, not the command
        // node's own: a Command's `.schema` is its own (typically empty)
        // output, not the data it writes - using it directly for
        // SaveIntoDataSourceCommand silently reported every declared field
        // as missing regardless of what was actually written, confirmed
        // the hard way by a real Delta write test failing PASS with a
        // MISSING_OUTPUT_FIELD violation on a field that genuinely was
        // present (see docs/SPARK_ADAPTER.md's "Delta Lake support"
        // section).
        val outputSchema = plan match {
          case cmd: InsertIntoHadoopFsRelationCommand    => cmd.query.schema
          case cmd: SaveIntoDataSourceCommand             => cmd.query.schema
          case cmd: CreateDataSourceTableAsSelectCommand => cmd.query.schema
          case other                                      => other.schema
        }
        val result = StructuralVerifier.verify(contract, translated.plan, inputSchemas, outputSchema, options)
        if (!result.passed) {
          throw new ContractViolationException(result, explain(contract, translated.plan, result))
        }
      case _ if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
        val violation = Violation(
          ViolationType.UnverifiableWrite,
          s"'${plan.getClass.getSimpleName}' looks like it may write or otherwise mutate data, but Invariant has no " +
            s"translation for it, so it was never checked against contract '${contract.id}@${contract.version}'.",
          remediation =
            "If this command genuinely doesn't write data, add its class to FailClosedCommands' known-safe list " +
              "(with the same reasoning documented there) and open an issue/PR. If it does write data, that's a " +
              "real translation gap in SparkPlanAdapter - see docs/SPARK_ADAPTER.md's " +
              "\"Fail-closed on unverifiable writes\" section."
        )
        val result = VerificationResult.of(s"${contract.id}@${contract.version}", List(violation))
        throw new ContractViolationException(result, explain(contract, translated.plan, result))
      case _ =>
        () // not a Command at all (a Read/Project/Filter/...) - definitely not a write
    }
  }

  /** Builds the full explanation `ContractViolationException.getMessage`
    * carries. Deterministic: built entirely from `result.violations` (an
    * already-deterministically-ordered list — see `StructuralVerifier`'s
    * "Determinism" doc) and the plan's own rendering, so the same
    * violation always produces the same message, byte for byte.
    */
  private[sparkadapter] def explain(contract: Contract, plan: com.example.ir.Plan, result: VerificationResult): String = {
    val sb = new StringBuilder

    sb.append(s"Contract violation: '${result.contract}' rejected this transformation. Write aborted.\n")

    sb.append("\nWhat the contract expects:\n")
    contract.inputs.foreach { input =>
      sb.append(s"  input  '${input.name}' at ${input.location}: ${describeFields(input.schema.fields)}\n")
    }
    contract.outputs.foreach { output =>
      sb.append(s"  output '${output.name}' at ${output.location}: ${describeFields(output.schema.fields)}\n")
    }

    sb.append("\nWhat the plan contains:\n")
    PlanPrinter.render(plan).linesIterator.foreach(line => sb.append("  ").append(line).append("\n"))

    sb.append(s"\nWhy it violates the contract (${result.violations.size} " + (if (result.violations.size == 1) "violation" else "violations") + "):\n")
    result.violations.zipWithIndex.foreach { case (v, i) =>
      sb.append(s"  ${i + 1}. [${v.violationType}] ${v.message}\n")
    }

    sb.append("\nHow to correct it:\n")
    result.violations.zipWithIndex.foreach { case (v, i) =>
      sb.append(s"  ${i + 1}. ${v.remediation}\n")
    }

    sb.toString()
  }

  private def describeFields(fields: List[com.example.contract.Field]): String =
    fields
      .map(f => s"${f.name}: ${f.fieldType}" + (if (f.required) "" else " (optional)"))
      .mkString(", ")
}
