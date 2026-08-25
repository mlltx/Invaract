// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.contract.Contract
import com.example.ir.PlanPrinter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.StructType

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

  /** Every recognized *read* shape's location/schema extraction, in one
    * place - shared by both `plan.collect` sites in `verifyOrThrow` below
    * (the raw plan, and a recognized write's own `query`), which used to
    * each hand-repeat the same three (now four) cases; adding
    * `DataSourceV2Relation` here reaches both sites at once instead of
    * needing to remember to update two copies, the same
    * single-source-of-truth reasoning `WriteCommandSupport.combined`
    * already applies to write recognition.
    */
  private val recognizedRead: PartialFunction[LogicalPlan, (String, StructType)] = {
    case lr: LogicalRelation => SparkPlanAdapter.locationOf(lr) -> lr.schema
    case sr: StreamingRelation => SparkPlanAdapter.streamingRelationLocationOf(sr) -> sr.schema
    case sr2: StreamingRelationV2 => SparkPlanAdapter.streamingRelationV2LocationOf(sr2) -> sr2.schema
    // A batch DataSourceV2 catalog read - see SparkPlanAdapter's own
    // DataSourceV2Relation case for why this is needed at all (any "pure"
    // DSv2 connector's reads, Iceberg's included, previously fell through
    // to the generic Unsupported translation and so could never satisfy a
    // contract's declared input).
    case dsv2: DataSourceV2Relation => SparkPlanAdapter.tableLocationAndFormat(dsv2.table)._1.getOrElse(dsv2.name) -> dsv2.schema
  }

  /** The check logic itself, exposed directly for tests and for callers
    * that want to verify without going through `SparkSession` construction
    * (`forContract` is a thin adapter to the shape `injectCheckRule` wants).
    */
  private[sparkadapter] def verifyOrThrow(contract: Contract, plan: LogicalPlan, options: VerificationOptions): Unit = {
    val translated = SparkPlanAdapter.translate(plan)
    translated.plan match {
      case _: com.example.ir.Write =>
        // Collects every recognized *read* shape found anywhere in the
        // plan via `recognizedRead` above - LogicalRelation for batch V1
        // reads, StreamingRelation/StreamingRelationV2 for a legacy-V1 or
        // DataSourceV2 streaming source (see docs/SPARK_ADAPTER.md's
        // "Streaming reads as a contract input"), and DataSourceV2Relation
        // for a batch DSv2 catalog read (any "pure" DSv2 connector's
        // reads, Iceberg's included). Each was added after a contract
        // declaring that kind of source as a required `input` was found
        // to always report MISSING_INPUT, even though data was genuinely
        // being read, because this collection didn't yet recognize it -
        // the same location-extraction logic SparkPlanAdapter's own
        // translation uses for each shape is reused here rather than
        // re-derived, so the two sites can't drift the way write
        // recognition once did (see WriteCommandSupport's class doc).
        //
        // `plan.collect` walks `children`, which is empty for Delta's row-
        // level DML commands (MergeIntoCommand/UpdateCommand/DeleteCommand
        // are effectively leaf nodes in the tree-traversal sense - their
        // `source`/`target` are ordinary case-class fields, not exposed as
        // children) - confirmed empirically by a real FAIL test never
        // throwing, not assumed to "just work" the way it does for every
        // other write shape. So this also walks `query` - the same field
        // `WriteCommandSupport` already extracted (MERGE's `source` for
        // DML, the same plan `plan.collect` would already reach on its own
        // for every other shape) - which is a real, independently
        // traversable `LogicalPlan`, unlike the outer command.
        val inputSchemas = (
          plan.collect(recognizedRead) ++
            WriteCommandSupport.combined.lift(plan).toList.flatMap(_.query.collect(recognizedRead))
        ).distinct.toList
        // WriteCommandSupport.combined is the same lookup translation used
        // to reach this ir.Write in the first place, so this can never
        // drift out of sync with it the way three independent matches
        // could (and once did - see WriteCommandSupport's class doc). Its
        // outputSchema is always the underlying query's schema, not the
        // command node's own: a Command's `.schema` is its own (typically
        // empty) output, not the data it writes - using that directly
        // silently reported every declared field as missing regardless of
        // what was actually written, confirmed the hard way by a real
        // Delta write test failing PASS with a MISSING_OUTPUT_FIELD
        // violation on a field that genuinely was present (see
        // docs/SPARK_ADAPTER.md's "Delta Lake support" section). The
        // `plan.schema` fallback only matters if `translated.plan` is an
        // `ir.Write` `SparkPlanAdapter` produced some other way (not
        // currently possible - `WriteCommandSupport.combined` is the only
        // producer of `ir.Write` - but kept as a safe default rather than
        // assuming that stays true forever).
        val outputSchema = WriteCommandSupport.combined.lift(plan).map(_.outputSchema).getOrElse(plan.schema)
        val result = StructuralVerifier.verify(contract, translated.plan, inputSchemas, outputSchema, options)
        if (!result.passed) {
          throw new ContractViolationException(result, explain(contract, translated.plan, result))
        }
      case _ =>
        // Checked before the fail-closed Command catch-all below: a
        // recognized state-changing CALL (currently just
        // rollback_to_snapshot - see StateChangingCallSupport) genuinely
        // verifies the resulting state, rather than being rejected
        // outright the way it was before this case existed and still is
        // for the nine other state-changing procedures StateChangingCallSupport
        // doesn't recognize.
        StateChangingCallSupport.extract(plan) match {
          case Some(info) =>
            val result = StructuralVerifier.verifyStateChange(contract, info.location, info.resultingSchema, options)
            if (!result.passed) {
              // No ir.Plan translation exists for a state-changing CALL
              // (there's no Spark write/query to translate) - a plain
              // description standing in for `explain`'s usual rendered
              // plan tree, reusing the rest of its explanation format
              // unchanged.
              val describedPlan =
                com.example.ir.Unsupported(s"CALL rollback_to_snapshot(...) targeting '${info.location}'")
              throw new ContractViolationException(result, explain(contract, describedPlan, result))
            }
          case None if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
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
          case None =>
            () // not a Command at all (a Read/Project/Filter/...) - definitely not a write
        }
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
