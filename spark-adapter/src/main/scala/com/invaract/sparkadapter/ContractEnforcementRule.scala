// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractValidator}
import com.invaract.ir.PlanPrinter
import com.invaract.sparkadapter.notification.{ContractValidationEvent, NotificationSink}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
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
  * Spark application → Logical plan → Invaract → PASS → execute
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
    session => {
      VersionCompatibilityGuard.check(session)
      (plan: LogicalPlan) => verifyOrThrow(contract, plan, options, None)
    }

  /** Same as `forContract(contract, options)`, but additionally publishes a
    * `ContractValidationEvent` to `sink` for every check this session's
    * enforcement performs — PASS or FAIL, not just the failures a caller
    * would otherwise only learn about via a thrown `ContractViolationException`.
    * A new overload rather than a third default parameter on the existing
    * method (see CLAUDE.md's "API Compatibility Requirement") — adding a
    * parameter to an already-published method signature is a binary break
    * for any existing compiled caller.
    *
    * This is a different moment than `SparkAdapterListener`'s `WriteEvent`:
    * this fires at analysis time, before Spark has executed anything (so a
    * FAILED event here means the write never happened), while `WriteEvent`
    * only fires once Spark reports a write actually completed. See
    * `com.invaract.sparkadapter.notification`'s types for the full
    * mechanism, and docs-site's "Notification sinks" guide for a worked
    * example.
    */
  def forContract(contract: Contract, options: VerificationOptions, sink: NotificationSink): SparkSession => LogicalPlan => Unit =
    session => {
      VersionCompatibilityGuard.check(session)
      (plan: LogicalPlan) => verifyOrThrow(contract, plan, options, Some(sink), Some(session.sparkContext.applicationId))
    }

  /** Builds a Spark check rule for "dry-run mode" (ROADMAP.md): installed
    * the same way as `forContract` — via
    * `SparkSession.Builder.withExtensions(_.injectCheckRule(...))` — but
    * with no contract to enforce at all. Rather than verifying a write, it
    * infers what a contract covering it would look like (see
    * `ContractInference`) and hands that to `onInferred`, so a user running
    * a real transformation for the first time, before any contract exists
    * for it, gets a concrete starting point to copy, edit, and use with
    * `forContract` from then on — see docs-site's "Dry-run mode" guide.
    *
    * Never throws, never blocks a write: there is nothing to enforce
    * without a contract, so unlike `forContract` this check rule only
    * observes. Only a plan recognized as an ordinary write (one
    * `WriteCommandSupport.combined` matches) triggers `onInferred` — the
    * same scope `verifyOrThrow`'s `ir.Write` branch covers, deliberately
    * excluding state-changing CALLs and row-level DML (MERGE/UPDATE/DELETE
    * have no "new output" to infer a dataset schema from — see
    * `WriteCommandInfo`'s row-level-DML cases in `WriteCommandSupport` for
    * why). `injectCheckRule` fires on every analyzed plan the session
    * produces, so `onInferred` may fire more than once for what a user
    * thinks of as a single write (e.g. an atomic CTAS's nested `AppendData`
    * against a `StagedTable` — see `WriteCommandSupport.namedRelationLocationAndFormat`'s
    * doc); a caller that only wants "the last one" should simply overwrite
    * its own captured value on each call, the same pattern
    * `SparkAdapterListener.lastWrite` already uses for the analogous
    * post-execution case.
    */
  def dryRun(onInferred: Contract => Unit): SparkSession => LogicalPlan => Unit =
    session => {
      VersionCompatibilityGuard.check(session)
      (plan: LogicalPlan) => inferOrIgnore(plan, onInferred)
    }

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
    // A real Hive-format catalog table read - see SparkPlanAdapter's own
    // HiveTableRelation case for why this is needed at all (a genuinely
    // Hive-native table, e.g. non-Parquet/ORC or with metastore conversion
    // disabled, previously fell through to the generic Unsupported
    // translation, so a contract declaring one as a required `input`
    // always reported MISSING_INPUT even though data was genuinely read).
    case htr: HiveTableRelation => SparkPlanAdapter.hiveTableRelationLocationOf(htr) -> htr.schema
  }

  /** Every recognized read anywhere in `plan` — the raw plan itself, plus
    * (for a recognized write) its own `query` — via `recognizedRead` above.
    * Shared by `verifyOrThrow`'s real enforcement and `inferOrIgnore`'s
    * dry-run inference, so the two can never disagree about what counts as
    * a contract input; see `verifyOrThrow`'s own call site for why both the
    * raw plan and `query` need walking (Delta's row-level DML commands are
    * leaf nodes in the tree-traversal sense).
    *
    * Takes the write's `query` directly rather than re-deriving it via a
    * second `WriteCommandSupport.combined.lift(plan)` — both call sites
    * already compute that lookup once for their own purposes (`verifyOrThrow`
    * for `outputSchema`, `inferOrIgnore` for the `WriteCommandInfo` it
    * infers from), so re-deriving it a second time here would just repeat
    * that match on every analyzed plan the session produces for no reason.
    */
  private def collectInputSchemas(plan: LogicalPlan, writeQuery: Option[LogicalPlan]): List[(String, StructType)] =
    (
      plan.collect(recognizedRead) ++
        writeQuery.toList.flatMap(_.collect(recognizedRead))
    ).distinct.toList

  /** The check logic itself, exposed directly for tests and for callers
    * that want to verify without going through `SparkSession` construction
    * (`forContract` is a thin adapter to the shape `injectCheckRule` wants).
    */
  private[sparkadapter] def verifyOrThrow(
      contract: Contract,
      plan: LogicalPlan,
      options: VerificationOptions,
      sink: Option[NotificationSink] = None,
      applicationId: Option[String] = None
  ): Unit = {
    val translated = SparkPlanAdapter.translate(plan)
    translated.plan match {
      case _: com.invaract.ir.Write =>
        // Every check below assumes a *structurally sound* contract -
        // StructuralVerifier.verify in particular reads contract.outputs.head
        // unconditionally. `injectCheckRule` calls this method for every
        // plan Spark analyzes in the session, not just writes, so this
        // guard belongs inside the write (and, below, state-changing-CALL)
        // branch specifically - guarding the whole method crashed an
        // unrelated plain read/transformation the moment an invalid
        // contract was merely *active*, confirmed the hard way by a real
        // test failure. ContractParser.parse never validates on its own (a
        // caller must invoke ContractValidator explicitly), and nothing
        // else on this path did either - exactly how a missing `outputs:`
        // key used to crash verify() with an unguarded
        // NoSuchElementException instead of a clean, actionable rejection.
        requireValidContract(contract, sink, applicationId)

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
        // WriteCommandSupport.combined is the same lookup translation used
        // to reach this ir.Write in the first place, so this can never
        // drift out of sync with it the way three independent matches
        // could (and once did - see WriteCommandSupport's class doc).
        // Computed once and reused by both inputSchemas and outputSchema
        // below, rather than each re-deriving it independently.
        val writeInfo = WriteCommandSupport.combined.lift(plan)
        val inputSchemas = collectInputSchemas(plan, writeInfo.map(_.query))
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
        val outputSchema = writeInfo.map(_.outputSchema).getOrElse(plan.schema)
        val structuralResult = StructuralVerifier.verify(contract, translated.plan, inputSchemas, outputSchema, options)
        // Checked alongside (never instead of) StructuralVerifier's own
        // checks: RowMutationSupport.classify is a separate, independent
        // classifier over the same `plan` (see its class doc for why it
        // isn't folded into WriteCommandInfo itself) - `None` for every
        // write shape that isn't row-level DML, a no-op for the vast
        // majority of writes a contract governs. `Extracted` runs the
        // normal rule check; `Unverifiable` (this module recognized the
        // plan as DML of a given kind but couldn't extract what a rule of
        // that kind needs - see RowMutationSupport's class doc) fails
        // closed instead of silently skipping the rule, but only when the
        // contract actually declares a rule that kind is relevant to -
        // RuleVerifier.appliesTo decides that, so an UPDATE this module
        // can't fully verify doesn't spuriously fail a contract that only
        // declares forbid_unconditional_delete, say.
        val ruleViolations = RowMutationSupport.classify(plan) match {
          case Some(RowMutationSupport.Classification.Extracted(_, mutation)) =>
            RuleVerifier.verify(contract.rules, mutation)
          case Some(RowMutationSupport.Classification.Unverifiable(kind)) =>
            val declaredRules = contract.rules.flatMap(_.interpret)
            if (declaredRules.exists(RuleVerifier.appliesTo(_, kind))) List(unverifiableDmlViolation(kind)) else Nil
          case None => Nil
        }
        val result = VerificationResult.of(structuralResult.contract, structuralResult.violations ++ ruleViolations)
        publishValidation(contract, result, sink, applicationId)
        if (!result.passed) {
          throw new ContractViolationException(result, explain(contract, translated.plan, result))
        }
      case _ =>
        // Checked before the fail-closed Command catch-all below: a
        // recognized state-changing CALL (nine procedures - see
        // StateChangingCallSupport) genuinely verifies the resulting
        // state, rather than being rejected outright the way it was
        // before this case existed. rewrite_table_path (the one remaining
        // state-changing procedure) is instead safe-listed in
        // FailClosedCommands, having no state a contract could ever check.
        StateChangingCallSupport.extract(plan) match {
          case Some(info) =>
            // Same reasoning as the ir.Write branch above: verifyStateChange
            // assumes a structurally sound contract too.
            requireValidContract(contract, sink, applicationId)
            val result = StructuralVerifier.verifyStateChange(contract, info.location, info.resultingSchema, options)
            publishValidation(contract, result, sink, applicationId)
            if (!result.passed) {
              // No ir.Plan translation exists for a state-changing CALL
              // (there's no Spark write/query to translate) - a plain
              // description standing in for `explain`'s usual rendered
              // plan tree, reusing the rest of its explanation format
              // unchanged.
              val describedPlan =
                com.invaract.ir.UnknownPlan(s"CALL ${info.callName}(...) targeting '${info.location}'")
              throw new ContractViolationException(result, explain(contract, describedPlan, result))
            }
          case None if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
            val violation = Violation(
              ViolationType.UnverifiableWrite,
              s"'${plan.getClass.getSimpleName}' looks like it may write or otherwise mutate data, but Invaract has no " +
                s"translation for it, so it was never checked against contract '${contract.id}@${contract.version}'.",
              remediation =
                "If this command genuinely doesn't write data, add its class to FailClosedCommands' known-safe list " +
                  "(with the same reasoning documented there) and open an issue/PR. If it does write data, that's a " +
                  "real translation gap in SparkPlanAdapter - see docs/SPARK_ADAPTER.md's " +
                  "\"Fail-closed on unverifiable writes\" section."
            )
            val result = VerificationResult.of(s"${contract.id}@${contract.version}", List(violation))
            publishValidation(contract, result, sink, applicationId)
            throw new ContractViolationException(result, explain(contract, translated.plan, result))
          case None =>
            () // not a Command at all (a Read/Project/Filter/...) - definitely not a write
        }
    }
  }

  /** The dry-run counterpart to `verifyOrThrow`: only the plain-write shape
    * (backed by a real `WriteCommandInfo` from `WriteCommandSupport.combined`
    * — the same lookup `SparkPlanAdapter.translatePlan`'s own `ir.Write`
    * case consults, so a match here is guaranteed to translate to `ir.Write`
    * too, with no need to also run that translation just to re-check it) is
    * inferrable — see `dryRun`'s own doc for why state-changing CALLs and
    * row-level DML are deliberately out of scope. Everything else (a
    * `.count()`, an intermediate transformation, a recognized-but-not-a-write
    * plan) is a silent no-op, the same "only a write matters" policy
    * `verifyOrThrow` follows for the analogous case.
    */
  private[sparkadapter] def inferOrIgnore(plan: LogicalPlan, onInferred: Contract => Unit): Unit =
    WriteCommandSupport.combined.lift(plan) match {
      case Some(writeInfo) => onInferred(ContractInference.infer(writeInfo, collectInputSchemas(plan, Some(writeInfo.query))))
      case None             => () // not a recognized write - nothing to infer a contract from
    }

  /** Throws if `contract` itself is structurally unsound per
    * `ContractValidator` (e.g. no declared outputs) - the same check every
    * other rejection in `verifyOrThrow` assumes has already passed. Not
    * called unconditionally by `verifyOrThrow` itself: see the call sites'
    * own comments for why it's scoped to just the write and state-changing-
    * CALL branches.
    */
  private def requireValidContract(contract: Contract, sink: Option[NotificationSink], applicationId: Option[String]): Unit = {
    val validation = ContractValidator.validate(contract)
    if (!validation.isValid) {
      val contractRef = s"${contract.id}@${contract.version}"
      val violations = validation.errors.map { issue =>
        Violation(
          ViolationType.InvalidContract,
          s"contract '$contractRef' is invalid at '${issue.path}': ${issue.message}",
          remediation = s"Fix the contract document (see the '${issue.path}' issue above) so it passes " +
            "ContractValidator.validate before it's used to verify any write."
        )
      }
      val result = VerificationResult.of(contractRef, violations)
      publishValidation(contract, result, sink, applicationId)
      val describedPlan = com.invaract.ir.UnknownPlan("(contract validation failed before any plan was checked)")
      throw new ContractViolationException(result, explain(contract, describedPlan, result))
    }
  }

  /** Publishes a `ContractValidationEvent` to `sink`, if one is configured —
    * a no-op otherwise, so every call site can invoke this unconditionally
    * rather than each guarding on `sink.isDefined` itself. Always called
    * *before* a FAILED result's `ContractViolationException` is thrown (see
    * every call site above), so a subscriber observes the rejection at the
    * same moment the writing job does.
    */
  private def publishValidation(
      contract: Contract,
      result: VerificationResult,
      sink: Option[NotificationSink],
      applicationId: Option[String]
  ): Unit =
    sink.foreach { s =>
      s.publish(
        ContractValidationEvent(
          contract = result.contract,
          status = result.status,
          violations = result.violations,
          timestamp = System.currentTimeMillis(),
          metadata = contract.extensions,
          applicationId = applicationId
        )
      )
    }

  /** Builds the full explanation `ContractViolationException.getMessage`
    * carries. Deterministic: built entirely from `result.violations` (an
    * already-deterministically-ordered list — see `StructuralVerifier`'s
    * "Determinism" doc) and the plan's own rendering, so the same
    * violation always produces the same message, byte for byte.
    */
  private[sparkadapter] def explain(contract: Contract, plan: com.invaract.ir.Plan, result: VerificationResult): String = {
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

  private def describeFields(fields: List[com.invaract.contract.Field]): String =
    fields
      .map(f => s"${f.name}: ${f.fieldType}" + (if (f.required) "" else " (optional)"))
      .mkString(", ")

  private def unverifiableDmlViolation(kind: RowMutationSupport.Kind): Violation = {
    val kindName = kind match {
      case RowMutationSupport.Kind.Merge  => "MERGE"
      case RowMutationSupport.Kind.Update => "UPDATE"
      case RowMutationSupport.Kind.Delete => "DELETE"
    }
    Violation(
      ViolationType.RuleUnverifiableDml,
      s"this operation is a $kindName the active contract declares a rule for, but Invaract could not " +
        "extract the structural fact that rule needs to check, so it was never actually verified.",
      remediation =
        "This is likely a genuine gap in Invaract's support for this operation's exact shape (e.g. an " +
          "Iceberg merge-on-read UPDATE, whose rewritten plan doesn't expose which columns changed) - open " +
          "an issue/PR. If the rule doesn't need to apply to this operation, remove it from the contract."
    )
  }
}
