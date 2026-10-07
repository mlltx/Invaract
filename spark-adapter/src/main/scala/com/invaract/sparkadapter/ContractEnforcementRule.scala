// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{AdapterCapabilities, CheckedWrite, ContractInference, InferredWrite, VerificationOptions, VerificationPipeline, VerificationResult, VerificationSetup}
import com.invaract.contract.{Contract, LogicalSchema, OrgPolicy}
import com.invaract.verification.notification.{InferenceStatus, NotificationSink}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal

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
  private val logger = LoggerFactory.getLogger(ContractEnforcementRule.getClass)

  /** Builds a Spark check rule (pass to
    * `SparkSession.Builder.withExtensions(_.injectCheckRule(...))`) that
    * verifies any write this session performs against `contract`, throwing
    * `ContractViolationException` to abort it if verification fails.
    *
    * Resolves any `ref://<id>` location `contract` declares (see
    * `com.invaract.verification.location`) before the first check runs -
    * see `resolveContractLocations`'s doc for why this is what makes the
    * feature attachable purely via `spark-submit --conf`, with no change
    * to the caller's own code.
    *
    * Also creates one `CheckpointRegistry` for the session: this rule sees
    * every plan Spark analyzes, so it records each one's output attribute
    * ids against its translation, and `SparkPlanAdapter` uses that to see
    * through a later `.checkpoint()` boundary (a `LogicalRDD` carrying the
    * same ids) back to the real reads and transformation - see that
    * class's own doc. Nothing is registered with Spark and nothing is
    * asynchronous, so there is no timing window.
    */
  def forContract(contract: Contract, options: VerificationOptions = VerificationOptions()): SparkSession => LogicalPlan => Unit =
    session => {
      VersionCompatibilityGuard.check(session)
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, None, None)
      val checkpointRegistry = new CheckpointRegistry
      (plan: LogicalPlan) => verifyOrThrow(governedContract, plan, governedOptions, None, checkpointRegistry = Some(checkpointRegistry))
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
    * `com.invaract.verification.notification`'s types for the full
    * mechanism, and docs-site's "Notification sinks" guide for a worked
    * example.
    */
  def forContract(contract: Contract, options: VerificationOptions, sink: NotificationSink): SparkSession => LogicalPlan => Unit =
    session => {
      VersionCompatibilityGuard.check(session)
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val runId = Some(session.sparkContext.applicationId)
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, Some(sink), runId)
      val checkpointRegistry = new CheckpointRegistry
      (plan: LogicalPlan) =>
        verifyOrThrow(governedContract, plan, governedOptions, Some(sink), runId, checkpointRegistry = Some(checkpointRegistry))
    }

  /** Spark configuration key naming an `id=location` `.properties` file
    * (the same shape `StaticMapLocationResolver.fromPropertiesFile` reads)
    * to resolve `contract`'s `ref://<id>` locations against - see
    * `com.invaract.verification.location`'s package for the syntax.
    *
    * Reading this from Spark's own configuration, rather than requiring a
    * caller to build a `LocationResolver` and call
    * `ContractLocationResolution.resolve` themselves, is what makes
    * location resolution something a platform or orchestration framework
    * can attach purely via `spark-submit --conf` - no change to the job's
    * own source at all, beyond the one line every Invaract user already
    * writes to install `forContract` in the first place. A job that wants
    * a resolver this key can't express (an `HttpLocationResolver`, a
    * mapping built at runtime) still calls `ContractLocationResolution.resolve`
    * explicitly before passing its contract to `forContract` - this
    * mechanism and that one compose freely, since a location already
    * resolved to a literal is simply left alone here (`LocationRef.id`
    * finds nothing left to resolve).
    */
  val LocationMapConfKey = "spark.invaract.locationMap"

  /** The resolver `resolveContractLocations` builds when
    * `LocationMapConfKey` isn't set on `session` - `NoOpLocationResolver`,
    * so a contract with no `ref://` locations is completely unaffected
    * (this whole mechanism is invisible to it) and one that does declare a
    * reference fails immediately with a clear, actionable message instead
    * of a confusing downstream `MissingInput`/`OutputLocationMismatch`.
    */
  private[sparkadapter] def resolveContractLocations(contract: Contract, session: SparkSession): Contract =
    VerificationSetup.resolveContractLocations(contract, SparkConfigSource(session))

  /** Spark configuration keys mirroring `VerificationOptions`'s three
    * `Boolean` flags — the same "attachable via spark-submit --conf, not
    * only via a Scala constructor argument" reasoning `LocationMapConfKey`
    * documents applies here too (see CLAUDE.md's "External Attachability
    * Requirement"). A platform can turn any of these on for a job it
    * doesn't own the source of with, e.g., `--conf
    * spark.invaract.rejectUndeclaredFields=true` — no code change needed.
    */
  val RejectUndeclaredInputsConfKey = "spark.invaract.rejectUndeclaredInputs"
  val RejectUndeclaredFieldsConfKey = "spark.invaract.rejectUndeclaredFields"
  val ComputeFingerprintConfKey = "spark.invaract.computeFingerprint"

  /** Attaches `VerificationOptions.staticDataQuality` — see that field's own
    * doc and docs/STATIC_DATA_QUALITY_VERIFICATION.md — the same
    * `spark-submit --conf spark.invaract.staticDataQuality=true`
    * attachability every other flag in this block documents.
    */
  val StaticDataQualityConfKey = "spark.invaract.staticDataQuality"

  /** Attaches `VerificationOptions.roleConsistency` — see that field's own
    * doc and docs/CONTRACT_MODEL.md's "Input and Output Types" section —
    * the same `spark-submit --conf spark.invaract.roleConsistency=true`
    * attachability every other flag in this block documents.
    */
  val RoleConsistencyConfKey = "spark.invaract.roleConsistency"

  /** Overlays the three conf keys above onto `options` — `||`, not a
    * replacement: a flag ends up `true` if *either* the caller's own
    * `VerificationOptions` already set it, or the matching conf key is
    * `"true"`, so a platform attaching a stricter check via `--conf` can
    * never be silently weakened by code that left a flag at its default,
    * and code that deliberately opted in can never be silently turned off
    * by a conf key's mere absence.
    */
  private[sparkadapter] def resolveVerificationOptions(options: VerificationOptions, session: SparkSession): VerificationOptions =
    VerificationSetup.resolveVerificationOptions(options, SparkConfigSource(session))

  /** Spark configuration key naming an organizational policy document
    * (`com.invaract.contract.OrgPolicy`, YAML — see docs/CONTRACT_MODEL.md's
    * "Organizational Policy" section) a platform attaches to *any* job that
    * already installs `forContract`, purely via `spark-submit --conf
    * spark.invaract.orgPolicy=<path>` — no change to that job's own source,
    * the same attachability `LocationMapConfKey` documents. Absent (the
    * default), org-policy enforcement is entirely inert: `enforceOrgPolicy`
    * returns `contract`/`options` unchanged, exactly today's behavior.
    */
  val OrgPolicyConfKey = "spark.invaract.orgPolicy"

  /** Spark configuration key naming additional organizational policy
    * documents — a comma-separated, ordered list of paths — layered on top
    * of `OrgPolicyConfKey`'s document for policy layering/inheritance: a
    * stricter, more specific policy (typically business-unit- or
    * team-owned) composing with the looser org-wide baseline, rather than
    * replacing it. See docs/CONTRACT_MODEL.md's "Policy layering" section
    * for the full design; in short, each overlay is *just another* ordinary
    * `OrgPolicy` YAML document (no new schema) that can only ever add
    * policies on top of the base — there is no mechanism for an overlay to
    * loosen, override, or exempt a rule the base (or an earlier overlay)
    * declares; only that rule's own owning document can do that, via its
    * own `exemptions`.
    *
    * Attachable purely via `spark-submit --conf
    * spark.invaract.orgPolicyOverlays=/policies/bu-finance.yaml,/policies/team-payments.yaml`
    * alongside the existing `spark.invaract.orgPolicy=/policies/org-wide.yaml`
    * — no change to the governed job's own source, the same attachability
    * every other `spark.invaract.*` key in this class documents. Setting
    * this key without `OrgPolicyConfKey` also set is rejected
    * (`resolveOrgPolicyLayers` throws) rather than silently treating the
    * first overlay as the base: layering is additive to a named org-wide
    * anchor, not a substitute for one, and allowing it to silently stand in
    * for a missing base risks a misconfigured job losing org-wide
    * governance entirely rather than failing loudly.
    */
  val OrgPolicyOverlaysConfKey = "spark.invaract.orgPolicyOverlays"

  /** Resolves the full, ordered stack of organizational policy layers this
    * session has configured, each paired with the path it was loaded from
    * (so a later validation failure can name exactly which layer it came
    * from, not just "the" policy): `OrgPolicyConfKey`'s document first (the
    * org-wide base), followed by each comma-separated path named by
    * `OrgPolicyOverlaysConfKey`, in the order listed. `Nil` when neither key
    * is set — a job with no org policy attached at all pays no cost and
    * sees no behavior change, exactly as when only the single-document
    * `OrgPolicyConfKey` mechanism existed.
    *
    * Every layer functions identically once resolved — a business-unit
    * overlay is not a different kind of document, just another `OrgPolicy`
    * parsed the same way as the base (see `OrgPolicyOverlaysConfKey`'s own
    * doc for why layering needs no new document shape at all).
    */
  private[sparkadapter] def resolveOrgPolicyLayers(session: SparkSession): List[(String, OrgPolicy)] =
    VerificationSetup.resolveOrgPolicyLayers(SparkConfigSource(session))

  /** Governs `contract`/`options` through the full, ordered stack of
    * organizational policy layers configured for this session
    * (`resolveOrgPolicyLayers`), if any — the eager, "stop ASAP" counterpart
    * to `resolveContractLocations`/`resolveVerificationOptions`, called once
    * per session build rather than per plan: a policy rule depends only on
    * the contract's own declared shape (does it declare a catalog, a
    * required field, field names matching a convention), never on what a
    * specific write actually does, so there's no reason to wait for a write
    * to happen before rejecting a non-compliant contract.
    *
    * Three things happen, in order, when at least one layer is configured:
    *   1. Every layer's `inject.rules` are merged into `contract.rules`
    *      (`OrgPolicyEvaluator.applyInjectedRulesFromLayers`) and every
    *      layer's `inject.minVerificationOptions` floors are ORed onto
    *      `options` (`applyMinVerificationOptions`, folded across layers) —
    *      so an org-wide *or* overlay DML rule, or a forced
    *      `VerificationOptions` flag from either, applies to every write
    *      this session's `verifyOrThrow` later checks, with no further
    *      change needed there.
    *   2. Every layer is evaluated independently and its violations unioned
    *      (`OrgPolicyEvaluator.evaluateLayers`) against the
    *      (already-injected) contract. `Warn`-mode violations, from any
    *      layer, are published to `sink` (if any) as one informational,
    *      non-blocking `VerificationResult` — this is the rollout
    *      mechanism: a platform (or a business unit, for its own overlay)
    *      introduces a new policy in `warn`, watches violations accumulate,
    *      then flips it to `enforce`, no code change on any governed job.
    *   3. Any `Enforce`-mode violation, from any layer, throws
    *      `ContractViolationException` immediately, through the exact same
    *      `Violation`/`explain`/notification-sink path a structural
    *      violation uses — before this method returns, so before
    *      `forContract`'s caller ever gets back a plan-check function to
    *      install. A non-compliant contract can't even finish installing.
    *
    * Throws `OrgPolicyParseException` (from parsing, from
    * `resolveOrgPolicyLayers` rejecting an overlay configured with no base,
    * from `OrgPolicyValidator` finding a layer itself malformed — e.g. an
    * exemption referencing a policy id outside that same layer's own
    * `policies`, which is exactly how a layer is prevented from exempting a
    * *different* layer's rule — or from
    * `requireKnownMinVerificationOptionKeys` rejecting an unrecognized
    * `inject.minVerificationOptions` key in any layer) rather than silently
    * ignoring a broken policy document, the same "fail loudly, not
    * quietly," principle `ContractParser`/`ContractValidator` apply to a
    * malformed contract.
    */
  private[sparkadapter] def enforceOrgPolicy(
      contract: Contract,
      options: VerificationOptions,
      session: SparkSession,
      sink: Option[NotificationSink],
      runId: Option[String]
  ): (Contract, VerificationOptions) =
    VerificationSetup.enforceOrgPolicy(contract, options, SparkConfigSource(session), sink, runId)

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
    * excluding state-changing CALLs. Row-level DML (MERGE/UPDATE/DELETE) is
    * *not* excluded, contrary to what this doc used to say: `combined`
    * includes those cases, so a draft is inferred from the target's current
    * schema - weakly, since DML has no "new output" (see `WriteCommandInfo`'s
    * row-level-DML cases in `WriteCommandSupport`). `DryRunReporter`
    * flags such a draft degraded. `injectCheckRule` fires on every analyzed plan the session
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
      val checkpointRegistry = new CheckpointRegistry
      (plan: LogicalPlan) => inferOrIgnore(plan, onInferred, Some(checkpointRegistry))
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
  private def collectInputSchemas(plan: LogicalPlan, writeQuery: Option[LogicalPlan]): List[(String, LogicalSchema)] =
    (
      plan.collect(recognizedRead) ++
        writeQuery.toList.flatMap(_.collect(recognizedRead))
    ).distinct.toList.map { case (location, schema) => location -> SparkSchemas.toLogicalSchema(schema) }

  /** The registry is a best-effort aid: it can only ever turn an unresolved
    * boundary into a resolved one, so a failure inside it (a pathologically
    * deep plan overflowing the stack, an unexpected Catalyst shape) must
    * degrade to "not resolved" - the behavior with no registry at all - and
    * never fail the job's own query analysis.
    */
  private def resolveCheckpoints(
      registry: Option[CheckpointRegistry],
      analyzedPlan: LogicalPlan
  ): (LogicalPlan, List[Diagnostic]) =
    registry match {
      case None => (analyzedPlan, Nil)
      case Some(r) =>
        failSafe("resolve", (analyzedPlan, List.empty[Diagnostic])) {
          r.bind(analyzedPlan)
          val substitution = r.substitute(analyzedPlan)
          (substitution.plan, substitution.diagnostics)
        }
    }

  private def recordCheckpointOrigin(registry: Option[CheckpointRegistry], plan: LogicalPlan, translated: com.invaract.ir.Plan): Unit =
    registry.foreach(r => failSafe("record", ())(r.record(plan, translated)))

  private def failSafe[A](what: String, fallback: A)(body: => A): A =
    try body
    catch {
      case NonFatal(e) => degraded(what, e, fallback)
      case e: StackOverflowError => degraded(what, e, fallback)
    }

  private def degraded[A](what: String, e: Throwable, fallback: A): A = {
    logger.warn(s"CheckpointRegistry could not $what .checkpoint() boundaries; treating them as opaque: $e")
    fallback
  }

  /** The check logic itself, exposed directly for tests and for callers
    * that want to verify without going through `SparkSession` construction
    * (`forContract` is a thin adapter to the shape `injectCheckRule` wants).
    *
    * What is Spark-specific happens here - checkpoint resolution, Catalyst
    * translation, recognizing a write / state-changing CALL / unverifiable
    * command, mapping schemas, classifying DML. Everything after that (the
    * checks, the fingerprint, the event, the rejection) is
    * `VerificationPipeline`, the same for every engine.
    */
  private[sparkadapter] def verifyOrThrow(
      contract: Contract,
      analyzedPlan: LogicalPlan,
      options: VerificationOptions,
      sink: Option[NotificationSink] = None,
      runId: Option[String] = None,
      checkpointRegistry: Option[CheckpointRegistry] = None,
      capabilities: Option[AdapterCapabilities] = SparkCapabilities.declared
  ): Unit = {
    // Every analyzed plan is offered to the registry - not just writes: a
    // plan that is later checkpointed is a plain query Dataset, seen here
    // long before any write (see CheckpointRegistry's class doc). A bare
    // checkpointed Dataset is bound to its origin the moment it first
    // appears; each resolvable `.checkpoint()` boundary in `analyzedPlan` is
    // then replaced by the plan it was made from, so everything below -
    // reads, schemas, lineage, fingerprint - works on the real
    // transformation.
    val (plan, resolutionDiagnostics) = resolveCheckpoints(checkpointRegistry, analyzedPlan)
    val translatedRaw = SparkPlanAdapter.translate(plan)
    val translated = translatedRaw.copy(diagnostics = translatedRaw.diagnostics ++ resolutionDiagnostics)
    recordCheckpointOrigin(checkpointRegistry, plan, translated.plan)
    translated.plan match {
      case _: com.invaract.ir.Write =>
        // What `SparkAdapterListener.lastWrite` reports for this write, so it
        // describes the same (checkpoint-resolved) plan this check does.
        SparkAdapterListener.stash(analyzedPlan, translated)
        // `injectCheckRule` calls this method for every plan Spark analyzes in
        // the session, not just writes, so the contract-validity guard lives in
        // `VerificationPipeline` and only runs for a write (and, below, a
        // state-changing CALL) - guarding the whole method crashed an unrelated
        // plain read/transformation the moment an invalid contract was merely
        // *active*. `checkedWrite` is passed by-name: the pipeline validates the
        // contract first and only then asks for the write.
        VerificationPipeline.verifyWrite(contract, checkedWrite(plan, translated), options, sink, runId, capabilities)
      case _ =>
        // Checked before the fail-closed Command catch-all below: a recognized
        // state-changing CALL (nine procedures - see StateChangingCallSupport)
        // genuinely verifies the resulting state, rather than being rejected
        // outright. rewrite_table_path (the one remaining state-changing
        // procedure) is instead safe-listed in FailClosedCommands, having no
        // state a contract could ever check.
        StateChangingCallSupport.extract(plan) match {
          case Some(info) =>
            VerificationPipeline.verifyStateChange(
              contract,
              s"CALL ${info.callName}(...) targeting '${info.location}'",
              info.location,
              SparkSchemas.toLogicalSchema(info.resultingSchema),
              SQLConf.get.caseSensitiveAnalysis,
              options,
              sink,
              runId
            )
          case None if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
            VerificationPipeline.rejectUnverifiableWrite(contract, plan.getClass.getSimpleName, translated.plan, sink, runId)
          case None =>
            () // not a Command at all (a Read/Project/Filter/...) - definitely not a write
        }
    }
  }

  /** What this adapter hands `VerificationPipeline` for a plan it translated to
    * an `ir.Write`.
    *
    * `WriteCommandSupport.combined` is the same lookup translation used to
    * reach that `ir.Write` in the first place, so this can never drift from it.
    * Its `query` is walked for reads too: Delta's row-level DML commands are
    * effectively leaf nodes in the tree-traversal sense (their `source`/`target`
    * are ordinary case-class fields, not children), so `plan.collect` alone
    * would miss them - confirmed empirically by a real FAIL test never
    * throwing.
    *
    * The output schema is always the underlying query's schema, never the
    * command node's own: a Command's `.schema` is its own (typically empty)
    * output, not the data it writes - using that directly silently reported
    * every declared field as missing (see docs/SPARK_ADAPTER.md's "Delta Lake
    * support"). The `plan.schema` fallback only matters for an `ir.Write`
    * produced some other way (not currently possible).
    *
    * Declared field names are matched the way Spark matches columns: by the
    * session's `spark.sql.caseSensitive`, read per check so a runtime change is
    * honoured (`SQLConf.get` is the active session's conf on the analyzer
    * thread this rule runs on).
    */
  private def checkedWrite(plan: LogicalPlan, translated: TranslationResult): CheckedWrite = {
    val writeInfo = WriteCommandSupport.combined.lift(plan)
    CheckedWrite(
      plan = translated.plan,
      inputSchemas = collectInputSchemas(plan, writeInfo.map(_.query)),
      outputSchema = SparkSchemas.toLogicalSchema(writeInfo.map(_.outputSchema).getOrElse(plan.schema)),
      caseSensitive = SQLConf.get.caseSensitiveAnalysis,
      // Classified once; a separate, independent classifier over the same plan
      // (see RowMutationSupport's class doc) - `None` for every write shape that
      // isn't row-level DML.
      rowMutation = RowMutationSupport.classify(plan),
      lineageBoundaryTypes = CheckpointRegistry.BoundarySourceTypes,
      // A resolution that had to pick the most recent of several same-source
      // plans is an assumption the fingerprint cannot reflect; disclosed with it.
      resolutionNotes = translated.diagnostics.filter(_.nodeType == CheckpointRegistry.ResolutionDiagnosticType).map(_.message)
    )
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
    *
    * Also runs `SparkPlanAdapter.translate` — the same translation
    * `verifyOrThrow` already performs for real enforcement — so
    * `ContractInference.infer` can observe each input's actual usage
    * (contributes to an output column vs. filter/join-only) via the real
    * `ir.Plan`/`ir.Lineage`, rather than dry-run and real enforcement each
    * maintaining their own notion of the transformation. See
    * `ContractInference`'s own doc for why this observation is surfaced as
    * a description, never a declared `datasetType`.
    */
  private[sparkadapter] def inferOrIgnore(
      analyzedPlan: LogicalPlan,
      onInferred: Contract => Unit,
      checkpointRegistry: Option[CheckpointRegistry] = None
  ): Unit =
    inferOutcome(analyzedPlan, checkpointRegistry).foreach {
      case outcome: InferenceOutcome.Inferred => onInferred(outcome.contract)
      case _: InferenceOutcome.Skipped => () // surfaced by DryRunReporter; this callback only ever wanted contracts
    }

  /** What dry-run mode made of one analyzed plan - the structured form
    * `inferOrIgnore` flattens to "a contract, or nothing". `DryRunReporter`
    * consumes this so a write dry-run mode could not infer from is reported
    * rather than indistinguishable from a plan that was never a write.
    */
  private[sparkadapter] sealed trait InferenceOutcome
  private[sparkadapter] object InferenceOutcome {

    /** `plan` is the checkpoint-resolved plan `contract` was inferred from, and `translated`
      * its translation - everything a self-check against that same write needs.
      */
    final case class Inferred(
        contract: Contract,
        plan: LogicalPlan,
        translated: TranslationResult,
        writeInfo: WriteCommandInfo,
        inputSchemas: List[(String, LogicalSchema)]
    ) extends InferenceOutcome

    /** A write-shaped plan with no inference: `status` is an `InferenceStatus`. */
    final case class Skipped(status: String, reason: String) extends InferenceOutcome
  }

  /** `None` for a plan that is not write-shaped at all (a read, a `.count()`, an
    * intermediate transformation) - the only silent case.
    *
    * The two `Skipped` cases mirror exactly what `verifyOrThrow` does with a
    * non-`ir.Write` plan, in the same order: a recognized state-changing CALL
    * first, then the fail-closed `Command` catch-all (`FailClosedCommands`).
    * Reusing those predicates, rather than a second opinion on what "might be
    * a write", means a plan enforcement would block is a plan dry-run reports
    * - which is the whole point of dry-run as a rehearsal for enforcement.
    */
  private[sparkadapter] def inferOutcome(
      analyzedPlan: LogicalPlan,
      checkpointRegistry: Option[CheckpointRegistry] = None
  ): Option[InferenceOutcome] = {
    // The same checkpoint resolution real enforcement does (see verifyOrThrow),
    // so a write downstream of a `.checkpoint()` infers the inputs it really
    // read rather than an empty contract.
    val (plan, _) = resolveCheckpoints(checkpointRegistry, analyzedPlan)
    lazy val translated = SparkPlanAdapter.translate(plan)
    checkpointRegistry.foreach(_ => recordCheckpointOrigin(checkpointRegistry, plan, translated.plan))
    WriteCommandSupport.combined.lift(plan) match {
      case Some(writeInfo) =>
        SparkAdapterListener.stash(analyzedPlan, translated)
        val inputSchemas = collectInputSchemas(plan, Some(writeInfo.query))
        Some(
          InferenceOutcome.Inferred(
            ContractInference.infer(
              InferredWrite(writeInfo.location, writeInfo.format, writeInfo.saveMode, SparkSchemas.toLogicalSchema(writeInfo.outputSchema)),
              inputSchemas,
              translated.plan
            ),
            plan,
            translated,
            writeInfo,
            inputSchemas
          )
        )
      case None =>
        StateChangingCallSupport.extract(plan) match {
          case Some(info) =>
            Some(
              InferenceOutcome.Skipped(
                InferenceStatus.SkippedUnsupported,
                s"state-changing CALL ${info.callName}(...) targeting '${info.location}': dry-run mode infers a " +
                  "contract from a write's output schema, and a procedure call has none"
              )
            )
          case None if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
            Some(
              InferenceOutcome.Skipped(
                InferenceStatus.SkippedUnrecognized,
                s"'${plan.getClass.getSimpleName}' may write data but Invaract has no translation for it; with a " +
                  "contract active, enforcement would reject it as an UNVERIFIABLE_WRITE"
              )
            )
          case None => None
        }
    }
  }

  /** Builds the full explanation `ContractViolationException.getMessage`
    * carries - see `VerificationPipeline.explain`, which owns it (it is the
    * same for every engine); kept here as the spelling this module's own
    * tests use.
    */
  private[sparkadapter] def explain(contract: Contract, plan: com.invaract.ir.Plan, result: VerificationResult): String =
    VerificationPipeline.explain(contract, plan, result)
}
