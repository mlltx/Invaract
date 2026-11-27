// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{AdapterCapabilities, AttachSetup, StructuralVerifier, CheckedWrite, ContractInference, InferredWrite, VerificationOptions, VerificationPipeline, VerificationResult, VerificationSetup}
import com.invaract.contract.{Contract, LogicalSchema, OrgPolicy}
import com.invaract.verification.notification.{InferenceStatus, JobInfo, NotificationSink}

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
    session =>
      // OpenLineage attaches through configuration alone, to a job that installs Invaract in code too
      AttachSetup.openLineageSink(SparkConfigSource(session)) match {
        case Some(openLineage) => forContract(contract, options, openLineage)(session)
        case None              => withoutSink(contract, options)(session)
      }

  private def withoutSink(contract: Contract, options: VerificationOptions): SparkSession => LogicalPlan => Unit =
    session => {
      SparkSettingNames.check(session, enforcing = true)
      VersionCompatibilityGuard.check(session)
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, None, None)
      val checkpointRegistry = new CheckpointRegistry
      // every write checked is recorded, so a declared output never written is reported when the job ends
      val outputs = DeclaredOutputs.forJob(session, governedContract, None, None, None)
      (plan: LogicalPlan) =>
        verifyOrThrow(governedContract, plan, governedOptions, None, None, Some(checkpointRegistry), SparkCapabilities.declared, None, outputs.record)
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
    session => withSink(contract, options, AttachSetup.withOpenLineage(Some(sink), SparkConfigSource(session)).get)(session)

  private def withSink(contract: Contract, options: VerificationOptions, sink: NotificationSink): SparkSession => LogicalPlan => Unit =
    session => {
      SparkSettingNames.check(session, enforcing = true)
      VersionCompatibilityGuard.check(session)
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val applicationId = Some(session.sparkContext.applicationId)
      // Which engine and job these events come from - the same identity dry-run mode reports (DryRunReporter.jobInfoOf).
      val job = Some(DryRunReporter.jobInfoOf(session))
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, Some(sink), applicationId, job)
      val checkpointRegistry = new CheckpointRegistry
      // every write checked is recorded, so a declared output never written is reported when the job ends
      val outputs = DeclaredOutputs.forJob(session, governedContract, Some(sink), applicationId, job)
      (plan: LogicalPlan) =>
        verifyOrThrow(governedContract, plan, governedOptions, Some(sink), applicationId, Some(checkpointRegistry), SparkCapabilities.declared, job, outputs.record)
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
    * writes to install `forContract` in the first place. A location service
    * is attached the same way (`LocationUrlConfKey`). A job that wants
    * a resolver these keys can't express (a mapping built at runtime)
    * still calls `ContractLocationResolution.resolve`
    * explicitly before passing its contract to `forContract` - this
    * mechanism and that one compose freely, since a location already
    * resolved to a literal is simply left alone here (`LocationRef.id`
    * finds nothing left to resolve).
    */
  val LocationMapConfKey = "spark.invaract.locationMap"

  /** `spark.invaract.locationUrl` - the base URL of a location service that
    * `ref://<id>` locations the location map does not hold are asked of
    * (`HttpLocationResolver`: `GET <url>/locations/<id>`), resolved with
    * the location map by `resolveContractLocations`.
    */
  val LocationUrlConfKey = "spark.invaract.locationUrl"

  /** `spark.invaract.locationTokenEnv` - the name of the environment
    * variable, on the driver, holding the location service's bearer token:
    * the name, so the token itself never appears in the job's Spark
    * configuration (which the Spark UI and event logs show).
    */
  val LocationTokenEnvConfKey = "spark.invaract.locationTokenEnv"

  /** The resolver `resolveContractLocations` builds when
    * `LocationMapConfKey` isn't set on `session` resolves nothing,
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

  /** Names `VerificationOptions.runtimeDataQuality`, for completeness: Spark
    * declares `check.runtimeDataQuality` unsupported (its check rule judges a
    * write's plan before any row is computed), so a contract with output
    * constraints run with this set is refused (`UNSUPPORTED_CONTRACT_FEATURE`)
    * rather than passed with its constraints unchecked.
    */
  val RuntimeDataQualityConfKey = "spark.invaract.runtimeDataQuality"

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
      applicationId: Option[String],
      job: Option[JobInfo] = None
  ): (Contract, VerificationOptions) =
    VerificationSetup.enforceOrgPolicy(contract, options, SparkConfigSource(session), sink, applicationId, job)

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
      SparkSettingNames.check(session, enforcing = false)
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
  private def recognizedRead(declaredInputs: Seq[String]): PartialFunction[LogicalPlan, List[(String, StructType)]] = {
    // Every location the relation reads, attributed as its translation attributes them (`SparkPlanAdapter.locationsOf`),
    // so a schema is keyed by the same location string the plan's `ir.Read` carries; were they to differ, that input's
    // schema would silently stop being checked.
    case lr: LogicalRelation => SparkPlanAdapter.locationsOf(lr, declaredInputs).map(_ -> lr.schema)
    case sr: StreamingRelation => List(SparkPlanAdapter.streamingRelationLocationOf(sr) -> sr.schema)
    case sr2: StreamingRelationV2 => List(SparkPlanAdapter.streamingRelationV2LocationOf(sr2) -> sr2.schema)
    // A batch DataSourceV2 catalog read - see SparkPlanAdapter's own
    // DataSourceV2Relation case for why this is needed at all (any "pure"
    // DSv2 connector's reads, Iceberg's included, previously fell through
    // to the generic Unsupported translation and so could never satisfy a
    // contract's declared input).
    case dsv2: DataSourceV2Relation => List(SparkPlanAdapter.tableLocationAndFormat(dsv2.table)._1.getOrElse(dsv2.name) -> dsv2.schema)
    // A real Hive-format catalog table read - see SparkPlanAdapter's own
    // HiveTableRelation case for why this is needed at all (a genuinely
    // Hive-native table, e.g. non-Parquet/ORC or with metastore conversion
    // disabled, previously fell through to the generic Unsupported
    // translation, so a contract declaring one as a required `input`
    // always reported MISSING_INPUT even though data was genuinely read).
    case htr: HiveTableRelation => List(SparkPlanAdapter.hiveTableRelationLocationOf(htr) -> htr.schema)
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
  private def collectInputSchemas(plan: LogicalPlan, writeQuery: Option[LogicalPlan], declaredInputs: Seq[String]): List[(String, LogicalSchema)] =
    (
      // With subqueries: a read inside an IN / EXISTS / scalar subquery is a read like any other.
      plan.collectWithSubqueries(recognizedRead(declaredInputs)).flatten ++
        writeQuery.toList.flatMap(_.collectWithSubqueries(recognizedRead(declaredInputs)).flatten)
    ).distinct.toList.map { case (location, schema) => location -> SparkSchemas.toLogicalSchema(schema) }

  /** The format of every recognized read `collectInputSchemas` finds, by the same location, where Spark says what it is
    * (`SparkPlanAdapter.readFormatOf`), for `check.inputFormat`. */
  private def collectInputFormats(plan: LogicalPlan, writeQuery: Option[LogicalPlan], declaredInputs: Seq[String]): Map[String, String] =
    (plan :: writeQuery.toList).flatMap(_.collectWithSubqueries(Function.unlift { read: LogicalPlan =>
      recognizedRead(declaredInputs).lift(read).map { reads =>
        reads.flatMap { case (location, _) => SparkPlanAdapter.readFormatOf(read).map(location -> _) }
      }
    }).flatten).toMap

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
      applicationId: Option[String] = None,
      checkpointRegistry: Option[CheckpointRegistry] = None,
      capabilities: Option[AdapterCapabilities] = SparkCapabilities.declared,
      job: Option[JobInfo] = None
  ): Unit = verifyOrThrow(contract, analyzedPlan, options, sink, applicationId, checkpointRegistry, capabilities, job, _ => ())

  /** `verifyOrThrow`, telling `written` every write it checks (`DeclaredOutputs.record`). */
  private[sparkadapter] def verifyOrThrow(
      contract: Contract,
      analyzedPlan: LogicalPlan,
      options: VerificationOptions,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      checkpointRegistry: Option[CheckpointRegistry],
      capabilities: Option[AdapterCapabilities],
      job: Option[JobInfo],
      written: com.invaract.ir.Write => Unit
  ): Unit = {
    def translate(): Translation = translateStage(analyzedPlan, checkpointRegistry, contract.inputs.map(_.location))
    def check(t: Translation): Unit =
      checkStage(contract, analyzedPlan, t, options, sink, applicationId, capabilities, job, written)
    // A plan too deep for this thread's stack is tried again on a thread with a large one (`DeepPlans`). Only the stage
    // that overflowed is run again: translation has a side effect that must happen once (`TableCreations.claim`), and an
    // overflow in the check happens while it is computing, before it publishes anything.
    DeepPlans.attempt(translate()) match {
      case Right(t) =>
        DeepPlans.attempt(check(t)).left.foreach { _ =>
          DeepPlans.onLargeStack(check(t)).left.foreach(_ => tooDeep(contract, analyzedPlan, sink, applicationId, job))
        }
      case Left(_) =>
        DeepPlans.onLargeStack(check(translate())).left.foreach(_ => tooDeep(contract, analyzedPlan, sink, applicationId, job))
    }
  }

  /** `analyzedPlan` with its checkpoints resolved, and its translation. */
  private final case class Translation(plan: LogicalPlan, translated: TranslationResult, declaredInputs: Seq[String])

  private def translateStage(analyzedPlan: LogicalPlan, checkpointRegistry: Option[CheckpointRegistry], declaredInputs: Seq[String]): Translation = {
    // Every analyzed plan is offered to the registry - not just writes: a
    // plan that is later checkpointed is a plain query Dataset, seen here
    // long before any write (see CheckpointRegistry's class doc). A bare
    // checkpointed Dataset is bound to its origin the moment it first
    // appears; each resolvable `.checkpoint()` boundary in `analyzedPlan` is
    // then replaced by the plan it was made from, so everything below -
    // reads, schemas, lineage, fingerprint - works on the real
    // transformation.
    val (plan, resolutionDiagnostics) = resolveCheckpoints(checkpointRegistry, analyzedPlan)
    val translatedRaw = SparkPlanAdapter.translate(plan, declaredInputs)
    // The insert inside a new-table `.saveAsTable()` gets the table it is filling (see `TableCreations`).
    val translated = translatedRaw.copy(plan = TableCreations.claim(translatedRaw.plan), diagnostics = translatedRaw.diagnostics ++ resolutionDiagnostics)
    recordCheckpointOrigin(checkpointRegistry, plan, translated.plan)
    Translation(plan, translated, declaredInputs)
  }

  private def checkStage(
      contract: Contract,
      analyzedPlan: LogicalPlan,
      translation: Translation,
      options: VerificationOptions,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      capabilities: Option[AdapterCapabilities],
      job: Option[JobInfo],
      written: com.invaract.ir.Write => Unit
  ): Unit = {
    val Translation(plan, translated, declaredInputs) = translation
    translated.plan match {
      case w: com.invaract.ir.Write =>
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
        // An output that declares partition keys, written by a shape whose partitioning Spark does not report, is refused
        // as unverifiable rather than passed unchecked (`PartitioningSupport`); the contract is held to be valid first, so
        // an invalid one is still reported as such.
        val partitioning = WriteCommandSupport.combined.lift(plan).flatMap(_.partitioning)
        PartitioningSupport.refusal(contract, w, partitioning, plan.getClass.getSimpleName).foreach { operation =>
          VerificationPipeline.requireValidContract(contract, sink, applicationId, job)
          VerificationPipeline.rejectUnverifiableWrite(contract, operation, translated.plan, sink, applicationId, job)
        }
        // With rejectUndeclaredInputs the platform asked for every input to be visible. A Dataset built from an RDD hides
        // what it was computed from (it is not a lineage boundary to see through, like a checkpoint: it is a source of its
        // own), so the write cannot be shown to read only declared inputs, and is refused rather than passed.
        if (options.rejectUndeclaredInputs &&
            StructuralVerifier.collectUnknownPlans(translated.plan).exists(_.sourceType == SparkPlanAdapter.RddRelationSourceType)) {
          VerificationPipeline.requireValidContract(contract, sink, applicationId, job)
          VerificationPipeline.rejectUnverifiableWrite(contract, RddBackedSource, translated.plan, sink, applicationId, job)
        }
        // One statement can be checked more than once (a saveAsTable, then the insert it runs): every check of it
        // publishes the same key (`WriteKeys`).
        val writeKey = WriteKeys.keyFor(analyzedPlan, com.invaract.verification.OutputChecker.namesOf(w))
        VerificationPipeline.verifyWrite(
          contract, checkedWrite(plan, translated, declaredInputs), options, sink, applicationId, capabilities, job, Some(writeKey)
        )
        // Recorded only once it has passed: a write the check blocked never lands, so it must not count as the output
        // having been written (`DeclaredOutputs`). A write that passes here and then fails while Spark runs it is still
        // recorded; that failure reaches the job as an exception of its own.
        written(w)
        // Passed: if this creates a data-source table, the insert Spark runs next fills it (see `TableCreations`).
        plan match {
          case _: org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand =>
            w.catalog.foreach(TableCreations.expect(w.dataset.location, _))
          case _ => ()
        }
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
              info.tableName.toList,
              SparkSchemas.toLogicalSchema(info.resultingSchema),
              SQLConf.get.caseSensitiveAnalysis,
              options,
              sink,
              applicationId,
              job
            )
          case None =>
            // DROP TABLE, TRUNCATE TABLE, ALTER TABLE ... DROP PARTITION, then the ALTER TABLE statements that change an
            // output's columns or location: each judged against what the contract says about the table, before the
            // fail-closed catch-all (DestructiveDdlSupport, SchemaDdlSupport).
            DestructiveDdlSupport.extract(plan) match {
              case Some(ddl) => checkRemoval(contract, ddl, sink, applicationId, job)
              case None =>
                SchemaDdlSupport.extract(plan) match {
                  case Some(ddl) => checkTableChange(contract, ddl, options, sink, applicationId, job)
                  case None if plan.isInstanceOf[Command] && !FailClosedCommands.isKnownSafe(plan) =>
                    VerificationPipeline.rejectUnverifiableWrite(
                      contract, FailClosedCommands.unverifiable(plan), translated.plan, sink, applicationId, job
                    )
                  case None if WriteCommandSupport.isForeachBatch(plan) =>
                    if (loggedForeachBatch.compareAndSet(false, true))
                      logger.info("Invaract: a foreachBatch stream is checked through the writes its function makes.")
                  case None =>
                    () // not a Command at all (a Read/Project/Filter/...) - definitely not a write
                }
            }
        }
    }
  }

  /** A statement that removes a table's data (`DestructiveDdlSupport`): allowed, with a PASSED validation, when the
    * contract lets the table be overwritten; refused otherwise. An allowed removal is not a write: a job that drops a
    * declared output and never rebuilds it still ends with `MISSING_OUTPUT` (`DeclaredOutputs`).
    */
  private def checkRemoval(
      contract: Contract,
      ddl: DestructiveDdlSupport.DestructiveDdl,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      job: Option[JobInfo]
  ): Unit = {
    VerificationPipeline.requireValidContract(contract, sink, applicationId, job)
    val contractRef = s"${contract.id}@${contract.version}"
    val described = com.invaract.ir.UnknownPlan(s"${ddl.statement} on '${ddl.table}'")
    DestructiveDdlSupport.judge(contract, ddl) match {
      case DestructiveDdlSupport.Verdict.Allowed =>
        VerificationPipeline.publishValidation(contract, VerificationResult.of(contractRef, Nil), sink, applicationId, job)
      case DestructiveDdlSupport.Verdict.Undeclared(operation) =>
        VerificationPipeline.rejectUnverifiableWrite(contract, operation, described, sink, applicationId, job)
      case DestructiveDdlSupport.Verdict.Refused(violation) =>
        VerificationPipeline.reject(contract, VerificationResult.of(contractRef, List(violation)), described, sink, applicationId, job)
    }
  }

  /** An ALTER TABLE statement that changes a table's columns or location (`SchemaDdlSupport`). New columns are checked
    * as the resulting state of the table (`VerificationPipeline.verifyStateChange`, which lets a table no declared
    * output names through, with a PASSED validation); moving a declared output away is refused; a rename is both: the
    * old name must not be a declared output, and the table is checked as the one the new name now is.
    */
  private def checkTableChange(
      contract: Contract,
      ddl: SchemaDdlSupport.SchemaDdl,
      options: VerificationOptions,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      job: Option[JobInfo]
  ): Unit = {
    def newState(target: TableTargets.Target, schema: org.apache.spark.sql.types.StructType): Unit =
      VerificationPipeline.verifyStateChange(
        contract,
        s"${ddl.statement} on '${target.display}'",
        target.location,
        target.names,
        SparkSchemas.toLogicalSchema(schema),
        SQLConf.get.caseSensitiveAnalysis,
        options,
        sink,
        applicationId,
        job
      )
    def refuseIfDeclared(target: TableTargets.Target, problem: String): Unit =
      SchemaDdlSupport.refusal(contract, ddl.statement, target, problem).foreach { operation =>
        VerificationPipeline.requireValidContract(contract, sink, applicationId, job)
        VerificationPipeline.rejectUnverifiableWrite(
          contract, operation, com.invaract.ir.UnknownPlan(s"${ddl.statement} on '${target.display}'"), sink, applicationId, job
        )
      }
    ddl match {
      case SchemaDdlSupport.NewSchema(_, target, schema) => newState(target, schema)
      case SchemaDdlSupport.Displaced(_, target, problem) => refuseIfDeclared(target, problem)
      case SchemaDdlSupport.RenamedOnto(_, source, target, schema) =>
        refuseIfDeclared(source, SchemaDdlSupport.RenamesAway)
        newState(target, schema)
    }
  }

  /** What a plan too deep to check even on a large stack means: a write (or anything that might be one) is refused, so
    * it is never let through unchecked; any other plan - a `count()`, a `show()` - is let through with a warning, since
    * it changes nothing and a contract has nothing to say about it.
    */
  private[sparkadapter] def tooDeep(
      contract: Contract,
      analyzedPlan: LogicalPlan,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      job: Option[JobInfo]
  ): Unit =
    // Both tests look only at the plan's root, so they cannot overflow themselves.
    if (analyzedPlan.isInstanceOf[Command] || WriteCommandSupport.combined.isDefinedAt(analyzedPlan)) {
      VerificationPipeline.rejectUnverifiableWrite(
        contract,
        DeeplyNestedPlan,
        com.invaract.ir.UnknownPlan("a plan too deeply nested to render", analyzedPlan.getClass.getSimpleName),
        sink,
        applicationId,
        job
      )
    } else if (warnedTooDeep.compareAndSet(false, true)) {
      logger.warn(
        "Invaract: a query's plan is too deeply nested to translate; it writes nothing, so it runs unchecked. " +
          "(Logged once per JVM.)"
      )
    }

  // Once per JVM. Package-visible so a test can reset them and see each message logged exactly once.
  private[sparkadapter] val warnedTooDeep = new java.util.concurrent.atomic.AtomicBoolean(false)
  private[sparkadapter] val loggedForeachBatch = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** The refusal, under `rejectUndeclaredInputs`, of a write that reads a Dataset built from an RDD. */
  private[sparkadapter] val RddBackedSource = com.invaract.verification.UnverifiableOperation(
    SparkPlanAdapter.RddRelationSourceType,
    "this write reads a DataFrame built from an RDD (createDataFrame(rdd), rdd.toDF), and Invaract cannot see what the " +
      "RDD was computed from",
    "Read the data with spark.read or spark.table so its source is visible, or run without rejectUndeclaredInputs if " +
      "inputs outside the contract are acceptable for this job."
  )

  /** The refusal of a write whose plan is too deep to check. */
  private[sparkadapter] val DeeplyNestedPlan = com.invaract.verification.UnverifiableOperation(
    "DeeplyNestedPlan",
    "this write's plan is too deeply nested for Invaract to verify",
    "Shorten the plan before writing: call .checkpoint() or .localCheckpoint() part-way through, or write an " +
      "intermediate table and read it back."
  )

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
  private def checkedWrite(plan: LogicalPlan, translated: TranslationResult, declaredInputs: Seq[String]): CheckedWrite = {
    val writeInfo = WriteCommandSupport.combined.lift(plan)
    CheckedWrite(
      plan = translated.plan,
      inputSchemas = collectInputSchemas(plan, writeInfo.map(_.query), declaredInputs),
      outputSchema = SparkSchemas.toLogicalSchema(writeInfo.map(_.outputSchema).getOrElse(plan.schema)),
      caseSensitive = SQLConf.get.caseSensitiveAnalysis,
      // Classified once; a separate, independent classifier over the same plan
      // (see RowMutationSupport's class doc) - `None` for every write shape that
      // isn't row-level DML.
      rowMutation = RowMutationSupport.classify(plan),
      lineageBoundaryTypes = CheckpointRegistry.BoundarySourceTypes,
      // A resolution that had to pick the most recent of several same-source
      // plans is an assumption the fingerprint cannot reflect; disclosed with it.
      resolutionNotes = translated.diagnostics.filter(_.nodeType == CheckpointRegistry.ResolutionDiagnosticType).map(_.message),
      // the CSV options each CSV read and write used, Spark's defaults filled in (check.csvOptions)
      formatOptions = CsvOptionsSupport.used(plan, writeInfo.map(_.location), plan +: writeInfo.map(_.query).toList, declaredInputs),
      // the format each read was made in (check.inputFormat)
      inputFormats = collectInputFormats(plan, writeInfo.map(_.query), declaredInputs),
      // the partition columns and overwrite mode, where Spark states them (check.partitioning)
      partitioning = writeInfo.flatMap(_.partitioning),
      // what column lineage is traced through when the checked plan is not what the write computes (a MERGE's clauses)
      lineagePlan = writeInfo.flatMap(lineagePlanOf(_, translated.plan, declaredInputs))
    )
  }

  /** The write `translated` is, as lineage sees it: `info.lineageQuery` in place of its own query, and the table it
    * creates stored where `info.createdAt` says. `None` when it has neither, or `translated` is not a write. */
  private[sparkadapter] def lineagePlanOf(info: WriteCommandInfo, translated: com.invaract.ir.Plan, declaredInputs: Seq[String]): Option[com.invaract.ir.Plan] =
    Some(translated).collect { case w: com.invaract.ir.Write if info.lineageQuery.isDefined || info.createdAt.isDefined => w }.map { write =>
      write.copy(
        input = info.lineageQuery.fold(write.input)(SparkPlanAdapter.translate(_, declaredInputs).plan),
        catalog = write.catalog.map(c => c.copy(location = c.location.orElse(info.createdAt)))
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
    // Dry-run never stops a job: a plan too deep to infer from is skipped with a warning (`DeepPlans`).
    DeepPlans.attempt(inferOutcome(analyzedPlan, checkpointRegistry)).fold(
      _ => logger.warn("Invaract dry-run mode: a query's plan is too deeply nested to infer a contract from; skipped."),
      _.foreach {
        case outcome: InferenceOutcome.Inferred => onInferred(outcome.contract)
        // surfaced by DryRunReporter; this callback only ever wanted contracts
        case _: InferenceOutcome.Skipped | _: InferenceOutcome.SkippedRemoval => ()
      }
    )

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

    /** A statement that removes data from the table answering to `removes` (`DestructiveDdlSupport`), reported
      * `SKIPPED_UNSUPPORTED`: a dry run then drafts that output with no `saveMode`, since the job's write mode no longer
      * says what happens to it. (A case class of its own, not a field on `Skipped`, so `Skipped`'s compiled shape is
      * unchanged for MiMa.) */
    final case class SkippedRemoval(reason: String, removes: List[String]) extends InferenceOutcome {
      def status: String = InferenceStatus.SkippedUnsupported
    }
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
    // A draft has no contract to attribute reads to, so each file read is attributed to its own dataset: the folder a
    // file is in, without trailing partition folders (`ReadLocations.datasetRoot`). Reading today's partition drafts
    // the dataset, so the draft still holds tomorrow; the draft's self-check sees the same locations.
    lazy val datasets = draftDatasets(plan)
    lazy val translated = SparkPlanAdapter.translate(plan, datasets)
    checkpointRegistry.foreach(_ => recordCheckpointOrigin(checkpointRegistry, plan, translated.plan))
    WriteCommandSupport.combined.lift(plan) match {
      case Some(writeInfo) =>
        SparkAdapterListener.stash(analyzedPlan, translated)
        val inputSchemas = collectInputSchemas(plan, Some(writeInfo.query), datasets)
        Some(
          InferenceOutcome.Inferred(
            ContractInference.infer(
              InferredWrite(
                writeInfo.location,
                writeInfo.format,
                writeInfo.saveMode,
                SparkSchemas.toLogicalSchema(writeInfo.outputSchema),
                formatOptions = CsvOptionsSupport.forDraft(plan, Some(writeInfo.location), Seq(plan, writeInfo.query), datasets),
                inputFormats = collectInputFormats(plan, Some(writeInfo.query), datasets),
                partitioning = writeInfo.partitioning
              ),
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
          case None if DestructiveDdlSupport.extract(plan).isDefined =>
            val ddl = DestructiveDdlSupport.extract(plan).get
            Some(
              InferenceOutcome.SkippedRemoval(
                s"${ddl.statement} on '${ddl.table}': allowed under enforcement when '${ddl.table}' is a declared output " +
                  "that may be overwritten",
                ddl.target.all
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

  /** The dataset each file read in `plan` belongs to (`ReadLocations.datasetRoot`), each once - what a dry run drafts
    * inputs at. Includes reads in a write's query and in subqueries.
    */
  private[sparkadapter] def draftDatasets(plan: LogicalPlan): List[String] = {
    val queries = plan :: WriteCommandSupport.combined.lift(plan).map(_.query).toList
    queries.flatMap(_.collectWithSubqueries {
      case LogicalRelation(h: org.apache.spark.sql.execution.datasources.HadoopFsRelation, _, _, _) =>
        // compared in canonical form: Spark spells a file `file:/x` in one list and `file:///x` in the other
        lazy val files = scala.util.Try(h.location.inputFiles.map(com.invaract.contract.Locations.canonical).toSet).getOrElse(Set.empty[String])
        h.location.rootPaths.map(p => ReadLocations.datasetRoot(p.toString, files.contains(com.invaract.contract.Locations.canonical(p.toString)))).toList
    }.flatten).distinct
  }

  /** Builds the full explanation `ContractViolationException.getMessage`
    * carries - see `VerificationPipeline.explain`, which owns it (it is the
    * same for every engine); kept here as the spelling this module's own
    * tests use.
    */
  private[sparkadapter] def explain(contract: Contract, plan: com.invaract.ir.Plan, result: VerificationResult): String =
    VerificationPipeline.explain(contract, plan, result)
}
