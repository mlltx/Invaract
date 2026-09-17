// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractValidator, OrgPolicy, OrgPolicyEvaluator, OrgPolicyParser, OrgPolicyValidator, PolicyViolation}
import com.invaract.fingerprint.{TransformationFingerprint, TransformationFingerprinter}
import com.invaract.ir.PlanPrinter
import com.invaract.sparkadapter.location.{ContractLocationResolution, LocationResolver, NoOpLocationResolver, StaticMapLocationResolver}
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
    *
    * Resolves any `ref://<id>` location `contract` declares (see
    * `com.invaract.sparkadapter.location`) before the first check runs -
    * see `resolveContractLocations`'s doc for why this is what makes the
    * feature attachable purely via `spark-submit --conf`, with no change
    * to the caller's own code.
    */
  def forContract(contract: Contract, options: VerificationOptions = VerificationOptions()): SparkSession => LogicalPlan => Unit =
    session => {
      VersionCompatibilityGuard.check(session)
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, None, None)
      (plan: LogicalPlan) => verifyOrThrow(governedContract, plan, governedOptions, None)
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
      val resolvedContract = resolveContractLocations(contract, session)
      val resolvedOptions = resolveVerificationOptions(options, session)
      val applicationId = Some(session.sparkContext.applicationId)
      val (governedContract, governedOptions) = enforceOrgPolicy(resolvedContract, resolvedOptions, session, Some(sink), applicationId)
      (plan: LogicalPlan) => verifyOrThrow(governedContract, plan, governedOptions, Some(sink), applicationId)
    }

  /** Spark configuration key naming an `id=location` `.properties` file
    * (the same shape `StaticMapLocationResolver.fromPropertiesFile` reads)
    * to resolve `contract`'s `ref://<id>` locations against - see
    * `com.invaract.sparkadapter.location`'s package for the syntax.
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
  private[sparkadapter] def resolveContractLocations(contract: Contract, session: SparkSession): Contract = {
    val resolver: LocationResolver = session.conf.getOption(LocationMapConfKey) match {
      case Some(path) => StaticMapLocationResolver.fromPropertiesFile(path)
      case None       => NoOpLocationResolver
    }
    ContractLocationResolution.resolve(contract, resolver)
  }

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

  /** Overlays the three conf keys above onto `options` — `||`, not a
    * replacement: a flag ends up `true` if *either* the caller's own
    * `VerificationOptions` already set it, or the matching conf key is
    * `"true"`, so a platform attaching a stricter check via `--conf` can
    * never be silently weakened by code that left a flag at its default,
    * and code that deliberately opted in can never be silently turned off
    * by a conf key's mere absence.
    */
  private[sparkadapter] def resolveVerificationOptions(options: VerificationOptions, session: SparkSession): VerificationOptions = {
    def confFlag(key: String): Boolean = session.conf.getOption(key).exists(_.toBoolean)
    options.copy(
      rejectUndeclaredInputs = options.rejectUndeclaredInputs || confFlag(RejectUndeclaredInputsConfKey),
      rejectUndeclaredFields = options.rejectUndeclaredFields || confFlag(RejectUndeclaredFieldsConfKey),
      computeFingerprint = options.computeFingerprint || confFlag(ComputeFingerprintConfKey)
    )
  }

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
  private[sparkadapter] def resolveOrgPolicyLayers(session: SparkSession): List[(String, OrgPolicy)] = {
    val overlayPaths = session.conf.getOption(OrgPolicyOverlaysConfKey).toList.flatMap(VersionCompatibilityGuard.splitCommaSeparated)
    (session.conf.getOption(OrgPolicyConfKey), overlayPaths) match {
      case (None, Nil) => Nil
      case (None, _) =>
        throw new com.invaract.contract.OrgPolicyParseException(
          s"'$OrgPolicyOverlaysConfKey' is set (${overlayPaths.mkString(", ")}) but '$OrgPolicyConfKey' is not - " +
            "policy layering requires an org-wide base policy to layer onto; set both, or neither"
        )
      case (Some(basePath), overlays) => (basePath :: overlays).map(path => path -> OrgPolicyParser.parseFile(path))
    }
  }

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
      applicationId: Option[String]
  ): (Contract, VerificationOptions) =
    resolveOrgPolicyLayers(session) match {
      case Nil => (contract, options)
      case layers =>
        val layersValidation = OrgPolicyValidator.validateLayers(layers)
        if (!layersValidation.isValid) {
          throw new com.invaract.contract.OrgPolicyParseException(
            s"Organizational policy layers are invalid: ${layersValidation.errors.mkString("; ")}"
          )
        }
        layers.foreach { case (path, layer) => requireKnownMinVerificationOptionKeys(layer, path) }

        val policies = layers.map(_._2)
        val governedContract = OrgPolicyEvaluator.applyInjectedRulesFromLayers(contract, policies)
        val governedOptions = policies.foldLeft(options)(applyMinVerificationOptions)
        val evaluation = OrgPolicyEvaluator.evaluateLayers(governedContract, policies)

        if (evaluation.hasBlockingViolations) {
          val violations = evaluation.enforceViolations.map(toViolation)
          val result = VerificationResult.of(s"${governedContract.id}@${governedContract.version}", violations)
          publishValidation(governedContract, result, sink, applicationId)
          // No parenthesized fragment here: PlanPrinter renders UnknownPlan
          // as "UnknownPlan(<description>)" verbatim - wrapping the
          // description in its own parens too (an earlier version of this
          // message did) produced a confusing doubled "((...))" that read
          // like a rendering bug on first encounter, rather than the true,
          // simple fact that no plan exists yet to show.
          val describedPlan = com.invaract.ir.UnknownPlan(
            "no transformation plan exists yet - rejected by organizational policy before any plan was analyzed"
          )
          throw new ContractViolationException(result, explain(governedContract, describedPlan, result))
        } else if (evaluation.warnViolations.nonEmpty) {
          // Never blocks - published (if a sink is configured) so a
          // platform can watch a newly-introduced policy's violations
          // accumulate before flipping it to Enforce. Built directly
          // rather than via VerificationResult.of: that helper infers
          // FAILED from a non-empty violation list, which would
          // misrepresent a Warn-only result as a rejection that never
          // actually happened.
          val result = VerificationResult(
            "PASSED",
            s"${governedContract.id}@${governedContract.version}",
            evaluation.warnViolations.map(toViolation)
          )
          publishValidation(governedContract, result, sink, applicationId)
        }

        (governedContract, governedOptions)
    }

  /** The only `inject.minVerificationOptions` keys `applyMinVerificationOptions`
    * actually reads — `VerificationOptions`'s three flag names. Kept as its
    * own named set (rather than inlined) so `requireKnownMinVerificationOptionKeys`
    * can validate against exactly the same list `applyMinVerificationOptions`
    * consults, with no risk of the two drifting apart.
    */
  private val KnownMinVerificationOptionKeys = Set("rejectUndeclaredInputs", "rejectUndeclaredFields", "computeFingerprint")

  /** Fails loudly on a `policy.inject.minVerificationOptions` key outside
    * `KnownMinVerificationOptionKeys` — a typo (e.g.
    * `rejectUndeclredFields`) would otherwise be silently ignored by
    * `applyMinVerificationOptions`'s plain `getOrElse(key, false)` lookup,
    * leaving a platform team believing a flag is enforced org-wide when it
    * genuinely isn't. `contract` itself can't run this check (it has no
    * `VerificationOptions` to validate against), so it lives here, next to
    * the one place that actually knows the real flag names. `policyPath`
    * names the specific layer this `policy` was loaded from, so a typo in a
    * business-unit overlay is attributed to that overlay's own path, not
    * misleadingly blamed on the org-wide base.
    */
  private[sparkadapter] def requireKnownMinVerificationOptionKeys(policy: OrgPolicy, policyPath: String): Unit = {
    val unknownKeys = policy.inject.minVerificationOptions.keySet -- KnownMinVerificationOptionKeys
    if (unknownKeys.nonEmpty) {
      throw new com.invaract.contract.OrgPolicyParseException(
        s"Organizational policy at '$policyPath' declares unrecognized " +
          s"inject.minVerificationOptions key(s): ${unknownKeys.toList.sorted.mkString(", ")} " +
          s"(known keys: ${KnownMinVerificationOptionKeys.toList.sorted.mkString(", ")})"
      )
    }
  }

  /** ORs `policy.inject.minVerificationOptions` floors onto `options` — a
    * flag a job's own `VerificationOptions` left `false` can still be forced
    * `true` by policy; the reverse never happens (a job can't use policy to
    * weaken a flag it already opted into). Keyed by option name, since
    * `InjectedDefaults.minVerificationOptions` is a plain `Map[String,
    * Boolean]` (`contract` cannot depend on this Spark-specific type). Safe
    * to call with an unrecognized key still present (an unknown key's value
    * is simply never consulted) — `enforceOrgPolicy` calls
    * `requireKnownMinVerificationOptionKeys` first specifically so that
    * case never reaches here silently.
    */
  private[sparkadapter] def applyMinVerificationOptions(options: VerificationOptions, policy: OrgPolicy): VerificationOptions = {
    def floor(key: String, current: Boolean): Boolean =
      current || policy.inject.minVerificationOptions.getOrElse(key, false)
    options.copy(
      rejectUndeclaredInputs = floor("rejectUndeclaredInputs", options.rejectUndeclaredInputs),
      rejectUndeclaredFields = floor("rejectUndeclaredFields", options.rejectUndeclaredFields),
      computeFingerprint = floor("computeFingerprint", options.computeFingerprint)
    )
  }

  /** Adapts a `com.invaract.contract.PolicyViolation` into this module's own
    * `Violation` shape, so an org-policy rejection flows through the exact
    * same `explain`/notification-sink/`demo/output/report.json` path a
    * structural violation already does. `column`/`location` are left unset,
    * the same convention whole-contract-level violations like
    * `ViolationType.InvalidContract` already use — the offending dataset (if
    * any) is named in `message`/`remediation` themselves, via
    * `PolicyViolation.dataset`.
    */
  private def toViolation(violation: PolicyViolation): Violation =
    Violation(ViolationType.OrgPolicyViolation, violation.message, violation.remediation)

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
        // RuleVerifier.anyRuleAppliesTo decides that (built-in or custom
        // rule types alike), so an UPDATE this module can't fully verify
        // doesn't spuriously fail a contract that only declares
        // forbid_unconditional_delete, say.
        // Classified once and reused below by both ruleViolations and
        // fingerprinting - RowMutationSupport.classify re-derives the same
        // RowMutation from the same `plan` either way, so computing it
        // twice would be pure waste (and, worse, a second place that could
        // silently drift from the first).
        val rowMutationClassification = RowMutationSupport.classify(plan)
        val ruleViolations = rowMutationClassification match {
          case Some(RowMutationSupport.Classification.Extracted(_, mutation)) =>
            RuleVerifier.verify(contract.rules, mutation, contract.customRuleTypes)
          case Some(RowMutationSupport.Classification.Unverifiable(kind)) =>
            if (RuleVerifier.anyRuleAppliesTo(contract.rules, kind, contract.customRuleTypes)) List(unverifiableDmlViolation(kind)) else Nil
          case None => Nil
        }
        // See docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.2: this is the
        // one branch with a real, complete ir.Plan already in hand
        // (`translated.plan`, produced above for structural verification
        // itself) - the state-changing-CALL and invalid-contract branches
        // below have no equivalent real plan to fingerprint, so they never
        // populate this field, flag on or not.
        //
        // The RowMutation (if any) feeds the fingerprint too - a MERGE's ON
        // condition, or a conditional DELETE's predicate, is real
        // transformation-defining behavior that ir.Plan alone never
        // captures (see Canonicalizer.canonicalizeRowMutation's own doc);
        // only the Extracted case has an actual RowMutation value to pass -
        // Unverifiable/None both mean "no RowMutation to fold in," not
        // "known to be absent," so the fingerprint in that case still just
        // reflects translated.plan alone, exactly as before RowMutation
        // support existed.
        val fingerprints =
          if (options.computeFingerprint) {
            val mutation = rowMutationClassification.collect {
              case RowMutationSupport.Classification.Extracted(_, m) => m
            }
            Some(TransformationFingerprinter.fingerprint(translated.plan, mutation))
          } else None
        val result = VerificationResult.of(structuralResult.contract, structuralResult.violations ++ ruleViolations, fingerprints)
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
    // ContractValidator only checks customRuleTypes's shape (empty key/class
    // name, collision with a built-in RuleType) - it lives in `contract`,
    // which can't depend on CustomRuleVerifier (a spark-adapter-only trait,
    // per the contract -> spark-adapter dependency direction), so it can
    // never actually resolve a named class. Resolving each entry here, once
    // per write, fails the same way an unresolvable NotificationSink class
    // or CustomPolicyEvaluator class does: loudly, at validation time,
    // before any rule check runs - rather than RuleVerifier silently
    // treating every rule naming that class as inapplicable.
    val unresolvableCustomRuleTypes = contract.customRuleTypes.toList.flatMap { case (ruleType, className) =>
      CustomRuleVerifierFactory.tryResolve(className).failed.toOption.map(e => (ruleType, className, e.getMessage))
    }
    if (!validation.isValid || unresolvableCustomRuleTypes.nonEmpty) {
      val contractRef = s"${contract.id}@${contract.version}"
      val validatorViolations = validation.errors.map { issue =>
        Violation(
          ViolationType.InvalidContract,
          s"contract '$contractRef' is invalid at '${issue.path}': ${issue.message}",
          remediation = s"Fix the contract document (see the '${issue.path}' issue above) so it passes " +
            "ContractValidator.validate before it's used to verify any write."
        )
      }
      val customRuleTypeViolations = unresolvableCustomRuleTypes.map { case (ruleType, className, message) =>
        Violation(
          ViolationType.InvalidContract,
          s"contract '$contractRef' declares customRuleTypes['$ruleType'] = '$className', which could not be resolved: $message",
          remediation = s"Fix or remove the customRuleTypes['$ruleType'] entry so '$className' names a class on the " +
            "classpath implementing CustomRuleVerifier with a public no-arg constructor."
        )
      }
      val result = VerificationResult.of(contractRef, validatorViolations ++ customRuleTypeViolations)
      publishValidation(contract, result, sink, applicationId)
      // See enforceOrgPolicy's identical describedPlan for why this reads
      // as a plain sentence rather than a parenthesized fragment: PlanPrinter
      // already wraps it as "UnknownPlan(<description>)", so an inner
      // "(...)" too would render as a confusing doubled "((...))".
      val describedPlan =
        com.invaract.ir.UnknownPlan("no transformation plan exists yet - contract validation failed before any plan was checked")
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
          applicationId = applicationId,
          fingerprints = result.fingerprints
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

    // Only present when VerificationOptions.computeFingerprint was true
    // for this check and a real plan existed to fingerprint - see
    // docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.4. Only each output's
    // combined hash is printed, not its separate expression/lineage
    // components (still available on `result.fingerprints` directly) -
    // and this never claims "changed"/"unchanged": there is no prior
    // fingerprint here to compare against, only this check's own values.
    result.fingerprints.foreach(appendFingerprints(sb, _))

    sb.toString()
  }

  private def appendFingerprints(sb: StringBuilder, fingerprints: TransformationFingerprint): Unit = {
    sb.append(s"\nFingerprints (v${fingerprints.version}, ${fingerprints.overall.algorithm}):\n")
    sb.append(s"  overall: ${fingerprints.overall.value}\n")
    if (fingerprints.outputs.nonEmpty) {
      sb.append("  outputs:\n")
      fingerprints.outputs.toList.sortBy(_._1).foreach { case (name, output) =>
        sb.append(s"    $name: ${output.combined.value}\n")
      }
    }
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
