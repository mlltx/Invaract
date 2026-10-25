// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{AttachPlan, AttachSetup, VerificationOptions}
import com.invaract.contract.{Contract, ContractParser}
import com.invaract.verification.notification.NotificationSink

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

/** Installs Invaract with **no code at all** in the job it's attached to —
  * only `spark-submit --conf`:
  *
  * {{{
  * spark-submit \
  *   --conf spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension \
  *   --conf spark.invaract.contract=/path/to/contract.yaml \
  *   --jars invaract-spark-adapter-0.5.0.jar \
  *   my-job.jar
  * }}}
  *
  * This is the same mechanism Delta Lake's own
  * `io.delta.sql.DeltaSparkSessionExtension` is attached with — Spark
  * reflectively instantiates every class named in `spark.sql.extensions`
  * (via a public no-arg constructor; see `SparkSession$.applyExtensions`)
  * and applies it to `SparkSessionExtensions` while a session is being
  * built. See CLAUDE.md's "External Attachability Requirement" and
  * ARCHITECTURE.md's ADR-008: this class is what finally closes the one
  * standing exception those describe — until now, even `forContract`
  * itself needed one line of a job's own source to install.
  *
  * A no-arg constructor means `apply` below never has a `SparkSession` to
  * read configuration from directly — `SparkSessionExtensions.injectCheckRule`
  * is the escape hatch, the same one `ContractEnforcementRule.forContract`
  * already returns a value shaped for: Spark invokes the registered
  * `SparkSession => LogicalPlan => Unit` once a real session using these
  * extensions is being built, with that real session in hand. `checkRuleFor`
  * below is exactly that function — everything it needs (the contract
  * path, dry-run mode, notification config) is read from `session.conf`
  * at that point, the same moment and mechanism `forContract`'s own
  * `resolveContractLocations`/`resolveVerificationOptions` already use.
  * Once `checkRuleFor` builds the real `ContractEnforcementRule.forContract`/
  * `.dryRun` check rule, every conf key those already understand
  * (`spark.invaract.locationMap`, `.rejectUndeclaredInputs`, `.rejectUndeclaredFields`,
  * `.computeFingerprint`) composes for free — this class adds no
  * duplicate handling for any of them.
  */
class InvaractSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectCheckRule(InvaractSparkSessionExtension.checkRuleFor)
}

object InvaractSparkSessionExtension {
  private val logger = LoggerFactory.getLogger(classOf[InvaractSparkSessionExtension])

  /** Path to the contract YAML to enforce, OR a `registry://<id>@<version>`
    * reference resolved via `registry.ContractSource` (see
    * docs/CONTRACT_REGISTRY.md §7 and `ContractSource`'s own doc) —
    * recognized here the same way `ref://` is recognized inside a
    * `Dataset.location`. Required unless `DryRunConfKey` is `"true"` —
    * dry-run mode has no contract to enforce at all (see
    * `ContractEnforcementRule.dryRun`'s own doc).
    */
  val ContractConfKey = "spark.invaract.contract"

  /** Runs dry-run mode instead of real enforcement when set to `"true"` —
    * see [[ContractEnforcementRule.dryRun]]. `ContractConfKey` is ignored
    * entirely in this mode, not merely left unvalidated, matching
    * `DemoJobHarness`'s own `--dry-run` handling.
    */
  val DryRunConfKey = "spark.invaract.dryRun"

  /** Path to a notification sink `.properties` file (the same shape
    * `NotificationConfig.load` reads — see docs-site's "Configure a
    * Notification Sink" guide). When set and the configured sink builds
    * successfully, this class both passes the sink to `forContract` (for
    * `ContractValidationEvent`s) and registers a `SparkAdapterListener` on
    * the same session (for `WriteEvent`s) — the same two-call wiring a
    * job's own code would otherwise have to perform by hand.
    */
  val NotifyConfigConfKey = "spark.invaract.notifyConfig"

  /** In dry-run mode, `NotifyConfigConfKey` takes on a second meaning: the sink receives a
    * `ContractInferenceEvent` per write and a `DryRunSummaryEvent` at application end
    * instead of the enforcement-mode events (see [[DryRunReporter]]). These two keys
    * describe the job in those events: `JobIdConfKey` is its stable identity across runs
    * and every `JobMetadataConfPrefix` entry rides along as an attribute.
    */
  val JobIdConfKey: String = DryRunReporter.JobIdConfKey
  val JobMetadataConfPrefix: String = DryRunReporter.JobMetadataConfPrefix

  /** Also in dry-run mode: `spark.invaract.contract.metadata.<key>=<value>` is attached to every
    * inferred draft's `extensions` (and echoed as each event's `metadata`), so a draft arrives
    * already tagged with what it is and who owns it, and - because enforcement copies a contract's
    * `extensions` into its own events - the same keys later link those events back to the dry run.
    * `JobMetadataConfPrefix` entries, by contrast, describe one run and stay out of the contract.
    */
  val ContractMetadataConfPrefix: String = DryRunReporter.ContractMetadataConfPrefix

  private[sparkadapter] def checkRuleFor: SparkSession => LogicalPlan => Unit =
    session =>
      // What to attach (dry-run or a contract, and which sink) is decided from configuration alone, by
      // verification-core's AttachSetup; installing it on this session is the only Spark-specific part.
      AttachSetup.select(SparkConfigSource(session)) match {
        case AttachPlan.DryRun(Some(sink)) =>
          // A configured sink turns dry-run into reporting mode (DryRunReporter): every write-shaped plan,
          // inferred or not, is published, plus one summary at application end.
          val reporter = DryRunReporter.installFor(session, sink)
          (plan: LogicalPlan) => reporter.check(plan)
        case AttachPlan.DryRun(None) =>
          // With no sink it stays the log-only behavior it always was.
          ContractEnforcementRule.dryRun(logInferredContract)(session)
        case AttachPlan.Enforce(contract, Some(sink)) =>
          session.listenerManager.register(new SparkAdapterListener(Some(sink), Some(contract)))
          ContractEnforcementRule.forContract(contract, VerificationOptions(), sink)(session)
        case AttachPlan.Enforce(contract, None) =>
          ContractEnforcementRule.forContract(contract)(session)
      }

  /** The sink dry-run mode should report to, or `None` for the log-only behavior.
    *
    * Unlike enforcement mode - where a `notifyConfig` that cannot be loaded is a real
    * misconfiguration worth failing the job over - dry-run's whole promise is that it
    * is safe to attach to a job nobody here owns. A missing file, an unreadable one, or
    * a typo'd `sink.class` therefore costs one WARN and a fallback to logging the
    * inferred contract, never the job itself. (Enforcement mode keeps failing loudly.)
    */
  private[sparkadapter] def dryRunSink(session: SparkSession): Option[NotificationSink] =
    AttachSetup.lenientSink(SparkConfigSource(session))

  /** The default `onInferred` callback for conf-driven dry-run mode — no
    * job code exists here to hand an inferred `Contract` to, so this logs
    * it at `WARN` (loud enough that it won't be missed alongside real
    * enforcement's own warnings, e.g. `VersionCompatibilityGuard`'s), the
    * same information `DemoJobHarness`'s own console output prints when
    * `--dry-run` is run by hand.
    */
  private def logInferredContract(contract: Contract): Unit =
    logger.warn(
      "Invaract dry-run mode (spark.invaract.dryRun=true): inferred contract from this " +
        "session's real inputs/outputs - copy it into a file, review it, and set " +
        s"$ContractConfKey to it to switch to enforced mode:\n${ContractParser.write(contract)}"
    )
}
