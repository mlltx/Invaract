// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, OrgPolicy, OrgPolicyEvaluator, OrgPolicyParseException, OrgPolicyParser, OrgPolicyValidator, PolicyViolation}
import com.invaract.ir
import com.invaract.verification.location.{ContractLocationResolution, LocationRef, LocationResolutionException, LocationResolver, StaticMapLocationResolver}
import com.invaract.verification.notification.{JobInfo, NotificationSink}

/** Everything an engine adapter does once, at session/job start, to turn
  * configuration into the contract and `VerificationOptions` it will enforce -
  * independent of the engine, because it reads configuration only through a
  * `ConfigSource`:
  *
  *   1. `resolveContractLocations`: `ref://<id>` locations in the contract,
  *      against a `locationMap` properties file.
  *   2. `resolveVerificationOptions`: the five `Boolean` options, overlaid from
  *      configuration (a flag is on if code *or* configuration turns it on).
  *   3. `enforceOrgPolicy`: organizational policy layers - rejecting a contract
  *      that doesn't satisfy platform-wide rules, injecting required rules, and
  *      raising the verification-option floor.
  *
  * This is what makes each capability attachable purely through the engine's
  * own configuration surface against a job whose source a platform doesn't own
  * (CLAUDE.md's External Attachability Requirement) - an adapter supplies a
  * `ConfigSource` spelling the neutral `InvaractConf` names its own way, and
  * gets all three for free. `spark-adapter`'s `ContractEnforcementRule` is a
  * thin caller of exactly these.
  */
object VerificationSetup {

  /** The steps `config` says the platform trusts (`InvaractConf.TrustedSteps`): what an adapter does not refuse as an
    * unverifiable step that ends the job. */
  def trustedSteps(config: ConfigSource): TrustedSteps = TrustedSteps.from(config)

  /** `ref://<id>` locations in `contract`, resolved from the `.properties` file `locationMap` names. A contract with
    * no `ref://` locations is completely unaffected, set or not; one that does declare a reference fails immediately,
    * naming the setting and the id (and, with a map set, the ids it does map), instead of a confusing downstream
    * `MissingInput`/`OutputLocationMismatch` against the literal `ref://...`.
    */
  def resolveContractLocations(contract: Contract, config: ConfigSource): Contract = {
    val setting = config.describe(InvaractConf.LocationMap)
    val resolver: LocationResolver = config.get(InvaractConf.LocationMap) match {
      case Some(path) =>
        if (!new java.io.File(path).isFile) {
          throw new LocationResolutionException(s"'$setting' names '$path', which is not a file that exists.")
        }
        val map = StaticMapLocationResolver.fromPropertiesFile(path)
        id =>
          if (map.ids.contains(id)) map.resolve(id)
          else {
            val known = if (map.ids.isEmpty) "it maps no ids at all" else "the ids it maps are: " + map.ids.toList.sorted.mkString(", ")
            throw new LocationResolutionException(
              s"The contract declares '${LocationRef.Scheme}$id', but the location map '$path' (named by '$setting') has no line for '$id'; $known."
            )
          }
      case None =>
        id =>
          throw new LocationResolutionException(
            s"The contract declares '${LocationRef.Scheme}$id', but no location map is set: set '$setting' to a .properties file " +
              s"with a line '$id=<real location>'."
          )
    }
    ContractLocationResolution.resolve(contract, resolver)
  }

  /** Overlays the five option flags onto `options` - `||`, not a replacement: a
    * flag ends up `true` if *either* the caller's own `VerificationOptions`
    * already set it, or the matching configuration value is `"true"`, so a
    * platform attaching a stricter check can never be silently weakened by code
    * that left a flag at its default, and code that deliberately opted in can
    * never be silently turned off by a key's mere absence.
    */
  def resolveVerificationOptions(options: VerificationOptions, config: ConfigSource): VerificationOptions = {
    def flag(name: String): Boolean = ConfigSource.flag(config, name)
    options.copy(
      rejectUndeclaredInputs = options.rejectUndeclaredInputs || flag(InvaractConf.RejectUndeclaredInputs),
      rejectUndeclaredFields = options.rejectUndeclaredFields || flag(InvaractConf.RejectUndeclaredFields),
      computeFingerprint = options.computeFingerprint || flag(InvaractConf.ComputeFingerprint),
      staticDataQuality = options.staticDataQuality || flag(InvaractConf.StaticDataQuality),
      roleConsistency = options.roleConsistency || flag(InvaractConf.RoleConsistency)
    )
  }

  /** The full, ordered stack of organizational policy layers configured, each
    * paired with the path it was loaded from (so a later validation failure can
    * name exactly which layer it came from): `orgPolicy`'s document first (the
    * org-wide base), then each comma-separated path in `orgPolicyOverlays`, in
    * the order listed. `Nil` when neither is set - a job with no org policy pays
    * no cost and sees no behavior change.
    *
    * Setting overlays without a base is rejected rather than silently treating
    * the first overlay as the base: layering is additive to a named org-wide
    * anchor, and letting it stand in for a missing one risks a misconfigured job
    * losing org-wide governance entirely rather than failing loudly.
    */
  private[invaract] def resolveOrgPolicyLayers(config: ConfigSource): List[(String, OrgPolicy)] = {
    val overlayPaths = config.get(InvaractConf.OrgPolicyOverlays).toList.flatMap(splitCommaSeparated)
    (config.get(InvaractConf.OrgPolicy), overlayPaths) match {
      case (None, Nil) => Nil
      case (None, _) =>
        throw new OrgPolicyParseException(
          s"'${config.describe(InvaractConf.OrgPolicyOverlays)}' is set (${overlayPaths.mkString(", ")}) but " +
            s"'${config.describe(InvaractConf.OrgPolicy)}' is not - " +
            "policy layering requires an org-wide base policy to layer onto; set both, or neither"
        )
      case (Some(basePath), overlays) => (basePath :: overlays).map(path => path -> OrgPolicyParser.parseFile(path))
    }
  }

  /** Applies the configured organizational policy layers to `contract` and
    * `options`: injects the rules/options they require and, if any `Enforce`
    * policy is violated, rejects the contract itself *before any plan is
    * analyzed* (throwing `ContractViolationException`, after publishing a
    * FAILED event). A `Warn` policy never blocks - it is published so a
    * platform can watch a newly-introduced policy's violations accumulate
    * before flipping it to Enforce. Checked once, eagerly, at setup: a policy
    * rule depends only on the contract's own declared shape, never on what a
    * job writes. Returns the governed contract and options unchanged when no
    * layer is configured.
    */
  def enforceOrgPolicy(
      contract: Contract,
      options: VerificationOptions,
      config: ConfigSource,
      sink: Option[NotificationSink],
      runId: Option[String],
      job: Option[JobInfo] = None
  ): (Contract, VerificationOptions) =
    resolveOrgPolicyLayers(config) match {
      case Nil => (contract, options)
      case layers =>
        val layersValidation = OrgPolicyValidator.validateLayers(layers)
        if (!layersValidation.isValid) {
          throw new OrgPolicyParseException(s"Organizational policy layers are invalid: ${layersValidation.errors.mkString("; ")}")
        }
        layers.foreach { case (path, layer) => requireKnownMinVerificationOptionKeys(layer, path) }

        val policies = layers.map(_._2)
        val governedContract = OrgPolicyEvaluator.applyInjectedRulesFromLayers(contract, policies)
        val governedOptions = policies.foldLeft(options)(applyMinVerificationOptions)
        val evaluation = OrgPolicyEvaluator.evaluateLayers(governedContract, policies)

        if (evaluation.hasBlockingViolations) {
          val violations = evaluation.enforceViolations.map(toViolation(governedContract, _))
          val result = VerificationResult.of(s"${governedContract.id}@${governedContract.version}", violations)
          VerificationPipeline.publishValidation(governedContract, result, sink, runId, job)
          // No parenthesized fragment here: PlanPrinter renders UnknownPlan as
          // "UnknownPlan(<description>)" verbatim - wrapping the description in
          // its own parens too produced a confusing doubled "((...))".
          val describedPlan = ir.UnknownPlan(
            "no transformation plan exists yet - rejected by organizational policy before any plan was analyzed"
          )
          throw new ContractViolationException(result, VerificationPipeline.explain(governedContract, describedPlan, result))
        } else if (evaluation.warnViolations.nonEmpty) {
          // Built directly rather than via VerificationResult.of: that helper
          // infers FAILED from a non-empty violation list, which would
          // misrepresent a Warn-only result as a rejection that never happened.
          val result = VerificationResult(
            "PASSED",
            s"${governedContract.id}@${governedContract.version}",
            evaluation.warnViolations.map(toViolation(governedContract, _))
          )
          VerificationPipeline.publishValidation(governedContract, result, sink, runId, job)
        }

        (governedContract, governedOptions)
    }

  private val KnownMinVerificationOptionKeys =
    Set("rejectUndeclaredInputs", "rejectUndeclaredFields", "computeFingerprint", "staticDataQuality", "roleConsistency")

  /** A policy's `inject.minVerificationOptions` naming an option this engine
    * doesn't know would otherwise be silently ignored - a typo'd floor that
    * enforces nothing - so it is rejected.
    */
  private[invaract] def requireKnownMinVerificationOptionKeys(policy: OrgPolicy, policyPath: String): Unit = {
    val unknownKeys = policy.inject.minVerificationOptions.keySet -- KnownMinVerificationOptionKeys
    if (unknownKeys.nonEmpty) {
      throw new OrgPolicyParseException(
        s"Organizational policy at '$policyPath' declares unrecognized " +
          s"inject.minVerificationOptions key(s): ${unknownKeys.toList.sorted.mkString(", ")} " +
          s"(known keys: ${KnownMinVerificationOptionKeys.toList.sorted.mkString(", ")})"
      )
    }
  }

  /** Raises `options` to `policy`'s floor: a flag the policy names `true` is on
    * whatever `options` said; a flag it doesn't name is left as it was.
    */
  private[invaract] def applyMinVerificationOptions(options: VerificationOptions, policy: OrgPolicy): VerificationOptions = {
    def floor(key: String, current: Boolean): Boolean =
      current || policy.inject.minVerificationOptions.getOrElse(key, false)
    options.copy(
      rejectUndeclaredInputs = floor("rejectUndeclaredInputs", options.rejectUndeclaredInputs),
      rejectUndeclaredFields = floor("rejectUndeclaredFields", options.rejectUndeclaredFields),
      computeFingerprint = floor("computeFingerprint", options.computeFingerprint),
      staticDataQuality = floor("staticDataQuality", options.staticDataQuality),
      roleConsistency = floor("roleConsistency", options.roleConsistency)
    )
  }

  private def toViolation(contract: Contract, violation: PolicyViolation): Violation = {
    val location = violation.dataset.flatMap(name => (contract.inputs ++ contract.outputs).find(_.name == name)).map(_.location)
    Violations.orgPolicy(violation.message, violation.remediation, location, violation.policyId)
  }

  /** Splits a comma-separated configuration value into its trimmed, non-blank entries. */
  private[invaract] def splitCommaSeparated(raw: String): List[String] =
    raw.split(",").map(_.trim).filter(_.nonEmpty).toList
}
