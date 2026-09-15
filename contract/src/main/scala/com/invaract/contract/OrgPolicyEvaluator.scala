// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.time.LocalDate

/** One policy rule's violation against one dataset, or against the
  * contract as a whole for a `ContractPolicy` type like `RequireExtension`
  * with no single offending dataset — `dataset` is `None` in that case.
  * `policyId`/`ruleType` let a caller
  * attribute the failure to a specific, named policy rule; `mode` is
  * `rule.mode` at evaluation time, so a caller can decide what "Enforce" vs.
  * "Warn" means for it without re-consulting the originating `PolicyRule`.
  */
case class PolicyViolation(
  policyId: String,
  ruleType: String,
  mode: PolicyMode,
  dataset: Option[String],
  message: String,
  remediation: String
)

/** The result of evaluating an `OrgPolicy` against one `Contract`, already
  * split by `PolicyMode` — `enforceViolations` is what a caller should treat
  * as fatal (see `hasBlockingViolations`); `warnViolations` is everything
  * else, informational only.
  */
case class OrgPolicyEvaluation(enforceViolations: List[PolicyViolation], warnViolations: List[PolicyViolation]) {
  def hasBlockingViolations: Boolean = enforceViolations.nonEmpty
  def allViolations: List[PolicyViolation] = enforceViolations ++ warnViolations
}

/** Evaluates a `Contract` against an `OrgPolicy` — pure, no Spark, the same
  * engine-independence `ContractValidator`/`ContractCompatibility` have.
  * Safe to call on a policy that hasn't been run through
  * `OrgPolicyValidator` first (an unrecognized or malformed policy type
  * simply produces no violation, via `PolicyRule.interpret`'s own
  * total/safe design) — though a caller building a real enforcement path
  * should still validate first, so a malformed policy surfaces as a clear
  * error rather than a silent no-op. See docs/CONTRACT_MODEL.md's
  * "Organizational Policy" section for the full design.
  */
object OrgPolicyEvaluator {

  /** Evaluates every policy rule in `policy` against `contract`, skipping
    * any rule an unexpired `PolicyExemption` covers for `contract.id`, and
    * splits the resulting violations by `PolicyMode`.
    */
  def evaluate(contract: Contract, policy: OrgPolicy, now: LocalDate = LocalDate.now()): OrgPolicyEvaluation = {
    val violations = policy.policies.flatMap(rule => evaluateRule(contract, rule, policy.exemptions, now))
    val (enforceViolations, warnViolations) = violations.partition(_.mode == PolicyMode.Enforce)
    OrgPolicyEvaluation(enforceViolations, warnViolations)
  }

  /** Merges `policy.inject.rules` into `contract.rules` — rules a governed
    * contract's author never has to declare themselves for `RuleVerifier`
    * to enforce them (e.g. an org-wide `forbid_unconditional_delete`).
    * Rules already present on `contract` (by `ruleType` + `properties`
    * equality) are not duplicated. A no-op, returning `contract` unchanged,
    * when `policy.inject.rules` is empty — so a policy with no injected
    * rules has zero effect on `contract.rules`, not an empty-but-different
    * list.
    */
  def applyInjectedRules(contract: Contract, policy: OrgPolicy): Contract =
    if (policy.inject.rules.isEmpty) contract
    else contract.copy(rules = contract.rules ++ policy.inject.rules.filterNot(contract.rules.contains))

  /** Every exemption in `policy` whose `reviewBy` falls within the next
    * `withinDays` days of `now` — still active (an already-expired one is
    * `OrgPolicyValidator`'s Warning to report, a different, past-tense
    * concern), but due to lapse soon. An exemption with no `reviewBy` at
    * all never appears here, since it never expires. Ordered by `reviewBy`
    * ascending, so the soonest-to-lapse exemption is always first — the
    * one a platform team most needs to act on.
    *
    * Meant for a proactive look-ahead (e.g. `OrgPolicyLintCli`'s
    * `--warn-expiring-within-days`), run *before* an exemption becomes
    * `OrgPolicyValidator`'s after-the-fact Warning — the difference
    * between a platform team choosing to renew or retire an exemption on
    * their own schedule, and finding out only once a job has already
    * started failing.
    */
  def expiringExemptions(policy: OrgPolicy, withinDays: Int, now: LocalDate = LocalDate.now()): List[PolicyExemption] = {
    val horizon = now.plusDays(withinDays.toLong)
    policy.exemptions
      .filter(_.reviewBy.exists(d => !d.isBefore(now) && !d.isAfter(horizon)))
      .sortBy(_.reviewBy.get.toEpochDay)
  }

  private def evaluateRule(
      contract: Contract,
      rule: PolicyRule,
      exemptions: List[PolicyExemption],
      now: LocalDate
  ): List[PolicyViolation] = {
    if (exemptions.exists(_.covers(contract.id, rule.id, now))) Nil
    else
      rule.interpret match {
        case None => Nil // unrecognized or malformed policy type - OrgPolicyValidator's job to flag this, not ours to crash on
        case Some(InterpretedPolicy.RequireExtension(key, value)) =>
          checkRequireExtension(rule, contract, key, value)
        case Some(datasetPolicy: InterpretedPolicy.DatasetPolicy) =>
          val datasets = datasetsInScope(contract, rule.scope).filter(matchesCondition(_, rule.when))
          datasetPolicy match {
            case InterpretedPolicy.RequireCatalog(technology) =>
              datasets.flatMap(checkRequireCatalog(rule, _, technology))
            case InterpretedPolicy.RequireField(name, fieldType) =>
              datasets.flatMap(checkRequireField(rule, _, name, fieldType))
            case InterpretedPolicy.FieldNamingConvention(pattern) =>
              datasets.flatMap(checkFieldNamingConvention(rule, _, pattern))
            case InterpretedPolicy.RequireFormat(formats) =>
              datasets.flatMap(checkRequireFormat(rule, _, formats))
            case InterpretedPolicy.RequireDatasetDescription =>
              datasets.flatMap(checkRequireDatasetDescription(rule, _))
          }
      }
  }

  private def datasetsInScope(contract: Contract, scope: PolicyScope): List[Dataset] = scope match {
    case PolicyScope.Inputs  => contract.inputs
    case PolicyScope.Outputs => contract.outputs
    case PolicyScope.All     => contract.inputs ++ contract.outputs
  }

  private def matchesCondition(dataset: Dataset, when: Option[PolicyCondition]): Boolean = when match {
    case None                       => true
    case Some(PolicyCondition(tag)) => hasSensitivityTag(dataset.schema.fields, tag)
  }

  private def hasSensitivityTag(fields: List[Field], tag: String): Boolean =
    fields.exists(f => f.sensitivityTags.contains(tag) || hasSensitivityTag(f.properties, tag))

  private def checkRequireCatalog(rule: PolicyRule, dataset: Dataset, technology: Option[String]): List[PolicyViolation] = {
    val satisfies = dataset.catalog.exists(c => c.required && technology.forall(c.technology.contains))
    if (satisfies) Nil
    else {
      val technologySuffix = technology.map(t => s" with technology '$t'").getOrElse("")
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          Some(dataset.name),
          s"organizational policy '${rule.id}'${describe(rule)} requires dataset '${dataset.name}' to declare " +
            s"catalog.required=true$technologySuffix, but it does not.",
          s"Add a 'catalog: {required: true${technology.map(t => s", technology: $t").getOrElse("")}}' block to " +
            s"dataset '${dataset.name}' in the contract."
        )
      )
    }
  }

  private def checkRequireField(rule: PolicyRule, dataset: Dataset, name: String, fieldType: Option[String]): List[PolicyViolation] = {
    val found = dataset.schema.field(name)
    val satisfies = found.exists(f => fieldType.forall(_.equalsIgnoreCase(f.fieldType)))
    if (satisfies) Nil
    else {
      val typeSuffix = fieldType.map(t => s" of type '$t'").getOrElse("")
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          Some(dataset.name),
          s"organizational policy '${rule.id}'${describe(rule)} requires dataset '${dataset.name}' to declare a " +
            s"field named '$name'$typeSuffix, but it does not.",
          s"Add a '$name' field$typeSuffix to dataset '${dataset.name}''s schema."
        )
      )
    }
  }

  private def checkFieldNamingConvention(rule: PolicyRule, dataset: Dataset, pattern: String): List[PolicyViolation] = {
    val compiled = java.util.regex.Pattern.compile(pattern)
    def collectNonConforming(fields: List[Field]): List[String] =
      fields.flatMap { f =>
        val self = if (compiled.matcher(f.name).matches()) Nil else List(f.name)
        self ++ collectNonConforming(f.properties)
      }
    val nonConforming = collectNonConforming(dataset.schema.fields)
    if (nonConforming.isEmpty) Nil
    else {
      val plural = nonConforming.size > 1
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          Some(dataset.name),
          s"organizational policy '${rule.id}'${describe(rule)} requires every field name in dataset " +
            s"'${dataset.name}' to match '$pattern', but ${nonConforming.mkString(", ")} " +
            s"${if (plural) "do" else "does"} not.",
          s"Rename ${if (plural) "these fields" else "this field"} to match the pattern '$pattern', or adjust " +
            s"the policy (e.g. an exemption) if this is intentional."
        )
      )
    }
  }

  private def checkRequireFormat(rule: PolicyRule, dataset: Dataset, formats: List[String]): List[PolicyViolation] = {
    val satisfies = dataset.format.exists(f => formats.exists(_.equalsIgnoreCase(f)))
    if (satisfies) Nil
    else {
      val allowedList = formats.mkString(", ")
      val actualSuffix = dataset.format.map(f => s" (currently '$f')").getOrElse(" (no format declared at all)")
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          Some(dataset.name),
          s"organizational policy '${rule.id}'${describe(rule)} requires dataset '${dataset.name}' to declare " +
            s"a format of one of [$allowedList], but it does not$actualSuffix.",
          s"Set dataset '${dataset.name}''s format to one of: $allowedList."
        )
      )
    }
  }

  private def checkRequireDatasetDescription(rule: PolicyRule, dataset: Dataset): List[PolicyViolation] = {
    val satisfies = dataset.description.exists(_.trim.nonEmpty)
    if (satisfies) Nil
    else {
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          Some(dataset.name),
          s"organizational policy '${rule.id}'${describe(rule)} requires dataset '${dataset.name}' to declare a " +
            s"non-blank description, but it does not.",
          s"Add a 'description' to dataset '${dataset.name}' explaining what it is/contains."
        )
      )
    }
  }

  /** Checked once against `contract` as a whole - see
    * `InterpretedPolicy.ContractPolicy`'s doc for why this doesn't go
    * through `datasetsInScope` the way the three dataset-level checks do.
    * A key present but mapped to YAML `null` (`extensions: { owner: }`)
    * is treated the same as the key being absent entirely - present-but-
    * blank isn't a real declaration.
    */
  private def checkRequireExtension(rule: PolicyRule, contract: Contract, key: String, value: Option[String]): List[PolicyViolation] = {
    val actual = contract.extensions.get(key).filter(_ != null)
    val satisfies = actual.exists(v => value.forall(pinned => String.valueOf(v) == pinned))
    if (satisfies) Nil
    else {
      val valueSuffix = value.map(v => s" with value '$v'").getOrElse("")
      val actualSuffix = actual.map(v => s" (currently '$v')").getOrElse("")
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          dataset = None,
          s"organizational policy '${rule.id}'${describe(rule)} requires contract '${contract.id}' to declare " +
            s"extensions.$key$valueSuffix, but it does not$actualSuffix.",
          s"Add 'extensions: { $key: ${value.getOrElse("<value>")} }' to the contract."
        )
      )
    }
  }

  private def describe(rule: PolicyRule): String = rule.description.map(d => s" ($d)").getOrElse("")
}
