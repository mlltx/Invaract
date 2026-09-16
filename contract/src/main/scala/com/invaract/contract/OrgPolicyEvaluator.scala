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
    * splits the resulting violations by `PolicyMode`. Every rule is
    * evaluated through the same `CustomPolicyEvaluator` interface —
    * `resolveEvaluator` picks a built-in `PolicyType`'s evaluator
    * (`builtinEvaluators`) first, falling back to `policy.customPolicyTypes`
    * only for a `ruleType` outside that set — so exemption coverage and
    * mode-splitting apply identically to a built-in and a custom type's
    * violations alike, since both happen here, before dispatch.
    */
  def evaluate(contract: Contract, policy: OrgPolicy, now: LocalDate = LocalDate.now()): OrgPolicyEvaluation = {
    val violations =
      policy.policies.flatMap(rule => evaluateRule(contract, rule, policy.exemptions, policy.customPolicyTypes, now))
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
      customPolicyTypes: Map[String, String],
      now: LocalDate
  ): List[PolicyViolation] =
    if (exemptions.exists(_.covers(contract.id, rule.id, now))) Nil
    else resolveEvaluator(rule.ruleType, customPolicyTypes).map(_.evaluate(contract, rule)).getOrElse(Nil)

  /** Looks up the `CustomPolicyEvaluator` that evaluates `ruleType` — a
    * built-in `PolicyType` (`builtinEvaluators`, below) always wins when
    * present; `customPolicyTypes`'s reflective escape hatch is consulted
    * only when `ruleType` matches neither. This one-directional fallback
    * is why a `customPolicyTypes` entry colliding with a built-in
    * `ruleType` is dead — the built-in lookup succeeds first, so the
    * `customPolicyTypes` entry for it is never even reached (see
    * `OrgPolicyValidator`'s matching Warning). Deliberately total, never
    * throws: neither a `ruleType` matching neither set nor a
    * `customPolicyTypes` entry naming an unresolvable class is ours to
    * crash a real job over — `OrgPolicyValidator`'s job to flag either.
    */
  private def resolveEvaluator(ruleType: String, customPolicyTypes: Map[String, String]): Option[CustomPolicyEvaluator] =
    builtinEvaluators.get(ruleType).orElse {
      customPolicyTypes.get(ruleType).flatMap(className => CustomPolicyEvaluatorFactory.tryResolve(className).toOption)
    }

  /** The seven built-in `PolicyType`s, each an ordinary
    * `CustomPolicyEvaluator` — the identical trait a third party's own
    * policy type implements via `OrgPolicy.customPolicyTypes`. Nothing
    * about a built-in type's *evaluation* is privileged anymore; what
    * makes it "built-in" is only that it ships compiled into this module
    * and `resolveEvaluator` checks this map before `customPolicyTypes` is
    * ever consulted, not a separate dispatch mechanism the way an earlier
    * version of this method had (a hardcoded match on `InterpretedPolicy`,
    * split by `DatasetPolicy`/`ContractPolicy` — see that trait's own doc,
    * still accurate as a *classification* even though evaluation no
    * longer branches on it directly here).
    *
    * Each entry is built via `interpreted` (below): every built-in
    * evaluator's real shape is "start from `rule.interpret`'s already
    * parsed/validated `InterpretedPolicy`, match this type's own case, and
    * produce no violations for anything else" — `interpreted` factors that
    * shared wrapper out once instead of repeating it seven times (the
    * `case Some(InterpretedPolicy.X(...)) => ...; case _ => Nil` shape
    * each entry would otherwise need is exactly what it replaces).
    * `interpret` returning `None` here (wrong `ruleType` for this
    * evaluator, or malformed properties for the right one) is the
    * identical total/safe behavior every built-in type always had.
    */
  private val builtinEvaluators: Map[String, CustomPolicyEvaluator] = Map(
    PolicyType.RequireCatalog -> interpreted { case (contract, rule, InterpretedPolicy.RequireCatalog(technology)) =>
      scopedDatasets(contract, rule).flatMap(checkRequireCatalog(rule, _, technology))
    },
    PolicyType.RequireField -> interpreted { case (contract, rule, InterpretedPolicy.RequireField(name, fieldType)) =>
      scopedDatasets(contract, rule).flatMap(checkRequireField(rule, _, name, fieldType))
    },
    PolicyType.FieldNamingConvention -> interpreted {
      case (contract, rule, InterpretedPolicy.FieldNamingConvention(pattern)) =>
        scopedDatasets(contract, rule).flatMap(checkFieldNamingConvention(rule, _, pattern))
    },
    PolicyType.RequireExtension -> interpreted { case (contract, rule, InterpretedPolicy.RequireExtension(key, value)) =>
      checkRequireExtension(rule, contract, key, value)
    },
    PolicyType.RequireFormat -> interpreted { case (contract, rule, InterpretedPolicy.RequireFormat(formats)) =>
      scopedDatasets(contract, rule).flatMap(checkRequireFormat(rule, _, formats))
    },
    PolicyType.RequireDatasetDescription -> interpreted {
      case (contract, rule, InterpretedPolicy.RequireDatasetDescription) =>
        scopedDatasets(contract, rule).flatMap(checkRequireDatasetDescription(rule, _))
    },
    PolicyType.RequireExtensionIf -> interpreted {
      case (contract, rule, InterpretedPolicy.RequireExtensionIf(ifKey, ifValue, thenKey, thenValue)) =>
        checkRequireExtensionIf(rule, contract, ifKey, ifValue, thenKey, thenValue)
    }
  )

  /** Builds a `CustomPolicyEvaluator` from `f`, a partial function over
    * `(contract, rule, rule.interpret's-unwrapped-value)` covering only
    * the one `InterpretedPolicy` case this built-in type matches —
    * `interpret` returning `None`, or `f` not being defined for the
    * `Some` it returns (both signal "not this evaluator's ruleType, or
    * malformed properties for it"), produce `Nil` rather than throwing,
    * the same total/safe contract every built-in evaluator has always had.
    * `contract`/`rule` ride along in the matched tuple (rather than `f`
    * closing over them) purely so every `builtinEvaluators` entry above
    * can stay a single `case` clause with no surrounding boilerplate.
    */
  private def interpreted(f: PartialFunction[(Contract, PolicyRule, InterpretedPolicy), List[PolicyViolation]]): CustomPolicyEvaluator =
    (contract, rule) => rule.interpret.flatMap(ip => f.lift((contract, rule, ip))).getOrElse(Nil)

  /** `datasetsInScope(contract, rule.scope)`, further narrowed by
    * `rule.when` — the identical "which datasets does this rule apply to"
    * computation every dataset-level built-in evaluator above needs, split
    * out once rather than repeated seven times.
    */
  private def scopedDatasets(contract: Contract, rule: PolicyRule): List[Dataset] =
    datasetsInScope(contract, rule.scope).filter(matchesCondition(_, rule.when))

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

  /** Whether `contract.extensions` declares `key` at all - and, if `value`
    * is set, that the declared value equals it exactly (case-sensitive:
    * extensions values are free-form, unlike `RequireField.fieldType`'s
    * closed type-name vocabulary, so no case-folding is applied). A key
    * present but mapped to YAML `null` (`extensions: { owner: }`) is
    * treated the same as the key being absent entirely - present-but-blank
    * isn't a real declaration. Shared by `checkRequireExtension` and
    * `checkRequireExtensionIf`, which both need this identical "is this
    * key/value satisfied" test - once for the rule's own outcome, once for
    * whether its `if` condition holds at all.
    */
  private def extensionSatisfies(contract: Contract, key: String, value: Option[String]): Boolean =
    contract.extensions.get(key).filter(_ != null).exists(v => value.forall(pinned => String.valueOf(v) == pinned))

  /** Checked once against `contract` as a whole - see
    * `InterpretedPolicy.ContractPolicy`'s doc for why this doesn't go
    * through `datasetsInScope` the way the dataset-level checks do.
    */
  private def checkRequireExtension(rule: PolicyRule, contract: Contract, key: String, value: Option[String]): List[PolicyViolation] = {
    if (extensionSatisfies(contract, key, value)) Nil
    else {
      val valueSuffix = value.map(v => s" with value '$v'").getOrElse("")
      val actualSuffix = contract.extensions.get(key).filter(_ != null).map(v => s" (currently '$v')").getOrElse("")
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

  /** Only when `contract` already satisfies `ifKey`/`ifValue` (via
    * `extensionSatisfies`) does it *also* need to satisfy `thenKey`/
    * `thenValue` - a contract that doesn't meet the `if` condition at all
    * produces no violation, since this rule simply doesn't apply to it,
    * the same as a `DatasetPolicy` rule against a dataset outside its
    * `scope`.
    */
  private def checkRequireExtensionIf(
      rule: PolicyRule,
      contract: Contract,
      ifKey: String,
      ifValue: Option[String],
      thenKey: String,
      thenValue: Option[String]
  ): List[PolicyViolation] = {
    if (!extensionSatisfies(contract, ifKey, ifValue)) Nil
    else if (extensionSatisfies(contract, thenKey, thenValue)) Nil
    else {
      val ifValueSuffix = ifValue.map(v => s"='$v'").getOrElse("")
      val thenValueSuffix = thenValue.map(v => s" with value '$v'").getOrElse("")
      val actualSuffix = contract.extensions.get(thenKey).filter(_ != null).map(v => s" (currently '$v')").getOrElse("")
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          dataset = None,
          s"organizational policy '${rule.id}'${describe(rule)} requires contract '${contract.id}' to declare " +
            s"extensions.$thenKey$thenValueSuffix whenever extensions.$ifKey$ifValueSuffix, but it does not$actualSuffix.",
          s"Add 'extensions: { $thenKey: ${thenValue.getOrElse("<value>")} }' to the contract."
        )
      )
    }
  }

  private def describe(rule: PolicyRule): String = rule.description.map(d => s" ($d)").getOrElse("")
}
