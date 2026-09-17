// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.time.LocalDate

/** Validates a parsed [[OrgPolicy]] beyond what `OrgPolicyParser` already
  * guarantees structurally — the org-policy counterpart to
  * `ContractValidator`. Runs a second pass and collects every issue found in
  * one call, rather than stopping at the first, reusing
  * `ValidationSeverity`/`ValidationIssue`/`ValidationResult` from
  * `ContractValidator.scala` rather than defining a parallel result shape.
  */
object OrgPolicyValidator {

  def validate(policy: OrgPolicy, now: LocalDate = LocalDate.now()): ValidationResult = {
    val issues = List.newBuilder[ValidationIssue]

    if (policy.version.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, "version", "Organizational policy version must not be empty")
    }

    policy.policies.zipWithIndex.foreach { case (rule, idx) =>
      val path = s"policies[$idx]"
      if (rule.id.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "Policy id must not be empty")
      }
      if (rule.ruleType.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "Policy type must not be empty")
      } else if (PolicyType.All.contains(rule.ruleType) && rule.interpret.isEmpty) {
        issues += ValidationIssue(
          ValidationSeverity.Error,
          path,
          s"Policy type '${rule.ruleType}' has malformed or missing properties for its shape" + ruleHint(rule.ruleType)
        )
      } else if (
        !PolicyType.All.contains(rule.ruleType) && !policy.customPolicyTypes.contains(rule.ruleType)
      ) {
        // Not an Error: this is the same "recorded but never acted on"
        // behavior PolicyType's own doc describes for a ruleType this
        // version of Invaract simply doesn't (yet) know - could be a
        // forward-compatible document authored against a newer Invaract,
        // not necessarily a mistake. Still worth surfacing as a Warning,
        // though, now that customPolicyTypes exists: this is exactly the
        // shape a typo'd or forgotten customPolicyTypes registration takes
        // (the rule references a type, but nothing maps it to a class), and
        // that failure mode was silent before this field existed.
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"Policy type '${rule.ruleType}' is not a built-in type and has no customPolicyTypes entry naming a " +
            "CustomPolicyEvaluator for it - this rule will never be evaluated"
        )
      }
      // A ContractPolicy type (PolicyType.ContractLevelTypes) checks the
      // contract as a whole, not one dataset at a time - scope/when only
      // ever narrow *which datasets* a DatasetPolicy rule applies to, so
      // declaring either here is silently inert rather than a mistake
      // OrgPolicyEvaluator itself can catch (it never even inspects them
      // for a ContractPolicy ruleType). Flagged as a Warning, not an
      // Error: the rule still evaluates correctly, just not the way
      // scope/when might make an author expect. Checked against the
      // shared set, not a single hardcoded ruleType, so a future
      // ContractPolicy addition (require_extension_if included) gets this
      // warning for free rather than needing its own `||` branch here.
      if (PolicyType.ContractLevelTypes.contains(rule.ruleType) && (rule.scope != PolicyScope.All || rule.when.isDefined)) {
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"'scope'/'when' have no effect on ${rule.ruleType} - it checks the contract as a whole, not individual datasets"
        )
      }
    }

    duplicateNames(policy.policies.map(_.id)).foreach { id =>
      issues += ValidationIssue(ValidationSeverity.Error, "policies", s"Duplicate policy id '$id'")
    }

    policy.customPolicyTypes.toList.sortBy(_._1).foreach { case (ruleType, className) =>
      val path = s"customPolicyTypes.$ruleType"
      if (ruleType.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, "customPolicyTypes", "A customPolicyTypes key must not be empty")
      } else if (PolicyType.All.contains(ruleType)) {
        // Dead entry, not a crash: OrgPolicyEvaluator.evaluateRule only
        // ever consults customPolicyTypes when rule.interpret is None,
        // which a built-in ruleType with well-formed properties never is -
        // the built-in interpretation always wins. Flagged the same way
        // ContractLevelTypes' inert scope/when is: a Warning, since it
        // still evaluates correctly, just not via the class this entry
        // names.
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"customPolicyTypes entry for '$ruleType' is dead - it's already a built-in PolicyType, which always takes precedence"
        )
      }
      if (className.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "A customPolicyTypes class name must not be empty")
      } else {
        CustomPolicyEvaluatorFactory.tryResolve(className).failed.foreach { e =>
          issues += ValidationIssue(
            ValidationSeverity.Error,
            path,
            s"customPolicyTypes class '$className' could not be resolved: ${e.getMessage}"
          )
        }
      }
    }

    val knownPolicyIds = policy.policies.map(_.id).toSet
    policy.exemptions.zipWithIndex.foreach { case (exemption, idx) =>
      val path = s"exemptions[$idx]"
      if (exemption.contractId.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "Exemption contractId must not be empty")
      }
      if (exemption.policyIds.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "Exemption must list at least one policyId")
      }
      exemption.policyIds.filterNot(knownPolicyIds.contains).foreach { unknownId =>
        issues += ValidationIssue(ValidationSeverity.Error, path, s"Exemption references unknown policy id '$unknownId'")
      }
      if (exemption.reason.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "Exemption reason must not be empty")
      }
      exemption.reviewBy.filter(_.isBefore(now)).foreach { date =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"Exemption's reviewBy date ($date) has already passed; it no longer suppresses its policy violation"
        )
      }
    }

    ValidationResult(issues.result())
  }

  /** Validates every layer in an ordered `List[OrgPolicy]` individually
    * (`validate`, each layer's issues re-pathed with its own index so a
    * caller can tell which layer an issue came from), plus one check that
    * only makes sense *across* layers: a policy `id` repeated in more than
    * one layer.
    *
    * This is deliberately a Warning, not an Error - unlike a duplicate `id`
    * *within* one document (still an Error, above), a repeated `id` across
    * layers is not unsafe. Each layer is evaluated independently
    * (`OrgPolicyEvaluator.evaluateLayers`), so a repeated id never causes
    * one layer's rule to shadow or be confused with another's during
    * evaluation itself - `PolicyExemption.covers` and violation attribution
    * both stay correctly scoped to the one layer that owns the id. It's
    * still worth flagging, though: a human reading two violations both
    * attributed to `id 'catalog-required'` (one from the org-wide layer,
    * one from a business unit's) can't tell them apart by id alone. See
    * docs/CONTRACT_MODEL.md's "Policy layering" section, which recommends
    * a naming convention (e.g. prefixing a layer's own rule ids, such as
    * `bu-finance-catalog-required`) rather than mechanically namespacing
    * ids - deliberately not automated here, the same "don't design for a
    * problem a naming convention already solves" restraint this codebase
    * applies elsewhere.
    *
    * Does not, itself, check that an exemption only references its own
    * layer's policies - that already falls out of `validate` running on
    * each layer in isolation (an exemption naming an id from a *different*
    * layer simply doesn't exist in *this* layer's own `policies`, so
    * `validate` already reports it as "references unknown policy id").
    */
  def validateLayers(layers: List[OrgPolicy], now: LocalDate = LocalDate.now()): ValidationResult = {
    val perLayer = layers.zipWithIndex.flatMap { case (layer, idx) =>
      validate(layer, now).issues.map(issue => issue.copy(path = s"layers[$idx].${issue.path}"))
    }
    val duplicateIds = duplicateNames(layers.flatMap(_.policies.map(_.id)))
    val crossLayerWarnings = duplicateIds.toList.sorted.map { id =>
      ValidationIssue(
        ValidationSeverity.Warning,
        "layers",
        s"Policy id '$id' is declared in more than one layer - each layer evaluates independently (no shadowing), " +
          "but violation messages won't distinguish which layer produced them; consider a per-layer id prefix"
      )
    }
    ValidationResult(perLayer ++ crossLayerWarnings)
  }

  private def ruleHint(ruleType: String): String = ruleType match {
    case PolicyType.RequireField           => " (expected a non-empty 'name' property)"
    case PolicyType.FieldNamingConvention  => " (expected a 'pattern' property that compiles as a valid regex)"
    case PolicyType.RequireExtension       => " (expected a non-empty 'key' property)"
    case PolicyType.RequireFormat          => " (expected a non-empty 'formats' property - a single format or a list)"
    case PolicyType.RequireExtensionIf     => " (expected non-empty 'ifKey' and 'thenKey' properties)"
    case _                                 => ""
  }

  private def duplicateNames(names: List[String]): Set[String] =
    names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSet
}
