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
      }
      // require_extension checks the contract as a whole (ContractPolicy),
      // not one dataset at a time - scope/when only ever narrow *which
      // datasets* a DatasetPolicy rule applies to, so declaring either here
      // is silently inert rather than a mistake OrgPolicyEvaluator itself
      // can catch (it never even inspects them for this ruleType). Flagged
      // as a Warning, not an Error: the rule still evaluates correctly,
      // just not the way scope/when might make an author expect.
      if (rule.ruleType == PolicyType.RequireExtension && (rule.scope != PolicyScope.All || rule.when.isDefined)) {
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          "'scope'/'when' have no effect on require_extension - it checks the contract as a whole, not individual datasets"
        )
      }
    }

    duplicateNames(policy.policies.map(_.id)).foreach { id =>
      issues += ValidationIssue(ValidationSeverity.Error, "policies", s"Duplicate policy id '$id'")
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

  private def ruleHint(ruleType: String): String = ruleType match {
    case PolicyType.RequireField           => " (expected a non-empty 'name' property)"
    case PolicyType.FieldNamingConvention  => " (expected a 'pattern' property that compiles as a valid regex)"
    case PolicyType.RequireExtension       => " (expected a non-empty 'key' property)"
    case PolicyType.RequireFormat          => " (expected a non-empty 'formats' property - a single format or a list)"
    case _                                 => ""
  }

  private def duplicateNames(names: List[String]): Set[String] =
    names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSet
}
