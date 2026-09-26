// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.time.LocalDate
import scala.util.Try

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

    // Dead entry, not a crash: OrgPolicyEvaluator.evaluateRule only ever
    // consults customPolicyTypes when rule.interpret is None, which a
    // built-in ruleType with well-formed properties never is - the
    // built-in interpretation always wins. Flagged the same way
    // ContractLevelTypes' inert scope/when is: a Warning, since it still
    // evaluates correctly, just not via the class this entry names. The
    // identical shape (empty key/class name is an Error, a collision with
    // a built-in name is a dead-entry Warning, an unresolvable class is an
    // Error) governs `typeGuarantees.customTypeGuaranteeTypes` below too -
    // see `validateCustomRegistry`.
    issues ++= validateCustomRegistry(
      policy.customPolicyTypes,
      PolicyType.All,
      "PolicyType",
      CustomPolicyEvaluatorFactory.tryResolve,
      "customPolicyTypes"
    )

    policy.typeGuarantees.enabled.zipWithIndex.foreach { case (checkType, idx) =>
      val path = s"typeGuarantees.enabled[$idx]"
      if (checkType.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "A typeGuarantees.enabled entry must not be empty")
      } else if (!TypeGuaranteeType.All.contains(checkType) && !policy.typeGuarantees.customTypeGuaranteeTypes.contains(checkType)) {
        // Mirrors the identical "not built-in, no customPolicyTypes entry
        // for it either" Warning above - could be a forward-compatible
        // document authored against a newer Invaract, but far more often a
        // typo'd or forgotten customTypeGuaranteeTypes registration.
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"typeGuarantees.enabled names '$checkType', which is not a built-in TypeGuaranteeType and has no " +
            "customTypeGuaranteeTypes entry naming a TypeGuaranteeCheck for it - it will never be evaluated"
        )
      }
    }

    issues ++= validateCustomRegistry(
      policy.typeGuarantees.customTypeGuaranteeTypes,
      TypeGuaranteeType.All,
      "TypeGuaranteeType",
      TypeGuaranteeCheckFactory.tryResolve,
      "typeGuarantees.customTypeGuaranteeTypes"
    )

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

    policy.controlTables.zipWithIndex.foreach { case (registration, idx) =>
      val path = s"controlTables[$idx]"
      if (registration.location.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "controlTables entry's location must not be empty")
      }
      if (registration.owner.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "controlTables entry's owner must not be empty")
      }
      registration.reviewBy.filter(_.isBefore(now)).foreach { date =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"controlTables entry's reviewBy date ($date) has already passed; it no longer counts as registered for " +
            "require_control_registration - the dataset falls back to being treated as unregistered"
        )
      }
    }

    duplicateNames(policy.controlTables.map(r => CrossContractValidator.normalize(r.location))).foreach { location =>
      issues += ValidationIssue(
        ValidationSeverity.Error,
        "controlTables",
        s"Duplicate controlTables location '$location' (compared after path normalization) - ambiguous which entry a matching dataset should be checked against"
      )
    }

    // controlTables is a catalogue independent of enforcement (see its own
    // doc) - a non-empty one with no require_control_registration rule
    // anywhere in this document (built-in type, so a customPolicyTypes
    // entry can never be what makes it "used" - the built-in always wins)
    // is very likely a platform team that built the catalogue and forgot
    // the rule that actually checks it, not an intentional "documentation
    // only" choice. Worth surfacing, the same "likely mistake, not fatal"
    // treatment every other maybe-dead-config Warning in this file gets.
    if (policy.controlTables.nonEmpty && !policy.policies.exists(_.ruleType == PolicyType.RequireControlRegistration)) {
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        "controlTables",
        s"controlTables has ${policy.controlTables.size} entr${if (policy.controlTables.size == 1) "y" else "ies"} " +
          "but no require_control_registration policy is attached - the catalogue is purely documentary until one is"
      )
    }

    ValidationResult(issues.result())
  }

  /** Validates every layer in an ordered `List[(String, OrgPolicy)]`
    * individually (`validate`, each layer's issues re-pathed with the
    * caller-supplied label - typically the file path it was loaded from -
    * so a caller can tell which layer an issue came from), plus one check
    * that only makes sense *across* layers: a policy `id` repeated in more
    * than one layer. The single entry point both real callers
    * (`ContractEnforcementRule.enforceOrgPolicy`, `OrgPolicyLintCli`) use to
    * validate a policy-layering stack, rather than each hand-rolling its own
    * per-layer loop alongside this one - see docs/CONTRACT_MODEL.md's
    * "Policy layering" section.
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
  def validateLayers(layers: List[(String, OrgPolicy)], now: LocalDate = LocalDate.now()): ValidationResult = {
    val perLayer = layers.flatMap { case (label, layer) =>
      validate(layer, now).issues.map(issue => issue.copy(path = s"$label: ${issue.path}"))
    }
    val duplicateIds = duplicateNames(layers.flatMap { case (_, layer) => layer.policies.map(_.id) })
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
    case PolicyType.RequireDatasetType     => " (if 'types' is set, expected a non-empty list of DATA_ASSET/SOURCE/CONTROL - omit 'types' entirely to require any declared type)"
    case PolicyType.ForbidControlSensitivityTags => " (if 'tags' is set, expected a non-empty list of strings - omit 'tags' entirely to use the default pii/financial set)"
    case PolicyType.RequireControlRegistration => " (if 'requireRegistration' is set, expected a boolean - omit it entirely to default to true)"
    case _                                 => ""
  }

  private def duplicateNames(names: List[String]): Set[String] =
    names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSet

  /** Validates a reflective plugin-class registry map — `customPolicyTypes`
    * and `typeGuarantees.customTypeGuaranteeTypes` are the identical shape
    * (a name mapped to a class name Invaract resolves reflectively),
    * parameterized only by which built-in name set a key can collide with,
    * that built-in set's own display name, and which `*Factory.tryResolve`
    * decides whether a class name actually resolves. `fieldPath` is both
    * the document key this map lives under (`"customPolicyTypes"`,
    * `"typeGuarantees.customTypeGuaranteeTypes"`) and the prefix every
    * issue's own `path`/message uses, so a caller never has to keep the two
    * in sync by hand.
    */
  private def validateCustomRegistry(
      entries: Map[String, String],
      builtinNames: Set[String],
      builtinKindLabel: String,
      resolve: String => Try[Any],
      fieldPath: String
  ): List[ValidationIssue] =
    entries.toList.sortBy(_._1).flatMap { case (name, className) =>
      val path = s"$fieldPath.$name"
      val entryIssues = List.newBuilder[ValidationIssue]
      if (name.trim.isEmpty) {
        entryIssues += ValidationIssue(ValidationSeverity.Error, fieldPath, s"A $fieldPath key must not be empty")
      } else if (builtinNames.contains(name)) {
        entryIssues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"$fieldPath entry for '$name' is dead - it's already a built-in $builtinKindLabel, which always takes precedence"
        )
      }
      if (className.trim.isEmpty) {
        entryIssues += ValidationIssue(ValidationSeverity.Error, path, s"A $fieldPath class name must not be empty")
      } else {
        resolve(className).failed.foreach { e =>
          entryIssues += ValidationIssue(ValidationSeverity.Error, path, s"$fieldPath class '$className' could not be resolved: ${e.getMessage}")
        }
      }
      entryIssues.result()
    }
}
