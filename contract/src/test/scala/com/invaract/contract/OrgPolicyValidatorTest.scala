// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class OrgPolicyValidatorTest extends AnyFunSuite {

  private val requireCatalog = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty)

  test("a well-formed policy with no issues is valid") {
    val policy = OrgPolicy("1.0", List(requireCatalog))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid)
    assert(result.errors.isEmpty)
  }

  test("an empty version is an Error") {
    val result = OrgPolicyValidator.validate(OrgPolicy(""))
    assert(!result.isValid)
    assert(result.errors.exists(_.path == "version"))
  }

  test("a policy rule with an empty id is an Error") {
    val policy = OrgPolicy("1.0", List(PolicyRule("", PolicyType.RequireCatalog, Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("Policy id must not be empty")))
  }

  test("a policy rule with an empty type is an Error") {
    val policy = OrgPolicy("1.0", List(PolicyRule("x", "", Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("Policy type must not be empty")))
  }

  test("a known policy type with malformed properties is an Error, not silently accepted") {
    // require_field with no 'name' property - interpret returns None
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireField, Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("an unrecognized policy type is not an Error - recorded, not interpreted - but is a Warning") {
    val policy = OrgPolicy("1.0", List(PolicyRule("custom", "some_future_type", Map("x" -> "y"))))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid)
    assert(result.warnings.exists(_.message.contains("will never be evaluated")))
  }

  test("require_extension with no 'key' property is an Error, not silently accepted") {
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireExtension, Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("require_extension with a 'key' property is valid") {
    val policy = OrgPolicy("1.0", List(PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))))
    assert(OrgPolicyValidator.validate(policy).isValid)
  }

  test("require_extension with a non-default scope is a Warning, since scope has no effect on it") {
    val policy = OrgPolicy(
      "1.0",
      List(PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"), scope = PolicyScope.Outputs))
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid, "a Warning, not an Error - the rule still evaluates correctly")
    assert(result.warnings.exists(_.message.contains("no effect on require_extension")))
  }

  test("require_extension with a 'when' condition is also a Warning, for the same reason as scope") {
    val policy = OrgPolicy(
      "1.0",
      List(PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"), when = Some(PolicyCondition("pii"))))
    )
    assert(OrgPolicyValidator.validate(policy).warnings.exists(_.message.contains("no effect on require_extension")))
  }

  test("require_extension with the default scope (All) and no 'when' triggers no warning") {
    val policy = OrgPolicy("1.0", List(PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))))
    assert(OrgPolicyValidator.validate(policy).warnings.isEmpty)
  }

  test("the scope/when warning is specific to require_extension - an ordinary dataset-scoped rule triggers nothing") {
    val policy = OrgPolicy(
      "1.0",
      List(PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs))
    )
    assert(OrgPolicyValidator.validate(policy).warnings.isEmpty)
  }

  test("require_format with no 'formats' property is an Error, not silently accepted") {
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireFormat, Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("require_format with an empty 'formats' list is an Error") {
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireFormat, Map("formats" -> List.empty[String]))))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("require_format with a 'formats' property is valid") {
    val policy = OrgPolicy("1.0", List(PolicyRule("delta-only", PolicyType.RequireFormat, Map("formats" -> "delta"))))
    assert(OrgPolicyValidator.validate(policy).isValid)
  }

  test("require_dataset_description is valid with no properties at all") {
    val policy = OrgPolicy("1.0", List(PolicyRule("described", PolicyType.RequireDatasetDescription, Map.empty)))
    assert(OrgPolicyValidator.validate(policy).isValid)
  }

  test("require_extension_if with no 'ifKey'/'thenKey' properties is an Error, not silently accepted") {
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireExtensionIf, Map.empty)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("require_extension_if with only 'ifKey' (missing 'thenKey') is an Error") {
    val policy = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireExtensionIf, Map("ifKey" -> "status"))))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("malformed or missing properties")))
  }

  test("require_extension_if with 'ifKey' and 'thenKey' is valid") {
    val policy = OrgPolicy(
      "1.0",
      List(PolicyRule("sunset-if-deprecated", PolicyType.RequireExtensionIf, Map("ifKey" -> "status", "thenKey" -> "sunsetDate")))
    )
    assert(OrgPolicyValidator.validate(policy).isValid)
  }

  test("require_extension_if with a non-default scope is a Warning, since scope has no effect on it") {
    val policy = OrgPolicy(
      "1.0",
      List(
        PolicyRule(
          "sunset-if-deprecated",
          PolicyType.RequireExtensionIf,
          Map("ifKey" -> "status", "thenKey" -> "sunsetDate"),
          scope = PolicyScope.Outputs
        )
      )
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid, "a Warning, not an Error - the rule still evaluates correctly")
    assert(result.warnings.exists(_.message.contains("no effect on require_extension_if")))
  }

  test("duplicate policy ids are an Error") {
    val policy = OrgPolicy(
      "1.0",
      List(
        PolicyRule("dup", PolicyType.RequireCatalog, Map.empty),
        PolicyRule("dup", PolicyType.RequireCatalog, Map.empty)
      )
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("Duplicate policy id 'dup'")))
  }

  test("an exemption is valid when it references a real policy id, has a contractId and a reason") {
    val policy = OrgPolicy(
      "1.0",
      List(requireCatalog),
      exemptions = List(PolicyExemption("some_contract", List("catalog-required"), "legacy"))
    )
    assert(OrgPolicyValidator.validate(policy).isValid)
  }

  test("an exemption with an empty contractId is an Error") {
    val policy = OrgPolicy("1.0", List(requireCatalog), exemptions = List(PolicyExemption("", List("catalog-required"), "reason")))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("contractId must not be empty")))
  }

  test("an exemption with no policyIds is an Error") {
    val policy = OrgPolicy("1.0", List(requireCatalog), exemptions = List(PolicyExemption("c", Nil, "reason")))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("at least one policyId")))
  }

  test("an exemption referencing an unknown policy id is an Error") {
    val policy = OrgPolicy("1.0", List(requireCatalog), exemptions = List(PolicyExemption("c", List("does-not-exist"), "reason")))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("unknown policy id 'does-not-exist'")))
  }

  test("an exemption with an empty reason is an Error") {
    val policy = OrgPolicy("1.0", List(requireCatalog), exemptions = List(PolicyExemption("c", List("catalog-required"), "")))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("reason must not be empty")))
  }

  test("an exemption whose reviewBy is exactly 'now' is not expired - a Warning is not raised at the boundary") {
    val now = LocalDate.of(2026, 1, 1)
    val policy = OrgPolicy(
      "1.0",
      List(requireCatalog),
      exemptions = List(PolicyExemption("c", List("catalog-required"), "reason", Some(now)))
    )
    val result = OrgPolicyValidator.validate(policy, now)
    assert(!result.warnings.exists(_.message.contains("already passed")))
  }

  test("an exemption whose reviewBy is one day before 'now' is a Warning, not an Error") {
    val now = LocalDate.of(2026, 1, 2)
    val policy = OrgPolicy(
      "1.0",
      List(requireCatalog),
      exemptions = List(PolicyExemption("c", List("catalog-required"), "reason", Some(now.minusDays(1))))
    )
    val result = OrgPolicyValidator.validate(policy, now)
    assert(result.isValid) // still no Errors
    assert(result.warnings.exists(_.message.contains("already passed")))
  }

  test("an exemption with no reviewBy never triggers the expiry warning") {
    val policy = OrgPolicy("1.0", List(requireCatalog), exemptions = List(PolicyExemption("c", List("catalog-required"), "reason", None)))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.warnings.isEmpty)
  }

  // -- customPolicyTypes -----------------------------------------------------

  private val lowercaseIdEvaluatorClass = classOf[ContractIdMustBeLowercaseEvaluator].getName

  test("a customPolicyTypes entry naming a resolvable class is valid, no issues") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> lowercaseIdEvaluatorClass))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid)
    assert(result.warnings.isEmpty)
  }

  test("a customPolicyTypes entry with an empty class name is an Error") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> ""))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(_.message.contains("class name must not be empty")))
  }

  test("a customPolicyTypes entry naming a class with no public no-arg constructor is an Error") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      customPolicyTypes = Map("require_lowercase_id" -> classOf[NoNoArgConstructorCustomPolicyEvaluator].getName)
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(result.errors.exists(e => e.path == "customPolicyTypes.require_lowercase_id" && e.message.contains("could not be resolved")))
  }

  test("a customPolicyTypes entry naming a class that doesn't implement CustomPolicyEvaluator is an Error") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      customPolicyTypes = Map("require_lowercase_id" -> classOf[NotACustomPolicyEvaluator].getName)
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(!result.isValid)
  }

  test("a customPolicyTypes entry naming a class that doesn't exist is an Error") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      customPolicyTypes = Map("require_lowercase_id" -> "com.invaract.contract.NoSuchClassAtAll")
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(!result.isValid)
  }

  test("a customPolicyTypes entry colliding with a built-in PolicyType is a Warning, not an Error") {
    val policy = OrgPolicy(
      "1.0",
      List(requireCatalog),
      customPolicyTypes = Map(PolicyType.RequireCatalog -> lowercaseIdEvaluatorClass)
    )
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid)
    assert(result.warnings.exists(w => w.path == s"customPolicyTypes.${PolicyType.RequireCatalog}" && w.message.contains("dead")))
  }

  test("a rule referencing a ruleType with a valid customPolicyTypes entry produces no 'will never be evaluated' warning") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> lowercaseIdEvaluatorClass))
    val result = OrgPolicyValidator.validate(policy)
    assert(!result.warnings.exists(_.message.contains("will never be evaluated")))
  }

  // -- validateLayers (policy layering/inheritance) --------------------------

  test("validateLayers with no layers at all is valid, with no issues") {
    val result = OrgPolicyValidator.validateLayers(Nil)
    assert(result.isValid)
    assert(result.issues.isEmpty)
  }

  test("validateLayers with well-formed, non-overlapping layers is valid") {
    val orgWide = OrgPolicy("1.0", List(requireCatalog))
    val buOverlay = OrgPolicy("1.0", List(PolicyRule("bu-owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))))
    val result = OrgPolicyValidator.validateLayers(List("org-wide" -> orgWide, "bu-overlay" -> buOverlay))
    assert(result.isValid)
    assert(result.warnings.isEmpty)
  }

  test("validateLayers surfaces each layer's own issues, path-prefixed with the caller-supplied label") {
    val orgWide = OrgPolicy("1.0", List(requireCatalog))
    val brokenOverlay = OrgPolicy("1.0", List(PolicyRule("bad", PolicyType.RequireField, Map.empty))) // missing required 'name'
    val result = OrgPolicyValidator.validateLayers(List("org-wide" -> orgWide, "bu-overlay" -> brokenOverlay))
    assert(!result.isValid)
    assert(result.errors.exists(e => e.path == "bu-overlay: policies[0]" && e.message.contains("malformed or missing properties")))
  }

  test("validateLayers: a policy id repeated across layers is a Warning, not an Error") {
    val orgWide = OrgPolicy("1.0", List(requireCatalog)) // id "catalog-required"
    val buOverlay = OrgPolicy("1.0", List(requireCatalog)) // same id, independent rule
    val result = OrgPolicyValidator.validateLayers(List("org-wide" -> orgWide, "bu-overlay" -> buOverlay))
    assert(result.isValid, "a Warning, not an Error - each layer still evaluates independently, no shadowing")
    assert(result.warnings.exists(w => w.path == "layers" && w.message.contains("'catalog-required'") && w.message.contains("more than one layer")))
  }

  test("validateLayers: a policy id unique to each layer triggers no cross-layer warning") {
    val orgWide = OrgPolicy("1.0", List(requireCatalog))
    val buOverlay = OrgPolicy("1.0", List(PolicyRule("bu-owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))))
    val result = OrgPolicyValidator.validateLayers(List("org-wide" -> orgWide, "bu-overlay" -> buOverlay))
    assert(!result.warnings.exists(_.message.contains("more than one layer")))
  }

  test("validateLayers: an exemption naming an id from a different layer is reported as an unknown policy id, not silently allowed") {
    val orgWide = OrgPolicy("1.0", List(requireCatalog)) // id "catalog-required", owned by layer 0
    val buOverlay = OrgPolicy(
      "1.0",
      exemptions = List(PolicyExemption("some_contract", List("catalog-required"), "the BU wants this exempted"))
    )
    val result = OrgPolicyValidator.validateLayers(List("org-wide" -> orgWide, "bu-overlay" -> buOverlay))
    assert(!result.isValid)
    assert(result.errors.exists(e => e.path == "bu-overlay: exemptions[0]" && e.message.contains("unknown policy id 'catalog-required'")))
  }
}
