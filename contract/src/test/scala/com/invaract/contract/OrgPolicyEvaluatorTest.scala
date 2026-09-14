// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class OrgPolicyEvaluatorTest extends AnyFunSuite {

  private def field(name: String, tags: Set[String] = Set.empty, properties: List[Field] = Nil): Field =
    Field(name, "string", required = false, nullable = true, properties, tags)

  private def dataset(
      name: String,
      fields: List[Field] = List(field("id")),
      catalog: Option[CatalogRequirement] = None
  ): Dataset =
    Dataset(name, s"loc/$name", format = Some("parquet"), schema = Schema(fields), catalog = catalog)

  private def contract(inputs: List[Dataset] = Nil, outputs: List[Dataset] = List(dataset("out")), rules: List[ContractRule] = Nil): Contract =
    Contract("test_contract", ContractVersion(1, 0, 0), "active", inputs, outputs, rules, Map.empty)

  private val now = LocalDate.of(2026, 1, 1)

  // -- require_catalog -----------------------------------------------------

  test("require_catalog: satisfied when catalog.required is true, no technology pinned") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", catalog = Some(CatalogRequirement(required = true)))))
    val eval = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now)
    assert(eval.allViolations.isEmpty)
  }

  test("require_catalog: violated when no catalog block is declared at all") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out")))
    val eval = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now)
    assert(eval.enforceViolations.size == 1)
    assert(eval.enforceViolations.head.dataset.contains("out"))
  }

  test("require_catalog: violated when catalog.required is false") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", catalog = Some(CatalogRequirement(required = false)))))
    val eval = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now)
    assert(eval.allViolations.size == 1)
  }

  test("require_catalog: technology pin satisfied only when it matches exactly") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map("technology" -> "hive"), scope = PolicyScope.Outputs)
    val matching = contract(outputs = List(dataset("out", catalog = Some(CatalogRequirement(required = true, technology = Some("hive"))))))
    val mismatching = contract(outputs = List(dataset("out", catalog = Some(CatalogRequirement(required = true, technology = Some("delta"))))))

    assert(OrgPolicyEvaluator.evaluate(matching, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
    assert(OrgPolicyEvaluator.evaluate(mismatching, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  // -- require_field --------------------------------------------------------

  test("require_field: satisfied when the named field is present") {
    val rule = PolicyRule("pii-flag", PolicyType.RequireField, Map("name" -> "pii_reviewed"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("id"), field("pii_reviewed")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_field: violated when the named field is absent") {
    val rule = PolicyRule("pii-flag", PolicyType.RequireField, Map("name" -> "pii_reviewed"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("id")))))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.message.contains("pii_reviewed"))
  }

  test("require_field: type pin is checked case-insensitively") {
    val rule = PolicyRule("typed", PolicyType.RequireField, Map("name" -> "id", "type" -> "STRING"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("id")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_field: type pin violated on mismatch") {
    val rule = PolicyRule("typed", PolicyType.RequireField, Map("name" -> "id", "type" -> "integer"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("id")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_field: only checks top-level fields, not nested struct properties") {
    val rule = PolicyRule("nested", PolicyType.RequireField, Map("name" -> "zip"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("address", properties = List(field("zip")))))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  // -- field_naming_convention ----------------------------------------------

  test("field_naming_convention: no violation when every field name matches") {
    val rule = PolicyRule("snake", PolicyType.FieldNamingConvention, Map("pattern" -> "^[a-z][a-z0-9_]*$"))
    val c = contract(outputs = List(dataset("out", fields = List(field("customer_id"), field("total_amount")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("field_naming_convention: violated when exactly one field name doesn't match") {
    val rule = PolicyRule("snake", PolicyType.FieldNamingConvention, Map("pattern" -> "^[a-z][a-z0-9_]*$"))
    val c = contract(outputs = List(dataset("out", fields = List(field("customer_id"), field("CustomerName")))))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.message.contains("CustomerName"))
    assert(violations.head.message.contains("does not")) // singular grammar
  }

  test("field_naming_convention: violated when every field name fails to match, pluralized message") {
    val rule = PolicyRule("snake", PolicyType.FieldNamingConvention, Map("pattern" -> "^[a-z][a-z0-9_]*$"))
    val c = contract(outputs = List(dataset("out", fields = List(field("Bad1"), field("Bad2")))))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1) // one violation per dataset, naming both fields
    assert(violations.head.message.contains("do not"))
  }

  test("field_naming_convention: recurses into nested struct fields") {
    val rule = PolicyRule("snake", PolicyType.FieldNamingConvention, Map("pattern" -> "^[a-z][a-z0-9_]*$"))
    val c = contract(outputs = List(dataset("out", fields = List(field("address", properties = List(field("ZipCode")))))))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.message.contains("ZipCode"))
  }

  test("field_naming_convention: a partial match is not a match - pattern must match the whole name") {
    // "^[a-z]+$" would only partial-match "abc123" if using find(); full-match via Matcher.matches() must reject it.
    val rule = PolicyRule("letters-only", PolicyType.FieldNamingConvention, Map("pattern" -> "^[a-z]+$"))
    val c = contract(outputs = List(dataset("out", fields = List(field("abc123")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  // -- scope ------------------------------------------------------------------

  test("scope Outputs: a policy scoped to outputs never fires on a violating input") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(
      inputs = List(dataset("in")), // no catalog - would violate if inputs were in scope
      outputs = List(dataset("out", catalog = Some(CatalogRequirement(required = true))))
    )
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("scope Inputs: a policy scoped to inputs never fires on a violating output") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Inputs)
    val c = contract(
      inputs = List(dataset("in", catalog = Some(CatalogRequirement(required = true)))),
      outputs = List(dataset("out")) // no catalog - would violate if outputs were in scope
    )
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("scope All: a policy fires on both inputs and outputs") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.All)
    val c = contract(inputs = List(dataset("in")), outputs = List(dataset("out")))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 2)
  }

  // -- when / sensitivityTag ----------------------------------------------

  test("when sensitivityTag: a dataset with no tagged field is skipped entirely") {
    val rule = PolicyRule(
      "pii-catalog",
      PolicyType.RequireCatalog,
      Map.empty,
      scope = PolicyScope.Outputs,
      when = Some(PolicyCondition("pii"))
    )
    val c = contract(outputs = List(dataset("out", fields = List(field("id"))))) // no pii tag, no catalog
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("when sensitivityTag: a dataset with a top-level tagged field is checked") {
    val rule = PolicyRule(
      "pii-catalog",
      PolicyType.RequireCatalog,
      Map.empty,
      scope = PolicyScope.Outputs,
      when = Some(PolicyCondition("pii"))
    )
    val c = contract(outputs = List(dataset("out", fields = List(field("email", tags = Set("pii"))))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("when sensitivityTag: a dataset with only a nested tagged field is still checked (recursion)") {
    val rule = PolicyRule(
      "pii-catalog",
      PolicyType.RequireCatalog,
      Map.empty,
      scope = PolicyScope.Outputs,
      when = Some(PolicyCondition("pii"))
    )
    val c = contract(outputs = List(dataset("out", fields = List(field("contact", properties = List(field("email", tags = Set("pii"))))))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("when sensitivityTag: a different tag does not satisfy the condition") {
    val rule = PolicyRule(
      "pii-catalog",
      PolicyType.RequireCatalog,
      Map.empty,
      scope = PolicyScope.Outputs,
      when = Some(PolicyCondition("pii"))
    )
    val c = contract(outputs = List(dataset("out", fields = List(field("amount", tags = Set("financial"))))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  // -- mode -------------------------------------------------------------------

  test("mode Enforce violations land in enforceViolations, not warnViolations") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs, mode = PolicyMode.Enforce)
    val eval = OrgPolicyEvaluator.evaluate(contract(), OrgPolicy("1.0", List(rule)), now)
    assert(eval.enforceViolations.size == 1)
    assert(eval.warnViolations.isEmpty)
    assert(eval.hasBlockingViolations)
  }

  test("mode Warn violations land in warnViolations, never block") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs, mode = PolicyMode.Warn)
    val eval = OrgPolicyEvaluator.evaluate(contract(), OrgPolicy("1.0", List(rule)), now)
    assert(eval.enforceViolations.isEmpty)
    assert(eval.warnViolations.size == 1)
    assert(!eval.hasBlockingViolations)
  }

  // -- exemptions ---------------------------------------------------------

  private val catalogRule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)

  test("an exemption covering this contract and policy suppresses the violation") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("test_contract", List("catalog-required"), "legacy"))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.isEmpty)
  }

  test("an exemption for a different contractId does not suppress the violation") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("some_other_contract", List("catalog-required"), "legacy"))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.size == 1)
  }

  test("an exemption for a different policyId does not suppress the violation") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("test_contract", List("some-other-policy"), "legacy"))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.size == 1)
  }

  test("an exemption with no reviewBy never expires") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("test_contract", List("catalog-required"), "legacy", reviewBy = None))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.isEmpty)
  }

  test("an exemption is still active exactly on its reviewBy date") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("test_contract", List("catalog-required"), "legacy", reviewBy = Some(now)))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.isEmpty)
  }

  test("an exemption expires the day after its reviewBy date - the violation re-surfaces") {
    val policy = OrgPolicy(
      "1.0",
      List(catalogRule),
      exemptions = List(PolicyExemption("test_contract", List("catalog-required"), "legacy", reviewBy = Some(now.minusDays(1))))
    )
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.size == 1)
  }

  // -- unrecognized policy type --------------------------------------------

  test("an unrecognized policy type never produces a violation - evaluation is total/safe") {
    val rule = PolicyRule("future", "some_future_type", Map("whatever" -> "value"))
    assert(OrgPolicyEvaluator.evaluate(contract(), OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("a known type with malformed properties never produces a violation either - just no-ops (OrgPolicyValidator's job to catch this)") {
    val rule = PolicyRule("bad", PolicyType.RequireField, Map.empty) // missing required 'name' property
    assert(OrgPolicyEvaluator.evaluate(contract(), OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  // -- applyInjectedRules ---------------------------------------------------

  test("applyInjectedRules is a no-op when the policy injects no rules") {
    val c = contract(rules = List(ContractRule("compatibility", Map("mode" -> "backward"))))
    val result = OrgPolicyEvaluator.applyInjectedRules(c, OrgPolicy("1.0"))
    assert(result == c)
  }

  test("applyInjectedRules appends injected rules to the contract's own rules") {
    val c = contract(rules = List(ContractRule("compatibility", Map("mode" -> "backward"))))
    val policy = OrgPolicy("1.0", inject = InjectedDefaults(rules = List(ContractRule("forbid_unconditional_delete", Map.empty))))
    val result = OrgPolicyEvaluator.applyInjectedRules(c, policy)
    assert(result.rules == List(ContractRule("compatibility", Map("mode" -> "backward")), ContractRule("forbid_unconditional_delete", Map.empty)))
  }

  test("applyInjectedRules does not duplicate a rule the contract already declares") {
    val c = contract(rules = List(ContractRule("forbid_unconditional_delete", Map.empty)))
    val policy = OrgPolicy("1.0", inject = InjectedDefaults(rules = List(ContractRule("forbid_unconditional_delete", Map.empty))))
    val result = OrgPolicyEvaluator.applyInjectedRules(c, policy)
    assert(result.rules == List(ContractRule("forbid_unconditional_delete", Map.empty)))
  }
}
