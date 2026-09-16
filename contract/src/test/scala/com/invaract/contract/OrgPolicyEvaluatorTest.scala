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

  private def contract(
      inputs: List[Dataset] = Nil,
      outputs: List[Dataset] = List(dataset("out")),
      rules: List[ContractRule] = Nil,
      extensions: Map[String, Any] = Map.empty
  ): Contract =
    Contract("test_contract", ContractVersion(1, 0, 0), "active", inputs, outputs, rules, extensions)

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
    val rule = PolicyRule("typed", PolicyType.RequireField, Map("name" -> "id", "fieldType" -> "STRING"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out", fields = List(field("id")))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_field: type pin violated on mismatch") {
    val rule = PolicyRule("typed", PolicyType.RequireField, Map("name" -> "id", "fieldType" -> "integer"), scope = PolicyScope.Outputs)
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

  // -- require_format ---------------------------------------------------------

  test("require_format: satisfied when the dataset's format matches, single scalar shorthand") {
    val rule = PolicyRule("delta-only", PolicyType.RequireFormat, Map("formats" -> "delta"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(format = Some("delta"))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_format: satisfied when the dataset's format matches any entry in a list") {
    val rule =
      PolicyRule("lakehouse-only", PolicyType.RequireFormat, Map("formats" -> List("delta", "iceberg")), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(format = Some("iceberg"))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_format: matched case-insensitively") {
    val rule = PolicyRule("delta-only", PolicyType.RequireFormat, Map("formats" -> "Delta"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(format = Some("DELTA"))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_format: violated when the dataset's format is not in the allowed list") {
    val rule =
      PolicyRule("lakehouse-only", PolicyType.RequireFormat, Map("formats" -> List("delta", "iceberg")), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(format = Some("parquet"))))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.message.contains("parquet"))
    assert(violations.head.dataset.contains("out"))
  }

  test("require_format: violated when the dataset declares no format at all") {
    val rule = PolicyRule("delta-only", PolicyType.RequireFormat, Map("formats" -> "delta"), scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(format = None)))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.message.contains("no format declared at all"))
  }

  test("require_format: an empty formats list is malformed - interpret returns None, no violation raised") {
    val rule = PolicyRule("empty-list", PolicyType.RequireFormat, Map("formats" -> List.empty[String]), scope = PolicyScope.Outputs)
    assert(rule.interpret.isEmpty)
    val c = contract(outputs = List(dataset("out").copy(format = None)))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  // -- require_dataset_description ---------------------------------------------

  test("require_dataset_description: satisfied when the dataset declares a non-blank description") {
    val rule = PolicyRule("described", PolicyType.RequireDatasetDescription, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(description = Some("Customer order records."))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_dataset_description: violated when the dataset declares no description at all") {
    val rule = PolicyRule("described", PolicyType.RequireDatasetDescription, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(description = None)))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.dataset.contains("out"))
  }

  test("require_dataset_description: a blank/whitespace-only description does not satisfy the rule") {
    val rule = PolicyRule("described", PolicyType.RequireDatasetDescription, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(outputs = List(dataset("out").copy(description = Some("   "))))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_dataset_description: honors scope like any other DatasetPolicy") {
    val rule = PolicyRule("described", PolicyType.RequireDatasetDescription, Map.empty, scope = PolicyScope.Outputs)
    val c = contract(
      inputs = List(dataset("in")), // no description - would violate if inputs were in scope
      outputs = List(dataset("out").copy(description = Some("Documented.")))
    )
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
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

  // -- require_extension ----------------------------------------------------

  test("require_extension: satisfied when the contract declares the key with any value") {
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))
    val c = contract(extensions = Map("owner" -> "data-platform-team"))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_extension: violated when the contract's extensions don't declare the key at all") {
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))
    val c = contract(extensions = Map.empty)
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.dataset.isEmpty, "a contract-level violation names no specific dataset")
    assert(violations.head.message.contains("owner"))
  }

  test("require_extension: a key present but mapped to YAML null is treated as not declared") {
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))
    val c = contract(extensions = Map("owner" -> null))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_extension: an unrelated extensions key does not satisfy the rule") {
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))
    val c = contract(extensions = Map("team" -> "data-platform"))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_extension: value pin satisfied only on an exact, case-sensitive match") {
    val rule = PolicyRule("status-active", PolicyType.RequireExtension, Map("key" -> "status", "value" -> "active"))
    val matching = contract(extensions = Map("status" -> "active"))
    val mismatching = contract(extensions = Map("status" -> "Active"))

    assert(OrgPolicyEvaluator.evaluate(matching, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
    assert(OrgPolicyEvaluator.evaluate(mismatching, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_extension: is checked once against the contract, not once per dataset - scope has no effect") {
    // Two outputs, zero inputs - if this were mistakenly routed through the
    // per-dataset path (datasetsInScope), scope: outputs would still see 2
    // datasets and could produce 2 violations, or scope: inputs would see
    // zero datasets and vacuously produce none. Either would be wrong: this
    // must always produce exactly one violation, from the contract itself.
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"), scope = PolicyScope.Inputs)
    val c = contract(outputs = List(dataset("out1"), dataset("out2")), extensions = Map.empty)
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
  }

  test("require_extension: an exemption still suppresses it, the same as any other policy type") {
    val rule = PolicyRule("owner-required", PolicyType.RequireExtension, Map("key" -> "owner"))
    val c = contract(extensions = Map.empty)
    val policy = OrgPolicy("1.0", List(rule), exemptions = List(PolicyExemption("test_contract", List("owner-required"), "legacy")))
    assert(OrgPolicyEvaluator.evaluate(c, policy, now).allViolations.isEmpty)
  }

  // -- require_extension_if --------------------------------------------------

  private def sunsetRule(ifValue: Map[String, Any] = Map("ifValue" -> "deprecated")): PolicyRule =
    PolicyRule(
      "sunset-date-if-deprecated",
      PolicyType.RequireExtensionIf,
      Map("ifKey" -> "status", "thenKey" -> "sunsetDate") ++ ifValue
    )

  test("require_extension_if: the 'if' condition not holding at all means no violation, regardless of 'then'") {
    val rule = sunsetRule()
    val c = contract(extensions = Map("status" -> "active")) // ifValue mismatch - condition doesn't hold
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_extension_if: 'if' condition absent entirely also means no violation") {
    val rule = sunsetRule()
    val c = contract(extensions = Map.empty) // no 'status' key at all
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_extension_if: 'if' holds and 'then' is present - satisfied") {
    val rule = sunsetRule()
    val c = contract(extensions = Map("status" -> "deprecated", "sunsetDate" -> "2026-12-01"))
    assert(OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
  }

  test("require_extension_if: 'if' holds and 'then' is absent - violated, contract-level (no dataset)") {
    val rule = sunsetRule()
    val c = contract(extensions = Map("status" -> "deprecated"))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
    assert(violations.head.dataset.isEmpty, "a contract-level violation names no specific dataset")
    assert(violations.head.message.contains("sunsetDate"))
    assert(violations.head.message.contains("status"))
  }

  test("require_extension_if: no ifValue means any non-null ifKey value trips the condition") {
    val rule = sunsetRule(ifValue = Map.empty) // no ifValue - any status at all triggers it
    val c = contract(extensions = Map("status" -> "anything"))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1, "thenKey (sunsetDate) is still absent, so the condition being met should still violate")
  }

  test("require_extension_if: thenValue pin is checked exactly, like require_extension's own value pin") {
    val rule = PolicyRule(
      "active-requires-reviewed-status",
      PolicyType.RequireExtensionIf,
      Map("ifKey" -> "status", "ifValue" -> "active", "thenKey" -> "reviewStatus", "thenValue" -> "approved")
    )
    val approved = contract(extensions = Map("status" -> "active", "reviewStatus" -> "approved"))
    val pending = contract(extensions = Map("status" -> "active", "reviewStatus" -> "pending"))
    assert(OrgPolicyEvaluator.evaluate(approved, OrgPolicy("1.0", List(rule)), now).allViolations.isEmpty)
    assert(OrgPolicyEvaluator.evaluate(pending, OrgPolicy("1.0", List(rule)), now).allViolations.size == 1)
  }

  test("require_extension_if: is checked once against the contract, not once per dataset - scope has no effect") {
    val rule = sunsetRule().copy(scope = PolicyScope.Inputs)
    val c = contract(outputs = List(dataset("out1"), dataset("out2")), extensions = Map("status" -> "deprecated"))
    val violations = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule)), now).allViolations
    assert(violations.size == 1)
  }

  test("require_extension_if: an exemption still suppresses it, the same as any other policy type") {
    val rule = sunsetRule()
    val c = contract(extensions = Map("status" -> "deprecated"))
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      exemptions = List(PolicyExemption("test_contract", List("sunset-date-if-deprecated"), "legacy"))
    )
    assert(OrgPolicyEvaluator.evaluate(c, policy, now).allViolations.isEmpty)
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

  // -- expiringExemptions --------------------------------------------------

  private def exemption(contractId: String, reviewBy: Option[LocalDate]): PolicyExemption =
    PolicyExemption(contractId, List("some-policy"), "reason", reviewBy)

  test("expiringExemptions: empty when the policy has no exemptions at all") {
    assert(OrgPolicyEvaluator.expiringExemptions(OrgPolicy("1.0"), withinDays = 30, now).isEmpty)
  }

  test("expiringExemptions: an exemption with no reviewBy never appears, however wide the window") {
    val policy = OrgPolicy("1.0", exemptions = List(exemption("c", None)))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 36500, now).isEmpty)
  }

  test("expiringExemptions: an already-past-due exemption does not appear (that's OrgPolicyValidator's Warning, not a look-ahead)") {
    val policy = OrgPolicy("1.0", exemptions = List(exemption("c", Some(now.minusDays(1)))))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 30, now).isEmpty)
  }

  test("expiringExemptions: an exemption expiring exactly today is included at the lower boundary") {
    val policy = OrgPolicy("1.0", exemptions = List(exemption("c", Some(now))))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 0, now) == List(policy.exemptions.head))
  }

  test("expiringExemptions: an exemption expiring exactly on the horizon is included at the upper boundary") {
    val policy = OrgPolicy("1.0", exemptions = List(exemption("c", Some(now.plusDays(30)))))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 30, now) == List(policy.exemptions.head))
  }

  test("expiringExemptions: an exemption expiring one day past the horizon is excluded") {
    val policy = OrgPolicy("1.0", exemptions = List(exemption("c", Some(now.plusDays(31)))))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 30, now).isEmpty)
  }

  test("expiringExemptions: results are ordered by reviewBy ascending, soonest first") {
    val soon = exemption("a", Some(now.plusDays(5)))
    val sooner = exemption("b", Some(now.plusDays(1)))
    val later = exemption("c", Some(now.plusDays(20)))
    val policy = OrgPolicy("1.0", exemptions = List(soon, later, sooner))
    assert(OrgPolicyEvaluator.expiringExemptions(policy, withinDays = 30, now) == List(sooner, soon, later))
  }

  // -- custom policy types (CustomPolicyEvaluator / customPolicyTypes) -----

  private val customClassName = classOf[ContractIdMustBeLowercaseEvaluator].getName

  test("a ruleType not in PolicyType.All, with a customPolicyTypes entry, dispatches to the named evaluator") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> customClassName))
    val violating = contract().copy(id = "Mixed_Case_Id")
    val eval = OrgPolicyEvaluator.evaluate(violating, policy, now)
    assert(eval.enforceViolations.size == 1)
    assert(eval.enforceViolations.head.policyId == "id-lowercase")
    assert(eval.enforceViolations.head.ruleType == "require_lowercase_id")
  }

  test("a custom evaluator producing no violations is reflected as no violations") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> customClassName))
    val satisfying = contract().copy(id = "already_lowercase")
    assert(OrgPolicyEvaluator.evaluate(satisfying, policy, now).allViolations.isEmpty)
  }

  test("a custom type's violation honors rule.mode, split into warnViolations under Warn") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty, mode = PolicyMode.Warn)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("require_lowercase_id" -> customClassName))
    val violating = contract().copy(id = "Mixed_Case_Id")
    val eval = OrgPolicyEvaluator.evaluate(violating, policy, now)
    assert(eval.enforceViolations.isEmpty)
    assert(eval.warnViolations.size == 1)
  }

  test("an unexpired exemption suppresses a custom type's violation the same as a built-in one") {
    val rule = PolicyRule("id-lowercase", "require_lowercase_id", Map.empty)
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      exemptions = List(PolicyExemption("Mixed_Case_Id", List("id-lowercase"), "reason")),
      customPolicyTypes = Map("require_lowercase_id" -> customClassName)
    )
    val violating = contract().copy(id = "Mixed_Case_Id")
    assert(OrgPolicyEvaluator.evaluate(violating, policy, now).allViolations.isEmpty)
  }

  test("a ruleType with no customPolicyTypes entry and no built-in match produces no violation (not a crash)") {
    val rule = PolicyRule("mystery", "totally_unrecognized_type", Map.empty)
    val policy = OrgPolicy("1.0", List(rule))
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.isEmpty)
  }

  test("a customPolicyTypes entry naming an unresolvable class produces no violation, does not throw") {
    val rule = PolicyRule("broken", "totally_unrecognized_type", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("totally_unrecognized_type" -> "com.invaract.contract.NoSuchClassAtAll"))
    assert(OrgPolicyEvaluator.evaluate(contract(), policy, now).allViolations.isEmpty)
  }

  test("a customPolicyTypes entry duplicating a built-in ruleType is inert - the built-in interpretation wins") {
    val rule = PolicyRule("catalog-required", PolicyType.RequireCatalog, Map.empty, scope = PolicyScope.Outputs)
    // Points require_catalog at an evaluator that always violates for every dataset in scope - if this were
    // ever consulted, the un-cataloged "out" dataset below would produce a violation via it too, on top of the
    // built-in check's own. It must not be: rule.interpret is Some(...) for a well-formed require_catalog rule,
    // so evaluateRule never even looks at customPolicyTypes for this rule.
    val policy = OrgPolicy(
      "1.0",
      List(rule),
      customPolicyTypes = Map(PolicyType.RequireCatalog -> classOf[AlwaysViolatesPerDatasetInScopeEvaluator].getName)
    )
    val eval = OrgPolicyEvaluator.evaluate(contract(outputs = List(dataset("out"))), policy, now)
    assert(eval.allViolations.size == 1) // the built-in require_catalog violation only, not a second one from the custom evaluator
  }

  test("an exception thrown by a custom evaluator's own evaluate() propagates, rather than being swallowed") {
    val rule = PolicyRule("throws", "throwing_type", Map.empty)
    val policy = OrgPolicy("1.0", List(rule), customPolicyTypes = Map("throwing_type" -> classOf[ThrowingCustomPolicyEvaluator].getName))
    intercept[RuntimeException] {
      OrgPolicyEvaluator.evaluate(contract(), policy, now)
    }
  }
}
