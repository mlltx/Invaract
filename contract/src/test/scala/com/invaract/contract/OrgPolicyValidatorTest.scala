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

  test("an unrecognized policy type is not an Error - recorded, not interpreted") {
    val policy = OrgPolicy("1.0", List(PolicyRule("custom", "some_future_type", Map("x" -> "y"))))
    val result = OrgPolicyValidator.validate(policy)
    assert(result.isValid)
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
}
