// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

/** `require_absolute_location`: an organization that does not allow relative locations - which match any location
  * ending in them, so cannot tell two teams' `.../orders` apart - says so in its policy. */
class RequireAbsoluteLocationTest extends AnyFunSuite {

  private val now = LocalDate.of(2026, 1, 1)

  private def ds(name: String, location: String) = Dataset(name, location, None, Schema(Nil))

  private def contract(inputs: List[Dataset] = Nil, outputs: List[Dataset] = List(ds("out", "/data/out"))) =
    Contract("c", ContractVersion(1, 0, 0), "active", inputs, outputs, Nil, Map.empty)

  private def rule(properties: Map[String, Any] = Map.empty, scope: PolicyScope = PolicyScope.All, mode: PolicyMode = PolicyMode.Enforce) =
    PolicyRule("absolute", PolicyType.RequireAbsoluteLocation, properties, scope = scope, mode = mode)

  private def violations(c: Contract, r: PolicyRule = rule()) = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(r)), now).allViolations

  test("interpret: no properties allows no catalogs; 'catalogs' is a list or one name; an empty list is malformed") {
    assert(rule().interpret.contains(InterpretedPolicy.RequireAbsoluteLocation(Nil)))
    assert(rule(Map("catalogs" -> java.util.Arrays.asList("spark_catalog", "local"))).interpret.contains(InterpretedPolicy.RequireAbsoluteLocation(List("spark_catalog", "local"))))
    assert(rule(Map("catalogs" -> "spark_catalog")).interpret.contains(InterpretedPolicy.RequireAbsoluteLocation(List("spark_catalog"))))
    assert(rule(Map("catalogs" -> java.util.Collections.emptyList())).interpret.isEmpty)
  }

  test("a malformed 'catalogs' is a validator error that says what was expected; a well-formed rule is not") {
    val malformed = OrgPolicyValidator.validate(OrgPolicy("1.0", List(rule(Map("catalogs" -> java.util.Collections.emptyList())))))
    assert(malformed.errors.map(_.message) == List(
      "Policy type 'require_absolute_location' has malformed or missing properties for its shape " +
        "(if 'catalogs' is set, expected a non-empty list of catalog names - omit it entirely to allow none)"
    ))
    assert(OrgPolicyValidator.validate(OrgPolicy("1.0", List(rule()))).isValid)
  }

  test("absolute locations satisfy it: a URL, a path from the root, a Windows drive path, file:, and an unresolved ref://") {
    List("/data/orders", "s3://bucket/orders", "S3A://bucket/orders", "hdfs://nn:8020/x", "C:\\data\\orders", "file:/data/orders", "ref://orders")
      .foreach(l => assert(violations(contract(outputs = List(ds("out", l)))).isEmpty, l))
  }

  test("a relative location is a violation naming the dataset, the location, and what to write instead") {
    val vs = violations(contract(inputs = List(ds("orders", "sales/orders")), outputs = List(ds("out", "/data/out"))))
    assert(vs.size == 1)
    val v = vs.head
    assert(v.policyId == "absolute" && v.ruleType == "require_absolute_location" && v.mode == PolicyMode.Enforce)
    assert(v.dataset.contains("orders"))
    assert(v.message == "organizational policy 'absolute' requires dataset 'orders' to declare an absolute location (a URL such as " +
      "'s3://bucket/orders', or a path from the root such as '/data/orders'), but 'sales/orders' is relative: it matches any " +
      "location that ends in it.")
    assert(v.remediation == "Write dataset 'orders''s location in full, as the URL or path it is stored at.")
  }

  test("every relative location is reported: inputs and outputs, bare names, ./ and dotted table names") {
    val c = contract(inputs = List(ds("a", "orders"), ds("b", "./x/orders")), outputs = List(ds("c", "gold.orders"), ds("d", "s3://b/ok")))
    assert(violations(c).flatMap(_.dataset) == List("a", "b", "c"))
  }

  test("scope narrows it like any dataset policy, and warn mode reports without enforcing") {
    val c = contract(inputs = List(ds("in", "orders")), outputs = List(ds("out", "/data/out")))
    assert(violations(c, rule(scope = PolicyScope.Outputs)).isEmpty)
    assert(violations(c, rule(scope = PolicyScope.Inputs)).flatMap(_.dataset) == List("in"))
    val warned = OrgPolicyEvaluator.evaluate(c, OrgPolicy("1.0", List(rule(mode = PolicyMode.Warn))), now)
    assert(warned.enforceViolations.isEmpty && warned.allViolations.map(_.mode) == List(PolicyMode.Warn))
  }

  test("catalogs: a table named in full from a listed catalog is allowed, in any case; anything shorter is not") {
    val r = rule(Map("catalogs" -> java.util.Arrays.asList("spark_catalog", "local")))
    def ok(location: String) = violations(contract(outputs = List(ds("out", location))), r).isEmpty
    assert(ok("spark_catalog/gold/orders"))
    assert(ok("SPARK_CATALOG/gold/orders"))
    assert(ok("local/db/t"))
    assert(ok("/data/orders"), "absolute still is")
    assert(!ok("gold/orders"))
    assert(!ok("spark_catalog"), "a catalog alone names no table")
    assert(!ok("other/gold/orders"))
    assert(!ok("spark_catalogue/gold/orders"))
  }

  test("catalogs: the violation says which catalogs a table may be named from") {
    val r = rule(Map("catalogs" -> java.util.Arrays.asList("spark_catalog", "local")))
    val v = violations(contract(outputs = List(ds("out", "gold/orders"))), r).head
    assert(v.message.contains("'/data/orders', or a table named in full from its catalog ('spark_catalog/...', 'local/...')), but 'gold/orders' is relative"))
    assert(v.remediation == "Write dataset 'out''s location in full, as the URL or path it is stored at, or as " +
      "'<catalog>/<namespace>/<table>' with one of: spark_catalog, local.")
  }

  test("parsed from a policy document like any other rule") {
    val policy = OrgPolicyParser.parse(
      """version: "1.0"
        |policies:
        |  - id: no-relative
        |    type: require_absolute_location
        |    catalogs: [spark_catalog]
        |""".stripMargin
    )
    assert(policy.policies.head.interpret.contains(InterpretedPolicy.RequireAbsoluteLocation(List("spark_catalog"))))
  }
}
