// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract._
import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `StaticDataQualityVerifier` — no Spark session
  * needed, since `ir.Plan`/`ir.Expr` and `Contract` are both plain data,
  * mirroring `SensitivityLineageSpec`'s/`PlanRuleVerifierSpec`'s own style.
  * `ir.PropertyAnalysis` itself (the underlying per-column reasoning) has
  * its own direct, example-by-example coverage in
  * `ir.PropertyAnalysisSpec` — these tests exercise the glue this module
  * adds on top: axiom seeding from a matched contract input, output
  * matching, and verdict-to-`DataQualityCheckResult`/`Violation` mapping.
  * Real end-to-end PASS/FAIL coverage through `ContractEnforcementRule`
  * against a live Spark session lives in `ContractEnforcementRuleSpec`.
  */
class StaticDataQualityVerifierSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))
  private def project(input: Plan, name: String, expr: Expr): Plan = Project(input, List(NamedExpr(name, expr)))

  private def dataset(name: String, location: String, fields: Field*): Dataset =
    Dataset(name, location, Some("parquet"), Schema(fields.toList))

  private def contractWith(inputs: List[Dataset], outputs: List[Dataset]): Contract =
    Contract(
      id = "test_contract",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = inputs,
      outputs = outputs,
      rules = Nil,
      extensions = Map.empty
    )

  private def notNullField(name: String) = Field(name, "string", nullable = false)
  private def equalsConstraint(value: Any) = FieldConstraint(FieldConstraintType.Equals, Map("value" -> value))
  private def oneOfConstraint(values: String*) = FieldConstraint(FieldConstraintType.OneOf, Map("values" -> java.util.Arrays.asList(values: _*)))
  private def rangeConstraint(gte: Option[Any] = None, lte: Option[Any] = None) =
    FieldConstraint(FieldConstraintType.Range, (gte.map("gte" -> _) ++ lte.map("lte" -> _)).toMap)
  private def lengthConstraint(exact: Option[Any] = None, min: Option[Any] = None, max: Option[Any] = None) =
    FieldConstraint(FieldConstraintType.Length, (exact.map("exact" -> _) ++ min.map("min" -> _) ++ max.map("max" -> _)).toMap)

  // --- Non-Write / non-matching plans -----------------------------------

  test("verify returns Nil for a plan that isn't a Write at all") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("id"))))
    assert(StaticDataQualityVerifier.verify(contract, Read(DatasetRef("raw.source"))).isEmpty)
  }

  test("verify returns Nil when the write's location matches no declared output") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("id"))))
    val plan = Write(DatasetRef("gold.somewhere_else"), project(Read(DatasetRef("raw.source")), "id", col("id")))
    assert(StaticDataQualityVerifier.verify(contract, plan).isEmpty)
  }

  test("verify picks the correct output among several by location, the same matching StructuralVerifier itself uses") {
    val contract = contractWith(
      Nil,
      List(
        dataset("out1", "gold.out1", notNullField("id")),
        dataset("out2", "gold.out2", Field("id", "string", nullable = true))
      )
    )
    val plan = Write(DatasetRef("gold.out2"), project(Read(DatasetRef("raw.source")), "id", col("id")))
    // out2 declares nullable = true, so no NOT NULL check is even attempted -
    // proving out1's (unrelated) NOT NULL field was NOT the one checked.
    assert(StaticDataQualityVerifier.verify(contract, plan).isEmpty)
  }

  test("checksFor contributes nothing for a nullable field with no constraints - nothing to prove") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("id", "string", nullable = true))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.source")), "id", col("id")))
    assert(StaticDataQualityVerifier.verify(contract, plan).isEmpty)
  }

  // --- NOT NULL (Example 1) ----------------------------------------------

  test("NOT NULL is Guaranteed when a Filter(IS NOT NULL) establishes it with no input axiom at all (Example 1)") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("customer_id"))))
    val filtered = Filter(Read(DatasetRef("raw.orders")), Function("ISNOTNULL", List(col("customer_id"))))
    val plan = Write(DatasetRef("gold.out"), project(filtered, "customer_id", col("customer_id")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("customer_id", "NOT NULL", DataQualityVerdict.Guaranteed)))
    assert(StaticDataQualityVerifier.violations(results).isEmpty)
  }

  test("NOT NULL is NotGuaranteed, not Violated or a false Guaranteed, with no filter at all") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("customer_id"))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "customer_id", col("customer_id")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("customer_id", "NOT NULL", DataQualityVerdict.NotGuaranteed)))
    assert(StaticDataQualityVerifier.violations(results).isEmpty)
  }

  test("NOT NULL is NotStaticallyVerifiable, not NotGuaranteed, when the column derives from a UDF") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("x"))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "x", UDF(Some("myFn"), List(col("customer_id")))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("x", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("NOT NULL is Violated, and becomes a real Violation, when the plan proves a null literal reaches the output") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", notNullField("x"))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "x", Literal(null, "string")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("x", "NOT NULL", DataQualityVerdict.Violated)))

    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.DataQualityViolation)
    assert(violations.head.column.contains("x"))
  }

  // --- Propagation through an axiom seeded from a matched input (Example 5) ---

  test("NOT NULL propagates from a matched contract input's own nullable = false, through a pure passthrough (Example 5)") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", notNullField("customer_id"))),
      List(dataset("out", "gold.out", notNullField("customer_id")))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      project(Read(DatasetRef("raw.orders")), "customer_id", col("customer_id", Some("raw.orders")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("customer_id", "NOT NULL", DataQualityVerdict.Guaranteed)))
  }

  test("axiom seeding matches a contract's declared (relative) input location against the plan's actual (absolute file:) location") {
    val contract = contractWith(
      List(dataset("orders", "demo/input/sample.csv", notNullField("value"))),
      List(dataset("out", "gold.out", notNullField("value")))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      project(Read(DatasetRef("file:/home/user/Invaract/demo/input/sample.csv")), "value", col("value", Some("file:/home/user/Invaract/demo/input/sample.csv")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("value", "NOT NULL", DataQualityVerdict.Guaranteed)))
  }

  test("an axiom is never seeded for an input the plan doesn't actually read under a matching scope") {
    // The contract declares customer_id non-null on an input the plan never reads (a self-join alias, "cur",
    // that doesn't match any declared input's location) - proving the axiom map is built from the plan's
    // REAL Read scopes, not naively keyed by the contract's own declared location string.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", notNullField("customer_id"))),
      List(dataset("out", "gold.out", notNullField("customer_id")))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      project(Read(DatasetRef("raw.orders"), alias = Some("cur")), "customer_id", col("customer_id", Some("cur")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("customer_id", "NOT NULL", DataQualityVerdict.Guaranteed)),
      "an explicit alias still resolves against the input's location via locationsMatch, exactly like StructuralVerifier's own input matching")
  }

  test("NOT NULL is NotGuaranteed, not Guaranteed, when the matched input field is itself nullable (no axiom to seed)") {
    // Mirrors the "propagates from a matched input's nullable = false" test above, but with the
    // input field left nullable (the default) - proving fieldAxiomState doesn't seed a Proven
    // axiom for every input field regardless of its own declared nullability.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("customer_id", "long"))),
      List(dataset("out", "gold.out", notNullField("customer_id")))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      project(Read(DatasetRef("raw.orders")), "customer_id", col("customer_id", Some("raw.orders")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("customer_id", "NOT NULL", DataQualityVerdict.NotGuaranteed)))
  }

  // --- EqualsConstant (Example 2) -----------------------------------------

  test("an equals constraint is Guaranteed when the plan produces exactly that constant (Example 2)") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("currency", "string", constraints = List(equalsConstraint("GBP"))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "currency", Literal("GBP", "string")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("currency", "= GBP", DataQualityVerdict.Guaranteed)))
  }

  test("an equals constraint is Violated, and becomes a real Violation, when the plan proves a different constant") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("currency", "string", constraints = List(equalsConstraint("GBP"))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "currency", Literal("USD", "string")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("currency", "= GBP", DataQualityVerdict.Violated)))
    assert(StaticDataQualityVerifier.violations(results).size == 1)
  }

  test("an equals constraint is NotGuaranteed, not Violated, for a plain passthrough with no axiom at all (Example 2's negative case)") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("currency", "string", constraints = List(equalsConstraint("GBP"))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "currency", col("currency")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("currency", "= GBP", DataQualityVerdict.NotGuaranteed)))
  }

  test("an equals constraint is NotStaticallyVerifiable, not NotGuaranteed, when the column derives from a UDF") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("currency", "string", constraints = List(equalsConstraint("GBP"))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "currency", UDF(Some("myFn"), List(col("raw_currency")))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("currency", "= GBP", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  // --- OneOf (Example 3) --------------------------------------------------

  test("a oneOf constraint is Guaranteed from a CASE WHEN with an ELSE, both branches constants (Example 3)") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("status", "string", constraints = List(oneOfConstraint("ACTIVE", "INACTIVE"))))))
    val cond = Conditional(List((col("is_active"), Literal("ACTIVE", "string"))), Some(Literal("INACTIVE", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "status", cond))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("status", "IN (ACTIVE, INACTIVE)", DataQualityVerdict.Guaranteed)))
  }

  test("a oneOf constraint is Violated when a proven value set escapes the declared allowed values") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("status", "string", constraints = List(oneOfConstraint("ACTIVE", "INACTIVE"))))))
    val cond = Conditional(List((col("is_active"), Literal("ACTIVE", "string"))), Some(Literal("PENDING", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "status", cond))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("status", "IN (ACTIVE, INACTIVE)", DataQualityVerdict.Violated)))
  }

  // --- Range (Examples 4 and 7) -------------------------------------------

  test("a range constraint is Guaranteed from a CASE WHEN clamp, with no input axiom at all (Example 4)") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0)))))))
    val branches = List((Comparison("<", col("amount"), Literal(0, "integer")), Literal(0, "integer")))
    val cond = Conditional(branches, Some(col("amount")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", cond))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", ">= 0", DataQualityVerdict.Guaranteed)))
  }

  test("a range constraint is Violated when negating a proven non-negative input axiom crosses the required lower bound (Example 7)") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      project(Read(DatasetRef("raw.orders")), "amount", Arithmetic("NEGATE", List(col("amount", Some("raw.orders")))))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", ">= 0", DataQualityVerdict.Violated)))
    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.DataQualityViolation)
  }

  test("a range constraint sitting exactly on the required boundary is NotGuaranteed, never a false Violated (conservative tie-break)") {
    // required gt=0 (strict), proven axiom gte=0 (inclusive): p does not provably escape required
    // (its lower bound 0 is not < required's lower bound 0), but p also isn't proven to satisfy the
    // strict bound either (tighten(required) would push p's lower bound to gt=0, which != p's own gte=0).
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("gte" -> 0)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("gt" -> 0))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", col("amount", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", "> 0", DataQualityVerdict.NotGuaranteed)))
  }

  test("a range constraint is NotStaticallyVerifiable, not NotGuaranteed, when the column derives from a UDF") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0)))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", UDF(Some("myFn"), List(col("raw_amount")))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", ">= 0", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a range constraint is Violated when both sides have a genuine lower bound and the proven one is strictly looser") {
    // Both proven and required carry only a lower bound (no upper bound on either side), so this
    // exercises escapesBelow's real (Some, Some) numeric comparison directly, distinct from the
    // boundary-tie test above (which ties exactly and never distinguishes < from <= or >).
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(-10)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", col("amount", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", ">= 0", DataQualityVerdict.Violated)))
  }

  test("a range constraint is Violated when the proven side is unbounded above but the required side is bounded") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(rangeConstraint(gte = Some(0)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("lte" -> 10))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", col("amount", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", "<= 10", DataQualityVerdict.Violated)))
  }

  test("a range constraint is Violated when both sides have a genuine upper bound and the proven one is strictly looser") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("lte" -> 100)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("lte" -> 50))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", col("amount", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", "<= 50", DataQualityVerdict.Violated)))
  }

  test("an upper-bound tie between an exclusive proven bound's value and an inclusive required bound is NotGuaranteed, not Violated") {
    // Both sides' upper bound resolve to the same raw number (50), but proven is `lte` (inclusive)
    // while required is `lt` (exclusive) - escapesAbove's plain numeric comparison ties (50 > 50 is
    // false), the same tie-conservative principle the lower-bound boundary test above documents,
    // this time exercising escapesAbove's (Some, Some) branch specifically rather than escapesBelow's.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("lte" -> 50)))))),
      List(dataset("out", "gold.out", Field("amount", "integer", constraints = List(FieldConstraint(FieldConstraintType.Range, Map("lt" -> 50))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "amount", col("amount", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("amount", "< 50", DataQualityVerdict.NotGuaranteed)))
  }

  // --- Length constraints (§3.9) -------------------------------------------

  test("a length constraint is Guaranteed from a string literal with the required exact length, with no input axiom at all") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("code", "string", constraints = List(lengthConstraint(exact = Some(10)))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "code", Literal("ABCDEFGHIJ", "string")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("code", "LENGTH = 10", DataQualityVerdict.Guaranteed)))
  }

  test("a length constraint propagates from a matched contract input's own declared length, through a pure passthrough (Example 5's shape)") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("identifier", "string", constraints = List(lengthConstraint(exact = Some(10)))))),
      List(dataset("out", "gold.out", Field("identifier", "string", constraints = List(lengthConstraint(exact = Some(10))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "identifier", col("identifier", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("identifier", "LENGTH = 10", DataQualityVerdict.Guaranteed)))
  }

  test("a length constraint is Guaranteed when a proven min/max envelope is fully within a wider required min/max") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(min = Some(5), max = Some(10)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(min = Some(1), max = Some(50))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH >= 1 and LENGTH <= 50", DataQualityVerdict.Guaranteed)))
  }

  test("a length constraint is NotGuaranteed, not Violated, for a plain passthrough with no axiom at all") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(exact = Some(10)))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH = 10", DataQualityVerdict.NotGuaranteed)))
  }

  test("a length constraint is NotStaticallyVerifiable, not NotGuaranteed, when the column derives from a UDF") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(exact = Some(10)))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", UDF(Some("myFn"), List(col("raw_name")))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH = 10", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a length constraint is Violated when both sides have a genuine min and the proven one is strictly looser") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(min = Some(1)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(min = Some(5))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH >= 5", DataQualityVerdict.Violated)))
  }

  test("a length constraint is Violated when the proven side is unbounded above but the required side is bounded") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(min = Some(0)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(max = Some(20))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH <= 20", DataQualityVerdict.Violated)))
  }

  test("a length constraint is Violated when the proven side is unbounded below but the required side has a genuine lower bound") {
    // The direct mirror of the "unbounded above" test above, isolating
    // lengthEscapesBelow's own (None, Some(_)) => true case specifically:
    // required's upper is left unset so escapesAbove independently reports
    // no escape, meaning only escapesBelow's own correctness can be what
    // drives this Violated verdict.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(max = Some(20)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(min = Some(5))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH >= 5", DataQualityVerdict.Violated)))
  }

  test("a length constraint's strict escape checks (not just the boundary tie) are exercised on both sides by a denormalized min/max proven state") {
    // A contract can declare an input's own length as `{min: 10, max: 10}`
    // instead of `{exact: 10}` - both interpret to the same real value
    // domain, but Property.Length keeps them as genuinely distinct case
    // class shapes (min/max is never auto-collapsed to exact once built -
    // only Length.tighten/widen's own fromBounds does that normalization).
    // Against an `exact: 10` requirement, this denormalized {min:10,max:10}
    // proven state fails the `tighten(required) == p` structural-equality
    // check even though the real numeric envelopes are identical, forcing
    // resolution through lengthEscapesBelow/lengthEscapesAbove - both of
    // which must correctly report "no escape" (10 is not < 10, and not >
    // 10) for the verdict to be the honest NotGuaranteed it should be,
    // rather than a false Violated.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(min = Some(10), max = Some(10)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(exact = Some(10))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH = 10", DataQualityVerdict.NotGuaranteed)))
  }

  test("a length constraint is Violated when both sides have a genuine max and the proven one is strictly looser") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(max = Some(100)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(max = Some(50))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH <= 50", DataQualityVerdict.Violated)))
  }

  test("a length constraint sitting exactly on the required boundary is Guaranteed, an exact tie is a real proof (unlike Range's exclusive/inclusive tie)") {
    // Length has no gt/lt exclusive variant, so an exact numeric tie between the proven and
    // required bound genuinely proves the constraint - deliberately distinct from rangeVerdict's
    // own boundary-tie test, which stays NotGuaranteed only because Range's exclusive/inclusive
    // distinction makes an equal-valued tie NOT a proof there.
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("name", "string", constraints = List(lengthConstraint(min = Some(5)))))),
      List(dataset("out", "gold.out", Field("name", "string", constraints = List(lengthConstraint(min = Some(5))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "name", col("name", Some("raw.orders"))))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("name", "LENGTH >= 5", DataQualityVerdict.Guaranteed)))
  }

  test("a length constraint's Violated verdict becomes a real Violation, the same as any other constraint kind") {
    val contract = contractWith(Nil, List(dataset("out", "gold.out", Field("code", "string", constraints = List(lengthConstraint(exact = Some(5)))))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "code", Literal("TOOLONG", "string")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("code", "LENGTH = 5", DataQualityVerdict.Violated)))
    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.DataQualityViolation)
  }

  // --- Struct/nested fields (checksForField's recursion) -------------------

  test("a NOT NULL constraint on a nested struct field is NotStaticallyVerifiable, not silently skipped") {
    val nested = Field("zip", "string", nullable = false)
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", col("address")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.zip", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a value constraint on a nested struct field is NotStaticallyVerifiable, with a dotted field path") {
    val nested = Field("country", "string", constraints = List(equalsConstraint("US")))
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", col("address")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.country", "= US", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("recursion into nested fields goes arbitrarily deep, dotting the full path") {
    val leaf = Field("code", "string", nullable = false)
    val mid = Field("geo", "struct", properties = List(leaf))
    val top = Field("address", "struct", properties = List(mid))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", top)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", col("address")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.geo.code", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a struct field's own NOT NULL check still uses real top-level analysis, unaffected by nested recursion") {
    val nested = Field("zip", "string") // nullable = true, no constraints of its own - contributes nothing
    val struct = Field("address", "struct", nullable = false, properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val filtered = Filter(Read(DatasetRef("raw.orders")), Function("ISNOTNULL", List(col("address"))))
    val plan = Write(DatasetRef("gold.out"), project(filtered, "address", col("address")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address", "NOT NULL", DataQualityVerdict.Guaranteed)))
  }

  test("a struct field with no declared constraints of its own, and no constrained nested fields, contributes nothing") {
    val nested = Field("zip", "string") // nullable = true (default), no constraints
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", col("address")))

    assert(StaticDataQualityVerifier.verify(contract, plan).isEmpty)
  }

  test("only constrained nested fields produce entries; unconstrained siblings are skipped, in declaration order") {
    val zip = Field("zip", "string", nullable = false)
    val city = Field("city", "string") // nothing to prove
    val country = Field("country", "string", constraints = List(equalsConstraint("US")))
    val struct = Field("address", "struct", properties = List(zip, city, country))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", col("address")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(
      DataQualityCheckResult("address.zip", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable),
      DataQualityCheckResult("address.country", "= US", DataQualityVerdict.NotStaticallyVerifiable)
    ))
  }

  test("a top-level field built by struct(...) is itself provably NOT NULL via the new StructConstruct resolution, even though its own nested fields stay NotStaticallyVerifiable") {
    val nested = Field("zip", "string", nullable = false)
    val struct = Field("address", "struct", nullable = false, properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // The top-level NOT NULL is now a real Guaranteed (a freshly-constructed
    // struct is provably non-null - ir.PropertyAnalysis's new StructConstruct
    // case), while the nested "address.zip" check stays
    // NotStaticallyVerifiable: checksForField's own recursion into
    // Field.properties is deliberately unconnected to this - see its own doc
    // for why (no axiom representation for a struct's internal fields).
    assert(results == List(
      DataQualityCheckResult("address", "NOT NULL", DataQualityVerdict.Guaranteed),
      DataQualityCheckResult("address.zip", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)
    ))
  }

  test("a top-level field that extracts one of its own just-constructed fields is checked with a real, proven verdict") {
    // e.g. `struct(col("zip"), col("city")).getField("zip").as("just_zip")` -
    // a flat (non-nested-in-the-contract) output field whose own expression
    // happens to be StructField(StructConstruct(...), name); this is exactly
    // the "construct, then extract, in the same plan" pattern
    // ir.PropertyAnalysis's new resolution traces through.
    val field = Field("just_zip", "string", constraints = List(equalsConstraint("94107")))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", field)))
    val built = StructConstruct(List("zip" -> Literal("94107", "string"), "city" -> Literal("SF", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "just_zip", StructField(built, "zip")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("just_zip", "= 94107", DataQualityVerdict.Guaranteed)))
  }

  // --- violations() itself -------------------------------------------------

  test("violations extracts only the Violated entries, in order, leaving Guaranteed/NotGuaranteed/NotStaticallyVerifiable out") {
    val results = List(
      DataQualityCheckResult("a", "NOT NULL", DataQualityVerdict.Guaranteed),
      DataQualityCheckResult("b", "NOT NULL", DataQualityVerdict.Violated),
      DataQualityCheckResult("c", "NOT NULL", DataQualityVerdict.NotGuaranteed),
      DataQualityCheckResult("d", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable),
      DataQualityCheckResult("e", "= GBP", DataQualityVerdict.Violated)
    )
    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.map(_.column) == List(Some("b"), Some("e")))
    assert(violations.forall(_.violationType == ViolationType.DataQualityViolation))
  }
}
