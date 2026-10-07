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
  private def fieldRangeConstraint(gte: Option[String] = None, gt: Option[String] = None, lte: Option[String] = None, lt: Option[String] = None) =
    FieldConstraint(FieldConstraintType.FieldRange, (gte.map("gte" -> _) ++ gt.map("gt" -> _) ++ lte.map("lte" -> _) ++ lt.map("lt" -> _)).toMap)
  private def projectFields(input: Plan, fields: (String, Expr)*): Plan = Project(input, fields.map { case (n, e) => NamedExpr(n, e) }.toList)

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

  // --- FieldRange constraints (cross-field/row-level) -----------------------

  test("a fieldRange gte constraint is Guaranteed when the field's own proven value is strictly above the sibling's") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> Literal(20L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= start_date", DataQualityVerdict.Guaranteed)))
  }

  test("a fieldRange gte constraint is Guaranteed at an exact tie - the boundary is inclusive") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> Literal(10L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= start_date", DataQualityVerdict.Guaranteed)))
  }

  test("a fieldRange gte constraint is Violated when the field's own proven value is strictly below the sibling's") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> Literal(5L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= start_date", DataQualityVerdict.Violated)))
    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.map(_.column) == List(Some("end_date")), "the violation is attributed to the constrained field, not the sibling it's compared against")
  }

  test("a fieldRange gt constraint is Violated at an exact tie - the bound is strict, unlike gte") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gt = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> Literal(10L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", "> start_date", DataQualityVerdict.Violated)))
  }

  test("a fieldRange gt constraint is Guaranteed when strictly above, and not Guaranteed merely by tying") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gt = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> Literal(11L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", "> start_date", DataQualityVerdict.Guaranteed)))
  }

  test("a fieldRange lte constraint is Guaranteed - the bound direction is flipped relative to gte") {
    // Field being checked is "start_date" here, referencing "end_date" - proves lte reuses
    // gteClause with the two sides swapped, not merely gte's own code path by coincidence.
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("end_date", "long"), Field("start_date", "long", constraints = List(fieldRangeConstraint(lte = Some("end_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "end_date" -> Literal(20L, "long"), "start_date" -> Literal(10L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("start_date", "<= end_date", DataQualityVerdict.Guaranteed)))
  }

  test("a fieldRange lte constraint is Violated when the field's own proven value exceeds the sibling's") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("end_date", "long"), Field("start_date", "long", constraints = List(fieldRangeConstraint(lte = Some("end_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "end_date" -> Literal(20L, "long"), "start_date" -> Literal(25L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("start_date", "<= end_date", DataQualityVerdict.Violated)))
  }

  test("a fieldRange lt constraint is Violated at an exact tie, mirroring gt's own strictness for the flipped direction") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("end_date", "long"), Field("start_date", "long", constraints = List(fieldRangeConstraint(lt = Some("end_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "end_date" -> Literal(20L, "long"), "start_date" -> Literal(20L, "long"))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("start_date", "< end_date", DataQualityVerdict.Violated)))
  }

  test("a fieldRange constraint is NotGuaranteed, not a false Guaranteed or Violated, when only one side's bound is known") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("start_date", "long", constraints = List(rangeConstraint(gte = Some(0)))))),
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))))
    )
    // start_date only has a proven LOWER bound (no upper); end_date has no proven bound at all -
    // gteClause needs end_date's own lower AND start_date's own upper, neither of which is known.
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(
        Read(DatasetRef("raw.orders")),
        "start_date" -> col("start_date", Some("raw.orders")),
        "end_date" -> col("end_date", Some("raw.orders"))
      )
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= start_date", DataQualityVerdict.NotGuaranteed)))
  }

  test("a fieldRange constraint is NotStaticallyVerifiable, not NotGuaranteed, when the constrained field itself derives from a UDF") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(10L, "long"), "end_date" -> UDF(Some("myFn"), Nil))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= start_date", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a fieldRange constraint is NotStaticallyVerifiable when it references a field name absent from the output entirely") {
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("nonexistent"))))))
    )
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "end_date", Literal(10L, "long")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("end_date", ">= nonexistent", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a fieldRange gte constraint's violated check is a strict comparison, not a tie, over non-degenerate proven ranges") {
    // Both point-literal tests above always have a proven lower == upper on each
    // side, so they can't distinguish `<` from `<=` in the violated check - this
    // uses genuinely distinct lower/upper bounds on each side instead.
    val contract = contractWith(
      List(dataset(
        "orders",
        "raw.orders",
        Field("a", "long", constraints = List(rangeConstraint(gte = Some(5), lte = Some(10)))),
        Field("b", "long", constraints = List(rangeConstraint(gte = Some(10), lte = Some(20))))
      )),
      List(dataset("out", "gold.out", Field("b", "long"), Field("a", "long", constraints = List(fieldRangeConstraint(gte = Some("b"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "a" -> col("a", Some("raw.orders")), "b" -> col("b", Some("raw.orders")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // a's own proven upper bound (10) exactly ties b's own proven lower bound (10) -
    // not a proven escape (a could be 10 and b could be 10, satisfying a >= b) -
    // must be NotGuaranteed, not a false Violated.
    assert(results == List(DataQualityCheckResult("a", ">= b", DataQualityVerdict.NotGuaranteed)))
  }

  test("a fieldRange gt constraint's violated check is non-strict, distinguishing it from an equality or a reversed comparison") {
    val contract = contractWith(
      List(dataset(
        "orders",
        "raw.orders",
        Field("a", "long", constraints = List(rangeConstraint(gte = Some(1), lte = Some(5)))),
        Field("b", "long", constraints = List(rangeConstraint(gte = Some(10), lte = Some(20))))
      )),
      List(dataset("out", "gold.out", Field("b", "long"), Field("a", "long", constraints = List(fieldRangeConstraint(gt = Some("b"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "a" -> col("a", Some("raw.orders")), "b" -> col("b", Some("raw.orders")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // a's own proven upper bound (5) is strictly below b's own proven lower bound
    // (10), not merely equal to or above it - real "<=" semantics, not "==" or ">=".
    assert(results == List(DataQualityCheckResult("a", "> b", DataQualityVerdict.Violated)))
  }

  test("a fieldRange gt constraint is NotGuaranteed, not a false Violated, when neither side's needed bound is known") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("a", "long", constraints = List(rangeConstraint(gte = Some(0)))))),
      List(dataset("out", "gold.out", Field("a", "long"), Field("b", "long", constraints = List(fieldRangeConstraint(gt = Some("a"))))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "a" -> col("a", Some("raw.orders")), "b" -> col("b", Some("raw.orders")))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // b has no proven bound at all (no axiom), and a only has a lower bound - gtClause
    // needs b's own lower+a's own upper (for holds) or b's own upper+a's own lower
    // (for violated); neither pair is available, so this must fall through to the
    // honest NotGuaranteed, never a false Violated from an unconditional fallback.
    assert(results == List(DataQualityCheckResult("b", "> a", DataQualityVerdict.NotGuaranteed)))
  }

  test("a fieldRange constraint combining two bounds is NotGuaranteed, not a false Guaranteed, when only one of the two clauses is proven") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("min_price", "double", constraints = List(rangeConstraint(gte = Some(0), lte = Some(10)))))),
      List(dataset(
        "out",
        "gold.out",
        Field("min_price", "double"),
        Field("max_price", "double"),
        Field("price", "double", constraints = List(fieldRangeConstraint(gte = Some("min_price"), lte = Some("max_price"))))
      ))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(
        Read(DatasetRef("raw.orders")),
        "min_price" -> col("min_price", Some("raw.orders")),
        "max_price" -> col("max_price"), // no axiom at all: fully unconstrained
        "price" -> Literal(50.0, "double")
      )
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // The gte(min_price) clause holds outright (price=50 >= min_price's own proven
    // upper bound 10); the lte(max_price) clause is Unknown (max_price has no proven
    // bound at all). One clause holding must NOT make the whole constraint Guaranteed.
    assert(results == List(DataQualityCheckResult("price", ">= min_price and <= max_price", DataQualityVerdict.NotGuaranteed)))
  }

  test("a fieldRange constraint combining two bounds is NotStaticallyVerifiable when only one referenced field is unsupported, not requiring both") {
    val contract = contractWith(
      Nil,
      List(dataset(
        "out",
        "gold.out",
        Field("min_price", "double"),
        Field("max_price", "double"),
        Field("price", "double", constraints = List(fieldRangeConstraint(gte = Some("min_price"), lte = Some("max_price"))))
      ))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(
        Read(DatasetRef("raw.orders")),
        "min_price" -> col("min_price"), // plain passthrough, no axiom: merely Unknown, not unsupported
        "max_price" -> UDF(Some("myFn"), Nil), // genuinely unsupported
        "price" -> Literal(50.0, "double")
      )
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("price", ">= min_price and <= max_price", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("a fieldRange constraint combining two bounds is Guaranteed only when both hold, describing both in order") {
    val contract = contractWith(
      Nil,
      List(dataset(
        "out",
        "gold.out",
        Field("min_price", "double"),
        Field("max_price", "double"),
        Field("price", "double", constraints = List(fieldRangeConstraint(gte = Some("min_price"), lte = Some("max_price"))))
      ))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(
        Read(DatasetRef("raw.orders")),
        "min_price" -> Literal(0.0, "double"),
        "max_price" -> Literal(100.0, "double"),
        "price" -> Literal(50.0, "double")
      )
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("price", ">= min_price and <= max_price", DataQualityVerdict.Guaranteed)))
  }

  test("a fieldRange constraint combining two bounds is Violated when either one breaks, even though the other holds") {
    val contract = contractWith(
      Nil,
      List(dataset(
        "out",
        "gold.out",
        Field("min_price", "double"),
        Field("max_price", "double"),
        Field("price", "double", constraints = List(fieldRangeConstraint(gte = Some("min_price"), lte = Some("max_price"))))
      ))
    )
    // price (150) satisfies gte(min_price=0) but breaks lte(max_price=100) - the overall
    // verdict must be Violated, not masked by the gte clause independently holding.
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(
        Read(DatasetRef("raw.orders")),
        "min_price" -> Literal(0.0, "double"),
        "max_price" -> Literal(100.0, "double"),
        "price" -> Literal(150.0, "double")
      )
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("price", ">= min_price and <= max_price", DataQualityVerdict.Violated)))
  }

  test("a fieldRange constraint declared on a nested field is always NotStaticallyVerifiable, even when the referenced name genuinely exists at the top level") {
    val nested = Field("end_date", "long", constraints = List(fieldRangeConstraint(gte = Some("start_date"))))
    val struct = Field("period", "struct", properties = List(nested))
    val contract = contractWith(
      Nil,
      List(dataset("out", "gold.out", Field("start_date", "long"), struct))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      projectFields(Read(DatasetRef("raw.orders")), "start_date" -> Literal(5L, "long"), "period" -> StructConstruct(List("end_date" -> Literal(10L, "long"))))
    )

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // Would be Guaranteed (10 >= 5) if nested fieldRange resolution wrongly consulted the
    // top-level "start_date" sibling - it must not, so this stays NotStaticallyVerifiable.
    assert(results == List(DataQualityCheckResult("period.end_date", ">= start_date", DataQualityVerdict.NotStaticallyVerifiable)))
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

  test("a top-level field built by struct(...) is itself provably NOT NULL via the StructConstruct resolution, and its nested field is now traced for real too") {
    val nested = Field("zip", "string", nullable = false)
    val struct = Field("address", "struct", nullable = false, properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    // The top-level NOT NULL is a real Guaranteed (a freshly-constructed
    // struct is provably non-null - ir.PropertyAnalysis's StructConstruct
    // case). The nested "address.zip" check is now ALSO a real Guaranteed:
    // checksForField wraps address's own defining Expr (the StructConstruct
    // ir.PropertyAnalysis.definingExpr recovers) in one more
    // StructField(_, "zip") access and resolves that - the same
    // StructField(StructConstruct(...), ...) resolution a flat output
    // column extracting its own just-built field already used, now reached
    // through a Field.properties-declared nested obligation instead.
    assert(results == List(
      DataQualityCheckResult("address", "NOT NULL", DataQualityVerdict.Guaranteed),
      DataQualityCheckResult("address.zip", "NOT NULL", DataQualityVerdict.Guaranteed)
    ))
  }

  // --- Nested fields: real tracing through definingExpr ---------------------

  test("a nested field's own constraint is Guaranteed when the struct is built in the same plan (StructConstruct)") {
    val nested = Field("country", "string", constraints = List(equalsConstraint("US")))
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("country" -> Literal("US", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.country", "= US", DataQualityVerdict.Guaranteed)))
  }

  test("a nested field's own constraint is Violated when the struct's real value provably breaks it") {
    val nested = Field("country", "string", constraints = List(equalsConstraint("US")))
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("country" -> Literal("CA", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.country", "= US", DataQualityVerdict.Violated)))
    val violations = StaticDataQualityVerifier.violations(results)
    assert(violations.map(_.column) == List(Some("address.country")))
  }

  test("a nested field's own length constraint traces through the same way a range/oneOf constraint does") {
    val nested = Field("zip", "string", constraints = List(lengthConstraint(exact = Some(5))))
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.zip", "LENGTH = 5", DataQualityVerdict.Guaranteed)))
  }

  test("a nested field declared on the contract but absent from the actual struct construction is NotStaticallyVerifiable, not a crash") {
    val nested = Field("missing", "string", nullable = false)
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("zip" -> Literal("94107", "string")))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.missing", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)))
  }

  test("nested tracing goes arbitrarily deep, wrapping StructField at each level") {
    val leaf = Field("code", "string", nullable = false)
    val mid = Field("geo", "struct", properties = List(leaf))
    val top = Field("address", "struct", properties = List(mid))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", top)))
    val built = StructConstruct(List("geo" -> StructConstruct(List("code" -> Literal("XYZ", "string")))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.orders")), "address", built))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.geo.code", "NOT NULL", DataQualityVerdict.Guaranteed)))
  }

  test("nested tracing correctly resolves through a ColumnReference passthrough rename above the real StructConstruct") {
    val nested = Field("country", "string", constraints = List(equalsConstraint("US")))
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    val built = StructConstruct(List("country" -> Literal("US", "string")))
    // built in an earlier Project, then passed straight through by a later
    // Project's own bare ColumnReference (a `.withColumn(...).select(...)`
    // shape) - definingExpr's own ColumnReference-chasing must still find
    // the real StructConstruct, not give up at the rename.
    val inner = project(Read(DatasetRef("raw.orders")), "address", built)
    val outer = project(inner, "address", col("address"))
    val plan = Write(DatasetRef("gold.out"), outer)

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.country", "= US", DataQualityVerdict.Guaranteed)))
  }

  test("a struct read straight from an input Read, with no intervening Project, stays NotStaticallyVerifiable (the documented out-of-scope case)") {
    val nested = Field("zip", "string", nullable = false)
    val struct = Field("address", "struct", properties = List(nested))
    val contract = contractWith(Nil, List(dataset("out", "gold.out", struct)))
    // Write directly wraps a bare Read - no Project at all defines "address".
    val plan = Write(DatasetRef("gold.out"), Read(DatasetRef("raw.orders")))

    val results = StaticDataQualityVerifier.verify(contract, plan)
    assert(results == List(DataQualityCheckResult("address.zip", "NOT NULL", DataQualityVerdict.NotStaticallyVerifiable)))
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
