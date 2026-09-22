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
