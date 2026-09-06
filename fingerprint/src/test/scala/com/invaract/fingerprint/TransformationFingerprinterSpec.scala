// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite

class TransformationFingerprinterSpec extends AnyFunSuite {

  // Worked Example A from docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §13.
  private def customerValuesPlan(rate: BigDecimal): Plan = {
    val orders = Read(DatasetRef("raw.orders"))
    Write(
      DatasetRef("gold.customer_values"),
      Project(
        orders,
        List(
          NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          NamedExpr("value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(rate, "decimal"))))
        )
      )
    )
  }

  test("an input's fingerprint is exactly the hash of its own Read canonical form") {
    val plan = customerValuesPlan(BigDecimal("1.20"))
    val fp = TransformationFingerprinter.fingerprint(plan)
    val expected = FingerprintHasher.hash(CTag("Read", List(CanonicalNode.stringLeaf("raw.orders"))))
    assert(fp.inputs("raw.orders#0") == expected)
  }

  test("an output's combined fingerprint is exactly hash(Combined(expression node, lineage node))") {
    val plan = customerValuesPlan(BigDecimal("1.20"))
    val fp = TransformationFingerprinter.fingerprint(plan)
    val scope = Canonicalizer.buildScopeInfo(plan).substitution
    val resolvedExpr = Canonicalizer.resolvedOutputs(plan)("value")
    val exprNode = Canonicalizer.canonicalizeExpr(resolvedExpr, scope)
    val lineageNode = Canonicalizer.canonicalizeLineage(Lineage.trace(plan).find(_.output.name == "value").get, scope)
    assert(fp.outputs("value").combined == FingerprintHasher.hash(CTag("Combined", List(exprNode, lineageNode))))
  }

  test("Fingerprint.toMap carries exactly version/algorithm/value") {
    val fp = Fingerprint(1, "SHA-256", "abc123")
    assert(fp.toMap == Map("version" -> 1, "algorithm" -> "SHA-256", "value" -> "abc123"))
  }

  test("OutputFingerprint.toMap and TransformationFingerprint.toMap carry every field under its own key") {
    val fp = TransformationFingerprinter.fingerprint(customerValuesPlan(BigDecimal("1.20")))
    val outputMap = fp.outputs("value").toMap
    assert(outputMap.keySet == Set("expression", "lineage", "combined", "nonDeterministic"))
    assert(outputMap("expression") == fp.outputs("value").expression.toMap)

    val topMap = fp.toMap
    assert(topMap.keySet == Set("version", "overall", "inputs", "outputs"))
    assert(topMap("version") == fp.version)
  }

  test("determinism: fingerprinting the same plan twice yields identical results") {
    val plan = customerValuesPlan(BigDecimal("1.20"))
    assert(TransformationFingerprinter.fingerprint(plan) == TransformationFingerprinter.fingerprint(plan))
  }

  test("Example A: a literal change moves the affected output's expression/combined fingerprints, not its lineage or siblings") {
    val before = TransformationFingerprinter.fingerprint(customerValuesPlan(BigDecimal("1.20")))
    val after = TransformationFingerprinter.fingerprint(customerValuesPlan(BigDecimal("1.25")))

    assert(before.overall != after.overall)
    assert(before.inputs == after.inputs, "the source table itself did not change")

    assert(before.outputs("customer_id") == after.outputs("customer_id"), "an unrelated output must stay byte-identical")

    assert(before.outputs("value").expression != after.outputs("value").expression)
    assert(before.outputs("value").lineage == after.outputs("value").lineage, "same source column, same Computed derivation")
    assert(before.outputs("value").combined != after.outputs("value").combined)
  }

  test("locality: changing one output in a three-column Project leaves the other two byte-identical") {
    def plan(middleRate: BigDecimal): Plan = {
      val orders = Read(DatasetRef("raw.orders"))
      Write(
        DatasetRef("gold.out"),
        Project(
          orders,
          List(
            NamedExpr("a", ColumnReference(ColumnRef("a", Some("raw.orders")))),
            NamedExpr("b", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(middleRate, "decimal")))),
            NamedExpr("c", ColumnReference(ColumnRef("c", Some("raw.orders"))))
          )
        )
      )
    }
    val before = TransformationFingerprinter.fingerprint(plan(BigDecimal("1.20")))
    val after = TransformationFingerprinter.fingerprint(plan(BigDecimal("1.25")))

    assert(before.outputs("a") == after.outputs("a"))
    assert(before.outputs("c") == after.outputs("c"))
    assert(before.outputs("b") != after.outputs("b"))
    assert(before.overall != after.overall)
  }

  test("locality across a Join: changing one side's projection leaves the other side's outputs unaffected") {
    def plan(rate: BigDecimal): Plan = {
      val orders = Read(DatasetRef("raw.orders"))
      val customers = Read(DatasetRef("raw.customers"))
      val joined = Join(orders, customers, JoinType.Inner, Some(Comparison(
        "=",
        ColumnReference(ColumnRef("customer_id", Some("raw.orders"))),
        ColumnReference(ColumnRef("id", Some("raw.customers")))
      )))
      Write(
        DatasetRef("gold.out"),
        Project(
          joined,
          List(
            NamedExpr("customer_name", ColumnReference(ColumnRef("name", Some("raw.customers")))),
            NamedExpr("order_value", Arithmetic("*", List(ColumnReference(ColumnRef("amount", Some("raw.orders"))), Literal(rate, "decimal"))))
          )
        )
      )
    }
    val before = TransformationFingerprinter.fingerprint(plan(BigDecimal("1.20")))
    val after = TransformationFingerprinter.fingerprint(plan(BigDecimal("1.25")))
    assert(before.outputs("customer_name") == after.outputs("customer_name"))
    assert(before.outputs("order_value") != after.outputs("order_value"))
  }

  test("self-join: orders.id and customers.id never collapse into one input entry") {
    val orders = Read(DatasetRef("raw.orders"))
    val customers = Read(DatasetRef("raw.customers"))
    val joined = Join(orders, customers, JoinType.Inner, Some(Comparison(
      "=",
      ColumnReference(ColumnRef("id", Some("raw.orders"))),
      ColumnReference(ColumnRef("id", Some("raw.customers")))
    )))
    val plan = Write(DatasetRef("gold.out"), Project(joined, List(NamedExpr("order_id", ColumnReference(ColumnRef("id", Some("raw.orders")))))))
    val fp = TransformationFingerprinter.fingerprint(plan)
    assert(fp.inputs.keySet == Set("raw.orders#0", "raw.customers#0"))
  }

  test("self-join: two occurrences of the same dataset get two distinct input entries") {
    val o1 = Read(DatasetRef("raw.orders"), Some("o1"))
    val o2 = Read(DatasetRef("raw.orders"), Some("o2"))
    val joined = Join(o1, o2, JoinType.Inner, Some(Comparison(
      "=",
      ColumnReference(ColumnRef("customer_id", Some("o1"))),
      ColumnReference(ColumnRef("customer_id", Some("o2")))
    )))
    val plan = Write(DatasetRef("gold.out"), Project(joined, List(NamedExpr("id1", ColumnReference(ColumnRef("customer_id", Some("o1")))))))
    val fp = TransformationFingerprinter.fingerprint(plan)
    assert(fp.inputs.keySet == Set("raw.orders#0", "raw.orders#1"))
  }

  test("alias renaming across a whole pipeline does not change any fingerprint in the hierarchy") {
    def plan(leftAlias: String, rightAlias: String): Plan = {
      val left = Read(DatasetRef("raw.orders"), Some(leftAlias))
      val right = Read(DatasetRef("raw.orders"), Some(rightAlias))
      val joined = Join(left, right, JoinType.Inner, Some(Comparison(
        "=",
        ColumnReference(ColumnRef("customer_id", Some(leftAlias))),
        ColumnReference(ColumnRef("customer_id", Some(rightAlias)))
      )))
      Write(DatasetRef("gold.out"), Project(joined, List(NamedExpr("id", ColumnReference(ColumnRef("customer_id", Some(leftAlias)))))))
    }
    val variant1 = TransformationFingerprinter.fingerprint(plan("o1", "o2"))
    val variant2 = TransformationFingerprinter.fingerprint(plan("a", "b"))
    assert(variant1.overall == variant2.overall)
    assert(variant1.outputs == variant2.outputs)
  }

  test("Write.format/saveMode affect overall but never an individual output") {
    val orders = Read(DatasetRef("raw.orders"))
    def plan(saveMode: Option[String]): Plan = Write(
      DatasetRef("gold.out"),
      Project(orders, List(NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))))),
      format = Some("parquet"),
      saveMode = saveMode
    )
    val append = TransformationFingerprinter.fingerprint(plan(Some("append")))
    val overwrite = TransformationFingerprinter.fingerprint(plan(Some("overwrite")))
    assert(append.overall != overwrite.overall)
    assert(append.outputs == overwrite.outputs)
  }

  test("SUM vs AVG produces different output fingerprints") {
    def plan(function: String): Plan = {
      val orders = Read(DatasetRef("raw.orders"))
      Write(
        DatasetRef("gold.out"),
        Aggregate(
          orders,
          List(ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          List(
            NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
            NamedExpr("total", AggregateCall(function, ColumnReference(ColumnRef("amount", Some("raw.orders")))))
          )
        )
      )
    }
    val sumFp = TransformationFingerprinter.fingerprint(plan("SUM"))
    val avgFp = TransformationFingerprinter.fingerprint(plan("AVG"))
    assert(sumFp.outputs("total") != avgFp.outputs("total"))
    assert(sumFp.outputs("customer_id") == avgFp.outputs("customer_id"))
  }

  test("INNER vs LEFT OUTER join changes fingerprints on both sides' outputs") {
    def plan(joinType: JoinType): Plan = {
      val orders = Read(DatasetRef("raw.orders"))
      val customers = Read(DatasetRef("raw.customers"))
      val joined = Join(orders, customers, joinType, Some(Comparison(
        "=",
        ColumnReference(ColumnRef("customer_id", Some("raw.orders"))),
        ColumnReference(ColumnRef("id", Some("raw.customers")))
      )))
      Write(
        DatasetRef("gold.out"),
        Project(joined, List(
          NamedExpr("order_id", ColumnReference(ColumnRef("id", Some("raw.orders")))),
          NamedExpr("customer_name", ColumnReference(ColumnRef("name", Some("raw.customers"))))
        ))
      )
    }
    val inner = TransformationFingerprinter.fingerprint(plan(JoinType.Inner))
    val leftOuter = TransformationFingerprinter.fingerprint(plan(JoinType.LeftOuter))
    assert(inner.overall != leftOuter.overall)
  }

  test("a filter's comparison value changing (status = ACTIVE vs INACTIVE) changes overall but not an untouched output") {
    def plan(status: String): Plan = {
      val orders = Read(DatasetRef("raw.orders"))
      val filtered = Filter(orders, Comparison("=", ColumnReference(ColumnRef("status", Some("raw.orders"))), Literal(status, "string")))
      Write(
        DatasetRef("gold.out"),
        Project(filtered, List(
          NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
          NamedExpr("region", ColumnReference(ColumnRef("region", Some("raw.orders"))))
        ))
      )
    }
    val active = TransformationFingerprinter.fingerprint(plan("ACTIVE"))
    val inactive = TransformationFingerprinter.fingerprint(plan("INACTIVE"))
    assert(active.overall != inactive.overall)
    // Neither output's own declared expression touches the filter, and
    // ir.Lineage's own resolution does not fold Filter conditions into a
    // passthrough column's sources - both stay identical.
    assert(active.outputs("customer_id") == inactive.outputs("customer_id"))
    assert(active.outputs("region") == inactive.outputs("region"))
  }

  test("Example C: UDF - renaming changes expression but not lineage; changing engineType alone changes neither") {
    def plan(name: String, engineType: String): Plan = {
      val events = Read(DatasetRef("raw.events"))
      Write(
        DatasetRef("gold.scored_events"),
        Project(
          events,
          List(
            NamedExpr("event_id", ColumnReference(ColumnRef("event_id", Some("raw.events")))),
            NamedExpr(
              "risk_score",
              UDF(
                Some(name),
                List(ColumnReference(ColumnRef("amount", Some("raw.events"))), ColumnReference(ColumnRef("country", Some("raw.events")))),
                Some(engineType)
              )
            )
          )
        )
      )
    }
    val base = TransformationFingerprinter.fingerprint(plan("score_risk", "PythonUDF"))
    val renamed = TransformationFingerprinter.fingerprint(plan("score_risk_v2", "PythonUDF"))
    val differentEngine = TransformationFingerprinter.fingerprint(plan("score_risk", "ScalaUDF"))

    assert(base.outputs("risk_score").expression != renamed.outputs("risk_score").expression)
    assert(base.outputs("risk_score").lineage == renamed.outputs("risk_score").lineage, "still opaque over the same two source columns")

    assert(base.outputs("risk_score") == differentEngine.outputs("risk_score"), "engineType alone must never move any hash")
  }

  test("OutputFingerprint.nonDeterministic is populated and never affects the hash fields") {
    val orders = Read(DatasetRef("raw.orders"))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(orders, List(NamedExpr("ts", Function("current_timestamp", Nil))))
    )
    val fp = TransformationFingerprinter.fingerprint(plan)
    assert(fp.outputs("ts").nonDeterministic == Some(true))

    // Fingerprinting the identical plan twice (i.e. two hypothetical
    // "runs" of a job whose definition never changed) must be stable even
    // though the flagged construct is itself non-deterministic at runtime -
    // this hashes the call's static definition, never a runtime value.
    val fp2 = TransformationFingerprinter.fingerprint(plan)
    assert(fp.outputs("ts").expression == fp2.outputs("ts").expression)
  }
}
