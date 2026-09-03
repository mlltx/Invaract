// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractVersion, Dataset, Field, Schema}
import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

class SensitivityLineageSpec extends AnyFunSuite {

  private def contractWith(inputs: List[Dataset]): Contract =
    Contract(
      id = "test_contract",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = inputs,
      outputs = List(
        Dataset("out", "gold.out", Some("parquet"), Schema(List(Field("out", "string"))))
      ),
      rules = Nil,
      extensions = Map.empty
    )

  private def dataset(name: String, location: String, fields: Field*): Dataset =
    Dataset(name, location, Some("csv"), Schema(fields.toList))

  test("propagate leaves an output's sensitivityTags empty when no source is tagged") {
    val contract = contractWith(List(dataset("orders", "raw.orders", Field("id", "integer"), Field("amount", "integer"))))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(Read(DatasetRef("raw.orders")), List(NamedExpr("amount", ColumnReference(ColumnRef("amount", Some("raw.orders"))))))
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.size == 1)
    assert(result.head.sensitivityTags.isEmpty)
  }

  test("propagate carries a directly-tagged source column's tags to a Direct-derivation output") {
    val contract = contractWith(
      List(dataset("customers", "raw.customers", Field("ssn", "string", sensitivityTags = Set("pii"))))
    )
    val plan = Write(
      DatasetRef("gold.out"),
      Project(Read(DatasetRef("raw.customers")), List(NamedExpr("ssn", ColumnReference(ColumnRef("ssn", Some("raw.customers"))))))
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.head.sensitivityTags == Set("pii"))
    assert(result.head.lineage.derivation == DerivationKind.Direct)
  }

  test("propagate carries tags transitively through Arithmetic, Cast, and Conditional derivations") {
    val contract = contractWith(
      List(dataset("orders", "raw.orders", Field("amount", "integer", sensitivityTags = Set("financial"))))
    )
    val amount = ColumnReference(ColumnRef("amount", Some("raw.orders")))

    def outputsOf(expr: Expr): Set[String] = {
      val plan = Write(DatasetRef("gold.out"), Project(Read(DatasetRef("raw.orders")), List(NamedExpr("out", expr))))
      SensitivityLineage.propagate(Lineage.trace(plan), contract).head.sensitivityTags
    }

    assert(outputsOf(Arithmetic("*", List(amount, Literal(2, "integer")))) == Set("financial"))
    assert(outputsOf(Cast(amount, "double")) == Set("financial"))
    assert(
      outputsOf(Conditional(List((Comparison(">", amount, Literal(1000, "integer")), Literal("high", "string"))), Some(Literal("low", "string")))) ==
        Set("financial"),
      "a CASE WHEN derived from a tagged column still carries the tag forward, even though the branch values themselves are literals"
    )
  }

  test("propagate unions tags from two differently-tagged sources feeding one output") {
    val contract = contractWith(
      List(
        dataset(
          "orders",
          "raw.orders",
          Field("amount", "integer", sensitivityTags = Set("financial")),
          Field("email", "string", sensitivityTags = Set("pii"))
        )
      )
    )
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        Read(DatasetRef("raw.orders")),
        List(
          NamedExpr(
            "combined",
            Arithmetic(
              "+",
              List(
                ColumnReference(ColumnRef("amount", Some("raw.orders"))),
                Cast(ColumnReference(ColumnRef("email", Some("raw.orders"))), "integer")
              )
            )
          )
        )
      )
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.head.sensitivityTags == Set("financial", "pii"))
  }

  test("propagate does not cross-attribute a tag from one dataset's field to a same-named field on another, untagged dataset") {
    val contract = contractWith(
      List(
        dataset("orders", "raw.orders", Field("id", "integer")),
        dataset("customers", "raw.customers", Field("id", "integer", sensitivityTags = Set("pii")))
      )
    )
    // order_id derives only from raw.orders.id, which is NOT tagged -
    // asserting only "no PII tag anywhere" wouldn't distinguish correct
    // matching from a bug that matched by bare name and tagged everything
    // named "id" regardless of which dataset it actually came from.
    val plan = Write(
      DatasetRef("gold.out"),
      Project(Read(DatasetRef("raw.orders")), List(NamedExpr("order_id", ColumnReference(ColumnRef("id", Some("raw.orders"))))))
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.head.sensitivityTags.isEmpty)
  }

  test("propagate matches a tagged input's contract-declared (relative) location against the plan's actual (absolute file:) location") {
    val contract = contractWith(List(dataset("orders", "demo/input/sample.csv", Field("value", "integer", sensitivityTags = Set("internal")))))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        Read(DatasetRef("file:/home/user/Invaract/demo/input/sample.csv")),
        List(NamedExpr("value", ColumnReference(ColumnRef("value", Some("file:/home/user/Invaract/demo/input/sample.csv")))))
      )
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.head.sensitivityTags == Set("internal"))
  }

  test("propagate contributes no tags for a source whose qualifier matches no contract input (e.g. a self-join alias)") {
    val contract = contractWith(List(dataset("orders", "raw.orders", Field("id", "integer", sensitivityTags = Set("pii")))))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(Read(DatasetRef("raw.orders"), alias = Some("cur")), List(NamedExpr("id", ColumnReference(ColumnRef("id", Some("cur"))))))
    )

    val result = SensitivityLineage.propagate(Lineage.trace(plan), contract)
    assert(result.head.sensitivityTags.isEmpty)
  }

  test("propagate is 1:1 and order-preserving with the input lineage list") {
    val contract = contractWith(List(dataset("orders", "raw.orders", Field("a", "integer"), Field("b", "integer"))))
    val plan = Write(
      DatasetRef("gold.out"),
      Project(
        Read(DatasetRef("raw.orders")),
        List(
          NamedExpr("a", ColumnReference(ColumnRef("a", Some("raw.orders")))),
          NamedExpr("b", ColumnReference(ColumnRef("b", Some("raw.orders"))))
        )
      )
    )

    val lineage = Lineage.trace(plan)
    val result = SensitivityLineage.propagate(lineage, contract)
    assert(result.map(_.lineage.output.name) == lineage.map(_.output.name))
  }
}
