// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.testkit.ColumnSource.{CastInput, FromInput, NonNullLong, RowCount, UniqueId}

import org.scalatest.funsuite.AnyFunSuite

class ScenarioRowsTest extends AnyFunSuite {

  private val schema = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true), LogicalField("amount", LogicalType.LongType, nullable = true)))

  private def input(rows: Map[String, Any]*) = ScenarioInput("in/orders", schema, rows.toList)

  private def job(in: ScenarioInput, columns: List[OutColumn], filterColumn: Option[String] = None) =
    ScenarioJob(List(in), columns, ScenarioOutput("out/report"), filterColumn = filterColumn)

  test("each row the input holds is projected through the job's columns, in order; a missing value reads as null") {
    val rows = ScenarioRows.of(job(
      input(Map("id" -> 1L, "amount" -> 5L), Map("id" -> 2L)),
      List(OutColumn("key", FromInput(0, "id")), OutColumn("total", CastInput(0, "amount", LogicalType.LongType)), OutColumn("one", NonNullLong(1)))
    ))
    assert(rows == List(Map("key" -> 1L, "total" -> 5L, "one" -> 1L), Map("key" -> 2L, "total" -> null, "one" -> 1L)))
  }

  test("a filtered job keeps only the rows whose filter column is above zero: not zero, not below, not null, not text") {
    val rows = ScenarioRows.of(job(
      input(Map("id" -> 1L, "amount" -> 1L), Map("id" -> 2L, "amount" -> 0L), Map("id" -> 3L, "amount" -> -1L), Map("id" -> 4L), Map("id" -> 5L, "amount" -> "7")),
      List(OutColumn("id", FromInput(0, "id"))),
      filterColumn = Some("amount")
    ))
    assert(rows == List(Map("id" -> 1L)))
  }

  test("a generated id is a fresh text per row, an input with no rows writes none, and a job that reads rows cannot group them") {
    val ids = ScenarioRows.of(job(input(Map("id" -> 1L), Map("id" -> 2L)), List(OutColumn("token", UniqueId)))).map(_("token"))
    assert(ids.size == 2 && ids.distinct.size == 2 && ids.forall(_.isInstanceOf[String]))
    assert(ScenarioRows.of(job(input(), List(OutColumn("id", FromInput(0, "id"))))).isEmpty)
    intercept[IllegalArgumentException](ScenarioRows.of(job(input(Map("id" -> 1L)), List(OutColumn("n", RowCount)))))
  }

  test("only a plain read-transform-write of one input can read rows") {
    val withRows = input(Map("id" -> 1L))
    val other = ScenarioInput("in/other", schema)
    intercept[IllegalArgumentException](ScenarioJob(List(withRows, other), Nil, ScenarioOutput("out/report"), join = Some(JoinOn("id", "id"))))
    intercept[IllegalArgumentException](ScenarioJob(List(withRows), List(OutColumn("id", FromInput(0, "id"))), ScenarioOutput("out/report"), groupBy = Some(List("id"))))
    intercept[IllegalArgumentException](ScenarioJob(List(withRows), Nil, ScenarioOutput("out/report"), rowChange = Some(RowChange.UnconditionalDelete)))
    intercept[IllegalArgumentException](ScenarioJob(List(withRows), Nil, ScenarioOutput("out/report"), untranslatableWrite = true))
    intercept[IllegalArgumentException](ScenarioJob(List(withRows), Nil, ScenarioOutput("out/report"), streaming = true))
    // the same shapes without rows are fine
    ScenarioJob(List(ScenarioInput("in/orders", schema), other), Nil, ScenarioOutput("out/report"), join = Some(JoinOn("id", "id")))
    ScenarioJob(List(ScenarioInput("in/orders", schema)), Nil, ScenarioOutput("out/report"), streaming = true)
  }
}
