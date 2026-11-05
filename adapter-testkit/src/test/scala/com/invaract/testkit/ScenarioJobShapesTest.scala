// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.testkit.ColumnSource.{FromInput, RowCount}

import org.scalatest.funsuite.AnyFunSuite

/** The job shapes `ScenarioJob` refuses: each special shape (grouping, an opaque step, a stream, further writes, branches
  * that rejoin) is a plain read-transform-write, so it is refused next to each of the shapes that are not, one at a time. */
class ScenarioJobShapesTest extends AnyFunSuite {

  private val in = ScenarioInput("in/orders", LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true))))
  private val out = ScenarioOutput("out/report")
  private val id = List(OutColumn("id", FromInput(0, "id")))

  private def job(
      groupBy: Option[List[String]] = None,
      opaqueStep: Boolean = false,
      streaming: Boolean = false,
      extraWrites: List[ExtraWrite] = Nil,
      rejoins: Int = 0,
      rowChange: Option[RowChange] = None,
      untranslatableWrite: Boolean = false,
      boundary: Boolean = false
  ) = ScenarioJob(List(in), id, out, rowChange = rowChange, untranslatableWrite = untranslatableWrite, boundary = boundary,
    streaming = streaming, opaqueStep = opaqueStep, groupBy = groupBy, extraWrites = extraWrites, rejoins = rejoins)

  /** Each shape that is not a plain read-transform-write, one at a time. */
  private val notPlain: List[(String, (Option[RowChange], Boolean, Boolean, Boolean))] = List(
    "a row change" -> ((Some(RowChange.UnconditionalDelete), false, false, false)),
    "an untranslatable write" -> ((None, true, false, false)),
    "a boundary" -> ((None, false, true, false)),
    "a stream" -> ((None, false, false, true))
  )

  private def refusedWith(special: (Option[RowChange], Boolean, Boolean, Boolean) => ScenarioJob, skip: Set[String] = Set.empty): Unit =
    notPlain.filterNot(n => skip(n._1)).foreach { case (what, (change, untranslatable, boundary, streaming)) =>
      withClue(what)(intercept[IllegalArgumentException](special(change, untranslatable, boundary, streaming)))
    }

  test("a grouping job is a plain read-transform-write, and only it has a row count") {
    job(groupBy = Some(List("id")))
    refusedWith((c, u, b, s) => job(groupBy = Some(List("id")), rowChange = c, untranslatableWrite = u, boundary = b, streaming = s))
    intercept[IllegalArgumentException](ScenarioJob(List(in), List(OutColumn("n", RowCount)), out))
    ScenarioJob(List(in), List(OutColumn("n", RowCount)), out, groupBy = Some(List("id")))
  }

  test("an opaque step is part of a plain read-transform-write") {
    job(opaqueStep = true)
    refusedWith((c, u, b, s) => job(opaqueStep = true, rowChange = c, untranslatableWrite = u, boundary = b, streaming = s))
  }

  test("a stream is a plain read-transform-write of one input") {
    job(streaming = true)
    refusedWith((c, u, b, _) => job(streaming = true, rowChange = c, untranslatableWrite = u, boundary = b), skip = Set("a stream"))
    intercept[IllegalArgumentException](ScenarioJob(List(in, in.copy(location = "in/other")), id, out, join = Some(JoinOn("id", "id")), streaming = true))
  }

  test("only a plain batch job makes further writes") {
    val extra = List(ExtraWrite(in, ScenarioOutput("out/audit")))
    job(extraWrites = extra)
    refusedWith((c, u, _, s) => job(extraWrites = extra, rowChange = c, untranslatableWrite = u, streaming = s), skip = Set("a boundary"))
    job(extraWrites = extra, boundary = true)
  }

  test("branches rejoin a number of times, or not at all, in a plain read-transform-write") {
    job(rejoins = 1)
    job(rejoins = 0, boundary = true)
    intercept[IllegalArgumentException](job(rejoins = -1))
    refusedWith((c, u, b, s) => job(rejoins = 1, rowChange = c, untranslatableWrite = u, boundary = b, streaming = s))
  }
}
