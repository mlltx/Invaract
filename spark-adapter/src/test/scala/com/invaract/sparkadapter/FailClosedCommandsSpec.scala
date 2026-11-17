// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.catalyst.analysis.UnresolvedNamespace
import org.apache.spark.sql.catalyst.plans.logical.{DropNamespace, OneRowRelation}
import org.scalatest.funsuite.AnyFunSuite

/** Direct, session-free unit coverage for `isKnownSafeIcebergProcedureCall`'s
  * reflection fallback - the one line mutation testing flagged as
  * uncovered by `IcebergConnectorSpec`'s real-session tests, since a real
  * Iceberg `Call` plan's `procedure()` reflection never fails there. See
  * docs/SPARK_ADAPTER.md's "Iceberg CALL procedure classification".
  */
class FailClosedCommandsSpec extends AnyFunSuite {

  test("isKnownSafeIcebergProcedureCall fails closed (returns false) when procedure() reflection fails") {
    // Any concrete LogicalPlan with no procedure() method exercises the
    // same NoSuchMethodException path a future Iceberg version reshaping
    // Call would - this must never silently resolve to "safe".
    assert(!FailClosedCommands.isKnownSafeIcebergProcedureCall(OneRowRelation()))
  }

  test("an unverifiable command is described in Spark's own terms: its class, the known-safe list, the translation gap") {
    val op = FailClosedCommands.unverifiable("MergeIntoCommand")
    assert(op.name == "MergeIntoCommand")
    assert(op.problem == "'MergeIntoCommand' looks like it may write or otherwise mutate data, but Invaract has no translation for it")
    assert(op.remediation.startsWith("If this command genuinely doesn't write data, add its class to FailClosedCommands' known-safe list"))
    assert(op.remediation.contains("SparkPlanAdapter") && op.remediation.contains("docs/SPARK_ADAPTER.md"))
  }

  private def dropDatabase(cascade: Boolean) = DropNamespace(UnresolvedNamespace(Seq("db")), ifExists = false, cascade = cascade)

  test("DROP DATABASE is known safe without CASCADE and not with it") {
    assert(FailClosedCommands.isKnownSafe(dropDatabase(cascade = false)))
    assert(!FailClosedCommands.isKnownSafe(dropDatabase(cascade = true)))
  }

  test("a DROP DATABASE ... CASCADE is described in its own words; any other plan by its class") {
    val cascade = FailClosedCommands.unverifiable(dropDatabase(cascade = true))
    assert(cascade.name == "DropNamespace")
    assert(cascade.problem.startsWith("DROP DATABASE ... CASCADE deletes every table in the database"))
    assert(cascade.remediation.startsWith("Drop the tables this job is allowed to remove one at a time"))
    assert(FailClosedCommands.unverifiable(dropDatabase(cascade = false)) == FailClosedCommands.unverifiable("DropNamespace"))
    assert(FailClosedCommands.unverifiable(OneRowRelation()) == FailClosedCommands.unverifiable("OneRowRelation"))
  }
}
