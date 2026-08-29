// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
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
}
