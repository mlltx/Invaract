// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

import org.scalatest.funsuite.AnyFunSuite

class RowMutationSpec extends AnyFunSuite {

  test("a default RowMutation has no match condition, no delete, and no updated columns") {
    val mutation = RowMutation()
    assert(mutation.matchCondition.isEmpty)
    assert(mutation.delete == DeleteScope.NotApplicable)
    assert(mutation.updatedColumns.isEmpty)
  }

  test("matchCondition carries the merge ON clause as a full Expr") {
    val condition = FunctionCall(
      "=",
      List(
        ColumnReference(ColumnRef("customer_id", Some("target"))),
        ColumnReference(ColumnRef("customer_id", Some("source")))
      )
    )
    val mutation = RowMutation(matchCondition = Some(condition))
    assert(mutation.matchCondition.contains(condition))
    assert(mutation.matchCondition.get.references == condition.references)
  }

  test("DeleteScope distinguishes not-applicable, unconditional, and conditional") {
    assert(DeleteScope.NotApplicable != DeleteScope.Unconditional)
    val condition = ColumnReference(ColumnRef("is_archived"))
    assert(DeleteScope.Conditional(condition).condition == condition)
  }

  test("updatedColumns carries a standalone UPDATE's assigned column names") {
    val mutation = RowMutation(updatedColumns = List("status", "updated_at"))
    assert(mutation.updatedColumns == List("status", "updated_at"))
  }
}
