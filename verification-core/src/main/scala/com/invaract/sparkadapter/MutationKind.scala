// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

/** The three row-level DML operation kinds a rule can be asked about —
  * engine-neutral, and the one vocabulary shared by built-in rule types, a
  * `CustomRuleVerifier`'s `appliesTo`, and an engine adapter's own DML
  * classification (`spark-adapter`'s `RowMutationSupport.Kind` is an alias
  * of this type). Connector-agnostic, independent of whether extraction for
  * that kind actually succeeded.
  */
sealed trait MutationKind
object MutationKind {
  case object Merge extends MutationKind
  case object Update extends MutationKind
  case object Delete extends MutationKind
}
