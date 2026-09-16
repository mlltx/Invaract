// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

/** The three row-level DML operation kinds a `CustomRuleVerifier` can be
  * asked about — the public, third-party-facing counterpart to
  * `RowMutationSupport.Kind` (`private[sparkadapter]`, since it's paired
  * with connector-specific extraction machinery no plugin author needs to
  * see). `RuleVerifier` translates between the two at the boundary: every
  * built-in rule type's own `appliesTo`/`verify` still works in terms of
  * `RowMutationSupport.Kind` internally, and a custom rule type's
  * `CustomRuleVerifier.appliesTo` is asked with the `MutationKind`
  * translation of whatever `RowMutationSupport.Kind` the plan classified
  * as — connector-agnostic either way, independent of whether extraction
  * for that kind actually succeeded.
  */
sealed trait MutationKind
object MutationKind {
  case object Merge extends MutationKind
  case object Update extends MutationKind
  case object Delete extends MutationKind
}
