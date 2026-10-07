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

/** What an engine adapter's DML classifier made of a plan - the engine-neutral
  * half of `spark-adapter`'s `RowMutationSupport.Classification` (which is an
  * alias of this type). `Extracted`: the plan is `kind`-shaped DML and the
  * facts a rule needs were extracted into an `ir.RowMutation`. `Unverifiable`:
  * the plan is genuinely `kind`-shaped DML but the adapter could not extract
  * what a rule of that kind needs, so `VerificationPipeline` fails closed
  * (only when the contract declares a rule that kind is relevant to).
  */
sealed trait MutationClassification { def kind: MutationKind }
object MutationClassification {
  case class Extracted(kind: MutationKind, mutation: com.invaract.ir.RowMutation) extends MutationClassification
  case class Unverifiable(kind: MutationKind) extends MutationClassification
}
