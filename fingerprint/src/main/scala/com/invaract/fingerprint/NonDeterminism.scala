// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._

/** Tri-state, metadata-only classification of whether an expression's
  * *definition* contains a known non-deterministic construct — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §9. Never consulted by
  * `Canonicalizer`/`FingerprintHasher`: this exists purely to populate
  * `OutputFingerprint.nonDeterministic`, a reported annotation, not a hash
  * input. Two definitions differing only in which named function they call
  * already differ in that `Function.name` field and are caught by the
  * ordinary hash regardless of this classifier.
  *
  * `Some(true)` — a known non-deterministic function name appears
  * somewhere in the (already passthrough-resolved) expression.
  * `Some(false)` — every node encountered is either a plain operator this
  * IR understands, or a `Function` whose name is not on the allowlist
  * below. A `Function` node is already documented (`ir.Expr`'s own doc) as
  * "a claim this IR understands what the named operation computes" - a
  * plain per-row computation - so absence from this allowlist is treated
  * as "not flagged as non-deterministic", not as a separately-verified
  * guarantee; see the module doc's own discussion of this trade-off.
  * `None` — a `UDF` sits anywhere in the resolved expression: its body is
  * opaque by design (`ir.UDF`'s own doc), so this module has no way to
  * know whether it calls something non-deterministic internally.
  */
object NonDeterminism {

  /** Function names (lower-cased) this module treats as producing a
    * different result on every evaluation. Deliberately small and
    * maintained by hand, not derived from Catalyst's own `deterministic`
    * flag (the IR carries no such field today - see the design doc's own
    * named limitation). Growing this list is a canonicalisation-format-
    * relevant change in the sense docs/SEMANTIC_LINEAGE_FINGERPRINTING.md
    * §10 describes (it changes reported metadata, never hash bytes), so a
    * real fingerprint-version bump should still accompany a real addition
    * here once this module is versioned for real.
    */
  val knownNonDeterministicFunctionNames: Set[String] = Set(
    "rand",
    "random",
    "randn",
    "uuid",
    "current_timestamp",
    "current_date",
    "now",
    "unix_timestamp",
    "monotonically_increasing_id",
    "input_file_name",
    "spark_partition_id"
  )

  def classify(expr: Expr): Option[Boolean] = expr match {
    case ColumnReference(_) => Some(false)
    case Literal(_, _)      => Some(false)
    case Alias(_, inner)    => classify(inner)
    case Cast(inner, _)     => classify(inner)
    case Arithmetic(_, operands) => combine(operands.map(classify))
    case Comparison(_, left, right) => combine(List(classify(left), classify(right)))
    case BooleanExpr(_, operands) => combine(operands.map(classify))
    case Conditional(branches, elseValue) =>
      val branchResults = branches.flatMap { case (c, v) => List(classify(c), classify(v)) }
      combine(branchResults ++ elseValue.map(classify).toList)
    case Function(name, args) =>
      val own: Option[Boolean] = Some(knownNonDeterministicFunctionNames.contains(name.toLowerCase))
      combine(own :: args.map(classify))
    case UDF(_, _, _) =>
      // Opaque unconditionally, regardless of its arguments' own
      // classification - an opaque body could do anything internally,
      // independent of what it's applied to. Mirrors ir.Lineage's own
      // "UDF is Opaque regardless of its arguments" rule for DerivationKind.
      None
    case AggregateCall(_, arg, _) => combine(List(Some(false), classify(arg)))
    case UnknownExpression(_, _, children) =>
      // An unrepresented construct is exactly as unknowable here as a UDF
      // - this module cannot say whether it's deterministic either.
      combine(None :: children.map(classify))
  }

  /** Combines several classifications conservatively: any `None` makes the
    * whole combination `None` (an opaque subtree could hide anything);
    * absent that, any `Some(true)` makes it `Some(true)`; otherwise
    * `Some(false)`. An empty list combines to `Some(false)` - vacuously,
    * nothing non-deterministic was found because nothing was there to find
    * (mirrors `ir.Lineage`'s own "empty combines to the identity" choices
    * for `combineOperation`/`combineUnion`).
    */
  private def combine(results: List[Option[Boolean]]): Option[Boolean] =
    if (results.exists(_.isEmpty)) None
    else if (results.exists(_.contains(true))) Some(true)
    else Some(false)
}
