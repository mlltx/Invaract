// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir.{Lineage, Plan}

/** One output column's fingerprints — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §3.
  *
  * `expression` and `lineage` answer different questions and can disagree
  * usefully: `expression` changes for any syntactic change to this
  * column's own (passthrough-resolved) declared computation; `lineage`,
  * built from `ir.Lineage`'s already-tested resolution, changes only when
  * the column's resolved source set, `DerivationKind`, or aggregation set
  * changes — so a literal changing (`amount * 1.20` → `amount * 1.25`)
  * moves `expression` but not `lineage` (same source, same `Computed`
  * derivation), while a `UDF` newly appearing in the chain moves both.
  * `combined` — `hash(expression ++ lineage)` — is the practical "did this
  * column change" signal a consumer should treat as primary.
  *
  * `nonDeterministic` is metadata only (see `NonDeterminism`'s own doc) —
  * never folded into `expression`/`lineage`/`combined`.
  */
final case class OutputFingerprint(
  expression: Fingerprint,
  lineage: Fingerprint,
  combined: Fingerprint,
  nonDeterministic: Option[Boolean]
) {
  def toMap: Map[String, Any] = Map(
    "expression" -> expression.toMap,
    "lineage" -> lineage.toMap,
    "combined" -> combined.toMap,
    "nonDeterministic" -> nonDeterministic
  )
}

/** The full fingerprint hierarchy for one transformation — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §3. `overall` covers the entire
  * plan (including, for a `Write`, its `format`/`saveMode`/`dataset`,
  * which never influence any individual `outputs` entry); `inputs` is
  * keyed `"<location>#<occurrenceIndex>"` so two occurrences of the same
  * physical dataset (a self-join) never collapse into one entry; `outputs`
  * is keyed by declared output column name.
  */
final case class TransformationFingerprint(
  version: Int,
  overall: Fingerprint,
  inputs: Map[String, Fingerprint],
  outputs: Map[String, OutputFingerprint]
) {
  def toMap: Map[String, Any] = Map(
    "version" -> version,
    "overall" -> overall.toMap,
    "inputs" -> inputs.map { case (k, v) => k -> v.toMap },
    "outputs" -> outputs.map { case (k, v) => k -> v.toMap }
  )
}

/** The single entry point this module exists to provide: turn a real
  * `ir.Plan` into a `TransformationFingerprint`. Composes
  * `Canonicalizer`/`FingerprintHasher`/`ir.Lineage` — see each's own doc
  * for the underlying rules; this object only wires them together at the
  * granularities docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §3 specifies.
  */
object TransformationFingerprinter {

  def fingerprint(plan: Plan): TransformationFingerprint = {
    val scopeInfo = Canonicalizer.buildScopeInfo(plan)
    val substitution = scopeInfo.substitution

    val overall = FingerprintHasher.hash(Canonicalizer.canonicalizePlan(plan, substitution))

    val inputs = scopeInfo.reads
      .map(r => r.inputKey -> FingerprintHasher.hash(CTag("Read", List(CanonicalNode.stringLeaf(r.location)))))
      .toMap

    val resolvedExprs = Canonicalizer.resolvedOutputs(plan)
    val lineages = Lineage.trace(plan)

    val outputs = lineages.map { columnLineage =>
      val name = columnLineage.output.name
      val resolvedExprOpt = resolvedExprs.get(name)
      // The getOrElse fallback is defensive, not expected to be reachable
      // for any real plan: resolvedExprs and lineages are built from two
      // structurally parallel top-level dispatches over the same plan
      // (see Canonicalizer.resolvedOutputs' own doc), so every name
      // ir.Lineage.trace produces already has a matching entry here by
      // construction. Kept rather than a bare `.get` to fail safe (a
      // distinctly-tagged fingerprint, not an exception) if that parallel
      // structure is ever broken by a future change to either dispatch.
      val exprNode = resolvedExprOpt.map(Canonicalizer.canonicalizeExpr(_, substitution)).getOrElse(CTag("NoExpression"))
      val lineageNode = Canonicalizer.canonicalizeLineage(columnLineage, substitution)

      val exprFingerprint = FingerprintHasher.hash(exprNode)
      val lineageFingerprint = FingerprintHasher.hash(lineageNode)
      val combinedFingerprint = FingerprintHasher.hash(CTag("Combined", List(exprNode, lineageNode)))
      val nonDeterministic = resolvedExprOpt.flatMap(NonDeterminism.classify)

      name -> OutputFingerprint(exprFingerprint, lineageFingerprint, combinedFingerprint, nonDeterministic)
    }.toMap

    TransformationFingerprint(FingerprintHasher.CurrentVersion, overall, inputs, outputs)
  }
}
