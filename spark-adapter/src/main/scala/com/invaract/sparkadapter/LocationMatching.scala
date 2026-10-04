// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

/** The one definition of "does a contract's declared location match a location
  * Spark reported" — `StructuralVerifier.locationsMatch` and
  * `normalizeSparkLocation` delegate here, and `LocationIndex` below is built
  * from the same two pure functions, so the indexed and the one-off paths cannot
  * disagree (`LocationMatchingSpec` also checks them against each other on
  * generated paths).
  *
  * The rule: strip a `file:` scheme from the actual side and turn `\` into `/`
  * on both sides (a contract authored on Windows still matches Spark's
  * forward-slash paths), then a declared location matches if it equals the
  * actual one or is a `/`-boundary suffix of it. The matching rule itself is
  * deliberately unchanged here — only where the work happens moved.
  */
private[sparkadapter] object LocationMatching {

  /** The declared side only needs separators normalized; it is authored, not
    * reported by Spark, so it never carries a `file:` scheme to strip.
    */
  def normalizeDeclared(declared: String): String = declared.replace('\\', '/')

  /** The bare, OS-agnostic form of a location as Spark itself reports it. */
  def normalizeActual(actual: String): String = actual.stripPrefix("file:").replace('\\', '/')

  def matchesNormalized(normalizedDeclared: String, normalizedActual: String): Boolean =
    normalizedActual == normalizedDeclared || normalizedActual.endsWith("/" + normalizedDeclared)

  def matches(declared: String, actual: String): Boolean =
    matchesNormalized(normalizeDeclared(declared), normalizeActual(actual))

  /** The text after the last `/`. A declared location can only match an actual
    * one that ends with it, so the two always share this segment — which is what
    * lets `LocationIndex` look candidates up instead of testing every pair.
    */
  private[sparkadapter] def lastSegment(normalized: String): String = normalized.substring(normalized.lastIndexOf('/') + 1)
}

/** Declared locations, normalized once and bucketed by their last path segment,
  * so finding the entries an actual location matches costs one hash lookup plus
  * a suffix test per same-named candidate, instead of normalizing and testing
  * every declared location for every actual one. A contract with hundreds of
  * datasets checked against a plan with hundreds of reads was `inputs × reads`
  * string normalizations per check (several times over); this is `reads` lookups.
  *
  * Results are always in entry order — the same order a linear scan with
  * `LocationMatching.matches` would produce — so a caller's output stays
  * deterministic (see `StructuralVerifier`'s "Determinism" doc).
  */
private[sparkadapter] final class LocationIndex[A] private (private val entries: Vector[(String, A)]) {
  private val normalized: Vector[String] = entries.map { case (declared, _) => LocationMatching.normalizeDeclared(declared) }
  private val bySegment: Map[String, Vector[Int]] =
    normalized.indices.groupBy(i => LocationMatching.lastSegment(normalized(i))).map { case (segment, idx) => segment -> idx.toVector.sorted }

  /** Positions (ascending) of every entry whose declared location matches `actual`. */
  def matchingIndices(actual: String): Vector[Int] = {
    val normalizedActual = LocationMatching.normalizeActual(actual)
    bySegment
      .getOrElse(LocationMatching.lastSegment(normalizedActual), Vector.empty)
      .filter(i => LocationMatching.matchesNormalized(normalized(i), normalizedActual))
  }

  def matching(actual: String): List[A] = matchingIndices(actual).map(entries(_)._2).toList

  def first(actual: String): Option[A] = matchingIndices(actual).headOption.map(entries(_)._2)

  def matchesAny(actual: String): Boolean = matchingIndices(actual).nonEmpty
}

private[sparkadapter] object LocationIndex {
  def apply[A](entries: Seq[(String, A)]): LocationIndex[A] = new LocationIndex(entries.toVector)
}
