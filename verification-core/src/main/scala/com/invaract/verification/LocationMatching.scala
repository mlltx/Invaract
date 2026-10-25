// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.Locations
import com.invaract.ir.CatalogIdentity

/** The one definition of "does a contract's declared location match a location the
  * engine reported" - `StructuralVerifier.locationsMatch` and `normalizeLocation`
  * delegate here, and `LocationIndex` below is built from the same two pure functions,
  * so the indexed and the one-off paths cannot disagree (`LocationMatchingSpec` also
  * checks them against each other on generated paths).
  *
  * **The canonical location form.** A location is a `/`-separated path or identifier.
  * An adapter whose engine names data some other way converts it to that form before
  * handing it to the pipeline - a table `project.dataset.table` becomes
  * `project/dataset/table` - and a contract author writes the same form. The rule does
  * not know about engines; it only compares canonical strings, which is what lets one
  * contract mean the same thing on every adapter.
  *
  * The rule: put both sides in `contract.Locations`' canonical form (`\` becomes `/`, a
  * `file:` scheme is dropped, `s3a://` is `s3://`, repeated, trailing and `./` segments go -
  * see there), then a declared location matches if it equals the actual one or is a
  * `/`-boundary suffix of it. So an absolute declared
  * location (`/data/orders`, `gs://bucket/orders`) matches only that exact location,
  * while a relative one (`orders`, `sales/orders`) matches any location that ends in
  * it - deliberately, so a contract need not know a deployment's root. The consequence
  * to design for: a relative declaration cannot tell two tenants' `.../orders` apart,
  * so a contract that must pin one declares it absolute.
  */
private[invaract] object LocationMatching {

  /** The canonical form of a name an engine addresses by parts rather than by path: BigQuery's
    * `project`, `dataset`, `table`, a Hive-style `database`, `table`, a Beam resource's segments.
    * Each part loses surrounding whitespace and one pair of backticks or double quotes (the
    * quoting the engine's own SQL uses), empty parts are dropped, and the rest are joined with
    * `/`: `Seq("proj", "ds", "orders")` is `proj/ds/orders`. An adapter calls this where it
    * reports a location, so every adapter spells it the same way and a contract author writes the
    * same form: `ds/orders` matches that table in any project, `proj/ds/orders` only in `proj`.
    *
    * A part is taken as given - the adapter splits the engine's identifier by the engine's own
    * rules (a BigQuery legacy project id can contain `.` and `:`), because only the engine knows
    * where one part ends.
    */
  def fromParts(parts: Seq[String]): String =
    parts.map(unquote).filter(_.nonEmpty).mkString("/")

  private def unquote(part: String): String = {
    val trimmed = part.trim
    if (trimmed.length >= 2 && ((trimmed.head == '`' && trimmed.last == '`') || (trimmed.head == '"' && trimmed.last == '"')))
      trimmed.substring(1, trimmed.length - 1).trim
    else trimmed
  }

  /** A declared location in canonical form (`contract.Locations.canonical`). */
  def normalizeDeclared(declared: String): String = Locations.canonical(declared)

  /** A location an engine reported, in the same canonical form: the two sides are put in one form, so how either is spelt
    * never decides a match. */
  def normalizeActual(actual: String): String = Locations.canonical(actual)

  def matchesNormalized(normalizedDeclared: String, normalizedActual: String): Boolean =
    normalizedActual == normalizedDeclared || normalizedActual.endsWith("/" + normalizedDeclared)

  def matches(declared: String, actual: String): Boolean =
    matchesNormalized(normalizeDeclared(declared), normalizeActual(actual))

  /** The name a catalog table goes by, whatever location the engine reports for it: its catalog, namespace and table
    * joined by `fromParts` (`spark_catalog/gold/orders`), so a contract can name the table (`gold/orders`, or just `orders`)
    * rather than where its files happen to be. `None` for an identity with no table. */
  def tableName(catalog: CatalogIdentity): Option[String] =
    catalog.table.map(table => fromParts(catalog.catalogName.toSeq ++ catalog.namespace :+ table))

  /** Every name a dataset answers to: the location the engine reported and, for a catalog table, its table name. A
    * declared location matches the dataset when it matches any of them. An engine reports one table two ways (Spark: the
    * table's files for an existing table, its qualified name for one being created), and the table name is the same in
    * both, so a contract that names the table matches either way. */
  def namesOf(location: String, catalog: Option[CatalogIdentity]): List[String] =
    (location :: catalog.flatMap(tableName).toList).distinct

  /** `declared` against every one of a dataset's `names` (`namesOf`). */
  def matchesAny(declared: String, names: Seq[String]): Boolean = names.exists(matches(declared, _))

  /** The text after the last `/`. A declared location can only match an actual
    * one that ends with it, so the two always share this segment — which is what
    * lets `LocationIndex` look candidates up instead of testing every pair.
    */
  private[invaract] def lastSegment(normalized: String): String = normalized.substring(normalized.lastIndexOf('/') + 1)
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
private[invaract] final class LocationIndex[A] private (private val entries: Vector[(String, A)]) {
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

  /** Positions (ascending, each once) of every entry whose declared location matches any of a dataset's `names`
    * (`LocationMatching.namesOf`). */
  def matchingIndicesOf(names: Seq[String]): Vector[Int] = names.flatMap(matchingIndices).distinct.sorted.toVector

  def firstOf(names: Seq[String]): Option[A] = matchingIndicesOf(names).headOption.map(entries(_)._2)

  def matchesAnyOf(names: Seq[String]): Boolean = names.exists(matchesAny)
}

private[invaract] object LocationIndex {
  def apply[A](entries: Seq[(String, A)]): LocationIndex[A] = new LocationIndex(entries.toVector)
}
