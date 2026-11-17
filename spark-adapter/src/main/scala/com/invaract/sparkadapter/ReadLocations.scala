// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.Locations
import com.invaract.verification.LocationMatching

/** Where a file read is checked as having read (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-03). Pure functions over
  * location strings; `SparkPlanAdapter` and `ContractEnforcementRule` apply them to a relation's root paths.
  *
  * Two rules:
  *   - **Every root path is a read.** A read of several paths (`spark.read.parquet(a, b)`) or of a glob Spark expands
  *     into several (`.../in/orders*`) reads each of them. Only the first used to be reported, so a second dataset read
  *     in the same call was invisible to `rejectUndeclaredInputs`.
  *   - **A read inside a declared input is a read of that input.** A partition folder (`.../orders/dt=2024-01-01`), a
  *     single file, or a glob of files inside a declared input is reported at the declared input's location. It used to
  *     be a different place, so the commonest way to read a partitioned dataset could not be put under a contract at
  *     all (`MISSING_INPUT`).
  *
  * Outputs are not affected: a write into a sub-folder of a declared output is still somewhere else.
  */
private[sparkadapter] object ReadLocations {

  /** `actual` itself if a declared location names it, or if it lies under none; otherwise the longest leading part of
    * it, in whole segments, that a declared location names. A leading part followed by a `..` segment is never used:
    * what the path climbs out of is not known from the text.
    */
  def attribute(actual: String, declared: Seq[String]): String =
    if (declared.isEmpty || declared.exists(d => LocationMatching.matches(d, actual))) actual
    else {
      val (root, segments) = split(Locations.canonical(actual))
      (segments.length - 1 to 1 by -1).iterator
        .filterNot(k => segments.drop(k).contains(".."))
        .map(k => root + segments.take(k).mkString("/"))
        .find(prefix => declared.exists(d => LocationMatching.matches(d, prefix)))
        .getOrElse(actual)
    }

  /** Every distinct location a relation with these root paths reads, attributed, in first-seen order. */
  def of(roots: Seq[String], declared: Seq[String]): List[String] = roots.map(attribute(_, declared)).toList.distinct

  /** The dataset a root path belongs to, for a dry run's draft (which has no contract to attribute to): a file's folder,
    * then without trailing `name=value` partition folders. Reading `.../orders/dt=2024-01-01` today drafts the input at
    * `.../orders`, so the draft still holds tomorrow, when the job reads `dt=2024-01-02`.
    */
  def datasetRoot(root: String, isFile: Boolean): String = {
    val (prefix, segments) = split(Locations.canonical(root))
    val folder = if (isFile && segments.nonEmpty) segments.dropRight(1) else segments
    val dataset = folder.reverse.dropWhile(isPartitionFolder).reverse
    if (dataset.isEmpty) Locations.canonical(root) else prefix + dataset.mkString("/")
  }

  private def isPartitionFolder(segment: String): Boolean = {
    val eq = segment.indexOf('=')
    eq > 0 && eq < segment.length - 1
  }

  private val UrlRoot = """^([A-Za-z][A-Za-z0-9+.-]+://)(.*)$""".r

  /** A canonical location as what is above its first segment (a URL's scheme, `/`, or nothing) and its segments. */
  private def split(canonical: String): (String, List[String]) = canonical match {
    case UrlRoot(scheme, rest)          => (scheme, rest.split('/').filter(_.nonEmpty).toList)
    case path if path.startsWith("/") => ("/", path.split('/').filter(_.nonEmpty).toList)
    case path                          => ("", path.split('/').filter(_.nonEmpty).toList)
  }
}
