// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** "Did you mean ...?" for a value or key a person probably mistyped: the known spelling closest to it, when it is close
  * enough to be a typo rather than a different word.
  */
object Suggestions {

  /** The number of single-character insertions, deletions and substitutions that turn `a` into `b`. */
  def distance(a: String, b: String): Int = {
    var previous = Array.tabulate(b.length + 1)(identity)
    for (i <- 1 to a.length) {
      val current = new Array[Int](b.length + 1)
      current(0) = i
      for (j <- 1 to b.length) {
        val substitution = previous(j - 1) + (if (a(i - 1) == b(j - 1)) 0 else 1)
        current(j) = math.min(substitution, math.min(previous(j), current(j - 1)) + 1)
      }
      previous = current
    }
    previous(b.length)
  }

  /** The candidate closest to `value`, ignoring case, if it is at most one edit away (two, for a value longer than four
    * characters) and is not `value` itself. Ties go to the alphabetically first candidate, so the answer never depends on
    * a collection's order.
    */
  def closest(value: String, candidates: Iterable[String]): Option[String] = {
    val lower = value.trim.toLowerCase
    val allowed = if (lower.length > 4) 2 else 1
    candidates.toList
      .filter(_ != value)
      .map(c => (distance(lower, c.toLowerCase), c))
      .filter(_._1 <= allowed)
      .sorted
      .headOption
      .map(_._2)
  }

  /** `" Did you mean 'x'?"`, or `""` when nothing is close. */
  def didYouMean(value: String, candidates: Iterable[String]): String =
    closest(value, candidates).fold("")(c => s" Did you mean '$c'?")
}
