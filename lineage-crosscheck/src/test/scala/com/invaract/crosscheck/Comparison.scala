// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

/** How Invaract's lineage of one write differs from one reference's: one line per difference, sorted, with the job's
  * own directory written `<out>` and the shared inputs' `<in>` so the lines are the same on every run and machine. */
object Comparison {

  def against(reference: Reference, observed: Observed, in: String): List[String] = {
    val out = observed.dir
    val ours = observed.invaract
    val theirs = reference match {
      case Reference.OpenLineageSpark => observed.openLineageSpark
      case Reference.Spline           => observed.spline
    }
    val name = reference.name
    def stable(s: String) = s.replace(out, "<out>").replace(in, "<in>")
    def sides[A](what: String, a: Set[A], b: Set[A]): List[String] =
      (a -- b).toList.map(x => s"$what: only invaract: ${stable(x.toString)}") ++
        (b -- a).toList.map(x => s"$what: only $name: ${stable(x.toString)}")
    val datasets =
      sides("output", ours.output.toSet, theirs.output.toSet) ++ sides("input", ours.inputs, theirs.inputs)
    val lines = reference match {
      case Reference.OpenLineageSpark =>
        def subtypes[K](what: String, a: Map[K, Set[String]], b: Map[K, Set[String]]) =
          (a.keySet intersect b.keySet).toList.filter(k => a(k) != b(k)).map { k =>
            s"$what subtype: ${stable(k.toString)}: invaract ${a(k).toList.sorted.mkString(",")}, $name ${b(k).toList.sorted.mkString(",")}"
          }
        datasets ++
          sides("direct", ours.direct.keySet, theirs.direct.keySet) ++ subtypes("direct", ours.direct, theirs.direct) ++
          sides("indirect", ours.indirect.keySet, theirs.indirect.keySet) ++ subtypes("indirect", ours.indirect, theirs.indirect)
      case Reference.Spline =>
        // Spline does not classify edges: every column an output column's expression refers to is a dependency of it,
        // a CASE WHEN's condition and a window's partitioning included. Its indirect lineage is the columns a Filter,
        // Join, Aggregate's grouping or Sort refers to, for the whole output.
        val ourDependencies = ours.direct.keySet ++ ours.indirect.keySet.collect { case Indirect(Some(f), d, c) => Direct(f, d, c) }
        datasets ++
          sides("dependency", ourDependencies, theirs.direct.keySet) ++
          sides("indirect", ours.indirect.keySet.filter(_.field.isEmpty), theirs.indirect.keySet)
    }
    lines.distinct.sorted
  }
}
