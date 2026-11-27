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

/** What one tool reported about a write, one line per dataset and edge, in the same stable form as the differences:
  * the baseline pins Invaract's (so lineage that was right cannot change unseen, even where every tool agrees) and the
  * coverage page shows it beside the job's code, for a reader to check. */
object Description {
  def of(captured: Captured, out: String, in: String): List[String] = {
    def stable(s: String) = s.replace(out, "<out>").replace(in, "<in>")
    def kinds(subtypes: Set[String]) = if (subtypes.isEmpty) "" else subtypes.toList.sorted.mkString("  [", ", ", "]")
    captured.inputs.toList.map(d => s"input:  ${stable(d.toString)}").sorted ++
      captured.output.toList.map(d => s"output: ${stable(d.toString)}") ++
      captured.direct.toList.map { case (e, s) => s"${stable(e.toString)}${kinds(s)}" }.sorted ++
      captured.indirect.toList.map { case (e, s) => s"${stable(e.toString)}${kinds(s)}" }.sorted
  }
}

/** A corpus job's code as it is written in Corpus.scala, for the coverage page: read from the source, so the page shows
  * exactly what ran. A `job(...)(body)` is its body; a `Job(...)` its `run` function. */
object CorpusSource {
  private lazy val text = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("src/test/scala/com/invaract/crosscheck/Corpus.scala")), "UTF-8")

  /** The index just past the bracket closing the one at `open`, skipping string literals. */
  private def closing(open: Int): Int = {
    var depth = 0
    var i = open
    var quoted = false
    while (i < text.length) {
      val c = text.charAt(i)
      if (quoted) { if (c == '\\') i += 1 else if (c == '"') quoted = false }
      else if (c == '"') quoted = true
      else if (c == '(' || c == '{') depth += 1
      else if (c == ')' || c == '}') { depth -= 1; if (depth == 0) return i + 1 }
      i += 1
    }
    sys.error(s"unbalanced brackets in Corpus.scala from offset $open")
  }

  /** The top-level arguments of the bracket at `open`, as written. */
  private def arguments(open: Int): List[String] = {
    val end = closing(open) - 1
    val parts = scala.collection.mutable.ListBuffer.empty[String]
    var start = open + 1
    var i = start
    while (i < end) {
      val c = text.charAt(i)
      if (c == '"') { var j = i + 1; while (text.charAt(j) != '"') { if (text.charAt(j) == '\\') j += 1; j += 1 }; i = j }
      else if (c == '(' || c == '{') i = closing(i) - 1
      else if (c == ',') { parts += text.substring(start, i); start = i + 1 }
      i += 1
    }
    (parts += text.substring(start, end)).toList.map(_.trim)
  }

  /** A function's body, its indentation made the code's own: a block's statements flush left, an expression's
    * continuation lines indented under its first. */
  private def lambdaBody(f: String): String = {
    val raw = f.substring(f.indexOf("=>") + 2).trim
    val block = raw.startsWith("{") && raw.endsWith("}")
    val lines = (if (block) raw.substring(1, raw.length - 1) else raw).split("\n").toList.map(_.replaceAll("\\s+$", ""))
    val kept = lines.dropWhile(_.trim.isEmpty).reverse.dropWhile(_.trim.isEmpty).reverse
    def dedent(ls: List[String]) = {
      val n = ls.filter(_.trim.nonEmpty).map(_.takeWhile(_ == ' ').length).reduceOption(_ min _).getOrElse(0)
      ls.map(_.drop(n))
    }
    (if (block) dedent(kept) else kept.headOption.map(_.trim).toList ++ dedent(kept.drop(1)).map("  " + _)).mkString("\n")
  }

  def code(id: String): String = {
    val small = text.indexOf(s"""job("$id",""")
    if (small >= 0) {
      val args = text.indexOf('(', small)
      val body = closing(args)
      lambdaBody(text.substring(body + 1, closing(body) - 1))
    } else {
      val big = text.indexOf(s"""Job("$id",""")
      require(big >= 0, s"job $id is not in Corpus.scala")
      lambdaBody(arguments(text.indexOf('(', big))(3))
    }
  }
}
