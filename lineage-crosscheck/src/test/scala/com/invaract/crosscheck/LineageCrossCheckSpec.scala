// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths => JPaths}

/** Runs the corpus (Corpus.jobs), each job with Invaract, openlineage-spark and Spline capturing it, and holds the
  * result to what is checked in:
  *
  *   - each job's differences from each reference are explained (`Job.notes`), and a job that now agrees has no note left;
  *   - the differences are exactly those in crosscheck-baseline.txt, so a change in Invaract's lineage (or in a
  *     reference's) never passes unseen;
  *   - Invaract's own lineage of every job is the baseline's, so lineage that was right cannot change unseen even
  *     where every tool agrees;
  *   - docs-site's "Spark Column Lineage Coverage" page is the one this run generates.
  *
  * `CROSSCHECK_UPDATE=true sbt test` rewrites the baseline and the page from the run (the notes still have to be right).
  */
class LineageCrossCheckSpec extends AnyFunSuite {
  private lazy val fixture = Fixture.create()
  private val results = scala.collection.mutable.LinkedHashMap.empty[Job, Outcome]
  private val update = sys.env.get("CROSSCHECK_UPDATE").contains("true")

  private val baselineFile: Path = JPaths.get("src/test/resources/crosscheck-baseline.txt")
  private val pageFile: Path = JPaths.get("../docs-site/src/content/docs/reference/spark-column-lineage.md")
  private val versions = "openlineage-spark 1.55.0, Spline agent 2.4.0, Spark " + org.apache.spark.SPARK_VERSION

  for (job <- Corpus.jobs) test(s"${job.id}: ${job.description}") {
    val observed = Harness.run(job, fixture)
    assert(observed.refused.isEmpty, s"Invaract refused the job's write, so it published no lineage of it: ${observed.refused.getOrElse("")}")
    assert(observed.invaract.output.isDefined, "Invaract published nothing for the write")
    // a reference publishing nothing for a write is a difference like any other: it needs a note saying why
    val in = Harness.slashes(fixture.root.resolve("inputs"))
    val diffs = Reference.all.map(r => r -> Comparison.against(r, observed, in)).toMap
    results(job) = Outcome(job, diffs, Description.of(observed.invaract, observed.dir, in))
    for (reference <- Reference.all) {
      val lines = diffs(reference)
      if (lines.nonEmpty) info(s"vs ${reference.name}:\n  " + lines.mkString("\n  "))
      if (lines.nonEmpty)
        assert(job.notes.contains(reference), s"differs from ${reference.name}, and no note in Corpus.scala says why:\n  " + lines.mkString("\n  "))
      else
        assert(!job.notes.contains(reference), s"agrees with ${reference.name} now: remove its note from Corpus.scala")
    }
  }

  test("the differences are the baseline's, and the coverage page is this run's") {
    assert(results.size == Corpus.jobs.size, "every job ran")
    val ordered = Corpus.jobs.map(results)
    val baseline = Report.baseline(ordered)
    val page = Report.page(ordered, versions)
    def read(p: Path) = if (Files.exists(p)) new String(Files.readAllBytes(p), StandardCharsets.UTF_8) else ""
    if (update) {
      Files.createDirectories(baselineFile.getParent)
      Files.write(baselineFile, baseline.getBytes(StandardCharsets.UTF_8))
      Files.write(pageFile, page.getBytes(StandardCharsets.UTF_8))
    } else {
      val was = read(baselineFile).split("\n").toSet
      val is = baseline.split("\n").toSet
      assert(read(baselineFile) == baseline,
        "Invaract's lineage, or a reference's, changed since the baseline:\n" +
          (was -- is).toList.sorted.map("- " + _).mkString("\n") + "\n" + (is -- was).toList.sorted.map("+ " + _).mkString("\n") +
          "\nIf the change is right, explain it in Corpus.scala and run CROSSCHECK_UPDATE=true sbt test.")
      assert(read(pageFile) == page, s"$pageFile is out of date: run CROSSCHECK_UPDATE=true sbt test")
    }
  }
}
