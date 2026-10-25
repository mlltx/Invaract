// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability}

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class ScenarioCatalogueTest extends AnyFunSuite {

  /** The reference declaration with some capabilities re-declared: `id -> (status, note)`. */
  private def declaring(adapter: String, overrides: Map[String, (String, Option[String])]): AdapterCapabilities = {
    val base = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
    val rewritten = new StringBuilder
    var skip = false
    base.replace("adapter: reference", s"adapter: $adapter").split("\n").foreach { line =>
      val key = line.trim.stripSuffix(":")
      if (line.startsWith("  ") && !line.startsWith("    ") && line.trim.endsWith(":")) {
        overrides.get(key) match {
          case Some((status, note)) =>
            rewritten.append(s"  $key:\n    status: $status\n")
            note.foreach(n => rewritten.append(s"    note: $n\n"))
            skip = true
          case None => rewritten.append(line + "\n"); skip = false
        }
      } else if (!skip) rewritten.append(line + "\n")
    }
    AdapterCapabilities.parse(rewritten.toString).fold(e => throw new AssertionError(e.mkString("; ")), identity)
  }

  private val reference = ReferenceAdapter.declared

  /** Table rows (not the header or the rule), split into cells on unescaped pipes. */
  private def rows(table: String): List[List[String]] =
    table.split("\n").toList.drop(2).map(_.split("(?<!\\\\)\\|", -1).toList.drop(1).dropRight(1).map(_.trim))

  test("without a declaration the table lists every scenario once, in the catalogue's order, with empty columns to fill in") {
    val table = ScenarioCatalogue.render(None)
    val header = table.split("\n").head
    assert(header == "| # | Scenario | What it means for a user | What it checks | Evidence for | Neutral result | Forecast | Why |")
    val body = rows(table)
    assert(body.map(_(1)) == Scenarios.all.map(s => s"`${s.id}`"))
    assert(body.map(_.head) == (1 to Scenarios.all.size).map(_.toString).toList)
    assert(body.forall(_.size == 8), "every row has the same number of cells as the header")
    assert(body.forall(r => r(6).isEmpty && r(7).isEmpty))
  }

  test("each row says what the scenario checks, the capabilities it is evidence for, and the neutral result") {
    val byId = rows(ScenarioCatalogue.render(None)).map(r => r(1).stripPrefix("`").stripSuffix("`") -> r).toMap
    val format = byId("output-format-mismatch")
    val mismatch = Scenarios.all.find(_.id == "output-format-mismatch").get
    assert(format(2) == mismatch.userStory)
    assert(format(3) == mismatch.description)
    assert(format(4) == "`check.format`")
    assert(format(5) == "blocked: `OUTPUT_FORMAT_MISMATCH`")
    assert(byId("conforming-write")(5) == "allowed")
    // a scenario with several capabilities in focus lists them all, in a stable order
    val several = Scenarios.all.find(_.focus.size > 1).get
    assert(byId(several.id)(4) == several.focus.toList.sortBy(_.id).map(c => s"`${c.id}`").mkString(", "))
  }

  test("every scenario says in plain words what it means for the team whose job it is, and no two say the same") {
    val stories = Scenarios.all.map(_.userStory)
    assert(stories.forall(_.trim.nonEmpty))
    assert(stories.distinct.size == stories.size, "two scenarios share a story")
    // a story is for a reader outside the kit, so it must not just restate the technical description
    assert(Scenarios.all.forall(s => s.userStory != s.description))
    // plain words: no violation-type constants or capability ids leaking into the story
    assert(Scenarios.all.forall(s => !"[A-Z]{3,}_[A-Z_]+".r.pattern.matcher(s.userStory).find()), "a story names a violation type")
    assert(Scenarios.all.forall(s => !s.userStory.contains("`")), "a story carries code formatting")
  }

  test("a scenario without a story cannot be built") {
    val e = intercept[IllegalArgumentException](Scenarios.all.head.copy(userStory = "  "))
    assert(e.getMessage.contains("conforming-write"))
  }

  test("a violation list is sorted so the table does not change between runs") {
    val several = Scenarios.all.head.copy(expect = Expectation.Reject(Set("ZED", "ALPHA", "MIKE")))
    assert(ScenarioCatalogue.neutral(several) == "blocked: `ALPHA`, `MIKE`, `ZED`")
    assert(ScenarioCatalogue.neutral(Scenarios.all.find(_.id == "conforming-write").get) == "allowed")
    assert(ScenarioCatalogue.describeBlock(Set("ZED", "ALPHA")) == "blocked: `ALPHA`, `ZED`")
  }

  test("with a declaration the table adds what the kit expects of that adapter, by name") {
    val table = ScenarioCatalogue.render(Some(declaring("sketch", Map.empty)))
    assert(table.split("\n").head == "| # | Scenario | What it means for a user | What it checks | Evidence for | Neutral result | What the kit expects of `sketch` | Forecast | Why |")
    assert(rows(table).forall(_.size == 9))
  }

  test("for an adapter that supports everything the kit expects exactly the neutral result on every scenario") {
    Scenarios.all.foreach { s =>
      val expected = ScenarioCatalogue.forecast(s, reference)
      s.expect match {
        case Expectation.Pass          => assert(expected == ScenarioCatalogue.Forecast.Allows, s.id)
        case Expectation.Reject(types) => assert(expected == ScenarioCatalogue.Forecast.Blocks(types), s.id)
      }
    }
  }

  test("a capability declared unsupported makes the scenarios that need it a safe refusal, and says so in the table") {
    val caps = declaring("sketch", Map("check.format" -> (("unsupported", Some("this engine has no format concept")))))
    val formatScenario = Scenarios.all.find(_.id == "output-format-matches").get
    assert(ScenarioCatalogue.forecast(formatScenario, caps) == ScenarioCatalogue.Forecast.RefusesAsUnsupported)
    val row = rows(ScenarioCatalogue.render(Some(caps))).find(_(1) == "`output-format-matches`").get
    assert(row(6) == "must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`)")
  }

  test("a capability that does not apply skips the scenarios that need it, with the adapter's own reason, escaped for a table") {
    val caps = declaring("sketch", Map("write.batch" -> (("not-applicable", Some("the engine only streams | never batches")))))
    val scenario = Scenarios.all.find(_.operations.contains(Capability.WriteBatch)).get
    ScenarioCatalogue.forecast(scenario, caps) match {
      case ScenarioCatalogue.Forecast.Skipped(reason) => assert(reason.contains("the engine only streams"))
      case other                                      => fail(s"expected a skip, got $other")
    }
    val table = ScenarioCatalogue.render(Some(caps))
    assert(table.contains("skipped: ") && table.contains("streams \\| never batches"))
    assert(rows(table).forall(_.size == 9), "an escaped pipe does not add a cell")
  }

  test("the forecast is the kit's own rule: for several declarations it agrees with what Conformance does to a real adapter") {
    val declarations = List(
      declaring("a", Map.empty),
      declaring("b", Map("write.batch" -> (("not-applicable", Some("streams only"))))),
      declaring("c", Map("read.batch" -> (("unsupported", Some("push-only sources"))))),
      declaring("d", Map("check.format" -> (("unsupported", Some("no formats")))))
    )
    declarations.foreach { caps =>
      val report = Conformance.evaluate(new ReferenceAdapter(caps))
      assert(report.failures == Nil, caps.adapter)
      val skippedByKit = report.skipped.map(_.scenario.id).toSet
      val skippedByForecast = Scenarios.all.filter(s => ScenarioCatalogue.forecast(s, caps).isInstanceOf[ScenarioCatalogue.Forecast.Skipped]).map(_.id).toSet
      assert(skippedByForecast == skippedByKit, caps.adapter)
      val refusals = Scenarios.all.filter(s => ScenarioCatalogue.forecast(s, caps) == ScenarioCatalogue.Forecast.RefusesAsUnsupported).map(_.id)
      assert(refusals.forall(id => report.conforming.exists(_.scenario.id == id)), s"${caps.adapter}: a refusal the kit expects is one the reference adapter gives")
    }
  }

  test("the summary counts what the catalogue is evidence for, attests, and only uses as operations") {
    val summary = ScenarioCatalogue.coverageSummary
    val focused = Scenarios.all.flatMap(_.focus).toSet.size
    assert(summary.startsWith(s"${Scenarios.all.size} scenarios; $focused capabilities are the focus of at least one; ${Scenarios.attested.size} are attested"))
    val operationsOnly = Capability.all.size - focused - Scenarios.attested.size
    assert(summary.contains(s"; $operationsOnly are exercised only as operations a job needs."))
  }

  test("main writes the table to the given path, with or without a declaration, and refuses anything else") {
    val dir = Files.createTempDirectory("catalogue")
    try {
      val out = dir.resolve("table.md")
      ScenarioCatalogue.main(Array(out.toString))
      assert(new String(Files.readAllBytes(out), StandardCharsets.UTF_8) == ScenarioCatalogue.render(None))

      val declaration = dir.resolve("invaract-capabilities-sketch.yaml")
      val yaml = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("reference/invaract-capabilities-reference.yaml"), "UTF-8").mkString
      Files.write(declaration, yaml.replace("adapter: reference", "adapter: sketch").getBytes(StandardCharsets.UTF_8))
      ScenarioCatalogue.main(Array(out.toString, declaration.toString))
      assert(new String(Files.readAllBytes(out), StandardCharsets.UTF_8).contains("What the kit expects of `sketch`"))

      intercept[IllegalArgumentException](ScenarioCatalogue.main(Array.empty[String]))
      intercept[IllegalArgumentException](ScenarioCatalogue.main(Array(out.toString, declaration.toString, "extra")))

      val broken = dir.resolve("broken.yaml")
      Files.write(broken, "adapter: x\n".getBytes(StandardCharsets.UTF_8))
      val thrown = intercept[IllegalArgumentException](ScenarioCatalogue.main(Array(out.toString, broken.toString)))
      assert(thrown.getMessage.contains(broken.toString) && thrown.getMessage.contains("not a valid declaration"))
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder[java.nio.file.Path]()).forEach(p => Files.delete(p))
    }
  }
}
