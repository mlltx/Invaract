// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class LocationMatchingSpec extends AnyFunSuite {

  // The rule exactly as `StructuralVerifier.locationsMatch` was written before it
  // moved to `LocationMatching`: the reference every indexed lookup must agree with.
  private def reference(declared: String, actual: String): Boolean = {
    val d = declared.replace('\\', '/')
    val a = actual.stripPrefix("file:").replace('\\', '/')
    a == d || a.endsWith("/" + d)
  }

  test("matches: equal, suffix on a path boundary, and the file: scheme / backslash normalization") {
    assert(LocationMatching.matches("data/out", "data/out"))
    assert(LocationMatching.matches("data/out", "file:/home/u/data/out"))
    assert(LocationMatching.matches("out", "s3://bucket/data/out"))
    assert(LocationMatching.matches("C:\\work\\out.parquet", "file:/C:/work/out.parquet"))
    assert(LocationMatching.matches("data\\out", "/x/data/out"))
    assert(LocationMatching.matches("/abs/out", "file:/abs/out"))
  }

  test("matches: a suffix must start on a path boundary, and a longer declared path never matches a shorter actual one") {
    assert(!LocationMatching.matches("orders", "s3://b/xorders"))
    assert(!LocationMatching.matches("data/out", "out"))
    assert(!LocationMatching.matches("a/orders", "b/orders"))
    assert(!LocationMatching.matches("out", "data/out/"))
    assert(!LocationMatching.matches("/data/out", "s3://b/data/out"))
  }

  test("canonical form: an absolute declared location matches only itself, a relative one matches any location ending in it") {
    // absolute: exact only, so it pins one tenant's data
    assert(LocationMatching.matches("/data/orders", "/data/orders"))
    assert(!LocationMatching.matches("/data/orders", "/other-tenant/data/orders"))
    assert(LocationMatching.matches("gs://bucket/orders", "gs://bucket/orders"))
    assert(!LocationMatching.matches("gs://bucket/orders", "gs://other/bucket/orders"))
    // relative: a suffix on a boundary, so it need not know a deployment's root
    assert(LocationMatching.matches("orders", "/other-tenant/data/orders"))
    assert(LocationMatching.matches("data/orders", "/other-tenant/data/orders"))
  }

  test("canonical form: an engine whose names are not paths converts them first - a dotted table id does not match until it does") {
    // BigQuery-style `project.dataset.table` has no `/` boundary, so the raw id matches nothing but itself
    assert(!LocationMatching.matches("dataset/table", "project.dataset.table"))
    // converted to the canonical `/`-separated form, the same contract matches
    val canonical = "project.dataset.table".replace('.', '/')
    assert(LocationMatching.matches("dataset/table", canonical))
    assert(LocationMatching.matches("table", canonical))
    assert(LocationMatching.matches("project/dataset/table", canonical))
    assert(!LocationMatching.matches("other/table", canonical))
  }

  test("matches: the file: scheme is only stripped from the actual side, not the declared one") {
    assert(!LocationMatching.matches("file:/data/out", "/data/out"))
    assert(LocationMatching.normalizeActual("file:/data/out") == "/data/out")
    assert(LocationMatching.normalizeDeclared("file:/data/out") == "file:/data/out")
  }

  test("matchesNormalized and lastSegment") {
    assert(LocationMatching.matchesNormalized("b", "a/b"))
    assert(!LocationMatching.matchesNormalized("a/b", "b"))
    assert(LocationMatching.lastSegment("a/b/c") == "c")
    assert(LocationMatching.lastSegment("c") == "c")
    assert(LocationMatching.lastSegment("a/b/") == "")
    assert(LocationMatching.lastSegment("") == "")
  }

  test("LocationIndex returns the entries a linear scan would, in declaration order") {
    val index = LocationIndex(Seq("orders" -> 0, "archive/orders" -> 1, "customers" -> 2, "orders" -> 3))
    assert(index.matchingIndices("s3://b/archive/orders") == Vector(0, 1, 3))
    assert(index.matching("s3://b/archive/orders") == List(0, 1, 3))
    assert(index.first("s3://b/archive/orders").contains(0))
    assert(index.matchingIndices("file:/w/customers") == Vector(2))
    assert(index.matchingIndices("file:/w/unknown").isEmpty)
    assert(index.matching("file:/w/unknown").isEmpty)
    assert(index.first("file:/w/unknown").isEmpty)
    assert(index.matchesAny("x/customers"))
    assert(!index.matchesAny("x/customers2"))
  }

  test("LocationIndex over no entries matches nothing") {
    val index = LocationIndex(Seq.empty[(String, Int)])
    assert(index.matchingIndices("anything").isEmpty)
    assert(!index.matchesAny("anything"))
  }

  test("LocationIndex buckets by the last segment but still applies the full suffix test inside a bucket") {
    val index = LocationIndex(Seq("a/orders" -> "a", "b/orders" -> "b"))
    assert(index.matching("root/a/orders") == List("a"))
    assert(index.matching("root/b/orders") == List("b"))
    assert(index.matching("root/c/orders").isEmpty)
    assert(index.matching("orders").isEmpty)
  }

  test("LocationIndex handles degenerate locations: empty, trailing slash, backslashes") {
    val index = LocationIndex(Seq("" -> 0, "dir/" -> 1, "w\\x" -> 2))
    assert(index.matchingIndices("") == Vector(0))
    assert(index.matchingIndices("a/dir/") == Vector(0, 1))
    assert(index.matchingIndices("file:/p/w/x") == Vector(2))
  }

  test("PROPERTY: the index agrees with the reference rule on thousands of generated location pairs") {
    val rnd = new Random(20261004)
    val segments = Array("a", "b", "orders", "x", "data", "", "w", "C:", "out.parquet")
    def path(): String = {
      val n = rnd.nextInt(4)
      val sep = if (rnd.nextInt(5) == 0) "\\" else "/"
      val body = Seq.fill(n + 1)(segments(rnd.nextInt(segments.length))).mkString(sep)
      val prefix = rnd.nextInt(4) match { case 0 => "file:"; case 1 => "s3://bkt/"; case 2 => "/"; case _ => "" }
      prefix + body
    }
    val declared = Seq.fill(60)(path())
    val index = LocationIndex(declared.zipWithIndex)
    var matchedAtLeastOnce = 0
    (1 to 4000).foreach { _ =>
      val actual = path()
      val expected = declared.indices.filter(i => reference(declared(i), actual)).toVector
      assert(index.matchingIndices(actual) == expected, s"declared=$declared actual=$actual")
      assert(expected.forall(i => LocationMatching.matches(declared(i), actual)))
      if (expected.nonEmpty) matchedAtLeastOnce += 1
    }
    assert(matchedAtLeastOnce > 200, "the generator should produce real matches, or this property proves little")
  }

  test("a contract-sized index is usable at scale: 2000 declared locations, 2000 lookups") {
    val declared = (0 until 2000).map(i => s"warehouse/db$i/table$i" -> i)
    val index = LocationIndex(declared)
    (0 until 2000).foreach { i =>
      assert(index.matchingIndices(s"s3://bucket/warehouse/db$i/table$i") == Vector(i))
    }
    assert(index.matchingIndices("s3://bucket/warehouse/db1/table2").isEmpty)
  }

  test("fromParts joins the parts with '/', trimming whitespace and one pair of backticks or double quotes") {
    assert(LocationMatching.fromParts(Seq("proj", "ds", "orders")) == "proj/ds/orders")
    assert(LocationMatching.fromParts(Seq("`proj`", " ds ", "\"orders\"")) == "proj/ds/orders")
    assert(LocationMatching.fromParts(Seq("orders")) == "orders")
    assert(LocationMatching.fromParts(Nil) == "")
  }

  test("fromParts drops empty parts, including a part that is only quotes around nothing") {
    assert(LocationMatching.fromParts(Seq("", "ds", "  ", "orders")) == "ds/orders")
    assert(LocationMatching.fromParts(Seq("``", "ds", "\"\"", "orders")) == "ds/orders")
  }

  test("fromParts unquotes only a matching pair, only at the ends, and leaves inner quotes alone") {
    assert(LocationMatching.fromParts(Seq("`a")) == "`a")
    assert(LocationMatching.fromParts(Seq("a`")) == "a`")
    assert(LocationMatching.fromParts(Seq("`a\"")) == "`a\"")
    assert(LocationMatching.fromParts(Seq("a`b")) == "a`b")
    assert(LocationMatching.fromParts(Seq("`")) == "`")
    assert(LocationMatching.fromParts(Seq("` a `")) == "a")
  }

  test("BigQuery-shaped names: a dataset-qualified declaration matches the table in any project, a project-qualified one pins it") {
    val actual = LocationMatching.fromParts(Seq("proj", "ds", "orders"))
    val otherProject = LocationMatching.fromParts(Seq("other", "ds", "orders"))
    assert(LocationMatching.matches("ds/orders", actual))
    assert(LocationMatching.matches("ds/orders", otherProject))
    assert(LocationMatching.matches("proj/ds/orders", actual))
    assert(!LocationMatching.matches("proj/ds/orders", otherProject))
    assert(!LocationMatching.matches("ds/orders", LocationMatching.fromParts(Seq("proj", "ds2", "orders"))))
    assert(!LocationMatching.matches("ds/orders", LocationMatching.fromParts(Seq("proj", "xds", "orders"))))
  }

  test("a dotted declaration is one segment, so it does not match the parts-built name (write it with '/')") {
    assert(!LocationMatching.matches("proj.ds.orders", LocationMatching.fromParts(Seq("proj", "ds", "orders"))))
    assert(LocationMatching.matches("proj.ds.orders", "proj.ds.orders"))
  }
}
