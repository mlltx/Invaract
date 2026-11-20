// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.scalatest.funsuite.AnyFunSuite

/** `ReadLocations`: where a file read is checked as having read. Pure; no Spark session. */
class ReadLocationsSpec extends AnyFunSuite {
  import ReadLocations._

  test("a location a declared input names is kept exactly as it is, even when a longer declared input names a part of it") {
    assert(attribute("file:/data/in/orders", Seq("in/orders")) == "file:/data/in/orders")
    assert(attribute("/data/in/orders/dt=1", Seq("in/orders", "in/orders/dt=1")) == "/data/in/orders/dt=1")
  }

  test("a location inside a declared input is reported at that input") {
    assert(attribute("/data/in/orders/dt=1/p.parquet", Seq("in/orders")) == "/data/in/orders")
    assert(attribute("file:/data/in/orders/dt=1/p.parquet", Seq("/data/in/orders")) == "/data/in/orders")
    assert(attribute("/data/in/orders/x", Seq("orders")) == "/data/in/orders")
  }

  test("the longest declared input a location lies inside wins") {
    assert(attribute("/data/in/orders/dt=1/p.parquet", Seq("orders", "in/orders/dt=1")) == "/data/in/orders/dt=1")
    assert(attribute("/data/in/orders/dt=1/p.parquet", Seq("in/orders/dt=1", "orders")) == "/data/in/orders/dt=1", "whatever the order")
  }

  test("a match is by whole segments: orders2 is not inside orders") {
    assert(attribute("/data/in/orders2/x", Seq("in/orders")) == "/data/in/orders2/x")
  }

  test("a parent of a declared input is not inside it") {
    assert(attribute("/data/in/orders", Seq("in/orders/dt=1")) == "/data/in/orders")
  }

  test("a URL keeps its scheme and bucket, and the store's other spellings count as the same store") {
    assert(attribute("s3a://b/in/orders/x", Seq("s3://b/in/orders")) == "s3://b/in/orders")
    assert(attribute("s3://b/in/orders/x", Seq("s3://b")) == "s3://b")
  }

  test("a part followed by .. is never used: what it climbs out of is not known") {
    assert(attribute("/data/in/orders/../secret/x", Seq("in/orders")) == "/data/in/orders/../secret/x")
    assert(attribute("/data/in/orders/a/../b", Seq("orders/a")) == "/data/in/orders/a/../b")
  }

  test("with no declared inputs, or none it lies inside, a location is unchanged") {
    assert(attribute("/data/in/orders/x", Nil) == "/data/in/orders/x")
    assert(attribute("/data/in/orders/x", Seq("in/customers")) == "/data/in/orders/x")
    assert(attribute("relative/a/b", Seq("a")) == "relative/a")
  }

  test("of: every root path, attributed, each once, in first-seen order") {
    assert(of(Seq("/d/orders/a", "/d/orders2", "/d/orders/b"), Seq("orders")) == List("/d/orders", "/d/orders2"))
    assert(of(Seq("/d/b", "/d/a"), Nil) == List("/d/b", "/d/a"))
    assert(of(Nil, Seq("x")) == Nil)
  }

  test("datasetRoot: a file's folder, without trailing partition folders") {
    assert(datasetRoot("file:/d/orders/dt=1/h=2/p.parquet", isFile = true) == "/d/orders")
    assert(datasetRoot("/d/orders/dt=1", isFile = false) == "/d/orders")
    assert(datasetRoot("/d/orders/p.parquet", isFile = true) == "/d/orders")
    assert(datasetRoot("/d/orders", isFile = false) == "/d/orders")
    assert(datasetRoot("/d/orders/x=1/data", isFile = false) == "/d/orders/x=1/data", "only trailing partition folders")
    assert(datasetRoot("/dt=1", isFile = false) == "/dt=1", "a location that is only a partition folder is left whole")
    assert(datasetRoot("s3://b/t/dt=1/f", isFile = true) == "s3://b/t")
    assert(datasetRoot("/d/=x", isFile = false) == "/d/=x", "a segment starting with = is no partition")
    assert(datasetRoot("/d/x=", isFile = false) == "/d/x=", "nor one ending with it")
    assert(datasetRoot("/d/t/=b=c", isFile = false) == "/d/t/=b=c", "a segment whose first = starts it has no key, whatever follows")
  }
}
