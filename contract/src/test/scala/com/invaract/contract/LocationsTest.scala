// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class LocationsTest extends AnyFunSuite {
  import Locations.{canonical, isAbsolute}

  test("backslashes become slashes, and surrounding space goes") {
    assert(canonical("data\\orders") == "data/orders")
    assert(canonical("  /data/orders  ") == "/data/orders")
    assert(canonical("C:\\work\\out.parquet") == "C:/work/out.parquet")
  }

  test("a file: scheme is dropped, in any case and with any number of slashes after it") {
    assert(canonical("file:/data/orders") == "/data/orders")
    assert(canonical("file:///data/orders") == "/data/orders")
    assert(canonical("FILE:/data/orders") == "/data/orders")
    assert(canonical("file:data/orders") == "data/orders")
    assert(canonical("files/data") == "files/data", "only the scheme, not a path starting with the same letters")
  }

  test("a URL scheme is lower-cased and one store's spellings are folded; the authority and path are kept") {
    assert(canonical("S3A://bucket/orders") == "s3://bucket/orders")
    assert(canonical("s3n://bucket/orders") == "s3://bucket/orders")
    assert(canonical("s3://bucket/orders") == "s3://bucket/orders")
    assert(canonical("abfss://c@acct.dfs.core.windows.net/x") == "abfs://c@acct.dfs.core.windows.net/x")
    assert(canonical("wasbs://c@acct/x") == "wasb://c@acct/x")
    assert(canonical("GS://Bucket/Orders") == "gs://Bucket/Orders")
    assert(canonical("hdfs://nn:8020/data/orders") == "hdfs://nn:8020/data/orders")
  }

  test("empty and '.' segments go: repeated, trailing and leading-dot slashes change nothing; '..' and case stay") {
    assert(canonical("data//orders/") == "data/orders")
    assert(canonical("./data/./orders") == "data/orders")
    assert(canonical("/data//orders//") == "/data/orders")
    assert(canonical("s3://bucket//orders/") == "s3://bucket/orders")
    assert(canonical("/x/../orders") == "/x/../orders")
    assert(canonical("Data/Orders") == "Data/Orders")
  }

  test("degenerate locations keep their meaning: the root, empty, and a scheme with no path") {
    assert(canonical("/") == "/")
    assert(canonical("//") == "/")
    assert(canonical("") == "")
    assert(canonical("s3://") == "s3://")
    assert(canonical("file:") == "")
  }

  test("a single letter before ':' is a Windows drive, never a URL scheme") {
    assert(canonical("c://data") == "c:/data")
    assert(isAbsolute("c://data"))
  }

  test("isAbsolute: a URL, a path from the root and a drive path name one place; anything else is relative") {
    List("/data/orders", "s3://bucket/orders", "S3A://bucket/orders", "C:\\data\\orders", "c:/data", "ref://orders",
      "file:/data/orders", "  /data  ", "hdfs://nn:8020/x", "/").foreach(l => assert(isAbsolute(l), l))
    List("orders", "sales/orders", "./orders", "file:orders", "c:data", "ab:/x", "", "gold.orders").foreach(l => assert(!isAbsolute(l), l))
  }
}
