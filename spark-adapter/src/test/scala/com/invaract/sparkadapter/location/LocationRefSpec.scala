// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

import org.scalatest.funsuite.AnyFunSuite

class LocationRefSpec extends AnyFunSuite {

  test("a plain literal location is not a reference") {
    assert(LocationRef.id("demo/input/sample.csv").isEmpty)
  }

  test("a location that merely contains the scheme, not starting with it, is not a reference") {
    assert(LocationRef.id("s3://bucket/ref://not-a-real-id").isEmpty)
  }

  test("ref://<id> extracts the id") {
    assert(LocationRef.id("ref://orders-input") == Some("orders-input"))
  }

  test("the scheme is case-sensitive: REF:// is a literal, not a reference") {
    assert(LocationRef.id("REF://orders-input").isEmpty)
  }

  test("ref:// with no id at all is treated as a literal, not a reference to an empty id") {
    assert(LocationRef.id("ref://").isEmpty)
  }

  test("an id may itself contain slashes") {
    assert(LocationRef.id("ref://team/orders-input") == Some("team/orders-input"))
  }

  test("the empty string is not a reference") {
    assert(LocationRef.id("").isEmpty)
  }
}
