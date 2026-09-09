// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class StaticMapLocationResolverSpec extends AnyFunSuite {

  private def withPropertiesFile(contents: String)(body: Path => Unit): Unit = {
    val file = Files.createTempFile("invaract-location-map-test", ".properties")
    try {
      Files.write(file, contents.getBytes("UTF-8"))
      body(file)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("resolve returns the mapped location for a known id") {
    val resolver = new StaticMapLocationResolver(Map("orders-input" -> "demo/input/sample.csv"))
    assert(resolver.resolve("orders-input") == "demo/input/sample.csv")
  }

  test("resolve throws LocationResolutionException for an unknown id") {
    val resolver = new StaticMapLocationResolver(Map("orders-input" -> "demo/input/sample.csv"))
    val ex = intercept[LocationResolutionException] {
      resolver.resolve("does-not-exist")
    }
    assert(ex.getMessage.contains("does-not-exist"))
  }

  test("resolve throws on an empty mapping") {
    val resolver = new StaticMapLocationResolver(Map.empty)
    assertThrows[LocationResolutionException] {
      resolver.resolve("orders-input")
    }
  }

  test("fromPropertiesFile loads every key as an id -> location mapping") {
    withPropertiesFile(
      """orders-input=demo/input/sample.csv
        |result-output=demo/output/result.parquet
        |""".stripMargin
    ) { file =>
      val resolver = StaticMapLocationResolver.fromPropertiesFile(file.toString)
      assert(resolver.resolve("orders-input") == "demo/input/sample.csv")
      assert(resolver.resolve("result-output") == "demo/output/result.parquet")
    }
  }

  test("fromPropertiesFile throws when the file does not exist") {
    assertThrows[java.io.FileNotFoundException] {
      StaticMapLocationResolver.fromPropertiesFile("/does/not/exist/invaract-location-map.properties")
    }
  }

  test("fromArgs parses id=location pairs") {
    val resolver = StaticMapLocationResolver.fromArgs(Seq("orders-input=demo/input/sample.csv"))
    assert(resolver.resolve("orders-input") == "demo/input/sample.csv")
  }

  test("fromArgs splits only on the first = so a location containing = is preserved intact") {
    val resolver = StaticMapLocationResolver.fromArgs(Seq("orders-input=s3://bucket/path?token=abc=123"))
    assert(resolver.resolve("orders-input") == "s3://bucket/path?token=abc=123")
  }

  test("fromArgs throws IllegalArgumentException on an entry with no =") {
    assertThrows[IllegalArgumentException] {
      StaticMapLocationResolver.fromArgs(Seq("not-a-valid-pair"))
    }
  }

  test("fromArgs on an empty sequence produces a resolver that resolves nothing") {
    val resolver = StaticMapLocationResolver.fromArgs(Seq.empty)
    assertThrows[LocationResolutionException] {
      resolver.resolve("anything")
    }
  }
}
