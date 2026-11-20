// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** `PinnedSettings`: the settings a platform pins in the file `INVARACT_PINNED_CONF` names (H-07). */
class PinnedSettingsSpec extends AnyFunSuite {
  import PinnedSettings._

  private def file(lines: String*): String = {
    val f = Files.createTempFile("invaract-pins", ".properties")
    Files.write(f, lines.mkString("\n").getBytes("UTF-8"))
    f.toString
  }

  test("no variable, or an empty one: nothing is pinned") {
    assert(load(Map.empty) == Map.empty)
    assert(load(Map(EnvVar -> "  ")) == Map.empty)
    assert(load(Map("OTHER" -> file("spark.invaract.dryRun=false"))) == Map.empty)
  }

  test("a readable file: its settings, as Spark spells them, values trimmed") {
    val path = file("spark.invaract.contract = /contracts/orders.yaml ", "spark.invaract.dryRun=false", "# a comment")
    assert(load(Map(EnvVar -> s" $path ")) == Map("spark.invaract.contract" -> "/contracts/orders.yaml", "spark.invaract.dryRun" -> "false"))
  }

  test("a file that cannot be read stops the job, naming the variable and the path") {
    val missing = Files.createTempDirectory("invaract-pins").resolve("absent.properties").toString
    val e = intercept[IllegalStateException](load(Map(EnvVar -> missing)))
    assert(e.getMessage.contains(EnvVar) && e.getMessage.contains(missing))
  }

  test("a key that is not an Invaract setting stops the job") {
    val path = file("spark.invaract.dryRun=false", "spark.sql.extensions=x")
    val e = intercept[IllegalArgumentException](load(Map(EnvVar -> path)))
    assert(e.getMessage.contains("'spark.sql.extensions'") && e.getMessage.contains(path) && e.getMessage.contains("spark.invaract."))
  }

  test("a misspelt Invaract setting stops the job, with the setting it was probably meant to be") {
    val path = file("spark.invaract.dryrun=false")
    val e = intercept[IllegalArgumentException](load(Map(EnvVar -> path)))
    assert(e.getMessage.contains(EnvVar) && e.getMessage.contains("'spark.invaract.dryrun'") && e.getMessage.contains("Did you mean 'spark.invaract.dryRun'?"))
  }

  test("get: a pinned key's value, nothing for any other; the pins in force are restored after a test's own") {
    assert(get("spark.invaract.dryRun").isEmpty, "this test JVM pins nothing")
    withPinsForTest(Map("spark.invaract.dryRun" -> "false")) {
      assert(get("spark.invaract.dryRun").contains("false"))
      assert(get("spark.invaract.contract").isEmpty)
      withPinsForTest(Map.empty)(assert(current.isEmpty))
      assert(current == Map("spark.invaract.dryRun" -> "false"))
    }
    assert(current.isEmpty)
  }
}
