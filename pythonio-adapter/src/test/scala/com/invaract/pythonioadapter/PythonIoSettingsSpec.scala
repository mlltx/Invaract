// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** The `invaract` package forwards the settings it finds in the job's environment by their neutral names, from its own list
  * (`python/invaract/_settings.py`); the checker refuses a setting it does not know. The two lists must agree, or a setting a
  * platform sets would be dropped by the package, or refused by the checker. */
class PythonIoSettingsSpec extends AnyFunSuite {

  private val source = new String(Files.readAllBytes(Paths.get("python/invaract/_settings.py")), StandardCharsets.UTF_8)

  test("every setting the checker reads is one the package forwards, and no other (a new one needs adding to invaract/_settings.py)") {
    val block = source.substring(source.indexOf("SETTINGS = {"), source.indexOf("\n}", source.indexOf("SETTINGS = {")))
    val forwarded = "(?m)^\\s+\"([A-Za-z]+)\":".r.findAllMatchIn(block).map(_.group(1)).toSet
    assert(forwarded == PythonIoConfigSource.Known)
  }

}
