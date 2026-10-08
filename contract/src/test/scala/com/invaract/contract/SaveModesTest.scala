// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class SaveModesTest extends AnyFunSuite {

  private def contractWith(saveMode: Option[String]): Contract =
    ContractParser.parse(
      s"""id: sm
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: out/o
         |${saveMode.map(m => s"    saveMode: $m\n").getOrElse("")}    schema:
         |      fields:
         |        - name: id
         |          type: long
         |""".stripMargin
    )

  test("the canonical vocabulary is exactly the four modes, as lower-case strings") {
    assert(SaveModes.All == Set("append", "overwrite", "ignore", "error"))
    assert(SaveModes.Append == "append" && SaveModes.Overwrite == "overwrite")
    assert(SaveModes.Ignore == "ignore" && SaveModes.Error == "error")
  }

  test("isCanonical ignores case and surrounding whitespace, and rejects anything else") {
    assert(SaveModes.isCanonical("append"))
    assert(SaveModes.isCanonical("OVERWRITE"))
    assert(SaveModes.isCanonical(" Ignore "))
    assert(SaveModes.isCanonical("error"))
    assert(!SaveModes.isCanonical("WRITE_TRUNCATE"))
    assert(!SaveModes.isCanonical("merge"))
    assert(!SaveModes.isCanonical(""))
  }

  test("a canonical or absent saveMode raises no warning") {
    assert(ContractValidator.validate(contractWith(None)).warnings.isEmpty)
    SaveModes.All.foreach { m =>
      assert(ContractValidator.validate(contractWith(Some(m))).warnings.isEmpty, m)
    }
  }

  test("a non-canonical saveMode is a warning naming the path and the canonical set, never an error") {
    val result = ContractValidator.validate(contractWith(Some("WRITE_TRUNCATE")))
    assert(result.isValid, "the vocabulary is open: an unknown mode must not fail validation")
    val warning = result.warnings.find(_.path == "outputs[0].saveMode").getOrElse(fail(s"no saveMode warning in ${result.issues}"))
    assert(warning.message.contains("WRITE_TRUNCATE"))
    assert(warning.message.contains("append, error, ignore, overwrite"))
  }
}
