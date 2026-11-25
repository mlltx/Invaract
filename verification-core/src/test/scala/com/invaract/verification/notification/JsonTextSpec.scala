// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.invaract.verification.notification.JsonText.{obj, Obj}

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

/** `JsonText` must give well-formed JSON for any text a job can put in an event, and the same text back when read. */
class JsonTextSpec extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  private val mapper = new ObjectMapper()

  /** Any UTF-16 text, unpaired surrogates and control characters included. */
  private val anyText: Gen[String] = Gen.listOf(Gen.choose(Char.MinValue, Char.MaxValue)).map(_.mkString)

  /** `s` with every unpaired surrogate replaced, as JsonText writes it. */
  private def paired(s: String): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (Character.isHighSurrogate(c) && i + 1 < s.length && Character.isLowSurrogate(s.charAt(i + 1))) { sb.append(c).append(s.charAt(i + 1)); i += 1 }
      else if (Character.isSurrogate(c)) sb.append('�')
      else sb.append(c)
      i += 1
    }
    sb.toString
  }

  test("any text is written as one JSON string that reads back as the same text (unpaired surrogates as U+FFFD)") {
    forAll(anyText, minSuccessful(2000)) { s =>
      val json = JsonText.render(s)
      assert(mapper.readTree(json).asText() == paired(s))
    }
  }

  test("no raw control character, U+2028 or U+2029 survives into the text") {
    forAll(anyText, minSuccessful(1000)) { s =>
      val json = JsonText.render(Map("k" -> s))
      assert(!json.exists(c => c < 0x20 || c == ' ' || c == ' '))
    }
  }

  test("each escape: the short forms where JSON has them, a six-character escape otherwise") {
    assert(JsonText.render("a\"b\\c") == "\"a\\\"b\\\\c\"")
    assert(JsonText.render("\n\r\t\b\f") == "\"\\n\\r\\t\\b\\f\"")
    assert(JsonText.render("\u0000\u0001\u001f") == "\"\\u0000\\u0001\\u001f\"")
    assert(JsonText.render(" ") == "\" \"", "U+0020 is the first character not escaped")
    assert(JsonText.render("  ") == "\"\\u2028\\u2029\"")
    assert(JsonText.render("é中") == "\"é中\"", "other characters are written as they are")
    assert(JsonText.render("😀") == "\"😀\"", "a surrogate pair stays")
    assert(JsonText.render("x\ud83dy") == "\"x�y\"", "a lone high surrogate is replaced")
    assert(JsonText.render("x\ude00y") == "\"x�y\"", "a lone low surrogate is replaced")
    assert(JsonText.render("\ud83d") == "\"�\"", "a high surrogate at the end is replaced")
  }

  test("values: numbers, booleans, null, options, collections") {
    assert(JsonText.render(obj("i" -> 1, "l" -> 2L, "d" -> 1.5, "b" -> true, "n" -> null)) == """{"i":1,"l":2,"d":1.5,"b":true,"n":null}""")
    assert(JsonText.render(List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity, Float.NaN)) == "[null,null,null,null]")
    assert(JsonText.render(Some("x")) == "\"x\"" && JsonText.render(None) == "null")
    assert(JsonText.render(BigDecimal("1E+3")) == "1000" && JsonText.render(new java.math.BigDecimal("1E+3")) == "1000")
    assert(JsonText.render(Array(1, 2)) == "[1,2]" && JsonText.render(java.util.Arrays.asList("a", "b")) == "[\"a\",\"b\"]")
    assert(JsonText.render(Vector.empty) == "[]" && JsonText.render(Obj(Nil)) == "{}")
    val javaMap = new java.util.LinkedHashMap[String, Any](); javaMap.put("k", 1)
    assert(JsonText.render(javaMap) == """{"k":1}""")
    assert(JsonText.render(new Object { override def toString = "x\ny" }) == "\"x\\ny\"", "anything else is its text")
  }

  test("obj keeps its entries' order and leaves out None, not null") {
    assert(JsonText.render(obj("z" -> 1, "a" -> None, "m" -> Some(2), "b" -> null)) == """{"z":1,"m":2,"b":null}""")
  }

  test("canonical sorts every object's keys, at every depth, and render does not") {
    val value = obj("b" -> obj("d" -> 1, "c" -> 2), "a" -> List(Map("y" -> 1, "x" -> 2)))
    assert(JsonText.canonical(value) == """{"a":[{"x":2,"y":1}],"b":{"c":2,"d":1}}""")
    assert(JsonText.render(value).startsWith("""{"b":{"d":1,"c":2}"""))
  }

  test("NotificationJson: plain text renders exactly as it did, so eventIds do not change; control characters are now escaped") {
    val violation = com.invaract.verification.Violation("T", "plain message", "fix it", Some("col"))
    val event = ContractValidationEvent("c@1", "FAILED", List(violation), 1L, Map("owner" -> "team"), Some("run-1"))
    val json = NotificationJson.toJson(event)
    assert(json.contains(""""message": "plain message"""") && json.contains(""""owner": "team""""))
    val tricky = event.copy(violations = List(violation.copy(message = "bell\u0007 nul\u0000")))
    val parsed = mapper.readTree(NotificationJson.toJson(tricky))
    assert(parsed.get("violations").get(0).get("message").asText() == "bell\u0007 nul\u0000")
  }

  test("for text the old escaping handled (no control character but \\n, \\r, \\t; no U+2028/9; no lone surrogate) it is unchanged, so eventIds are") {
    def old(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""
    val handled = anyText.map(_.filter(c => (c >= 0x20 || c == '\n' || c == '\r' || c == '\t') && c != '\u2028' && c != '\u2029' && !Character.isSurrogate(c)))
    forAll(handled, minSuccessful(2000))(s => assert(JsonText.quote(s) == old(s)))
  }

  test("NotificationJson writes a non-finite number as null") {
    val event = ContractValidationEvent("c@1", "PASSED", Nil, 1L, Map("ratio" -> Double.NaN, "big" -> Float.PositiveInfinity, "ok" -> 1.5))
    val parsed = mapper.readTree(NotificationJson.toJson(event))
    assert(parsed.get("metadata").get("ratio").isNull && parsed.get("metadata").get("big").isNull && parsed.get("metadata").get("ok").asDouble() == 1.5)
  }
}
