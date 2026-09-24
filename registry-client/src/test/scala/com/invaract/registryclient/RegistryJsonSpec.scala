// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.registryclient

import org.scalatest.funsuite.AnyFunSuite

import RegistryJson._

class RegistryJsonSpec extends AnyFunSuite {

  test("parses an empty array") {
    assert(parse("[]") == JArray(Nil))
  }

  test("parses an empty object") {
    assert(parse("{}") == JObject(Map.empty))
  }

  test("parses a flat array of strings") {
    assert(parse("""["a", "b", "c"]""") == JArray(List(JString("a"), JString("b"), JString("c"))))
  }

  test("parses an object with string, number, boolean, and null fields") {
    val result = parse("""{"name": "x", "count": 3, "active": true, "note": null}""")
    assert(result == JObject(Map(
      "name" -> JString("x"),
      "count" -> JNumber(3.0),
      "active" -> JBool(true),
      "note" -> JNull
    )))
  }

  test("parses nested arrays of objects") {
    val result = parse("""[{"version": "1.0.0", "status": "active"}, {"version": "2.0.0", "status": "draft"}]""")
    assert(result == JArray(List(
      JObject(Map("version" -> JString("1.0.0"), "status" -> JString("active"))),
      JObject(Map("version" -> JString("2.0.0"), "status" -> JString("draft")))
    )))
  }

  test("parses negative and fractional numbers") {
    assert(parse("-3") == JNumber(-3.0))
    assert(parse("3.5") == JNumber(3.5))
    assert(parse("1e10") == JNumber(1e10))
  }

  test("handles standard escape sequences in strings") {
    assert(parse(""""line1\nline2"""") == JString("line1\nline2"))
    assert(parse(""""quote:\""""") == JString("quote:\""))
    assert(parse(""""back\\slash"""") == JString("back\\slash"))
  }

  // Deliberately built by runtime concatenation, not a literal escape
  // sequence in this file's own source: Scala (like Java) pre-processes
  // backslash-u unicode escapes in *source text* before the lexer even
  // tokenizes string literals, so writing that escape directly in this
  // file would be silently replaced by the real character at COMPILE
  // time, never reaching RegistryJson's own runtime escape handling at
  // all - a real gotcha this test previously fell into (confirmed by
  // hand: a deliberately-broken handler for this escape in RegistryJson
  // still passed this test unchanged, since the test never actually
  // exercised it).
  private val backslash = "\\"
  private def unicodeEscape(hex: String): String = backslash + "u" + hex

  test("handles a unicode escape") {
    val jsonText = "\"" + unicodeEscape("0041") + unicodeEscape("0042") + "\""
    assert(parse(jsonText) == JString("AB"))
  }

  test("rejects a truncated unicode escape that has exactly 4 hex digits but no closing quote") {
    val jsonText = "\"" + unicodeEscape("0041")
    assertThrows[RegistryJson.RegistryJsonParseException] { parse(jsonText) }
  }

  test("ignores whitespace between tokens") {
    assert(parse("""  { "a" : 1 , "b" : 2 }  """) == JObject(Map("a" -> JNumber(1), "b" -> JNumber(2))))
  }

  test("rejects trailing content after a valid value") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse("""{"a": 1} garbage""")
    }
  }

  test("rejects an unterminated string") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse(""""unterminated""")
    }
  }

  test("rejects an unterminated object") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse("""{"a": 1""")
    }
  }

  test("rejects an unterminated array") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse("""[1, 2""")
    }
  }

  test("rejects a missing colon in an object") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse("""{"a" 1}""")
    }
  }

  test("rejects completely empty input") {
    assertThrows[RegistryJson.RegistryJsonParseException] {
      parse("")
    }
  }

  test("parses the literals true/false/null when they are the entire input (exact end-of-buffer boundary)") {
    assert(parse("true") == JBool(true))
    assert(parse("false") == JBool(false))
    assert(parse("null") == JNull)
  }

  test("rejects a truncated literal that runs off the end of the input") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("tru") }
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("fals") }
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("nul") }
  }

  test("rejects a literal-shaped token that is the wrong word at the same length") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("trux") }
  }

  test("rejects an object key that isn't quoted") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("""{a:1}""") }
  }

  test("rejects a string value that isn't quoted") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("""{"a":b}""") }
  }

  test("rejects a truncated escape sequence that ends exactly on the backslash") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("\"abc\\") }
  }

  test("rejects a truncated unicode escape shorter than 4 hex digits") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("\"\\u12\"") }
  }

  test("rejects an unknown escape sequence") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("\"\\q\"") }
  }

  test("parses a number immediately followed by an unsigned exponent digit, with nothing after it") {
    assert(parse("1e5") == JNumber(1e5))
  }

  test("parses an exponent with an explicit sign") {
    assert(parse("1e+5") == JNumber(1e5))
    assert(parse("1e-5") == JNumber(1e-5))
  }

  test("rejects an object consisting of only its opening brace") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("{") }
  }

  test("rejects an array consisting of only its opening bracket") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("[") }
  }

  test("parses a negative number that is the entire input") {
    assert(parse("-42") == JNumber(-42.0))
  }

  test("rejects a bare minus sign with no following digit") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("-") }
  }

  test("rejects a decimal point with no digit after it") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("1.") }
  }

  test("rejects an exponent marker with no digit after it") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("1e") }
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("1e+") }
  }

  test("distinguishes an object missing its colon entirely (runs off the end) from one with a wrong character") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("""{"a"""") }
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("""{"a" 1}""") }
  }

  test("distinguishes an array missing its closing bracket (runs off the end) from one with a wrong character") {
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("[1, 2") }
    assertThrows[RegistryJson.RegistryJsonParseException] { parse("[1, 2}") }
  }
}
