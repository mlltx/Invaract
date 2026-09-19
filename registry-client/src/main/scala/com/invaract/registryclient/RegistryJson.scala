// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.registryclient

/** A minimal, hand-rolled JSON reader for exactly the response shapes
  * docs/CONTRACT_REGISTRY.md §3 specifies — not a general-purpose JSON
  * library. This repo has no JSON library dependency anywhere
  * (`DemoJobHarness.reportToJson` is a hand-rolled serializer for the
  * same reason); this module follows the same "boring, minimal deps"
  * instinct for the small amount of JSON the registry's non-contract
  * endpoints (`GET /contracts`, `GET .../versions`, and error/
  * compatibility-report bodies) actually need. Contract bodies themselves
  * are YAML, parsed by the existing `ContractParser` — this class never
  * touches those.
  */
private[registryclient] sealed trait RegistryJson
private[registryclient] object RegistryJson {
  case object JNull extends RegistryJson
  case class JBool(value: Boolean) extends RegistryJson
  case class JNumber(value: Double) extends RegistryJson
  case class JString(value: String) extends RegistryJson
  case class JArray(items: List[RegistryJson]) extends RegistryJson
  case class JObject(fields: Map[String, RegistryJson]) extends RegistryJson

  class RegistryJsonParseException(message: String) extends RuntimeException(message)

  /** Parses `text` into a `RegistryJson` tree. Throws
    * `RegistryJsonParseException` on malformed input — there is no
    * partial/best-effort result, since every caller in this module needs
    * a specific known shape and would fail anyway on a partial parse.
    */
  def parse(text: String): RegistryJson = {
    val (value, rest) = parseValue(skipWhitespace(text, 0), text)
    val trailing = skipWhitespace(text, rest)
    if (trailing != text.length) {
      throw new RegistryJsonParseException(s"Unexpected trailing content at offset $trailing in: $text")
    }
    value
  }

  private def skipWhitespace(text: String, from: Int): Int = {
    var i = from
    while (i < text.length && text.charAt(i).isWhitespace) i += 1
    i
  }

  private def parseValue(at: Int, text: String): (RegistryJson, Int) = {
    if (at >= text.length) throw new RegistryJsonParseException(s"Unexpected end of input at offset $at")
    text.charAt(at) match {
      case '{' => parseObject(at, text)
      case '[' => parseArray(at, text)
      case '"' => val (s, next) = parseStringLiteral(at, text); (JString(s), next)
      case 't' => expectLiteral(at, text, "true"); (JBool(true), at + 4)
      case 'f' => expectLiteral(at, text, "false"); (JBool(false), at + 5)
      case 'n' => expectLiteral(at, text, "null"); (JNull, at + 4)
      case c if c == '-' || c.isDigit => parseNumber(at, text)
      case other => throw new RegistryJsonParseException(s"Unexpected character '$other' at offset $at")
    }
  }

  private def expectLiteral(at: Int, text: String, literal: String): Unit = {
    if (at + literal.length > text.length || text.substring(at, at + literal.length) != literal) {
      throw new RegistryJsonParseException(s"Expected '$literal' at offset $at")
    }
  }

  private def parseObject(at: Int, text: String): (JObject, Int) = {
    var i = skipWhitespace(text, at + 1)
    var fields = Map.empty[String, RegistryJson]
    if (i < text.length && text.charAt(i) == '}') {
      return (JObject(fields), i + 1)
    }
    var continue = true
    while (continue) {
      i = skipWhitespace(text, i)
      val (key, afterKey) = parseStringLiteral(i, text)
      i = skipWhitespace(text, afterKey)
      if (i >= text.length || text.charAt(i) != ':') {
        throw new RegistryJsonParseException(s"Expected ':' at offset $i")
      }
      i = skipWhitespace(text, i + 1)
      val (value, afterValue) = parseValue(i, text)
      fields += key -> value
      i = skipWhitespace(text, afterValue)
      if (i < text.length && text.charAt(i) == ',') {
        i = skipWhitespace(text, i + 1)
      } else {
        continue = false
      }
    }
    if (i >= text.length || text.charAt(i) != '}') {
      throw new RegistryJsonParseException(s"Expected '}' at offset $i")
    }
    (JObject(fields), i + 1)
  }

  private def parseArray(at: Int, text: String): (JArray, Int) = {
    var i = skipWhitespace(text, at + 1)
    if (i < text.length && text.charAt(i) == ']') {
      return (JArray(Nil), i + 1)
    }
    var items = List.newBuilder[RegistryJson]
    var continue = true
    while (continue) {
      i = skipWhitespace(text, i)
      val (value, afterValue) = parseValue(i, text)
      items += value
      i = skipWhitespace(text, afterValue)
      if (i < text.length && text.charAt(i) == ',') {
        i = skipWhitespace(text, i + 1)
      } else {
        continue = false
      }
    }
    if (i >= text.length || text.charAt(i) != ']') {
      throw new RegistryJsonParseException(s"Expected ']' at offset $i")
    }
    (JArray(items.result()), i + 1)
  }

  private def parseStringLiteral(at: Int, text: String): (String, Int) = {
    if (at >= text.length || text.charAt(at) != '"') {
      throw new RegistryJsonParseException(s"Expected string at offset $at")
    }
    val sb = new StringBuilder
    var i = at + 1
    var closed = false
    while (i < text.length && !closed) {
      text.charAt(i) match {
        case '"' => closed = true; i += 1
        case '\\' =>
          if (i + 1 >= text.length) throw new RegistryJsonParseException(s"Unterminated escape at offset $i")
          text.charAt(i + 1) match {
            case '"'  => sb.append('"'); i += 2
            case '\\' => sb.append('\\'); i += 2
            case '/'  => sb.append('/'); i += 2
            case 'n'  => sb.append('\n'); i += 2
            case 't'  => sb.append('\t'); i += 2
            case 'r'  => sb.append('\r'); i += 2
            case 'b'  => sb.append('\b'); i += 2
            case 'f'  => sb.append('\f'); i += 2
            case 'u' =>
              if (i + 6 > text.length) throw new RegistryJsonParseException(s"Truncated unicode escape at offset $i")
              val code = Integer.parseInt(text.substring(i + 2, i + 6), 16)
              sb.append(code.toChar)
              i += 6
            case other => throw new RegistryJsonParseException(s"Unknown escape '\\$other' at offset $i")
          }
        case c => sb.append(c); i += 1
      }
    }
    if (!closed) throw new RegistryJsonParseException(s"Unterminated string starting at offset $at")
    (sb.toString(), i)
  }

  private def parseNumber(at: Int, text: String): (JNumber, Int) = {
    var i = at
    if (i < text.length && text.charAt(i) == '-') i += 1

    val afterSign = i
    while (i < text.length && text.charAt(i).isDigit) i += 1
    if (i == afterSign) throw new RegistryJsonParseException(s"Expected a digit at offset $afterSign")

    if (i < text.length && text.charAt(i) == '.') {
      i += 1
      val afterDot = i
      while (i < text.length && text.charAt(i).isDigit) i += 1
      if (i == afterDot) throw new RegistryJsonParseException(s"Expected a digit after '.' at offset $afterDot")
    }

    if (i < text.length && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
      i += 1
      if (i < text.length && (text.charAt(i) == '+' || text.charAt(i) == '-')) i += 1
      val afterExpSign = i
      while (i < text.length && text.charAt(i).isDigit) i += 1
      if (i == afterExpSign) throw new RegistryJsonParseException(s"Expected a digit in exponent at offset $afterExpSign")
    }

    (JNumber(text.substring(at, i).toDouble), i)
  }
}
