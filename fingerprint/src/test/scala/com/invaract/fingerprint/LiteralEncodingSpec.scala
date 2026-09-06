// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import org.scalatest.funsuite.AnyFunSuite

class LiteralEncodingSpec extends AnyFunSuite {

  private def bytes(value: Any, literalType: String = "irrelevant"): Vector[Byte] =
    Encoding.encode(LiteralEncoding.encode(value, literalType)).toVector

  test("the same literal always encodes identically") {
    assert(bytes(5, "integer") == bytes(5, "integer"))
  }

  test("Int and Long carrying the same numeric value encode identically (width-independent)") {
    assert(bytes(5: Int, "integer") == bytes(5L: Long, "integer"))
  }

  test("a decimal's exact scale is preserved: 1.20 and 1.2 are different literals") {
    val oneTwenty = bytes(BigDecimal("1.20"), "decimal")
    val oneTwo = bytes(BigDecimal("1.2"), "decimal")
    assert(oneTwenty != oneTwo, "different declared precision must not be normalised away")
  }

  test("a decimal literal changing value produces a different encoding (the 1.20 -> 1.25 worked example)") {
    assert(bytes(BigDecimal("1.20"), "decimal") != bytes(BigDecimal("1.25"), "decimal"))
  }

  test("scala.math.BigDecimal and java.math.BigDecimal carrying the same (unscaled, scale) encode identically") {
    val scalaBd = BigDecimal("1.20")
    val javaBd = new java.math.BigDecimal("1.20")
    assert(bytes(scalaBd, "decimal") == bytes(javaBd, "decimal"))
  }

  test("the same literalType is required for identical encoding: same numeric value, different literalType differs") {
    assert(bytes(5, "integer") != bytes(5, "long"))
  }

  test("Double: NaN of any payload encodes identically (non-raw bit conversion)") {
    val nan1 = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    val nan2 = java.lang.Double.longBitsToDouble(0x7ff8000000000002L)
    assert(bytes(nan1) == bytes(nan2))
  }

  test("Double: -0.0 and 0.0 encode differently (sign preserved bit-exact)") {
    assert(bytes(-0.0) != bytes(0.0))
  }

  test("Double: different textual forms of the same value encode identically") {
    assert(bytes(0.1) == bytes(1.0e-1))
  }

  test("Boolean true and false encode differently") {
    assert(bytes(true) != bytes(false))
  }

  test("String: NFC-equivalent Unicode forms (precomposed vs decomposed) encode identically") {
    val precomposed = "é" // é, single code point
    val decomposed = "é" // e + combining acute accent
    assert(bytes(precomposed) == bytes(decomposed))
  }

  test("String: genuinely different strings encode differently") {
    assert(bytes("ACTIVE") != bytes("INACTIVE"))
  }

  test("typed NULL (value == null) encodes distinctly from any real value, and literalType still participates") {
    val nullInt = bytes(null, "integer")
    val nullLong = bytes(null, "long")
    assert(nullInt != bytes(0, "integer"))
    assert(nullInt != nullLong, "literalType must still be hashed even for a null value")
  }

  test("an unrecognized runtime value type falls back to a distinctly-tagged encoding, not the String encoding") {
    case class Weird(x: Int)
    val weirdBytes = bytes(Weird(1))
    val stringBytes = bytes(Weird(1).toString)
    assert(weirdBytes != stringBytes, "the fallback must be visibly distinct from a real String literal")
  }
}
