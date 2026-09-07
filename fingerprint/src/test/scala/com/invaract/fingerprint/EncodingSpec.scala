// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import org.scalatest.funsuite.AnyFunSuite

class EncodingSpec extends AnyFunSuite {

  test("encode is deterministic: encoding the same tree twice yields identical bytes") {
    val node = CTag("Arithmetic", List(CanonicalNode.stringLeaf("*"), CanonicalNode.intLeaf(5)))
    assert(Encoding.encode(node).toVector == Encoding.encode(node).toVector)
  }

  test("decode(encode(node)) round-trips for a leaf") {
    val node = CanonicalNode.stringLeaf("hello")
    assert(Encoding.decode(Encoding.encode(node)) == node)
  }

  test("decode(encode(node)) round-trips for a nested tag tree") {
    val node = CTag(
      "Arithmetic",
      List(
        CanonicalNode.stringLeaf("*"),
        CTag("ColumnReference", List(CTag("ColumnRef", List(CanonicalNode.stringLeaf("amount"), CTag("Option"))))),
        CTag("Literal", List(CTag("Decimal", List(CanonicalNode.stringLeaf("120"), CanonicalNode.stringLeaf("2"))), CanonicalNode.stringLeaf("decimal")))
      )
    )
    assert(Encoding.decode(Encoding.encode(node)) == node)
  }

  test("decode(encode(node)) round-trips for a tag with zero fields") {
    val node = CTag("Null")
    assert(Encoding.decode(Encoding.encode(node)) == node)
  }

  test("decode(encode(node)) round-trips for a leaf with zero-length bytes") {
    val node = CLeaf(Vector.empty)
    assert(Encoding.decode(Encoding.encode(node)) == node)
  }

  test("varint round-trips for lengths spanning multiple encoded bytes (>127 fields/bytes)") {
    val manyFields = (0 until 300).map(i => CanonicalNode.intLeaf(i)).toList
    val node = CTag("Many", manyFields)
    assert(Encoding.decode(Encoding.encode(node)) == node)
  }

  test("no concatenation collision: two different trees whose leaf boundaries could naively coincide encode differently") {
    // "a" ++ "bc" must not equal "ab" ++ "c" once each is length-prefixed.
    val left = CTag("T", List(CanonicalNode.stringLeaf("a"), CanonicalNode.stringLeaf("bc")))
    val right = CTag("T", List(CanonicalNode.stringLeaf("ab"), CanonicalNode.stringLeaf("c")))
    assert(Encoding.encode(left).toVector != Encoding.encode(right).toVector)
  }

  test("a Tag and a Leaf with coincidentally similar content never collide (kind byte disambiguates)") {
    val asTag = CTag("x")
    val asLeaf = CanonicalNode.stringLeaf("x")
    assert(Encoding.encode(asTag).toVector != Encoding.encode(asLeaf).toVector)
  }

  test("sortKey orders same-length leaves by their unsigned byte content") {
    // sortKey orders by the *full* self-delimited encoding (kind byte +
    // length prefix + content, per §10) - for leaves of equal length, the
    // identical length prefix cancels out, isolating a content comparison.
    val a = CanonicalNode.stringLeaf("aaa")
    val b = CanonicalNode.stringLeaf("aab")
    val c = CanonicalNode.stringLeaf("aac")
    val sorted = List(c, a, b).sortBy(Encoding.sortKey)
    assert(sorted == List(a, b, c))
  }

  test("decode rejects a byte string with an unrecognized kind marker") {
    val malformed = Array[Byte](2, 0) // 2 is neither TagKind (0) nor LeafKind (1)
    assertThrows[IllegalArgumentException](Encoding.decode(malformed))
  }

  test("decode rejects a byte string with trailing bytes after a complete node") {
    val complete = Encoding.encode(CanonicalNode.stringLeaf("x"))
    val withTrailingGarbage = complete ++ Array[Byte](9, 9, 9)
    assertThrows[IllegalArgumentException](Encoding.decode(withTrailingGarbage))
  }

  test("sortKey is a total, deterministic order: sorting the same list twice yields the same result") {
    val nodes = List(
      CanonicalNode.stringLeaf("ab"),
      CanonicalNode.stringLeaf("aaa"),
      CTag("X", List(CanonicalNode.intLeaf(1))),
      CanonicalNode.stringLeaf("aab")
    )
    assert(nodes.sortBy(Encoding.sortKey) == nodes.sortBy(Encoding.sortKey))
  }

  test("sortKey is a pure function of the node's own encoded bytes: equal nodes sort identically") {
    val n1 = CTag("X", List(CanonicalNode.intLeaf(1)))
    val n2 = CTag("X", List(CanonicalNode.intLeaf(1)))
    assert(Encoding.sortKey(n1) == Encoding.sortKey(n2))
  }
}
