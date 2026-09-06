// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import java.nio.charset.StandardCharsets.UTF_8

/** The canonical tree a `Plan`/`Expr`/`ColumnLineage` value is reduced to
  * before hashing — see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §2.1
  * "Node representation". Every `ir` case class becomes a `CTag` whose
  * `tag` is a short, stable, registered label and whose `fields` are that
  * node's own fields, each itself canonicalized; a primitive value (a
  * literal's encoded bytes, an operator name, a boolean) is a `CLeaf`.
  *
  * `CLeaf` wraps a `Vector[Byte]`, not `Array[Byte]`: this type is compared
  * and pattern-matched extensively in tests (and, via `Encoding`, sorted —
  * see `Canonicalizer`'s treatment of `Aggregate.groupBy`/
  * `Window.partitionBy`/`Lineage`'s `Set`-valued fields), and `Array`'s
  * reference-equality `equals`/`hashCode` would silently break both if used
  * here instead.
  */
sealed trait CanonicalNode

final case class CTag(tag: String, fields: List[CanonicalNode] = Nil) extends CanonicalNode
final case class CLeaf(bytes: Vector[Byte]) extends CanonicalNode

object CanonicalNode {
  def stringLeaf(s: String): CLeaf = CLeaf(s.getBytes(UTF_8).toVector)
  def boolLeaf(b: Boolean): CLeaf = CLeaf(Vector(if (b) 1.toByte else 0.toByte))
  def intLeaf(i: Int): CLeaf = stringLeaf(i.toString)

  /** The generic `Option[X]` encoding used everywhere an `ir` field is
    * `Option[...]` (`UDF.name`, `Conditional.elseValue`, `Join.condition`,
    * `Write.format`/`saveMode`, a substituted `ColumnRef.qualifier`, ...):
    * zero fields for `None`, exactly one — the wrapped node — for `Some`.
    * Distinguishing the two by field *count* rather than, say, a leaf byte
    * flag keeps every option uniform with how every other variable-arity
    * field in this encoding already works (see `Conditional`'s `Else`
    * node, `UDF`'s `Args` node).
    */
  def optionNode(opt: Option[CanonicalNode]): CanonicalNode =
    CTag("Option", opt.toList)
}

/** Renders a `CanonicalNode` to an unambiguous byte string, and back — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §10 "Input encoding". Every node
  * is self-delimiting (a one-byte kind marker, then a varint-length-
  * prefixed body), so concatenating sibling nodes directly, with no extra
  * outer wrapper, can never produce the same bytes for two different trees
  * — the classic "`"a" ++ "bc"` looks like `"ab" ++ "c"`" concatenation-
  * collision bug this design is built to avoid.
  *
  * `decode` exists for exactly one reason: an `encode(node) == encode
  * (decode(encode(node)))`-shaped round-trip property test is strong,
  * cheap evidence the encoding is actually injective on whatever trees a
  * test throws at it — not because anything in this module ever needs to
  * decode a fingerprint back into a tree in production. It is not part of
  * this module's stable API in the way `Canonicalizer`/`FingerprintHasher`
  * are.
  */
object Encoding {
  private val TagKind: Byte = 0
  private val LeafKind: Byte = 1

  def encode(node: CanonicalNode): Array[Byte] = {
    val buf = new scala.collection.mutable.ArrayBuffer[Byte]()
    writeNode(node, buf)
    buf.toArray
  }

  private def writeNode(node: CanonicalNode, buf: scala.collection.mutable.ArrayBuffer[Byte]): Unit = node match {
    case CTag(tag, fields) =>
      buf += TagKind
      val tagBytes = tag.getBytes(UTF_8)
      writeVarInt(tagBytes.length, buf)
      buf ++= tagBytes
      writeVarInt(fields.length, buf)
      fields.foreach(writeNode(_, buf))
    case CLeaf(bytes) =>
      buf += LeafKind
      writeVarInt(bytes.length, buf)
      buf ++= bytes
  }

  private def writeVarInt(nonNegative: Int, buf: scala.collection.mutable.ArrayBuffer[Byte]): Unit = {
    require(nonNegative >= 0, s"length must be non-negative, got $nonNegative")
    var v = nonNegative
    var continue = true
    while (continue) {
      val low7 = v & 0x7f
      v = v >>> 7
      if (v != 0) {
        buf += (low7 | 0x80).toByte
      } else {
        buf += low7.toByte
        continue = false
      }
    }
  }

  /** Round-trips `encode`'s output back into a `CanonicalNode` — test-only
    * use, see this object's own doc.
    */
  def decode(bytes: Array[Byte]): CanonicalNode = {
    val (node, consumed) = readNode(bytes, 0)
    require(consumed == bytes.length, s"trailing bytes after decoding: consumed $consumed of ${bytes.length}")
    node
  }

  private def readVarInt(bytes: Array[Byte], offset: Int): (Int, Int) = {
    var result = 0
    var shift = 0
    var pos = offset
    var continue = true
    while (continue) {
      val b = bytes(pos) & 0xff
      result |= (b & 0x7f) << shift
      pos += 1
      shift += 7
      if ((b & 0x80) == 0) continue = false
    }
    (result, pos - offset)
  }

  private def readNode(bytes: Array[Byte], offset: Int): (CanonicalNode, Int) = {
    val kind = bytes(offset)
    var pos = offset + 1
    if (kind == TagKind) {
      val (tagLen, tagLenSize) = readVarInt(bytes, pos)
      pos += tagLenSize
      val tag = new String(bytes, pos, tagLen, UTF_8)
      pos += tagLen
      val (fieldCount, fieldCountSize) = readVarInt(bytes, pos)
      pos += fieldCountSize
      val fields = (0 until fieldCount).map { _ =>
        val (child, size) = readNode(bytes, pos)
        pos += size
        child
      }.toList
      (CTag(tag, fields), pos - offset)
    } else if (kind == LeafKind) {
      val (len, lenSize) = readVarInt(bytes, pos)
      pos += lenSize
      val payload = bytes.slice(pos, pos + len).toVector
      pos += len
      (CLeaf(payload), pos - offset)
    } else {
      throw new IllegalArgumentException(s"unrecognized node kind byte $kind at offset $offset")
    }
  }

  /** Lowercase hex of `encode(node)` — the sort key `Canonicalizer` uses to
    * canonically order a set-like field (`Aggregate.groupBy`/
    * `Window.partitionBy`, and `Lineage`'s `Set`-valued `sources`/
    * `aggregations`; see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §2.4) by
    * the same bytes that will ultimately be hashed, rather than by some
    * separate, potentially-inconsistent comparator. Two hex characters per
    * byte preserves unsigned-byte-value ordering exactly under plain
    * `String` comparison.
    */
  def sortKey(node: CanonicalNode): String = {
    val bytes = encode(node)
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02x"))
    sb.toString()
  }
}
