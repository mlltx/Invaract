// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

/** A single hash, tagged with the exact canonicalisation/hash ruleset that
  * produced it — see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §10. `version`
  * is bumped on *any* change that could change output bytes for some input
  * (a new/renamed/reordered tag, a changed field-order or literal-encoding
  * rule, a changed hash algorithm, ...); comparing two `Fingerprint`s with
  * different `version`s is a decision for a future, out-of-scope
  * comparison stage, not this module — but this shape exists so that stage
  * always has the information to detect the mismatch rather than assume
  * comparability.
  */
final case class Fingerprint(version: Int, algorithm: String, value: String) {

  /** The same field-map shape `Violation.toMap` already establishes for
    * crossing the `spark-adapter` notification JSON boundary (see
    * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.5) — kept here, next to
    * the type it describes, rather than in `spark-adapter`, so any future
    * consumer of this module gets the same JSON-friendly shape for free.
    */
  def toMap: Map[String, Any] = Map("version" -> version, "algorithm" -> algorithm, "value" -> value)
}

object FingerprintHasher {

  /** The canonicalisation/encoding ruleset version this object's `hash`
    * implements. See `Fingerprint`'s own doc for what bumping it means.
    * 4: a node's hash is computed from its children's hashes (`Digests`), so a plan that shares nodes is hashed in
    * time and space in proportion to its distinct nodes; every value changed.
    */
  val CurrentVersion: Int = 4

  val Algorithm: String = "SHA-256"

  def hash(node: CanonicalNode): Fingerprint = new Digests().fingerprint(node)

  private[fingerprint] def toHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02x"))
    sb.toString()
  }
}

/** The hash of each canonical node, worked out from its own tag or bytes and its children's hashes (a Merkle hash),
  * and remembered per node object - see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §10. Hashing the full encoding of
  * the tree instead would cost as much as the tree drawn out in full, which for a plan whose branches rejoin (see
  * `ir.Plan`) is exponential in its size; a node object shared by several parents is hashed here once, and its hash
  * is the same as that of an equal copy, so sharing never changes a fingerprint.
  *
  * What is hashed for a node is self-delimiting, as `Encoding` is: a kind byte, then for a leaf its length and
  * bytes, for a tag its name's length and bytes, the number of its fields, and each field's 32-byte hash. The work
  * is an explicit stack, not recursion: a canonical tree is as deep as the plan it came from.
  *
  * One instance belongs to one fingerprint: it holds on to every node it has hashed.
  */
private[fingerprint] final class Digests {
  private val memo = new com.invaract.ir.IdentityMemo[CanonicalNode, Array[Byte]]

  def fingerprint(node: CanonicalNode): Fingerprint =
    Fingerprint(FingerprintHasher.CurrentVersion, FingerprintHasher.Algorithm, hex(node))

  def hex(node: CanonicalNode): String = FingerprintHasher.toHex(of(node))

  def of(root: CanonicalNode): Array[Byte] = {
    // (node, its fields already hashed?)
    val stack = scala.collection.mutable.Stack[(CanonicalNode, Boolean)]((root, false))
    while (stack.nonEmpty) {
      val (node, ready) = stack.pop()
      if (memo.get(node).isEmpty) node match {
        case CLeaf(bytes) =>
          memo.getOrElseUpdate(node, digest(Encoding.LeafKind, bytes.toArray, Nil))
        case CTag(tag, fields) if ready =>
          memo.getOrElseUpdate(node, digest(Encoding.TagKind, tag.getBytes(java.nio.charset.StandardCharsets.UTF_8), fields.map(f => memo.get(f).get)))
        case CTag(_, fields) =>
          stack.push((node, true))
          fields.foreach(f => stack.push((f, false))) // one already hashed is skipped when it is popped
      }
    }
    memo.get(root).get
  }

  private def digest(kind: Byte, bytes: Array[Byte], children: List[Array[Byte]]): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance(FingerprintHasher.Algorithm)
    md.update(kind)
    md.update(Encoding.varInt(bytes.length))
    md.update(bytes)
    if (kind == Encoding.TagKind) {
      md.update(Encoding.varInt(children.length))
      children.foreach(c => md.update(c))
    }
    md.digest()
  }
}
