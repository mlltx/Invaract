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
    */
  val CurrentVersion: Int = 2

  val Algorithm: String = "SHA-256"

  def hash(node: CanonicalNode): Fingerprint =
    Fingerprint(CurrentVersion, Algorithm, toHex(digest(Encoding.encode(node))))

  private def digest(bytes: Array[Byte]): Array[Byte] =
    java.security.MessageDigest.getInstance(Algorithm).digest(bytes)

  private def toHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02x"))
    sb.toString()
  }
}
