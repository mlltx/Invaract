// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import java.text.Normalizer

import CanonicalNode.stringLeaf

/** Canonicalises `ir.Literal(value, literalType)` — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §5 "Literal and type
  * normalisation rules". Dispatches on `value`'s actual runtime JVM type,
  * never on `literalType` (a free-form string this module does not
  * interpret, matching the contract module's own type vocabulary) —
  * `literalType` is still always hashed alongside the value, as its own
  * field, so two literals with the same underlying number but a different
  * declared `literalType` are still a real, visible difference.
  */
object LiteralEncoding {

  def encode(value: Any, literalType: String): CanonicalNode =
    CTag("Literal", List(encodeValue(value), stringLeaf(literalType)))

  private def encodeValue(value: Any): CanonicalNode = value match {
    // A typed SQL NULL (`value == null`, `literalType` still populated) -
    // fully understood, not an UnknownExpression. No value bytes at all;
    // literalType alone (hashed by the caller above) is enough to
    // distinguish e.g. a null integer from a null string.
    case null => CTag("Null")

    // Integral types: canonical decimal ASCII digits, explicit '-' only
    // when negative, no leading zeros/plus - stable regardless of the
    // value's original bit width, so a translator representing the same
    // number as Int vs. Long never spuriously differs here (a difference
    // in the accompanying literalType string still would, deliberately).
    case i: Int              => CTag("Int", List(stringLeaf(BigInt(i).toString)))
    case l: Long              => CTag("Int", List(stringLeaf(BigInt(l).toString)))
    case s: Short             => CTag("Int", List(stringLeaf(BigInt(s.toInt).toString)))
    case b: Byte              => CTag("Int", List(stringLeaf(BigInt(b.toInt).toString)))
    case bi: BigInt           => CTag("Int", List(stringLeaf(bi.toString)))
    case bi: java.math.BigInteger => CTag("Int", List(stringLeaf(bi.toString)))

    // Exact precision, never a display-normalized value: the (unscaled
    // value, scale) pair as the source model actually carries it. `1.20`
    // and `1.2` (unscaledValue 120/scale 2 vs. 12/scale 1) are genuinely
    // different declared precisions and must not be folded together, per
    // the design doc's explicit warning against display-formatting-based
    // canonicalisation.
    case bd: BigDecimal =>
      CTag("Decimal", List(stringLeaf(bd.underlying.unscaledValue.toString), stringLeaf(bd.scale.toString)))
    case bd: java.math.BigDecimal =>
      CTag("Decimal", List(stringLeaf(bd.unscaledValue.toString), stringLeaf(bd.scale.toString)))

    // IEEE-754 bit pattern, not a textual rendering - avoids ambiguity
    // between different textual forms of the same float ("0.1" vs.
    // "1.0E-1"). `doubleToLongBits`/`floatToIntBits` (the non-raw
    // variants) canonicalize every NaN payload identically - distinguishing
    // NaN payloads is not a plausible business-logic signal - while -0.0
    // vs. 0.0 stays bit-exact (not folded together), since IEEE-754 sign
    // matters to some computations. See §5's own table for this rationale.
    case d: Double => CTag("Double", List(stringLeaf(java.lang.Double.doubleToLongBits(d).toString)))
    case f: Float  => CTag("Float", List(stringLeaf(java.lang.Float.floatToIntBits(f).toString)))

    case bool: Boolean => CTag("Boolean", List(CanonicalNode.boolLeaf(bool)))

    // Unicode NFC normalisation is the one endorsed string normalisation
    // here: two byte-different encodings of the identical rendered text
    // (e.g. a precomposed vs. decomposed accented character) are the same
    // string by any reasonable definition. Not case-folding, not
    // trimming, not locale-aware comparison - none of those are applied.
    case s: String => CTag("String", List(stringLeaf(Normalizer.normalize(s, Normalizer.Form.NFC))))

    // A Catalyst `BinaryType` literal's value is a raw `Array[Byte]` -
    // confirmed directly (`Literal.value.getClass.getName == "[B"` for a
    // real Spark binary literal), not a Catalyst-internal wrapper type the
    // way `ArrayType`/`StructType`/`MapType` literals are (`GenericArrayData`
    // et al., which do have a stable, content-based `toString` - confirmed
    // directly too, e.g. `[1,2,3]` for the same array twice). `Array`'s own
    // `toString`/`equals`/`hashCode` are reference-identity-based
    // (`[B@1a2b3c4d`), so falling through to the generic
    // `UnrecognizedLiteralValueType` case below - which hashes `toString`
    // - would silently make the fingerprint of the exact same binary
    // literal different on every JVM run: precisely the "same model ->
    // same fingerprint" guarantee this whole module exists to provide,
    // broken by the one literal runtime type whose default representation
    // isn't content-based. Hashed by actual byte content instead, via the
    // existing `CLeaf` wrapper (a `Vector[Byte]`, never a raw `Array` -
    // see `CanonicalNode`'s own doc for why that distinction matters here
    // too).
    case bytes: Array[Byte] => CTag("Binary", List(CLeaf(bytes.toVector)))

    // `Literal.value: Any` is otherwise unconstrained by the IR - this is
    // the best-effort fallback for a runtime type this encoder has no
    // dedicated case for. Deliberately a distinct tag, never silently
    // reusing the String encoding above, so a canonical form can always be
    // told apart from a genuine string literal. See docs/
    // SEMANTIC_LINEAGE_FINGERPRINTING.md §11's named limitation for this.
    case other => CTag("UnrecognizedLiteralValueType", List(stringLeaf(other.toString)))
  }
}
