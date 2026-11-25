// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import scala.collection.JavaConverters._

/** Writes JSON text (RFC 8259) from plain Scala and Java values, with no JSON library at runtime: Invaract's events are
  * published from inside engines whose own Jackson versions it must not disturb (see `NotificationJson`).
  *
  * Every string is escaped so the result is always well-formed, whatever a job put in it: `"` and `\`, every control
  * character below U+0020 (as a short escape where JSON has one, else a six-character unicode escape), U+2028 and U+2029 (legal JSON, but line breaks to JavaScript
  * and to line-oriented readers), and an unpaired UTF-16 surrogate, which no encoding can represent, replaced with
  * U+FFFD. A double that is not finite (`NaN`, `Infinity`) is `null`, JSON having no such numbers.
  *
  * Objects keep their entries' order: an ordered `Seq` of pairs (`JsonText.obj`) renders in that order, a `Map` in
  * its iteration order (or sorted by key, for canonical text).
  */
private[invaract] object JsonText {

  /** An object whose entries render in the order given. */
  final case class Obj(entries: Seq[(String, Any)])

  /** `Obj` with the entries whose value is `None` left out: an absent optional field, not `null`. */
  def obj(entries: (String, Any)*): Obj = Obj(entries.filter { case (_, v) => v != None })

  def render(value: Any): String = {
    val sb = new java.lang.StringBuilder(256)
    write(value, sb, sortKeys = false)
    sb.toString
  }

  /** The same, with every object's keys sorted: equal content gives equal text, however it was built. */
  def canonical(value: Any): String = {
    val sb = new java.lang.StringBuilder(256)
    write(value, sb, sortKeys = true)
    sb.toString
  }

  def quote(s: String): String = {
    val sb = new java.lang.StringBuilder(s.length + 2)
    writeString(s, sb)
    sb.toString
  }

  private def write(value: Any, sb: java.lang.StringBuilder, sortKeys: Boolean): Unit = value match {
    case null                    => sb.append("null")
    case s: String               => writeString(s, sb)
    case Obj(entries)            => writeObject(entries, sb, sortKeys)
    case m: Map[_, _]            => writeObject(m.toSeq.map { case (k, v) => String.valueOf(k) -> v }, sb, sortKeys)
    case m: java.util.Map[_, _]  => writeObject(m.asScala.toSeq.map { case (k, v) => String.valueOf(k) -> v }, sb, sortKeys)
    case Some(v)                 => write(v, sb, sortKeys)
    case None                    => sb.append("null")
    case b: Boolean              => sb.append(b)
    case d: Double               => writeDouble(d, sb)
    case f: Float                => writeDouble(f.toDouble, sb)
    case n: java.lang.Double     => writeDouble(n.doubleValue, sb)
    case n: java.lang.Float      => writeDouble(n.doubleValue, sb)
    case n: BigDecimal           => sb.append(n.bigDecimal.toPlainString)
    case n: java.math.BigDecimal => sb.append(n.toPlainString)
    case n: Number               => sb.append(n.toString)
    case it: Iterable[_]         => writeArray(it.iterator, sb, sortKeys)
    case a: Array[_]             => writeArray(a.iterator, sb, sortKeys)
    case it: java.lang.Iterable[_] => writeArray(it.iterator.asScala, sb, sortKeys)
    case other                   => writeString(other.toString, sb)
  }

  private def writeDouble(d: Double, sb: java.lang.StringBuilder): Unit =
    if (d.isNaN || d.isInfinite) sb.append("null") else sb.append(d)

  private def writeObject(entries: Seq[(String, Any)], sb: java.lang.StringBuilder, sortKeys: Boolean): Unit = {
    val ordered = if (sortKeys) entries.sortBy(_._1) else entries
    sb.append('{')
    var first = true
    ordered.foreach { case (k, v) =>
      if (!first) sb.append(',')
      first = false
      writeString(k, sb)
      sb.append(':')
      write(v, sb, sortKeys)
    }
    sb.append('}')
  }

  private def writeArray(items: Iterator[_], sb: java.lang.StringBuilder, sortKeys: Boolean): Unit = {
    sb.append('[')
    var first = true
    items.foreach { item =>
      if (!first) sb.append(',')
      first = false
      write(item, sb, sortKeys)
    }
    sb.append(']')
  }

  private val Hex = "0123456789abcdef"

  private def writeString(s: String, sb: java.lang.StringBuilder): Unit = {
    sb.append('"')
    var i = 0
    val n = s.length
    while (i < n) {
      val c = s.charAt(i)
      c match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case _ if c < 0x20 || c == ' ' || c == ' ' => unicodeEscape(c, sb)
        case _ if Character.isHighSurrogate(c) =>
          if (i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
            sb.append(c).append(s.charAt(i + 1))
            i += 1
          } else sb.append('�')
        case _ if Character.isLowSurrogate(c) => sb.append('�')
        case _ => sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }

  private def unicodeEscape(c: Char, sb: java.lang.StringBuilder): Unit =
    sb.append("\\u").append(Hex.charAt((c >> 12) & 0xf)).append(Hex.charAt((c >> 8) & 0xf)).append(Hex.charAt((c >> 4) & 0xf)).append(Hex.charAt(c & 0xf))
}
