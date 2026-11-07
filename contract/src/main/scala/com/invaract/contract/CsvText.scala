// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.nio.charset.Charset
import java.time.format.DateTimeFormatter

import scala.util.Try

/** How a `CsvOptions` value is read: the text a contract writes for a character (`\t` for a tab, as YAML's plain text and
  * Spark's options both write it), a character set name, and a date-time pattern. Shared by the validator, and by every engine
  * that compares a read's or write's options with the declared ones. */
object CsvText {

  /** The one character `text` stands for (`\t` is a tab), or `None` when it is not one character. */
  def character(text: String): Option[Char] =
    text match {
      case "\\t" => Some('\t')
      case one if one.length == 1 => Some(one.charAt(0))
      case _ => None
    }

  /** Whether `name` is a character set the JVM knows, by any of its names (`UTF-8`, `utf8`, `latin1`). */
  def knownEncoding(name: String): Boolean = Try(Charset.isSupported(name.trim)).getOrElse(false)

  /** A character set's canonical name (`latin1` is `ISO-8859-1`), or the name as written when the JVM does not know it. */
  def canonicalEncoding(name: String): String = Try(Charset.forName(name.trim).name).getOrElse(name.trim)

  /** Why `pattern` is not a `DateTimeFormatter` pattern, or `None` when it is one. */
  def patternError(pattern: String): Option[String] =
    Try(DateTimeFormatter.ofPattern(pattern)).failed.toOption.map(e => Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
}
