// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.util.Locale

/** An engine-independent column type: what a contract's `type:` field means,
  * and what an engine adapter maps its own native types *to* before anything is
  * compared against a contract.
  *
  * Until this existed, the "type vocabulary" of a contract was whatever
  * `org.apache.spark.sql.types.DataType` happened to print: scalar types were
  * compared against Spark's `DataType.typeName`, nested ones were parsed with
  * Spark's `DataType.fromDDL`. That made every checker depend on Spark, and
  * left an engine whose types are spelled differently (BigQuery's `INT64`,
  * `NUMERIC`, a `REPEATED` mode that is an array in everything but name; Beam's
  * schema `FieldType`) with nowhere to land. Now an adapter supplies a
  * `LogicalSchema` and the checkers work on that alone.
  *
  * ## Two renderings
  *
  *   - `typeName` is the contract vocabulary's own keyword (`integer`, `long`,
  *     `decimal(10,2)`, `array`) — what a plain, non-nested `type:` is compared
  *     against, exactly.
  *   - `catalogString` is the compact form used inside a nested type and in
  *     violation messages (`int`, `bigint`, `array<int>`,
  *     `struct<a:int,b:string>`).
  *
  * Both match what Spark prints for the equivalent type (asserted by
  * `spark-adapter`'s `SparkSchemasSpec`), so introducing this model changed no
  * existing verdict and no existing message.
  */
sealed trait LogicalType {

  /** The contract vocabulary's keyword for this type. A container is just
    * `array`/`map`/`struct` (the shallow form of a declaration: "any array"); a
    * decimal carries its parameters, `decimal(10,2)`.
    */
  def typeName: String

  /** The compact rendering used inside nested types and in violation messages. */
  def catalogString: String

  /** True for `array`/`map`/`struct`. */
  def isNested: Boolean = false
}

object LogicalType {

  /** A type with no parameters and no children. */
  sealed abstract class Scalar(val typeName: String, val catalogString: String) extends LogicalType

  case object StringType extends Scalar("string", "string")
  case object BooleanType extends Scalar("boolean", "boolean")
  case object ByteType extends Scalar("byte", "tinyint")
  case object ShortType extends Scalar("short", "smallint")
  case object IntegerType extends Scalar("integer", "int")
  case object LongType extends Scalar("long", "bigint")
  case object FloatType extends Scalar("float", "float")
  case object DoubleType extends Scalar("double", "double")
  case object DateType extends Scalar("date", "date")

  /** An instant on the timeline (timezone-aware). */
  case object TimestampType extends Scalar("timestamp", "timestamp")

  /** A wall-clock date-time with no timezone. */
  case object TimestampNtzType extends Scalar("timestamp_ntz", "timestamp_ntz")
  case object BinaryType extends Scalar("binary", "binary")

  case class DecimalType(precision: Int, scale: Int) extends LogicalType {
    def typeName: String = s"decimal($precision,$scale)"
    def catalogString: String = typeName
  }

  case class ArrayType(elementType: LogicalType) extends LogicalType {
    def typeName: String = "array"
    def catalogString: String = s"array<${elementType.catalogString}>"
    override def isNested: Boolean = true
  }

  case class MapType(keyType: LogicalType, valueType: LogicalType) extends LogicalType {
    def typeName: String = "map"
    def catalogString: String = s"map<${keyType.catalogString},${valueType.catalogString}>"
    override def isNested: Boolean = true
  }

  case class StructType(fields: List[LogicalField]) extends LogicalType {
    def typeName: String = "struct"
    def catalogString: String = s"struct<${fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString(",")}>"
    override def isNested: Boolean = true
  }

  /** A native type with no logical equivalent (an interval, a geography, a
    * UDT, ...). It compares equal only to the identical native type, never to
    * anything else: an adapter must not guess a lossy mapping, since a wrong
    * guess would make a mismatch invisible.
    */
  case class OtherType(typeName: String, catalogString: String) extends LogicalType

  object OtherType {
    def apply(name: String): OtherType = OtherType(name, name)
  }

  /** Parses a type as a contract writes it — `array<int>`,
    * `map<string,decimal(10,2)>`, `struct<a:int,b:string>`, arbitrarily nested —
    * or `None` when the text is not a type this grammar understands.
    *
    * The grammar is the one Spark's DDL parser accepts for these forms (a
    * contract's nested types have always been written that way), kept as the
    * contract's own definition rather than delegated to an engine:
    *
    *   - scalar keywords and their aliases: `int`/`integer`, `bigint`/`long`,
    *     `smallint`/`short`, `tinyint`/`byte`, `float`/`real`, `double`,
    *     `string`, `boolean`, `date`, `timestamp`/`timestamp_ltz`,
    *     `timestamp_ntz`, `binary`;
    *   - `decimal`/`dec`/`numeric`, `decimal(p)` and `decimal(p,s)` — a bare
    *     `decimal` is `decimal(10,0)`, as in Spark;
    *   - `array<T>`, `map<K,V>`, `struct<name:T,...>` (`name T` also works; a
    *     name may be `` `quoted` ``; `NOT NULL` and `COMMENT '...'` after a
    *     field's type are accepted and, apart from nullability, ignored);
    *   - any other identifier, optionally with parenthesised parameters, is an
    *     `OtherType` (so `array<timestamp_ntz>` and a vendor type nest
    *     alike).
    *
    * Keywords are case-insensitive; struct field names keep their case.
    */
  def parse(text: String): Option[LogicalType] =
    try Some(new TypeParser(text).parseAll())
    catch { case TypeParser.Failure => None }

  /** Struct field case rule, shared by callers that match names under a
    * session's case sensitivity.
    */
  def nameKey(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  private final class TypeParser(text: String) {
    private var pos = 0

    def parseAll(): LogicalType = {
      val t = parseType()
      skipWhitespace()
      if (pos != text.length) TypeParser.fail()
      t
    }

    private def parseType(): LogicalType = {
      skipWhitespace()
      val keyword = readIdentifier().toLowerCase(Locale.ROOT)
      keyword match {
        case "array" =>
          expect('<')
          val element = parseType()
          expect('>')
          ArrayType(element)
        case "map" =>
          expect('<')
          val key = parseType()
          expect(',')
          val value = parseType()
          expect('>')
          MapType(key, value)
        case "struct" =>
          expect('<')
          StructType(parseFields())
        case "decimal" | "dec" | "numeric" => parseDecimal()
        case "string"                      => StringType
        case "boolean"                     => BooleanType
        case "tinyint" | "byte"            => ByteType
        case "smallint" | "short"          => ShortType
        case "int" | "integer"             => IntegerType
        case "bigint" | "long"             => LongType
        case "float" | "real"              => FloatType
        case "double"                      => DoubleType
        case "date"                        => DateType
        case "timestamp" | "timestamp_ltz" => TimestampType
        case "timestamp_ntz"               => TimestampNtzType
        case "binary"                      => BinaryType
        case other =>
          val params = readParameters()
          OtherType(if (params.isEmpty) other else s"$other($params)")
      }
    }

    private def parseDecimal(): LogicalType = {
      skipWhitespace()
      if (peek == '(') {
        pos += 1
        val precision = readInt()
        skipWhitespace()
        val scale = if (peek == ',') { pos += 1; readInt() } else 0
        expect(')')
        DecimalType(precision, scale)
      } else DecimalType(10, 0)
    }

    /** The comma-separated body of a `(...)` suffix with whitespace removed, or
      * `""` when there is none.
      */
    private def readParameters(): String = {
      skipWhitespace()
      if (peek != '(') ""
      else {
        val close = text.indexOf(')', pos)
        if (close < 0) TypeParser.fail()
        val body = text.substring(pos + 1, close).filterNot(_.isWhitespace)
        pos = close + 1
        body
      }
    }

    private def parseFields(): List[LogicalField] = {
      skipWhitespace()
      if (peek == '>') { pos += 1; Nil }
      else {
        val fields = List.newBuilder[LogicalField]
        var more = true
        while (more) {
          fields += parseField()
          skipWhitespace()
          peek match {
            case ',' => pos += 1
            case '>' => pos += 1; more = false
            case _   => TypeParser.fail()
          }
        }
        fields.result()
      }
    }

    private def parseField(): LogicalField = {
      skipWhitespace()
      val name = if (peek == '`') readQuotedName() else readIdentifier()
      skipWhitespace()
      if (peek == ':') pos += 1
      val dataType = parseType()
      var nullable = true
      var more = true
      while (more) {
        if (tryKeyword("not")) { if (!tryKeyword("null")) TypeParser.fail(); nullable = false }
        else if (tryKeyword("comment")) skipQuotedString()
        else more = false
      }
      LogicalField(name, dataType, nullable)
    }

    private def readQuotedName(): String = {
      pos += 1 // opening backtick
      val sb = new StringBuilder
      var done = false
      while (!done) {
        if (pos >= text.length) TypeParser.fail()
        val c = text.charAt(pos)
        if (c == '`') {
          if (pos + 1 < text.length && text.charAt(pos + 1) == '`') { sb.append('`'); pos += 2 }
          else { pos += 1; done = true }
        } else { sb.append(c); pos += 1 }
      }
      if (sb.isEmpty) TypeParser.fail()
      sb.toString
    }

    private def skipQuotedString(): Unit = {
      skipWhitespace()
      if (peek != '\'') TypeParser.fail()
      pos += 1
      val close = text.indexOf('\'', pos)
      if (close < 0) TypeParser.fail()
      pos = close + 1
    }

    /** Consumes `keyword` (case-insensitively, as a whole word) if it is next. */
    private def tryKeyword(keyword: String): Boolean = {
      skipWhitespace()
      val start = pos
      if (pos < text.length && isIdentifierStart(text.charAt(pos)) && readIdentifier().equalsIgnoreCase(keyword)) true
      else { pos = start; false }
    }

    private def readIdentifier(): String = {
      val start = pos
      if (pos >= text.length || !isIdentifierStart(text.charAt(pos))) TypeParser.fail()
      while (pos < text.length && isIdentifierPart(text.charAt(pos))) pos += 1
      text.substring(start, pos)
    }

    private def readInt(): Int = {
      skipWhitespace()
      val start = pos
      while (pos < text.length && text.charAt(pos).isDigit) pos += 1
      if (start == pos) TypeParser.fail()
      text.substring(start, pos).toInt
    }

    private def expect(c: Char): Unit = {
      skipWhitespace()
      if (peek != c) TypeParser.fail()
      pos += 1
    }

    private def peek: Char = if (pos < text.length) text.charAt(pos) else '\u0000'

    private def skipWhitespace(): Unit =
      while (pos < text.length && text.charAt(pos).isWhitespace) pos += 1

    private def isIdentifierStart(c: Char): Boolean = c.isLetter || c == '_'
    private def isIdentifierPart(c: Char): Boolean = c.isLetterOrDigit || c == '_'
  }

  private object TypeParser {
    case object Failure extends RuntimeException with scala.util.control.NoStackTrace
    def fail(): Nothing = throw Failure
  }
}

/** One column of a `LogicalSchema` (or one field of a `LogicalType.StructType`).
  * Nullability defaults to `true`, the safe assumption for an engine that does
  * not report it.
  */
case class LogicalField(name: String, dataType: LogicalType, nullable: Boolean = true)

/** A dataset's actual columns as an engine reports them, in engine-neutral
  * terms — what an adapter hands the verification checkers in place of its own
  * schema type (Spark's `StructType`, a BigQuery `Schema`, a Beam `Schema`).
  * See `LogicalType`.
  */
case class LogicalSchema(fields: List[LogicalField]) {
  def isEmpty: Boolean = fields.isEmpty
}

object LogicalSchema {
  val empty: LogicalSchema = LogicalSchema(Nil)
}
