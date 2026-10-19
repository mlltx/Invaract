// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}

import scala.util.Try

/** The columns of a pyarrow schema as pyarrow prints it, which is how the Python SDK's Parquet write publishes the schema its
  * files are written with (the `schema` display item of `WriteToParquet`):
  *
  * {{{
  * id: int64 not null
  * amount: decimal128(10, 2)
  * tags: list<item: string>
  *   child 0, item: string
  * }}}
  *
  * One line per column, `name: type`, with ` not null` for a column that may not be null; the indented lines under a nested
  * column repeat what its type already says, and a `-- schema metadata --` block may follow. The types map as
  * docs/BEAM_PYTHON_ADAPTER.md's type table says: a timestamp with a time zone is an instant (`timestamp`), one without is a
  * local date-time (`timestamp_ntz`), and a type with no neutral equivalent (an unsigned integer, a half float) keeps its own
  * name as an `OtherType`, which equals only itself. Anything that is not a schema in this form is `None`, never a guess.
  */
private[beamadapter] object PyArrowSchemas {

  def parse(text: String): Option[LogicalSchema] =
    Try {
      val lines = text.split("\n").toList.takeWhile(!_.startsWith("-- ")).filter(l => l.nonEmpty && !l.head.isWhitespace)
      if (lines.isEmpty) None else Some(LogicalSchema(lines.map(field)))
    }.toOption.flatten

  /** `name: type` (with ` not null` when the column may not be null). */
  private def field(text: String): LogicalField = {
    val at = text.indexOf(": ")
    // a pyarrow type name starts with a letter (`int64`, `list<...>`); anything else is not a field line (a line that ends at the
    // colon throws here, which `parse` turns into no schema)
    require(at > 0 && text.charAt(at + 2).isLetter, s"not a pyarrow field: $text")
    val (dataType, nullable) = withNullability(text.substring(at + 2))
    LogicalField(text.substring(0, at), dataType, nullable)
  }

  private def withNullability(text: String): (LogicalType, Boolean) = {
    val t = text.trim
    if (t.endsWith(NotNull)) (dataType(t.stripSuffix(NotNull).trim), false) else (dataType(t), true)
  }

  private val NotNull = " not null"

  private val Timestamp = """timestamp\[\w+(, tz=(.+))?\]""".r
  private val Decimal = """decimal(?:32|64|128|256)\((\d+), (-?\d+)\)""".r

  private def dataType(t: String): LogicalType = t.indexOf('<') match {
    case -1 => primitive(t)
    case open =>
      val inner = t.substring(open + 1, t.lastIndexOf('>'))
      t.substring(0, open) match {
        case "list" | "large_list" | "list_view" | "large_list_view" | "fixed_size_list" => LogicalType.ArrayType(withNullability(valueOf(inner))._1)
        case "struct" => LogicalType.StructType(topLevel(inner).map(field))
        case "map" =>
          topLevel(inner) match {
            case key :: value :: Nil => LogicalType.MapType(withNullability(key)._1, withNullability(value)._1)
            case _                   => LogicalType.OtherType(t)
          }
        // a dictionary-encoded column holds values of its value type
        case "dictionary" =>
          topLevel(inner).collectFirst { case v if v.startsWith("values=") => dataType(v.stripPrefix("values=")) }.getOrElse(LogicalType.OtherType(t))
        case _ => LogicalType.OtherType(t)
      }
  }

  /** The type in `item: T`, a list's element as pyarrow names it. */
  private def valueOf(element: String): String = element.indexOf(": ") match {
    case -1 => element
    case at => element.substring(at + 2)
  }

  /** `text` split at the commas that are not inside a nested type, a precision or a unit. */
  private def topLevel(text: String): List[String] = {
    val parts = List.newBuilder[String]
    var depth = 0
    var start = 0
    text.indices.foreach { i =>
      text(i) match {
        case '<' | '(' | '[' => depth += 1
        case '>' | ')' | ']' => depth -= 1
        case ',' if depth == 0 =>
          parts += text.substring(start, i).trim
          start = i + 1
        case _ => ()
      }
    }
    parts += text.substring(start).trim
    parts.result().filter(_.nonEmpty)
  }

  private def primitive(t: String): LogicalType = t match {
    case "bool"                                       => LogicalType.BooleanType
    case "int8"                                       => LogicalType.ByteType
    case "int16"                                      => LogicalType.ShortType
    case "int32"                                      => LogicalType.IntegerType
    case "int64"                                      => LogicalType.LongType
    case "float"                                      => LogicalType.FloatType
    case "double"                                     => LogicalType.DoubleType
    case "string" | "large_string" | "string_view"    => LogicalType.StringType
    case "binary" | "large_binary" | "binary_view"    => LogicalType.BinaryType
    case "date32[day]" | "date64[ms]"                 => LogicalType.DateType
    case "month_day_nano_interval"                    => LogicalType.IntervalType
    case Timestamp(_, zone)                           => if (zone == null) LogicalType.TimestampNtzType else LogicalType.TimestampType
    case Decimal(p, s) if p.toInt <= 38               => LogicalType.DecimalType(p.toInt, s.toInt)
    case other if other.startsWith("time32[") || other.startsWith("time64[") => LogicalType.TimeType
    case other if other.startsWith("fixed_size_binary[")                    => LogicalType.BinaryType
    case other                                        => LogicalType.OtherType(other)
  }
}
