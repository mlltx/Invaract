// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import org.scalatest.funsuite.AnyFunSuite

/** The schema a Python Parquet write publishes, as pyarrow prints it (docs/BEAM_PYTHON_ADAPTER.md, D5). The texts are pyarrow's own
  * output for the schemas named, taken from a real `WriteToParquet` (record_fixtures.py records one in parquet_job.pb). */
class PyArrowSchemasSpec extends AnyFunSuite {
  import LogicalType._

  private def columns(text: String): List[(String, LogicalType, Boolean)] =
    PyArrowSchemas.parse(text).getOrElse(fail(s"not parsed: $text")).fields.map(f => (f.name, f.dataType, f.nullable))

  test("one column per unindented line, nullable unless it says not null") {
    assert(columns("id: int64 not null\namount: int64") == List(("id", LongType, false), ("amount", LongType, true)))
  }

  test("the lines under a nested column and the metadata block are not columns") {
    val text = "tags: list<item: string>\n  child 0, item: string\nm: int32\n-- schema metadata --\npandas: '{\"index_columns\": []}'"
    assert(columns(text) == List(("tags", ArrayType(StringType), true), ("m", IntegerType, true)))
  }

  test("the scalar types") {
    val text = List(
      "a: bool", "b: int8", "c: int16", "d: int32", "e: int64", "f: float", "g: double", "h: string", "i: large_string", "j: binary",
      "k: large_binary", "l: date32[day]", "m: date64[ms]", "n: time64[us]", "o: time32[ms]", "p: fixed_size_binary[16]",
      "q: month_day_nano_interval", "r: string_view", "s: binary_view"
    ).mkString("\n")
    assert(columns(text).map(_._2) == List(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, StringType, BinaryType, BinaryType, DateType,
      DateType, TimeType, TimeType, BinaryType, IntervalType, StringType, BinaryType
    ))
  }

  test("a timestamp with a time zone is an instant; one without is a local date-time") {
    assert(columns("a: timestamp[us, tz=UTC]\nb: timestamp[ns]\nc: timestamp[ms, tz=+01:00]").map(_._2) == List(TimestampType, TimestampNtzType, TimestampType))
  }

  test("a decimal keeps its precision and scale up to 38 digits; a wider one has no neutral equivalent") {
    assert(columns("a: decimal128(10, 2)\nb: decimal256(38, 0)\nc: decimal256(39, 4)\nd: decimal32(5, -1)").map(_._2) ==
      List(DecimalType(10, 2), DecimalType(38, 0), OtherType("decimal256(39, 4)"), DecimalType(5, -1)))
  }

  test("a type with no neutral equivalent keeps its own name, so it matches only itself") {
    assert(columns("a: uint32\nb: halffloat\nc: duration[s]\nd: null").map(_._2) ==
      List(OtherType("uint32"), OtherType("halffloat"), OtherType("duration[s]"), OtherType("null")))
  }

  test("lists, structs and maps, nested, with the nullability of what they hold dropped as the contract's types do") {
    val text = "tags: list<item: string not null>\n" +
      "fixed: fixed_size_list<item: int32>[3]\n" +
      "big: large_list<item: list<item: double>>\n" +
      "customer: struct<name: string, since: date32[day] not null, price: decimal128(10, 2)> not null\n" +
      "attrs: map<string, int32>\n" +
      "nested: map<string, struct<a: int64>>"
    assert(columns(text) == List(
      ("tags", ArrayType(StringType), true),
      ("fixed", ArrayType(IntegerType), true),
      ("big", ArrayType(ArrayType(DoubleType)), true),
      ("customer", StructType(List(LogicalField("name", StringType), LogicalField("since", DateType, nullable = false), LogicalField("price", DecimalType(10, 2)))), false),
      ("attrs", MapType(StringType, IntegerType), true),
      ("nested", MapType(StringType, StructType(List(LogicalField("a", LongType)))), true)
    ))
  }

  test("a list view is a list, and a list written without an item name still has its element type") {
    assert(columns("a: list_view<item: int64>\nb: large_list_view<item: bool>\nc: list<int16>").map(_._2) ==
      List(ArrayType(LongType), ArrayType(BooleanType), ArrayType(ShortType)))
  }

  test("a dictionary-encoded column holds its value type") {
    assert(columns("a: dictionary<values=string, indices=int32, ordered=0>").map(_._2) == List(StringType))
    assert(columns("b: dictionary<indices=int8>").map(_._2) == List(OtherType("dictionary<indices=int8>")))
  }

  test("a nested type this reader does not know, or a map that is not a key and a value, keeps its own name") {
    assert(columns("a: union<x: int32>\nb: map<string>").map(_._2) == List(OtherType("union<x: int32>"), OtherType("map<string>")))
  }

  test("text that is not a pyarrow schema is no schema, never a guess") {
    assert(PyArrowSchemas.parse("").isEmpty)
    assert(PyArrowSchemas.parse("  child 0, item: string").isEmpty, "only indented lines")
    assert(PyArrowSchemas.parse("-- schema metadata --\nid: int64").isEmpty, "nothing before the metadata")
    assert(PyArrowSchemas.parse("{'fields': []}").isEmpty, "a dictionary, not name: type lines")
    assert(PyArrowSchemas.parse("id int64").isEmpty)
    assert(PyArrowSchemas.parse(": int64").isEmpty, "a column with no name")
  }

  test("a column name that itself holds a colon is cut at the first colon and space") {
    assert(PyArrowSchemas.parse("a:b: int64") == Some(LogicalSchema(List(LogicalField("a:b", LongType)))))
  }
}
