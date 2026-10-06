// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/** `SparkSchemas` is the one place a Spark type becomes the engine-neutral
  * `LogicalType`, and `LogicalType.parse` replaced Spark's own `DataType.fromDDL`
  * for a contract's nested types. Both swaps must change nothing a user can see,
  * so this spec pins them against Spark itself rather than against hand-written
  * expectations: for every type below, the logical type prints exactly what Spark
  * prints (`typeName`, `catalogString`) and the neutral parser reads exactly what
  * Spark's parser reads.
  */
class SparkSchemasSpec extends AnyFunSuite {

  private val scalars: List[DataType] = List(
    StringType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
    DateType, TimestampType, TimestampNTZType, BinaryType
  )

  private val decimals: List[DataType] = List(DecimalType(10, 2), DecimalType(38, 18), DecimalType(1, 0), DecimalType.SYSTEM_DEFAULT)

  private val nested: List[DataType] = List(
    ArrayType(IntegerType),
    ArrayType(ArrayType(StringType)),
    MapType(StringType, LongType),
    MapType(StringType, ArrayType(DecimalType(5, 1))),
    StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))),
    StructType(Seq(StructField("pt", StructType(Seq(StructField("x", IntegerType), StructField("y", DoubleType)))))),
    ArrayType(StructType(Seq(StructField("k", StringType), StructField("v", MapType(StringType, TimestampNTZType))))),
    StructType(Nil)
  )

  // Types with no logical equivalent: carried through under Spark's own spelling.
  private val unmapped: List[DataType] =
    List(NullType, CalendarIntervalType, DayTimeIntervalType(), YearMonthIntervalType(), CharType(3), VarcharType(5))

  private val everything = scalars ++ decimals ++ nested ++ unmapped

  test("every mapped type prints exactly what Spark prints: typeName and catalogString") {
    everything.foreach { dt =>
      val logical = SparkSchemas.toLogicalType(dt)
      assert(logical.typeName == dt.typeName, s"typeName of $dt")
      assert(logical.catalogString == dt.catalogString, s"catalogString of $dt")
    }
  }

  test("a struct whose field names need quoting renders as Spark does") {
    // Whatever Spark does with a space/dot/backtick in a field name, the logical rendering must follow it,
    // since this string appears in violation messages.
    val dt = StructType(Seq(StructField("a b", IntegerType), StructField("c.d", StringType), StructField("e`f", LongType)))
    assert(SparkSchemas.toLogicalType(dt).catalogString == dt.catalogString)
  }

  test("only array, map and struct are nested") {
    nested.foreach(dt => assert(SparkSchemas.toLogicalType(dt).isNested, dt))
    (scalars ++ decimals ++ unmapped).foreach(dt => assert(!SparkSchemas.toLogicalType(dt).isNested, dt))
  }

  test("the scalar and decimal mappings land on the logical type of the same meaning") {
    assert(SparkSchemas.toLogicalType(StringType) == LogicalType.StringType)
    assert(SparkSchemas.toLogicalType(BooleanType) == LogicalType.BooleanType)
    assert(SparkSchemas.toLogicalType(ByteType) == LogicalType.ByteType)
    assert(SparkSchemas.toLogicalType(ShortType) == LogicalType.ShortType)
    assert(SparkSchemas.toLogicalType(IntegerType) == LogicalType.IntegerType)
    assert(SparkSchemas.toLogicalType(LongType) == LogicalType.LongType)
    assert(SparkSchemas.toLogicalType(FloatType) == LogicalType.FloatType)
    assert(SparkSchemas.toLogicalType(DoubleType) == LogicalType.DoubleType)
    assert(SparkSchemas.toLogicalType(DateType) == LogicalType.DateType)
    assert(SparkSchemas.toLogicalType(TimestampType) == LogicalType.TimestampType)
    assert(SparkSchemas.toLogicalType(TimestampNTZType) == LogicalType.TimestampNtzType)
    assert(SparkSchemas.toLogicalType(BinaryType) == LogicalType.BinaryType)
    assert(SparkSchemas.toLogicalType(DecimalType(10, 2)) == LogicalType.DecimalType(10, 2))
  }

  test("containers map element by element, dropping container-level nullability flags") {
    val dt = MapType(StringType, ArrayType(IntegerType, containsNull = false), valueContainsNull = false)
    assert(SparkSchemas.toLogicalType(dt) == LogicalType.MapType(LogicalType.StringType, LogicalType.ArrayType(LogicalType.IntegerType)))
  }

  test("a type with no logical equivalent keeps Spark's own spelling and equals only itself") {
    assert(SparkSchemas.toLogicalType(NullType) == LogicalType.OtherType(NullType.typeName, NullType.catalogString))
    assert(SparkSchemas.toLogicalType(CalendarIntervalType) == LogicalType.OtherType(CalendarIntervalType.typeName, CalendarIntervalType.catalogString))
    assert(SparkSchemas.toLogicalType(CharType(3)) != SparkSchemas.toLogicalType(StringType))
    assert(SparkSchemas.toLogicalType(VarcharType(5)) != SparkSchemas.toLogicalType(VarcharType(6)))
  }

  test("a schema keeps every field's name, order and nullability") {
    val schema = new StructType()
      .add("id", LongType, nullable = false)
      .add("name", StringType)
      .add("tags", ArrayType(StringType))
    assert(
      SparkSchemas.toLogicalSchema(schema) == LogicalSchema(
        List(
          LogicalField("id", LogicalType.LongType, nullable = false),
          LogicalField("name", LogicalType.StringType, nullable = true),
          LogicalField("tags", LogicalType.ArrayType(LogicalType.StringType), nullable = true)
        )
      )
    )
    assert(SparkSchemas.toLogicalSchema(new StructType()) == LogicalSchema.empty)
  }

  // --- LogicalType.parse replaces DataType.fromDDL ---------------------------------------------

  test("LogicalType.parse reads every declared nested type exactly as Spark's DDL parser does") {
    val declared = List(
      "array<int>", "ARRAY<INT>", "array<bigint>", "array<string>", "array<array<int>>",
      "map<string,int>", "map<string, bigint>", "map<string,array<decimal(3,1)>>",
      "struct<a:int,b:string>", "struct<a int, b string>", "struct<a:int NOT NULL,b:string>",
      "struct<`b c`:string>", "struct<a:int COMMENT 'the id'>", "struct<a:struct<b:array<int>>>",
      "struct<a:decimal>", "struct<a:decimal(7)>", "struct<a:dec(3,1)>", "struct<a:numeric(8,3)>",
      "array<timestamp>", "array<timestamp_ltz>", "array<timestamp_ntz>", "array<real>", "array<tinyint>",
      "array<smallint>", "array<binary>", "array<date>", "array<boolean>", "array<double>", "array<float>",
      "array<varchar(5)>", "array<char(3)>", "struct<>", "map<int,map<string,array<long>>>"
    )
    declared.foreach { text =>
      val sparkSide = SparkSchemas.toLogicalType(DataType.fromDDL(text))
      assert(LogicalType.parse(text).contains(sparkSide), s"'$text': neutral parse differs from Spark's")
    }
  }

  test("LogicalType.parse accepts the catalogString of every nested type, so a rendered type can be declared back") {
    (nested ++ decimals).foreach { dt =>
      val logical = SparkSchemas.toLogicalType(dt)
      assert(LogicalType.parse(dt.catalogString).contains(logical), dt.catalogString)
    }
  }

  test("what Spark's DDL parser rejects, LogicalType.parse rejects too") {
    List("array<", "array<>", "map<string>", "struct<a:>", "struct<a:int", "array<int>>", "<int>", "").foreach { text =>
      assert(scala.util.Try(DataType.fromDDL(text)).isFailure, s"'$text' unexpectedly parsed in Spark")
      assert(LogicalType.parse(text).isEmpty, s"'$text' should not parse")
    }
  }
}
