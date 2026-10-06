// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import com.invaract.contract.LogicalType._

import org.scalatest.funsuite.AnyFunSuite

class LogicalTypeTest extends AnyFunSuite {

  private def parsed(text: String): LogicalType =
    LogicalType.parse(text).getOrElse(fail(s"expected '$text' to parse"))

  // --- the two renderings ---------------------------------------------------

  test("each scalar has the contract keyword as typeName and the compact form as catalogString") {
    val expected = List(
      (StringType, "string", "string"),
      (BooleanType, "boolean", "boolean"),
      (ByteType, "byte", "tinyint"),
      (ShortType, "short", "smallint"),
      (IntegerType, "integer", "int"),
      (LongType, "long", "bigint"),
      (FloatType, "float", "float"),
      (DoubleType, "double", "double"),
      (DateType, "date", "date"),
      (TimestampType, "timestamp", "timestamp"),
      (TimestampNtzType, "timestamp_ntz", "timestamp_ntz"),
      (BinaryType, "binary", "binary")
    )
    expected.foreach { case (t, name, catalog) =>
      assert(t.typeName == name, t)
      assert(t.catalogString == catalog, t)
      assert(!t.isNested, t)
    }
  }

  test("a decimal's typeName carries its precision and scale; containers are the bare keyword") {
    assert(DecimalType(10, 2).typeName == "decimal(10,2)")
    assert(DecimalType(10, 2).catalogString == "decimal(10,2)")
    assert(!DecimalType(10, 2).isNested)
    assert(ArrayType(IntegerType).typeName == "array")
    assert(MapType(StringType, LongType).typeName == "map")
    assert(StructType(Nil).typeName == "struct")
  }

  test("containers render their contents in catalogString and report isNested") {
    val struct = StructType(List(LogicalField("a", IntegerType), LogicalField("b", ArrayType(DecimalType(5, 1)))))
    assert(ArrayType(IntegerType).catalogString == "array<int>")
    assert(MapType(StringType, LongType).catalogString == "map<string,bigint>")
    assert(struct.catalogString == "struct<a:int,b:array<decimal(5,1)>>")
    assert(StructType(Nil).catalogString == "struct<>")
    List[LogicalType](ArrayType(IntegerType), MapType(StringType, LongType), struct).foreach(t => assert(t.isNested, t))
  }

  test("OtherType keeps the native spelling, and a one-argument form uses it for both renderings") {
    val other = OtherType("interval day to second", "interval day to second")
    assert(other.typeName == "interval day to second" && other.catalogString == "interval day to second")
    assert(OtherType("geography") == OtherType("geography", "geography"))
    assert(OtherType("null", "void").catalogString == "void")
    assert(!other.isNested)
  }

  // --- parse: scalars -------------------------------------------------------

  test("parse: every scalar keyword and alias, case-insensitively and trimmed") {
    val cases = List(
      "string" -> StringType, "boolean" -> BooleanType,
      "tinyint" -> ByteType, "byte" -> ByteType,
      "smallint" -> ShortType, "short" -> ShortType,
      "int" -> IntegerType, "integer" -> IntegerType,
      "bigint" -> LongType, "long" -> LongType,
      "float" -> FloatType, "real" -> FloatType,
      "double" -> DoubleType, "date" -> DateType,
      "timestamp" -> TimestampType, "timestamp_ltz" -> TimestampType,
      "timestamp_ntz" -> TimestampNtzType, "binary" -> BinaryType
    )
    cases.foreach { case (text, expected) =>
      assert(parsed(text) == expected, text)
      assert(parsed(s"  ${text.toUpperCase}  ") == expected, text)
    }
  }

  test("parse: decimal forms - bare is (10,0), one argument is scale 0, two are explicit; dec/numeric are aliases") {
    assert(parsed("decimal") == DecimalType(10, 0))
    assert(parsed("decimal(7)") == DecimalType(7, 0))
    assert(parsed("decimal(10,2)") == DecimalType(10, 2))
    assert(parsed("DECIMAL( 12 , 4 )") == DecimalType(12, 4))
    assert(parsed("dec(3,1)") == DecimalType(3, 1))
    assert(parsed("numeric(8,3)") == DecimalType(8, 3))
  }

  test("parse: an unknown identifier becomes OtherType, with its parameters whitespace-stripped") {
    assert(parsed("geography") == OtherType("geography"))
    assert(parsed("Varchar( 5 )") == OtherType("varchar(5)"))
    assert(parsed("foo(1, 2)") == OtherType("foo(1,2)"))
  }

  // --- parse: containers ----------------------------------------------------

  test("parse: array, map and struct, nested arbitrarily") {
    assert(parsed("array<int>") == ArrayType(IntegerType))
    assert(parsed("map<string, bigint>") == MapType(StringType, LongType))
    assert(parsed("array<map<string,array<dec(3,1)>>>") == ArrayType(MapType(StringType, ArrayType(DecimalType(3, 1)))))
    assert(parsed("array<timestamp_ntz>") == ArrayType(TimestampNtzType))
    assert(
      parsed("struct<a:int,b:string>") ==
        StructType(List(LogicalField("a", IntegerType), LogicalField("b", StringType)))
    )
    assert(parsed("struct<>") == StructType(Nil))
    assert(parsed(" struct < > ") == StructType(Nil))
  }

  test("parse: a struct field can use a space instead of a colon, a quoted name, NOT NULL and COMMENT") {
    assert(parsed("struct<a int, b string>") == StructType(List(LogicalField("a", IntegerType), LogicalField("b", StringType))))
    assert(parsed("struct<`b c`:string>") == StructType(List(LogicalField("b c", StringType))))
    assert(parsed("struct<`a``b`:int>") == StructType(List(LogicalField("a`b", IntegerType))))
    assert(parsed("struct<a:int NOT NULL>") == StructType(List(LogicalField("a", IntegerType, nullable = false))))
    assert(parsed("struct<a:int not   null,b:string>") == StructType(List(LogicalField("a", IntegerType, nullable = false), LogicalField("b", StringType))))
    assert(parsed("struct<a:int COMMENT 'the id'>") == StructType(List(LogicalField("a", IntegerType))))
    assert(
      parsed("struct<a:int NOT NULL COMMENT 'x', b:string>") ==
        StructType(List(LogicalField("a", IntegerType, nullable = false), LogicalField("b", StringType)))
    )
  }

  test("parse: struct field names keep their case; a field named like a keyword is still a name") {
    assert(parsed("struct<Id:int,Not:string>") == StructType(List(LogicalField("Id", IntegerType), LogicalField("Not", StringType))))
  }

  // --- parse: rejection -----------------------------------------------------

  test("parse: text that is not a type returns None rather than throwing") {
    val bad = List(
      "", "   ", "<int>", "array", "array<>", "array<int", "array<int>>", "array<int> trailing",
      "map<string>", "map<string,int", "map<,int>", "struct<a:>", "struct<a:int", "struct<a:int,>",
      "struct<a:int b:int>", "struct<:int>", "struct<`unterminated:int>", "struct<``:int>",
      "struct<a:int NOT>", "struct<a:int NOT int>", "struct<a:int COMMENT x>", "struct<a:int COMMENT 'open>",
      "decimal(", "decimal(x)", "decimal(10,)", "decimal(10,2", "foo(1", "int int", "123", "interval day to second"
    )
    bad.foreach(text => assert(LogicalType.parse(text).isEmpty, s"'$text' should not parse"))
  }

  test("parse then catalogString round-trips the forms a contract writes") {
    List("array<int>", "map<string,bigint>", "struct<a:int,b:array<string>>", "array<decimal(10,2)>").foreach { text =>
      assert(parsed(text).catalogString == text)
    }
  }

  // --- name matching --------------------------------------------------------

  test("nameKey folds case only when the session is case-insensitive") {
    assert(LogicalType.nameKey("Id", caseSensitive = false) == "id")
    assert(LogicalType.nameKey("Id", caseSensitive = true) == "Id")
  }

  // --- schema ---------------------------------------------------------------

  test("LogicalField defaults to nullable; LogicalSchema reports emptiness") {
    assert(LogicalField("a", IntegerType).nullable)
    assert(LogicalSchema.empty.isEmpty)
    assert(!LogicalSchema(List(LogicalField("a", IntegerType))).isEmpty)
  }
}
