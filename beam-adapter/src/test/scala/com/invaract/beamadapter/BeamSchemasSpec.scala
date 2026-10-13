// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import org.apache.avro.Schema
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.schemas.{Schema => BeamSchema}
import org.apache.beam.sdk.schemas.Schema.FieldType
import org.scalatest.funsuite.AnyFunSuite

class BeamSchemasSpec extends AnyFunSuite {
  import LogicalType._

  private def avro(fields: String): Option[LogicalSchema] =
    BeamSchemas.fromAvroJson(s"""{"type":"record","name":"R","fields":[$fields]}""")

  private def field(name: String, avroType: String) = s"""{"name":"$name","type":$avroType}"""

  private def typeOf(avroType: String): LogicalType = avro(field("c", avroType)).get.fields.head.dataType

  // --- Avro ---

  test("Avro primitives map to the neutral types, and a plain type is not nullable") {
    val schema = avro(
      List("boolean" -> "boolean", "i" -> "int", "l" -> "long", "f" -> "float", "d" -> "double", "b" -> "bytes", "s" -> "string")
        .map { case (n, t) => field(n, s""""$t"""") }
        .mkString(",")
    ).get
    assert(schema.fields.map(f => f.name -> f.dataType) == List(
      "boolean" -> BooleanType, "i" -> IntegerType, "l" -> LongType, "f" -> FloatType, "d" -> DoubleType, "b" -> BinaryType, "s" -> StringType
    ))
    assert(schema.fields.forall(!_.nullable))
  }

  test("a union with null is the other type, nullable; a union of several types has no neutral type") {
    val schema = avro(field("a", """["null","long"]""") + "," + field("b", """["long","null"]""") + "," + field("c", """["string","long"]""") + "," + field("d", """["null","string","long"]""")).get
    assert(schema.fields.map(f => f.dataType -> f.nullable) == List(
      LongType -> true, LongType -> true, OtherType("union") -> false, OtherType("union") -> true
    ))
  }

  test("nested Avro types: records, arrays, maps, enums and fixed") {
    assert(typeOf("""{"type":"array","items":"string"}""") == ArrayType(StringType))
    assert(typeOf("""{"type":"map","values":"long"}""") == MapType(StringType, LongType))
    assert(typeOf("""{"type":"enum","name":"E","symbols":["A","B"]}""") == StringType)
    assert(typeOf("""{"type":"fixed","name":"F","size":4}""") == BinaryType)
    assert(typeOf("""{"type":"record","name":"In","fields":[{"name":"x","type":["null","int"]}]}""") ==
      StructType(List(LogicalField("x", IntegerType, nullable = true))))
    assert(typeOf("""{"type":"array","items":{"type":"array","items":"long"}}""") == ArrayType(ArrayType(LongType)))
  }

  test("a nested or unrecognised Avro type is not nullable unless it is a union with null") {
    def nullable(avroType: String) = avro(field("c", avroType)).get.fields.head.nullable
    assert(!nullable("""{"type":"array","items":"string"}"""))
    assert(!nullable("""{"type":"map","values":"string"}"""))
    assert(!nullable("""{"type":"record","name":"In","fields":[]}"""))
    assert(!nullable("""7"""))
    assert(nullable("""["null",{"type":"array","items":"string"}]"""))
  }

  test("Avro logical types") {
    assert(typeOf("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}""") == DecimalType(10, 2))
    assert(typeOf("""{"type":"bytes","logicalType":"decimal"}""") == DecimalType(38, 0))
    assert(typeOf("""{"type":"int","logicalType":"date"}""") == DateType)
    assert(typeOf("""{"type":"int","logicalType":"time-millis"}""") == TimeType)
    assert(typeOf("""{"type":"long","logicalType":"time-micros"}""") == TimeType)
    assert(typeOf("""{"type":"long","logicalType":"timestamp-millis"}""") == TimestampType)
    assert(typeOf("""{"type":"long","logicalType":"timestamp-micros"}""") == TimestampType)
    assert(typeOf("""{"type":"long","logicalType":"local-timestamp-millis"}""") == TimestampNtzType)
    assert(typeOf("""{"type":"long","logicalType":"local-timestamp-micros"}""") == TimestampNtzType)
    assert(typeOf("""{"type":"string","logicalType":"uuid"}""") == StringType)
    assert(typeOf("""{"type":"long"}""") == LongType)
  }

  test("a reference to a named type, a bare null and an unrecognised shape keep a type of their own, never a guess") {
    assert(typeOf(""""Address"""") == OtherType("Address"))
    assert(avro(field("n", """"null"""")).get.fields.head == LogicalField("n", OtherType("null"), nullable = true))
    assert(typeOf("""{"type":"mystery"}""") == OtherType("mystery"))
    assert(typeOf("""{"nothing":1}""") == OtherType("unknown"))
    assert(typeOf("""7""") == OtherType("unknown"))
  }

  test("only a record schema yields columns; text that is not JSON yields none") {
    assert(BeamSchemas.fromAvroJson(""""string"""").isEmpty)
    assert(BeamSchemas.fromAvroJson("""{"type":"array","items":"long"}""").isEmpty)
    assert(BeamSchemas.fromAvroJson("{ not json").isEmpty)
    assert(BeamSchemas.fromAvroJson("""{"type":"record","name":"R"}""").isEmpty, "a record with no fields entry is not a schema")
    assert(avro("").get.fields.isEmpty)
  }

  // --- Beam rows ---

  test("Beam row field types map to the neutral types and carry their nullability") {
    val schema = BeamSchema.builder()
      .addByteField("byte").addInt16Field("short").addInt32Field("int").addInt64Field("long")
      .addFloatField("float").addDoubleField("double").addStringField("string").addBooleanField("bool").addByteArrayField("bytes")
      .addDateTimeField("when").addNullableField("maybe", FieldType.INT64)
      .build()
    val mapped = BeamSchemas.fromBeam(schema)
    assert(mapped.fields.map(f => f.name -> f.dataType) == List(
      "byte" -> ByteType, "short" -> ShortType, "int" -> IntegerType, "long" -> LongType, "float" -> FloatType, "double" -> DoubleType,
      "string" -> StringType, "bool" -> BooleanType, "bytes" -> BinaryType, "when" -> TimestampType, "maybe" -> LongType
    ))
    assert(mapped.fields.map(_.nullable) == List.fill(10)(false) :+ true)
  }

  test("Beam arrays, iterables, maps and rows are mapped by structure; a decimal has no neutral precision") {
    val inner = BeamSchema.builder().addInt64Field("x").build()
    val schema = BeamSchema.builder()
      .addArrayField("a", FieldType.STRING)
      .addIterableField("it", FieldType.INT32)
      .addMapField("m", FieldType.STRING, FieldType.INT64)
      .addRowField("r", inner)
      .addDecimalField("dec")
      .build()
    assert(BeamSchemas.fromBeam(schema).fields.map(_.dataType) == List(
      ArrayType(StringType),
      ArrayType(IntegerType),
      MapType(StringType, LongType),
      StructType(List(LogicalField("x", LongType, nullable = false))),
      OtherType("decimal")
    ))
  }

  test("Beam logical types are told apart by identifier and an unknown one keeps it") {
    def logical(identifier: String): LogicalType = {
      val lt = new BeamSchema.LogicalType[String, String] {
        def getIdentifier: String = identifier
        def getArgumentType: FieldType = FieldType.STRING
        def getArgument: String = ""
        def getBaseType: FieldType = FieldType.STRING
        def toBaseType(input: String): String = input
        def toInputType(base: String): String = base
      }
      BeamSchemas.fromBeam(BeamSchema.builder().addLogicalTypeField("c", lt).build()).fields.head.dataType
    }
    assert(logical("beam:logical_type:datetime:v1") == TimestampNtzType)
    assert(logical("beam:logical_type:timestamp:v1") == TimestampType)
    assert(logical("beam:logical_type:date:v1") == DateType)
    assert(logical("beam:logical_type:time:v1") == TimeType)
    assert(logical("beam:logical_type:fixed_bytes:v1") == BinaryType)
    assert(logical("my:own:type") == OtherType("my:own:type"))
  }

  // --- where a collection's columns come from ---

  private def collection(coder: Option[org.apache.beam.sdk.coders.Coder[_]], row: Option[BeamSchema] = None) =
    BeamCollection("c", bounded = true, coder, row)

  test("a collection's columns come from its Beam row schema first, then from the Avro schema its coder carries") {
    val avroSchema = new Schema.Parser().parse("""{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}""")
    val fromAvro = BeamSchemas.of(collection(Some(AvroGenericCoder.of(avroSchema))))
    assert(fromAvro.contains(LogicalSchema(List(LogicalField("a", LongType, nullable = false)))))
    val row = BeamSchema.builder().addStringField("b").build()
    val preferred = BeamSchemas.of(collection(Some(AvroGenericCoder.of(avroSchema)), Some(row)))
    assert(preferred.map(_.fields.map(_.name)).contains(List("b")))
  }

  test("a collection whose coder carries no schema, or that has no coder, exposes no columns") {
    assert(BeamSchemas.of(collection(Some(StringUtf8Coder.of()))).isEmpty)
    assert(BeamSchemas.of(collection(None)).isEmpty)
  }
}
