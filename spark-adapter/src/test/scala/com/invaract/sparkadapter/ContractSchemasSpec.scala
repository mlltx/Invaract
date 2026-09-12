// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Dataset, Field, Schema}

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/** Covers `ContractSchemas.toStructType` — the forward direction of the
  * mapping `ContractInferenceSpec` covers in reverse (a real Spark schema ->
  * contract `Schema`). Pure structural conversion, no Spark plan involved,
  * so unlike most of this module's specs there's no need for a real
  * `SparkSession` (ADR-005's preference is about translating a real Catalyst
  * plan, which doesn't apply here).
  */
class ContractSchemasSpec extends AnyFunSuite {

  private def field(
    name: String,
    fieldType: String,
    required: Boolean = false,
    nullable: Boolean = true,
    properties: List[Field] = Nil
  ): Field = Field(name, fieldType, required, nullable, properties)

  test("maps every fully-specified scalar type to its Spark DataType") {
    val cases = List(
      "string" -> StringType,
      "integer" -> IntegerType,
      "long" -> LongType,
      "short" -> ShortType,
      "byte" -> ByteType,
      "double" -> DoubleType,
      "float" -> FloatType,
      "boolean" -> BooleanType,
      "date" -> DateType,
      "timestamp" -> TimestampType,
      "binary" -> BinaryType
    )

    cases.foreach { case (contractType, sparkType) =>
      val schema = Schema(List(field("f", contractType)))
      val result = ContractSchemas.toStructType(schema)
      assert(result == StructType(Seq(StructField("f", sparkType, nullable = true))), s"for type '$contractType'")
    }
  }

  test("matches field type case-insensitively") {
    val schema = Schema(List(field("f", "STRING")))
    assert(ContractSchemas.toStructType(schema) == StructType(Seq(StructField("f", StringType, nullable = true))))
  }

  test("preserves nullable = true") {
    val schema = Schema(List(field("f", "string", nullable = true)))
    assert(ContractSchemas.toStructType(schema).fields.head.nullable)
  }

  test("preserves nullable = false") {
    val schema = Schema(List(field("f", "string", required = true, nullable = false)))
    assert(!ContractSchemas.toStructType(schema).fields.head.nullable)
  }

  test("converts a struct field's nested properties recursively") {
    val schema = Schema(
      List(
        field(
          "address",
          "struct",
          nullable = true,
          properties = List(
            field("street", "string", nullable = false),
            field("zip", "string", nullable = true)
          )
        )
      )
    )

    val expected = StructType(
      Seq(
        StructField(
          "address",
          StructType(
            Seq(
              StructField("street", StringType, nullable = false),
              StructField("zip", StringType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    assert(ContractSchemas.toStructType(schema) == expected)
  }

  test("treats non-empty properties as a struct even when fieldType says otherwise") {
    // Field.isStruct is defined purely by properties.nonEmpty (see Field's own
    // doc) - a mismatched fieldType text must never cause this to fall through
    // to the scalar-type lookup (and its UnsupportedFieldTypeException).
    val schema = Schema(List(field("weird", "string", properties = List(field("inner", "string")))))

    val result = ContractSchemas.toStructType(schema)
    assert(result.fields.head.dataType.isInstanceOf[StructType])
    assert(result.fields.head.dataType == StructType(Seq(StructField("inner", StringType, nullable = true))))
  }

  test("an empty (non-struct) properties list does not trigger struct handling") {
    val schema = Schema(List(field("f", "string", properties = Nil)))
    assert(ContractSchemas.toStructType(schema) == StructType(Seq(StructField("f", StringType, nullable = true))))
  }

  test("preserves field order and count across multiple fields") {
    val schema = Schema(List(field("a", "string"), field("b", "integer"), field("c", "boolean")))
    val result = ContractSchemas.toStructType(schema)
    assert(result.fieldNames.toList == List("a", "b", "c"))
    assert(result.fields.length == 3)
  }

  test("an empty schema converts to an empty StructType") {
    assert(ContractSchemas.toStructType(Schema(Nil)) == StructType(Nil))
  }

  List("array", "map", "decimal").foreach { unsupportedType =>
    test(s"'$unsupportedType' throws UnsupportedFieldTypeException: not enough information in the contract model") {
      val schema = Schema(List(field("f", unsupportedType)))
      assertThrows[UnsupportedFieldTypeException] {
        ContractSchemas.toStructType(schema)
      }
    }
  }

  test("an unrecognized field type throws UnsupportedFieldTypeException") {
    val schema = Schema(List(field("f", "totally_made_up_type")))
    assertThrows[UnsupportedFieldTypeException] {
      ContractSchemas.toStructType(schema)
    }
  }

  test("an unsupported type nested inside a struct still throws") {
    val schema = Schema(List(field("outer", "struct", properties = List(field("bad", "map")))))
    assertThrows[UnsupportedFieldTypeException] {
      ContractSchemas.toStructType(schema)
    }
  }

  test("Dataset overload delegates to the dataset's schema exactly") {
    val schema = Schema(List(field("id", "string", required = true, nullable = false)))
    val dataset = Dataset(name = "orders", location = "raw.orders", format = Some("table"), schema = schema)

    assert(ContractSchemas.toStructType(dataset) == ContractSchemas.toStructType(schema))
  }
}
