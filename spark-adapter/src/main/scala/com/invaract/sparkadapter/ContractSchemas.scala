// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Dataset, Field, Schema}

import org.apache.spark.sql.types._

/** Raised by [[ContractSchemas.toStructType]] when a field's contract `type`
  * doesn't carry enough information to derive a Spark `DataType` from alone.
  *
  * `array`/`map`/`decimal` are recognized contract types (see
  * `ContractValidator.KnownTypes`), but the contract model has no field for
  * an array's element type, a map's key/value types, or a decimal's
  * precision/scale — unlike `struct`, which is fully specified via `properties`.
  * Guessing one would risk silently constructing the wrong Spark type (e.g. a
  * `decimal` precision/scale that truncates real data), so this fails closed
  * instead: declare that one field's Spark type yourself, the same way you
  * would before this converter existed.
  */
class UnsupportedFieldTypeException(fieldName: String, fieldType: String)
    extends RuntimeException(
      s"Cannot derive a Spark type for field '$fieldName' of contract type '$fieldType'. " +
        "ContractSchemas.toStructType only derives types the contract model fully specifies on " +
        "its own: string, integer, long, short, byte, double, float, boolean, date, timestamp, " +
        "binary, and struct (via nested 'properties'). 'array'/'map'/'decimal' aren't derivable " +
        "this way — the contract model doesn't carry an element type, key/value types, or " +
        "precision/scale for them. Declare this one field's Spark type yourself for now."
    )

/** Converts a contract-declared `Schema`/`Dataset` into the Spark `StructType`
  * it describes — the forward direction of the mapping `ContractInference.schemaOf`
  * already performs in reverse (a real write's actual Spark schema -> a contract
  * `Schema`, for dry-run mode).
  *
  * Exists so a job can derive the schema it reads with directly from the same
  * contract Invaract's own enforcement rule is already checking that job
  * against, instead of a second, hand-written `StructType`/column list that
  * can silently drift from the contract over time. See docs-site's "Install
  * and Configure Invaract Explicitly in Code" guide's "Derive a Spark schema
  * from the contract" section for a worked example.
  *
  * This is a pure, best-effort structural conversion, not a new verification
  * capability — a job's own compile-time or runtime schema is still whatever
  * that job's code actually builds; `StructuralVerifier` remains the
  * authority on whether the two ended up agreeing.
  */
object ContractSchemas {

  /** Converts a dataset's declared schema. Equivalent to
    * `toStructType(dataset.schema)`.
    */
  def toStructType(dataset: Dataset): StructType = toStructType(dataset.schema)

  def toStructType(schema: Schema): StructType = StructType(schema.fields.map(toStructField))

  private def toStructField(field: Field): StructField =
    StructField(field.name, dataTypeOf(field), nullable = field.nullable)

  /** A field with non-empty `properties` is a struct regardless of its
    * declared `fieldType` text (see `Field.isStruct`'s own doc) — checked
    * before looking at `fieldType` at all, so a mismatched/omitted
    * `type: struct` on such a field never falls through to
    * `UnsupportedFieldTypeException`.
    */
  private def dataTypeOf(field: Field): DataType =
    if (field.isStruct) StructType(field.properties.map(toStructField))
    else
      field.fieldType.toLowerCase match {
        case "string"    => StringType
        case "integer"   => IntegerType
        case "long"      => LongType
        case "short"     => ShortType
        case "byte"      => ByteType
        case "double"    => DoubleType
        case "float"     => FloatType
        case "boolean"   => BooleanType
        case "date"      => DateType
        case "timestamp" => TimestampType
        case "binary"    => BinaryType
        case _           => throw new UnsupportedFieldTypeException(field.name, field.fieldType)
      }
}
