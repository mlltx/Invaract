// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}

import org.apache.spark.sql.types._

/** The one place Spark's schema types become the engine-neutral
  * `com.invaract.contract.LogicalSchema` the verification checkers
  * (`SchemaChecker`, `InputChecker`, `OutputChecker`, `StructuralVerifier`,
  * `ContractInference`) compare against a contract.
  *
  * This is the Spark half of what any engine adapter owes the checkers: its own
  * `toLogicalType`. Nothing here decides a verdict; it only restates a type in
  * the shared vocabulary, so that mapping is the single, reviewable place a
  * Spark type's meaning can be lost or changed.
  *
  * ## No guessing
  *
  * A Spark type with no logical equivalent (an interval, a `NullType`, a UDT, a
  * CHAR/VARCHAR) becomes `LogicalType.OtherType` carrying Spark's own
  * `typeName`/`catalogString`, so it compares equal only to the identical Spark
  * type — a mismatch stays visible instead of being papered over by a lossy
  * mapping. `typeName` and `catalogString` of every mapped type are the ones
  * Spark itself prints (see `SparkSchemasSpec`), which is what keeps every
  * existing violation message and verdict unchanged.
  */
private[sparkadapter] object SparkSchemas {

  def toLogicalSchema(schema: StructType): LogicalSchema =
    LogicalSchema(schema.fields.map(toLogicalField).toList)

  def toLogicalType(dataType: DataType): LogicalType = dataType match {
    case StringType       => LogicalType.StringType
    case BooleanType      => LogicalType.BooleanType
    case ByteType         => LogicalType.ByteType
    case ShortType        => LogicalType.ShortType
    case IntegerType      => LogicalType.IntegerType
    case LongType         => LogicalType.LongType
    case FloatType        => LogicalType.FloatType
    case DoubleType       => LogicalType.DoubleType
    case DateType         => LogicalType.DateType
    case TimestampType    => LogicalType.TimestampType
    case TimestampNTZType => LogicalType.TimestampNtzType
    case BinaryType       => LogicalType.BinaryType
    case d: DecimalType   => LogicalType.DecimalType(d.precision, d.scale)
    case a: ArrayType     => LogicalType.ArrayType(toLogicalType(a.elementType))
    case m: MapType       => LogicalType.MapType(toLogicalType(m.keyType), toLogicalType(m.valueType))
    case s: StructType    => LogicalType.StructType(s.fields.map(toLogicalField).toList)
    case other            => LogicalType.OtherType(other.typeName, other.catalogString)
  }

  private def toLogicalField(field: StructField): LogicalField =
    LogicalField(field.name, toLogicalType(field.dataType), field.nullable)
}
