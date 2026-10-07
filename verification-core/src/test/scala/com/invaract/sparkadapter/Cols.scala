// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}

import scala.language.implicitConversions

/** A tiny fluent builder for the `LogicalSchema` a checker spec hands the
  * verification core - `Cols().add("id", IntegerType, nullable = false)` -
  * so a spec reads like the Spark-typed ones it replaced without needing
  * Spark. A nested struct is added by passing another `Cols`.
  */
final class Cols private (val fields: List[LogicalField]) {
  def add(name: String, dataType: LogicalType, nullable: Boolean = true): Cols =
    new Cols(fields :+ LogicalField(name, dataType, nullable))

  def add(name: String, nested: Cols): Cols = add(name, LogicalType.StructType(nested.fields))

  def toLogical: LogicalSchema = LogicalSchema(fields)
}

object Cols {
  def apply(): Cols = new Cols(Nil)

  implicit def schemaToLogicalSchema(schema: Cols): LogicalSchema = schema.toLogical
}
