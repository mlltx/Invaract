// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.LogicalSchema

import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

/** Lets a spec that builds a real Spark schema (`df.schema`, a hand-built
  * `StructType`) hand it straight to the engine-neutral checkers, which take a
  * `LogicalSchema`. The conversion is `SparkSchemas.toLogicalSchema` — the same
  * one `ContractEnforcementRule` applies at the real Spark boundary — so a spec
  * using this still exercises Spark's types, the mapping and the checker end to
  * end. Specs that test a checker on its own build a `LogicalSchema` directly.
  */
trait SparkSchemaConversions {
  implicit def structTypeToLogicalSchema(schema: StructType): LogicalSchema = SparkSchemas.toLogicalSchema(schema)
}
