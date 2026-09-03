// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object InvaractPlugin {
  private var eventLog: List[String] = List()

  def logEvent(msg: String): Unit = {
    eventLog = eventLog :+ s"[${System.currentTimeMillis()}] $msg"
  }

  def getEvents: List[String] = eventLog

  def clearEvents(): Unit = {
    eventLog = List()
  }

  def validate(df: DataFrame): DataFrame = {
    logEvent(s"Validating DataFrame with schema: ${df.schema.fieldNames.mkString(", ")}")

    val requiredColumns = Set("id", "value")
    val missingColumns = requiredColumns -- df.columns.toSet

    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required columns: ${missingColumns.mkString(", ")}")
    }

    logEvent(s"Schema validation passed. Row count: ${df.count()}")
    df
  }

  def addComputedColumn(df: DataFrame): DataFrame = {
    logEvent("Adding computed column: value_squared")

    df.withColumn(
      "value_squared",
      col("value") * col("value")
    )
  }

  /** A second, independent computed column - a comparison feeding a
    * two-way CASE WHEN, rather than another arithmetic expression - so
    * the demo pipeline's own translated IR (see docs/TRANSFORMATION_IR.md)
    * exercises more than one expression category: `addComputedColumn`
    * above is `Arithmetic`; this is `Comparison` + `Conditional`.
    */
  def addValueTier(df: DataFrame): DataFrame = {
    logEvent("Adding computed column: value_tier")

    df.withColumn(
      "value_tier",
      when(col("value") > 50, lit("high")).otherwise(lit("low"))
    )
  }

  def process(df: DataFrame): DataFrame = {
    logEvent(s"Processing started. Input schema: ${df.schema.fieldNames.mkString(", ")}")

    val validated = validate(df)
    val withComputed = addComputedColumn(validated)
    val withTier = addValueTier(withComputed)

    logEvent("Processing complete")
    withTier
  }
}
