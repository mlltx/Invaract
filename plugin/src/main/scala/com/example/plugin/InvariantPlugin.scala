// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object InvariantPlugin {
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

  def process(df: DataFrame): DataFrame = {
    logEvent(s"Processing started. Input schema: ${df.schema.fieldNames.mkString(", ")}")

    val validated = validate(df)
    val withComputed = addComputedColumn(validated)

    logEvent("Processing complete")
    withComputed
  }
}
