// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.plugin

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class InvaractPluginTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("InvaractPluginTest")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("validate should pass with required columns") {
    val df = spark.createDataFrame(Seq((1, 10), (2, 20)))
      .toDF("id", "value")

    val result = InvaractPlugin.validate(df)
    assert(result.count() == 2)
  }

  test("validate should fail with missing columns") {
    val df = spark.createDataFrame(Seq((1, 10), (2, 20)))
      .toDF("id", "other")

    assertThrows[IllegalArgumentException] {
      InvaractPlugin.validate(df)
    }
  }

  test("addComputedColumn should add value_squared") {
    val df = spark.createDataFrame(Seq((1, 10), (2, 20)))
      .toDF("id", "value")

    val result = InvaractPlugin.addComputedColumn(df)
    assert(result.columns.contains("value_squared"))

    val rows = result.collect()
    assert(rows(0).getInt(2) == 100) // 10 * 10
    assert(rows(1).getInt(2) == 400) // 20 * 20
  }

  test("addValueTier should classify values above 50 as 'high', and 50 or below as 'low'") {
    val df = spark.createDataFrame(Seq((1, 50), (2, 51)))
      .toDF("id", "value")

    val result = InvaractPlugin.addValueTier(df)
    assert(result.columns.contains("value_tier"))

    val rows = result.collect()
    assert(rows(0).getString(2) == "low")  // 50 is not > 50
    assert(rows(1).getString(2) == "high") // 51 is > 50
  }

  test("process should perform full transformation") {
    val df = spark.createDataFrame(Seq((1, 5), (2, 10)))
      .toDF("id", "value")

    val result = InvaractPlugin.process(df)
    assert(result.columns.contains("value_squared"))
    assert(result.columns.contains("value_tier"))
    assert(result.count() == 2)

    val events = InvaractPlugin.getEvents
    assert(events.nonEmpty)
    assert(events.exists(_.contains("Processing started")))
  }
}
