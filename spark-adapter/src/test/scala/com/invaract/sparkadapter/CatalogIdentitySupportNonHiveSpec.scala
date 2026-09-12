// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Companion to `CatalogIdentitySupportSpec`, in its own suite specifically
  * because `spark.sql.catalogImplementation` is a *static* Spark config -
  * confirmed the hard way (`AnalysisException: Cannot modify the value of a
  * static config`) - so a non-Hive session catalog needs its own,
  * separately-built `SparkSession` rather than reconfiguring an
  * already-running Hive-enabled one.
  */
class CatalogIdentitySupportNonHiveSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CatalogIdentitySupportNonHiveSpec")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  test("fromCatalogTable never resolves a Hive metastore location on a non-Hive session catalog") {
    // A leftover/residual hive.metastore.uris value (e.g. from Hadoop
    // site-wide config a non-Hive job still inherits) must never leak into
    // a non-Hive session's reported catalog identity - `location` is only
    // ever meaningful once `technology` is actually "hive".
    spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "thrift://leftover-config-not-actually-in-use:9083")

    val table = CatalogTable(
      identifier = TableIdentifier("some_table", Some("default")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
    )
    val identity = CatalogIdentitySupport.fromCatalogTable(table)

    assert(identity.technology.contains("in-memory"))
    assert(identity.location.isEmpty, s"a non-Hive session catalog must never report a Hive metastore location, got ${identity.location}")
  }
}
