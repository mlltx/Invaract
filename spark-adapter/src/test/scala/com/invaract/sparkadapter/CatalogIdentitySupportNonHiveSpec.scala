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

  // A real, named class (not `org.apache.spark.sql.delta.catalog.DeltaCatalog`
  // itself - no compile-time Delta dependency here) whose `getClass.getSimpleName`
  // is literally "DeltaCatalog", the same reflective match
  // `technologyOfCatalogPlugin` uses - lets `fromV2`'s technology == "delta"
  // branch be exercised without a real Delta session.
  private class DeltaCatalog(pluginName: String) extends org.apache.spark.sql.connector.catalog.CatalogPlugin {
    override def initialize(name: String, options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit = ()
    override def name(): String = pluginName
  }

  test("fromV2 never resolves a location for a Delta catalog under spark_catalog on a non-Hive session") {
    // A leftover/residual hive.metastore.uris value AND a real table at the
    // exact identifier deltaSessionCatalogMetastoreLocation would look up -
    // so a bug that skips the "session is actually Hive" check would find a
    // real table and wrongly return a location instead of None.
    spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "thrift://leftover-config-not-actually-in-use:9083")
    spark.sql("CREATE TABLE catalog_identity_probe_delta_non_hive (id INT) USING PARQUET")
    val identifier = org.apache.spark.sql.connector.catalog.Identifier.of(Array("default"), "catalog_identity_probe_delta_non_hive")

    val identity = CatalogIdentitySupport.fromV2(new DeltaCatalog("spark_catalog"), identifier)

    assert(identity.technology.contains("delta"))
    assert(identity.location.isEmpty, s"a non-Hive session catalog must never report a Hive metastore location for Delta either, got ${identity.location}")
  }
}
