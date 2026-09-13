// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Direct unit coverage for `CatalogIdentitySupport`'s Hive-metastore-URI
  * resolution and technology-gated location lookup. `HiveConnectorSpec`/
  * `StructuralVerifierSpec` already exercise this class indirectly, through
  * a real write/read - but never against the specific boundaries this
  * file's own logic branches on (an explicitly-empty `hive.metastore.uris`,
  * and a non-Hive session catalog). A scoped Stryker run against this file
  * (see CLAUDE.md's Mutation Testing Requirement) found exactly these
  * boundaries genuinely uncovered - not by inspection.
  */
class CatalogIdentitySupportSpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CatalogIdentitySupportSpec")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  // Every test sets exactly the Hadoop-conf keys it needs, rather than
  // relying on a previous test's leftover values - `hiveMetastoreLocation`
  // reads `sessionState.newHadoopConf()`, a fresh copy of
  // `sparkContext.hadoopConfiguration` taken on every call, so a direct
  // mutation of the latter here is visible to the very next call.
  override def afterEach(): Unit = {
    spark.sparkContext.hadoopConfiguration.unset("hive.metastore.uris")
    spark.sparkContext.hadoopConfiguration.unset("javax.jdo.option.ConnectionURL")
  }

  private def tableIn(db: String, name: String): CatalogTable =
    CatalogTable(
      identifier = TableIdentifier(name, Some(db)),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
    )

  test("hiveMetastoreLocation falls back to the embedded connection URL when hive.metastore.uris is set but empty") {
    // A real, observed shape - some deployments set hive.metastore.uris to
    // an explicitly empty string (e.g. a templated config file with an
    // unfilled placeholder) rather than leaving the key unset entirely.
    // Either way "" is not a usable metastore address, so it must be
    // treated exactly like "unset" - fall through to the embedded
    // connection URL - never returned as-is.
    spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "")
    spark.sparkContext.hadoopConfiguration.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:catalogIdentitySupportSpec;create=true")

    val location = CatalogIdentitySupport.hiveMetastoreLocation

    assert(
      location.contains("embedded:jdbc:derby:memory:catalogIdentitySupportSpec;create=true"),
      s"an empty hive.metastore.uris must fall back to the embedded connection URL, got $location"
    )
  }

  test("hiveMetastoreLocation reports the real thrift URI when hive.metastore.uris is genuinely set") {
    spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "thrift://real-metastore.example.com:9083")
    spark.sparkContext.hadoopConfiguration.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:catalogIdentitySupportSpec;create=true")

    val location = CatalogIdentitySupport.hiveMetastoreLocation

    assert(location.contains("thrift://real-metastore.example.com:9083"))
  }

  test("fromCatalogTable resolves the real Hive metastore location on a Hive session catalog") {
    spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "thrift://real-metastore.example.com:9083")

    val identity = CatalogIdentitySupport.fromCatalogTable(tableIn("default", "some_table"))

    assert(identity.technology.contains("hive"))
    assert(identity.location.contains("thrift://real-metastore.example.com:9083"))
    assert(identity.namespace == List("default"))
    assert(identity.table.contains("some_table"))
  }

  // `getClass.getSimpleName` is how `technologyOfCatalogPlugin` recognizes
  // Delta - a real, named class (not an anonymous one, whose
  // `getSimpleName` isn't reliably predictable) called literally
  // `NotDeltaCatalog` gives a real, controllable "definitely not delta"
  // technology without needing a real DSv2 connector dependency.
  private class NotDeltaCatalog(pluginName: String) extends org.apache.spark.sql.connector.catalog.CatalogPlugin {
    override def initialize(name: String, options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit = ()
    override def name(): String = pluginName
  }

  test("fromV2 never resolves a location when the resolved technology isn't delta, even under spark_catalog on a Hive session") {
    // A real table registered exactly where deltaSessionCatalogMetastoreLocation
    // would look, so a bug that skips the technology check would find it
    // and wrongly return a location instead of None.
    spark.sql("CREATE TABLE catalog_identity_probe_not_delta (id INT) USING PARQUET")
    val identifier = org.apache.spark.sql.connector.catalog.Identifier.of(Array("default"), "catalog_identity_probe_not_delta")

    val identity = CatalogIdentitySupport.fromV2(new NotDeltaCatalog("spark_catalog"), identifier)

    assert(identity.location.isEmpty, s"a non-Delta technology must never resolve a Hive metastore location, got ${identity.location}")
  }
}
