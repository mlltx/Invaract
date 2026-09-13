// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir.CatalogIdentity

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable => SparkCatalogTable}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}

/** Builds `ir.CatalogIdentity` (the *observed* catalog registration a real
  * write/read resolved to) for every write/read shape `WriteCommandSupport`/
  * `SparkPlanAdapter` recognize — see docs/ADDING_A_SPARK_CONNECTOR.md-style
  * per-shape rigor: every case here was derived from data already reachable
  * at each call site (a `CatalogTable`/`Identifier`+`CatalogPlugin` pair),
  * never guessed.
  *
  * Reads the *active* `SparkSession` via `SparkSession.active` rather than
  * having a session threaded through `WriteCommandSupport`/`SparkPlanAdapter`'s
  * otherwise-pure `LogicalPlan => X` functions - the same technique
  * `WriteCommandSupport.createDataSourceTableAsSelect`'s own
  * `defaultTablePath` fallback already uses for exactly this reason (a real
  * session is always active on the thread `injectCheckRule`/translation run
  * on; see that case's own comment). Every lookup is wrapped in `Try` and
  * degrades to `None`, never throws - a catalog-identity enrichment must
  * never be able to turn a working write/read into a crash.
  */
private[sparkadapter] object CatalogIdentitySupport {

  private def activeSession: Option[SparkSession] = scala.util.Try(SparkSession.active).toOption

  /** Spark's own `spark.sql.catalogImplementation` conf value - literally
    * `"hive"` or `"in-memory"` (Spark's own two built-in session catalog
    * implementations), confirmed via `SQLConf.CATALOG_IMPLEMENTATION`'s own
    * checkValues. This is a *session-level* fact, not something to guess
    * per write shape: any V1 `CatalogTable`-bearing command (`.saveAsTable()`
    * against a plain Parquet/CSV/... table, not just Hive's own reflective
    * commands) is routed through the Hive metastore the moment a session
    * enables it, confirmed empirically in `HiveConnectorSpec`/
    * `docs/connectors/hive.md`'s "External tables" section - `technology`
    * for a V1 write/read is this session-wide setting, never hardcoded.
    */
  private[sparkadapter] def sessionCatalogTechnology: Option[String] =
    activeSession.flatMap(s => scala.util.Try(s.conf.get("spark.sql.catalogImplementation")).toOption)

  /** The Hive metastore's own network address - `hive.metastore.uris`
    * (`thrift://host:port[,thrift://host2:port2]`), read from the active
    * session's Hadoop configuration, the durable "which Hive" answer a
    * `catalogName` (a local Spark-session alias) can't give. Empty/unset
    * (a real, common case - an embedded/local metastore, e.g. this
    * module's own `HiveConnectorSpec` test fixture) falls back to a
    * clearly-labeled `"embedded:<connection-url>"` derived from
    * `javax.jdo.option.ConnectionURL`, itself only when set - never
    * fabricated when neither is available.
    */
  private[sparkadapter] def hiveMetastoreLocation: Option[String] =
    activeSession.flatMap { session =>
      scala.util.Try {
        val hadoopConf = session.sessionState.newHadoopConf()
        Option(hadoopConf.get("hive.metastore.uris")).filter(_.nonEmpty)
          .orElse(
            Option(hadoopConf.get("javax.jdo.option.ConnectionURL")).filter(_.nonEmpty).map(url => s"embedded:$url")
          )
      }.toOption.flatten
    }

  /** For any V1 write/read shape with a real `CatalogTable` - always
    * present for Hive's own reflective commands
    * (`CreateHiveTableAsSelectCommand`/`InsertIntoHiveTable`/
    * `HiveTableRelation`), and present when `Some` for
    * `InsertIntoHadoopFsRelationCommand.catalogTable`/`LogicalRelation.catalogTable`/
    * `WriteToStream.catalogTable` (a bare path write/read has none - see
    * each call site's own "confirmed genuinely unregistered" case, which
    * never calls this at all).
    *
    * `catalogName`/`namespace`/`table` come directly from
    * `TableIdentifier`'s own three parts (`catalog`/`database`/`table` -
    * confirmed via `javap` that Spark 3.5.7's `TableIdentifier` carries all
    * three, not just `table`/`database`) - `catalog` defaults to Spark's
    * own well-known session-catalog name when unset, the same
    * `CatalogManager.SESSION_CATALOG_NAME` constant every V1 table
    * implicitly belongs to.
    */
  private[sparkadapter] def fromCatalogTable(table: SparkCatalogTable): CatalogIdentity = {
    val technology = sessionCatalogTechnology
    val location = if (technology.contains("hive")) hiveMetastoreLocation else None
    CatalogIdentity(
      technology = technology,
      catalogName = Some(table.identifier.catalog.getOrElse("spark_catalog")),
      location = location,
      namespace = table.identifier.database.toList,
      table = Some(table.identifier.table)
    )
  }

  /** For any DSv2 write/read shape with a resolved `CatalogPlugin`+
    * `Identifier` pair - the same pair `WriteCommandSupport`'s
    * `catalogTableRefOf`/`namedRelationLocationAndFormat` already resolve
    * for location purposes.
    *
    * `technology` is matched by the catalog plugin's own simple class name
    * - the same reflection-friendly convention `streamSinkFormatOf`/
    * `jdbcLocationOf`/`SparkAdapterListener`'s `icebergSnapshotIdOfTable`
    * already use, chosen over a hard `isInstanceOf` check for the same
    * reason: this module has no compile-time dependency on Delta/Iceberg/
    * JDBC's own catalog classes. An unrecognized catalog class falls back
    * to its own simple name, not a blank/guessed label - an honest "this
    * is some catalog implementation Invaract doesn't have a friendly name
    * for yet," not silence.
    *
    * `location` is `None` for every DSv2 catalog except one confirmed
    * exception - see `deltaSessionCatalogMetastoreLocation`'s own doc.
    * No other reflective accessor in this module can read a DSv2 catalog's
    * own network endpoint (Iceberg's `SparkCatalog`, for one, doesn't
    * expose its underlying `org.apache.iceberg.catalog.Catalog`'s
    * `properties()`/URI through any public method this module could call
    * without a compile-time Iceberg dependency). A real, disclosed gap -
    * see docs/connectors/hive.md's catalog-registration coverage ledger -
    * not something to fabricate a value for.
    */
  private[sparkadapter] def fromV2(catalog: CatalogPlugin, identifier: Identifier): CatalogIdentity = {
    val technology = technologyOfCatalogPlugin(catalog)
    CatalogIdentity(
      technology = Some(technology),
      catalogName = Some(catalog.name),
      location = deltaSessionCatalogMetastoreLocation(catalog, technology, identifier),
      namespace = identifier.namespace.toList,
      table = Some(identifier.name)
    )
  }

  /** The one confirmed exception to "a DSv2 catalog never reports a
    * location": Delta's own `DeltaCatalog`, when installed as
    * `spark.sql.catalog.spark_catalog` - the way essentially every real
    * deployment configures it (see docs/connectors/delta.md's own worked
    * example) - doesn't run a separate metadata service the way Iceberg's
    * catalog implementations do. It registers a Delta table as an
    * ordinary `CatalogTable` (`provider = "delta"`) in whichever catalog
    * `spark.sql.catalogImplementation` names, confirmed empirically, not
    * assumed: `HiveConnectorSpec`'s own "a Delta EXTERNAL table registered
    * in the Hive metastore" test independently reads the very same table
    * back via `spark.sessionState.catalog.getTableMetadata` and gets a
    * real `CatalogTableType.EXTERNAL`/`provider.contains("delta")` result.
    * That means a Delta table really can be registered through "the wrong
    * Hive metastore" the exact same way a Parquet one can - so once the
    * table already exists there, its metastore location is just as
    * resolvable and just as worth checking.
    *
    * Two conditions keep this narrow and honest rather than a guess:
    *
    *   - `catalog.name == "spark_catalog"` - the literal name Spark gives
    *     the session/default catalog. `sessionState.catalog` (a V1
    *     `SessionCatalog`) only ever tracks *that one* catalog's own
    *     tables; a second, independently-named Delta catalog instance
    *     (`spark.sql.catalog.other = DeltaCatalog`, unusual but possible)
    *     has no guaranteed relationship to it, so this only fires for the
    *     one case actually proven safe.
    *   - the table must already exist in that catalog. A brand-new
    *     `ReplaceTableAsSelect`/`CreateTableAsSelect` (the first write
    *     that creates the table) is verified *before* the table exists,
    *     so `getTableMetadata` genuinely can't find it yet - the same
    *     "physical location untrusted before commit" limitation this
    *     module's own `StagedTable` handling already documents elsewhere.
    *     `location` correctly stays `None` for that first write, and
    *     resolves once the table exists for every write after.
    *
    * Iceberg is deliberately NOT given the same treatment: even its "Hive"
    * catalog flavor talks to the metastore through Iceberg's own thrift
    * client, entirely bypassing `spark.sessionState.catalog` - so the same
    * lookup would either find nothing or, worse, find an unrelated table,
    * and neither has been confirmed empirically the way Delta's has.
    */
  private def deltaSessionCatalogMetastoreLocation(catalog: CatalogPlugin, technology: String, identifier: Identifier): Option[String] =
    if (technology != "delta" || catalog.name != "spark_catalog" || !sessionCatalogTechnology.contains("hive")) None
    else activeSession.flatMap { session =>
      val tableIdentifier = new org.apache.spark.sql.catalyst.TableIdentifier(identifier.name, identifier.namespace.lastOption)
      scala.util.Try(session.sessionState.catalog.getTableMetadata(tableIdentifier)).toOption.flatMap(_ => hiveMetastoreLocation)
    }

  /** Same as `fromV2`, for the read-side shapes (`DataSourceV2Relation`/
    * `StreamingRelationV2`) whose `catalog`/`identifier` are each
    * independently `Option`-valued on Spark's own class, rather than
    * already paired the way `WriteCommandSupport.catalogTableRefOf`
    * resolves them for the write side.
    */
  private[sparkadapter] def fromV2Option(catalog: Option[CatalogPlugin], identifier: Option[Identifier]): Option[CatalogIdentity] =
    (catalog, identifier) match {
      case (Some(c), Some(i)) => Some(fromV2(c, i))
      case _                  => None
    }

  private def technologyOfCatalogPlugin(catalog: CatalogPlugin): String =
    catalog.getClass.getSimpleName match {
      case "DeltaCatalog" => "delta"
      // Iceberg's two catalog entrypoints: SparkCatalog (a pure Iceberg
      // catalog) and SparkSessionCatalog (wraps the session catalog,
      // delegating non-Iceberg tables to it) - both are genuinely Iceberg
      // when reached via this path, confirmed via the add-spark-connector
      // Iceberg onboarding pass.
      case "SparkCatalog" | "SparkSessionCatalog" => "iceberg"
      case "JDBCTableCatalog" => "jdbc"
      case other => other
    }
}
