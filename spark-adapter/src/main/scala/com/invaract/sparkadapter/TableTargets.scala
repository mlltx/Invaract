// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.LocationMatching
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}

import scala.util.Try

/** The table a statement names, as a contract can name it: where its files are, and its table name
  * (`LocationMatching.tableName`, `spark_catalog/default/orders`). Shared by the statements that change a table without
  * a query to check - `DestructiveDdlSupport` (DROP, TRUNCATE) and `SchemaDdlSupport` (ALTER TABLE) - so they name a
  * table exactly as a write to it is named.
  *
  * Every catalog lookup is inside a `Try`: naming a table must never turn a statement into a crash.
  */
private[sparkadapter] object TableTargets {

  /** @param location where the table's files are (or, for a table that does not exist yet, where Spark would put them);
    *   its name when neither can be found
    * @param names the table's name, in the form a contract declares it
    */
  final case class Target(location: String, names: List[String]) {

    /** Everything a declared output can match the table by. */
    def all: List[String] = (location :: names).distinct

    /** The table as a message names it: its table name, else its location. */
    def display: String = names.headOption.getOrElse(location)
  }

  /** The name Spark gives its session catalog (`CatalogManager.SESSION_CATALOG_NAME`, which is not public). */
  val SessionCatalogName = "spark_catalog"

  private def session: Option[SparkSession] = Try(SparkSession.active).toOption

  /** A table of the session catalog, by the name a V1 command carries: its own location and name when it exists, and
    * when it does not, the location Spark would give it. `None` only when no session can be asked.
    */
  def ofV1(name: TableIdentifier): Option[Target] = session.flatMap { s =>
    Try {
      val catalog = s.sessionState.catalog
      v1Table(name) match {
        case Some(table) => ofCatalogTable(table)
        case None =>
          val qualified = name.copy(database = Some(name.database.getOrElse(catalog.getCurrentDatabase)))
          val names = List(LocationMatching.fromParts(name.catalog.getOrElse(SessionCatalogName) +: qualified.database.toSeq :+ name.table))
          Target(catalog.defaultTablePath(qualified).toString, names)
      }
    }.toOption
  }

  /** The session catalog's record of a table, if it has one. */
  def v1Table(name: TableIdentifier): Option[CatalogTable] =
    session.flatMap(s => Try(s.sessionState.catalog.getTableMetadata(name)).toOption)

  /** A table the session catalog has a record of. */
  def ofCatalogTable(table: CatalogTable): Target = {
    val names = LocationMatching.tableName(CatalogIdentitySupport.fromCatalogTable(table)).toList
    Target(table.storage.locationUri.map(_.toString).getOrElse(names.head), names)
  }

  /** A table a catalyst command resolved: the location the table reports, else (a table of the session catalog) the
    * session catalog's, else its name. */
  def ofResolvedTable(rt: ResolvedTable): Target = {
    val names = namesOf(rt.catalog, rt.identifier)
    val location = SparkPlanAdapter.tableLocationAndFormat(rt.table)._1
      .orElse(sessionCatalogLocation(rt.catalog, rt.identifier))
      .getOrElse(names.head)
    Target(location, names)
  }

  /** A table a statement names, which may not exist: `DROP TABLE IF EXISTS`. A table of the session catalog is found
    * there (a managed table's V2 form reports no location), or given the location Spark would give it; any other
    * catalog's table reports its own location, else is known by its name.
    */
  def ofResolvedIdentifier(ri: ResolvedIdentifier): Target = {
    val names = namesOf(ri.catalog, ri.identifier)
    val location = sessionCatalogLocation(ri.catalog, ri.identifier)
      .orElse(Try(ri.catalog.asInstanceOf[TableCatalog].loadTable(ri.identifier)).toOption.flatMap(SparkPlanAdapter.tableLocationAndFormat(_)._1))
      .getOrElse(names.head)
    Target(location, names)
  }

  private def namesOf(catalog: CatalogPlugin, identifier: Identifier): List[String] =
    LocationMatching.tableName(CatalogIdentitySupport.fromV2(catalog, identifier)).toList

  private def sessionCatalogLocation(catalog: CatalogPlugin, identifier: Identifier): Option[String] =
    if (catalog.name != SessionCatalogName || identifier.namespace.length != 1) None
    else ofV1(TableIdentifier(identifier.name, identifier.namespace.headOption)).map(_.location)
}
