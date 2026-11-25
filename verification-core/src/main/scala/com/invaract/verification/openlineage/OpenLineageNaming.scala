// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.notification.CatalogInfo

import java.nio.file.Paths

/** A dataset's OpenLineage identity: `namespace` (where it lives) and `name` (what it is there). */
final case class DatasetId(namespace: String, name: String)

/** Names datasets the way the OpenLineage naming conventions do (https://openlineage.io/docs/spec/naming), so a dataset
  * Invaract publishes is the same node in a lineage graph as the one an engine's own OpenLineage integration publishes:
  *
  *   - a local path (`/data/out`, `file:/data/out`, `file:///data/out`): namespace `file`, name the absolute path (a
  *     relative path is resolved against the working directory, the job's own);
  *   - object storage: namespace `s3://bucket` (`s3a`, `s3n` are `s3`), `gs://bucket`; name the key, without a leading `/`;
  *   - Azure (`abfss://container@account.dfs.core.windows.net/path`), HDFS (`hdfs://host:port/path`) and any other
  *     `scheme://authority/path`: namespace `scheme://authority`, name `/path`;
  *   - a JDBC URL (`jdbc:postgresql://host:5432/db`): the URL it wraps, `postgresql` named `postgres` as the conventions do;
  *   - `bigquery:project.dataset.table` (Beam): namespace `bigquery`, name `project.dataset.table`.
  *
  * Credentials in a location (`s3://key:secret@bucket`, `postgresql://user:pass@host`) never reach a name. A location
  * with no scheme and no `/` (`orders`, `db.orders`) is the catalog table it resolved to when it resolved to one, and
  * otherwise a path relative to the working directory, as an engine would read it.
  */
object OpenLineageNaming {

  private val Scheme = "^([A-Za-z][A-Za-z0-9+.-]*):(.*)$".r
  /** A Windows drive (`C:\data`, `C:/data`): a path, not a scheme. */
  private val WindowsDrive = "^[A-Za-z]:[\\\\/].*$".r

  /** The dataset at `location`, resolving a relative path against `workingDirectory`. */
  def dataset(location: String): DatasetId = dataset(location, None, System.getProperty("user.dir"))

  /** The same, for a dataset that resolved to `catalog`: a bare name (no scheme, no `/`) is that catalog's table. */
  def dataset(location: String, catalog: Option[CatalogInfo], workingDirectory: => String): DatasetId = {
    val trimmed = location.trim
    trimmed match {
      case bare if isBare(bare) && catalog.flatMap(table).isDefined => catalog.flatMap(table).get
      case WindowsDrive() => DatasetId("file", trimmed.replace('\\', '/'))
      case Scheme(rawScheme, rest) =>
        val scheme = rawScheme.toLowerCase
        if (scheme == "jdbc") dataset(rest, None, workingDirectory)
        else if (scheme == "file") DatasetId("file", fileName(rest, workingDirectory))
        else if (rest.startsWith("//")) hierarchical(canonicalScheme(scheme), rest.substring(2))
        else DatasetId(canonicalScheme(scheme), withoutCredentials(rest.replace(':', '.')))
      case path => DatasetId("file", absolute(path, workingDirectory))
    }
  }

  private def isBare(location: String): Boolean = !location.contains('/') && !location.contains('\\')

  /** The catalog table a dataset resolved to, for the `symlinks` facet: namespace the catalog (its location when it has
    * one, else `<technology>://<catalog name>`), name `<namespace parts>.<table>`. `None` when there is no table. */
  def table(catalog: CatalogInfo): Option[DatasetId] =
    catalog.table.map { table =>
      val where = catalog.location.map(l => dataset(l, None, System.getProperty("user.dir")).namespace).orElse {
        val technology = catalog.technology.getOrElse("catalog")
        Some(catalog.catalogName.fold(technology)(c => s"$technology://$c"))
      }.get
      DatasetId(where, (catalog.namespace :+ table).mkString("."))
    }

  private def canonicalScheme(scheme: String): String = scheme match {
    case "s3a" | "s3n"           => "s3"
    case "postgresql"            => "postgres"
    case "gcs"                   => "gs"
    case other                   => other
  }

  /** `authority/path` after `scheme://`. */
  private def hierarchical(scheme: String, afterSlashes: String): DatasetId = {
    val slash = afterSlashes.indexOf('/')
    val (rawAuthority, rawPath) = if (slash < 0) (afterSlashes, "") else (afterSlashes.substring(0, slash), afterSlashes.substring(slash))
    val authority = withoutCredentials(rawAuthority)
    val path = withoutQuery(rawPath)
    scheme match {
      case "s3" | "gs" => DatasetId(s"$scheme://$authority", path.stripPrefix("/"))
      case "postgres" | "mysql" | "sqlserver" | "oracle" =>
        DatasetId(s"$scheme://$authority", path.stripPrefix("/").replace('/', '.'))
      case _ => DatasetId(s"$scheme://$authority", if (path.isEmpty) "/" else path)
    }
  }

  private def withoutCredentials(authority: String): String = {
    val at = authority.lastIndexOf('@')
    // `abfss://container@account...` keeps its container: only `user:secret@` is a credential.
    if (at < 0) authority
    else {
      val user = authority.substring(0, at)
      if (user.contains(':')) authority.substring(at + 1) else authority
    }
  }

  private def withoutQuery(path: String): String = {
    val q = path.indexOf('?')
    val h = path.indexOf('#')
    val end = List(q, h).filter(_ >= 0).sorted.headOption.getOrElse(path.length)
    path.substring(0, end)
  }

  /** What follows `file:`: `///abs`, `//host/abs`, `/abs` or a relative path. */
  private def fileName(rest: String, workingDirectory: => String): String =
    if (rest.startsWith("///")) rest.substring(2)
    else if (rest.startsWith("//")) {
      val slash = rest.indexOf('/', 2)
      if (slash < 0) "/" else rest.substring(slash)
    } else absolute(rest, workingDirectory)

  private def absolute(path: String, workingDirectory: => String): String =
    if (path.startsWith("/")) Paths.get(path).normalize.toString
    else Paths.get(workingDirectory).resolve(path).normalize.toString.replace('\\', '/')
}
