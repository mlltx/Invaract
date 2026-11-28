// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.notification.CatalogInfo

/** A dataset's OpenLineage identity: `namespace` (where it lives) and `name` (what it is there). */
final case class DatasetId(namespace: String, name: String)

/** Names datasets the way the OpenLineage naming conventions do (https://openlineage.io/docs/spec/naming), so a dataset
  * Invaract publishes is the same node in a lineage graph as the one an engine's own OpenLineage integration publishes:
  *
  *   - a local path (`/data/out`, `file:/data/out`, `file:///data/out`): namespace `file`, name the absolute path (a
  *     relative path is resolved against the working directory, the job's own);
  *   - object storage: namespace `s3://bucket` (`s3a`, `s3n` are `s3`), `gs://bucket`, and Azure
  *     `abfss://container@account.dfs.core.windows.net` (`abfs` is `abfss`, `wasb` is `wasbs`); name the key or path,
  *     without a leading `/` (the bucket or container itself is `/`);
  *   - HDFS (`hdfs://host:port/path`) and any other `scheme://authority/path`: namespace `scheme://authority` (the
  *     scheme alone when there is no authority, `hdfs:///path`), name `/path`;
  *   - a trailing `/` is dropped: `s3://bucket/out/` is `s3://bucket/out`;
  *   - a JDBC URL and its table (`jdbc:postgresql://host/db` and `public.orders`): namespace `postgres://host:5432`, name
  *     `db.public.orders`, each database's default port, database properties and Oracle's thin URLs as openlineage-spark
  *     reads them (`postgresql` is `postgres`, as the conventions spell it);
  *   - `bigquery:project.dataset.table` (Beam): namespace `bigquery`, name `project.dataset.table`;
  *   - a Kafka topic (`broker:9092/orders`, format `kafka`): namespace `kafka://broker:9092`, name the topic; a Pub/Sub
  *     topic (`projects/p/topics/t`, format `pubsub`): namespace `pubsub`, name `topic:p:t`.
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

  /** The same, for a dataset that resolved to `catalog`: a bare name (no scheme, no `/`) is that catalog's table, named
    * by where it is stored when the catalog says (as openlineage-spark names a table), else by the catalog. */
  def dataset(location: String, catalog: Option[CatalogInfo], workingDirectory: => String): DatasetId = {
    val trimmed = location.trim
    trimmed match {
      // a catalog table: by where it is stored when that is known, as openlineage-spark names it (a table created by
      // the write itself has only its name as its location), else as the catalog's table
      case bare if isBare(bare) && catalog.flatMap(table).isDefined =>
        catalog.flatMap(_.location).filterNot(isBare).map(dataset(_, None, workingDirectory)).getOrElse(catalog.flatMap(table).get)
      case WindowsDrive() => DatasetId("file", absolute(trimmed, workingDirectory))
      case Scheme(rawScheme, rest) =>
        val scheme = rawScheme.toLowerCase
        if (scheme == "jdbc") jdbc(rest, workingDirectory)
        else if (scheme == "file") DatasetId("file", fileName(rest, workingDirectory))
        else if (rest.startsWith("//")) hierarchical(canonicalScheme(scheme), rest.substring(2))
        else DatasetId(canonicalScheme(scheme), withoutTrailingSlash(withoutCredentials(rest.replace(':', '.'))))
      case path => DatasetId("file", absolute(path, workingDirectory))
    }
  }

  /** The same, for a dataset of `format`: a Kafka topic (`kafka`) or a Pub/Sub topic or subscription (`pubsub`) is
    * located as an adapter writes it, not as a path or URL, and named as the conventions name it. */
  def dataset(location: String, catalog: Option[CatalogInfo], workingDirectory: => String, format: Option[String]): DatasetId =
    format.map(_.toLowerCase) match {
      case Some("kafka")  => kafka(location.trim)
      case Some("pubsub") => pubsub(location.trim)
      case _              => dataset(location, catalog, workingDirectory)
    }

  /** A Kafka topic, located `servers/topic` (`broker:9092/orders`, `b1:9092,b2:9092/orders`, `orders` with no servers
    * stated; `kafka://` before it is allowed): namespace `kafka://` and the first server, or `kafka` with none; name the
    * topic. */
  private def kafka(location: String): DatasetId = {
    // the topic follows the last `/` (servers and a topic hold none, so the first `/` is the same one)
    val unprefixed = location.stripPrefix("kafka://")
    unprefixed.lastIndexOf('/') match {
      case -1 => DatasetId("kafka", unprefixed)
      case cut =>
        val server = withoutCredentials(unprefixed.substring(0, cut).split(',').head.trim)
        DatasetId(if (server.isEmpty) "kafka" else s"kafka://$server", unprefixed.substring(cut + 1))
    }
  }

  private val PubSubPath = "^projects/([^/]+)/(topics|subscriptions)/([^/]+)$".r

  /** A Pub/Sub topic or subscription, located by its path (`projects/p/topics/t`): namespace `pubsub`, name
    * `topic:p:t` (`subscription:p:s`). */
  private def pubsub(location: String): DatasetId = location match {
    case PubSubPath(project, kind, name) => DatasetId("pubsub", s"${kind.stripSuffix("s")}:$project:$name")
    case other                           => DatasetId("pubsub", other)
  }

  /** A JDBC table, `jdbc:<url>/<table>` (an adapter may write the `jdbc:` twice: its own prefix and the URL's). A URL
    * with an authority (`postgresql://host:5432/db`) is named as any `scheme://authority/path` is; an opaque one (`h2:`
    * a file or `mem:` name, `oracle:thin:@host:1521:sid`, `sqlite:` a file) as openlineage-spark names it: namespace the
    * URL, name the table. */
  private def jdbc(afterPrefix: String, workingDirectory: => String): DatasetId = {
    val url = afterPrefix.stripPrefix("jdbc:")
    url match {
      case Scheme(rawScheme, rest) if rawScheme.equalsIgnoreCase("oracle") && oracle(rest).isDefined => oracle(rest).get
      case Scheme(rawScheme, rest) if rest.startsWith("//") => database(canonicalScheme(rawScheme.toLowerCase), rest.substring(2))
      case Scheme(rawScheme, rest) =>
        val scheme = canonicalScheme(rawScheme.toLowerCase)
        rest.lastIndexOf('/') match {
          case -1  => DatasetId(scheme, withoutCredentials(rest))
          case cut => DatasetId(s"$scheme:${opaqueWithoutCredentials(rest.substring(0, cut))}", rest.substring(cut + 1))
        }
      case _ => dataset(url, None, workingDirectory)
    }
  }

  /** The port a database listens on when its URL names none, as openlineage-spark assumes it (a database with no entry,
    * MariaDB or SQL Server, is named without one, as openlineage-spark names it). */
  private val DefaultPorts = Map("postgres" -> 5432, "mysql" -> 3306, "oracle" -> 1521, "db2" -> 6789, "teradata" -> 1025, "trino" -> 443)

  /** A table in a database, `authority[/database][;properties|?properties]/table` after `scheme://` (an adapter adds the
    * table after the URL): namespace `scheme://host:port`, the port defaulted (`DefaultPorts`); name `database.table`,
    * the database the URL's path (Trino's `catalog/schema` kept as it is; another's `/` a `.`, as a Beam location
    * splits a schema from its table) or its `databaseName`/`database` (SQL Server) or `DATABASE` (Teradata) property,
    * and the port Teradata's `DBS_PORT`. Credentials, in the authority or a property, never reach either. */
  private def database(scheme: String, afterSlashes: String): DatasetId = {
    val cut = afterSlashes.lastIndexOf('/')
    val (url, table) = if (cut < 0) (afterSlashes, "") else (afterSlashes.substring(0, cut), afterSlashes.substring(cut + 1))
    val end = url.indexWhere(c => c == '/' || c == ';' || c == '?') match { case -1 => url.length; case i => i }
    val authority = withoutCredentials(url.substring(0, end))
    val rest = url.substring(end).stripPrefix("/")
    val pathEnd = rest.indexWhere(c => c == ';' || c == '?') match { case -1 => rest.length; case i => i }
    // Teradata's path is its properties (`DATABASE=sales,DBS_PORT=1030`)
    val (path, propertyText) = if (rest.substring(0, pathEnd).contains('=')) ("", rest) else (rest.substring(0, pathEnd), rest.substring(pathEnd))
    val properties = propertyText.split("[;?&,]").toList.flatMap { p =>
      p.split("=", 2) match { case Array(k, v) => List(k.trim.toLowerCase(java.util.Locale.ROOT) -> v.trim); case _ => Nil }
    }.toMap
    val db = properties.get("databasename").orElse(properties.get("database"))
      .getOrElse(if (scheme == "trino") path else path.replace('/', '.'))
    val port = properties.get("dbs_port").orElse(DefaultPorts.get(scheme).map(_.toString))
    val host = if (authority.contains(':') || authority.isEmpty) authority else port.fold(authority)(p => s"$authority:$p")
    DatasetId(s"$scheme://$host", if (db.isEmpty) table else s"$db.$table")
  }

  /** An Oracle thin URL after `oracle:` (`thin:[user/password]@host[:port]:sid`, `thin:@[//]host[:port]/service`, an
    * adapter's `/table` after it), named as openlineage-spark names it: namespace `oracle://host:port` (1521 by
    * default), name `sid.table` or `service.table`. `None` for a form it is not (a TNS descriptor). */
  private def oracle(rest: String): Option[DatasetId] = {
    val cut = rest.lastIndexOf('/')
    val at = rest.lastIndexOf('@')
    if (at < 0) None
    else {
      // a `/` before the `@` is the credentials' (`user/password@`), not the table's: then there is no table (`>` and `>=`
      // are the same here: a `/` and an `@` are never at one index)
      val (address0, table) = if (cut > at) (rest.substring(at + 1, cut), rest.substring(cut + 1)) else (rest.substring(at + 1), "")
      val target = address0.stripPrefix("//")
      val (address, database) = target.indexOf('/') match {
        case -1 => target.split(':').toList match {
            case host :: port :: sid :: Nil => (s"$host:$port", sid)
            case _                          => (target, "")
          }
        case slash => (target.substring(0, slash), target.substring(slash + 1))
      }
      if (address.isEmpty || address.startsWith("(")) None
      else {
        val host = if (address.contains(':')) address else s"$address:1521"
        Some(DatasetId(s"oracle://$host", List(database, table).filter(_.nonEmpty).mkString(".")))
      }
    }
  }

  /** An opaque JDBC URL with no credentials in it: neither `user/password@` (Oracle's thin driver) nor a `user=` or
    * `password=` property. */
  private def opaqueWithoutCredentials(url: String): String = {
    // `[driver type:][user/password]@host...`: keep the driver type (`thin:`), when there is one before the `@`. With no
    // `@` (`at` -1) nothing is cut: no colon is before it, and the rest is the whole URL. (`colon < at` and `<=` are the
    // same: the two are different characters, never at one index.)
    val at = url.lastIndexOf('@')
    val colon = url.indexOf(':')
    val hostOnly = (if (colon < at) url.substring(0, colon + 1) else "") + url.substring(at + 1)
    hostOnly.split(';').toList match {
      case head :: properties => (head :: properties.filterNot(p => CredentialProperty.pattern.matcher(p).matches)).mkString(";")
      case Nil                => hostOnly
    }
  }

  private val CredentialProperty = "(?i)\\s*(user|username|password|pwd)\\s*=.*".r

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
    case "abfs"                  => "abfss" // the same container, over TLS or not: one dataset
    case "wasb"                  => "wasbs"
    case other                   => other
  }

  /** `authority/path` after `scheme://`. */
  private def hierarchical(scheme: String, afterSlashes: String): DatasetId = {
    val slash = afterSlashes.indexOf('/')
    val (rawAuthority, rawPath) = if (slash < 0) (afterSlashes, "") else (afterSlashes.substring(0, slash), afterSlashes.substring(slash))
    val authority = withoutCredentials(rawAuthority)
    // a trailing `/` names the same directory (`s3://b/out/` is `s3://b/out`), as openlineage-spark names it
    val path = withoutQuery(rawPath).reverse.dropWhile(_ == '/').reverse
    // no authority (`hdfs:///data`, the cluster's default file system): namespace the scheme alone
    val namespace = if (authority.isEmpty) scheme else s"$scheme://$authority"
    scheme match {
      // object storage and Azure: the key or path without a leading `/`; a bucket or container itself is `/`
      case "s3" | "gs" | "abfss" | "wasbs" => DatasetId(namespace, if (path.isEmpty) "/" else path.stripPrefix("/"))
      case "postgres" | "mysql" | "mariadb" | "sqlserver" | "oracle" | "db2" | "teradata" | "trino" | "redshift" =>
        database(scheme, afterSlashes.reverse.dropWhile(_ == '/').reverse)
      case _ => DatasetId(namespace, if (path.isEmpty) "/" else path)
    }
  }

  private def withoutCredentials(authority: String): String = {
    val at = authority.lastIndexOf('@')
    // `abfss://container@account...` keeps its container: only `user:secret@` (or an empty `@`) is user information.
    if (at < 0) authority
    else {
      val user = authority.substring(0, at)
      if (user.isEmpty || user.contains(':')) authority.substring(at + 1) else authority
    }
  }

  private def withoutTrailingSlash(name: String): String = {
    val trimmed = name.reverse.dropWhile(_ == '/').reverse
    if (trimmed.isEmpty) name else trimmed
  }

  private def withoutQuery(path: String): String = {
    val q = path.indexOf('?')
    val h = path.indexOf('#')
    // `>= 0` and `> 0` are the same here: a path is empty or starts with `/`, so neither is ever at index 0
    val end = List(q, h).filter(_ >= 0).sorted.headOption.getOrElse(path.length)
    path.substring(0, end)
  }

  /** What follows `file:`: `///abs`, `//host/abs`, `/abs` or a relative path. */
  private def fileName(rest: String, workingDirectory: => String): String =
    if (rest.startsWith("//")) absolute("/" + rest.drop(2).dropWhile(_ != '/'), workingDirectory) // the host is not the path
    else absolute(rest, workingDirectory)

  /** `path` made absolute and normal (no `.`, `..` or empty segments), with forward slashes on every platform: the name
    * every other integration publishes for the same file. Plain string work, not `java.nio.file.Paths`, which names
    * `/data/out` `\data\out` on Windows and refuses `/C:/data` (a Windows path as a `file:` URI carries it) outright. A
    * Windows drive is kept as `C:/`. */
  private def absolute(path: String, workingDirectory: => String): String = {
    val slashed = path.replace('\\', '/')
    val full = if (slashed.startsWith("/") || isDrive(slashed)) slashed else workingDirectory.replace('\\', '/') + "/" + slashed
    val unrooted = full.dropWhile(_ == '/')
    val (root, rest) = if (isDrive(unrooted)) (unrooted.substring(0, 3), unrooted.substring(3)) else ("/", unrooted)
    val segments = rest.split('/').foldLeft(List.empty[String]) {
      case (kept, "" | ".") => kept
      case (kept, "..")     => kept.drop(1)
      case (kept, segment)  => segment :: kept
    }
    root + segments.reverse.mkString("/")
  }

  private def isDrive(path: String): Boolean = WindowsDrive.pattern.matcher(path).matches
}
