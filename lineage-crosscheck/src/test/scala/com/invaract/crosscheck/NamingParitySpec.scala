// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import com.invaract.verification.openlineage.{DatasetId, OpenLineageNaming}
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils
import org.scalatest.funsuite.AnyFunSuite

/** Invaract's dataset names beside openlineage-spark's own naming code (`FilesystemDatasetUtils`, `JdbcDatasetUtils`), for
  * the locations no corpus job can reach: object stores, HDFS, DBFS and the JDBC databases a job connects to. A dataset
  * both publish is one node in a lineage graph only when they name it the same, so every location here must be named
  * the same, except where Invaract is deliberately stricter (each listed in `Deliberate`, with why). */
class NamingParitySpec extends AnyFunSuite {

  private val locations = List(
    "s3://bucket/path/to/data", "s3a://bucket/path/to/data", "s3n://bucket/path", "s3://bucket", "s3://bucket/path/",
    "s3://bucket/a//b", "s3://Bucket/Path", "gs://bucket/path", "gs://bucket/dir/file.parquet", "gs://bucket/path/", "gs://bucket",
    "abfss://container@account.dfs.core.windows.net/path", "abfs://container@account.dfs.core.windows.net/",
    "abfs://container@account.dfs.core.windows.net/a/b/", "abfss://container@account.dfs.core.windows.net",
    "abfss://container@account.dfs.core.windows.net/path?x=1", "wasbs://container@account.blob.core.windows.net/path",
    "wasb://container@account.blob.core.windows.net/path",
    "hdfs://namenode:8020/data/t", "hdfs://namenode/data/t", "hdfs:///data/t", "hdfs://nn/data/t/date=2024-01-01",
    "hdfs://nn:8020/data/t/", "hdfs://nn:8020", "hdfs://nn/a/../b", "viewfs://cluster/data/t",
    "dbfs:/mnt/data/t", "dbfs:/mnt/data/t/", "dbfs:/Volumes/cat/sch/vol/f", "oss://bucket/path", "cos://bucket-app/path",
    "r2://bucket/p", "file:/tmp/x", "file:/tmp/x/", "file:///tmp/x", "/tmp/x"
  )

  /** JDBC tables: the connection URL and the table, as a job names them. */
  private val tables = List(
    "jdbc:postgresql://db.host:5432/sales" -> "public.orders", "jdbc:postgresql://db.host/sales" -> "orders",
    "jdbc:mysql://db.host:3306/sales" -> "orders", "jdbc:sqlserver://db.host:1433;databaseName=sales" -> "dbo.orders",
    "jdbc:oracle:thin:@db.host:1521:orcl" -> "SALES.ORDERS", "jdbc:db2://db.host:50000/sales" -> "orders",
    "jdbc:h2:mem:bank" -> "ACCOUNTS", "jdbc:teradata://db.host/DATABASE=sales" -> "orders", "jdbc:trino://db.host:8080/hive/sales" -> "orders",
    "jdbc:mysql://db.host/sales" -> "orders", "jdbc:mariadb://db.host/sales" -> "orders", "jdbc:db2://db.host/sales" -> "orders",
    "jdbc:sqlserver://db.host;databaseName=sales" -> "orders", "jdbc:sqlserver://db.host:1433;database=sales;user=u;password=p" -> "orders",
    "jdbc:sqlserver://db.host" -> "orders", "jdbc:oracle:thin:@//db.host:1521/svc" -> "S.T", "jdbc:oracle:thin:@db.host/svc" -> "T",
    "jdbc:oracle:thin:u/p@db.host:1521:orcl" -> "T", "jdbc:teradata://db.host/DATABASE=sales,DBS_PORT=1030" -> "orders",
    "jdbc:teradata://db.host" -> "orders", "jdbc:redshift://c.r.amazonaws.com:5439/dev" -> "public.orders",
    "jdbc:postgresql://u:p@db.host/sales" -> "orders", "jdbc:postgresql://db.host:5432/sales?user=u&password=p" -> "orders",
    "jdbc:trino://db.host/hive" -> "sales.orders", "jdbc:sqlite:/var/db.sqlite" -> "events"
  )

  /** Where Invaract names a dataset otherwise, on purpose. */
  private val Deliberate = Map(
    "s3a://user:secret@bucket/path" -> "credentials never reach a name; openlineage-spark keeps them in the namespace",
    "S3://bucket/path" -> "a scheme is case-insensitive (RFC 3986): Invaract lower-cases it, openlineage-spark keeps it"
  )

  private def ours(location: String): Option[DatasetId] = scala.util.Try(OpenLineageNaming.dataset(location)).toOption
  private def theirs(location: String): Option[DatasetId] = scala.util.Try {
    val d = FilesystemDatasetUtils.fromLocation(new java.net.URI(location))
    DatasetId(d.getNamespace, d.getName)
  }.toOption

  test("every file system location is named as openlineage-spark names it") {
    val differ = for (l <- locations; if ours(l) != theirs(l)) yield s"$l: invaract ${ours(l)}, openlineage-spark ${theirs(l)}"
    assert(differ.isEmpty, differ.mkString("\n"))
  }

  test("every JDBC table is named as openlineage-spark names it") {
    val differ = for ((url, table) <- tables) yield {
      val mine = ours(s"$url/$table")
      val reference = scala.util.Try {
        val d = JdbcDatasetUtils.getDatasetIdentifier(url, table, new java.util.Properties())
        DatasetId(d.getNamespace, d.getName)
      }.toOption
      if (mine == reference) None else Some(s"$url $table: invaract $mine, openlineage-spark $reference")
    }
    assert(differ.flatten.isEmpty, differ.flatten.mkString("\n"))
  }

  test("the deliberate differences are still differences, and still what Invaract means them to be") {
    assert(ours("s3a://user:secret@bucket/path").contains(DatasetId("s3://bucket", "path")))
    assert(ours("S3://bucket/path").contains(DatasetId("s3://bucket", "path")))
    Deliberate.keys.foreach(l => assert(ours(l) != theirs(l), s"$l is named alike now: remove it from Deliberate"))
  }
}
