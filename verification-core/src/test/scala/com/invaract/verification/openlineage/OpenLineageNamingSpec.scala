// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.notification.CatalogInfo

import org.scalatest.funsuite.AnyFunSuite

/** Dataset names per the OpenLineage naming conventions (docs/OPENLINEAGE.md, section 2.2). */
class OpenLineageNamingSpec extends AnyFunSuite {
  private def named(location: String, catalog: Option[CatalogInfo] = None): DatasetId =
    OpenLineageNaming.dataset(location, catalog, "/work/job")

  test("local paths: namespace file, the absolute path; a relative one against the working directory") {
    assert(named("/data/out/report") == DatasetId("file", "/data/out/report"))
    assert(named("file:/data/out/report") == DatasetId("file", "/data/out/report"))
    assert(named("file:///data/out/report") == DatasetId("file", "/data/out/report"))
    assert(named("file://host/data/out") == DatasetId("file", "/data/out"))
    assert(named("file://host") == DatasetId("file", "/"))
    assert(named("file:rel/out") == DatasetId("file", "/work/job/rel/out"))
    assert(named("data/out/report") == DatasetId("file", "/work/job/data/out/report"))
    assert(named("./data/../out") == DatasetId("file", "/work/job/out"))
    assert(named("/a/./b/../c") == DatasetId("file", "/a/c"))
    assert(named("  /trimmed  ") == DatasetId("file", "/trimmed"))
    assert(named("C:\\data\\out") == DatasetId("file", "C:/data/out"))
  }

  test("the default working directory is the process's own") {
    assert(OpenLineageNaming.dataset("rel") == DatasetId("file", java.nio.file.Paths.get(System.getProperty("user.dir"), "rel").normalize.toString))
  }

  test("object storage: the bucket is the namespace, the key the name; s3a and s3n are s3, gcs is gs") {
    assert(named("s3://bucket/a/b") == DatasetId("s3://bucket", "a/b"))
    assert(named("s3a://bucket/a/b") == DatasetId("s3://bucket", "a/b"))
    assert(named("s3n://bucket/a/b") == DatasetId("s3://bucket", "a/b"))
    assert(named("S3A://bucket/a") == DatasetId("s3://bucket", "a"), "a scheme is case-insensitive")
    assert(named("gs://bucket/a/b") == DatasetId("gs://bucket", "a/b"))
    assert(named("gcs://bucket/a") == DatasetId("gs://bucket", "a"))
    assert(named("s3://bucket") == DatasetId("s3://bucket", ""))
  }

  test("credentials never reach a name; an Azure container before @ is not a credential") {
    assert(named("s3a://AKIA:secret@bucket/a") == DatasetId("s3://bucket", "a"))
    assert(named("postgresql://user:pass@db.host:5432/sales/public/orders") == DatasetId("postgres://db.host:5432", "sales.public.orders"))
    assert(named("abfss://container@account.dfs.core.windows.net/path/t") == DatasetId("abfss://container@account.dfs.core.windows.net", "/path/t"))
    assert(named("hdfs://user:pw@nn:8020/w") == DatasetId("hdfs://nn:8020", "/w"))
  }

  test("a password holding '@' is still stripped whole; an empty user part is no part of the host") {
    assert(named("s3a://user:p@ss@bucket/key") == DatasetId("s3://bucket", "key"))
    assert(named("hdfs://u:a@b@nn:8020/w") == DatasetId("hdfs://nn:8020", "/w"))
    assert(named("hdfs://@nn:8020/w") == DatasetId("hdfs://nn:8020", "/w"))
    assert(named("hdfs://nn/a?x=1?y=2#f") == DatasetId("hdfs://nn", "/a"), "the first ? ends the path")
    assert(named("hdfs://nn/a#f?x") == DatasetId("hdfs://nn", "/a"), "so does the first #, whichever comes first")
  }

  test("other hierarchical schemes: namespace scheme://authority, name the path; a query or fragment is not part of it") {
    assert(named("hdfs://nn:8020/warehouse/t") == DatasetId("hdfs://nn:8020", "/warehouse/t"))
    assert(named("hdfs://nn:8020") == DatasetId("hdfs://nn:8020", "/"))
    assert(named("wasbs://c@acct.blob.core.windows.net/x?sv=token") == DatasetId("wasbs://c@acct.blob.core.windows.net", "/x"))
    assert(named("http://host/a#frag") == DatasetId("http://host", "/a"))
    assert(named("mysql://h:3306/db/t") == DatasetId("mysql://h:3306", "db.t"))
  }

  test("JDBC URLs are named by the database URL they wrap") {
    assert(named("jdbc:postgresql://h:5432/db/s/t") == DatasetId("postgres://h:5432", "db.s.t"))
  }

  test("an opaque scheme (bigquery:...) is that scheme's namespace, its colons dots") {
    assert(named("bigquery:proj.ds.t") == DatasetId("bigquery", "proj.ds.t"))
    assert(named("bigquery:proj:ds.t") == DatasetId("bigquery", "proj.ds.t"))
  }

  test("a bare name is its catalog table when it resolved to one, else a relative path") {
    val catalog = CatalogInfo(Some("hive"), Some("spark_catalog"), None, List("sales"), Some("orders"))
    assert(named("sales.orders", Some(catalog)) == DatasetId("hive://spark_catalog", "sales.orders"))
    assert(named("sales.orders") == DatasetId("file", "/work/job/sales.orders"))
    assert(named("report.parquet", Some(catalog.copy(table = None))) == DatasetId("file", "/work/job/report.parquet"))
    assert(named("/abs/t", Some(catalog)) == DatasetId("file", "/abs/t"), "a path stays a path")
  }

  test("table: a catalog's table, under its location's namespace or technology://catalog") {
    assert(OpenLineageNaming.table(CatalogInfo(Some("iceberg"), Some("prod"), Some("s3://wh/db"), List("db"), Some("t"))) == Some(DatasetId("s3://wh", "db.t")))
    assert(OpenLineageNaming.table(CatalogInfo(Some("iceberg"), Some("prod"), None, List("a", "b"), Some("t"))) == Some(DatasetId("iceberg://prod", "a.b.t")))
    assert(OpenLineageNaming.table(CatalogInfo(Some("hive"), None, None, Nil, Some("t"))) == Some(DatasetId("hive", "t")))
    assert(OpenLineageNaming.table(CatalogInfo(None, None, None, Nil, Some("t"))) == Some(DatasetId("catalog", "t")))
    assert(OpenLineageNaming.table(CatalogInfo(Some("hive"), Some("c"), None, Nil, None)).isEmpty)
  }
}
