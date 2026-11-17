// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.InvaractConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** `RemoteFiles` (H-15): a setting that names a file may name it by any Hadoop file system URI. */
class RemoteFilesSpec extends AnyFunSuite with BeforeAndAfterAll {
  import RemoteFiles._

  private var spark: SparkSession = _
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-remote-files")
    spark = SparkSession.builder().master("local[1]").appName("RemoteFilesSpec").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def file(name: String, content: String): Path = Files.write(dir.resolve(name), content.getBytes("UTF-8"))

  test("isRemote: a URL scheme of two letters or more, other than Invaract's own references") {
    List("file:/x", "file:///x", "s3a://b/k", "hdfs://nn:8020/x", "dbfs:/x", "abfss://c@a.dfs.core.windows.net/x").foreach(v => assert(isRemote(v), v))
    List("/data/contract.yaml", "contract.yaml", "rel/dir/c.yaml", "C:/x", "c:/x", "registry://orders@1.0.0", "ref://orders", "s3a:x", "").foreach(v =>
      assert(!isRemote(v), v)
    )
  }

  test("localize: a file: URI is copied to a readable local file with the same bytes; a plain path is unchanged") {
    val original = file("contract.yaml", "id: x\n")
    val copy = localize("spark.invaract.contract", original.toUri.toString, spark)
    assert(copy != original.toUri.toString && copy.endsWith("-contract.yaml"))
    assert(new String(Files.readAllBytes(java.nio.file.Paths.get(copy)), "UTF-8") == "id: x\n")
    assert(localize("spark.invaract.contract", original.toString, spark) == original.toString)
  }

  test("localize: a remote file that cannot be read is refused, naming the setting and the value") {
    val missing = dir.resolve("absent.yaml").toUri.toString
    val e = intercept[IllegalArgumentException](localize("spark.invaract.orgPolicy", missing, spark))
    assert(e.getMessage.startsWith(s"'spark.invaract.orgPolicy' names '$missing', which could not be read: "))
  }

  test("localizeSetting: the file settings and each overlay are made local, once per session; other settings are untouched") {
    val policy = file("policy.yaml", "version: \"1.0\"\n").toUri.toString
    val first = localizeSetting(InvaractConf.OrgPolicy, "spark.invaract.orgPolicy", s" $policy ", spark)
    assert(first != policy && Files.exists(java.nio.file.Paths.get(first)))
    assert(localizeSetting(InvaractConf.OrgPolicy, "spark.invaract.orgPolicy", policy, spark) == first, "fetched once")
    List(InvaractConf.Contract, InvaractConf.LocationMap, InvaractConf.NotifyConfig).foreach { name =>
      assert(localizeSetting(name, "k", policy, spark) == first, name)
    }
    val plain = dir.resolve("plain.yaml").toString
    assert(localizeSetting(InvaractConf.OrgPolicyOverlays, "k", s"$policy, $plain,,", spark) == s"$first,$plain")
    assert(localizeSetting(InvaractConf.LocationUrl, "k", policy, spark) == policy, "a URL setting names no file")
    assert(localizeSetting(InvaractConf.Contract, "k", "registry://orders@1.0.0", spark) == "registry://orders@1.0.0")
  }

  test("a session reads its file settings through SparkConfigSource as local copies") {
    val map = file("locations.properties", "orders=/data/orders\n").toUri.toString
    spark.conf.set("spark.invaract.locationMap", map)
    try {
      val local = SparkConfigSource(spark).get(InvaractConf.LocationMap).get
      assert(!RemoteFiles.isRemote(local) && new String(Files.readAllBytes(java.nio.file.Paths.get(local)), "UTF-8") == "orders=/data/orders\n")
    } finally spark.conf.unset("spark.invaract.locationMap")
  }
}
