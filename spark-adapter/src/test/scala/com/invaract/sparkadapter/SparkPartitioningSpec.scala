// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.PartitionOverwriteModes
import com.invaract.verification.{ContractViolationException, ViolationType, WritePartitioning}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}

import java.nio.file.{Files, Path}

/** `check.partitioning` on a real Spark session (with Delta, for its `.save(path)` and table writes): the partition columns
  * and overwrite mode each write shape states, against an output's declared partition keys and `partitionOverwriteMode`, and a
  * shape that states none refused rather than passed (docs/SPARK_ADAPTER.md, "Partitioning"). Iceberg's V2 writes are in
  * `IcebergConnectorSpec`. */
class SparkPartitioningSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-partitioning")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkPartitioningSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def at(name: String): String = scratchDir.resolve(name).toString

  /** An output at `location` partitioned by `keys`, in that order, with `extras` (a `partitionOverwriteMode:` line). */
  private def partitioned(location: String, keys: List[String], extras: String = "", format: String = "parquet"): String = {
    def field(name: String) = {
      val position = keys.indexOf(name)
      val partition = if (position < 0) "" else s", partitioned: true, partitionKeyPosition: ${position + 1}"
      s"        - { name: $name, type: long$partition }"
    }
    s"""id: partitioning
       |version: "1.0.0"
       |outputs:
       |  - name: report
       |    location: $location
       |    format: $format
       |$extras
       |    schema:
       |      fields:
       |${List("id", "region", "value").map(field).mkString("\n")}
       |""".stripMargin
  }

  private def frame() = spark.createDataFrame(Seq((1L, 1L, 10L), (2L, 2L, 20L))).toDF("id", "region", "value")

  private def refused(body: => Unit): ContractViolationException = intercept[ContractViolationException](body)

  private def withConf[T](key: String, value: String)(body: => T): T = {
    spark.conf.set(key, value)
    try body
    finally spark.conf.unset(key)
  }

  // --- what each shape states (PartitioningSupport) ------------------------------------------------

  test("configuredMode: the write's own option in any case, else the session setting, lower-cased") {
    assert(PartitioningSupport.configuredMode(Map.empty) == PartitionOverwriteModes.Static, "Spark's default is static")
    assert(PartitioningSupport.configuredMode(Map("partitionOverwriteMode" -> "DYNAMIC")) == PartitionOverwriteModes.Dynamic)
    assert(PartitioningSupport.configuredMode(Map("PARTITIONOVERWRITEMODE" -> " dynamic ")) == PartitionOverwriteModes.Dynamic)
    assert(PartitioningSupport.configuredMode(Map("other" -> "dynamic")) == PartitionOverwriteModes.Static)
    withConf("spark.sql.sources.partitionOverwriteMode", "dynamic") {
      assert(PartitioningSupport.configuredMode(Map.empty) == PartitionOverwriteModes.Dynamic)
      assert(PartitioningSupport.configuredMode(Map("partitionOverwriteMode" -> "static")) == PartitionOverwriteModes.Static, "the option wins")
    }
  }

  test("fileWrite: dynamic only when configured so and some partition column takes its value from the data") {
    val dynamic = Map("partitionOverwriteMode" -> "dynamic")
    assert(PartitioningSupport.fileWrite(List("a", "b"), dynamic, 0) == WritePartitioning(List("a", "b"), PartitionOverwriteModes.Dynamic))
    assert(PartitioningSupport.fileWrite(List("a", "b"), dynamic, 1).overwriteMode == PartitionOverwriteModes.Dynamic)
    assert(PartitioningSupport.fileWrite(List("a", "b"), dynamic, 2).overwriteMode == PartitionOverwriteModes.Static, "every column static")
    assert(PartitioningSupport.fileWrite(Nil, dynamic, 0) == WritePartitioning(Nil, PartitionOverwriteModes.Static), "nothing to partition")
    assert(PartitioningSupport.fileWrite(List("a"), Map.empty, 0).overwriteMode == PartitionOverwriteModes.Static)
  }

  test("identityColumns: plain columns in order, nested ones dotted; any derived transform says nothing") {
    val identity: Seq[Transform] = Seq(Expressions.identity("region"), Expressions.identity("nested.day"))
    assert(PartitioningSupport.identityColumns(identity).contains(List("region", "nested.day")))
    assert(PartitioningSupport.identityColumns(Nil).contains(Nil))
    assert(PartitioningSupport.identityColumns(identity :+ Expressions.days("ts")).isEmpty)
    assert(PartitioningSupport.identityColumns(Seq(Expressions.bucket(4, "id"))).isEmpty)
  }

  // --- file-source writes --------------------------------------------------------------------------

  test("a write partitioned by the declared keys, in order, passes; another order, other keys or none are refused, naming both") {
    val out = at("by_keys")
    withContract(partitioned(out, List("region", "id"))) {
      frame().write.mode("overwrite").partitionBy("region", "id").parquet(out)
      def mismatch(body: => Unit) = {
        val List(v) = refused(body).result.violations
        assert(v.violationType == ViolationType.OutputPartitioningMismatch)
        (v.expected, v.actual)
      }
      assert(mismatch(frame().write.mode("overwrite").partitionBy("id", "region").parquet(out)) == ((Some("region, id"), Some("id, region"))))
      // the guide's example (guides/partitioning-outputs.mdx, "Write it from Spark")
      assert(refused(frame().write.mode("overwrite").partitionBy("region").parquet(out)).result.violations.map(_.message) ==
        List("contract declares the output partitioned by region, id but the write partitions by region"))
      assert(mismatch(frame().write.mode("overwrite").partitionBy("region").parquet(out)) == ((Some("region, id"), Some("region"))))
      assert(mismatch(frame().write.mode("overwrite").parquet(out)) == ((Some("region, id"), Some("none"))))
    }
  }

  test("an output that declares no partition keys is not held to any partitioning") {
    val out = at("unkeyed")
    withContract(partitioned(out, Nil)) {
      frame().write.mode("overwrite").partitionBy("region").parquet(out)
      frame().write.mode("overwrite").parquet(out)
    }
  }

  test("an overwrite is held to the declared overwrite mode: the write's option, else the session's setting") {
    val out = at("overwrite_mode")
    withContract(partitioned(out, List("region"), "    partitionOverwriteMode: dynamic")) {
      frame().write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("region").parquet(out)
      withConf("spark.sql.sources.partitionOverwriteMode", "dynamic")(frame().write.mode("overwrite").partitionBy("region").parquet(out))
      val List(v) = refused(frame().write.mode("overwrite").partitionBy("region").parquet(out)).result.violations
      assert(v.violationType == ViolationType.OutputPartitioningMismatch)
      assert((v.expected, v.actual) == ((Some("dynamic overwrite"), Some("static overwrite"))))
      // an append replaces nothing, so its mode is not compared
      frame().write.mode("append").partitionBy("region").parquet(out)
    }
    withContract(partitioned(out, List("region"))) { // static is the default a contract declares
      frame().write.mode("overwrite").partitionBy("region").parquet(out)
      val List(v) = refused(frame().write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("region").parquet(out))
        .result.violations
      assert((v.expected, v.actual) == ((Some("static overwrite"), Some("dynamic overwrite"))))
    }
  }

  test("an INSERT OVERWRITE into a partitioned table: dynamic as Spark decides it, static when its PARTITION clause names every key") {
    val location = scratchDir.resolve("sales_tbl").toUri.toString
    spark.sql(s"CREATE TABLE sales_tbl (id BIGINT, value BIGINT, region BIGINT) USING parquet PARTITIONED BY (region) LOCATION '$location'")
    frame().select("id", "value", "region").createOrReplaceTempView("sales_src")
    val tableLocation = spark.sql("DESCRIBE TABLE EXTENDED sales_tbl").where("col_name = 'Location'").head().getString(1)
    withContract(partitioned(tableLocation, List("region"), "    partitionOverwriteMode: dynamic")) {
      withConf("spark.sql.sources.partitionOverwriteMode", "dynamic") {
        spark.sql("INSERT OVERWRITE TABLE sales_tbl SELECT id, value, region FROM sales_src")
        val List(v) = refused(spark.sql("INSERT OVERWRITE TABLE sales_tbl PARTITION (region = 1) SELECT id, value FROM sales_src")).result.violations
        assert((v.expected, v.actual) == ((Some("dynamic overwrite"), Some("static overwrite"))))
      }
    }
  }

  test("a new partitioned data source table is written whole, by its declared keys") {
    val location = scratchDir.resolve("new_tbl").toUri.toString
    withContract(partitioned(location, List("region"))) {
      frame().write.partitionBy("region").option("path", location).saveAsTable("new_partitioned_tbl")
    }
    withContract(partitioned(scratchDir.resolve("new_tbl_2").toUri.toString, List("region"))) {
      val violations = refused(frame().write.option("path", scratchDir.resolve("new_tbl_2").toUri.toString).saveAsTable("new_unpartitioned_tbl"))
        .result.violations
      assert(violations.map(_.violationType).contains(ViolationType.OutputPartitioningMismatch))
    }
  }

  // --- Delta ---------------------------------------------------------------------------------------

  test("a Delta .save(path) given .partitionBy is checked; one with none is refused as unverifiable, not passed") {
    val out = at("delta_out")
    withContract(partitioned(out, List("region"), format = "delta")) {
      frame().write.format("delta").mode("overwrite").partitionBy("region").save(out)
      val List(v) = refused(frame().write.format("delta").mode("overwrite").partitionBy("id").save(out)).result.violations
      assert(v.violationType == ViolationType.OutputPartitioningMismatch)
      // an append with no .partitionBy keeps the table's partitioning, which the write does not state
      val List(u) = refused(frame().write.format("delta").mode("append").save(out)).result.violations
      assert(u.violationType == ViolationType.UnverifiableWrite)
      assert(u.message.contains("Output 'report' declares partition keys (region)") && u.message.contains("SaveIntoDataSourceCommand"))
      assert(u.remediation.contains(".partitionBy(...)"))
    }
    // with no partition keys declared, the same write is checked as before
    withContract(partitioned(out, Nil, format = "delta"))(frame().write.format("delta").mode("append").save(out))
  }

  test("a Delta table write states its table's partitioning (V2): an append is checked against the declared keys") {
    spark.sql("CREATE TABLE delta_sales (id BIGINT, region BIGINT, value BIGINT) USING delta PARTITIONED BY (region)")
    val location = scratchDir.resolve("warehouse").resolve("delta_sales").toString
    withContract(partitioned(location, List("region"), format = "delta")) {
      frame().writeTo("delta_sales").append()
    }
    withContract(partitioned(location, List("id"), format = "delta")) {
      val List(v) = refused(frame().writeTo("delta_sales").append()).result.violations
      assert((v.violationType, v.expected, v.actual) == ((ViolationType.OutputPartitioningMismatch, Some("id"), Some("region"))))
    }
  }

  test("an invalid contract is reported as invalid, not as an unverifiable partitioning") {
    val out = at("delta_invalid")
    frame().write.format("delta").mode("overwrite").partitionBy("region").save(out)
    val invalid = partitioned(out, List("region"), format = "delta").replace("id: partitioning", "id: \" \"")
    val types = refused(withContract(invalid)(frame().write.format("delta").mode("append").save(out))).result.violations.map(_.violationType)
    assert(types.nonEmpty && !types.contains(ViolationType.UnverifiableWrite), types)
  }

  // --- dry run -------------------------------------------------------------------------------------

  test("a dry run's draft declares the keys and a dynamic overwrite the write used") {
    val out = at("drafted")
    frame().write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("region").parquet(out)
    val plan = capturedPlans.reverseIterator.find(_.getClass.getSimpleName == "InsertIntoHadoopFsRelationCommand").get
    val outcome = ContractEnforcementRule.inferOutcome(plan).collect { case i: ContractEnforcementRule.InferenceOutcome.Inferred => i }.get
    val output = outcome.contract.outputs.head
    assert(output.partitionKeys == List("region"))
    assert(output.partitionOverwriteMode.contains(PartitionOverwriteModes.Dynamic))
  }
}
