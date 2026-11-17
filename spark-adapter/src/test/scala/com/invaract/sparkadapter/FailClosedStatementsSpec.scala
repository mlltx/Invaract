// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** The fail-closed policy judged by the SQL a person writes, not by class names. `FailClosedCommands` is keyed by the
  * class of the *analyzed* plan, and for many statements that is a catalyst command, not the `execution.command` class
  * the statement later runs as: a class-name test alone once missed that `DROP DATABASE ... CASCADE` arrives as the
  * safe-listed `DropNamespace`, and passed. These tests run the statements themselves under a contract whose only output
  * none of them touches.
  */
class FailClosedStatementsSpec extends ConnectorSpecBase {
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-fail-closed-statements")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FailClosedStatementsSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  /** A contract none of the statements below writes to. */
  private def contract: String =
    s"""id: statements
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: ${dir.resolve("declared_out")}
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |""".stripMargin

  private def refusal(sql: String): ContractViolationException =
    intercept[ContractViolationException](withContract(contract)(spark.sql(sql).collect()))

  test("DROP DATABASE without CASCADE passes: Spark refuses to drop a database that holds a table, so it deletes nothing") {
    spark.sql("CREATE DATABASE empty_db")
    withContract(contract)(spark.sql("DROP DATABASE empty_db").collect())
    assert(!spark.catalog.databaseExists("empty_db"))
  }

  test("DROP DATABASE ... CASCADE is refused, in its own words, and the tables in the database keep their rows") {
    spark.sql("CREATE DATABASE full_db")
    spark.sql("CREATE TABLE full_db.t (id long) USING parquet")
    spark.sql("INSERT INTO full_db.t VALUES (1), (2)")
    val e = refusal("DROP DATABASE full_db CASCADE")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("DROP DATABASE ... CASCADE deletes every table in the database"))
    assert(e.getMessage.contains("Drop the tables this job is allowed to remove one at a time"))
    assert(spark.table("full_db.t").count() == 2)
  }

  test("DROP DATABASE IF EXISTS ... CASCADE is refused for a database that does not exist: the statement is judged, not what it would find") {
    assert(refusal("DROP DATABASE IF EXISTS no_such_db CASCADE").result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
  }

  test("statements that delete or replace rows of a table the contract does not declare are never let through") {
    spark.sql("CREATE TABLE plain_t (id long) USING parquet")
    spark.sql("INSERT INTO plain_t VALUES (1), (2)")
    spark.sql("CREATE TABLE delta_t (id long) USING delta")
    spark.sql("INSERT INTO delta_t VALUES (1)")
    spark.sql("INSERT INTO delta_t VALUES (2)")
    val source = dir.resolve("load_source")
    spark.range(3).write.parquet(source.toString)
    val statements = List(
      s"LOAD DATA LOCAL INPATH '$source' INTO TABLE plain_t",
      "RESTORE TABLE delta_t TO VERSION AS OF 1",
      "TRUNCATE TABLE plain_t",
      "DROP TABLE plain_t"
    )
    statements.foreach { sql =>
      val types = refusal(sql).result.violations.map(_.violationType)
      assert(types == List(ViolationType.UnverifiableWrite), s"$sql: $types")
    }
    assert(spark.table("plain_t").count() == 2, "every refused statement left plain_t's rows alone")
    assert(spark.table("delta_t").count() == 2, "and delta_t's")
  }
}
