// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider

import java.nio.file.Files
import java.util.Properties

/** JDBC reads and writes against an embedded, in-memory Derby database (on the test classpath through Hive): a JDBC
  * table is located as `jdbc:<connection URL>/<table>` whether the job reads it or writes it (docs/connectors/jdbc.md).
  */
class JdbcConnectorSpec extends ConnectorSpecBase {
  private val url = "jdbc:derby:memory:jdbc_connector_spec;create=true"
  private val props = {
    val p = new Properties()
    p.setProperty("driver", "org.apache.derby.jdbc.EmbeddedDriver")
    p
  }
  private def tableLocation(table: String) = s"jdbc:$url/$table"

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("JdbcConnectorSpec")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("invaract-jdbc-test").resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def outputContract(location: String): String =
    s"""id: jdbc_out
       |version: "1.0.0"
       |outputs:
       |  - name: orders
       |    location: "$location"
       |    format: jdbc
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |""".stripMargin

  test("PASS: a JDBC write to the table the contract declares is checked and lands") {
    withContract(outputContract(tableLocation("ORDERS_OK")))(df().write.mode("overwrite").jdbc(url, "ORDERS_OK", props))
    assert(spark.read.jdbc(url, "ORDERS_OK", props).count() == 2)
  }

  test("FAIL: a JDBC write to another table is refused, and the refusal never shows the connection options") {
    val e = intercept[ContractViolationException](
      withContract(outputContract(tableLocation("ORDERS_OK")))(df().write.mode("overwrite").jdbc(url, "ORDERS_ELSEWHERE", props))
    )
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
    assert(e.getMessage.contains(tableLocation("ORDERS_ELSEWHERE")))
    assert(!e.getMessage.contains("driver") && !e.getMessage.contains("Map("), "options can hold a password")
  }

  test("a JDBC table read and written is the same location to the contract") {
    df().write.mode("overwrite").jdbc(url, "ORDERS_SOURCE", props)
    val out = Files.createTempDirectory("invaract-jdbc-out").resolve("copy").toString
    val yaml =
      s"""id: jdbc_in
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: "${tableLocation("ORDERS_SOURCE")}"
         |    format: jdbc
         |    schema:
         |      fields:
         |        - { name: ID, type: long }
         |        - { name: VALUE, type: long }
         |outputs:
         |  - name: copy
         |    location: $out
         |    schema:
         |      fields:
         |        - { name: ID, type: long }
         |""".stripMargin
    withContract(yaml, VerificationOptions(rejectUndeclaredInputs = true)) {
      spark.read.jdbc(url, "ORDERS_SOURCE", props).select("ID").write.mode("overwrite").parquet(out) // must not throw
    }
  }

  private def saveInto(options: Map[String, String]) =
    SaveIntoDataSourceCommand(spark.range(1).queryExecution.analyzed, new JdbcRelationProvider, options, SaveMode.Append)

  test("a save is located by its path first, then by a JDBC url and table (in any case), else by its source alone") {
    val withPath = WriteCommandSupport.combined(saveInto(Map("path" -> "/data/x", "url" -> url, "dbtable" -> "T")))
    assert(withPath.location == "/data/x" && withPath.diagnostic.isEmpty)
    val jdbc = WriteCommandSupport.combined(saveInto(Map("URL" -> url, "DBTable" -> "T")))
    assert(jdbc.location == tableLocation("T") && jdbc.diagnostic.isEmpty)
    assert(jdbc.format.contains("jdbc"))
    Seq(Map.empty[String, String], Map("url" -> url), Map("dbtable" -> "T")).foreach { options =>
      val neither = WriteCommandSupport.combined(saveInto(options))
      assert(neither.location == "JdbcRelationProvider(no path)", options)
      assert(neither.diagnostic.exists(_.nodeType == "SaveIntoDataSourceCommand"), options)
    }
  }
}
