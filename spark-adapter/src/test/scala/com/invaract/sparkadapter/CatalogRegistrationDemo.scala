// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser
import com.invaract.sparkadapter.notification.FileNotificationSink

import org.apache.spark.sql.SparkSession

import java.nio.file.Files

/** A runnable, standalone demonstration of mandatory catalog registration
  * (docs-site's "Require Catalog Registration" guide) against a real,
  * embedded Hive metastore — not a `ScalaTest` spec, and not exercising any
  * test-only helper: every call here (`ContractParser.parse`,
  * `ContractEnforcementRule.forContract`, `FileNotificationSink`,
  * `ContractViolationException`) is the same public API a real job's own
  * code would call. It lives in `spark-adapter`'s test sources purely for
  * dependency convenience — `spark-hive` (needed to `enableHiveSupport()`
  * with a real embedded metastore) is already a well-audited, CVE-remediated
  * test-scope dependency here (see `build.sbt`'s own comments), and adding
  * it as a second, compile-scope copy to `runner` just to ship this demo
  * would duplicate that whole remediation surface for a demo-only jar. Run
  * via `./dev/catalog-registration-demo`, which invokes this three times
  * (`sbt "Test/runMain com.invaract.sparkadapter.CatalogRegistrationDemo <scenario>"`)
  * — one real, separate Spark session per scenario, the same way three
  * different real jobs (each carrying its own contract) would each install
  * Invaract; there is no public API for swapping a session's active
  * contract mid-flight, since a real job never needs to.
  */
object CatalogRegistrationDemo {

  private val Scenarios = Set("pass", "missing-registration", "wrong-metastore")

  def main(args: Array[String]): Unit = {
    val scenario = args.headOption.filter(Scenarios.contains).getOrElse(
      sys.error(s"usage: CatalogRegistrationDemo <${Scenarios.mkString("|")}>")
    )

    val scratchDir = Files.createTempDirectory("invaract-catalog-demo")
    System.setProperty("derby.stream.error.file", scratchDir.resolve("derby.log").toString)

    val eventsFile = scratchDir.resolve("events.jsonl")
    val sink = new FileNotificationSink
    sink.configure(Map("path" -> eventsFile.toString))

    val outputLocation = scratchDir.resolve("sales").toString
    val contractYaml = contractFor(scenario, outputLocation)
    val contract = ContractParser.parse(contractYaml)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"CatalogRegistrationDemo-$scenario")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${scratchDir.resolve("metastore_db")};create=true")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .withExtensions(_.injectCheckRule(ContractEnforcementRule.forContract(contract, VerificationOptions(), sink)))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Registered the same way a real job's own code would - see
    // SparkAdapterListener's own class doc for why this is a genuinely
    // different moment than the check rule above: WriteEvent only
    // publishes once Spark reports the write actually completed, so it's
    // the only way to see the real catalog identity a successful write
    // resolved to, not just what the pre-write check compared it against.
    spark.listenerManager.register(new SparkAdapterListener(Some(sink), Some(contract)))

    val rule = "=" * 100
    println(rule)
    println(s"Scenario: $scenario")
    println(rule)
    println("Contract:")
    println(contractYaml)

    try {
      scenario match {
        case "pass" =>
          // A real Hive EXTERNAL table, registered under exactly the
          // technology/table the contract declares, BEFORE the write it
          // gates - matching how a platform team would set up a table once,
          // then run many jobs against it.
          spark.sql(s"CREATE EXTERNAL TABLE sales_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$outputLocation'")
          spark.sql("INSERT INTO sales_tbl SELECT 1, 10")
          println("\nResult: write SUCCEEDED (catalog registration satisfied)")
          // SparkAdapterListener.onSuccess publishes WriteEvent asynchronously
          // on Spark's own listener-bus thread (see its own class doc), so a
          // real caller - and this demo - has to wait for it rather than
          // assume it has already landed by the time control returns from
          // the write above. Mirrors HiveConnectorSpec's own
          // captureNotifications helper: poll the real events file for the
          // WriteEvent line instead of a fixed, flaky sleep.
          org.scalatest.concurrent.Eventually.eventually(
            org.scalatest.concurrent.Eventually.timeout(org.scalatest.time.Span(5, org.scalatest.time.Seconds))
          ) {
            val published = Files.exists(eventsFile) &&
              Files.readAllLines(eventsFile).toArray.toIndexedSeq.exists(_.toString.contains("\"eventType\": \"WRITE\""))
            assert(published, "expected a WriteEvent to be published by now")
          }

        case "missing-registration" =>
          // A bare-path write with a perfectly correct schema - no catalog
          // entry at all. Before this feature, this passed cleanly.
          import spark.implicits._
          Seq((1L, 10L)).toDF("id", "value").write.mode("overwrite").parquet(outputLocation)
          println("\nResult: write SUCCEEDED - THIS SHOULD NOT HAPPEN (missing-registration should be rejected)")

        case "wrong-metastore" =>
          // A real Hive registration exists - just not under the metastore
          // location the contract declares.
          spark.sql(s"CREATE EXTERNAL TABLE sales_tbl (id BIGINT, value BIGINT) STORED AS PARQUET LOCATION '$outputLocation'")
          spark.sql("INSERT INTO sales_tbl SELECT 1, 10")
          println("\nResult: write SUCCEEDED - THIS SHOULD NOT HAPPEN (wrong-metastore should be rejected)")
      }
    } catch {
      case e: ContractViolationException =>
        println("\nResult: write REJECTED\n")
        println(e.getMessage)
    }

    println(s"\n${"-" * 100}")
    println("Published notification events (real captured JSON, one line per event):")
    println("-" * 100)
    if (Files.exists(eventsFile)) {
      Files.readAllLines(eventsFile).forEach(line => println(line))
    } else {
      println("(none published)")
    }
    println(rule)

    spark.stop()
  }

  private def contractFor(scenario: String, outputLocation: String): String = {
    val catalogBlock = scenario match {
      case "pass" =>
        """    catalog:
          |      required: true
          |      technology: hive
          |      table: sales_tbl
          |""".stripMargin
      case "missing-registration" =>
        """    catalog:
          |      required: true
          |      technology: hive
          |""".stripMargin
      case "wrong-metastore" =>
        """    catalog:
          |      required: true
          |      technology: hive
          |      location: thrift://not-the-real-metastore.example.com:9083
          |""".stripMargin
    }
    s"""id: catalog_registration_demo
       |version: "1.0.0"
       |outputs:
       |  - name: sales
       |    location: $outputLocation
       |    format: parquet
       |$catalogBlock    schema:
       |      fields:
       |        - name: id
       |          type: long
       |          required: true
       |        - name: value
       |          type: long
       |          required: true
       |""".stripMargin
  }
}
