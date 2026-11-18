// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.notification.{ContractValidationEvent, TestNotificationSink}
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}
import java.util.concurrent.{Executors, TimeUnit}

/** One statement can be checked more than once (Spark checks a `saveAsTable`, then the insert it runs inside it): every
  * check of one write publishes the same `writeKey`, and each write its own (H-16 of
  * docs/SPARK_ADAPTER_HARDENING_PLAN.md). */
class WriteKeysSpec extends ConnectorSpecBase {
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-write-keys")
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("WriteKeysSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def contract(outputs: String*): String =
    (List("id: write_keys", "version: \"1.0.0\"", "outputs:") ++ outputs.zipWithIndex.map { case (location, i) =>
      s"  - { name: out$i, location: $location, schema: { fields: [ { name: id, type: long } ] } }"
    }).mkString("\n")

  private def table(name: String) = s"spark_catalog/default/$name"

  /** The validation events `body` publishes, under a contract declaring `outputs`. */
  private def events(outputs: String*)(body: => Unit): List[ContractValidationEvent] = {
    val sink = new TestNotificationSink
    withSink(sink)(withContract(contract(outputs: _*))(body))
    sink.events.collect { case v: ContractValidationEvent => v }
  }

  private def keysOf(found: List[ContractValidationEvent]): List[String] = found.flatMap(_.writeKey).distinct

  test("a path write: one event, one key, named after the location") {
    val out = dir.resolve("plain").toString
    val found = events(out)(spark.range(3).write.parquet(out))
    assert(found.map(_.status) == List("PASSED"))
    assert(found.forall(_.writeKey.isDefined))
    assert(keysOf(found).size == 1 && keysOf(found).head.startsWith(out + "#"), keysOf(found))
  }

  test("saveAsTable of a new table and of an existing one: each checked twice, each one write") {
    spark.sql("DROP TABLE IF EXISTS k_new")
    val created = events(table("k_new"))(spark.range(3).write.saveAsTable("k_new"))
    assert(created.size == 2 && keysOf(created).size == 1, created.map(_.writeKey))
    val appended = events(table("k_new"))(spark.range(3).write.mode("append").saveAsTable("k_new"))
    assert(appended.size == 2 && keysOf(appended).size == 1, appended.map(_.writeKey))
    assert(keysOf(created) != keysOf(appended), "two statements, two writes")
  }

  test("CREATE TABLE ... USING delta AS SELECT: every check of it shares one key; INSERT and MERGE are one event each") {
    spark.sql("DROP TABLE IF EXISTS k_delta")
    val ctas = events(table("k_delta"))(spark.sql("CREATE TABLE k_delta USING delta AS SELECT id FROM range(3)"))
    assert(ctas.size >= 2 && keysOf(ctas).size == 1, ctas.map(_.writeKey))
    val insert = events(table("k_delta"))(spark.sql("INSERT INTO k_delta SELECT id FROM range(3)"))
    val merge = events(table("k_delta"))(
      spark.sql("MERGE INTO k_delta t USING (SELECT id FROM range(5)) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *")
    )
    List(insert, merge).foreach(found => assert(found.size == 1 && keysOf(found).size == 1, found.map(_.writeKey)))
    assert(keysOf(ctas ++ insert ++ merge).size == 3)
  }

  test("two appends to the same place, one after the other, are two writes") {
    val out = dir.resolve("twice").toString
    val found = events(out) {
      spark.range(3).write.mode("append").parquet(out)
      spark.range(3).write.mode("append").parquet(out)
    }
    assert(found.size == 2 && keysOf(found).size == 2, found.map(_.writeKey))
  }

  test("sixteen threads each writing a table of their own: sixteen writes") {
    val names = (0 until 16).map(i => s"k_thread_$i")
    names.foreach(n => spark.sql(s"DROP TABLE IF EXISTS $n"))
    val pool = Executors.newFixedThreadPool(16)
    val found =
      try events(names.map(table): _*) {
        val futures = names.map(n => pool.submit(new Runnable { def run(): Unit = spark.range(3).write.saveAsTable(n) }))
        futures.foreach(_.get(5, TimeUnit.MINUTES))
      }
      finally pool.shutdown()
    assert(found.size == 32, "each saveAsTable is checked twice")
    assert(keysOf(found).size == 16, found.map(_.writeKey))
    names.foreach(n => assert(keysOf(found).count(_.contains(s"/$n#")) == 1, n))
  }

  test("keyFor inside a checked write's execution: one shared name makes it the same write; none, a new one") {
    import org.apache.spark.sql.execution.SQLExecution
    val outer = spark.range(1).queryExecution.analyzed
    val outerKey = WriteKeys.keyFor(outer, List("/w/a", "spark_catalog/default/t"))
    assert(outerKey.startsWith("/w/a#"))
    val execution = spark.sessionState.executePlan(outer)
    val (sharesOne, sharesNone) = SQLExecution.withNewExecutionId(execution) {
      (
        WriteKeys.keyFor(spark.range(2).queryExecution.analyzed, List("/w/elsewhere", "spark_catalog/default/t")),
        WriteKeys.keyFor(spark.range(3).queryExecution.analyzed, List("/w/other"))
      )
    }
    assert(sharesOne == outerKey, "a name in common (the table) is enough")
    assert(sharesNone != outerKey && sharesNone.startsWith("/w/other#"))
    assert(WriteKeys.keyFor(spark.range(4).queryExecution.analyzed, List("/w/a")) != outerKey, "outside the execution, a new write")
  }
}
