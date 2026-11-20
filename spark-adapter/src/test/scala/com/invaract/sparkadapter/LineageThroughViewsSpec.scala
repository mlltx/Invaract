// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser
import com.invaract.ir
import com.invaract.verification.{ContractViolationException, SensitivityLineage, VerificationOptions, ViolationType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path}

/** Column lineage, and so sensitivity tags, must not depend on how a job is written. The same logic through the
  * DataFrame API, a temp view with `spark.sql`, a persistent view, a CTE, or a plain copy with no `select` must trace an
  * output column back to the input column it comes from (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-14).
  */
class LineageThroughViewsSpec extends ConnectorSpecBase {
  private var dir: Path = _
  private def orders = dir.resolve("orders").toString

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-lineage-views")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("LineageThroughViewsSpec")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val s = spark
    import s.implicits._
    Seq((1L, 10L, "a@x.com"), (2L, 20L, "b@x.com")).toDF("id", "value", "email").write.parquet(orders)
    spark.read.parquet(orders).write.format("parquet").saveAsTable("orders_t")
    spark.read.parquet(orders).createOrReplaceTempView("o")
    spark.read.parquet(orders).createOrReplaceTempView("o2")
  }

  private def contract(inputLocation: String) =
    s"""id: lineage
       |version: "1.0.0"
       |inputs:
       |  - name: orders
       |    location: $inputLocation
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |        - { name: email, type: string, sensitivityTags: [pii] }
       |outputs:
       |  - name: out
       |    location: ${dir.resolve("out")}
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |""".stripMargin

  /** Each output column of `df`, written somewhere, with the names of its source columns and its sensitivity tags. */
  private def lineageOf(df: DataFrame, inputLocation: String = orders): Map[String, (Set[String], Set[String])] = {
    val plan = SparkPlanAdapter.translateAsWrite(df.queryExecution.analyzed, ir.DatasetRef("out")).plan
    SensitivityLineage
      .propagate(plan, ContractParser.parse(contract(inputLocation)))
      .map(l => l.lineage.output.name -> (l.lineage.sources.map(_.name), l.sensitivityTags))
      .toMap
  }

  private def assertEmailTraced(df: DataFrame, column: String, inputLocation: String = orders): Unit = {
    val lineage = lineageOf(df, inputLocation)
    assert(lineage.get(column).exists(_._1 == Set("email")), s"$column should come from email: $lineage")
    assert(lineage.get(column).exists(_._2 == Set("pii")), s"$column should carry pii: $lineage")
  }

  test("the same upper(email) through the DataFrame API, a temp view, an aliased temp view and a subquery") {
    assertEmailTraced(spark.read.parquet(orders).select(col("id"), upper(col("email")).as("e2")), "e2")
    assertEmailTraced(spark.sql("SELECT id, upper(email) AS e2 FROM o"), "e2")
    assertEmailTraced(spark.sql("SELECT x.id, upper(x.email) AS e2 FROM o x"), "e2")
    assertEmailTraced(spark.sql("SELECT id, e2 FROM (SELECT id, upper(email) AS e2 FROM o) t"), "e2")
  }

  test("a join of two temp views traces each side's columns") {
    val df = spark.sql("SELECT a.id, b.email AS e2 FROM o a JOIN o2 b ON a.id = b.id")
    assertEmailTraced(df, "e2")
    assert(lineageOf(df).get("id").exists(_._1 == Set("id")))
  }

  test("a CTE, referenced once, twice, nested, and inside a temp view") {
    assertEmailTraced(spark.sql("WITH c AS (SELECT id, email FROM o) SELECT id, upper(email) AS e2 FROM c"), "e2")
    val twice = spark.sql("WITH c AS (SELECT id, email FROM o) SELECT a.id, b.email AS e2 FROM c a JOIN c b ON a.id = b.id")
    assertEmailTraced(twice, "e2")
    assertEmailTraced(spark.sql("WITH c AS (SELECT id, email FROM o), d AS (SELECT id, email AS m FROM c) SELECT id, m FROM d"), "m")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW cte_view AS WITH c AS (SELECT id, email FROM o) SELECT id, email AS m FROM c")
    assertEmailTraced(spark.sql("SELECT id, m FROM cte_view"), "m")
  }

  test("a persistent view over a catalog table traces to the table's column") {
    spark.sql("CREATE OR REPLACE VIEW orders_v AS SELECT id, email AS mail FROM orders_t")
    assertEmailTraced(spark.table("orders_v"), "mail", inputLocation = "spark_catalog/default/orders_t")
  }

  test("a write with no select of its own - a copy, a filter, a sort and limit, a distinct, a union - traces every column") {
    val read = spark.read.parquet(orders)
    List(
      "copy" -> read,
      "filter" -> read.filter(col("id") > 1),
      "sort and limit" -> read.orderBy("id").limit(1),
      "distinct" -> read.distinct().repartition(2),
      "union" -> read.union(read)
    ).foreach { case (shape, df) =>
      val lineage = lineageOf(df)
      assert(lineage.keySet == Set("id", "value", "email"), s"$shape: $lineage")
      assert(lineage("email") == (Set("email"), Set("pii")), s"$shape: $lineage")
      assert(lineage("id")._1 == Set("id"), s"$shape: $lineage")
    }
  }

  test("a write that already ends in a select or an aggregation is translated exactly as before") {
    val read = spark.read.parquet(orders)
    List(read.select(col("id"), col("email")), read.groupBy("id").agg(sum("value").as("total"))).foreach { df =>
      val analyzed = df.queryExecution.analyzed
      assert(SparkPlanAdapter.translateAsWrite(analyzed, ir.DatasetRef("out")).plan == ir.Write(ir.DatasetRef("out"), SparkPlanAdapter.translate(analyzed).plan))
    }
  }

  test("a plain copy's write gets a projection of the query's own columns, in order") {
    val written = SparkPlanAdapter.translateAsWrite(spark.read.parquet(orders).queryExecution.analyzed, ir.DatasetRef("out")).plan
    written match {
      case ir.Write(_, ir.Project(_: ir.Read, columns), _, _, _) => assert(columns.map(_.name) == List("id", "value", "email"))
      case other                                                 => fail(ir.PlanPrinter.render(other))
    }
  }

  test("input checks through a view and a CTE are unchanged: a declared input read through one is read") {
    val out = dir.resolve("out").toString
    withContract(contract(orders), VerificationOptions(rejectUndeclaredInputs = true)) {
      spark.sql("SELECT id FROM o").write.mode("overwrite").parquet(out)
      spark.sql("WITH c AS (SELECT id FROM o) SELECT id FROM c").write.mode("overwrite").parquet(out)
    }
    val e = intercept[ContractViolationException](
      withContract(contract(dir.resolve("other").toString))(spark.sql("WITH c AS (SELECT id FROM o) SELECT id FROM c").write.mode("overwrite").parquet(out))
    )
    assert(e.result.violations.map(_.violationType).contains(ViolationType.MissingInput))
  }
}
