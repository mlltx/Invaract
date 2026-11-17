// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir
import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import com.invaract.verification.notification.{ContractValidationEvent, TestNotificationSink}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Path}

/** A read inside an `IN`, `EXISTS` or scalar subquery is a read like any other (docs/SPARK_ADAPTER_HARDENING_PLAN.md,
  * H-02). A subquery's plan hangs off an expression rather than the plan's children, so it used to be invisible to
  * `rejectUndeclaredInputs`, to the plan-shape rules, to lineage and to the fingerprint.
  */
class SubqueryReadsSpec extends ConnectorSpecBase {
  private var dir: Path = _
  private def orders = dir.resolve("orders").toString
  private def sec = dir.resolve("sec").toString
  private def other = dir.resolve("other").toString
  private def out = dir.resolve("out").toString

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-subquery-reads")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SubqueryReadsSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val s = spark
    import s.implicits._
    Seq((1L, 10L), (2L, 20L)).toDF("id", "value").write.parquet(orders)
    Seq((1L, 5L), (3L, 7L)).toDF("id", "amount").write.parquet(sec)
    Seq((1L, 5L)).toDF("id", "amount").write.parquet(other)
    spark.read.parquet(orders).createOrReplaceTempView("o")
    spark.read.parquet(sec).createOrReplaceTempView("sec")
    spark.read.parquet(other).createOrReplaceTempView("other")
  }

  private def contract(inputs: Seq[String], rules: String = ""): String =
    s"""id: subqueries
       |version: "1.0.0"
       |inputs:
       |${inputs.zipWithIndex.map { case (loc, i) => s"  - { name: in$i, location: $loc, schema: { fields: [ { name: id, type: long } ] } }" }.mkString("\n")}
       |outputs:
       |  - name: out
       |    location: $out
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |$rules
       |""".stripMargin

  private val strict = VerificationOptions(rejectUndeclaredInputs = true)

  private val readingSecInASubquery = List(
    "scalar" -> "SELECT id, (SELECT max(id) FROM sec) AS m FROM o",
    "IN" -> "SELECT id, value FROM o WHERE id IN (SELECT id FROM sec)",
    "EXISTS" -> "SELECT id, value FROM o WHERE EXISTS (SELECT 1 FROM sec WHERE sec.id = o.id)"
  )

  private def write(sql: String): Unit = spark.sql(sql).write.mode("overwrite").parquet(out)

  test("an input read only inside a subquery is UNDECLARED_INPUT under rejectUndeclaredInputs, and nothing is written") {
    readingSecInASubquery.foreach { case (kind, sql) =>
      val e = intercept[ContractViolationException](withContract(contract(Seq(orders)), strict)(write(sql)))
      assert(e.result.violations.map(_.violationType) == List(ViolationType.UndeclaredInput), kind)
      assert(e.getMessage.contains("/sec"), kind)
      assert(!Files.exists(dir.resolve("out")), kind)
    }
  }

  test("the same queries pass when the subquery's input is declared too") {
    readingSecInASubquery.foreach { case (_, sql) =>
      withContract(contract(Seq(orders, sec)), strict)(write(sql)) // must not throw
    }
  }

  test("a declared input read only inside a subquery is read: no MISSING_INPUT") {
    withContract(contract(Seq(orders)))(write("SELECT 1L AS id, (SELECT max(value) FROM o) AS m"))
  }

  test("a subquery inside a subquery is seen too") {
    val sql = "SELECT id FROM o WHERE id IN (SELECT id FROM other WHERE id IN (SELECT id FROM sec))"
    val e = intercept[ContractViolationException](withContract(contract(Seq(orders, other)), strict)(write(sql)))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UndeclaredInput))
    assert(e.getMessage.contains("/sec"))
  }

  test("a subquery anywhere a query can hold one: SELECT list, WHERE, HAVING, an aggregate, a join condition") {
    val shapes = List(
      "select list" -> "SELECT id, (SELECT max(id) FROM sec) AS m FROM o",
      "where" -> "SELECT id FROM o WHERE id IN (SELECT id FROM sec)",
      "having" -> "SELECT id, sum(value) AS t FROM o GROUP BY id HAVING sum(value) > (SELECT min(amount) FROM sec)",
      "aggregate" -> "SELECT id, sum(value) + (SELECT max(amount) FROM sec) AS t FROM o GROUP BY id",
      "join condition" -> "SELECT a.id FROM o a JOIN o b ON a.id = b.id AND a.id IN (SELECT id FROM sec)"
    )
    shapes.foreach { case (where, sql) =>
      val reads = com.invaract.verification.StructuralVerifier.collectReads(SparkPlanAdapter.translate(spark.sql(sql).queryExecution.analyzed).plan)
      assert(reads.exists(_.dataset.location.endsWith("/sec")), s"$where: ${reads.map(_.dataset.location)}")
    }
  }

  test("a plan with no subquery has no extra join") {
    val rendered = ir.PlanPrinter.render(SparkPlanAdapter.translate(spark.sql("SELECT id FROM o WHERE id > 1").queryExecution.analyzed).plan)
    assert(!rendered.contains("LeftSemi") && !rendered.contains("subquery#"), rendered)
  }

  test("an IN subquery becomes a semi join on true whose right side reads the subquery's table") {
    val translated = SparkPlanAdapter.translate(spark.sql("SELECT id FROM o WHERE id IN (SELECT id FROM sec)").queryExecution.analyzed).plan
    val joins = com.invaract.verification.PlanFacts.of(translated).joins
    assert(joins.size == 1)
    val join = joins.head
    assert(join.joinType == ir.JoinType.LeftSemi && join.condition.contains(ir.Literal(true, "boolean")))
    assert(com.invaract.verification.StructuralVerifier.collectReads(join.right).map(_.dataset.location).exists(_.endsWith("/sec")))
  }

  test("forbid_cross_join: a subquery's join is a join like any other, and the subquery's own attachment is not a cross join") {
    val rules = "rules:\n  - type: forbid_cross_join"
    withContract(contract(Seq(orders, sec), rules))(write("SELECT id FROM o WHERE id IN (SELECT id FROM sec)"))
    val e = intercept[ContractViolationException](
      withContract(contract(Seq(orders, sec, other), rules))(write("SELECT id FROM o WHERE id IN (SELECT s.id FROM sec s CROSS JOIN other x)"))
    )
    assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleCrossJoinViolation))
  }

  test("a scalar subquery's value traces to the subquery's column, and no outer column is traced into the subquery") {
    val plan = SparkPlanAdapter.translateAsWrite(
      spark.sql("SELECT id, (SELECT max(amount) FROM sec) AS m FROM o WHERE id IN (SELECT id FROM sec)").queryExecution.analyzed,
      ir.DatasetRef("x")
    ).plan
    val lineage = ir.Lineage.trace(plan).map(l => l.output.name -> l.sources).toMap
    assert(lineage("m").map(_.name) == Set("amount"), lineage)
    assert(lineage("id").map(_.name) == Set("id"), lineage)
    assert(lineage("id").forall(_.qualifier.forall(q => !q.contains("sec"))), s"id must come from o only: $lineage")
  }

  test("two jobs that differ only in the subquery's table fingerprint differently") {
    def fingerprintOf(sql: String, subqueryInput: String): String = {
      val sink = new TestNotificationSink
      withSink(sink)(withContract(contract(Seq(orders, subqueryInput)), VerificationOptions(computeFingerprint = true))(write(sql)))
      sink.events.collect { case e: ContractValidationEvent => e }.last.fingerprints.map(_.overall.value).getOrElse(fail("no fingerprint"))
    }
    val withSec = fingerprintOf("SELECT id FROM o WHERE id IN (SELECT id FROM sec)", sec)
    assert(withSec != fingerprintOf("SELECT id FROM o WHERE id IN (SELECT id FROM other)", other))
    assert(withSec == fingerprintOf("SELECT id FROM o WHERE id IN (SELECT id FROM sec)", sec), "and the same job fingerprints the same")
  }
}
