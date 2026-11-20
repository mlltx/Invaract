// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser
import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** The messages a newcomer meets, as a real job produces them (H-17 of docs/SPARK_ADAPTER_HARDENING_PLAN.md): the
  * advice each gives must be something the job's owner can do, and must be true. */
class NewcomerMessagesSpec extends AnyFunSuite {

  private def session(name: String, confs: (String, String)*)(install: SparkSession => (SparkSession => org.apache.spark.sql.catalyst.plans.logical.LogicalPlan => Unit)): SparkSession = {
    val builder = SparkSession.builder().master("local[1]").appName(name).config("spark.ui.enabled", "false")
      .withExtensions(e => e.injectCheckRule(s => install(s)(s)))
    val s = confs.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  private def causes(e: Throwable): List[Throwable] = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toList

  test("(a) an unrecognized statement's advice links the supported statements and the issue tracker, not Invaract's own code") {
    val dir = Files.createTempDirectory("invaract-messages-a")
    val contract = ContractParser.parse(
      s"""id: a
         |version: "1.0.0"
         |outputs:
         |  - { name: out, location: ${dir.resolve("out")}, schema: { fields: [ { name: id, type: long } ] } }
         |""".stripMargin
    )
    @volatile var armed = false
    val spark = session("NewcomerMessagesSpec-a") { _ =>
      val rule = ContractEnforcementRule.forContract(contract)
      s => { val r = rule(s); p => if (armed) r(p) }
    }
    try {
      spark.sql("CREATE TABLE loaded (id BIGINT) USING parquet")
      val source = dir.resolve("source").toString
      spark.range(1).write.parquet(source)
      armed = true
      val e = intercept[ContractViolationException](spark.sql(s"LOAD DATA LOCAL INPATH '$source' INTO TABLE loaded"))
      val v = e.result.violations.head
      assert(v.violationType == ViolationType.UnverifiableWrite)
      assert(v.remediation.contains(FailClosedCommands.ConnectorSupportPage) && v.remediation.contains(FailClosedCommands.IssuesPage))
      assert(!v.remediation.contains("FailClosedCommands"), v.remediation)
    } finally spark.stop()
  }

  test("(b) the nullability advice holds: filtering nulls out is still refused, coalesce passes") {
    val dir = Files.createTempDirectory("invaract-messages-b")
    val out = dir.resolve("out").toString
    val contract = ContractParser.parse(
      s"""id: b
         |version: "1.0.0"
         |outputs:
         |  - { name: out, location: $out, schema: { fields: [ { name: id, type: long, required: true, nullable: false } ] } }
         |""".stripMargin
    )
    @volatile var armed = false
    val spark = session("NewcomerMessagesSpec-b") { _ =>
      val rule = ContractEnforcementRule.forContract(contract)
      s => { val r = rule(s); p => if (armed) r(p) }
    }
    try {
      val src = dir.resolve("src").toString
      spark.sql("SELECT CAST(NULL AS BIGINT) AS id UNION ALL SELECT 1L AS id").write.parquet(src)
      armed = true
      val e = intercept[ContractViolationException](spark.read.parquet(src).filter(col("id").isNotNull).write.mode("overwrite").parquet(out))
      assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputFieldNullabilityMismatch))
      assert(e.result.violations.head.remediation.contains("coalesce(id, <default>)"))
      spark.read.parquet(src).select(coalesce(col("id"), lit(0L)).as("id")).write.mode("overwrite").parquet(out) // must not throw
    } finally spark.stop()
  }

  test("(d) a contract the organizational policy refuses: every query is refused, with a headline that says so") {
    val dir: Path = Files.createTempDirectory("invaract-messages-d")
    val policy = Files.write(
      dir.resolve("policy.yaml"),
      "version: \"1.0\"\npolicies:\n  - id: catalog-required\n    type: require_catalog\n    scope: outputs\n".getBytes("UTF-8")
    )
    val contract = ContractParser.parse(
      s"""id: d
         |version: "1.0.0"
         |outputs:
         |  - { name: out, location: ${dir.resolve("out")}, schema: { fields: [ { name: id, type: long } ] } }
         |""".stripMargin
    )
    val spark = session("NewcomerMessagesSpec-d", "spark.invaract.orgPolicy" -> policy.toString)(_ => ContractEnforcementRule.forContract(contract))
    try {
      val e = intercept[Throwable](spark.range(3).count())
      val refusal = causes(e).collectFirst { case c: ContractViolationException => c }.getOrElse(fail(s"expected a refusal, got $e"))
      assert(refusal.getMessage.startsWith("Contract 'd@1.0.0' does not meet the organizational policy, so Invaract will not run this job's queries."))
      assert(!refusal.getMessage.contains("Write aborted"))
    } finally spark.stop()
  }
}
