// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.ir
import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

import java.io.File
import java.nio.file.{Files, Path}

/** Every root path of a read is a read, and a read inside a declared input is a read of that input
  * (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-03), judged by real enforcement.
  */
class ReadLocationsEnforcementSpec extends ConnectorSpecBase {
  private var dir: Path = _
  private def in(name: String) = dir.resolve("in").resolve(name).toString
  private def orders = in("orders")
  private def orders2 = in("orders2")
  private def part = in("part")
  private def out = dir.resolve("out").toString

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-read-locations")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ReadLocationsEnforcementSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val s = spark
    import s.implicits._
    Seq((1L, 10L), (2L, 20L)).toDF("id", "value").write.parquet(orders)
    Seq((3L, 30L)).toDF("id", "value").write.parquet(orders2)
    Seq((1L, 10L)).toDF("id", "value").withColumn("dt", lit("2024-01-01")).write.partitionBy("dt").parquet(part)
    Seq((2L, 20L)).toDF("id", "value").withColumn("dt", lit("2024-01-02")).write.mode("append").partitionBy("dt").parquet(part)
  }

  private def input(name: String, location: String, fields: String = "{ name: id, type: long }") =
    s"  - { name: $name, location: $location, schema: { fields: [ $fields ] } }"

  private def contract(inputs: String*): String =
    s"""id: read_locations
       |version: "1.0.0"
       |inputs:
       |${inputs.mkString("\n")}
       |outputs:
       |  - { name: out, location: $out, schema: { fields: [ { name: id, type: long } ] } }
       |""".stripMargin

  private val strict = VerificationOptions(rejectUndeclaredInputs = true)
  private def write(df: DataFrame): Unit = df.select("id").write.mode("overwrite").parquet(out)
  private def violations(yaml: String, options: VerificationOptions = VerificationOptions())(df: => DataFrame): List[String] =
    try { withContract(yaml, options)(write(df)); Nil }
    catch { case e: ContractViolationException => e.result.violations.map(_.violationType) }

  test("a read of two paths reads both: the second, undeclared, is UNDECLARED_INPUT") {
    assert(violations(contract(input("orders", orders)), strict)(spark.read.parquet(orders, orders2)) == List(ViolationType.UndeclaredInput))
    assert(violations(contract(input("orders", orders)), strict)(spark.read.parquet(orders2, orders)) == List(ViolationType.UndeclaredInput), "whichever comes first")
  }

  test("a glob that matches two datasets reads both") {
    assert(violations(contract(input("orders", orders)), strict)(spark.read.parquet(in("orders*"))) == List(ViolationType.UndeclaredInput))
  }

  test("with both declared, a two-path read passes, and the second path's schema is checked") {
    assert(violations(contract(input("orders", orders), input("orders2", orders2)), strict)(spark.read.parquet(orders, orders2)) == Nil)
    val e = intercept[ContractViolationException](
      withContract(contract(input("orders", orders), input("orders2", orders2, "{ name: id, type: long }, { name: missing, type: long, required: true }")))(
        write(spark.read.parquet(orders, orders2))
      )
    )
    assert(e.result.violations.map(v => v.violationType -> v.location.exists(_.endsWith("/in/orders2"))) == List(ViolationType.MissingInputField -> true))
  }

  test("a dataset next to a declared one, whose name starts the same, is not inside it") {
    assert(violations(contract(input("orders", orders)), strict)(spark.read.parquet(orders2)) == List(ViolationType.MissingInput, ViolationType.UndeclaredInput))
  }

  test("a partition folder, a single file and a glob of files inside a declared input are reads of it") {
    val partitionFolder = s"$part/dt=2024-01-01"
    val oneFile = new File(orders).listFiles().map(_.getAbsolutePath).find(_.endsWith(".parquet")).get
    val cases = List(
      "partition folder" -> (contract(input("part", part)), spark.read.parquet(partitionFolder)),
      "partition folder with basePath" -> (contract(input("part", part)), spark.read.option("basePath", part).parquet(partitionFolder)),
      "one file" -> (contract(input("orders", orders)), spark.read.parquet(oneFile)),
      "glob of files" -> (contract(input("orders", orders)), spark.read.parquet(s"$orders/*.parquet"))
    )
    cases.foreach { case (shape, (yaml, df)) => assert(violations(yaml, strict)(df) == Nil, shape) }
  }

  test("an input declared as a partition folder keeps working as before") {
    assert(violations(contract(input("day", s"$part/dt=2024-01-01")), strict)(spark.read.parquet(s"$part/dt=2024-01-01")) == Nil)
  }

  test("outputs are not affected: a write into a sub-folder of the declared output is still elsewhere") {
    val e = intercept[ContractViolationException](
      withContract(contract(input("orders", orders)))(spark.read.parquet(orders).select("id").write.mode("overwrite").parquet(s"$out/sub"))
    )
    assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
  }

  test("an aliased two-path read through a temp view keeps its lineage to the first location") {
    spark.read.parquet(orders, orders2).createOrReplaceTempView("both")
    val plan = SparkPlanAdapter.translateAsWrite(spark.sql("SELECT b.id AS i FROM both b").queryExecution.analyzed, ir.DatasetRef("x")).plan
    val lineage = ir.Lineage.trace(plan)
    assert(lineage.map(_.output.name) == List("i"))
    assert(lineage.head.sources.map(_.name) == Set("id"))
    val reads = com.invaract.verification.StructuralVerifier.collectReads(plan)
    assert(reads.map(_.dataset.location.split('/').last) == List("orders", "orders2"))
    assert(reads.head.alias.isDefined && reads(1).alias.isEmpty, "the alias names the read the columns come from")
  }

  test("dry-run drafts a partitioned input at the dataset, so the draft holds for the next day's partition") {
    capturedPlans.clear()
    spark.read.parquet(s"$part/dt=2024-01-01").select("id").write.mode("overwrite").parquet(out)
    val writePlan = capturedPlans.find(WriteCommandSupport.combined.isDefinedAt).get
    var drafted: Option[Contract] = None
    ContractEnforcementRule.inferOrIgnore(writePlan, c => drafted = Some(c))
    val draft = drafted.getOrElse(fail("no draft"))
    assert(draft.inputs.map(_.location) == List(part.replace('\\', '/')))
    withContract(ContractParser.write(draft), strict)(spark.read.parquet(s"$part/dt=2024-01-02").select("id").write.mode("overwrite").parquet(out))
  }
}
