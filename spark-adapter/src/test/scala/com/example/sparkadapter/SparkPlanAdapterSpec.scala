// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.ir._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.nio.file.{Files, Path}

class SparkPlanAdapterSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var inputCsv: Path = _
  private var outputDir: Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("SparkPlanAdapterSpec").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    outputDir = Files.createTempDirectory("invariant-spark-adapter-test")
    inputCsv = outputDir.resolve("sample.csv")
    Files.write(
      inputCsv,
      "id,value\n1,10\n2,20\n3,30\n4,40\n".getBytes("UTF-8")
    )
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def readSample() =
    spark.read.option("header", "true").option("inferSchema", "true").csv(inputCsv.toString)

  test("translates a bare Read from a CSV relation") {
    val df = readSample()
    val result = SparkPlanAdapter.translate(df.queryExecution.analyzed)

    result.plan match {
      case Read(DatasetRef(location), None) => assert(location.contains("sample.csv"))
      case other                            => fail(s"expected a bare Read, got $other")
    }
    assert(result.diagnostics.isEmpty)
  }

  test("translates the worked example: GROUP BY with a passthrough column and a SUM aggregate") {
    val df = readSample()
    val agg = df.groupBy("id").agg(sum("value").as("lifetime_value"))

    val result = SparkPlanAdapter.translateAsWrite(agg.queryExecution.analyzed, DatasetRef("gold.customer_orders"))

    val lineage = Lineage.trace(result.plan)
    val idLineage = lineage.find(_.output.name == "id").get
    assert(!idLineage.aggregated)
    assert(idLineage.sources.exists(_.name == "id"))

    val lifetimeValue = lineage.find(_.output.name == "lifetime_value").get
    assert(lifetimeValue.aggregated)
    assert(lifetimeValue.sources.exists(_.name == "value"))

    result.plan match {
      case Write(DatasetRef("gold.customer_orders"), Aggregate(Read(_, None), List(ColumnReference(_)), aggregates)) =>
        assert(aggregates.map(_.name) == List("id", "lifetime_value"))
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  test("translates Filter and Cast, preserving the target type as a CAST(...) function call") {
    val df = readSample()
    val filtered = df.filter(col("value") > 20).withColumn("value_d", col("value").cast("double"))

    val result = SparkPlanAdapter.translate(filtered.queryExecution.analyzed)

    result.plan match {
      case Project(Filter(_, condition), columns) =>
        assert(condition.references.exists(_.name == "value"))
        val castExpr = columns.find(_.name == "value_d").get.expr
        castExpr match {
          case FunctionCall("CAST", List(ColumnReference(ColumnRef("value", _)), Literal("double", "type"))) => // expected
          case other => fail(s"unexpected cast translation: $other")
        }
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty)
  }

  test("translates a self-join, preserving each side's alias for lineage disambiguation") {
    val left = readSample().as("cur")
    val right = readSample().as("arch")
    val joined = left.join(right, left("id") === right("id"), "inner").select(left("value").as("cur_value"))

    val result = SparkPlanAdapter.translate(joined.queryExecution.analyzed)

    result.plan match {
      case Project(Join(Read(_, Some("cur")), Read(_, Some("arch")), JoinType.Inner, Some(_)), _) => // expected
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  test("translates Union, one branch per input DataFrame") {
    val a = readSample().select("id", "value")
    val b = readSample().select("id", "value")
    val union = a.union(b)

    val result = SparkPlanAdapter.translate(union.queryExecution.analyzed)

    result.plan match {
      case Union(children) => assert(children.size == 2)
      case other            => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  test("translates a windowed aggregate, hoisting partition/order to the plan node") {
    val df = readSample()
    val spec = Window.partitionBy("id").orderBy("value")
    val windowed = df.withColumn("running_total", sum("value").over(spec))

    val result = SparkPlanAdapter.translate(windowed.queryExecution.analyzed)

    // Spark's analyzer wraps the Window node in one or more outer Projects
    // that just reselect [id, value, running_total] — real structure, not
    // an adapter artifact, so the test searches for it rather than
    // asserting an exact nesting depth that's an analyzer implementation
    // detail.
    def findWindow(plan: Plan): Option[com.example.ir.Window] = plan match {
      case w: com.example.ir.Window => Some(w)
      case other                     => other.children.flatMap(findWindow).headOption
    }

    val window = findWindow(result.plan).getOrElse(fail(s"no Window node found in ${PlanPrinter.render(result.plan)}"))
    assert(window.partitionBy.nonEmpty)
    assert(window.orderBy.nonEmpty)
    val runningTotal = window.windowExprs.find(_.name == "running_total").get.expr
    assert(runningTotal.isInstanceOf[AggregateCall])
  }

  test("flags a UDF with a diagnostic but still produces a best-effort translation") {
    spark.udf.register("triple", (x: Int) => x * 3)
    val df = readSample()
    val withUdf = df.selectExpr("id", "triple(value) as tripled")

    val result = SparkPlanAdapter.translate(withUdf.queryExecution.analyzed)

    assert(result.diagnostics.exists(_.nodeType.contains("UDF")))
    result.plan match {
      case Project(_, columns) =>
        assert(columns.exists(_.name == "tripled"))
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  test("falls back to Unsupported with a diagnostic for a plan node with no translation, instead of throwing") {
    val df = readSample()
    val exploded = df.select(col("id"), explode(array(col("value"), col("value"))).as("v"))

    val result = SparkPlanAdapter.translate(exploded.queryExecution.analyzed)

    assert(result.diagnostics.nonEmpty)
    val hasUnsupportedNode = {
      def contains(plan: Plan): Boolean = plan match {
        case _: Unsupported => true
        case other           => other.children.exists(contains)
      }
      contains(result.plan)
    }
    assert(hasUnsupportedNode, s"expected an Unsupported node somewhere in ${PlanPrinter.render(result.plan)}")
  }

  test("end to end: a real write via spark-submit-style DataFrame.write is captured through SparkAdapterListener") {
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    try {
      val df = readSample()
      val agg = df.groupBy("id").agg(sum("value").as("lifetime_value"))
      val outputPath = outputDir.resolve("customer_orders.parquet").toString
      agg.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // QueryExecutionListener callbacks run on Spark's own dedicated
      // listener thread, asynchronously with respect to the action that
      // triggered them, so the result may not be visible immediately.
      val result = eventually(timeout(Span(5, Seconds))) {
        listener.lastWrite.getOrElse(fail("listener has not captured a write yet"))
      }
      result.plan match {
        case Write(DatasetRef(location), Aggregate(Read(_, None), _, aggregates)) =>
          assert(location.contains("customer_orders.parquet"))
          assert(aggregates.map(_.name) == List("id", "lifetime_value"))
        case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
      }

      val lineage = Lineage.trace(result.plan)
      assert(lineage.find(_.output.name == "lifetime_value").get.aggregated)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
