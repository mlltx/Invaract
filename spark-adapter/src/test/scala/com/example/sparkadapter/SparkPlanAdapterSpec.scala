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
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkPlanAdapterSpec")
      // Delta's session extension/catalog only activate for `.format("delta")`
      // usage - confirmed harmless to every other test in this suite by the
      // full suite still passing with this enabled (see docs/SPARK_ADAPTER.md's
      // "Delta Lake support" section for how this was investigated).
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
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
      case Write(DatasetRef("gold.customer_orders"), Aggregate(Read(_, None), List(ColumnReference(_)), aggregates), format, saveMode) =>
        assert(aggregates.map(_.name) == List("id", "lifetime_value"))
        // translateAsWrite wraps a bare (never actually written) plan as a
        // Write for convenience — there's no real format or save mode to
        // report.
        assert(format.isEmpty)
        assert(saveMode.isEmpty)
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

  test("translates Sort, capturing direction and null ordering") {
    val df = readSample()
    val sorted = df.orderBy(col("value").desc_nulls_last)

    val result = SparkPlanAdapter.translate(sorted.queryExecution.analyzed)

    // As with the windowed-aggregate test above, the analyzer may wrap the
    // Sort in an outer Project — search for it rather than assuming exact
    // nesting depth.
    def findSort(plan: Plan): Option[Sort] = plan match {
      case s: Sort => Some(s)
      case other    => other.children.flatMap(findSort).headOption
    }

    val sort = findSort(result.plan).getOrElse(fail(s"no Sort node found in ${PlanPrinter.render(result.plan)}"))
    sort.order match {
      case List(SortOrder(ColumnReference(ColumnRef("value", _)), ascending, nullsFirst)) =>
        assert(!ascending, "desc_nulls_last should translate to ascending = false")
        assert(!nullsFirst, "desc_nulls_last should translate to nullsFirst = false")
      case other => fail(s"unexpected sort order: $other")
    }
  }

  test("translates every join type Spark supports") {
    val left = readSample().as("l")
    val right = readSample().as("r")

    def findJoin(plan: Plan): Option[Join] = plan match {
      case j: Join => Some(j)
      case other    => other.children.flatMap(findJoin).headOption
    }

    val namedJoins = Seq(
      "left_outer"  -> JoinType.LeftOuter,
      "right_outer" -> JoinType.RightOuter,
      "full_outer"  -> JoinType.FullOuter,
      "leftsemi"    -> JoinType.LeftSemi,
      "leftanti"    -> JoinType.LeftAnti
    )
    namedJoins.foreach { case (sparkName, expected) =>
      val joined = left.join(right, left("id") === right("id"), sparkName)
      val result = SparkPlanAdapter.translate(joined.queryExecution.analyzed)
      val join = findJoin(result.plan).getOrElse(fail(s"no Join node found for '$sparkName' in ${PlanPrinter.render(result.plan)}"))
      assert(join.joinType == expected, s"expected $expected for '$sparkName', got ${join.joinType}")
    }

    // Cross join has its own DataFrame method (no join condition).
    val crossed = left.crossJoin(right)
    val crossResult = SparkPlanAdapter.translate(crossed.queryExecution.analyzed)
    val crossJoin = findJoin(crossResult.plan).getOrElse(fail(s"no Join node found for cross join in ${PlanPrinter.render(crossResult.plan)}"))
    assert(crossJoin.joinType == JoinType.Cross)
  }

  test("translates a multi-way join chain, recursing through nested Join nodes") {
    val a = readSample().as("a")
    val b = readSample().as("b")
    val c = readSample().as("c")
    val chained = a.join(b, a("id") === b("id")).join(c, a("id") === c("id"))

    val result = SparkPlanAdapter.translate(chained.queryExecution.analyzed)

    def countJoins(plan: Plan): Int = plan match {
      case j: Join => 1 + countJoins(j.left) + countJoins(j.right)
      case other     => other.children.map(countJoins).sum
    }
    assert(countJoins(result.plan) == 2, s"expected 2 Join nodes in ${PlanPrinter.render(result.plan)}")
  }

  test("translates COUNT, AVG, MIN, MAX, and COUNT(DISTINCT ...) aggregates") {
    val df = readSample()
    val agg = df.groupBy("id").agg(
      count("value").as("cnt"),
      countDistinct("value").as("cnt_distinct"),
      avg("value").as("avg_value"),
      min("value").as("min_value"),
      max("value").as("max_value")
    )

    val result = SparkPlanAdapter.translate(agg.queryExecution.analyzed)

    result.plan match {
      case Aggregate(_, _, aggregates) =>
        val byName = aggregates.map(a => a.name -> a.expr).toMap
        assert(byName("cnt").isInstanceOf[AggregateCall])
        assert(byName("cnt").asInstanceOf[AggregateCall].function == "COUNT")
        assert(!byName("cnt").asInstanceOf[AggregateCall].distinct)
        assert(byName("cnt_distinct").asInstanceOf[AggregateCall].function == "COUNT")
        assert(byName("cnt_distinct").asInstanceOf[AggregateCall].distinct)
        assert(byName("avg_value").asInstanceOf[AggregateCall].function == "AVG")
        assert(byName("min_value").asInstanceOf[AggregateCall].function == "MIN")
        assert(byName("max_value").asInstanceOf[AggregateCall].function == "MAX")
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  test("wraps a multi-argument aggregate (corr) in an ARGS(...) function call with a diagnostic") {
    val df = readSample()
    val agg = df.groupBy("id").agg(corr(col("value"), col("value")).as("correlation"))

    val result = SparkPlanAdapter.translate(agg.queryExecution.analyzed)

    result.plan match {
      case Aggregate(_, _, aggregates) =>
        aggregates.find(_.name == "correlation").get.expr match {
          case AggregateCall(_, FunctionCall("ARGS", args), _) => assert(args.size == 2)
          case other                                            => fail(s"unexpected multi-arg aggregate translation: $other")
        }
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.nonEmpty, "a multi-argument aggregate should be flagged, not silently narrowed")
  }

  test("translates CASE WHEN and IS NULL via the generic expression fallback, with no diagnostic needed") {
    val df = readSample()
    // A single select(), not chained withColumn() calls: chaining nests a
    // second Project atop the first, and the outer one just re-references
    // "bucket" as a plain column — the computation itself lives one level
    // down, which the test isn't asserting about here.
    val withCase = df.select(
      col("id"),
      col("value"),
      when(col("value") > 20, lit("high")).otherwise(lit("low")).as("bucket"),
      col("value").isNull.as("value_is_null")
    )

    val result = SparkPlanAdapter.translate(withCase.queryExecution.analyzed)

    result.plan match {
      case Project(_, columns) =>
        val bucket = columns.find(_.name == "bucket").get.expr
        assert(bucket.isInstanceOf[FunctionCall], s"expected CASE WHEN to fall through to a generic FunctionCall, got $bucket")

        columns.find(_.name == "value_is_null").get.expr match {
          case FunctionCall("ISNULL", List(ColumnReference(ColumnRef("value", _)))) => // expected
          case other                                                                  => fail(s"unexpected IS NULL translation: $other")
        }
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty, "the generic fallback is not opaque — it shouldn't need a diagnostic")
  }

  test("translates .limit(n) as a transparent pass-through, preserving the underlying plan") {
    val df = readSample()
    val limited = df.limit(2)

    val result = SparkPlanAdapter.translate(limited.queryExecution.analyzed)

    result.plan match {
      case Read(DatasetRef(location), None) => assert(location.contains("sample.csv"))
      case other                            => fail(s"expected Limit to be transparent to translation, got ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty)
  }

  test("translates .distinct() as a transparent pass-through (Deduplicate), preserving the underlying plan") {
    val df = readSample()
    val deduped = df.select("id", "value").distinct()

    val result = SparkPlanAdapter.translate(deduped.queryExecution.analyzed)

    result.plan match {
      case Project(Read(DatasetRef(location), None), _) => assert(location.contains("sample.csv"))
      case other                                          => fail(s"expected Distinct to be transparent to translation, got ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty)
  }

  test("translates .repartition(n), .coalesce(n), and .repartition(col) as transparent pass-throughs") {
    val df = readSample()

    def assertTransparent(transformed: org.apache.spark.sql.DataFrame, label: String): Unit = {
      val result = SparkPlanAdapter.translate(transformed.queryExecution.analyzed)
      result.plan match {
        case Read(DatasetRef(location), None) => assert(location.contains("sample.csv"), s"$label: unexpected location")
        case other                            => fail(s"$label: expected a transparent pass-through, got ${PlanPrinter.render(other)}")
      }
      assert(result.diagnostics.isEmpty, s"$label: should not need a diagnostic")
    }

    assertTransparent(df.repartition(4), "repartition(n)")
    assertTransparent(df.coalesce(1), "coalesce(n)")
    assertTransparent(df.repartition(col("id")), "repartition(col)")
  }

  test("reads translate the same way regardless of source file format: CSV, JSON, and Parquet") {
    val jsonPath = outputDir.resolve("sample.json")
    Files.write(
      jsonPath,
      "{\"id\":1,\"value\":10}\n{\"id\":2,\"value\":20}\n".getBytes("UTF-8")
    )
    val parquetPath = outputDir.resolve("sample_read_format_test.parquet").toString
    readSample().write.mode(SaveMode.Overwrite).parquet(parquetPath)

    val results = Seq(
      "csv"     -> SparkPlanAdapter.translate(readSample().queryExecution.analyzed),
      "json"    -> SparkPlanAdapter.translate(spark.read.json(jsonPath.toString).queryExecution.analyzed),
      "parquet" -> SparkPlanAdapter.translate(spark.read.parquet(parquetPath).queryExecution.analyzed)
    )

    results.foreach { case (format, result) =>
      result.plan match {
        case Read(DatasetRef(location), None) => assert(location.nonEmpty, s"$format: expected a non-empty location")
        case other                            => fail(s"$format: expected a bare Read regardless of source format, got ${PlanPrinter.render(other)}")
      }
      assert(result.diagnostics.isEmpty, s"$format: a plain relation read should not need a diagnostic")
    }

    val jsonLocation = results.find(_._1 == "json").get._2.plan.asInstanceOf[Read].dataset.location
    val parquetLocation = results.find(_._1 == "parquet").get._2.plan.asInstanceOf[Read].dataset.location
    assert(jsonLocation.contains("sample.json"))
    assert(parquetLocation.contains("sample_read_format_test.parquet"))
  }

  // Previously a KNOWN GAP (see ROADMAP.md Phase 1c): a JDBCRelation fell
  // through to locationOf's generic catalogTable/.toString fallback (the
  // same lower-fidelity path a HadoopFsRelation only takes when it can't
  // determine a root path), and translatePlan's LogicalRelation case
  // treated that fallback as diagnostic-worthy for every JDBC read, even
  // though JDBCRelation always carries a precise url/table identity of its
  // own. locationOf now special-cases JDBCRelation directly.
  test("translates a JDBC read with a precise location, not the generic relation fallback") {
    val jdbcUrl = s"jdbc:h2:mem:invariant_test_${System.nanoTime()};DB_CLOSE_DELAY=-1"
    val conn = java.sql.DriverManager.getConnection(jdbcUrl)
    try {
      val stmt = conn.createStatement()
      stmt.execute("CREATE TABLE orders(id INT, amount INT)")
      stmt.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
      stmt.close()

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "orders")
        .option("driver", "org.h2.Driver")
        .load()

      val result = SparkPlanAdapter.translate(df.queryExecution.analyzed)

      result.plan match {
        case Read(DatasetRef(location), None) =>
          assert(location.contains(jdbcUrl), s"expected the JDBC url in the location, got '$location'")
          assert(location.contains("ORDERS") || location.contains("orders"), s"expected the table name in the location, got '$location'")
        case other => fail(s"expected a bare Read, got ${PlanPrinter.render(other)}")
      }
      // The whole point of the fix: a JDBC read is precise enough that it
      // shouldn't be flagged as having used the generic fallback.
      assert(result.diagnostics.isEmpty, s"a JDBC read should not need a fallback diagnostic: ${result.diagnostics}")
    } finally {
      conn.close()
    }
  }

  // Investigated empirically against a real Delta-enabled session before
  // writing this (see docs/SPARK_ADAPTER.md's "Delta Lake support"
  // section): a `.format("delta").save(path)` write does NOT go through
  // InsertIntoHadoopFsRelationCommand like Parquet/CSV/JSON above - it
  // analyzes to SaveIntoDataSourceCommand instead, the same generic
  // command any CreatableRelationProvider-based `.save(...)` write uses.
  // Before the SaveIntoDataSourceCommand case was added, this fell
  // through to Unsupported - which, per ContractEnforcementRule's "only
  // ir.Write is checked" design, meant a Delta write passed through
  // completely unverified. This test is the translation half of closing
  // that gap; ContractEnforcementRuleSpec's Delta tests are the
  // enforcement half.
  test("translates a Delta write via SaveIntoDataSourceCommand, not falling through to Unsupported") {
    val outputPath = outputDir.resolve("delta_write_test").toString
    val df = readSample()

    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df.write.format("delta").mode(SaveMode.Overwrite).save(outputPath)

    val result = eventually(timeout(Span(5, Seconds))) {
      listener.lastWrite.getOrElse(fail("listener has not captured the Delta write yet"))
    }

    result.plan match {
      case Write(DatasetRef(location), Read(_, None), format, saveMode) =>
        assert(location.contains(outputPath), s"expected the Delta table path in the location, got '$location'")
        assert(format.contains("delta"), s"expected format 'delta' via DataSourceRegister.shortName(), got $format")
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write over a bare Read, got ${PlanPrinter.render(other)}")
    }
    // Confirms this is a real, precise translation, not a best-effort
    // fallback that merely happens to produce a Write node.
    assert(result.diagnostics.isEmpty, s"a Delta write with a 'path' option should not need a fallback diagnostic: ${result.diagnostics}")
  }

  // Investigated empirically (see docs/SPARK_ADAPTER.md's "Fail-closed on
  // unverifiable writes" section): `.saveAsTable(...)` against a *new*
  // table analyzes to CreateDataSourceTableAsSelectCommand, a third write
  // shape distinct from both InsertIntoHadoopFsRelationCommand (existing
  // table) and SaveIntoDataSourceCommand (Delta/JDBC/... `.save()`).
  // `table.provider` gives the format directly - no DataSourceRegister
  // lookup needed here, unlike the other two cases.
  test("translates a .saveAsTable() write via CreateDataSourceTableAsSelectCommand, not falling through to Unsupported") {
    val outputPath = outputDir.resolve("save_as_table_test").toString
    val df = readSample()

    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    df.write.option("path", outputPath).mode(SaveMode.Overwrite).saveAsTable("spark_plan_adapter_save_as_table_test")

    val result = eventually(timeout(Span(5, Seconds))) {
      listener.lastWrite.getOrElse(fail("listener has not captured the .saveAsTable() write yet"))
    }

    result.plan match {
      case Write(DatasetRef(location), Read(_, None), format, saveMode) =>
        // The filename only, not the full native outputPath: Spark
        // normalizes a catalog table's storage location into a
        // forward-slash file: URI regardless of platform, so on Windows
        // outputPath's backslashes would never appear in it verbatim
        // (same convention every other location assertion in this file
        // uses, e.g. `location.contains("sample.csv")`).
        assert(location.contains("save_as_table_test"), s"expected the table's path in the location, got '$location'")
        assert(format.contains("parquet"), s"expected the default 'parquet' format via table.provider, got $format")
        assert(saveMode.contains("overwrite"))
      case other => fail(s"expected a Write over a bare Read, got ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty, s"a .saveAsTable() write with an explicit path should not need a fallback diagnostic: ${result.diagnostics}")
  }

  // Investigated empirically before writing this (see docs/SPARK_ADAPTER.md's
  // "Delta Lake reads" section): unlike the three write shapes above, Delta
  // reads needed no new translatePlan case and no location-precision fix.
  // The relation Delta hands back for both `.load(path)` and a catalog table
  // reference is `org.apache.spark.sql.delta.DeltaLog$$anon$2` - an
  // anonymous subclass of Spark's own `HadoopFsRelation`, not a distinct
  // relation type - so the existing `case h: HadoopFsRelation =>` branches
  // in `locationOf`/`translatePlan` already match it via ordinary subtyping
  // and already extract the precise physical path. This test proves that
  // through the real translation path, not just by inspecting the relation
  // class - a precise `ir.Read` with no fallback diagnostic either way.
  test("translates a Delta read (.load(path) and a catalog table reference) with a precise location, no new case needed") {
    val df = readSample()
    val path = outputDir.resolve("delta_read_test").toString
    df.write.format("delta").mode(SaveMode.Overwrite).save(path)

    val loadResult = SparkPlanAdapter.translate(spark.read.format("delta").load(path).queryExecution.analyzed)
    loadResult.plan match {
      case Read(DatasetRef(location), None) =>
        // Filename only, not the full native path - see the .saveAsTable()
        // test above for why (Windows path-separator mismatch against
        // Spark's normalized file: URIs).
        assert(location.contains("delta_read_test"), s"expected the Delta table's physical path in the location, got '$location'")
      case other => fail(s"expected a bare Read, got ${PlanPrinter.render(other)}")
    }
    assert(loadResult.diagnostics.isEmpty, s".load(path) should resolve via the HadoopFsRelation branch, no fallback: ${loadResult.diagnostics}")

    // Forward slashes only when building a LOCATION clause: on Windows,
    // path's native backslashes collide with SQL string-literal escaping
    // when interpolated directly (confirmed by a real CI failure - see
    // ContractEnforcementRuleSpec's MERGE INTO fail-closed test for the
    // same fix). Spark/Hadoop accept forward-slash paths on Windows too.
    spark.sql(s"CREATE TABLE IF NOT EXISTS spark_plan_adapter_delta_read_tbl USING delta LOCATION '${path.replace('\\', '/')}'")
    val catalogResult = SparkPlanAdapter.translate(spark.table("spark_plan_adapter_delta_read_tbl").queryExecution.analyzed)
    catalogResult.plan match {
      case Read(DatasetRef(location), Some("spark_plan_adapter_delta_read_tbl")) =>
        assert(location.contains("delta_read_test"), s"expected the same physical path via catalogTable.storage, got '$location'")
      case other => fail(s"expected an aliased Read, got ${PlanPrinter.render(other)}")
    }
    assert(catalogResult.diagnostics.isEmpty, s"a catalog table reference should resolve via the same HadoopFsRelation branch, no fallback: ${catalogResult.diagnostics}")
  }

  // Closes the streaming-read-as-input coverage-ledger gap: neither
  // StreamingRelation (the legacy V1 path Delta itself uses) nor
  // StreamingRelationV2 (the modern DataSourceV2 path - rate, Kafka, ...)
  // was previously a recognized read shape, so a contract declaring a
  // streaming source as a required input always reported MISSING_INPUT.
  // A path-based Delta streaming source and a path-less rate one exercise
  // both branches of each case's fallback-diagnostic condition - neither
  // was reachable through this suite's other Delta streaming tests
  // (which chain a `rate` source into a Delta *sink*, wrapped in typed
  // encoding nodes this translator doesn't descend through), confirmed
  // by a real mutation-testing run finding both conditions uncovered.
  test("translates a streaming Delta read (.readStream.format(\"delta\").load(path)) with a precise location, no fallback needed") {
    val path = outputDir.resolve("streaming_read_translation_test").toString
    readSample().write.format("delta").mode(SaveMode.Overwrite).save(path)

    val result = SparkPlanAdapter.translate(spark.readStream.format("delta").load(path).queryExecution.analyzed)
    result.plan match {
      case Read(DatasetRef(location), None) =>
        assert(location.contains("streaming_read_translation_test"), s"expected the Delta source's physical path in the location, got '$location'")
      case other => fail(s"expected a bare Read, got ${PlanPrinter.render(other)}")
    }
    assert(result.diagnostics.isEmpty, s"a path-based streaming Delta read shouldn't need a fallback diagnostic: ${result.diagnostics}")
  }

  test("translates a streaming rate read (no path option) via the fallback branch, with a diagnostic naming the source") {
    val result = SparkPlanAdapter.translate(spark.readStream.format("rate").load().queryExecution.analyzed)
    result.plan match {
      case Read(DatasetRef(location), None) =>
        assert(location == "rate", s"expected the source name as a best-effort location for a source with no physical location, got '$location'")
      case other => fail(s"expected a bare Read, got ${PlanPrinter.render(other)}")
    }
    assert(
      result.diagnostics.exists(_.nodeType == "StreamingRelationV2"),
      s"a source with no resolvable location should report a fallback diagnostic: ${result.diagnostics}"
    )
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
        case Write(DatasetRef(location), Aggregate(Read(_, None), _, aggregates), format, saveMode) =>
          assert(location.contains("customer_orders.parquet"))
          assert(aggregates.map(_.name) == List("id", "lifetime_value"))
          // A real write via spark-submit-style DataFrame.write.parquet(...)
          // — proves format and save mode capture both work on the actual
          // end-to-end path, not just a hand-built
          // InsertIntoHadoopFsRelationCommand.
          assert(format.contains("parquet"))
          assert(saveMode.contains("overwrite"))
        case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
      }

      val lineage = Lineage.trace(result.plan)
      assert(lineage.find(_.output.name == "lifetime_value").get.aggregated)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  // SparkAdapterListener.onSuccess gates on WriteCommandSupport.combined
  // (see that object's class doc) rather than capturing every analyzed
  // plan unconditionally - this is the other half of that guarantee: not
  // just "a write is captured" (the test above) but "a non-write action
  // is NOT captured", proving the gate actually discriminates rather than
  // always firing.
  test("SparkAdapterListener does not capture a non-write action (.count())") {
    val listener = new SparkAdapterListener
    spark.listenerManager.register(listener)
    try {
      readSample().count()
      // No write ever ran on this session/listener pairing, so lastWrite
      // must still be empty - give the async listener thread the same
      // grace period the positive test does before asserting.
      Thread.sleep(500)
      assert(listener.lastWrite.isEmpty, s"listener captured a non-write plan: ${listener.lastWrite}")
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
