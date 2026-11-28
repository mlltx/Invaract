// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir
import com.invaract.verification.notification.{LineageDataset, WriteLineage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** The Spark constructs the lineage cross-check found Invaract losing track of (lineage-crosscheck's corpus, the jobs of
  * docs/OPENLINEAGE.md's second review), each traced from a real analyzed plan to the edges a write's OpenLineage event
  * carries. An edge is `<field or *> <KIND>/<SUBTYPE> <dataset>.<column>`, the dataset being its directory's name. */
class LineageEdgeCasesSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var dir: Path = _
  private def at(name: String) = dir.resolve(name).toString.replace('\\', '/')
  private def orders = spark.read.parquet(at("orders"))
  private def customers = spark.read.parquet(at("customers"))
  private def events = spark.read.parquet(at("events"))

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-lineage-edges")
    spark = SparkSession.builder().master("local[1]").appName("LineageEdgeCasesSpec")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val s = spark
    import s.implicits._
    Seq((1L, 10L, 100L, "open"), (2L, 20L, 200L, "paid")).toDF("id", "customer_id", "amount", "status").write.parquet(at("orders"))
    Seq((3L, 10L, 50L, "paid")).toDF("id", "customer_id", "amount", "status").write.parquet(at("archive"))
    Seq((10L, 1L, "x", "y")).toDF("customer_id", "id", "amount", "status").write.parquet(at("swapped"))
    Seq((10L, "Ann", "uk"), (20L, "Bo", "fr")).toDF("customer_id", "name", "country").write.parquet(at("customers"))
    spark.createDataFrame(Seq((1L, (5L, "x"), Seq("t1", "t2")))).toDF("id", "payload", "tags").write.parquet(at("events"))
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def reads(p: ir.Plan): List[String] = p match {
    case r: ir.Read => List(r.dataset.location)
    case other      => other.children.flatMap(reads)
  }

  /** The edges `df`'s write would publish, every read's columns known as they are when Invaract checks a write. */
  private def edges(df: DataFrame): Set[String] = edges(df.queryExecution.analyzed)

  private def edges(analyzed: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Set[String] = {
    val plan = SparkPlanAdapter.translateAsWrite(analyzed, ir.DatasetRef("out")).plan
    val inputs = reads(plan).distinct.map { location =>
      val schema = scala.util.Try(spark.read.parquet(location).schema).toOption.map(SparkSchemas.toLogicalSchema)
      LineageDataset(location, schema = schema)
    }
    WriteLineage.trace(plan, inputs).map { e =>
      s"${e.field.getOrElse("*")} ${e.transformation.kind}/${e.transformation.subtype} ${e.source.location.split('/').last}.${e.source.field}"
    }.toSet
  }

  private def values(df: DataFrame, field: String): Set[String] = values(df.queryExecution.analyzed, field)
  private def values(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan, field: String): Set[String] =
    edges(plan).filter(_.startsWith(s"$field DIRECT/")).map(_.split(' ').last)

  // --- a union matches columns by position -------------------------------------------------------------------------------

  test("a union's column comes from each input's column at its position, not the one of its name") {
    val swapped = orders.select("id", "customer_id").union(orders.select("customer_id", "id"))
    assert(values(swapped, "id") == Set("orders.id", "orders.customer_id"))
    assert(values(swapped, "customer_id") == Set("orders.customer_id", "orders.id"))
    val reads = orders.select("id", "customer_id").union(spark.read.parquet(at("swapped")).select("customer_id", "id"))
    assert(values(reads, "id") == Set("orders.id", "swapped.customer_id"), "an input read with its columns in another order")
    val byName = orders.select("id", "amount").unionByName(orders.select(col("amount"), col("customer_id").as("id")))
    assert(values(byName, "id") == Set("orders.id", "orders.customer_id"), "unionByName: Spark reorders the second input")
  }

  test("a union of reads with no projection of their own still matches by position") {
    val plain = orders.union(spark.read.parquet(at("swapped")))
    assert(values(plain, "id") == Set("orders.id", "swapped.customer_id"))
  }

  // --- names in another case ---------------------------------------------------------------------------------------------

  test("a column referred to in another case is named as the input has it") {
    val df = orders.select(col("AMOUNT"), (col("Id") + 1).as("next_id"))
    assert(edges(df) == Set("AMOUNT DIRECT/IDENTITY orders.amount", "next_id DIRECT/TRANSFORMATION orders.id"))
    val twice = orders.select(col("amount")).select(col("AMOUNT").as("a"))
    assert(values(twice, "a") == Set("orders.amount"), "a projection's column, referred to in another case")
  }

  // --- operators that were opaque -----------------------------------------------------------------------------------------

  test("hints, observe(), sample() and REBALANCE are traced through") {
    assert(values(orders.join(broadcast(customers), "customer_id").select("id", "name"), "name") == Set("customers.name"))
    orders.createOrReplaceTempView("o"); customers.createOrReplaceTempView("c")
    val hinted = spark.sql("SELECT /*+ BROADCAST(c), REBALANCE */ o.id, c.name FROM o JOIN c ON o.customer_id = c.customer_id")
    assert(values(hinted, "id") == Set("orders.id") && values(hinted, "name") == Set("customers.name"))
    assert(values(orders.observe("m", count(lit(1)).as("rows")).select("id"), "id") == Set("orders.id"))
    assert(values(orders.sample(0.5, 1L).select("id"), "id") == Set("orders.id"))
  }

  test("offset() is traced through, and the sort below it still shapes the rows") {
    val df = orders.orderBy("amount").offset(1).select("id")
    assert(edges(df) == Set("id DIRECT/IDENTITY orders.id", "* INDIRECT/SORT orders.amount"))
    val plan = SparkPlanAdapter.translateAsWrite(df.queryExecution.analyzed, ir.DatasetRef("out")).plan
    def limits(p: ir.Plan): List[ir.Limit] = p match {
      case l: ir.Limit => l :: limits(l.input)
      case other       => other.children.flatMap(limits)
    }
    assert(limits(plan) == limits(plan).map(_.copy(limit = Int.MaxValue, offset = 1)) && limits(plan).size == 1, "every row after the first")
  }

  test("a typed filter keeps rows by a function of every column it gives the function") {
    val s = spark
    import s.implicits._
    val df = orders.as[OrderRowForLineage].filter(o => o.amount > 60).select("id")
    assert(edges(df) == Set("id DIRECT/IDENTITY orders.id") ++ Set("id", "customer_id", "amount", "status").map(c => s"* INDIRECT/FILTER orders.$c"))
  }

  test("INTERSECT and EXCEPT keep the left input's values; both inputs decide which; without ALL, each row once") {
    val keys = Set("* INDIRECT/JOIN orders.customer_id", "* INDIRECT/JOIN customers.customer_id", "customer_id DIRECT/IDENTITY orders.customer_id")
    val once = keys + "* INDIRECT/GROUP_BY orders.customer_id"
    assert(edges(orders.select("customer_id").intersect(customers.select("customer_id"))) == once)
    assert(edges(orders.select("customer_id").intersectAll(customers.select("customer_id"))) == keys)
    assert(edges(orders.select("customer_id").except(customers.select("customer_id"))) == once)
    assert(edges(orders.select("customer_id").exceptAll(customers.select("customer_id"))) == keys)
  }

  test("a LATERAL join traces both sides, and the outer column the subquery correlates on joins them") {
    orders.createOrReplaceTempView("o"); customers.createOrReplaceTempView("c")
    val df = spark.sql("SELECT o.id, t.name FROM o, LATERAL (SELECT max(c.name) AS name FROM c WHERE c.customer_id = o.customer_id) t")
    assert(values(df, "id") == Set("orders.id") && values(df, "name") == Set("customers.name"))
    assert(edges(df).contains("* INDIRECT/JOIN orders.customer_id"), edges(df))
    val uncorrelated = spark.sql("SELECT o.id, t.n FROM o, LATERAL (SELECT count(*) AS n FROM c) t")
    assert(!edges(uncorrelated).exists(_.startsWith("* INDIRECT/JOIN")), "no correlation, no condition")
    val conditioned = spark.sql("SELECT o.id, t.name FROM o JOIN LATERAL (SELECT c.name FROM c WHERE c.customer_id = o.customer_id) t ON o.amount > 1")
    assert(Set("* INDIRECT/JOIN orders.customer_id", "* INDIRECT/JOIN orders.amount").subsetOf(edges(conditioned)), "its own ON condition too")
  }

  // --- typed Dataset operations ----------------------------------------------------------------------------------------

  private val ordersColumns = Set("id", "customer_id", "amount", "status").map(c => s"orders.$c")
  private def opaque(df: DataFrame, field: String): Set[(String, String)] = opaque(df.queryExecution.analyzed, field)
  private def opaque(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan, field: String): Set[(String, String)] =
    edges(plan).filter(_.startsWith(s"$field DIRECT/")).map(e => e.split(' ')(1) -> e.split(' ').last)

  test("a typed map, flatMap or mapPartitions: each column an opaque computation of every column the function is given") {
    val s = spark
    import s.implicits._
    val typed = orders.as[OrderRowForLineage]
    assert(opaque(typed.map(o => (o.id, o.amount * 2)).toDF("id", "doubled"), "doubled") == ordersColumns.map("DIRECT/TRANSFORMATION" -> _))
    assert(values(typed.flatMap(o => Seq(o.id, o.amount)).toDF("value"), "value") == ordersColumns)
    assert(values(typed.mapPartitions(_.map(_.id)).toDF("id"), "id") == ordersColumns)
    assert(values(orders.select("id", "amount").as[(Long, Long)].map(t => t._1 + t._2).toDF("sum"), "sum") == Set("orders.id", "orders.amount"),
      "only the columns it is given")
  }

  test("groupByKey and mapGroups: the key groups the rows; each column an opaque computation of the key's and the rows' columns") {
    val s = spark
    import s.implicits._
    val df = orders.as[OrderRowForLineage].groupByKey(_.customer_id).mapGroups((k, rows) => (k, rows.map(_.amount).sum)).toDF("customer_id", "total")
    assert(values(df, "total") == ordersColumns)
    assert(edges(df).filter(_.startsWith("* INDIRECT/GROUP_BY")).map(_.split(' ').last) == ordersColumns, "the key is a function of the whole row")
  }

  // --- streaming ---------------------------------------------------------------------------------------------------------

  private def stream = spark.readStream.schema(orders.schema).parquet(at("orders")).withColumn("ts", col("id").cast("timestamp"))
  private def groupedBy(df: DataFrame) = edges(df).filter(_.startsWith("* INDIRECT/GROUP_BY")).map(_.split(' ').last)

  test("a watermark changes no column: a windowed aggregation above it is traced") {
    val df = stream.withWatermark("ts", "10 seconds").groupBy(window(col("ts"), "1 minute"), col("status")).agg(sum("amount").as("total"))
    assert(opaque(df, "total") == Set("DIRECT/AGGREGATION" -> "orders.amount"))
    assert(groupedBy(df) == Set("orders.id", "orders.status"), "the window is a function of the event time, here the id")
  }

  test("a deduplication within a watermark is a deduplication by its keys") {
    val df = stream.withWatermark("ts", "10 seconds").dropDuplicatesWithinWatermark("customer_id")
    assert(groupedBy(df) == Set("orders.customer_id"))
    assert(opaque(df, "amount") == Set("DIRECT/AGGREGATION" -> "orders.amount"))
  }

  test("mapGroupsWithState: each column an opaque computation of the group's columns, and of its initial state's when it has one") {
    val s = spark
    import s.implicits._
    import org.apache.spark.sql.streaming.GroupStateTimeout
    val rows = stream.drop("ts").as[OrderRowForLineage].groupByKey(_.customer_id)
    val plain = rows.mapGroupsWithState[Long, (Long, Long)](GroupStateTimeout.NoTimeout())((k, rs, _) => (k, rs.map(_.amount).sum)).toDF("customer_id", "total")
    assert(values(plain, "total") == ordersColumns)
    assert(groupedBy(plain) == ordersColumns, "the key is a function of the whole row")
    val initial = customers.select("customer_id", "name").as[(Long, String)].groupByKey(_._1).mapValues(_._2.length.toLong)
    val seeded = rows.mapGroupsWithState[Long, (Long, Long)](GroupStateTimeout.NoTimeout(), initial)((k, rs, _) => (k, rs.map(_.amount).sum)).toDF("customer_id", "total")
    assert(values(seeded, "total") == ordersColumns ++ Set("customers.customer_id", "customers.name"))
    assert(edges(seeded).filter(_.startsWith("* INDIRECT/JOIN")).map(_.split(' ').last) == ordersColumns ++ Set("customers.customer_id", "customers.name"),
      "the group's key and its initial state's are matched, each a function of every column its function is given")
  }

  // --- PySpark's pandas and Arrow functions (their plan nodes, built here as PySpark builds them) ------------------------

  private def attr(name: String) = org.apache.spark.sql.catalyst.expressions.AttributeReference(name, org.apache.spark.sql.types.LongType)()
  private val python = org.apache.spark.sql.catalyst.expressions.Literal(true) // the Python function: not looked at

  test("mapInPandas and mapInArrow: each column an opaque computation of every column the function is given") {
    import org.apache.spark.sql.catalyst.plans.logical.{MapInPandas, PythonMapInArrow}
    val input = orders.queryExecution.analyzed
    assert(opaque(MapInPandas(python, Seq(attr("tripled")), input, isBarrier = false), "tripled") == ordersColumns.map("DIRECT/TRANSFORMATION" -> _))
    assert(values(PythonMapInArrow(python, Seq(attr("id")), orders.select("id", "amount").queryExecution.analyzed, isBarrier = false), "id") ==
      Set("orders.id", "orders.amount"), "only the columns it is given")
  }

  test("applyInPandas and applyInPandasWithState: the grouping columns group the rows; each column an opaque computation of the group") {
    import org.apache.spark.sql.catalyst.plans.logical.{FlatMapGroupsInPandas, FlatMapGroupsInPandasWithState}
    import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
    val input = orders.queryExecution.analyzed
    val key = input.output.filter(_.name == "customer_id")
    val grouped = FlatMapGroupsInPandas(key, python, Seq(attr("total")), input)
    assert(values(grouped, "total") == ordersColumns)
    assert(edges(grouped).filter(_.startsWith("* INDIRECT/GROUP_BY")).map(_.split(' ').last) == Set("orders.customer_id"))
    val stateful = FlatMapGroupsInPandasWithState(python, key, Seq(attr("total")), new org.apache.spark.sql.types.StructType().add("n", "long"),
      OutputMode.Update(), GroupStateTimeout.NoTimeout(), input)
    assert(values(stateful, "total") == ordersColumns)
    assert(edges(stateful).filter(_.startsWith("* INDIRECT/GROUP_BY")).map(_.split(' ').last) == Set("orders.customer_id"))
  }

  test("a cogroup's applyInPandas: both inputs' groups, matched by key; each column an opaque computation of both") {
    import org.apache.spark.sql.catalyst.plans.logical.FlatMapCoGroupsInPandas
    val left = orders.select("customer_id", "amount").queryExecution.analyzed
    val right = customers.select("customer_id", "name").queryExecution.analyzed
    val cogroup = FlatMapCoGroupsInPandas(1, 1, python, Seq(attr("n")), left, right)
    assert(values(cogroup, "n") == Set("orders.customer_id", "orders.amount", "customers.customer_id", "customers.name"))
    val indirect = edges(cogroup).filter(_.startsWith("* INDIRECT/")).map(e => e.split(' ')(1) -> e.split(' ').last)
    assert(indirect == Set("INDIRECT/JOIN", "INDIRECT/GROUP_BY").flatMap(k => Set(k -> "orders.customer_id", k -> "customers.customer_id")), indirect)
  }

  test("pandas-on-Spark's default index: every column passed through, and a row number from no column") {
    import org.apache.spark.sql.catalyst.plans.logical.AttachDistributedSequence
    val indexed = AttachDistributedSequence(attr("distributed_sequence_id"), orders.queryExecution.analyzed)
    assert(opaque(indexed, "amount") == Set("DIRECT/IDENTITY" -> "orders.amount"))
    assert(opaque(indexed, "distributed_sequence_id").isEmpty)
  }

  // --- reads -------------------------------------------------------------------------------------------------------------

  test("one read of two directories traces each column to both") {
    val df = spark.read.parquet(at("orders"), at("archive")).select("id", "amount")
    assert(values(df, "id") == Set("orders.id", "archive.id") && values(df, "amount") == Set("orders.amount", "archive.amount"))
  }

  // --- quality ----------------------------------------------------------------------------------------------------------

  test("a field of joinWith's struct is that field's column, copied") {
    val o = orders; val c = customers
    val df = o.joinWith(c, o("customer_id") === c("customer_id")).select(col("_1.id").as("id"), col("_2.name").as("name"))
    assert(edges(df).filter(_.contains(" DIRECT/")) == Set("id DIRECT/IDENTITY orders.id", "name DIRECT/IDENTITY customers.name"))
  }

  test("a cast to a column's own type is no transformation; a cast to another type is") {
    orders.createOrReplaceTempView("o")
    spark.sql("CREATE OR REPLACE TEMP VIEW paid AS SELECT id, amount FROM o WHERE status = 'paid'")
    assert(edges(spark.sql("SELECT id FROM paid")).contains("id DIRECT/IDENTITY orders.id"))
    assert(edges(orders.select(col("id").cast("int").as("id"))) == Set("id DIRECT/TRANSFORMATION orders.id"))
    assert(edges(orders.select(col("id").cast("long").as("id"))) == Set("id DIRECT/IDENTITY orders.id"))
  }

  test("IF(c, x, x) is x") {
    val a = org.apache.spark.sql.catalyst.expressions.AttributeReference("a", org.apache.spark.sql.types.LongType)()
    val b = org.apache.spark.sql.catalyst.expressions.AttributeReference("b", org.apache.spark.sql.types.BooleanType)()
    val same = org.apache.spark.sql.catalyst.expressions.If(b, a, a)
    assert(SparkPlanAdapter.translateExprStandalone(same) == SparkPlanAdapter.translateExprStandalone(a))
    val other = org.apache.spark.sql.catalyst.expressions.If(b, a, org.apache.spark.sql.catalyst.expressions.Literal(0L))
    assert(SparkPlanAdapter.translateExprStandalone(other).isInstanceOf[ir.Conditional])
  }

  test("a correlated scalar subquery's value depends on the outer column it correlates on; an uncorrelated one's does not") {
    orders.createOrReplaceTempView("o"); customers.createOrReplaceTempView("c")
    val correlated = edges(spark.sql("SELECT id, (SELECT max(c.name) FROM c WHERE c.customer_id = o.customer_id) AS name FROM o"))
    assert(correlated.contains("name INDIRECT/CONDITIONAL orders.customer_id") && correlated.contains("name DIRECT/AGGREGATION customers.name"), correlated)
    val plain = edges(spark.sql("SELECT id, (SELECT max(c.name) FROM c) AS name FROM o"))
    assert(!plain.exists(_.startsWith("name INDIRECT/CONDITIONAL")) && plain.contains("name DIRECT/AGGREGATION customers.name"), plain)
  }

  test("an aggregate's FILTER clause decides its value; without one, nothing does") {
    orders.createOrReplaceTempView("o")
    val filtered = edges(spark.sql("SELECT customer_id, sum(amount) FILTER (WHERE status = 'paid') AS paid FROM o GROUP BY customer_id"))
    assert(filtered.contains("paid INDIRECT/CONDITIONAL orders.status") && filtered.contains("paid DIRECT/AGGREGATION orders.amount"), filtered)
    val plain = edges(spark.sql("SELECT customer_id, sum(amount) AS paid FROM o GROUP BY customer_id"))
    assert(!plain.exists(_.contains("CONDITIONAL")), plain)
  }

  test("each micro-batch of a file stream reads the stream's directory, not the files that arrived") {
    val seen = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val write: (DataFrame, Long) => Unit = (batch, _) =>
      reads(SparkPlanAdapter.translate(batch.queryExecution.analyzed).plan).foreach(seen.add)
    val query = spark.readStream.schema(orders.schema).parquet(at("orders")).select("id")
      .writeStream.option("checkpointLocation", at("checkpoint")).foreachBatch(write)
      .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow()).start()
    query.awaitTermination()
    import scala.collection.JavaConverters._
    assert(seen.asScala.nonEmpty && seen.asScala.forall(_.stripSuffix("/").endsWith("/orders")), seen.asScala.toList)
    assert(SparkPlanAdapter.locationOf(orders.queryExecution.analyzed.collectFirst { case l: org.apache.spark.sql.execution.datasources.LogicalRelation => l }.get)
      .endsWith("/orders"), "a batch read: its root path, as before")
  }
}

final case class OrderRowForLineage(id: Long, customer_id: Long, amount: Long, status: String)
