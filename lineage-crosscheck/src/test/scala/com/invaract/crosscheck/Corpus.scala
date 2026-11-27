// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** The two tools Invaract's lineage is held up against. */
sealed abstract class Reference(val name: String)
object Reference {
  case object OpenLineageSpark extends Reference("openlineage-spark")
  case object Spline extends Reference("spline")
  val all: List[Reference] = List(OpenLineageSpark, Spline)
}

/** Why Invaract's lineage of a job differs from a reference's. */
sealed abstract class Cause(val label: String)
object Cause {
  /** Invaract misses or misreports something: a known gap, listed as one in the docs. */
  case object Gap extends Cause("Invaract gap")
  /** The reference misses or adds something Invaract is right about. */
  case object Reference extends Cause("the other tool's limit")
  /** Both are defensible: a choice, explained. */
  case object Choice extends Cause("a convention")
}

final case class Note(cause: Cause, text: String)

/** One Spark job of the corpus: it makes exactly one write (to the one dataset its contract declares, at
  * `contractLocation`).
  *
  * `notes` explains, for each reference Invaract does not agree with on this job, why: the difference is either the
  * other tool's (and Invaract is right), a deliberate choice, or a gap in Invaract. The spec fails when a job
  * disagrees with a reference and has no note for it, or has a note and agrees. */
final case class Job(
    id: String,
    construct: String,
    description: String,
    run: (SparkSession, Fixture, Out) => Unit,
    notes: Map[Reference, Note] = Map.empty,
    contractLocation: Out => String = out => out.at("out"),
    extensions: List[String] = Nil,
    conf: Out => Map[String, String] = _ => Map.empty
)

/** The inputs every job reads, written once for the whole corpus. */
final class Fixture(val root: Path) {
  def at(name: String): String = Harness.slashes(root.resolve("inputs").resolve(name))
  val orders: String = at("orders")
  val customers: String = at("customers")
  val events: String = at("events")
  val salesCsv: String = at("sales.csv")
  val jdbcUrl: String = s"jdbc:h2:${root.resolve("inputs").resolve("bank")};DB_CLOSE_DELAY=-1"
  /** orders again, partitioned by status (status=open/, status=paid/): the column comes from the directory names. */
  val ordersByStatus: String = at("orders_by_status")
  /** a second directory of orders, the same schema. */
  val ordersArchive: String = at("orders_archive")
  /** orders as a Delta table with two versions: 0 holds the three orders, 1 adds a fourth. */
  val ordersDelta: String = at("orders_delta")

  def read(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)
  def orders(spark: SparkSession): DataFrame = read(spark, orders)
  def customers(spark: SparkSession): DataFrame = read(spark, customers)
  def events(spark: SparkSession): DataFrame = spark.read.json(events)
}

object Fixture {
  def create(): Fixture = {
    val fixture = new Fixture(Files.createTempDirectory("lineage-crosscheck"))
    val spark = SparkSession.builder().master("local[1]").appName("fixture").config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
    try {
      import spark.implicits._
      Seq((1L, 10L, 100L, "open"), (2L, 20L, 200L, "paid"), (3L, 10L, 50L, "paid"))
        .toDF("id", "customer_id", "amount", "status").write.parquet(fixture.orders)
      Seq((10L, "Ann", "uk", "ann@example.com"), (20L, "Bo", "fr", "bo@example.com"))
        .toDF("customer_id", "name", "country", "email").write.parquet(fixture.customers)
      Seq("""{"id":1,"payload":{"a":5,"b":"x"},"tags":["t1","t2"]}""", """{"id":2,"payload":{"a":7,"b":"y"},"tags":["t3"]}""")
        .toDS().write.text(fixture.events)
      Seq(("north", 10L), ("south", 20L)).toDF("region", "amount").write.option("header", "true").csv(fixture.salesCsv)
      val orders = spark.read.parquet(fixture.orders)
      orders.write.partitionBy("status").parquet(fixture.ordersByStatus)
      orders.write.parquet(fixture.ordersArchive)
      orders.write.format("delta").save(fixture.ordersDelta)
      Seq((4L, 20L, 400L, "open")).toDF("id", "customer_id", "amount", "status").write.format("delta").mode("append").save(fixture.ordersDelta)
    } finally spark.stop()
    val connection = java.sql.DriverManager.getConnection(fixture.jdbcUrl)
    try {
      val statement = connection.createStatement()
      statement.execute("CREATE TABLE ACCOUNTS (ID BIGINT PRIMARY KEY, BALANCE BIGINT)")
      statement.execute("INSERT INTO ACCOUNTS VALUES (1, 100), (2, 200)")
    } finally connection.close()
    fixture
  }
}

/** Typed rows for the Dataset-API jobs. */
final case class OrderRow(id: Long, customer_id: Long, amount: Long, status: String)
final case class Doubled(id: Long, doubled: Long)

object Corpus {
  private def write(df: DataFrame, out: Out): Unit = df.write.mode("overwrite").parquet(out.at("out"))

  private def job(id: String, construct: String, description: String, notes: (Reference, Note)*)(body: (SparkSession, Fixture) => DataFrame): Job =
    Job(id, construct, description, (spark, fixture, out) => write(body(spark, fixture), out), notes.toMap)

  private def gap(text: String) = Note(Cause.Gap, text)
  private def theirs(text: String) = Note(Cause.Reference, text)
  private def choice(text: String) = Note(Cause.Choice, text)
  private val OLS = Reference.OpenLineageSpark
  private val Spline = Reference.Spline

  private val optimizerJoinFilter = theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there.")

  private val delta = List("io.delta.sql.DeltaSparkSessionExtension")
  private val deltaConf: Out => Map[String, String] = _ => Map("spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  private val iceberg = List("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  private val icebergConf: Out => Map[String, String] = out => Map(
    "spark.sql.catalog.ice" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type" -> "hadoop",
    "spark.sql.catalog.ice.warehouse" -> out.at("ice")
  )
  private def tableLocation(name: String): Out => String = out => s"${out.warehouse}/$name"

  val jobs: List[Job] = List(
    // --- projections -------------------------------------------------------------------------------------------
    job("select", "Projection", "a subset of the input's columns")((s, f) => f.orders(s).select("id", "amount")),
    job("rename", "Projection", "a column renamed")((s, f) => f.orders(s).select(col("amount").as("total"))),
    job("arithmetic", "Projection", "a column computed from two others")((s, f) =>
      f.orders(s).select(col("id"), (col("amount") * 2 + col("id")).as("score"))),
    job("with-column-replace", "Projection", "withColumn replacing an existing column")((s, f) =>
      f.orders(s).withColumn("amount", col("amount") + 1)),
    job("functions", "Projection", "coalesce, concat and a cast over several columns", OLS -> choice("openlineage-spark counts coalesce's argument as a condition of the value (INDIRECT/CONDITIONAL) as well as its value; Invaract counts it as the value only: coalesce returns it whenever it is not null."))((s, f) =>
      f.orders(s).select(concat(col("status"), lit("-"), col("id").cast("string")).as("label"), coalesce(col("status"), lit("none")).as("s"))),
    job("literal", "Projection", "a constant column and the current time beside an input column")((s, f) =>
      f.orders(s).select(col("id"), lit(1).as("one"), current_timestamp().as("at"))),
    job("case-when", "Conditional", "CASE WHEN: the value from one column, chosen by another")((s, f) =>
      f.orders(s).select(when(col("amount") > 150, col("status")).otherwise(lit("small")).as("size"))),
    job("masking", "Projection", "a hash of a column (sha2), which OpenLineage marks as masking")((s, f) =>
      f.customers(s).select(col("customer_id"), sha2(col("email"), 256).as("email_hash"))),

    // --- filters and ordering -------------------------------------------------------------------------------------
    job("filter", "Filter", "rows kept by a condition on a column not written")((s, f) =>
      f.orders(s).filter(col("amount") > 60).select("id")),
    job("sort-limit", "Sort", "top rows by a column")((s, f) => f.orders(s).orderBy(col("amount").desc).limit(2).select("id")),
    job("distinct", "Deduplication", "distinct rows", Spline -> theirs("Spline records no grouping for a deduplication. Invaract, like openlineage-spark, reports the columns it deduplicates on as GROUP_BY: Spark itself turns a deduplication into an aggregate grouped by them."))((s, f) => f.orders(s).select("status").distinct()),
    job("drop-duplicates", "Deduplication", "dropDuplicates on a subset of columns", Spline -> theirs("Spline records no grouping for a deduplication. Invaract, like openlineage-spark, reports the columns it deduplicates on as GROUP_BY: Spark itself turns a deduplication into an aggregate grouped by them."))((s, f) => f.orders(s).dropDuplicates("customer_id")),

    // --- joins ------------------------------------------------------------------------------------------------------
    job("inner-join", "Join", "an inner join, columns from both sides", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) =>
      f.orders(s).join(f.customers(s), "customer_id").select("id", "name", "amount")),
    job("left-join", "Join", "a left join on two conditions", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) => {
      val o = f.orders(s).as("o"); val c = f.customers(s).as("c")
      o.join(c, col("o.customer_id") === col("c.customer_id") && col("o.status") =!= col("c.country"), "left").select(col("o.id"), col("c.name"))
    }),
    job("self-join", "Join", "a table joined with itself under two aliases", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) => {
      val a = f.orders(s).as("a"); val b = f.orders(s).as("b")
      a.join(b, col("a.customer_id") === col("b.customer_id")).select(col("a.id").as("left_id"), col("b.id").as("right_id"))
    }),

    // --- aggregation and windows ------------------------------------------------------------------------------------
    job("group-by", "Aggregation", "sum, count and max by a key")((s, f) =>
      f.orders(s).groupBy("customer_id").agg(sum("amount").as("total"), count("id").as("n"), max("status").as("top"))),
    job("global-aggregate", "Aggregation", "count(*) over the whole input")((s, f) => f.orders(s).agg(count(lit(1)).as("n"))),
    job("join-aggregate", "Aggregation", "a join, a filter and an aggregate of an expression", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) =>
      f.orders(s).join(f.customers(s), "customer_id").filter(col("country") === "uk").groupBy(col("name")).agg(sum(col("amount") * 2).as("total"))),
    job("window", "Window", "row_number over a partition, ordered")((s, f) =>
      f.orders(s).select(col("id"), row_number().over(Window.partitionBy("customer_id").orderBy(col("amount").desc)).as("rank"))),
    job("window-aggregate", "Window", "a running sum over a window")((s, f) =>
      f.orders(s).select(col("id"), sum("amount").over(Window.partitionBy("customer_id").orderBy("id")).as("running"))),
    job("pivot", "Aggregation", "a pivot of one column's values into columns")((s, f) =>
      f.orders(s).groupBy("customer_id").pivot("status", Seq("open", "paid")).agg(sum("amount"))),
    job("rollup", "Aggregation", "rollup over two keys", OLS -> theirs("openlineage-spark does not trace a rollup's grouping keys through the Expand Spark computes it with, so it reports no source for them and no grouping. Invaract traces each key to its column."), Spline -> theirs("Spline does not trace a rollup's grouping keys through Spark's Expand either. Invaract traces each key to its column."))((s, f) =>
      f.orders(s).rollup("customer_id", "status").agg(sum("amount").as("total"))),

    // --- set operations ---------------------------------------------------------------------------------------------
    job("union", "Set operation", "a union of two inputs' columns")((s, f) =>
      f.orders(s).select(col("customer_id").as("key")).union(f.customers(s).select(col("customer_id").as("key")))),

    // --- nested data --------------------------------------------------------------------------------------------------
    job("nested-field", "Nested data", "a field of a struct column")((s, f) => f.events(s).select(col("id"), col("payload.a").as("a"))),
    job("struct-build", "Nested data", "a struct built from two columns")((s, f) =>
      f.orders(s).select(col("id"), struct(col("amount"), col("status")).as("detail"))),
    job("explode", "Nested data", "explode of an array column", OLS -> theirs("openlineage-spark reads the optimized plan, where Spark adds a size(tags) > 0 filter before an explode; it reports the array column as FILTER too. Invaract reads the plan the job wrote."))((s, f) => f.events(s).select(col("id"), explode(col("tags")).as("tag"))),

    // --- SQL ----------------------------------------------------------------------------------------------------------
    job("sql-view", "SQL", "SQL over a temporary view")((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("SELECT id, amount * 10 AS cents FROM orders WHERE status = 'paid'")
    }),
    job("sql-cte", "SQL", "a CTE referenced twice", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."), Spline -> theirs("Spline traces no column through a CTE referenced twice. Invaract inlines the CTE, as Spark's optimizer does, and traces through it."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("WITH big AS (SELECT id, customer_id FROM orders WHERE amount > 60) SELECT a.id, b.customer_id FROM big a JOIN big b ON a.id = b.id")
    }),
    job("in-subquery", "SQL", "a filter by IN (subquery) over another input", OLS -> theirs("openlineage-spark reads the optimized plan, where Spark has rewritten the IN subquery into a semi join, so it calls these columns JOIN. Invaract reads the plan the job wrote: a filter."), Spline -> theirs("Spline does not follow a subquery inside a filter: it misses the subquery's input and the columns it filters on."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT id FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'uk')")
    }),
    job("exists-subquery", "SQL", "a filter by EXISTS (correlated subquery)", OLS -> theirs("openlineage-spark reads the optimized plan, where Spark has rewritten the EXISTS subquery into a semi join, so it calls these columns JOIN. Invaract reads the plan the job wrote: a filter."), Spline -> theirs("Spline does not follow a subquery inside a filter: it misses the subquery's input and the column it correlates on."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)")
    }),
    job("scalar-subquery", "SQL", "a scalar subquery in the select list", OLS -> theirs("openlineage-spark does not follow a scalar subquery in the select list: it misses the input the subquery reads and the column its value comes from."), Spline -> theirs("Spline does not follow a scalar subquery in the select list either."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT id, (SELECT max(customer_id) FROM customers) AS top_customer FROM orders")
    }),

    // --- code Spark cannot see into -----------------------------------------------------------------------------------
    job("scala-udf", "UDF", "a Scala UDF over two columns")((s, f) => {
      val tier = udf((amount: Long, status: String) => if (amount > 100) status else "low")
      f.orders(s).select(col("id"), tier(col("amount"), col("status")).as("tier"))
    }),
    job("typed-map", "Dataset API", "a typed map over a Dataset of case classes")((s, f) => {
      import s.implicits._
      f.orders(s).as[OrderRow].map(o => Doubled(o.id, o.amount * 2)).toDF()
    }),
    job("rdd", "Dataset API", "a DataFrame built from an RDD of another input's rows", Spline -> theirs("Spline does not look into an RDD for the files it read; Invaract, like openlineage-spark, reports them as the input (with no column traced through the RDD)."))((s, f) => {
      val rows = f.orders(s).rdd
      s.createDataFrame(rows, f.orders(s).schema).select("id")
    }),

    // --- plan boundaries ----------------------------------------------------------------------------------------------
    job("cache", "Caching", "a cached DataFrame, then a projection of it")((s, f) => {
      val cached = f.orders(s).filter(col("amount") > 60).cache(); cached.count()
      cached.select(col("id"), (col("amount") + 1).as("next"))
    }),
    job("local-checkpoint", "Caching", "a projection of a locally checkpointed DataFrame", OLS -> theirs("openlineage-spark does not see past a localCheckpoint(). Invaract resolves the checkpoint to the plan it was made from and traces through it."), Spline -> theirs("Spline does not see past a localCheckpoint() either."))((s, f) =>
      f.orders(s).localCheckpoint().select(col("id"), col("amount"))),
    job("repartition", "Physical", "repartition and coalesce between read and write")((s, f) =>
      f.orders(s).repartition(2, col("customer_id")).coalesce(1).select("id", "customer_id")),

    // --- formats ------------------------------------------------------------------------------------------------------
    job("csv-input", "Formats", "a CSV input with a header")((s, f) =>
      s.read.option("header", "true").option("inferSchema", "true").csv(f.salesCsv).select(col("region"), (col("amount") * 2).as("double"))),
    job("multiple-inputs", "Formats", "Parquet and JSON inputs joined (USING: the key is the left side's)", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) =>
      f.orders(s).join(f.events(s).select(col("id"), col("payload.b").as("b")), "id").select("id", "amount", "b")),
    job("using-right-join", "Join", "a right join USING a key: the key is the right side's", OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on the key of the side a right join filters; it reports that column as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))((s, f) =>
      f.customers(s).join(f.orders(s).select("customer_id", "amount"), Seq("customer_id"), "right").select("customer_id", "name", "amount")),
    job("sql-distinct", "Deduplication", "SELECT DISTINCT", Spline -> theirs("Spline records no grouping for a deduplication. Invaract, like openlineage-spark, reports the columns it deduplicates on as GROUP_BY: Spark itself turns a deduplication into an aggregate grouped by them."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("SELECT DISTINCT customer_id, status FROM orders")
    }),

    // --- how and where the write lands --------------------------------------------------------------------------------
    Job("partitioned", "Writes", "a write partitioned by a column",
      (s, f, out) => f.orders(s).select("id", "amount", "status").write.mode("overwrite").partitionBy("status").parquet(out.at("out")),
      notes = Map(OLS -> theirs("openlineage-spark reports the partition column of a partitioned write as transformed and as shaping the rows; the column is written unchanged, which Invaract reports as IDENTITY."))),
    Job("append", "Writes", "an append to an existing directory",
      (s, f, out) => f.orders(s).select("id", "amount").write.mode("append").parquet(out.at("out"))),
    Job("save-as-table", "Catalog tables", "saveAsTable: a new catalog table from a query",
      (s, f, out) => f.orders(s).select(col("id"), (col("amount") * 2).as("double")).write.mode("overwrite").saveAsTable("doubled"),
      contractLocation = tableLocation("doubled")),
    Job("insert-into", "Catalog tables", "insertInto an existing table, by position",
      (s, f, out) => {
        s.sql("CREATE TABLE totals (customer BIGINT, total BIGINT) USING parquet")
        f.orders(s).groupBy("customer_id").agg(sum("amount")).write.insertInto("totals")
      },
      contractLocation = tableLocation("totals"),
      notes = Map(OLS -> gap("Spark's analyzer casts each column to the table's column type, here to the type it already has. Invaract counts that cast as a TRANSFORMATION, so a column copied unchanged is not IDENTITY."))),
    Job("sql-insert-select", "Catalog tables", "SQL INSERT INTO ... SELECT with a join",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
        s.sql("CREATE TABLE by_country (country STRING, amount BIGINT) USING parquet")
        s.sql("INSERT INTO by_country SELECT c.country, o.amount FROM orders o JOIN customers c ON o.customer_id = c.customer_id")
      },
      contractLocation = tableLocation("by_country"),
      notes = Map(OLS -> theirs("openlineage-spark reads Spark's optimized plan, where the optimizer adds an isnotnull filter on each column a join compares; it reports those columns as FILTER as well as JOIN. Invaract reads the plan the job wrote, which filters nothing there."))),
    Job("ctas", "Catalog tables", "SQL CREATE TABLE AS SELECT",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("CREATE TABLE paid USING parquet AS SELECT id, amount FROM orders WHERE status = 'paid'")
      },
      contractLocation = tableLocation("paid")),

    // --- table formats ------------------------------------------------------------------------------------------------
    Job("delta-write", "Delta Lake", "a Delta table written from a query",
      (s, f, out) => f.orders(s).select(col("id"), (col("amount") + 1).as("next")).write.format("delta").mode("overwrite").save(out.at("out")),
      extensions = delta, conf = deltaConf),
    Job("delta-merge", "Delta Lake", "MERGE INTO a Delta table from another input",
      (s, f, out) => {
        f.orders(s).limit(0).select("id", "amount").write.format("delta").save(out.at("out"))
        f.orders(s).createOrReplaceTempView("orders")
        s.sql(s"MERGE INTO delta.`${out.at("out")}` t USING orders o ON t.id = o.id " +
          "WHEN MATCHED THEN UPDATE SET t.amount = o.amount WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (o.id, o.amount)")
      },
      extensions = delta, conf = deltaConf,
      notes = Map(
        OLS -> choice("A MERGE's ON condition decides which rows each of its clauses writes: Invaract reports its columns as INDIRECT/JOIN, as for any join. openlineage-spark reports no indirect lineage for a MERGE."),
        Spline -> choice("Spline also counts the rows a MERGE leaves as they are (no clause touches them), so the table's own amount is a source of amount; Invaract, like openlineage-spark, counts the rows the MERGE's clauses write. Spline records no indirect lineage, so not the ON condition either."))),
    Job("delta-merge-clauses", "Delta Lake", "MERGE with conditional, DELETE, partial INSERT and NOT MATCHED BY SOURCE clauses",
      (s, f, out) => {
        f.orders(s).limit(0).select("id", "amount", "status").write.format("delta").save(out.at("out"))
        f.orders(s).createOrReplaceTempView("orders")
        s.sql(s"MERGE INTO delta.`${out.at("out")}` t USING orders o ON t.id = o.id " +
          "WHEN MATCHED AND o.status = 'cancelled' THEN DELETE " +
          "WHEN MATCHED AND o.amount > t.amount THEN UPDATE SET t.amount = o.amount " +
          "WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (o.id, o.amount * 100) " +
          "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.status = 'gone'")
      },
      extensions = delta, conf = deltaConf,
      notes = Map(
        OLS -> theirs("openlineage-spark does not follow a MERGE's clauses: it reports the amount the INSERT computes (amount * 100) as a copy, misses the table's own amount that the NOT MATCHED BY SOURCE clause keeps, and reports no indirect lineage - neither the ON condition nor the clause conditions that decide which rows are written."),
        Spline -> theirs("Spline records no indirect lineage for a MERGE: neither its ON condition nor the clause conditions that decide which rows are written."))),
    Job("iceberg-write", "Apache Iceberg", "an Iceberg table created from a query (writeTo)",
      (s, f, out) => f.orders(s).select(col("id"), col("amount")).writeTo("ice.db.orders_copy").using("iceberg").createOrReplace(),
      contractLocation = _ => "ice.db.orders_copy",
      extensions = iceberg, conf = icebergConf),

    Job("iceberg-append", "Apache Iceberg", "an append to an existing Iceberg table",
      (s, f, out) => {
        s.sql("CREATE TABLE ice.db.ledger (id BIGINT, amount BIGINT) USING iceberg")
        f.orders(s).select(col("id"), (col("amount") * 100).as("amount")).writeTo("ice.db.ledger").append()
      },
      contractLocation = out => "file:" + out.at("ice") + "/db/ledger",
      extensions = iceberg, conf = icebergConf),

    // --- streaming ----------------------------------------------------------------------------------------------------
    Job("streaming", "Structured Streaming", "a file stream into a file sink, run once (availableNow)",
      (s, f, out) => {
        val q = s.readStream.schema(f.orders(s).schema).parquet(f.orders).select(col("id"), (col("amount") * 3).as("tripled"))
          .writeStream.format("parquet").option("checkpointLocation", out.at("checkpoint"))
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow()).start(out.at("out"))
        q.awaitTermination()
      },
      notes = Map(
        OLS -> theirs("openlineage-spark publishes no dataset for a file stream written to a file sink. Invaract checks a stream once, when it starts, and publishes its lineage then."),
        Spline -> theirs("Spline's agent records no lineage for a Structured Streaming query. Invaract checks a stream once, when it starts, and publishes its lineage then."))),

    // --- Hive ---------------------------------------------------------------------------------------------------------
    Job("hive-insert", "Hive tables", "INSERT INTO a Hive SerDe (text) table from a query",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("CREATE TABLE ledger_h (id BIGINT, amount BIGINT) STORED AS TEXTFILE")
        s.sql("INSERT INTO ledger_h SELECT id, amount * 2 FROM orders WHERE status = 'paid'")
      },
      contractLocation = tableLocation("ledger_h"),
      conf = out => Map(
        "spark.sql.catalogImplementation" -> "hive",
        "spark.hadoop.javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=${out.at("metastore_db")};create=true"
      )),

    // --- other sources ------------------------------------------------------------------------------------------------
    job("jdbc-input", "JDBC", "a table read over JDBC", Spline -> theirs("Spline cannot parse an H2 file URL and names the table jdbc://:null."))((s, f) =>
      s.read.jdbc(f.jdbcUrl, "ACCOUNTS", new java.util.Properties()).select(col("ID").as("id"), col("BALANCE").as("balance"))),

    // --- names: where lineage traced by column name could go wrong ------------------------------------------------------
    job("swap-names", "Names", "two columns swapping names")((s, f) =>
      f.orders(s).select(col("amount").as("id"), col("id").as("amount"))),
    job("rename-chain", "Names", "withColumnRenamed three times, through a temporary name, to swap two columns")((s, f) =>
      f.orders(s).withColumnRenamed("amount", "x").withColumnRenamed("id", "amount").withColumnRenamed("x", "id").select("id", "amount")),
    job("case-insensitive", "Names", "columns referred to in another case than the input's", OLS -> gap("With spark.sql.caseSensitive off (Spark's default), a column can be referred to in any case. Invaract names each source column as the job wrote it (AMOUNT, Id), not as the input's schema has it (amount, id), so the edge names a column the dataset does not have."), Spline -> gap("With spark.sql.caseSensitive off (Spark's default), a column can be referred to in any case. Invaract names each source column as the job wrote it (AMOUNT, Id), not as the input's schema has it (amount, id), so the edge names a column the dataset does not have."))((s, f) =>
      f.orders(s).select(col("AMOUNT"), (col("Id") + 1).as("next_id"))),
    Job("case-sensitive", "Names", "two output columns differing only in case (spark.sql.caseSensitive=true)",
      (s, f, out) => f.orders(s).select(col("amount"), (col("id") * 2).as("Amount")).write.mode("overwrite").parquet(out.at("out")),
      conf = _ => Map("spark.sql.caseSensitive" -> "true"),
      notes = Map(OLS -> theirs("With spark.sql.caseSensitive on, Amount and amount are different columns, and Amount is id * 2. openlineage-spark matches the column ignoring case and reports Amount as a copy of amount."))),
    job("dotted-name", "Names", "a column whose name contains a dot")((s, f) =>
      f.orders(s).select(col("id"), col("amount").as("unit.price")).select(col("id"), (col("`unit.price`") * 2).as("total.price"))),
    job("struct-star", "Nested data", "every field of a struct, as columns (payload.*)")((s, f) => f.events(s).select(col("id"), col("payload.*"))),
    job("with-field", "Nested data", "a struct field replaced in place (withField)", OLS -> choice("withField builds the new struct only when the struct is not null: openlineage-spark, reading the optimized plan where that test is explicit, also reports payload as a condition of the value (INDIRECT/CONDITIONAL). Invaract reports payload and id as the struct's sources."), Spline -> theirs("Spline's agent records nothing for this write."))((s, f) =>
      f.events(s).select(col("id"), col("payload").withField("a", col("payload.a") + col("id")).as("payload"))),
    job("higher-order", "Nested data", "higher-order functions over an array (transform, filter)")((s, f) =>
      f.events(s).select(col("id"), transform(col("tags"), t => upper(t)).as("upper_tags"), filter(col("tags"), t => t =!= "t1").as("kept"))),
    job("array-access", "Nested data", "array elements by index (tags(0), element_at)")((s, f) =>
      f.events(s).select(col("id"), col("tags")(0).as("first_tag"), element_at(col("tags"), -1).as("last_tag"))),

    // --- joins, harder cases -------------------------------------------------------------------------------------------
    job("broadcast-hint", "Join", "a join with a broadcast hint (broadcast())", OLS -> gap("Invaract does not translate the plan node a broadcast hint adds (ResolvedHint): nothing on the hinted side is traced, so name has no source and the hinted side's join key is missing. (openlineage-spark also reports the join key as FILTER: the optimizer's isnotnull filter.)"), Spline -> gap("Invaract does not translate the plan node a broadcast hint adds (ResolvedHint): nothing on the hinted side is traced, so name has no source and the hinted side's join key is missing. (openlineage-spark also reports the join key as FILTER: the optimizer's isnotnull filter.)"))((s, f) =>
      f.orders(s).join(broadcast(f.customers(s)), "customer_id").select("id", "name")),
    job("sql-hints", "Join", "SQL hints: BROADCAST on a join, REBALANCE on the result", OLS -> gap("Invaract translates neither node the hints add (ResolvedHint for BROADCAST, RebalancePartitions for REBALANCE): no written column is traced. (openlineage-spark also reports the join key as FILTER: the optimizer's isnotnull filter.)"), Spline -> gap("Invaract translates neither node the hints add (ResolvedHint for BROADCAST, RebalancePartitions for REBALANCE): no written column is traced. (openlineage-spark also reports the join key as FILTER: the optimizer's isnotnull filter.)"))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT /*+ BROADCAST(c), REBALANCE */ o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id")
    }),
    job("semi-join", "Join", "a left semi join: rows of one input that have a match in another", OLS -> optimizerJoinFilter)((s, f) =>
      f.orders(s).join(f.customers(s).filter(col("country") === "uk"), Seq("customer_id"), "left_semi").select("id", "amount")),
    job("anti-join", "Join", "a left anti join: rows of one input with no match in another", OLS -> optimizerJoinFilter)((s, f) =>
      f.orders(s).join(f.customers(s), Seq("customer_id"), "left_anti").select("id", "amount")),
    job("full-outer-using", "Join", "a full outer join USING a key: the key is either side's", OLS -> choice("A full outer join's USING key is coalesce(left key, right key): openlineage-spark counts each key as a condition of the value (INDIRECT/CONDITIONAL) as well as its value; Invaract counts it as the value, and both keys as JOIN."))((s, f) =>
      f.orders(s).join(f.customers(s), Seq("customer_id"), "full_outer").select("customer_id", "id", "name")),
    job("non-equi-join", "Join", "a join on an inequality", OLS -> optimizerJoinFilter)((s, f) => {
      val o = f.orders(s).as("o"); val c = f.customers(s).as("c")
      o.join(c, col("o.amount") > col("c.customer_id") * 5).select(col("o.id"), col("c.name"))
    }),
    job("cross-join", "Join", "a cross join")((s, f) => f.orders(s).crossJoin(f.customers(s).select("country")).select("id", "country")),
    job("self-join-same-frame", "Join", "one DataFrame joined with a filter of itself (Spark renames one side's columns)", OLS -> optimizerJoinFilter)((s, f) => {
      val o = f.orders(s); val paid = o.filter(col("status") === "paid")
      o.as("x").join(paid.as("y"), col("x.customer_id") === col("y.customer_id")).select(col("x.id"), col("y.amount").as("paid_amount"))
    }),
    job("lateral-join", "Join", "a LATERAL subquery correlated with the outer row", OLS -> gap("Invaract does not translate a LATERAL join (LateralJoin): id and name have no source, and the outer column the subquery correlates on (orders.customer_id) is missing."), Spline -> gap("Invaract does not translate a LATERAL join (LateralJoin): id and name have no source. Spline traces the outer side (id) but not the subquery: its input and the columns it filters on."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT o.id, t.name FROM orders o, LATERAL (SELECT max(c.name) AS name FROM customers c WHERE c.customer_id = o.customer_id) t")
    }),
    job("join-with", "Join", "a typed joinWith, then fields of the two structs it makes", OLS -> gap("joinWith wraps each side's row in a struct (_1, _2). Invaract traces a field taken from such a struct to every column of the struct, as TRANSFORMATION, not to the field's own column. (openlineage-spark also reports the join keys as FILTER: the optimizer's isnotnull filter.)"))((s, f) => {
      val o = f.orders(s); val c = f.customers(s)
      o.joinWith(c, o("customer_id") === c("customer_id")).select(col("_1.id").as("id"), col("_2.name").as("name"))
    }),

    // --- aggregation, harder cases ---------------------------------------------------------------------------------------
    job("having", "Aggregation", "HAVING on aggregates that are not written", OLS -> theirs("HAVING's max(status) decides which groups are kept, which Invaract reports as INDIRECT/FILTER; openlineage-spark reports it as an aggregation, DIRECT/AGGREGATION, in the dataset-wide list where only indirect types belong."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id HAVING count(*) > 1 AND max(status) = 'paid'")
    }),
    job("aggregate-filter", "Aggregation", "an aggregate with a FILTER clause", Spline -> gap("An aggregate's FILTER clause decides which rows it sums, so paid depends on status. Invaract does not report that, nor does openlineage-spark; Spline does."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("SELECT customer_id, sum(amount) FILTER (WHERE status = 'paid') AS paid FROM orders GROUP BY customer_id")
    }),
    job("distinct-collect", "Aggregation", "count distinct and collect_list")((s, f) =>
      f.orders(s).groupBy("customer_id").agg(countDistinct("status").as("statuses"), collect_list("id").as("ids"))),
    job("group-by-expression", "Aggregation", "grouping by a computed key", OLS -> theirs("Grouping by upper(status): Invaract reports status as GROUP_BY; openlineage-spark gives it DIRECT/TRANSFORMATION in the dataset-wide list, where only indirect types belong."))((s, f) =>
      f.orders(s).groupBy(upper(col("status")).as("s")).agg(avg("amount").as("mean"))),
    job("cube-grouping-id", "Aggregation", "cube with grouping_id()", OLS -> theirs("openlineage-spark does not trace a cube's grouping keys through the Expand Spark computes it with, so it reports no source for them and no grouping. Invaract traces each key to its column."), Spline -> theirs("Spline does not trace a cube's grouping keys through Spark's Expand either. Invaract traces each key to its column."))((s, f) =>
      f.orders(s).cube("customer_id", "status").agg(grouping_id().as("level"), sum("amount").as("total"))),
    job("lag", "Window", "lag over a partition")((s, f) =>
      f.orders(s).select(col("id"), lag("amount", 1).over(Window.partitionBy("customer_id").orderBy("id")).as("previous"))),
    job("window-frame", "Window", "a moving average over a row frame, no partition")((s, f) =>
      f.orders(s).select(col("id"), avg("amount").over(Window.orderBy("id").rowsBetween(-1, 0)).as("moving"))),
    job("unpivot", "Aggregation", "unpivot: two columns into rows of name and value", OLS -> theirs("openlineage-spark does not trace through the Expand Spark computes an unpivot with: value has no source. Invaract traces it to both unpivoted columns."), Spline -> theirs("Spline does not trace through an unpivot's Expand either. Invaract traces value to both unpivoted columns."))((s, f) =>
      f.orders(s).unpivot(Array(col("id")), Array(col("amount"), col("customer_id")), "metric", "value")),
    job("sql-stack", "Aggregation", "SQL stack(): two columns into rows")((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("SELECT id, stack(2, 'amount', amount, 'customer', customer_id) AS (metric, value) FROM orders")
    }),
    job("posexplode-outer", "Nested data", "posexplode_outer of an array")((s, f) =>
      f.events(s).select(col("id"), posexplode_outer(col("tags")).as(Seq("pos", "tag")))),
    job("lateral-view", "Nested data", "SQL LATERAL VIEW explode", OLS -> theirs("openlineage-spark reads the optimized plan, where Spark adds a size(tags) > 0 filter before an explode; it reports the array column as FILTER too. Invaract reads the plan the job wrote."))((s, f) => {
      f.events(s).createOrReplaceTempView("events")
      s.sql("SELECT id, tag FROM events LATERAL VIEW explode(tags) t AS tag")
    }),

    // --- typed Dataset operations -----------------------------------------------------------------------------------------
    job("typed-filter", "Dataset API", "a typed filter (a Scala function), then a column", OLS -> gap("Invaract does not translate a typed filter (TypedFilter, a Scala function over each row): no column is traced through it."), Spline -> gap("Invaract does not translate a typed filter (TypedFilter, a Scala function over each row): no column is traced through it."))((s, f) => {
      import s.implicits._
      f.orders(s).as[OrderRow].filter(o => o.amount > 60).select("id")
    }),
    job("group-by-key", "Dataset API", "groupByKey and mapGroups")((s, f) => {
      import s.implicits._
      f.orders(s).as[OrderRow].groupByKey(_.customer_id).mapGroups((k, rows) => (k, rows.map(_.amount).sum)).toDF("customer_id", "total")
    }),
    job("flat-map", "Dataset API", "a typed flatMap")((s, f) => {
      import s.implicits._
      f.orders(s).as[OrderRow].flatMap(o => Seq(o.id, o.amount)).toDF("value")
    }),

    // --- set operations, harder cases ---------------------------------------------------------------------------------------
    job("union-positional", "Set operation", "a union matching columns by position, not name", OLS -> gap("A union matches columns by position. Invaract matches them by name, so where the second side's columns are in another order it traces each output column to the wrong one: id should come from both id and customer_id, and customer_id from both customer_id and id."), Spline -> gap("A union matches columns by position. Invaract matches them by name, so where the second side's columns are in another order it traces each output column to the wrong one: id should come from both id and customer_id, and customer_id from both customer_id and id."))((s, f) =>
      f.orders(s).select("id", "customer_id").union(f.orders(s).select("customer_id", "id"))),
    job("union-by-name", "Set operation", "unionByName with the columns in another order")((s, f) =>
      f.orders(s).select("id", "amount").unionByName(f.orders(s).select(col("amount"), col("customer_id").as("id")))),
    job("union-missing", "Set operation", "unionByName allowing missing columns")((s, f) =>
      f.orders(s).select("id", "amount").unionByName(f.customers(s).select(col("customer_id").as("id")), allowMissingColumns = true)),
    job("intersect", "Set operation", "intersect of two inputs' keys", OLS -> gap("Invaract does not translate INTERSECT or EXCEPT: the output column has no source, and neither side's key is reported as deciding which rows are kept."), Spline -> gap("Invaract does not translate INTERSECT or EXCEPT: the output column has no source, and neither side's key is reported as deciding which rows are kept."))((s, f) =>
      f.orders(s).select("customer_id").intersect(f.customers(s).select("customer_id"))),
    job("except-all", "Set operation", "exceptAll of two inputs' keys", OLS -> gap("Invaract does not translate INTERSECT or EXCEPT: the output column has no source, and neither side's key is reported as deciding which rows are kept. (openlineage-spark, reading Spark's rewrite of EXCEPT ALL as a union, also reports the subtracted side's column as a source of the value.)"), Spline -> gap("Invaract does not translate INTERSECT or EXCEPT: the output column has no source, and neither side's key is reported as deciding which rows are kept."))((s, f) =>
      f.orders(s).select("customer_id").exceptAll(f.customers(s).select("customer_id"))),

    // --- views and subqueries -----------------------------------------------------------------------------------------------
    job("nested-views", "SQL", "a temporary view over a temporary view", OLS -> gap("Spark puts a cast to each column's own type above a view's query. Invaract counts that cast as a TRANSFORMATION, so a column the view passes through unchanged is not IDENTITY."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("CREATE OR REPLACE TEMP VIEW paid AS SELECT id, amount FROM orders WHERE status = 'paid'")
      s.sql("SELECT id, amount * 2 AS doubled FROM paid")
    }),
    job("catalog-table-read", "Catalog tables", "a catalog table (over existing files) read by name")((s, f) => {
      s.sql(s"CREATE TABLE orders_t USING parquet LOCATION '${f.orders}'")
      s.table("orders_t").select("id", "amount")
    }),
    job("permanent-view", "SQL", "a permanent SQL view over a catalog table", OLS -> gap("Spark puts a cast to each column's own type above a view's query. Invaract counts that cast as a TRANSFORMATION, so a column the view passes through unchanged is not IDENTITY."))((s, f) => {
      s.sql(s"CREATE TABLE orders_t USING parquet LOCATION '${f.orders}'")
      s.sql("CREATE VIEW big AS SELECT id, amount FROM orders_t WHERE amount > 60")
      s.table("big").select(col("id"), (col("amount") + 1).as("next"))
    }),
    job("correlated-scalar", "SQL", "a correlated scalar subquery in the select list", OLS -> gap("The subquery's value depends on the outer row's customer_id, which it correlates on; Invaract does not report that column. (openlineage-spark, reading the subquery rewritten as a join and an aggregate, also types customers.customer_id JOIN and GROUP_BY.)"), Spline -> gap("Spline reports the outer column the subquery correlates on (orders.customer_id), which Invaract misses; Spline misses the subquery's own input, customers, and the column its value comes from."))((s, f) => {
      f.orders(s).createOrReplaceTempView("orders"); f.customers(s).createOrReplaceTempView("customers")
      s.sql("SELECT id, (SELECT max(c.name) FROM customers c WHERE c.customer_id = o.customer_id) AS name FROM orders o")
    }),
    job("cache-table", "Caching", "CACHE TABLE ... AS SELECT, then a query of it")((s, f) => {
      f.orders(s).createOrReplaceTempView("orders")
      s.sql("CACHE TABLE paid AS SELECT id, amount FROM orders WHERE status = 'paid'")
      s.sql("SELECT id, amount FROM paid")
    }),

    // --- inputs, harder cases --------------------------------------------------------------------------------------------------
    job("partition-column-input", "Inputs", "a partitioned input: a column that comes from the directory names")((s, f) =>
      s.read.parquet(f.ordersByStatus).select(col("id"), col("status"))),
    job("file-metadata", "Inputs", "the file each row came from (input_file_name, _metadata)")((s, f) =>
      f.orders(s).select(col("id"), input_file_name().as("file"), col("_metadata.file_name").as("name"))),
    job("multiple-paths", "Inputs", "one read of two directories", OLS -> gap("One read of two directories: Invaract traces the columns to the first directory only; the second is an input with no column lineage."), Spline -> theirs("Spline reports only the first directory of a read of several."))((s, f) => s.read.parquet(f.orders, f.ordersArchive).select("id", "amount")),
    job("glob-path", "Inputs", "a read through a glob pattern")((s, f) => s.read.parquet(f.orders + "/*.parquet").select("id", "amount")),
    job("local-data", "Inputs", "a join with data defined in the job (a local relation)", OLS -> optimizerJoinFilter)((s, f) => {
      import s.implicits._
      f.orders(s).join(Seq(("open", "O"), ("paid", "P")).toDF("status", "code"), "status").select("id", "code")
    }),
    Job("delta-time-travel", "Delta Lake", "a Delta table read as of an earlier version",
      (s, f, out) => s.read.format("delta").option("versionAsOf", "0").load(f.ordersDelta).select("id", "amount")
        .write.mode("overwrite").parquet(out.at("out")),
      extensions = delta, conf = deltaConf),
    job("single-file", "Inputs", "a read of one file, not a directory")((s, f) => {
      val file = java.nio.file.Files.list(java.nio.file.Paths.get(f.orders)).iterator().asScala.map(_.toString).filter(_.endsWith(".parquet")).toList.min
      s.read.parquet(file).select("id", "amount")
    }),

    // --- plan boundaries, harder cases ------------------------------------------------------------------------------------------
    Job("checkpoint", "Caching", "a projection of a (reliably) checkpointed DataFrame",
      (s, f, out) => {
        s.sparkContext.setCheckpointDir(out.at("checkpoints"))
        f.orders(s).checkpoint().select(col("id"), col("amount")).write.mode("overwrite").parquet(out.at("out"))
      },
      notes = Map(OLS -> theirs("openlineage-spark does not see past a checkpoint(). Invaract resolves the checkpoint to the plan it was made from and traces through it."), Spline -> theirs("Spline does not see past a checkpoint() either."))),
    job("observe", "Physical", "observe(): metrics collected as the rows pass", OLS -> gap("Invaract does not translate observe() (CollectMetrics), so no column is traced through it."), Spline -> gap("Invaract does not translate observe() (CollectMetrics), so no column is traced through it."))((s, f) =>
      f.orders(s).observe("metrics", count(lit(1)).as("rows")).select("id", "amount")),
    job("sample", "Physical", "a random sample", OLS -> gap("Invaract does not translate sample() (Sample), so no column is traced through it."), Spline -> gap("Invaract does not translate sample() (Sample), so no column is traced through it."))((s, f) => f.orders(s).sample(0.5, 42L).select("id")),
    job("offset", "Sort", "rows after the first, by order (offset)", OLS -> gap("Invaract does not translate offset() (Offset), so no column is traced through it; it reports only the sort below it."), Spline -> gap("Invaract does not translate offset() (Offset), so no column is traced through it; it reports only the sort below it."))((s, f) => f.orders(s).orderBy("id").offset(1).select("id")),

    // --- writes, harder cases -----------------------------------------------------------------------------------------------------
    Job("insert-column-list", "Catalog tables", "INSERT INTO a table with a column list in another order",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("CREATE TABLE pairs (a BIGINT, b BIGINT) USING parquet")
        s.sql("INSERT INTO pairs (b, a) SELECT id, amount FROM orders")
      },
      contractLocation = tableLocation("pairs")),
    Job("insert-static-partition", "Catalog tables", "INSERT OVERWRITE of one static partition",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("CREATE TABLE by_status (id BIGINT, amount BIGINT, status STRING) USING parquet PARTITIONED BY (status)")
        s.sql("INSERT OVERWRITE by_status PARTITION (status = 'paid') SELECT id, amount FROM orders WHERE status = 'paid'")
      },
      contractLocation = tableLocation("by_status")),
    Job("insert-directory", "Writes", "SQL INSERT OVERWRITE DIRECTORY",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql(s"INSERT OVERWRITE DIRECTORY '${out.at("out")}' USING parquet SELECT id, amount FROM orders")
      }),
    Job("bucketed-table", "Catalog tables", "saveAsTable bucketed and sorted",
      (s, f, out) => f.orders(s).select("id", "customer_id").write.bucketBy(2, "customer_id").sortBy("id").saveAsTable("bucketed"),
      contractLocation = tableLocation("bucketed"),
      notes = Map(OLS -> choice("openlineage-spark reports a bucketed write's bucket and sort columns as shaping the rows (INDIRECT); Invaract, as for partitionBy, reports how the write lays out its files as no lineage."))),
    Job("json-output", "Formats", "a JSON output",
      (s, f, out) => f.orders(s).select("id", "status").write.mode("overwrite").json(out.at("out"))),
    Job("jdbc-output", "JDBC", "a table written over JDBC",
      (s, f, out) => f.orders(s).select("id", "amount").write.mode("overwrite").jdbc(s"jdbc:h2:${out.at("db")}", "TOTALS", new java.util.Properties()),
      contractLocation = out => s"jdbc:jdbc:h2:${out.at("db")}/TOTALS",
      notes = Map(Spline -> theirs("Spline cannot parse an H2 file URL and names the table jdbc://:null."))),
    Job("delta-update", "Delta Lake", "UPDATE of a Delta table",
      (s, f, out) => {
        s.sql(s"CREATE TABLE delta.`${out.at("out")}` (id BIGINT, amount BIGINT, status STRING) USING delta")
        s.sql(s"UPDATE delta.`${out.at("out")}` SET amount = amount * 2 WHERE status = 'paid'")
      },
      extensions = delta, conf = deltaConf,
      notes = Map(OLS -> gap("An UPDATE writes the table's rows back to it. Invaract reports the table as the write's input, which openlineage-spark does not, but it traces each column as a copy: it misses the SET expression (amount * 2, a TRANSFORMATION) and the WHERE condition (status, which decides the value of amount)."))),
    Job("delta-delete", "Delta Lake", "DELETE from a Delta table",
      (s, f, out) => {
        s.sql(s"CREATE TABLE delta.`${out.at("out")}` (id BIGINT, amount BIGINT, status STRING) USING delta")
        s.sql(s"DELETE FROM delta.`${out.at("out")}` WHERE status = 'open'")
      },
      extensions = delta, conf = deltaConf,
      notes = Map(OLS -> gap("Invaract reports the table as the input of the rows a DELETE keeps, which openlineage-spark does not, but it misses the WHERE condition (status, FILTER) that decides which rows are kept. For an Iceberg DELETE it reports it."))),
    Job("delta-replace-where", "Delta Lake", "a Delta overwrite of the rows a predicate selects (replaceWhere)",
      (s, f, out) => {
        s.sql(s"CREATE TABLE delta.`${out.at("out")}` (id BIGINT, amount BIGINT, status STRING) USING delta")
        f.orders(s).filter(col("status") === "paid").select("id", "amount", "status")
          .write.format("delta").mode("overwrite").option("replaceWhere", "status = 'paid'").save(out.at("out"))
      },
      extensions = delta, conf = deltaConf,
      notes = Map(OLS -> choice("Delta carries out a replaceWhere on a data column by also rewriting the files that hold rows the predicate does not select: it reads them back from the table and writes them again, in a second write of its own. Invaract checks and reports that write too, so the table is an input of its own; openlineage-spark does not see Delta's internal write."), Spline -> choice("Delta carries out a replaceWhere on a data column by also rewriting the files that hold rows the predicate does not select: it reads them back from the table and writes them again, in a second write of its own. Invaract checks and reports that write too, so the table is an input of its own; Spline does not see Delta's internal write."))),
    Job("iceberg-merge", "Apache Iceberg", "MERGE INTO an Iceberg table",
      (s, f, out) => {
        s.sql("CREATE TABLE ice.db.acct (id BIGINT, amount BIGINT) USING iceberg")
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("MERGE INTO ice.db.acct t USING orders o ON t.id = o.id " +
          "WHEN MATCHED THEN UPDATE SET t.amount = t.amount + o.amount WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (o.id, o.amount)")
      },
      contractLocation = _ => "db/acct",
      extensions = iceberg, conf = icebergConf,
      notes = Map(OLS -> gap("Invaract does not translate the MergeRows node Spark plans an Iceberg MERGE with, so no column of the table is traced: only the ON condition's columns are reported. It also names the table by its catalog identifier, where openlineage-spark names it by its location (both name a table appended to by its location)."), Spline -> theirs("Spline's agent records nothing for an Iceberg MERGE."))),
    Job("iceberg-update", "Apache Iceberg", "UPDATE of an Iceberg table",
      (s, f, out) => {
        s.sql("CREATE TABLE ice.db.upd (id BIGINT, amount BIGINT, status STRING) USING iceberg")
        s.sql("UPDATE ice.db.upd SET amount = amount * 2 WHERE status = 'paid'")
      },
      contractLocation = _ => "db/upd",
      extensions = iceberg, conf = icebergConf,
      notes = Map(OLS -> gap("Invaract names a table an UPDATE changes by its catalog identifier, where openlineage-spark names it by its location (both name a table appended to by its location). It also reports the columns the UPDATE leaves unchanged as computed and conditional on status: Spark's plan writes them as if(status = 'paid', id, id). openlineage-spark reports Iceberg's _file metadata column as shaping the rows."), Spline -> theirs("Spline's agent records nothing for an Iceberg UPDATE."))),
    Job("iceberg-delete", "Apache Iceberg", "DELETE from an Iceberg table",
      (s, f, out) => {
        s.sql("CREATE TABLE ice.db.del (id BIGINT, amount BIGINT, status STRING) USING iceberg")
        s.sql("DELETE FROM ice.db.del WHERE status = 'open'")
      },
      contractLocation = _ => "db/del",
      extensions = iceberg, conf = icebergConf,
      notes = Map(OLS -> gap("openlineage-spark reports no input and no column lineage for an Iceberg DELETE; Invaract reports the rows it keeps and the condition that decides them, but names the table by its catalog identifier, where openlineage-spark names it by its location (both name a table appended to by its location)."), Spline -> theirs("Spline's agent records nothing for an Iceberg DELETE."))),
    Job("iceberg-overwrite-partitions", "Apache Iceberg", "overwritePartitions of an Iceberg table",
      (s, f, out) => {
        s.sql("CREATE TABLE ice.db.by_status (id BIGINT, amount BIGINT, status STRING) USING iceberg PARTITIONED BY (status)")
        f.orders(s).select("id", "amount", "status").writeTo("ice.db.by_status").overwritePartitions()
      },
      contractLocation = out => "file:" + out.at("ice") + "/db/by_status",
      extensions = iceberg, conf = icebergConf),
    Job("iceberg-rtas", "Apache Iceberg", "CREATE OR REPLACE TABLE ... AS SELECT",
      (s, f, out) => {
        f.orders(s).createOrReplaceTempView("orders")
        s.sql("CREATE OR REPLACE TABLE ice.db.paid USING iceberg AS SELECT id, amount FROM orders WHERE status = 'paid'")
      },
      contractLocation = _ => "ice.db.paid",
      extensions = iceberg, conf = icebergConf),
    Job("foreach-batch", "Structured Streaming", "a stream whose micro-batches are written by foreachBatch",
      (s, f, out) => {
        val write: (DataFrame, Long) => Unit = (batch, _) => batch.write.mode("append").parquet(out.at("out"))
        val q = s.readStream.schema(f.orders(s).schema).parquet(f.orders).select(col("id"), (col("amount") * 3).as("tripled"))
          .writeStream.option("checkpointLocation", out.at("checkpoint")).foreachBatch(write)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow()).start()
        q.awaitTermination()
      },
      notes = Map(OLS -> gap("Invaract checks each micro-batch a foreachBatch function writes and publishes its lineage, but names the batch's input by the files that batch read, so each batch reads new datasets; openlineage-spark names the stream's source directory."), Spline -> theirs("Spline records each micro-batch's write with no input and no column lineage.")))

  )
}
