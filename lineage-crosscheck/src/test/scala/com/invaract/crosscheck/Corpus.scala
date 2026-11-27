// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path}

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

  def read(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)
  def orders(spark: SparkSession): DataFrame = read(spark, orders)
  def customers(spark: SparkSession): DataFrame = read(spark, customers)
  def events(spark: SparkSession): DataFrame = spark.read.json(events)
}

object Fixture {
  def create(): Fixture = {
    val fixture = new Fixture(Files.createTempDirectory("lineage-crosscheck"))
    val spark = SparkSession.builder().master("local[1]").appName("fixture").config("spark.ui.enabled", "false").getOrCreate()
    try {
      import spark.implicits._
      Seq((1L, 10L, 100L, "open"), (2L, 20L, 200L, "paid"), (3L, 10L, 50L, "paid"))
        .toDF("id", "customer_id", "amount", "status").write.parquet(fixture.orders)
      Seq((10L, "Ann", "uk", "ann@example.com"), (20L, "Bo", "fr", "bo@example.com"))
        .toDF("customer_id", "name", "country", "email").write.parquet(fixture.customers)
      Seq("""{"id":1,"payload":{"a":5,"b":"x"},"tags":["t1","t2"]}""", """{"id":2,"payload":{"a":7,"b":"y"},"tags":["t3"]}""")
        .toDS().write.text(fixture.events)
      Seq(("north", 10L), ("south", 20L)).toDF("region", "amount").write.option("header", "true").csv(fixture.salesCsv)
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
      notes = Map(OLS -> choice("Spark's analyzer casts each column to the table's column type, here to the type it already has; Invaract reports the cast in the plan as a TRANSFORMATION, openlineage-spark as IDENTITY."))),
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
      s.read.jdbc(f.jdbcUrl, "ACCOUNTS", new java.util.Properties()).select(col("ID").as("id"), col("BALANCE").as("balance")))
  )
}
