// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.CatalogIdentity
import org.scalatest.funsuite.AnyFunSuite

/** How an IO's own name for its data becomes a location (`BeamLocations`), and BigQuery's table schema as columns. */
class BeamLocationsSpec extends AnyFunSuite {
  import BeamLocations._

  private def bq(project: Option[String], dataset: String, table: String) =
    BigQueryTable(
      (project.toList ++ List(dataset, table)).mkString("/"),
      CatalogIdentity(technology = Some("bigquery"), namespace = project.toList :+ dataset, table = Some(table))
    )

  test("a BigQuery table spec is located by its parts, in every spelling Beam accepts") {
    assert(bigQueryTable("proj:ds.orders").contains(bq(Some("proj"), "ds", "orders")))
    assert(bigQueryTable("proj.ds.orders").contains(bq(Some("proj"), "ds", "orders")))
    assert(bigQueryTable("ds.orders").contains(bq(None, "ds", "orders")))
    assert(bigQueryTable(" proj:ds.orders ").contains(bq(Some("proj"), "ds", "orders")), "surrounding whitespace is not part of a name")
    // a domain-scoped legacy project id contains '.': only the part after the last ':' is dataset.table
    assert(bigQueryTable("example.com:proj:ds.orders").contains(bq(Some("example.com:proj"), "ds", "orders")))
    assert(bigQueryTable("""{"projectId":"proj","datasetId":"ds","tableId":"orders"}""").contains(bq(Some("proj"), "ds", "orders")))
    assert(bigQueryTable("""{"datasetId":"ds","tableId":"orders"}""").contains(bq(None, "ds", "orders")))
    assert(bigQueryTable("proj:ds.orders$20240101").contains(bq(Some("proj"), "ds", "orders")), "a partition decorator names a partition of the table")
    assert(bigQueryTable("""{"projectId":"proj","datasetId":"ds","tableId":"orders$2024"}""").contains(bq(Some("proj"), "ds", "orders")))
  }

  test("a BigQuery table spec that is not one names no table") {
    Seq("orders", "a.b.c.d", "proj:orders", "proj:a.b.c", """{"tableId":"orders"}""", """{"datasetId":"ds"}""", """{"datasetId":"","tableId":"t"}""", "{not json")
      .foreach(spec => assert(bigQueryTable(spec).isEmpty, spec))
  }

  test("a Kafka topic is located under the servers it is read from, or alone when none are stated") {
    assert(kafkaTopic(Some("broker:9092"), "orders") == "broker:9092/orders")
    assert(kafkaTopic(None, "orders") == "orders")
  }

  test("a JDBC table is located under the connection URL, a qualified name by its parts") {
    assert(jdbcTable("jdbc:postgresql://db/shop", "orders") == "jdbc:postgresql://db/shop/orders")
    assert(jdbcTable("jdbc:postgresql://db/shop/", "public.orders") == "jdbc:postgresql://db/shop/public/orders")
    assert(jdbcTable("jdbc:mysql://db/shop", "`orders`") == "jdbc:mysql://db/shop/orders", "the engine's own quoting is not part of the name")
  }

  test("a query reads one table only when it is a plain SELECT ... FROM that table") {
    Map(
      "select id from orders" -> "orders",
      "SELECT id, amount FROM public.orders WHERE amount > 0" -> "public.orders",
      "select * from orders o where o.id > 1" -> "orders",
      "select * from orders as o" -> "orders",
      "select id from orders group by id" -> "orders",
      "select id from orders order by id" -> "orders",
      "select id from orders limit 10;" -> "orders",
      "select id from \"Orders\"" -> "\"Orders\"",
      "select id\nfrom orders\nwhere x in (1, 2)" -> "orders"
    ).foreach { case (query, table) => assert(singleTableOf(query).contains(table), query) }
  }

  test("a query that names another table, or is not a plain SELECT, reads no single table") {
    Seq(
      "select o.id from orders o join customers c on o.cid = c.id",
      "select id from orders, customers",
      "select id from orders union select id from refunds",
      "select id from orders intersect select id from refunds",
      "select id from orders except select id from refunds",
      "select id from (select id from orders) t",
      "select id from orders where id in (select id from refunds)",
      "with t as (select 1) select * from t",
      "delete from orders",
      "select 1"
    ).foreach(query => assert(singleTableOf(query).isEmpty, query))
  }

  test("an INSERT statement writes the table it names; anything else names none") {
    assert(insertTableOf("INSERT INTO report (id) VALUES (?)").contains("report"))
    assert(insertTableOf("  insert into public.report values (?)").contains("public.report"))
    assert(insertTableOf("update report set x = ?").isEmpty)
    assert(insertTableOf("merge into report using x").isEmpty)
  }

  test("a read pattern's folder is the pattern without a glob file name") {
    assert(folderOf("/data/in/orders/*.parquet") == "/data/in/orders")
    assert(folderOf("/data/in/orders/part-?.parquet") == "/data/in/orders")
    assert(folderOf("/data/in/orders/[ab].parquet") == "/data/in/orders")
    assert(folderOf("/data/in/orders/{a,b}.parquet") == "/data/in/orders")
    assert(folderOf("/data/in/orders/") == "/data/in/orders")
    assert(folderOf("/data/in/orders/part-0.parquet") == "/data/in/orders/part-0.parquet", "a plain file name is the file")
    assert(folderOf("C:\\data\\in\\*.avro") == "C:/data/in")
    assert(folderOf("*.parquet") == "")
    assert(folderOf("/data/in/orders/date=*/part-*.parquet") == "/data/in/orders", "a partitioned layout is the dataset's folder")
    assert(folderOf("/data/in/orders/**") == "/data/in/orders")
    assert(folderOf("/data/in/orders/**/*.parquet") == "/data/in/orders")
    assert(folderOf("/data/*/orders") == "/data/*/orders", "a glob before a plain segment is left as written")
    assert(folderOf("/data/*/orders/*.parquet") == "/data/*/orders")
  }

  // --- BigQuery table schemas ---

  private def field(name: String, t: LogicalType, nullable: Boolean = true) = LogicalField(name, t, nullable)

  test("a BigQuery table schema maps to the neutral types, its modes to nullability and arrays") {
    val json =
      """{"fields":[
        |{"name":"s","type":"STRING"},{"name":"b","type":"BYTES"},{"name":"i","type":"INTEGER","mode":"REQUIRED"},{"name":"i64","type":"INT64"},
        |{"name":"f","type":"FLOAT"},{"name":"f64","type":"FLOAT64"},{"name":"bool","type":"BOOLEAN"},{"name":"bool2","type":"BOOL"},
        |{"name":"n","type":"NUMERIC"},{"name":"n2","type":"NUMERIC","precision":"10","scale":"2"},
        |{"name":"bn","type":"BIGNUMERIC","precision":"38","scale":"4"},{"name":"bn2","type":"BIGNUMERIC"},{"name":"bn3","type":"BIGNUMERIC","precision":"39"},
        |{"name":"bn4","type":"BIGNUMERIC","precision":"20"},
        |{"name":"d","type":"DATE"},{"name":"dt","type":"DATETIME"},{"name":"t","type":"TIME"},{"name":"ts","type":"TIMESTAMP"},
        |{"name":"j","type":"JSON"},{"name":"g","type":"GEOGRAPHY"},{"name":"iv","type":"INTERVAL"},{"name":"r","type":"RANGE"},
        |{"name":"tags","type":"string","mode":"repeated"},
        |{"name":"addr","type":"RECORD","fields":[{"name":"city","type":"STRING","mode":"REQUIRED"}]},
        |{"name":"st","type":"STRUCT","fields":[{"name":"x","type":"INT64"}]},{"name":"empty","type":"RECORD"},{"name":"untyped"}
        |]}""".stripMargin
    assert(
      BeamSchemas.fromBigQueryJson(json).contains(
        LogicalSchema(List(
          field("s", LogicalType.StringType), field("b", LogicalType.BinaryType), field("i", LogicalType.LongType, nullable = false), field("i64", LogicalType.LongType),
          field("f", LogicalType.DoubleType), field("f64", LogicalType.DoubleType), field("bool", LogicalType.BooleanType), field("bool2", LogicalType.BooleanType),
          field("n", LogicalType.DecimalType(38, 9)), field("n2", LogicalType.DecimalType(10, 2)),
          field("bn", LogicalType.DecimalType(38, 4)), field("bn2", LogicalType.OtherType("bignumeric")), field("bn3", LogicalType.OtherType("bignumeric")),
          field("bn4", LogicalType.DecimalType(20, 0)),
          field("d", LogicalType.DateType), field("dt", LogicalType.TimestampNtzType), field("t", LogicalType.TimeType), field("ts", LogicalType.TimestampType),
          field("j", LogicalType.JsonType), field("g", LogicalType.GeographyType), field("iv", LogicalType.IntervalType), field("r", LogicalType.OtherType("range")),
          field("tags", LogicalType.ArrayType(LogicalType.StringType), nullable = false),
          field("addr", LogicalType.StructType(List(field("city", LogicalType.StringType, nullable = false)))),
          field("st", LogicalType.StructType(List(field("x", LogicalType.LongType)))), field("empty", LogicalType.OtherType("struct")), field("untyped", LogicalType.OtherType(""))
        ))
      )
    )
  }

  test("text that is not a BigQuery table schema has no columns") {
    Seq("Custom Schema Function", "{}", """{"fields":"x"}""", "[1,2]", "{not json").foreach(t => assert(BeamSchemas.fromBigQueryJson(t).isEmpty, t))
  }
}
