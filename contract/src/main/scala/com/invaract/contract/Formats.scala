// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors
package com.invaract.contract

/** The canonical vocabulary for a dataset's storage or serialization format - the values
  * `Dataset.format` is written in, and the values an engine adapter reports in `ir.Write.format`.
  *
  * Like `SaveModes`, the set is deliberately open: a contract may declare any string and the check
  * compares case-insensitively, because an engine can write a format none of these names. But an
  * adapter must report the names below for the formats that *are* in this list (Spark reports a
  * data source's registered short name, which already matches; a BigQuery adapter reports
  * `bigquery`, not `BIGQUERY_TABLE`), so one contract means the same thing on every engine.
  */
object Formats {
  // File formats.
  val Parquet = "parquet"
  val Orc = "orc"
  val Avro = "avro"
  val Csv = "csv"
  val Json = "json"
  val Text = "text"

  // Table formats.
  val Delta = "delta"
  val Iceberg = "iceberg"
  val Hudi = "hudi"

  /** A catalog table whose file layout the contract does not pin ("the data is a table"). */
  val Table = "table"

  // Stores a table lives in rather than a file layout.
  val Hive = "hive"
  val Jdbc = "jdbc"
  val BigQuery = "bigquery"
  val Kafka = "kafka"

  val All: Set[String] = Set(Parquet, Orc, Avro, Csv, Json, Text, Table, Delta, Iceberg, Hudi, Hive, Jdbc, BigQuery, Kafka)

  /** Whether `format` is one of the canonical names (case-insensitively, as the check compares). */
  def isCanonical(format: String): Boolean = All.contains(format.trim.toLowerCase)
}

/** The canonical vocabulary for `catalog.technology`: which catalog implementation a dataset is
  * registered in. Open in the same way as `Formats`. An engine's own catalog class names
  * (`SparkCatalog`, `JDBCTableCatalog`) are not technologies; an adapter maps them onto these.
  */
object CatalogTechnologies {
  val Hive = "hive"
  val Delta = "delta"
  val Iceberg = "iceberg"
  val Hudi = "hudi"
  val Jdbc = "jdbc"
  val Glue = "glue"
  val BigQuery = "bigquery"

  /** Spark's built-in non-persistent catalog (`spark.sql.catalogImplementation=in-memory`). */
  val InMemory = "in-memory"

  val All: Set[String] = Set(Hive, Delta, Iceberg, Hudi, Jdbc, Glue, BigQuery, InMemory)

  def isCanonical(technology: String): Boolean = All.contains(technology.trim.toLowerCase)
}
