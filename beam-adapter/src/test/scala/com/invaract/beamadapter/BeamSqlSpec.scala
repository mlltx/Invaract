// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.ir.{DatasetRef, Plan, Read}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.{PBegin, PCollection, Row}
import org.scalatest.funsuite.AnyFunSuite

/** Beam SQL: a query is an opaque step like any function (its columns are known, its logic is not), and the tables a query
  * defines for itself (`CREATE EXTERNAL TABLE`) are real reads and writes inside it, which the translator sees. */
class BeamSqlSpec extends AnyFunSuite {

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  private def readsIn(plan: Plan): List[String] = plan match {
    case Read(DatasetRef(location), _, _) => List(location)
    case other                            => other.children.flatMap(readsIn)
  }

  test("a query over a read keeps the read beneath it, and what it writes has the query's columns") {
    val p = pipeline
    val totals: PCollection[Row] = p.apply("Read", ParquetIO.read(ExampleJob.OrdersSchema).withBeamSchemas(true).from("/data/in/orders/*.parquet"))
      .apply("Totals", SqlTransform.query("SELECT id, SUM(amount) AS total FROM PCOLLECTION GROUP BY id"))
    totals.apply("Write", BigQueryIO.write[Row]().to("proj:ds.report").useBeamSchema().withCreateDisposition(CreateDisposition.CREATE_NEVER))
    val Seq(w) = BeamTranslator.translate(p).writes
    assert(readsIn(w.plan) == List("/data/in/orders"))
    assert(w.outputSchema.exists(_.fields.map(_.name) == List("id", "total")))
  }

  test("the tables a query defines for itself are reads and writes the translator sees") {
    val p = pipeline
    PBegin.in(p).apply(
      "Sql",
      SqlTransform.query("INSERT INTO report SELECT id, SUM(amount) FROM orders GROUP BY id")
        .withDdlString("CREATE EXTERNAL TABLE orders (id BIGINT, amount BIGINT) TYPE text LOCATION '/data/in/orders/*'")
        .withDdlString("CREATE EXTERNAL TABLE report (id BIGINT, total BIGINT) TYPE text LOCATION '/data/out/report'")
    )
    val translation = BeamTranslator.translate(p)
    val Seq(w) = translation.writes
    assert(w.location.endsWith("/data/out/report") && w.format.contains("text"))
    assert(readsIn(w.plan) == List("/data/in/orders"))
    assert(w.outputSchema.exists(_.fields.map(_.name).head == "id"), "the rows the query writes have its columns")
  }
}
