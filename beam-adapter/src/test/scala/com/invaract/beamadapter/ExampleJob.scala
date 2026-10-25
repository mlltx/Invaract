// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Contract, ContractParser}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory

import java.nio.file.{Files, Path}

/** "A team's job": it reads orders from `in/orders` and writes them to `outputFolder`, and it knows nothing about Invaract -
  * it parses its command line the way any Beam job does, which is the only reason `--runner=...InvaractRunner` can reach it.
  * `BeamAttachSpec` runs it with and without the flags to show what a platform team gets without changing the job's source.
  */
final class ExampleJob(root: Path, outputFolder: String = "out/report", pinRunner: Boolean = false) {
  import ExampleJob._

  /** The job's `main`. With `pinRunner` it also sets its runner in code after reading its command line, as some real jobs do. */
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().create()
    if (pinRunner) options.setRunner(classOf[DirectRunner])
    val pipeline = Pipeline.create(options)
    build(pipeline)
    pipeline.run().waitUntilFinish()
  }

  /** The job's steps: read the orders, write them out. */
  def build(pipeline: Pipeline): Unit =
    pipeline
      .apply("ReadOrders", ParquetIO.read(OrdersSchema).from(s"${root.resolve("in/orders")}/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to(root.resolve(outputFolder).toString).withSuffix(".parquet"))

  /** The Parquet files the job wrote, by name (Beam leaves a `.temp-beam` working folder beside them). */
  def written: List[String] = {
    val dir = root.resolve(outputFolder)
    if (!Files.isDirectory(dir)) Nil
    else {
      val stream = Files.list(dir)
      try stream.toArray.toList.map(p => p.asInstanceOf[Path].getFileName.toString).filter(_.endsWith(".parquet")).sorted
      finally stream.close()
    }
  }
}

object ExampleJob {
  val OrdersSchema: Schema = new Schema.Parser().parse(
    """{"type":"record","name":"Orders","fields":[{"name":"id","type":"long"},{"name":"amount","type":["null","long"],"default":null}]}"""
  )

  /** An `in/orders` dataset under `root` with the given `(id, amount)` rows. */
  def withOrders(root: Path, rows: (Long, Long)*): Path = {
    val records = rows.toList.map { case (id, amount) =>
      val r = new GenericData.Record(OrdersSchema)
      r.put("id", java.lang.Long.valueOf(id))
      r.put("amount", java.lang.Long.valueOf(amount))
      r: GenericRecord
    }
    BeamScenarioJobs.materialize(root.resolve("in/orders"), OrdersSchema, records)
    root
  }

  /** The contract the job is held to: orders in, `out/report` out, both with `id` (required) and `amount`. */
  def contractYaml(outputLocation: String = "out/report", inputLocation: String = "in/orders"): String =
    s"""id: orders_report
       |version: "1.0.0"
       |inputs:
       |  - name: orders
       |    location: $inputLocation
       |    format: parquet
       |    schema:
       |      fields:
       |        - { name: id, type: long, required: true }
       |        - { name: amount, type: long }
       |outputs:
       |  - name: report
       |    location: $outputLocation
       |    format: parquet
       |    schema:
       |      fields:
       |        - { name: id, type: long, required: true }
       |        - { name: amount, type: long }
       |""".stripMargin

  def contract(outputLocation: String = "out/report", inputLocation: String = "in/orders"): Contract =
    ContractParser.parse(contractYaml(outputLocation, inputLocation))
}
