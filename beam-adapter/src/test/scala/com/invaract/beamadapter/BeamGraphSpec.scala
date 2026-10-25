// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.{FileIO, GenerateSequence}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.schemas.{Schema => BeamSchema}
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.Row
import org.scalatest.funsuite.AnyFunSuite

/** What the graph walk reports about a constructed pipeline, before anything runs. */
class BeamGraphSpec extends AnyFunSuite {

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  test("the top-level steps come in the order data flows, with the composite steps holding their parts") {
    val p = pipeline
    p.apply("ReadOrders", ParquetIO.read(ExampleJob.OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(ExampleJob.OrdersSchema)).to("/data/out/report"))
    val steps = BeamGraph.of(p)
    assert(steps.map(_.name) == List("ReadOrders", "WriteReport"))
    assert(steps.forall(_.isComposite), "an IO transform is made of smaller steps")
    assert(steps.head.withDescendants.size > 1 && steps.head.withDescendants.head == steps.head)
    assert(steps.head.display.get("filePattern").contains("/data/in/orders/*.parquet"))
    assert(steps.head.className.endsWith("AutoValue_ParquetIO_Read"))
  }

  test("a step with nothing inside it is not a composite") {
    val p = pipeline
    p.apply("Rows", Create.of("a"))
    val all = BeamGraph.of(p).flatMap(_.withDescendants)
    assert(all.exists(!_.isComposite))
    assert(all.filter(!_.isComposite).forall(_.children.isEmpty))
  }

  test("a collection says whether it is finite and which row schema it carries") {
    val p = pipeline
    val schema = BeamSchema.builder().addInt64Field("id").build()
    p.apply("Bounded", Create.of(Row.withSchema(schema).addValues(java.lang.Long.valueOf(1L)).build()).withRowSchema(schema))
    p.apply("Unbounded", GenerateSequence.from(0))
    val steps = BeamGraph.of(p).map(s => s.name -> s).toMap
    val bounded = steps("Bounded").outputs.head
    assert(bounded.bounded)
    assert(bounded.rowSchema.contains(schema))
    val unbounded = steps("Unbounded").outputs.head
    assert(!unbounded.bounded)
    assert(unbounded.rowSchema.isEmpty)
  }

  test("a collection of Avro records carries a coder; its input collections are the previous step's outputs") {
    val p = pipeline
    p.apply("ReadOrders", ParquetIO.read(ExampleJob.OrdersSchema).from("/data/in/orders/*.parquet"))
      .apply("WriteReport", FileIO.write[GenericRecord]().via(ParquetIO.sink(ExampleJob.OrdersSchema)).to("/data/out/report"))
    val Seq(read, write) = BeamGraph.of(p)
    assert(read.outputs.head.coder.isDefined)
    assert(write.inputs.map(_.name) == read.outputs.map(_.name))
  }
}
