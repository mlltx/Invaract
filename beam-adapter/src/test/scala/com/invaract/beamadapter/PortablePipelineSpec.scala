// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import org.apache.beam.model.pipeline.v1.RunnerApi
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.util.construction.PipelineTranslation
import org.scalatest.funsuite.AnyFunSuite

/** A pipeline read from its portable form (docs/BEAM_PYTHON_ADAPTER.md, D2): the recordings are real Python pipelines
  * (`beam-adapter/python/tests/record_fixtures.py`); the few built here are shapes an SDK does not produce today. */
class PortablePipelineSpec extends AnyFunSuite {
  import PortablePipelineSpec._

  test("the Python SDK's wrapper around the whole pipeline is not a step: its children are the job's own steps") {
    assert(fixture("parquet_job").map(_.name) == List("ReadOrders", "Positive", "Project", "WriteReport"))
  }

  test("a step is named by the class its python_type annotation names, else by its URN") {
    val top = fixture("parquet_job")
    assert(top.map(_.className) == List(
      "apache_beam.io.parquetio.ReadFromParquet", "apache_beam.transforms.core.ParDo", "apache_beam.transforms.core.ParDo",
      "apache_beam.io.parquetio.WriteToParquet"
    ))
    val noAnnotation = steps(pipeline(transform("Read", "beam:transform:read:v1")))
    assert(noAnnotation.map(_.className) == List("beam:transform:read:v1"))
    val emptyAnnotation = steps(pipeline(transform("Read", "beam:transform:read:v1", annotations = Map("python_type" -> ""))))
    assert(emptyAnnotation.map(_.className) == List("beam:transform:read:v1"), "an empty annotation names nothing")
  }

  test("display data by key, with the class that published each item; a key published twice keeps its first value") {
    val read = fixture("parquet_job").head
    assert(read.display("file_pattern") == "data/in/orders/*.parquet")
    assert(read.displayNamespaces("file_pattern") == "apache_beam.io.parquetio._ParquetSource")
    val print = fixture("side_effects").find(_.name == "PushToService").get
    // the ParDo publishes `fn` first (the wrapper class), then the function it wraps publishes `fn` again
    assert(print.display("fn") == "apache_beam.transforms.core.CallableWrapperDoFn")
    assert(print.displayNamespaces("fn") == "apache_beam.transforms.core.ParDo")
  }

  test("display values of every kind are text; an item with no key is known by its label; only labelled items are read") {
    val step = steps(pipeline(transform("S", "u", display = List(
      labelled("ns", "s", "label s", _.setStringValue("text")),
      labelled("ns", "b", "label b", _.setBoolValue(true)),
      labelled("ns", "i", "label i", _.setIntValue(8192)),
      labelled("ns", "d", "label d", _.setDoubleValue(1.5)),
      labelled("ns", "", "No Key", _.setStringValue("by label")),
      labelled("ns", "unset", "label unset", identity),
      // an item of another kind, even one whose payload reads as a labelled one, is not display data this adapter reads
      labelled("ns", "other", "label other", _.setStringValue("not labelled")).toBuilder.setUrn("beam:display_data:other:v1").build(),
      RunnerApi.DisplayData.newBuilder().setUrn(PortablePipeline.LabelledDisplayData).setPayload(spec("x").getUrnBytes).build()
    )))).head
    assert(step.display == Map("s" -> "text", "b" -> "true", "i" -> "8192", "d" -> "1.5", "No Key" -> "by label"))
  }

  test("a collection is named by its id, with the coder Java knows for it; a pickled Python coder is no coder") {
    val top = fixture("parquet_dicts")
    val dicts = top.head.outputs.head
    assert(dicts.coder.isEmpty && dicts.rowSchema.isEmpty)
    val lines = top(1).outputs.head
    assert(lines.coder.contains(StringUtf8Coder.of()))
    assert(top(2).inputs.map(_.name) == List(lines.name), "the write reads what the step before it makes, by the same name")
  }

  test("a row coder brings its schema, with the types and nullability Python gave it") {
    val rows = fixture("parquet_job").find(_.name == "Project").get.outputs.head
    assert(rows.rowSchema.map(BeamSchemas.fromBeam).contains(LogicalSchema(List(
      LogicalField("id", LogicalType.LongType, nullable = false),
      LogicalField("amount", LogicalType.LongType, nullable = true),
      LogicalField("tags", LogicalType.ArrayType(LogicalType.StringType), nullable = false)
    ))))
  }

  test("a row schema with a field Java cannot decode (a computed Select field has no type) leaves the collection with no columns") {
    val selected = fixture("joined").find(_.name == "Select").get.outputs.head
    assert(selected.rowSchema.isEmpty && selected.coder.isEmpty)
  }

  test("a stream is marked unbounded; anything else is bounded") {
    val top = fixture("pubsub_job")
    assert(!top.head.outputs.head.bounded, "a Pub/Sub read")
    assert(fixture("parquet_job").head.outputs.head.bounded)
    val unspecified = steps(pipeline(transform("S", "u", outputs = Map("out" -> "c")), collections = Map("c" -> RunnerApi.IsBounded.Enum.UNSPECIFIED)))
    assert(unspecified.head.outputs.head.bounded)
  }

  test("each input is known by the tag it was given under, and a collection given twice is one input") {
    val join = fixture("joined").find(_.name == "Join").get
    // the SDK hands CoGroupByKey its inputs by position, whatever the dictionary named them; they are listed in tag order
    assert(join.inputTags.keySet == join.inputs.map(_.name).toSet && join.inputs.map(i => join.inputTags(i.name)) == List("0", "1"))
    val twice = steps(pipeline(
      transform("S", "u", inputs = Map("a" -> "c", "b" -> "c")),
      collections = Map("c" -> RunnerApi.IsBounded.Enum.BOUNDED)
    )).head
    assert(twice.inputs.map(_.name) == List("c"))
    val ordered = steps(pipeline(
      transform("S", "u", inputs = Map("b" -> "y", "a" -> "x", "c" -> "z")),
      collections = Map("x" -> RunnerApi.IsBounded.Enum.BOUNDED, "y" -> RunnerApi.IsBounded.Enum.BOUNDED, "z" -> RunnerApi.IsBounded.Enum.BOUNDED)
    )).head
    assert(ordered.inputs.map(_.name) == List("x", "y", "z"), "in the order of their tags")
  }

  test("steps keep their children, in order") {
    val read = fixture("text_and_avro").head
    assert(read.children.map(_.name) == List("ReadLines/Read"))
    assert(read.children.head.children.map(_.name.stripPrefix("ReadLines/Read/")) == List("Impulse", "EmitSource", "SDFBoundedSourceReader"))
  }

  test("a pipeline whose roots are its steps (a Java pipeline's portable form) is read as it is") {
    val p = Pipeline.create(PipelineOptionsFactory.create())
    p.apply("Lines", Create.of("a")).apply("WriteLines", TextIO.write().to("/data/out/lines"))
    val top = PortablePipeline.steps(PipelineTranslation.toProto(p).toByteArray)
    assert(top.map(_.name) == List("Lines", "WriteLines"))
    assert(top.head.outputs.head.coder.contains(StringUtf8Coder.of()))
  }

  test("a root with no URN but no children is a step of its own") {
    assert(steps(pipeline(transform("Lonely", ""))).map(_.name) == List("Lonely"))
  }

  test("a root with no URN whose children are named under it is a step of its own (a Java composite)") {
    val child = transform("Outer/Inner", "beam:transform:impulse:v1")
    val outer = RunnerApi.PTransform.newBuilder(transform("Outer", "")).addSubtransforms("c").build()
    val p = pipeline(outer).toBuilder
    p.getComponentsBuilder.putTransforms("c", child)
    val top = steps(p.build())
    assert(top.map(_.name) == List("Outer") && top.head.children.map(_.name) == List("Outer/Inner"))
    val mixed = RunnerApi.PTransform.newBuilder(transform("Root", "")).addSubtransforms("c").addSubtransforms("d").build()
    val q = pipeline(mixed).toBuilder
    q.getComponentsBuilder.putTransforms("c", child).putTransforms("d", transform("Root/Part", "u"))
    assert(steps(q.build()).map(_.name) == List("Root"), "one child named under it is enough to make it a step")
  }

  test("a key-value coder with a part Java does not know is no coder") {
    val coders = kvCoder.updated("coder", RunnerApi.Coder.newBuilder().setSpec(spec("beam:coder:kv:v1")).addComponentCoderIds("s").addComponentCoderIds("p").build())
      .updated("p", RunnerApi.Coder.newBuilder().setSpec(spec("beam:coder:pickled_python:v1")).build())
    val kv = steps(pipeline(transform("S", "u", outputs = Map("out" -> "c")), collections = Map("c" -> RunnerApi.IsBounded.Enum.BOUNDED), coder = coders)).head
    assert(kv.outputs.head.coder.isEmpty)
  }

  test("a key-value coder of known coders is rehydrated whole") {
    val kv = steps(pipeline(transform("S", "u", outputs = Map("out" -> "c")), collections = Map("c" -> RunnerApi.IsBounded.Enum.BOUNDED), coder = kvCoder)).head
    assert(kv.outputs.head.coder.contains(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
  }

  test("bytes that are not a pipeline throw: the checker reports that it could not run, and the job does not start") {
    intercept[Exception](PortablePipeline.steps("not a pipeline".getBytes("UTF-8")))
  }
}

private[beamadapter] object PortablePipelineSpec {

  def fixtureBytes(name: String): Array[Byte] = {
    val in = getClass.getClassLoader.getResourceAsStream(s"portable/$name.pb")
    require(in != null, s"no recorded pipeline portable/$name.pb")
    try Iterator.continually(in.read()).takeWhile(_ != -1).map(_.toByte).toArray finally in.close()
  }

  def fixture(name: String): List[BeamStep] = PortablePipeline.steps(fixtureBytes(name))

  def steps(p: RunnerApi.Pipeline): List[BeamStep] = PortablePipeline.steps(p.toByteArray)

  def spec(urn: String): RunnerApi.FunctionSpec = RunnerApi.FunctionSpec.newBuilder().setUrn(urn).build()

  def labelled(namespace: String, key: String, label: String, value: RunnerApi.LabelledPayload.Builder => RunnerApi.LabelledPayload.Builder): RunnerApi.DisplayData =
    RunnerApi.DisplayData.newBuilder()
      .setUrn(PortablePipeline.LabelledDisplayData)
      .setPayload(value(RunnerApi.LabelledPayload.newBuilder().setNamespace(namespace).setKey(key).setLabel(label)).build().toByteString)
      .build()

  def transform(
      name: String,
      urn: String,
      inputs: Map[String, String] = Map.empty,
      outputs: Map[String, String] = Map.empty,
      display: List[RunnerApi.DisplayData] = Nil,
      annotations: Map[String, String] = Map.empty
  ): RunnerApi.PTransform = {
    val b = RunnerApi.PTransform.newBuilder().setUniqueName(name).setSpec(spec(urn))
    inputs.foreach { case (k, v) => b.putInputs(k, v) }
    outputs.foreach { case (k, v) => b.putOutputs(k, v) }
    display.foreach(b.addDisplayData)
    // a ByteString made without naming the vendored protobuf class, whose package changes with the Beam release
    annotations.foreach { case (k, v) => b.putAnnotations(k, spec(v).getUrnBytes) }
    b.build()
  }

  private val stringCoder = RunnerApi.Coder.newBuilder().setSpec(spec("beam:coder:string_utf8:v1")).build()

  def kvCoder: Map[String, RunnerApi.Coder] =
    Map("s" -> stringCoder, "coder" -> RunnerApi.Coder.newBuilder().setSpec(spec("beam:coder:kv:v1")).addComponentCoderIds("s").addComponentCoderIds("s").build())

  def pipeline(
      t: RunnerApi.PTransform,
      collections: Map[String, RunnerApi.IsBounded.Enum] = Map.empty,
      coder: Map[String, RunnerApi.Coder] = Map("coder" -> stringCoder)
  ): RunnerApi.Pipeline = {
    val components = RunnerApi.Components.newBuilder().putTransforms("t", t)
    coder.foreach { case (id, c) => components.putCoders(id, c) }
    collections.foreach { case (id, bounded) =>
      components.putPcollections(id, RunnerApi.PCollection.newBuilder().setUniqueName(id).setCoderId("coder").setIsBounded(bounded).build())
    }
    RunnerApi.Pipeline.newBuilder().setComponents(components).addRootTransformIds("t").build()
  }
}
