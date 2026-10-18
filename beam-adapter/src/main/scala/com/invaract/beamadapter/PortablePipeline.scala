// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.model.pipeline.v1.RunnerApi
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.beam.sdk.util.construction.RehydratedComponents

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/** A pipeline read from its portable form: the language-neutral description (Beam's `RunnerApi.Pipeline` protocol buffer) every
  * Beam SDK produces before it submits a job, which is how a pipeline written in Python reaches this adapter
  * (docs/BEAM_PYTHON_ADAPTER.md). The result is the same tree of steps `BeamGraph` builds from a Java pipeline, so the one
  * translator (`BeamTranslator`) reads both:
  *
  *  - a step's `className` is the class the SDK names in its `python_type` annotation (`apache_beam.io.parquetio.WriteToParquet`),
  *    or its URN when there is none (a step the Python SDK did not build, such as one expanded by Java);
  *  - its display data are the labelled items the step published, by key, with the class that published each one;
  *  - a collection's coder is rehydrated into Beam's Java coder when it is one Java knows (strings, numbers, key-value pairs,
  *    rows with their schema), and is absent when it is not (a pickled Python object), which is how a collection of plain
  *    Python objects comes out with no columns.
  *
  * Nothing here runs Python or reads a step's own code: only what the description states.
  */
private[beamadapter] object PortablePipeline {

  /** The top-level steps of the pipeline `bytes` hold (a serialized `RunnerApi.Pipeline`). Throws when they are not one. */
  def steps(bytes: Array[Byte]): List[BeamStep] = steps(RunnerApi.Pipeline.parseFrom(bytes))

  def steps(pipeline: RunnerApi.Pipeline): List[BeamStep] = {
    val components = pipeline.getComponents
    val rehydrated = RehydratedComponents.forComponents(components)
    val collections = mutable.Map.empty[String, BeamCollection]

    def collection(id: String): BeamCollection = collections.getOrElseUpdate(id, {
      val pc = components.getPcollectionsOrThrow(id)
      // a coder Java does not know (a pickled Python object) leaves the collection with no coder, and so with no columns
      val coder = Try(rehydrated.getCoder(pc.getCoderId): Coder[_]).filter(known).toOption
      BeamCollection(
        name = id,
        bounded = pc.getIsBounded != RunnerApi.IsBounded.Enum.UNBOUNDED,
        coder = coder,
        rowSchema = coder.collect { case s: SchemaCoder[_] => s.getSchema }
      )
    })

    def step(id: String): BeamStep = {
      val t = components.getTransformsOrThrow(id)
      val items = t.getDisplayDataList.asScala.toList.flatMap(displayItem)
      // a key published twice (a wrapper and the function it wraps both name `fn`) keeps its first value
      val first = items.groupBy(_._2).map { case (key, all) => key -> all.head }
      BeamStep(
        name = t.getUniqueName,
        className = Option(t.getAnnotationsMap.get(PythonTypeAnnotation)).map(_.toStringUtf8).filter(_.nonEmpty).getOrElse(t.getSpec.getUrn),
        // a protocol buffer map has no order: by tag, so the same pipeline always translates the same way
        inputs = byTag(t.getInputsMap).distinct.map(collection),
        outputs = byTag(t.getOutputsMap).distinct.map(collection),
        display = first.map { case (key, (_, _, value)) => key -> value },
        children = t.getSubtransformsList.asScala.toList.map(step),
        inputTags = t.getInputsMap.asScala.toList.map { case (tag, pc) => pc -> tag }.toMap,
        displayNamespaces = first.map { case (key, (namespace, _, _)) => key -> namespace }
      )
    }

    // the Python SDK wraps the whole pipeline in one root step that is not one of the job's: its children are not named under it
    // (Beam names a step's own parts `<step>/<part>`), so they are the job's top-level steps
    pipeline.getRootTransformIdsList.asScala.toList.flatMap { id =>
      val root = components.getTransformsOrThrow(id)
      val children = root.getSubtransformsList.asScala.toList
      if (root.getSpec.getUrn.isEmpty && children.nonEmpty && children.forall(c => !components.getTransformsOrThrow(c).getUniqueName.startsWith(root.getUniqueName + "/")))
        children.map(step)
      else List(step(id))
    }
  }

  /** Whether Java knows `coder` and everything it is made of: a coder it does not know is rehydrated as a stand-in that only
    * carries the bytes along (`UnknownCoderWrapper`), which says nothing about what the items are. */
  private def known(coder: Coder[_]): Boolean =
    coder.getClass.getSimpleName != "UnknownCoderWrapper" && coder.getCoderArguments.asScala.forall(known)

  private def byTag(collections: java.util.Map[String, String]): List[String] = collections.asScala.toList.sortBy(_._1).map(_._2)

  /** The annotation in which the Python SDK names the class a step was built from. */
  val PythonTypeAnnotation = "python_type"

  /** The URN of a labelled display-data item, the only kind the SDKs publish. */
  val LabelledDisplayData = "beam:display_data:labelled:v1"

  /** `(namespace, key, value)` of one display-data item; the key is its label when an SDK gave it none. */
  private def displayItem(d: RunnerApi.DisplayData): Option[(String, String, String)] =
    if (d.getUrn != LabelledDisplayData) None
    else
      Try(RunnerApi.LabelledPayload.parseFrom(d.getPayload)).toOption.flatMap { p =>
        val value = p.getValueCase match {
          case RunnerApi.LabelledPayload.ValueCase.STRING_VALUE => Some(p.getStringValue)
          case RunnerApi.LabelledPayload.ValueCase.BOOL_VALUE   => Some(p.getBoolValue.toString)
          case RunnerApi.LabelledPayload.ValueCase.INT_VALUE    => Some(p.getIntValue.toString)
          case RunnerApi.LabelledPayload.ValueCase.DOUBLE_VALUE => Some(p.getDoubleValue.toString)
          case _                                                => None
        }
        val key = if (p.getKey.nonEmpty) p.getKey else p.getLabel
        value.map(v => (p.getNamespace, key, v))
      }
}
