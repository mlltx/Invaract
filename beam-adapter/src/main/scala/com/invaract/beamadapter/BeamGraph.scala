// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.schemas.{Schema => BeamSchema}
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.display.DisplayData
import org.apache.beam.sdk.values.PCollection

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/** One collection of data flowing between two steps (Beam's `PCollection`): its name (unique in a pipeline), whether it
  * is finite, the coder that encodes its items (which, for Avro records, carries the Avro schema) and, for rows, the Beam
  * schema. Everything here is read from public calls, before anything runs (docs/BEAM_ADAPTER_SURVEY.md, section 3).
  */
private[beamadapter] final case class BeamCollection(
    name: String,
    bounded: Boolean,
    coder: Option[Coder[_]],
    rowSchema: Option[BeamSchema]
)

/** One step of a pipeline as the graph walk reports it: a composite step (made of smaller ones, in `children`) or a
  * primitive one. `display` is the step's own display data - the label sheet a step publishes about itself, which is
  * where a read states its file pattern.
  */
private[beamadapter] final case class BeamStep(
    name: String,
    transform: PTransform[_, _],
    inputs: List[BeamCollection],
    outputs: List[BeamCollection],
    display: Map[String, String],
    children: List[BeamStep]
) {
  def className: String = transform.getClass.getName
  def isComposite: Boolean = children.nonEmpty

  /** This step and every step beneath it, outermost first. */
  lazy val withDescendants: List[BeamStep] = this :: children.flatMap(_.withDescendants)
}

/** The pipeline graph: the steps of a constructed pipeline, in the order Beam reports them (inputs before the steps that
  * use them), as a tree of the top-level steps. Available as soon as the job is constructed, before `run` is called, from
  * `Pipeline.traverseTopologically` - which is why an adapter can refuse a job before anything has started.
  */
private[beamadapter] object BeamGraph {

  /** The top-level steps of `pipeline`. */
  def of(pipeline: Pipeline): List[BeamStep] = {
    val frames = new java.util.ArrayDeque[mutable.ListBuffer[BeamStep]]()
    var result: List[BeamStep] = Nil
    frames.push(mutable.ListBuffer.empty[BeamStep])
    pipeline.traverseTopologically(new PipelineVisitor.Defaults {
      override def enterCompositeTransform(node: TransformHierarchy#Node): PipelineVisitor.CompositeBehavior = {
        if (!node.isRootNode) frames.push(mutable.ListBuffer.empty[BeamStep])
        PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM
      }

      override def leaveCompositeTransform(node: TransformHierarchy#Node): Unit =
        if (node.isRootNode) result = frames.pop().toList
        else {
          val children = frames.pop().toList
          frames.peek().append(stepOf(node, children))
        }

      override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
        frames.peek().append(stepOf(node, Nil))
    })
    result
  }

  private def stepOf(node: TransformHierarchy#Node, children: List[BeamStep]): BeamStep =
    BeamStep(
      name = node.getFullName,
      transform = node.getTransform,
      inputs = node.getInputs.values.asScala.toList.collect { case c: PCollection[_] => collectionOf(c) },
      outputs = node.getOutputs.values.asScala.toList.collect { case c: PCollection[_] => collectionOf(c) },
      display = displayOf(node.getTransform),
      children = children
    )

  private def collectionOf(c: PCollection[_]): BeamCollection =
    BeamCollection(
      name = c.getName,
      bounded = c.isBounded == PCollection.IsBounded.BOUNDED,
      // a collection whose coder cannot be inferred throws; "no coder to read" is the honest answer then
      coder = Try(c.getCoder: Coder[_]).toOption,
      rowSchema = if (c.hasSchema) Some(c.getSchema) else None
    )

  private def displayOf(transform: PTransform[_, _]): Map[String, String] =
    Try(DisplayData.from(transform).items().asScala.toList).getOrElse(Nil).flatMap { item =>
      Option(item.getValue).map(v => item.getKey -> v.toString)
    }.toMap
}
