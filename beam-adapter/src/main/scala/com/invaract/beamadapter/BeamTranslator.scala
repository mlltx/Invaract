// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.LogicalSchema
import com.invaract.ir.{DatasetRef, Plan, Read, UnknownPlan, Write}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.WriteFiles
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.Contextful

import scala.collection.mutable
import scala.util.Try

/** A file write the translator recognised, with what the contract checks need: where it writes and in which format
  * (`None` when the writer's format could not be told), the plan that produces its data, and the columns of the data it
  * writes and of every recognised read upstream of it (`None` when the collection exposes no schema).
  */
private[beamadapter] final case class TranslatedWrite(
    location: String,
    format: Option[String],
    plan: Write,
    outputSchema: Option[LogicalSchema],
    inputSchemas: List[(String, LogicalSchema)]
)

/** Something that changes data but that the translation cannot check: refused, never passed unchecked (fail closed). */
private[beamadapter] final case class UnverifiableStep(operation: String, plan: Plan)

private[beamadapter] final case class BeamTranslation(writes: List[TranslatedWrite], unverifiable: List[UnverifiableStep])

/** The pipeline graph (`BeamGraph`) as the engine-neutral plan (`ir.Plan`) the verifiers work on.
  *
  * What it recognises is deliberately small and exact: reads and file writes of the IO transforms whose destination the
  * graph states (Parquet, Avro and text files). Every other step becomes an `ir.UnknownPlan` that still carries the plans
  * of the steps feeding it, so an unrecognised step never hides a read beneath it; and a step that ends the pipeline
  * without being a recognised write is reported as unverifiable. The design reasons are docs/BEAM_ADAPTER.md's D1 to D9.
  */
private[beamadapter] object BeamTranslator {

  def translate(pipeline: Pipeline): BeamTranslation = translate(BeamGraph.of(pipeline))

  def translate(top: List[BeamStep]): BeamTranslation = {
    val nodes = top.flatMap(visible)
    val producers = nodes.flatMap(n => n.step.outputs.map(_.name -> n)).toMap
    val consumed = nodes.flatMap(_.step.inputs.map(_.name)).toSet

    val planOf = mutable.Map.empty[String, Plan]
    def sourcesOf(node: Node): List[Node] = node.step.inputs.flatMap(i => producers.get(i.name)).distinct
    def plan(node: Node): Plan = planOf.getOrElseUpdate(
      node.step.name,
      node match {
        case r: ReadNode => Read(DatasetRef(r.location))
        case o: OtherNode =>
          UnknownPlan(s"${o.step.name} (${simpleName(o.step)})", simpleName(o.step), sourcesOf(o).map(plan))
        case w: WriteNode => Write(DatasetRef(w.location), inputPlan(w), w.format)
        case u: UnverifiableWriteNode => UnknownPlan(s"${u.step.name}: ${u.why}", simpleName(u.step), sourcesOf(u).map(plan))
      }
    )
    def inputPlan(w: WriteNode): Plan = sourcesOf(w) match {
      case Nil     => UnknownPlan(s"${w.step.name}: no input was found", simpleName(w.step))
      case sources => sources.map(plan) match { case only :: Nil => only; case many => UnknownPlan(s"${w.step.name}: several inputs", "Inputs", many) }
    }
    def readsBeneath(node: Node): List[ReadNode] = {
      val seen = mutable.LinkedHashSet.empty[String]
      def walk(n: Node): List[ReadNode] =
        if (!seen.add(n.step.name)) Nil
        else
          n match {
            case r: ReadNode => List(r)
            case _           => sourcesOf(n).flatMap(walk)
          }
      sourcesOf(node).flatMap(walk)
    }

    val writes = nodes.collect { case w: WriteNode => w }.map { w =>
      val schemas = readsBeneath(w).flatMap(r => r.step.outputs.headOption.flatMap(BeamSchemas.of).map(r.location -> _)).distinct
      TranslatedWrite(w.location, w.format, plan(w).asInstanceOf[Write], w.step.inputs.headOption.flatMap(BeamSchemas.of), schemas)
    }

    val unwritable = nodes.collect { case u: UnverifiableWriteNode => u }.map(u => UnverifiableStep(u.why, plan(u)))
    val endsPipeline = nodes.collect { case o: OtherNode if o.step.outputs.forall(c => !consumed.contains(c.name)) => o }
      .map(o => UnverifiableStep(s"a step that ends the pipeline but is not a recognised write: ${o.step.name} (${simpleName(o.step)})", plan(o)))
    BeamTranslation(writes, unwritable ++ endsPipeline)
  }

  // --- the steps that matter, found top-down so the outermost recognisable step wins ---

  private sealed trait Node { def step: BeamStep }
  private final case class ReadNode(step: BeamStep, location: String, format: String) extends Node
  private final case class WriteNode(step: BeamStep, location: String, format: Option[String]) extends Node
  private final case class UnverifiableWriteNode(step: BeamStep, why: String) extends Node
  private final case class OtherNode(step: BeamStep) extends Node

  private def visible(step: BeamStep): List[Node] =
    readOf(step) match {
      case Some(read) => List(read)
      case None =>
        if (isWriteUnit(step)) List(writeOf(step))
        else if (step.isComposite && containsIo(step)) step.children.flatMap(visible)
        else List(OtherNode(step))
    }

  private def containsIo(step: BeamStep): Boolean =
    step.withDescendants.exists(s => readOf(s).isDefined || s.transform.isInstanceOf[WriteFiles[_, _, _]])

  /** The outermost step that holds exactly one file write and no recognised read: the write as the job wrote it
    * (`TextIO.write()`, `FileIO.write()`, or a team's composite around one). Anything holding several is searched deeper. */
  private def isWriteUnit(step: BeamStep): Boolean = {
    val all = step.withDescendants
    all.count(_.transform.isInstanceOf[WriteFiles[_, _, _]]) == 1 && !all.exists(s => readOf(s).isDefined)
  }

  private def simpleName(step: BeamStep): String = step.className.substring(step.className.lastIndexOf('.') + 1)

  // --- reads ---

  private val ReadFormats = Map("AutoValue_ParquetIO_Read" -> "parquet", "AutoValue_TextIO_Read" -> "text", "AutoValue_AvroIO_Read" -> "avro")

  private def readOf(step: BeamStep): Option[ReadNode] =
    ReadFormats.get(simpleName(step)).flatMap(format => step.display.get("filePattern").map(p => ReadNode(step, readLocation(p), format)))

  /** The folder a read pattern reads from: the pattern `in/orders/` plus a glob file name is `in/orders` (a contract declares the folder). */
  private[beamadapter] def readLocation(pattern: String): String = {
    val path = pattern.replace('\\', '/')
    val last = path.substring(path.lastIndexOf('/') + 1)
    val folder = if (last.exists("*?[{".contains(_))) path.substring(0, math.max(path.lastIndexOf('/'), 0)) else path
    folder.stripSuffix("/")
  }

  // --- writes ---

  private def writeOf(step: BeamStep): Node = {
    val sink = step.withDescendants.map(_.transform).collectFirst { case w: WriteFiles[_, _, _] => w.getSink.getClass.getName }
    val prefix = step.withDescendants.flatMap(_.display.get("filePrefix")).headOption
    sink match {
      case Some("org.apache.beam.sdk.io.TextSink") =>
        prefix.fold[Node](UnverifiableWriteNode(step, "a text write whose file prefix is not published"))(p => WriteNode(step, folder(p), Some("text")))
      case Some(avro) if avro.endsWith(".AvroSink") =>
        prefix.fold[Node](UnverifiableWriteNode(step, "an Avro write whose file prefix is not published"))(p => WriteNode(step, folder(p), Some("avro")))
      case Some("org.apache.beam.sdk.io.FileIO$Write$ViaFileBasedSink") =>
        fileIoWrite(step) match {
          case Some((location, sinkClass)) => WriteNode(step, location, formatOf(sinkClass))
          case None                        => UnverifiableWriteNode(step, "a FileIO write whose destination or sink cannot be read")
        }
      case Some(other) => UnverifiableWriteNode(step, s"a file write through $other, which this adapter does not recognise")
      case None        => UnverifiableWriteNode(step, "a write whose sink cannot be read")
    }
  }

  private def folder(path: String): String = path.replace('\\', '/').stripSuffix("/")

  private def formatOf(sinkClass: String): Option[String] =
    if (sinkClass.contains("ParquetIO")) Some("parquet")
    else if (sinkClass.contains("AvroIO")) Some("avro")
    else if (sinkClass.contains("TextIO")) Some("text")
    else None

  /** `(output directory, sink class)` of a `FileIO.write()`. Beam publishes neither: the writer's display data names only
    * its sink class, and the destination and the sink are held in non-public members of `FileIO.Write` (docs/BEAM_ADAPTER.md,
    * "Friction log"). They are read by reflection, confined to this method and to Beam release 2.77.0 (the one the adapter
    * is tested with); any failure here returns `None` and the write is refused as unverifiable rather than guessed.
    * A separate temporary directory (`withTempDirectory`) is not the destination, and the output directory is read directly,
    * so it cannot be mistaken for one. */
  private def fileIoWrite(step: BeamStep): Option[(String, String)] =
    step.withDescendants.find(_.className.endsWith("AutoValue_FileIO_Write")).flatMap { s =>
      Try {
        val t = s.transform
        def member(name: String): AnyRef = {
          val m = t.getClass.getDeclaredMethod(name)
          m.setAccessible(true)
          m.invoke(t)
        }
        val directory = Option(member("getOutputDirectory")).map(_.asInstanceOf[ValueProvider[String]].get()).filter(_.nonEmpty)
        val sinkFn = member("getSinkFn").asInstanceOf[Contextful[Contextful.Fn[AnyRef, AnyRef]]]
        val sink = sinkFn.getClosure.apply(null, null)
        directory.map(d => folder(d) -> sink.getClass.getName)
      }.toOption.flatten
    }
}
