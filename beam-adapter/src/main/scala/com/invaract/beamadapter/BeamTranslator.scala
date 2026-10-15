// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.LogicalSchema
import com.invaract.ir.{DatasetRef, Plan, Read, UnknownPlan, Write}
import org.apache.beam.sdk.Pipeline

import scala.collection.mutable

/** A write the translator recognised, with what the contract checks need: where it writes and in which format (`None` when the
  * writer's format could not be told), the plan that produces its data (with the save mode and catalog table a BigQuery write
  * states), the columns of the data it writes (the write's own declared schema, else the data's; `None` when neither is
  * known), the columns of every located read upstream of it, and the reads upstream of it that could not be located.
  */
private[beamadapter] final case class TranslatedWrite(
    location: String,
    format: Option[String],
    plan: Write,
    outputSchema: Option[LogicalSchema],
    inputSchemas: List[(String, LogicalSchema)],
    unlocatedReads: List[String] = Nil,
    bounded: Boolean = true
)

/** Something that changes data but that the translation cannot check: refused, never passed unchecked (fail closed). */
private[beamadapter] final case class UnverifiableStep(operation: String, plan: Plan)

private[beamadapter] final case class BeamTranslation(writes: List[TranslatedWrite], unverifiable: List[UnverifiableStep])

/** The pipeline graph (`BeamGraph`) as the engine-neutral plan (`ir.Plan`) the verifiers work on.
  *
  * What it recognises is deliberately small and exact: the IO transforms `BeamIO` knows, whose source or destination the graph
  * states. Every other step becomes an `ir.UnknownPlan` that still carries the plans of the steps feeding it, so an unrecognised
  * step never hides a read beneath it; a read from outside the pipeline that states no location becomes an `ir.UnknownPlan` of
  * source type `UnlocatedSourceType`, which the verifiers treat as a place a declared input may be read without being seen; and a
  * step that ends the pipeline without being a recognised write is reported as unverifiable. The design reasons are
  * docs/BEAM_ADAPTER.md's D1 to D12.
  */
private[beamadapter] object BeamTranslator {

  /** The `ir.UnknownPlan.sourceType` of a read from outside the pipeline that states no location. */
  val UnlocatedSourceType = "UnlocatedRead"

  def translate(pipeline: Pipeline): BeamTranslation = translate(BeamGraph.of(pipeline))

  def translate(top: List[BeamStep]): BeamTranslation = {
    val nodes = top.flatMap(visible)
    val producers = nodes.flatMap(n => n.step.outputs.map(_.name -> n)).toMap
    val consumed = nodes.flatMap(_.step.inputs.map(_.name)).toSet

    val planOf = mutable.Map.empty[String, Plan]
    // by step name: a Node is a case class over the whole step subtree, which `distinct` would hash in full
    def sourcesOf(node: Node): List[Node] = {
      val seen = mutable.Set.empty[String]
      node.step.inputs.flatMap(i => producers.get(i.name)).filter(n => seen.add(n.step.name))
    }
    def passThrough(n: Node, description: String, sourceType: String): Plan = UnknownPlan(description, sourceType, sourcesOf(n).map(plan))
    def plan(node: Node): Plan = planOf.getOrElseUpdate(
      node.step.name,
      node match {
        case r: ReadNode =>
          r.locations.map(l => Read(DatasetRef(l), catalog = r.catalog)) match {
            case only :: Nil => only
            case many        => UnknownPlan(s"${r.step.name}: several sources", BeamIO.simpleName(r.step), many)
          }
        case u: UnlocatedReadNode     => passThrough(u, s"${u.step.name}: ${u.what}", UnlocatedSourceType)
        case f: FileReaderNode        => passThrough(f, s"${f.step.name} (${BeamIO.simpleName(f.step)})", BeamIO.simpleName(f.step))
        case o: OtherNode             => passThrough(o, s"${o.step.name} (${BeamIO.simpleName(o.step)})", BeamIO.simpleName(o.step))
        case w: WriteNode             => writePlan(w)
        case u: UnverifiableWriteNode => passThrough(u, s"${u.step.name}: ${u.why}", BeamIO.simpleName(u.step))
      }
    )
    def writePlan(w: WriteNode): Write = Write(DatasetRef(w.location), inputPlan(w), w.format, w.saveMode, w.catalog)
    def inputPlan(w: WriteNode): Plan = sourcesOf(w) match {
      case Nil     => UnknownPlan(s"${w.step.name}: no input was found", BeamIO.simpleName(w.step))
      case sources => sources.map(plan) match { case only :: Nil => only; case many => UnknownPlan(s"${w.step.name}: several inputs", "Inputs", many) }
    }
    /** Every node upstream of `node`, each once. */
    def upstream(node: Node): List[Node] = {
      val seen = mutable.Set.empty[String]
      def walk(n: Node): List[Node] = if (!seen.add(n.step.name)) Nil else n :: sourcesOf(n).flatMap(walk)
      sourcesOf(node).flatMap(walk)
    }

    val writes = nodes.collect { case w: WriteNode => w }.map { w =>
      val above = upstream(w)
      val located = above.collect { case r: ReadNode => r }
      // a read's columns: its own output's; for the folder a FileIO.match() reads, those of the reader its files are handed to
      val readSchemas = located.flatMap(r => r.step.outputs.headOption.flatMap(BeamSchemas.of).toList.flatMap(s => r.locations.map(_ -> s)))
      val readerSchemas = above.collect { case f: FileReaderNode => f }.flatMap { f =>
        f.step.outputs.headOption.flatMap(BeamSchemas.of).toList.flatMap(s => upstream(f).collect { case r: ReadNode => r }.flatMap(_.locations).map(_ -> s))
      }
      val unlocated = above.collect { case u: UnlocatedReadNode => s"${u.step.name}: ${u.what}" } ++
        above.collect { case f: FileReaderNode if !upstream(f).exists(_.isInstanceOf[ReadNode]) => s"${f.step.name}: files handed to it by a step that does not say where they are" }
      TranslatedWrite(
        w.location,
        w.format,
        writePlan(w),
        w.declaredSchema.orElse(w.step.inputs.headOption.flatMap(BeamSchemas.of)),
        (readSchemas ++ readerSchemas).distinct,
        unlocated.distinct,
        bounded = w.step.inputs.forall(_.bounded)
      )
    }

    val unwritable = nodes.collect { case u: UnverifiableWriteNode => u }.map(u => UnverifiableStep(u.why, plan(u)))
    val endsPipeline = nodes.collect { case o: OtherNode if o.step.outputs.forall(c => !consumed.contains(c.name)) => o }
      .map(o => UnverifiableStep(s"a step that ends the pipeline but is not a recognised write: ${o.step.name} (${BeamIO.simpleName(o.step)})", plan(o)))
    BeamTranslation(writes, unwritable ++ endsPipeline)
  }

  // --- the steps that matter, found top-down so the outermost recognisable step wins ---

  private sealed trait Node { def step: BeamStep }
  private final case class ReadNode(step: BeamStep, locations: List[String], catalog: Option[com.invaract.ir.CatalogIdentity]) extends Node
  private final case class FileReaderNode(step: BeamStep) extends Node
  private final case class UnlocatedReadNode(step: BeamStep, what: String) extends Node
  private final case class WriteNode(
      step: BeamStep,
      location: String,
      format: Option[String],
      saveMode: Option[String],
      catalog: Option[com.invaract.ir.CatalogIdentity],
      declaredSchema: Option[LogicalSchema]
  ) extends Node
  private final case class UnverifiableWriteNode(step: BeamStep, why: String) extends Node
  private final case class OtherNode(step: BeamStep) extends Node

  private def visible(step: BeamStep): List[Node] =
    BeamIO.read(step).map(readNode(step, _)).orElse(BeamIO.write(step).map(writeNode(step, _))) match {
      case Some(node) => List(node)
      case None =>
        if (isFileWriteUnit(step)) List(writeNode(step, BeamIO.fileWrite(step)))
        else if (step.isComposite && containsIo(step)) step.children.flatMap(visible)
        else List(OtherNode(step))
    }

  private def readNode(step: BeamStep, read: IoRead): Node = read match {
    case IoRead.Located(locations, _, catalog) => ReadNode(step, locations, catalog)
    case IoRead.FileReader(_)                  => FileReaderNode(step)
    case IoRead.Unlocated(what)                => UnlocatedReadNode(step, what)
  }

  private def writeNode(step: BeamStep, write: IoWrite): Node = write match {
    case IoWrite.Located(location, format, saveMode, catalog, schema) => WriteNode(step, location, format, saveMode, catalog, schema)
    case IoWrite.Unverifiable(why)                                    => UnverifiableWriteNode(step, why)
  }

  private def isIo(s: BeamStep): Boolean = BeamIO.read(s).isDefined || BeamIO.write(s).isDefined || BeamIO.isFileWrite(s)

  private def containsIo(step: BeamStep): Boolean = step.withDescendants.exists(isIo)

  /** The outermost step that holds exactly one file write and no other IO: the write as the job wrote it (`TextIO.write()`,
    * `FileIO.write()`, or a team's composite around one). Anything holding more is searched deeper. */
  private def isFileWriteUnit(step: BeamStep): Boolean = {
    val all = step.withDescendants
    all.count(BeamIO.isFileWrite) == 1 && !all.exists(s => BeamIO.read(s).isDefined || BeamIO.write(s).isDefined)
  }

  /** The folder a read pattern reads from (kept here for the tests that pin it). */
  private[beamadapter] def readLocation(pattern: String): String = BeamLocations.folderOf(pattern)
}
