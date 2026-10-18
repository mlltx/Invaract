// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.LogicalSchema
import com.invaract.ir.{Aggregate, BooleanExpr, ColumnRef, ColumnReference, Comparison, DatasetRef, Expr, Filter, Join, Plan, Read, UDF, UnknownPlan, Write}
import org.apache.beam.sdk.Pipeline

import scala.collection.mutable

/** A write the translator recognised, with what the contract checks need: where it writes and in which format (`None` when the
  * writer's format could not be told), the plan that produces its data (with the save mode and catalog table a BigQuery write
  * states), the columns of the data it writes (the write's own declared schema, else the data's; `None` when neither is
  * known), the columns of every located read upstream of it, and the reads upstream of it that could not be located.
  *
  * For the transformation-shape rules (docs/BEAM_ADAPTER.md, D19): `opaqueCombines` are the steps upstream of it that combine
  * several collections in code the translation cannot read (a `CoGroupByKey`, a function given another collection as a side
  * input), any of which could be a cross join; `unreadableShapes` the schema transforms upstream whose fields could not be read.
  */
private[beamadapter] final case class TranslatedWrite(
    location: String,
    format: Option[String],
    plan: Write,
    outputSchema: Option[LogicalSchema],
    inputSchemas: List[(String, LogicalSchema)],
    unlocatedReads: List[String] = Nil,
    opaqueCombines: List[String] = Nil,
    unreadableShapes: List[String] = Nil
)

/** Something that changes data but that the translation cannot check: refused, never passed unchecked (fail closed). `step` is
  * the step's full name when it is a step that ends the pipeline without being a recognised write, which a platform may name
  * as trusted (`TrustedSteps`, docs/BEAM_ADAPTER.md, D14); a write it cannot verify has none, since trusting it would pass data
  * written somewhere the contract cannot see. */
private[beamadapter] final case class UnverifiableStep(operation: String, plan: Plan, step: Option[String] = None)

/** The pipeline's writes, what it cannot verify, and the steps that end it but read only what writes report (`reportSteps`, let
  * through: D14). */
private[beamadapter] final case class BeamTranslation(writes: List[TranslatedWrite], unverifiable: List[UnverifiableStep], reportSteps: List[String] = Nil)

/** The pipeline graph (`BeamGraph`) as the engine-neutral plan (`ir.Plan`) the verifiers work on.
  *
  * What it recognises is deliberately small and exact: the IO transforms `BeamIO` knows, whose source or destination the graph
  * states. Every other step becomes an `ir.UnknownPlan` that still carries the plans of the steps feeding it, so an unrecognised
  * step never hides a read beneath it; a read from outside the pipeline that states no location becomes an `ir.UnknownPlan` of
  * source type `UnlocatedSourceType`, which the verifiers treat as a place a declared input may be read without being seen; and a
  * step that ends the pipeline without being a recognised write is reported as unverifiable. The design reasons are
  * docs/BEAM_ADAPTER.md's D1 to D12. Beam's schema transforms (`BeamShapes`) become the `ir.Filter`, `ir.Aggregate` and
  * `ir.Join` they are, with the columns they use, so the transformation-shape rules can be checked (D19).
  */
private[beamadapter] object BeamTranslator {

  /** The `ir.UnknownPlan.sourceType` of a read from outside the pipeline that states no location. */
  val UnlocatedSourceType = "UnlocatedRead"

  /** The `ir.UDF.engineType` of a filter's predicate: Java code, of which only the columns it is given are known. */
  val PredicateEngineType = "BeamPredicate"

  def translate(pipeline: Pipeline): BeamTranslation = translate(BeamGraph.of(pipeline))

  def translate(top: List[BeamStep]): BeamTranslation = {
    val nodes = top.flatMap(visible)
    val producers = nodes.flatMap(n => n.step.outputs.map(_.name -> n)).toMap
    val consumers = nodes.flatMap(n => inputsOf(n.step).map(_.name -> n)).groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }
    val consumed = consumers.keySet

    val planOf = mutable.Map.empty[String, Plan]
    // by step name: a Node is a case class over the whole step subtree, which `distinct` would hash in full. What a write hands on
    // (the files it wrote, a signal that it is done) is about the write, not data it read, so a write is never a source of data.
    def sourcesOf(node: Node): List[Node] = {
      val seen = mutable.Set.empty[String]
      inputsOf(node.step).flatMap(i => producers.get(i.name)).filterNot(isWrite).filter(n => seen.add(n.step.name))
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
        // a reader handed files by a step that does not say where they are is itself a read whose source is not located
        case f: FileReaderNode =>
          passThrough(f, s"${f.step.name} (${BeamIO.simpleName(f.step)})", if (readsLocated(f)) BeamIO.simpleName(f.step) else UnlocatedSourceType)
        case o: OtherNode =>
          BeamShapes.of(o.step) match {
            case Some(shape) => shaped(o, shape)
            case None        => passThrough(o, s"${o.step.name} (${BeamIO.simpleName(o.step)})", BeamIO.simpleName(o.step))
          }
        case w: WriteNode             => writePlan(w)
        case u: UnverifiableWriteNode => passThrough(u, s"${u.step.name}: ${u.why}", BeamIO.simpleName(u.step))
      }
    )
    /** A schema transform as the plan node it is: the filter, grouping or join, over the plans of what feeds it. */
    def shaped(o: OtherNode, shape: SchemaShape): Plan = shape match {
      case SchemaShape.Filtered(predicates) =>
        // one predicate per filter, each a function of the columns it is given, all of which must hold
        val conditions = predicates.map(cols => UDF(None, cols.map(c => ColumnReference(ColumnRef(c))), Some(PredicateEngineType)): Expr)
        Filter(single(o), conditions.reduceOption((a, b) => BooleanExpr("AND", List(a, b))).getOrElse(UDF(None, Nil, Some(PredicateEngineType))))
      case SchemaShape.Grouped(keys) => Aggregate(single(o), keys.map(k => ColumnReference(ColumnRef(k))), Nil)
      case SchemaShape.Joined(joinType, first :: rest) =>
        def sidePlan(side: JoinSide): Plan =
          producers.get(side.collection).map(plan).getOrElse(UnknownPlan(s"${o.step.name}: no step produces ${side.collection}", BeamIO.simpleName(o.step)))
        rest.foldLeft(sidePlan(first)) { (joined, side) =>
          // each side is matched with the first on its key columns, position by position; a side with none is a cross product
          val pairs = first.keys.zip(side.keys).map { case (l, r) =>
            Comparison("=", ColumnReference(ColumnRef(l, Some(first.qualifier))), ColumnReference(ColumnRef(r, Some(side.qualifier)))): Expr
          }
          Join(joined, sidePlan(side), joinType, pairs.reduceOption((a, b) => BooleanExpr("AND", List(a, b))))
        }
      case SchemaShape.Joined(_, Nil) | SchemaShape.Unreadable(_) =>
        passThrough(o, s"${o.step.name} (${BeamIO.simpleName(o.step)})", UnreadableShapeSourceType)
    }
    def single(o: OtherNode): Plan = sourcesOf(o).map(plan) match {
      case only :: Nil => only
      case many        => UnknownPlan(s"${o.step.name}: several inputs", "Inputs", many)
    }
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

    def readsLocated(f: FileReaderNode): Boolean = upstream(f).exists(_.isInstanceOf[ReadNode])

    val writes = nodes.collect { case w: WriteNode => w }.map { w =>
      val above = upstream(w)
      val located = above.collect { case r: ReadNode => r }
      // a read's columns: its own output's; for the folder a FileIO.match() reads, those of the reader its files are handed to
      // a text read parsed straight away into rows (JsonToRow, CsvIO.parseRows) has the parser's columns
      def parsedColumns(r: ReadNode): Option[LogicalSchema] =
        r.step.outputs.headOption.flatMap(o => consumers.getOrElse(o.name, Nil) match {
          case parser :: Nil if BeamIO.isTextParser(parser.step) => parser.step.outputs.headOption.flatMap(BeamSchemas.of)
          case _                                                 => None
        })
      val readSchemas = located.flatMap(r => r.columns.orElse(parsedColumns(r)).toList.flatMap(s => r.locations.map(_ -> s)))
      val readerSchemas = above.collect { case f: FileReaderNode => f }.flatMap { f =>
        f.columns.toList.flatMap(s => upstream(f).collect { case r: ReadNode => r }.flatMap(_.locations).map(_ -> s))
      }
      val unlocated = above.collect { case u: UnlocatedReadNode => s"${u.step.name}: ${u.what}" } ++
        above.collect { case f: FileReaderNode if !readsLocated(f) => s"${f.step.name}: files handed to it by a step that does not say where they are" }
      // a write of what a formatter made from rows (ToJson) has those rows' columns; a text write of it is in the formatter's format
      val formatter = sourcesOf(w) match {
        case (o: OtherNode) :: Nil => BeamIO.textFormatterFormat(o.step).map(f => f -> o.step.inputs.headOption.flatMap(BeamSchemas.of))
        case _                     => None
      }
      val written = formatter.filter(_ => w.format.contains("text")).fold(w)(f => w.copy(format = Some(f._1)))
      val others = above.collect { case o: OtherNode => o }
      // what the transformation-shape rules cannot see past (D19): a schema transform it could not read, or a step that combines
      // several collections in code (not Flatten, which only concatenates them)
      val unreadable = others.collect { case o if BeamShapes.of(o.step).exists(_.isInstanceOf[SchemaShape.Unreadable]) => describe(o.step) }
      val combines = others.collect {
        case o if !BeamShapes.recognises(o.step) && !BeamIO.isUnion(o.step) && sourcesOf(o).size > 1 => describe(o.step)
      }
      TranslatedWrite(
        written.location,
        written.format,
        writePlan(written),
        w.declaredSchema.orElse(w.step.inputs.headOption.flatMap(BeamSchemas.of)).orElse(formatter.flatMap(_._2)),
        (readSchemas ++ readerSchemas).distinct,
        unlocated.distinct,
        combines.distinct,
        unreadable.distinct
      )
    }

    /** Whether everything `node` reads is what writes report - the files they wrote, the tables they loaded - directly or
      * through steps that themselves read only that: it sees names, not data (D14). A write's result that holds rows (a BigQuery
      * write's failed inserts) is data, and so is anything that does not come from a write. */
    def readsOnlyReports(node: Node, via: Set[String]): Boolean = {
      val read = inputsOf(node.step)
      read.nonEmpty && read.forall { c =>
        producers.get(c.name) match {
          case Some(w) if isWrite(w)                          => BeamIO.isWriteReport(c)
          case Some(o: OtherNode) if !via(o.step.name) => readsOnlyReports(o, via + node.step.name)
          case _                                              => false
        }
      }
    }

    val unwritable = nodes.collect { case u: UnverifiableWriteNode => u }.map(u => UnverifiableStep(u.why, plan(u)))
    val (reporting, ending) = nodes.collect { case o: OtherNode if o.step.outputs.forall(c => !consumed.contains(c.name)) => o }
      .partition(o => readsOnlyReports(o, Set.empty))
    val endsPipeline = ending.map(o =>
      UnverifiableStep(s"a step that ends the pipeline but is not a recognised write: ${describe(o.step)}", plan(o), Some(o.step.name))
    )
    BeamTranslation(writes, unwritable ++ endsPipeline, reporting.map(o => describe(o.step)))
  }

  /** The `ir.UnknownPlan.sourceType` of a schema transform whose fields could not be read (`SchemaShape.Unreadable`). */
  val UnreadableShapeSourceType = "UnreadableSchemaTransform"

  private def isWrite(node: Node): Boolean = node.isInstanceOf[WriteNode] || node.isInstanceOf[UnverifiableWriteNode]

  private def describe(step: BeamStep): String = s"${step.name} (${BeamIO.simpleName(step)})"

  /** What `step` reads: its own inputs, then what the steps inside it take from outside it. A composite does not always list
    * everything it uses (a schema `Join` lists only its left side; the right reaches the steps inside it directly), so a read
    * feeding only those inner steps would otherwise not be upstream of anything. */
  private[beamadapter] def inputsOf(step: BeamStep): List[BeamCollection] = {
    val inner = step.withDescendants.tail
    val madeInside = inner.flatMap(_.outputs.map(_.name)).toSet
    val seen = mutable.Set.empty[String]
    (step.inputs ++ inner.flatMap(_.inputs).filterNot(c => madeInside.contains(c.name))).filter(c => seen.add(c.name))
  }

  // --- the steps that matter, found top-down so the outermost recognisable step wins ---

  private sealed trait Node { def step: BeamStep }
  private final case class ReadNode(step: BeamStep, locations: List[String], catalog: Option[com.invaract.ir.CatalogIdentity], schema: Option[LogicalSchema])
      extends Node {
    def columns: Option[LogicalSchema] = schema.orElse(step.outputs.headOption.flatMap(BeamSchemas.of))
  }
  private final case class FileReaderNode(step: BeamStep, schema: Option[LogicalSchema]) extends Node {
    def columns: Option[LogicalSchema] = schema.orElse(step.outputs.headOption.flatMap(BeamSchemas.of))
  }
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
    if (BeamIO.isInJobData(step)) List(OtherNode(step))
    else BeamIO.read(step).map(readNode(step, _)).orElse(BeamIO.write(step).map(writeNode(step, _))) match {
      case Some(node) => List(node)
      case None =>
        if (isFileWriteUnit(step)) List(writeNode(step, BeamIO.fileWrite(step)))
        else if (BeamShapes.recognises(step)) List(OtherNode(step))
        // a team's own step that holds IO, or a schema transform, is looked into so what it does is seen
        else if (step.isComposite && (containsIo(step) || step.withDescendants.tail.exists(BeamShapes.recognises))) step.children.flatMap(visible)
        else List(OtherNode(step))
    }

  private def readNode(step: BeamStep, read: IoRead): Node = read match {
    case IoRead.Located(locations, _, catalog, schema) => ReadNode(step, locations, catalog, schema)
    case IoRead.FileReader(_, schema)                  => FileReaderNode(step, schema)
    case IoRead.Unlocated(what)                => UnlocatedReadNode(step, what)
  }

  private def writeNode(step: BeamStep, write: IoWrite): Node = write match {
    case IoWrite.Located(location, format, saveMode, catalog, schema) => WriteNode(step, location, format, saveMode, catalog, schema)
    case IoWrite.Unverifiable(why)                                    => UnverifiableWriteNode(step, why)
  }

  private def isIo(s: BeamStep): Boolean = BeamIO.read(s).isDefined || BeamIO.write(s).isDefined || BeamIO.isFileWrite(s)

  /** Whether an IO step is beneath `step`, not counting the steps that make data in the job (an Impulse inside `Create.of`). */
  private def containsIo(step: BeamStep): Boolean = withoutJobData(step).exists(isIo)

  /** The outermost step that holds exactly one file write and no other IO: the write as the job wrote it (`TextIO.write()`,
    * `FileIO.write()`, or a team's composite around one). Anything holding more is searched deeper. */
  private def isFileWriteUnit(step: BeamStep): Boolean = {
    val all = withoutJobData(step)
    all.count(BeamIO.isFileWrite) == 1 && !all.exists(s => BeamIO.read(s).isDefined || BeamIO.write(s).isDefined)
  }

  /** `step` and every step beneath it, leaving out the steps that make data in the job and what they are built from (a
    * `Create.of` inside Beam's own `WriteFiles` is not a read inside the write). */
  private def withoutJobData(step: BeamStep): List[BeamStep] =
    if (BeamIO.isInJobData(step)) Nil else step :: step.children.flatMap(withoutJobData)

  /** The folder a read pattern reads from (kept here for the tests that pin it). */
  private[beamadapter] def readLocation(pattern: String): String = BeamLocations.folderOf(pattern)
}
