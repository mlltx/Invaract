// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Formats, LogicalSchema}
import com.invaract.ir.{Aggregate, BooleanExpr, CatalogIdentity, ColumnRef, ColumnReference, Comparison, DatasetRef, Expr, Filter, Join, Plan, Read, UDF, UnknownPlan, Write}
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

/** Something that changes data but that the translation cannot check: refused, never passed unchecked (fail closed). `name` is
  * the step's full name and `operation` what it is and why it cannot be checked. `trustable` is whether a platform may name it as
  * trusted (`TrustedSteps`, docs/BEAM_ADAPTER.md, D14): a step that ends the pipeline without being a recognised write is, a write
  * it cannot verify is not, since trusting that would pass data written somewhere the contract cannot see. */
private[beamadapter] final case class UnverifiableStep(name: String, operation: String, plan: Plan, trustable: Boolean)

/** The pipeline's writes, what it cannot verify, and the steps that end it but read only what writes report (`reportSteps`, let
  * through: D14). */
private[beamadapter] final case class BeamTranslation(writes: List[TranslatedWrite], unverifiable: List[UnverifiableStep], reportSteps: List[String] = Nil)

/** The pipeline graph (`BeamGraph`) as the engine-neutral plan (`ir.Plan`) the verifiers work on.
  *
  * What it recognises is deliberately small and exact: the IO transforms a `StepCatalogue` knows, whose source or destination the
  * graph states. Every other step becomes an `ir.UnknownPlan` that still carries the plans of the steps feeding it, so an
  * unrecognised step never hides a read beneath it; a read from outside the pipeline that states no location becomes an
  * `ir.UnknownPlan` of source type `UnlocatedSourceType`, which the verifiers treat as a place a declared input may be read without
  * being seen; and a step that ends the pipeline without being a recognised write is reported as unverifiable. The design reasons
  * are docs/BEAM_ADAPTER.md's D1 to D12. Beam's schema transforms (`BeamShapes`) become the `ir.Filter`, `ir.Aggregate` and
  * `ir.Join` they are, with the columns they use, so the transformation-shape rules can be checked (D19).
  */
private[beamadapter] object BeamTranslator {

  /** The `ir.UnknownPlan.sourceType` of a read from outside the pipeline that states no location. */
  val UnlocatedSourceType = "UnlocatedRead"

  /** The `ir.UnknownPlan.sourceType` of a schema transform whose fields could not be read (`SchemaShape.Unreadable`). */
  val UnreadableShapeSourceType = "UnreadableSchemaTransform"

  /** The `ir.UDF.engineType` of a filter's predicate: code, of which only the columns it is given are known. */
  val PredicateEngineType = "BeamPredicate"

  def translate(pipeline: Pipeline): BeamTranslation = translate(BeamGraph.of(pipeline), JavaSteps)

  /** The top-level steps `top`, whose steps `io` knows (`JavaSteps` for a Java pipeline, `PythonSteps` for one read from
    * its portable form). */
  def translate(top: List[BeamStep], io: StepCatalogue): BeamTranslation = new Translation(top.flatMap(visible(_, io)), io).result

  /** What `step` reads: its own inputs, then what the steps inside it take from outside it. A composite does not always list
    * everything it uses (a schema `Join` lists only its left side; the right reaches the steps inside it directly), so a read
    * feeding only those inner steps would otherwise not be upstream of anything. */
  private[beamadapter] def inputsOf(step: BeamStep): List[BeamCollection] = {
    val inner = step.withDescendants.tail
    val madeInside = inner.flatMap(_.outputs.map(_.name)).toSet
    distinctByName(step.inputs ++ inner.flatMap(_.inputs).filterNot(c => madeInside.contains(c.name)))(_.name)
  }

  private def describe(step: BeamStep): String = s"${step.name} (${step.simpleName})"

  /** `items` in order, each name once. By name: a node is a case class over the whole step subtree, which `distinct` would hash
    * in full. */
  private def distinctByName[A](items: List[A])(name: A => String): List[A] = {
    val seen = mutable.Set.empty[String]
    items.filter(i => seen.add(name(i)))
  }

  // --- the steps that matter, found top-down so the outermost recognisable step wins ---

  private sealed trait Node { def step: BeamStep }

  /** A read whose source is stated; `schema` is the one the read states for the dataset (else its output's columns are used). */
  private final case class ReadNode(step: BeamStep, locations: List[String], catalog: Option[CatalogIdentity], schema: Option[LogicalSchema]) extends Node

  /** A reader of files matched by an upstream step. */
  private final case class FileReaderNode(step: BeamStep, schema: Option[LogicalSchema]) extends Node
  private final case class UnlocatedReadNode(step: BeamStep, what: String) extends Node
  private final case class WriteNode(step: BeamStep, write: IoWrite.Located) extends Node
  private final case class UnverifiableWriteNode(step: BeamStep, why: String) extends Node
  private final case class OtherNode(step: BeamStep) extends Node

  /** The columns a read yields: the ones it states, else its output's. */
  private def columnsRead(step: BeamStep, stated: Option[LogicalSchema]): Option[LogicalSchema] =
    stated.orElse(step.outputs.headOption.flatMap(BeamSchemas.of))

  private def visible(step: BeamStep, io: StepCatalogue): List[Node] =
    if (io.isInJobData(step)) List(OtherNode(step))
    else io.read(step).map(readNode(step, _)).orElse(io.write(step).map(writeNode(step, _))) match {
      case Some(node) => List(node)
      case None =>
        if (isFileWriteUnit(step, io)) List(writeNode(step, io.fileWrite(step)))
        else if (io.recognisesShape(step)) List(OtherNode(step))
        // a team's own step that holds IO, or a schema transform, is looked into so what it does is seen
        else if (step.isComposite && (containsIo(step, io) || step.withDescendants.tail.exists(io.recognisesShape))) step.children.flatMap(visible(_, io))
        else List(OtherNode(step))
    }

  private def readNode(step: BeamStep, read: IoRead): Node = read match {
    case IoRead.Located(locations, _, catalog, schema) => ReadNode(step, locations, catalog, schema)
    case IoRead.FileReader(_, schema)                  => FileReaderNode(step, schema)
    case IoRead.Unlocated(what)                        => UnlocatedReadNode(step, what)
  }

  private def writeNode(step: BeamStep, write: IoWrite): Node = write match {
    case located: IoWrite.Located  => WriteNode(step, located)
    case IoWrite.Unverifiable(why) => UnverifiableWriteNode(step, why)
  }

  private def isIo(s: BeamStep, io: StepCatalogue): Boolean = io.read(s).isDefined || io.write(s).isDefined || io.isFileWrite(s)

  /** Whether an IO step is beneath `step`, not counting the steps that make data in the job (an Impulse inside `Create.of`). */
  private def containsIo(step: BeamStep, io: StepCatalogue): Boolean = withoutJobData(step, io).exists(isIo(_, io))

  /** The outermost step that holds exactly one file write and no other IO: the write as the job wrote it (`TextIO.write()`,
    * `FileIO.write()`, or a team's composite around one). Anything holding more is searched deeper. */
  private def isFileWriteUnit(step: BeamStep, io: StepCatalogue): Boolean = {
    val all = withoutJobData(step, io)
    all.count(io.isFileWrite) == 1 && !all.exists(s => io.read(s).isDefined || io.write(s).isDefined)
  }

  /** `step` and every step beneath it, leaving out the steps that make data in the job and what they are built from (a
    * `Create.of` inside Beam's own `WriteFiles` is not a read inside the write). */
  private def withoutJobData(step: BeamStep, io: StepCatalogue): List[BeamStep] =
    if (io.isInJobData(step)) Nil else step :: step.children.flatMap(withoutJobData(_, io))

  private def isWrite(node: Node): Boolean = node.isInstanceOf[WriteNode] || node.isInstanceOf[UnverifiableWriteNode]

  /** What a formatter feeding a write turns rows into: the text format, and the columns of the rows. */
  private final case class TextFormatter(format: String, columns: Option[LogicalSchema])

  /** One pipeline's translation, over its visible steps `nodes` in the order Beam reports them (inputs before the steps that use
    * them). Each node's plan and the nodes upstream of it are worked out once and kept, so a pipeline of thousands of steps is
    * translated in one pass, with no recursion deeper than one step. */
  private final class Translation(nodes: List[Node], io: StepCatalogue) {

    private val producers: Map[String, Node] = nodes.flatMap(n => n.step.outputs.map(_.name -> n)).toMap
    private val consumers: Map[String, List[Node]] =
      nodes.flatMap(n => inputsOf(n.step).map(_.name -> n)).groupBy(_._1).map { case (name, pairs) => name -> pairs.map(_._2) }
    private val plans = mutable.Map.empty[String, Plan]
    private val upstreams = mutable.Map.empty[String, List[Node]]

    def result: BeamTranslation = {
      // inputs first: each plan finds its sources' plans already made
      nodes.foreach(plan)
      val writes = nodes.collect { case w: WriteNode => translatedWrite(w) }
      val unwritable = nodes.collect { case u: UnverifiableWriteNode => UnverifiableStep(u.step.name, u.why, plan(u), trustable = false) }
      val (reporting, ending) = nodes.collect { case o: OtherNode if o.step.outputs.forall(c => !consumers.contains(c.name)) => o }
        .partition(o => readsOnlyReports(o.step.name))
      val endsPipeline = ending.map { o =>
        UnverifiableStep(o.step.name, s"a step that ends the pipeline but is not a recognised write: ${describe(o.step)}", plan(o), trustable = true)
      }
      BeamTranslation(writes, unwritable ++ endsPipeline, reporting.map(o => describe(o.step)))
    }

    /** The nodes whose output `node` reads. What a write hands on (the files it wrote, a signal that it is done) is about the
      * write, not data it read, so a write is never a source of data. */
    private def sourcesOf(node: Node): List[Node] =
      distinctByName(inputsOf(node.step).flatMap(i => producers.get(i.name)).filterNot(isWrite))(_.step.name)

    /** Every node upstream of `node`, each once, depth first (iterative: a pipeline can be thousands of steps deep). */
    private def upstream(node: Node): List[Node] = upstreams.getOrElseUpdate(node.step.name, {
      val seen = mutable.Set.empty[String]
      val out = mutable.ListBuffer.empty[Node]
      var pending = sourcesOf(node)
      while (pending.nonEmpty) {
        val n = pending.head
        pending = pending.tail
        if (seen.add(n.step.name)) {
          out += n
          pending = sourcesOf(n) ++ pending
        }
      }
      out.toList
    })

    // --- plans ---

    private def plan(node: Node): Plan = plans.getOrElseUpdate(node.step.name, node match {
      case r: ReadNode =>
        r.locations.map(l => Read(DatasetRef(l), catalog = r.catalog)) match {
          case only :: Nil => only
          case many        => UnknownPlan(s"${r.step.name}: several sources", r.step.simpleName, many)
        }
      case u: UnlocatedReadNode => passThrough(u, s"${u.step.name}: ${u.what}", UnlocatedSourceType)
      // a reader handed files by a step that does not say where they are is itself a read whose source is not located
      case f: FileReaderNode        => passThrough(f, describe(f.step), if (readsLocated(f)) f.step.simpleName else UnlocatedSourceType)
      case o: OtherNode             => io.shape(o.step).fold(passThrough(o, describe(o.step), o.step.simpleName))(shaped(o, _))
      case w: WriteNode             => writePlan(w, w.write)
      case u: UnverifiableWriteNode => passThrough(u, s"${u.step.name}: ${u.why}", u.step.simpleName)
    })

    /** A step the translation does not represent, carrying the plans of what feeds it so nothing upstream is hidden. */
    private def passThrough(n: Node, description: String, sourceType: String): Plan = UnknownPlan(description, sourceType, sourcesOf(n).map(plan))

    /** The plan feeding `node`: its one input's, or a node holding them all (or saying there is none). */
    private def inputPlan(node: Node): Plan = sourcesOf(node).map(plan) match {
      case only :: Nil => only
      case Nil         => UnknownPlan(s"${node.step.name}: no input was found", node.step.simpleName)
      case many        => UnknownPlan(s"${node.step.name}: several inputs", "Inputs", many)
    }

    private def writePlan(w: WriteNode, write: IoWrite.Located): Write =
      Write(DatasetRef(write.location), inputPlan(w), write.format, write.saveMode, write.catalog)

    /** A schema transform as the plan node it is: the filter, grouping or join, over the plans of what feeds it. */
    private def shaped(o: OtherNode, shape: SchemaShape): Plan = shape match {
      case SchemaShape.Filtered(predicates) =>
        // one predicate per filter, each a function of the columns it is given, all of which must hold
        val conditions = predicates.map(cols => UDF(None, cols.map(c => ColumnReference(ColumnRef(c))), Some(PredicateEngineType)): Expr)
        Filter(inputPlan(o), allOf(conditions).getOrElse(UDF(None, Nil, Some(PredicateEngineType))))
      case SchemaShape.Grouped(keys) => Aggregate(inputPlan(o), keys.map(k => ColumnReference(ColumnRef(k))), Nil)
      case SchemaShape.Joined(joinType, first :: rest) =>
        def sidePlan(side: JoinSide): Plan =
          producers.get(side.collection).map(plan).getOrElse(UnknownPlan(s"${o.step.name}: no step produces ${side.collection}", o.step.simpleName))
        rest.foldLeft(sidePlan(first)) { (joined, side) =>
          // each side is matched with the first on its key columns, position by position; a side with none is a cross product
          val pairs = first.keys.zip(side.keys).map { case (l, r) =>
            Comparison("=", ColumnReference(ColumnRef(l, Some(first.qualifier))), ColumnReference(ColumnRef(r, Some(side.qualifier)))): Expr
          }
          Join(joined, sidePlan(side), joinType, allOf(pairs))
        }
      case SchemaShape.Joined(_, Nil) | SchemaShape.Unreadable(_) => passThrough(o, describe(o.step), UnreadableShapeSourceType)
    }

    private def allOf(conditions: List[Expr]): Option[Expr] = conditions.reduceOption((a, b) => BooleanExpr("AND", List(a, b)))

    private def readsLocated(f: FileReaderNode): Boolean = upstream(f).exists(_.isInstanceOf[ReadNode])

    // --- writes ---

    private def translatedWrite(w: WriteNode): TranslatedWrite = {
      val above = upstream(w)
      val formatter = textFormatter(w)
      // a text write of what a formatter made from rows (ToJson) is in the formatter's format
      val written = formatter.filter(_ => w.write.format.contains(Formats.Text)).fold(w.write)(f => w.write.copy(format = Some(f.format)))
      val others = above.collect { case o: OtherNode => o }
      TranslatedWrite(
        written.location,
        written.format,
        writePlan(w, written),
        written.schema.orElse(w.step.inputs.headOption.flatMap(BeamSchemas.of)).orElse(formatter.flatMap(_.columns)),
        (readSchemas(above) ++ readerSchemas(above)).distinct,
        unlocatedReads(above).distinct,
        // what the transformation-shape rules cannot see past (D19): a step that combines several collections in code (not
        // Flatten, which only concatenates them), or a schema transform it could not read
        others.collect { case o if !io.recognisesShape(o.step) && !io.isUnion(o.step) && sourcesOf(o).size > 1 => describe(o.step) }.distinct,
        others.collect { case o if io.shape(o.step).exists(_.isInstanceOf[SchemaShape.Unreadable]) => describe(o.step) }.distinct
      )
    }

    /** The columns of each located read in `above`, by location: what the read yields, or, for a text read parsed straight away
      * into rows (`JsonToRow`, `CsvIO.parseRows`), the parser's columns. */
    private def readSchemas(above: List[Node]): List[(String, LogicalSchema)] =
      above.collect { case r: ReadNode => r }.flatMap { r =>
        columnsRead(r.step, r.schema).orElse(parsedColumns(r)).toList.flatMap(s => r.locations.map(_ -> s))
      }

    private def parsedColumns(r: ReadNode): Option[LogicalSchema] =
      r.step.outputs.headOption.flatMap(o => consumers.getOrElse(o.name, Nil) match {
        case parser :: Nil if io.isTextParser(parser.step) => parser.step.outputs.headOption.flatMap(BeamSchemas.of)
        case _                                             => None
      })

    /** The columns of each file reader in `above`, for the folders the matches feeding it read. */
    private def readerSchemas(above: List[Node]): List[(String, LogicalSchema)] =
      above.collect { case f: FileReaderNode => f }.flatMap { f =>
        columnsRead(f.step, f.schema).toList.flatMap(s => upstream(f).collect { case r: ReadNode => r }.flatMap(_.locations).map(_ -> s))
      }

    private def unlocatedReads(above: List[Node]): List[String] =
      above.collect {
        case u: UnlocatedReadNode                  => s"${u.step.name}: ${u.what}"
        case f: FileReaderNode if !readsLocated(f) => s"${f.step.name}: files handed to it by a step that does not say where they are"
      }

    /** The step feeding `w`, when it is the only one and turns rows into lines of a known format (`ToJson`). */
    private def textFormatter(w: WriteNode): Option[TextFormatter] = sourcesOf(w) match {
      case (o: OtherNode) :: Nil => io.textFormatterFormat(o.step).map(TextFormatter(_, o.step.inputs.headOption.flatMap(BeamSchemas.of)))
      case _                     => None
    }

    // --- steps that only report on writes (D14) ---

    /** The steps that read only what writes report - the files they wrote, the tables they loaded - directly or through steps
      * that themselves read only that: they see names, not data. A write's result that holds rows (a BigQuery write's failed
      * inserts) is data, and so is anything that does not come from a write. Decided in the order Beam reports the steps, so each
      * step's sources are decided first; a source not yet decided counts as data. */
    private lazy val readsOnlyReports: Set[String] =
      nodes.foldLeft(Set.empty[String]) {
        case (decided, o: OtherNode) =>
          val read = inputsOf(o.step)
          val onlyReports = read.nonEmpty && read.forall { c =>
            producers.get(c.name) match {
              case Some(w) if isWrite(w)  => BeamIO.isWriteReport(c)
              case Some(other: OtherNode) => decided(other.step.name)
              case _                      => false
            }
          }
          if (onlyReports) decided + o.step.name else decided
        case (decided, _) => decided
      }
  }
}
