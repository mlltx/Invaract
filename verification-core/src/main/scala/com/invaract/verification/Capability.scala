// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.LoaderOptions

import java.io.InputStream
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** A group of related `Capability`s, in the order the capability matrix shows them. */
sealed abstract class CapabilityCategory(val id: String, val title: String)

object CapabilityCategory {
  case object Operations extends CapabilityCategory("operations", "Operations an adapter recognizes")
  case object StructuralChecks extends CapabilityCategory("structural", "Structural checks")
  case object Rules extends CapabilityCategory("rules", "Rules")
  case object Analysis extends CapabilityCategory("analysis", "Analysis and lineage")
  case object Governance extends CapabilityCategory("governance", "Governance")
  case object Attachment extends CapabilityCategory("attachment", "Attaching it to a job")
  case object Reporting extends CapabilityCategory("reporting", "Reporting")
  case object FailClosed extends CapabilityCategory("failclosed", "Fail-closed behaviour")

  val all: List[CapabilityCategory] = List(Operations, StructuralChecks, Rules, Analysis, Governance, Attachment, Reporting, FailClosed)
}

/** One thing the verification engine can do, in engine-neutral terms - the unit an adapter must
  * state its position on. Every adapter declares every capability here (see `AdapterCapabilities`):
  * an adapter that cannot do something says so, with a reason, rather than the gap being discovered
  * later by someone whose contract silently stopped being checked.
  *
  * @param id a stable, dotted identifier used in declaration files and violations; never reused.
  * @param enforcesContract true when the capability is what makes a *declared contract requirement*
  *   actually checked (a schema, a rule, a catalog requirement). If a contract relies on one of
  *   these and the adapter declares it `Unsupported`, the pipeline rejects the write
  *   (`UNSUPPORTED_CONTRACT_FEATURE`) instead of passing a requirement nothing verified. The others
  *   describe infrastructure or optional reporting, and never block.
  */
final case class Capability(id: String, category: CapabilityCategory, description: String, enforcesContract: Boolean)

object Capability {
  import CapabilityCategory._

  // --- operations -------------------------------------------------------------------------------
  val ReadBatch = Capability("read.batch", Operations, "A batch read is recognized as one of the contract's inputs.", enforcesContract = false)
  val ReadStreaming = Capability("read.streaming", Operations, "A streaming read is recognized as one of the contract's inputs.", enforcesContract = false)
  val WriteBatch = Capability("write.batch", Operations, "A batch write is recognized and checked against the contract before it executes.", enforcesContract = false)
  val WriteStreaming = Capability("write.streaming", Operations, "A streaming write is recognized and checked against the contract.", enforcesContract = false)
  val WriteRowLevelDml = Capability("write.rowLevelDml", Operations, "Row-level MERGE / UPDATE / DELETE is recognized as a write.", enforcesContract = false)
  val WriteStateChange = Capability("write.stateChange", Operations, "A non-write operation that commits a schema change at a location (for example a snapshot rollback) is checked.", enforcesContract = false)

  // --- structural checks ------------------------------------------------------------------------
  val CheckInputExistence = Capability("check.inputExistence", StructuralChecks, "Declared inputs must be read (MISSING_INPUT), and undeclared reads can be rejected (UNDECLARED_INPUT).", enforcesContract = true)
  val CheckLocation = Capability("check.location", StructuralChecks, "A write lands where the contract says (OUTPUT_LOCATION_MISMATCH).", enforcesContract = true)
  val CheckSchema = Capability("check.schema", StructuralChecks, "Field presence, type and nullability of inputs and outputs.", enforcesContract = true)
  val CheckNestedTypes = Capability("check.nestedTypes", StructuralChecks, "Nested types (array, map, struct) are compared structurally, not just by keyword.", enforcesContract = true)
  val CheckFormat = Capability("check.format", StructuralChecks, "A declared output format is checked (OUTPUT_FORMAT_MISMATCH).", enforcesContract = true)
  val CheckSaveMode = Capability("check.saveMode", StructuralChecks, "A declared save mode is checked (OUTPUT_SAVE_MODE_MISMATCH).", enforcesContract = true)
  val CheckCatalogRegistration = Capability("check.catalogRegistration", StructuralChecks, "A declared catalog requirement is checked for inputs and outputs.", enforcesContract = true)

  // --- rules ------------------------------------------------------------------------------------
  val RulesDml = Capability("rules.dml", Rules, "DML rules: merge_condition, forbid_unconditional_delete, allowed_update_columns.", enforcesContract = true)
  val RulesPlanShape = Capability("rules.planShape", Rules, "Transformation-shape rules: required_group_by, forbid_cross_join, required_join_columns, required_filter_columns.", enforcesContract = true)
  val RulesCustom = Capability("rules.custom", Rules, "Custom rule types (customRuleTypes) resolve and run.", enforcesContract = true)

  // --- analysis and lineage ---------------------------------------------------------------------
  val AnalysisStaticDataQuality = Capability("analysis.staticDataQuality", Analysis, "Static data-quality proof of nullable and constraint declarations (opt-in).", enforcesContract = true)
  val AnalysisRoleConsistency = Capability("analysis.roleConsistency", Analysis, "A dataset's declared role (SOURCE / CONTROL / DATA_ASSET) is checked against its observed use (opt-in).", enforcesContract = true)
  val AnalysisFingerprint = Capability("analysis.fingerprint", Analysis, "A semantic fingerprint of the transformation is computed (opt-in).", enforcesContract = false)
  val AnalysisFunctionCatalog = Capability("analysis.functionCatalog", Analysis, "Engine-native function names are mapped onto the canonical function catalog, so non-determinism is classified - and fingerprints stay comparable - the same on every engine.", enforcesContract = false)
  val AnalysisSensitivityPropagation = Capability("analysis.sensitivityPropagation", Analysis, "Input sensitivity tags are propagated to the output columns derived from them (report-only).", enforcesContract = false)
  val LineageColumnLevel = Capability("lineage.columnLevel", Analysis, "Column-level lineage through the transformation (what each output column derives from).", enforcesContract = false)
  val LineageBoundaryResolution = Capability("lineage.boundaryResolution", Analysis, "A point that erases lineage (a checkpoint, a cache) is seen through to the work behind it.", enforcesContract = false)

  // --- governance -------------------------------------------------------------------------------
  val PolicyOrganizational = Capability("policy.organizational", Governance, "Organizational policy layers are enforced on the contract itself.", enforcesContract = false)

  // --- attaching it to a job --------------------------------------------------------------------
  val ConfigZeroCodeInstall = Capability("config.zeroCodeInstall", Attachment, "Installed on a job through the engine's own configuration, with no change to the job's source.", enforcesContract = false)
  val ConfigLocationRefs = Capability("config.locationRefs", Attachment, "ref://<id> locations in a contract resolve through configuration.", enforcesContract = false)
  val ConfigContractRegistry = Capability("config.contractRegistry", Attachment, "The contract can be named as registry://<id>@<version> and fetched from a registry.", enforcesContract = false)

  // --- reporting --------------------------------------------------------------------------------
  val ReportingNotifications = Capability("reporting.notifications", Reporting, "Validation and write events are published to a notification sink.", enforcesContract = false)
  val ReportingDryRun = Capability("reporting.dryRunInference", Reporting, "Dry-run mode infers a draft contract from a real write.", enforcesContract = false)

  // --- fail-closed ------------------------------------------------------------------------------
  val FailClosedUnverifiableWrites = Capability("failClosed.unverifiableWrites", FailClosed, "An operation that looks like it writes but cannot be translated is rejected rather than passed unchecked.", enforcesContract = false)

  /** Every capability, in the order the matrix shows them (category order, then declaration order). */
  val all: List[Capability] = List(
    ReadBatch, ReadStreaming, WriteBatch, WriteStreaming, WriteRowLevelDml, WriteStateChange,
    CheckInputExistence, CheckLocation, CheckSchema, CheckNestedTypes, CheckFormat, CheckSaveMode, CheckCatalogRegistration,
    RulesDml, RulesPlanShape, RulesCustom,
    AnalysisStaticDataQuality, AnalysisRoleConsistency, AnalysisFingerprint, AnalysisFunctionCatalog, AnalysisSensitivityPropagation, LineageColumnLevel, LineageBoundaryResolution,
    PolicyOrganizational,
    ConfigZeroCodeInstall, ConfigLocationRefs, ConfigContractRegistry,
    ReportingNotifications, ReportingDryRun,
    FailClosedUnverifiableWrites
  )

  val byId: Map[String, Capability] = all.map(c => c.id -> c).toMap
}

/** An adapter's position on one capability. */
sealed abstract class Support(val id: String)

object Support {

  /** Done, for every shape of the operation the adapter recognizes. */
  case object Supported extends Support("supported")

  /** Done for some shapes; the entry's note says which are not. Never blocks a write on its own. */
  case object Partial extends Support("partial")

  /** Not done. If a contract relies on a capability that `enforcesContract`, the write is rejected. */
  case object Unsupported extends Support("unsupported")

  /** Does not exist in this engine (an engine with no streaming, no catalog). */
  case object NotApplicable extends Support("not-applicable")

  val all: List[Support] = List(Supported, Partial, Unsupported, NotApplicable)
  val byId: Map[String, Support] = all.map(s => s.id -> s).toMap
}

/** Where an adapter can stop a bad operation - the other half of "what does this adapter do". */
sealed abstract class EnforcementPoint(val id: String, val description: String)

object EnforcementPoint {

  /** Inside the engine, before the operation executes, aborting it (Spark's check rule). */
  case object InEngineBlocking extends EnforcementPoint("in-engine-blocking", "Blocks inside the engine, before the operation executes")

  /** Before the job is submitted (a CI step, an orchestrator hook) - it can refuse to run it, but cannot see what the engine does at runtime. */
  case object PreSubmitGate extends EnforcementPoint("pre-submit-gate", "Gates before the job is submitted")

  /** After the fact - it reports a violation but cannot stop it. */
  case object ObserveOnly extends EnforcementPoint("observe-only", "Observes only; cannot block")

  val all: List[EnforcementPoint] = List(InEngineBlocking, PreSubmitGate, ObserveOnly)
  val byId: Map[String, EnforcementPoint] = all.map(p => p.id -> p).toMap
}

/** An adapter's note on one capability: why it is partial / unsupported / not applicable, and
  * optionally where it is documented.
  */
final case class CapabilityEntry(support: Support, note: Option[String] = None, docs: Option[String] = None)

/** What one engine adapter does and does not do - complete by construction. `parse` refuses a
  * declaration that leaves any capability undeclared, so the question "does this adapter check X?"
  * always has a written answer; the capability matrix in the docs is generated from these, and the
  * pipeline uses them to reject a contract that relies on something the adapter cannot verify.
  *
  * @param adapter the adapter's short name ("spark").
  * @param engine a human-readable engine and version ("Apache Spark 3.5").
  * @param enforcement where this adapter can stop a bad operation.
  */
final case class AdapterCapabilities(
    adapter: String,
    engine: String,
    enforcement: EnforcementPoint,
    enforcementNote: Option[String],
    entries: Map[Capability, CapabilityEntry]
) {
  def entryOf(capability: Capability): CapabilityEntry = entries(capability)

  def supportOf(capability: Capability): Support = entries(capability).support
}

object AdapterCapabilities {

  /** Parses a declaration file:
    *
    * {{{
    * adapter: spark
    * engine: Apache Spark 3.5
    * enforcement:
    *   point: in-engine-blocking        # or pre-submit-gate, observe-only
    *   note: ...
    * capabilities:
    *   read.batch:
    *     status: supported              # or partial, unsupported, not-applicable
    *     note: ...                      # required unless supported
    *     docs: connectors/delta.md      # optional
    * }}}
    *
    * `Left` carries every problem found (not just the first): an unknown capability id, a bad
    * status, a missing note, and - the point of the file - every capability left undeclared.
    */
  def parse(yaml: String): Either[List[String], AdapterCapabilities] =
    try {
      val loaderOptions = new LoaderOptions
      val root = new Yaml(new SafeConstructor(loaderOptions)).load[AnyRef](yaml)
      root match {
        case map: JMap[_, _] => fromMap(map.asInstanceOf[JMap[String, AnyRef]].asScala.toMap)
        case _               => Left(List("the declaration must be a YAML mapping with 'adapter', 'engine', 'enforcement' and 'capabilities'"))
      }
    } catch {
      case NonFatal(e) => Left(List(s"not valid YAML: ${e.getMessage}"))
    }

  /** `parse` of a classpath resource; `None` if the resource is absent. */
  def fromResource(path: String, classLoader: ClassLoader): Option[Either[List[String], AdapterCapabilities]] =
    Option(classLoader.getResourceAsStream(path)).map { stream =>
      try parse(read(stream))
      finally stream.close()
    }

  private def read(stream: InputStream): String = scala.io.Source.fromInputStream(stream, "UTF-8").mkString

  private def str(value: AnyRef): Option[String] = value match {
    case s: String if s.trim.nonEmpty => Some(s.trim)
    case _                            => None
  }

  private def fromMap(root: Map[String, AnyRef]): Either[List[String], AdapterCapabilities] = {
    val problems = List.newBuilder[String]

    val adapter = root.get("adapter").flatMap(str)
    if (adapter.isEmpty) problems += "'adapter' is required (the adapter's short name)"
    val engine = root.get("engine").flatMap(str)
    if (engine.isEmpty) problems += "'engine' is required (the engine and version, for the matrix)"

    val enforcementMap: Map[String, AnyRef] = root.get("enforcement") match {
      case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, AnyRef]].asScala.toMap
      case _                   => Map.empty
    }
    val point = enforcementMap.get("point").flatMap(str).flatMap(EnforcementPoint.byId.get)
    if (point.isEmpty) {
      problems += s"'enforcement.point' is required and must be one of: ${EnforcementPoint.all.map(_.id).mkString(", ")}"
    }
    val enforcementNote = enforcementMap.get("note").flatMap(str)

    val declared: Map[String, AnyRef] = root.get("capabilities") match {
      case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, AnyRef]].asScala.toMap
      case _ =>
        problems += "'capabilities' is required and must map each capability id to its declaration"
        Map.empty
    }

    val unknown = declared.keys.filterNot(Capability.byId.contains).toList.sorted
    unknown.foreach { id =>
      problems += s"unknown capability '$id' (known: ${Capability.all.map(_.id).mkString(", ")})"
    }

    val entries = Map.newBuilder[Capability, CapabilityEntry]
    Capability.all.foreach { capability =>
      declared.get(capability.id) match {
        case None =>
          problems += s"capability '${capability.id}' is not declared - every capability must be declared supported, partial, " +
            "unsupported or not-applicable; an undeclared gap is exactly what this file exists to prevent"
        case Some(m: JMap[_, _]) =>
          parseEntry(capability, m.asInstanceOf[JMap[String, AnyRef]].asScala.toMap) match {
            case Right(entry) => entries += capability -> entry
            case Left(error)  => problems += error
          }
        case Some(_) => problems += s"capability '${capability.id}' must be a mapping with a 'status'"
      }
    }

    val errors = problems.result()
    if (errors.nonEmpty) Left(errors)
    else Right(AdapterCapabilities(adapter.get, engine.get, point.get, enforcementNote, entries.result()))
  }

  private def parseEntry(capability: Capability, m: Map[String, AnyRef]): Either[String, CapabilityEntry] = {
    val status = m.get("status").flatMap(str).flatMap(Support.byId.get)
    val note = m.get("note").flatMap(str)
    val docs = m.get("docs").flatMap(str)
    status match {
      case None =>
        Left(s"capability '${capability.id}' needs a 'status' of: ${Support.all.map(_.id).mkString(", ")}")
      case Some(Support.Supported) => Right(CapabilityEntry(Support.Supported, note, docs))
      case Some(other) =>
        if (note.isEmpty) Left(s"capability '${capability.id}' is ${other.id}, so it needs a 'note' saying why (and, for partial, which shapes are not covered)")
        else Right(CapabilityEntry(other, note, docs))
    }
  }
}
