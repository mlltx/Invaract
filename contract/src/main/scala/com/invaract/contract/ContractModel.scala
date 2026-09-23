// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import scala.collection.JavaConverters._

/** Raised when a contract document cannot be parsed into the object model. */
class ContractParseException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Semantic version for a contract: MAJOR.MINOR.PATCH.
  *
  * Contract versioning follows the same MAJOR/MINOR/PATCH semantics as the
  * project itself (see docs/VERSIONING.md): MAJOR for breaking schema
  * changes, MINOR for backward-compatible additions, PATCH for non-schema
  * changes (description, metadata).
  */
case class ContractVersion(major: Int, minor: Int, patch: Int) extends Ordered[ContractVersion] {
  def compare(that: ContractVersion): Int = {
    if (major != that.major) major compare that.major
    else if (minor != that.minor) minor compare that.minor
    else patch compare that.patch
  }

  override def toString: String = s"$major.$minor.$patch"
}

object ContractVersion {
  private val Pattern = """^(\d+)\.(\d+)(?:\.(\d+))?$""".r

  def parse(raw: String): ContractVersion = raw.trim match {
    case Pattern(maj, min, null)  => ContractVersion(maj.toInt, min.toInt, 0)
    case Pattern(maj, min, patch) => ContractVersion(maj.toInt, min.toInt, patch.toInt)
    case other =>
      throw new ContractParseException(
        s"Invalid contract version '$other'. Expected MAJOR.MINOR[.PATCH], e.g. '1.0.0'"
      )
  }
}

/** A single field within a schema.
  *
  * Fields may nest via `properties` to represent struct/record types. A field
  * with a non-empty `properties` list is treated as a struct regardless of
  * its declared `fieldType`.
  *
  * @param required whether the field must be present (contract-level semantics)
  * @param nullable whether the field's value may be null when present
  * @param sensitivityTags open-vocabulary governance labels for this field
  *   (e.g. `"pii"`, `"financial"`, `"restricted"`) — Invaract does not
  *   define or restrict which tags exist, the same way `ContractRule.ruleType`
  *   accepts any string; it only propagates whichever tags an input field
  *   carries to every output column that transitively derives from it (see
  *   `spark-adapter`'s `SensitivityLineage`). Empty for a field with no
  *   declared sensitivity, which is most fields.
  */
case class Field(
  name: String,
  fieldType: String,
  required: Boolean = false,
  nullable: Boolean = true,
  properties: List[Field] = Nil,
  sensitivityTags: Set[String] = Set.empty,
  constraints: List[FieldConstraint] = Nil
) {
  def isStruct: Boolean = properties.nonEmpty
}

/** An ordered collection of fields describing the shape of a dataset. */
case class Schema(fields: List[Field]) {
  def field(name: String): Option[Field] = fields.find(_.name == name)
}

/** A single declarative, statically-verifiable value-domain requirement on
  * a `Field` — see docs/STATIC_DATA_QUALITY_VERIFICATION.md for the full
  * design. `NOT NULL` needs no entry here at all: `Field.nullable = false`
  * already states it, and `spark-adapter`'s static data-quality verifier
  * attempts to prove that existing declaration directly. This is for the
  * three kinds with no other contract representation.
  *
  * On an *input* field, a `constraints` entry is an axiom — trusted as
  * given, seeding the analysis, never independently re-verified against
  * real input data by this module. On an *output* field, it's an
  * obligation the analysis attempts to prove. Same shape, same `Field`,
  * different role purely by which side of the contract it sits on — the
  * same convention `required`/`nullable` already follow.
  */
case class FieldConstraint(constraintType: String, properties: Map[String, Any]) {

  /** Decodes `properties` into one of `InterpretedFieldConstraint`'s
    * shapes when `constraintType` is one this module currently interprets
    * and its properties are well-formed — `None` for both "not a type
    * this module interprets" and "malformed/unrecognized properties for a
    * type it does," the same total/safe convention `ContractRule.interpret`
    * already establishes; `ContractValidator` is where a malformed known
    * constraint type becomes a reported issue.
    */
  def interpret: Option[InterpretedFieldConstraint] = constraintType match {
    case FieldConstraintType.Equals =>
      if (properties.keySet != Set("value")) None
      else properties.get("value").filter(_ != null).map(v => InterpretedFieldConstraint.Equals(v, FieldConstraint.literalTypeOf(v)))

    case FieldConstraintType.OneOf =>
      if (properties.keySet != Set("values")) None
      else FieldConstraint.valueSet(properties.get("values")).map(vs => InterpretedFieldConstraint.OneOf(vs, FieldConstraint.literalTypeOf(vs.head)))

    case FieldConstraintType.Range =>
      val knownKeys = Set("gte", "gt", "lte", "lt")
      if (properties.keySet.diff(knownKeys).nonEmpty) None
      else {
        val parsed: Map[String, Option[BigDecimal]] = knownKeys.flatMap(k => properties.get(k).map(k -> FieldConstraint.numeric(_))).toMap
        if (parsed.values.exists(_.isEmpty)) None // a declared bound that failed to parse as numeric
        else {
          val gte = parsed.get("gte").flatten
          val gt = parsed.get("gt").flatten
          val lte = parsed.get("lte").flatten
          val lt = parsed.get("lt").flatten
          if (gte.isDefined && gt.isDefined) None // redundant/contradictory - pick one
          else if (lte.isDefined && lt.isDefined) None
          else if (List(gte, gt, lte, lt).forall(_.isEmpty)) None // at least one bound required
          else Some(InterpretedFieldConstraint.Range(gte, gt, lte, lt))
        }
      }

    case FieldConstraintType.Length =>
      val knownKeys = Set("exact", "min", "max")
      if (properties.keySet.diff(knownKeys).nonEmpty) None
      else {
        val parsed: Map[String, Option[Int]] = knownKeys.flatMap(k => properties.get(k).map(k -> FieldConstraint.nonNegativeInt(_))).toMap
        if (parsed.values.exists(_.isEmpty)) None // a declared bound that failed to parse as a non-negative integer
        else {
          val exact = parsed.get("exact").flatten
          val min = parsed.get("min").flatten
          val max = parsed.get("max").flatten
          if (exact.isDefined && (min.isDefined || max.isDefined)) None // redundant/contradictory - pick one
          else if (List(exact, min, max).forall(_.isEmpty)) None // at least one bound required
          else if (min.isDefined && max.isDefined && min.get > max.get) None // an empty, unsatisfiable range
          else Some(InterpretedFieldConstraint.Length(exact, min, max))
        }
      }

    case _ => None
  }
}

object FieldConstraint {
  private def literalTypeOf(v: Any): String = v match {
    case _: java.lang.Boolean                        => "boolean"
    case _: java.lang.Integer                         => "integer"
    case _: java.lang.Long                             => "long"
    case _: java.lang.Short                            => "short"
    case _: java.lang.Double                           => "double"
    case _: java.lang.Float                             => "float"
    case _: java.math.BigDecimal | _: scala.math.BigDecimal => "decimal"
    case _                                              => "string"
  }

  private def valueSet(raw: Option[Any]): Option[Set[Any]] = raw match {
    case Some(l: java.util.List[_]) =>
      val items = l.asScala.toList
      if (items.nonEmpty && items.forall(_ != null)) Some(items.toSet) else None
    case _ => None
  }

  private def numeric(raw: Any): Option[BigDecimal] = raw match {
    case null           => None
    case n: java.lang.Number => scala.util.Try(BigDecimal(n.toString)).toOption
    case s: String       => scala.util.Try(BigDecimal(s)).toOption
    case _                => None
  }

  private def nonNegativeInt(raw: Any): Option[Int] = numeric(raw).flatMap { bd =>
    if (bd.isValidInt && bd >= 0) Some(bd.toInt) else None
  }
}

/** The closed set of `FieldConstraint.constraintType`s this module
  * currently interprets — deliberately narrow, mirroring `RuleType`'s own
  * discipline, not a general constraint language. See
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md §2.3/§8.
  */
object FieldConstraintType {
  val Equals = "equals"
  val OneOf = "oneOf"
  val Range = "range"
  val Length = "length"

  val All: Set[String] = Set(Equals, OneOf, Range, Length)
}

/** A `FieldConstraint`, decoded into one of the shapes
  * `spark-adapter`'s static data-quality verifier currently knows how to
  * check. Deliberately narrow, mirroring `InterpretedRule`'s own
  * discipline — not a general constraint language.
  */
sealed trait InterpretedFieldConstraint
object InterpretedFieldConstraint {
  case class Equals(value: Any, literalType: String) extends InterpretedFieldConstraint
  case class OneOf(values: Set[Any], literalType: String) extends InterpretedFieldConstraint
  case class Range(gte: Option[BigDecimal], gt: Option[BigDecimal], lte: Option[BigDecimal], lt: Option[BigDecimal]) extends InterpretedFieldConstraint

  /** A string-valued field's length: either an `exact` length, or a
    * `min`/`max` (inclusive) range — never both `exact` and a `min`/`max`
    * on the same constraint (`FieldConstraint.interpret` rejects that
    * combination as malformed). See docs/STATIC_DATA_QUALITY_VERIFICATION.md
    * §3.9.
    */
  case class Length(exact: Option[Int], min: Option[Int], max: Option[Int]) extends InterpretedFieldConstraint
}

/** A dataset's expected data-catalog registration — e.g. "this must be
  * registered in a specific Hive metastore, under this database/table
  * name," not merely written to a physical location. Independent of
  * `Dataset.format`: a dataset can declare a format with no catalog
  * requirement at all (today's behavior, unchanged), a format with
  * `catalog.required = false` (an informational expected shape that
  * gates nothing), or a format with `catalog.required = true` (checked
  * by `StructuralVerifier`/`ContractEnforcementRule` against the actual
  * write's or read's resolved catalog identity).
  *
  * Every identity sub-field beyond `required` is independently optional,
  * the same "both sides known" convention `format`/`saveMode` already
  * use: declaring only `required: true` means "must be registered
  * *somewhere*"; adding `technology`/`catalogName`/`location`/`namespace`/
  * `table` progressively pins down *where*, and only the sub-fields
  * actually declared here are compared against the observed catalog
  * identity — an undeclared sub-field is never treated as "must be
  * absent."
  *
  * @param required whether this dataset must be registered in a catalog
  *   at all. `false` (or the whole `catalog` block being absent from
  *   the contract) means no check is performed — see the class doc.
  * @param technology the catalog implementation, e.g. `"hive"`,
  *   `"delta"`, `"iceberg"` — open-vocabulary, not a closed enum (an
  *   org's ecosystem can include catalog technologies Invaract has no
  *   built-in name for).
  * @param catalogName the Spark-level catalog plugin/session-catalog
  *   name, e.g. `"spark_catalog"` — a *local* alias, not necessarily
  *   unique across an organization's many Spark sessions.
  * @param location the catalog *service's* own address (e.g. a Hive
  *   metastore's `thrift://host:port` URI) — the durable, technology-
  *   specific answer to "which Hive," distinct from `catalogName`'s
  *   local alias.
  * @param namespace the database/schema path within the catalog.
  * @param table the table name within `namespace`.
  */
case class CatalogRequirement(
  required: Boolean,
  technology: Option[String] = None,
  catalogName: Option[String] = None,
  location: Option[String] = None,
  namespace: List[String] = Nil,
  table: Option[String] = None
)

/** A dataset the contract reads from (input) or writes to (output).
  *
  * @param location physical location of the dataset (table name, path, topic, etc.)
  * @param format   optional storage/serialization format (e.g. "table", "parquet", "delta")
  * @param saveMode optional expected write behavior for an output dataset
  *   toward data already present at `location` (e.g. "append", "overwrite",
  *   "ignore", "error"). Meaningless for an input dataset; only checked by
  *   `StructuralVerifier` against a plan's `Write` node.
  * @param catalog optional data-catalog registration requirement — see
  *   `CatalogRequirement`'s own doc. Unlike `saveMode`, meaningful for
  *   both inputs and outputs.
  * @param description optional free-form human-readable explanation of
  *   what this dataset is/contains — purely documentary, never checked by
  *   `RuleVerifier`/`StructuralVerifier`. Exists mainly so an
  *   organizational policy can require one (`require_dataset_description`
  *   — see `docs/CONTRACT_MODEL.md`'s "Organizational Policy" section),
  *   the same catalog-discoverability motivation `require_catalog`
  *   already has, but for a human reader rather than a catalog consumer.
  */
case class Dataset(
  name: String,
  location: String,
  format: Option[String],
  schema: Schema,
  saveMode: Option[String] = None,
  catalog: Option[CatalogRequirement] = None,
  description: Option[String] = None
)

/** Rule types Invaract itself knows how to interpret during verification
  * (see `InterpretedRule`, and `RuleVerifier` in `spark-adapter`) —
  * deliberately a narrow, closed set (the concrete first step
  * ROADMAP.md's "Full semantic DML verification" item names, plus a
  * second family — see below), not a general rule-expression language. A
  * `ContractRule.ruleType` outside this set is not necessarily inert,
  * though: if `Contract.customRuleTypes` names a `CustomRuleVerifier`
  * implementation for it (`spark-adapter`), `RuleVerifier` dispatches to
  * that instead — the same plug-in-without-a-source-change escape hatch
  * `OrgPolicy.customPolicyTypes`/`CustomPolicyEvaluator` already give
  * organizational policy types (see docs/CONTRACT_MODEL.md's
  * "Organizational Policy" section and docs/SPARK_ADAPTER.md's "Custom
  * rule types" section) — that escape hatch currently covers only the
  * three row-level-DML types below, not the four plan-shape ones (see
  * their own doc). A `ruleType` matching neither this set nor
  * `customRuleTypes` is still recorded on `Contract.rules` but never
  * acted on.
  */
object RuleType {
  /** A MERGE must match on exactly these columns. */
  val MergeCondition = "merge_condition"

  /** A DELETE (standalone, or a MERGE's not-matched-delete clause) may
    * never be unconditional — it must carry a filtering predicate rather
    * than delete every row.
    */
  val ForbidUnconditionalDelete = "forbid_unconditional_delete"

  /** An UPDATE (standalone, or a MERGE's matched-update clause) may only
    * assign to these columns.
    */
  val AllowedUpdateColumns = "allowed_update_columns"

  /** The transformation must aggregate (`GROUP BY`) by at least these
    * columns somewhere in its plan — see `spark-adapter`'s
    * `PlanRuleVerifier`.
    */
  val RequiredGroupBy = "required_group_by"

  /** No join in the transformation's plan may be a cartesian product — a
    * `CROSS JOIN`, or any join with no condition at all, regardless of its
    * declared type. Takes no properties, the same shape as
    * `ForbidUnconditionalDelete`.
    */
  val ForbidCrossJoin = "forbid_cross_join"

  /** At least one join in the transformation's plan must be conditioned on
    * an equality match covering these columns.
    */
  val RequiredJoinColumns = "required_join_columns"

  /** At least one filter in the transformation's plan must reference each
    * of these columns — e.g. "the plan must filter out nulls/soft-deletes
    * on this column somewhere," without requiring a specific predicate
    * shape.
    */
  val RequiredFilterColumns = "required_filter_columns"

  /** The three row-level-DML rule types (`RuleVerifier`, checked against
    * one extracted `ir.RowMutation`).
    */
  val DmlTypes: Set[String] = Set(MergeCondition, ForbidUnconditionalDelete, AllowedUpdateColumns)

  /** The four plan-shape rule types (`PlanRuleVerifier`, checked against a
    * transformation's whole `ir.Plan`, independent of whether it's DML at
    * all).
    */
  val PlanShapeTypes: Set[String] = Set(RequiredGroupBy, ForbidCrossJoin, RequiredJoinColumns, RequiredFilterColumns)

  val All: Set[String] = DmlTypes ++ PlanShapeTypes
}

/** A `ContractRule`, decoded into one of the shapes Invaract currently
  * knows how to verify. Deliberately narrow, mirroring `RuleType`'s seven
  * members — not a general rule-expression language.
  */
sealed trait InterpretedRule
object InterpretedRule {
  case class MergeCondition(columns: List[String]) extends InterpretedRule
  case object ForbidUnconditionalDelete extends InterpretedRule
  case class AllowedUpdateColumns(columns: List[String]) extends InterpretedRule
  case class RequiredGroupBy(columns: List[String]) extends InterpretedRule
  case object ForbidCrossJoin extends InterpretedRule
  case class RequiredJoinColumns(columns: List[String]) extends InterpretedRule
  case class RequiredFilterColumns(columns: List[String]) extends InterpretedRule
}

/** A declarative rule attached to the contract (e.g. compatibility mode,
  * quality expectation, or one of `RuleType`'s DML constraints). Beyond
  * `RuleType`'s three interpreted members, Invaract does not interpret
  * rule semantics — it records them; verification of those against a
  * transformation plan is future work (see ROADMAP.md, Phase 1 scope).
  */
case class ContractRule(ruleType: String, properties: Map[String, Any]) {

  /** Decodes `properties` into one of `InterpretedRule`'s shapes when
    * `ruleType` is one Invaract currently interprets and its properties
    * are well-formed. `None` covers both "not a rule type Invaract
    * interprets" and "malformed properties for a type it does" — this
    * stays total/safe so a caller never needs to catch an exception just
    * to check whether a rule applies; `ContractValidator` is where a
    * malformed *known* rule type becomes a reported issue instead.
    */
  def interpret: Option[InterpretedRule] = ruleType match {
    case RuleType.MergeCondition =>
      // Deliberately "columns", not "on": SnakeYAML's default (YAML 1.1)
      // resolver treats the bare key `on` as the boolean `true` (the
      // "Norway problem" — on/off/yes/no all resolve to booleans), so
      // `properties.get("on")` would silently be None for every contract
      // authored with an unquoted `on:` key. Confirmed empirically via a
      // real failing test, not assumed.
      ContractRule.stringList(properties.get("columns")).map(InterpretedRule.MergeCondition)
    case RuleType.ForbidUnconditionalDelete =>
      Some(InterpretedRule.ForbidUnconditionalDelete)
    case RuleType.AllowedUpdateColumns =>
      ContractRule.stringList(properties.get("columns")).map(InterpretedRule.AllowedUpdateColumns)
    case RuleType.RequiredGroupBy =>
      ContractRule.stringList(properties.get("columns")).map(InterpretedRule.RequiredGroupBy)
    case RuleType.ForbidCrossJoin =>
      Some(InterpretedRule.ForbidCrossJoin)
    case RuleType.RequiredJoinColumns =>
      ContractRule.stringList(properties.get("columns")).map(InterpretedRule.RequiredJoinColumns)
    case RuleType.RequiredFilterColumns =>
      ContractRule.stringList(properties.get("columns")).map(InterpretedRule.RequiredFilterColumns)
    case _ => None
  }
}

// Explicitly extends AbstractFunction2 to preserve exactly the type
// hierarchy (and the tupled/curried static forwarders that come with it)
// the compiler would have synthesized automatically had this case class
// been left without a hand-written companion object — confirmed via a
// real MiMa failure, not assumed: adding this companion without the
// extends clause silently dropped both from the compiled class file,
// which `mimaReportBinaryIssues` caught against the 0.1.0 baseline.
object ContractRule extends scala.runtime.AbstractFunction2[String, Map[String, Any], ContractRule] {

  /** `properties` values come straight from SnakeYAML's parse (see
    * `ContractParser.parseRules`) — a YAML list surfaces as a
    * `java.util.List[_]`, not a Scala `List`. `Some` only for a present,
    * non-empty list whose every element is a `String`; anything else
    * (absent, empty, wrong element type, not a list at all) is malformed
    * and decodes to `None`.
    */
  private[contract] def stringList(value: Option[Any]): Option[List[String]] = value match {
    case Some(l: java.util.List[_]) =>
      val items = l.asScala.toList
      if (items.nonEmpty && items.forall(_.isInstanceOf[String])) Some(items.map(_.toString))
      else None
    case _ => None
  }
}

/** The root contract object: the minimum representation required to verify
  * a transformation, modeled after the Open Data Contract Standard (ODCS).
  *
  * Invaract intentionally does not redefine concepts ODCS already
  * standardizes (schema, fields, types). Fields not recognized by Invaract
  * are preserved verbatim in `extensions` rather than rejected, so contracts
  * authored for other ODCS-based tooling remain valid inputs.
  *
  * @param customRuleTypes maps a `ContractRule.ruleType` this contract's
  *   own `rules` use to the fully-qualified class name of a
  *   `com.invaract.sparkadapter.CustomRuleVerifier` implementation that
  *   verifies it — the plug-in-without-editing-Invaract's-own-source
  *   extension point for a DML rule type the built-in `RuleType` set
  *   doesn't cover. A `ruleType` also present in `RuleType.All` is inert
  *   here — the built-in interpretation always wins
  *   (`ContractValidator` warns on this). Resolved by
  *   `com.invaract.sparkadapter.CustomRuleVerifierFactory` — this module
  *   has no Spark dependency to resolve it itself, so
  *   `ContractValidator` can only check this map's own shape (empty
  *   keys/values, a built-in collision), not whether a named class
  *   actually resolves; `spark-adapter`'s `ContractEnforcementRule` does
  *   that eagerly, the same moment it already runs `ContractValidator`.
  *   See docs/SPARK_ADAPTER.md's "Custom rule types" section.
  */
case class Contract(
  id: String,
  version: ContractVersion,
  status: String,
  inputs: List[Dataset],
  outputs: List[Dataset],
  rules: List[ContractRule],
  extensions: Map[String, Any],
  customRuleTypes: Map[String, String] = Map.empty
) {
  def input(name: String): Option[Dataset] = inputs.find(_.name == name)
  def output(name: String): Option[Dataset] = outputs.find(_.name == name)
}
