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
  sensitivityTags: Set[String] = Set.empty
) {
  def isStruct: Boolean = properties.nonEmpty
}

/** An ordered collection of fields describing the shape of a dataset. */
case class Schema(fields: List[Field]) {
  def field(name: String): Option[Field] = fields.find(_.name == name)
}

/** A dataset the contract reads from (input) or writes to (output).
  *
  * @param location physical location of the dataset (table name, path, topic, etc.)
  * @param format   optional storage/serialization format (e.g. "table", "parquet", "delta")
  * @param saveMode optional expected write behavior for an output dataset
  *   toward data already present at `location` (e.g. "append", "overwrite",
  *   "ignore", "error"). Meaningless for an input dataset; only checked by
  *   `StructuralVerifier` against a plan's `Write` node.
  */
case class Dataset(
  name: String,
  location: String,
  format: Option[String],
  schema: Schema,
  saveMode: Option[String] = None
)

/** Rule types Invaract currently interprets during verification (see
  * `InterpretedRule`, and `RuleVerifier` in `spark-adapter`). Any other
  * `ContractRule.ruleType` is still recorded on `Contract.rules` but not
  * acted on — this is deliberately a narrow, closed set (the concrete
  * first step ROADMAP.md's "Full semantic DML verification" item names),
  * not a general rule-expression language.
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

  val All: Set[String] = Set(MergeCondition, ForbidUnconditionalDelete, AllowedUpdateColumns)
}

/** A `ContractRule`, decoded into one of the shapes Invaract currently
  * knows how to verify. Deliberately narrow, mirroring `RuleType`'s three
  * members — not a general rule-expression language.
  */
sealed trait InterpretedRule
object InterpretedRule {
  case class MergeCondition(columns: List[String]) extends InterpretedRule
  case object ForbidUnconditionalDelete extends InterpretedRule
  case class AllowedUpdateColumns(columns: List[String]) extends InterpretedRule
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
  */
case class Contract(
  id: String,
  version: ContractVersion,
  status: String,
  inputs: List[Dataset],
  outputs: List[Dataset],
  rules: List[ContractRule],
  extensions: Map[String, Any]
) {
  def input(name: String): Option[Dataset] = inputs.find(_.name == name)
  def output(name: String): Option[Dataset] = outputs.find(_.name == name)
}
