// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** Why `ContractDraftMerger.merge` refused to produce a contract. Every one is
  * a fact about the drafts, reported instead of resolved by guessing.
  */
sealed trait MergeConflict {

  /** The (normalized) dataset location the conflict is about, when there is one. */
  def location: Option[String]

  def message: String
}

object MergeConflict {

  /** The same input location was read with different schemas in different drafts. */
  final case class InputSchemaConflict(loc: String, schemas: List[Schema]) extends MergeConflict {
    def location: Option[String] = Some(loc)
    def message: String =
      s"input '$loc' has ${schemas.size} different schemas across the drafts " +
        s"(${schemas.map(_.fields.map(f => s"${f.name}:${f.fieldType}").mkString("[", ", ", "]")).mkString(" vs ")}); " +
        "reconcile the drafts (re-run against the same data, or fix the stale one) and merge again"
  }

  /** The same output location was written with different schemas in different drafts. */
  final case class OutputSchemaConflict(loc: String, schemas: List[Schema]) extends MergeConflict {
    def location: Option[String] = Some(loc)
    def message: String =
      s"output '$loc' has ${schemas.size} different schemas across the drafts " +
        s"(${schemas.map(_.fields.map(f => s"${f.name}:${f.fieldType}").mkString("[", ", ", "]")).mkString(" vs ")}); " +
        "two writes to one location must agree on what they produce"
  }

  /** A draft that cannot be merged as written (a `derivedFrom` naming an input it doesn't
    * declare, two inputs sharing a name, ...). `draftIndex` is 0-based, in the order given.
    */
  final case class MalformedDraft(draftIndex: Int, message: String) extends MergeConflict {
    def location: Option[String] = None
  }

  /** `merge` was called with no drafts at all. */
  case object NoDrafts extends MergeConflict {
    def location: Option[String] = None
    def message: String = "there are no drafts to merge"
  }

  /** The merged contract failed `ContractValidator` (e.g. every draft was output-less). */
  final case class InvalidResult(message: String) extends MergeConflict {
    def location: Option[String] = None
  }
}

/** Merges the single-write contract drafts a dry-run produces (one per write, with positional
  * input names `input`, `input_1`, ... and `derivedFrom` naming what that write read) into ONE
  * multi-output contract - the step a job with several writes otherwise makes by hand.
  *
  * Pure and deterministic: no I/O, no Spark, output order follows first appearance across
  * `drafts` (draft by draft, input by input). Nothing is guessed:
  *
  *  - **Inputs** are one per distinct normalized location (`file:` stripped, `\` as `/`, no
  *    trailing `/`). The same location with different schemas in different drafts is an
  *    `InputSchemaConflict`, never silently resolved to one of them.
  *  - **Names** are derived from the location (its last path segment without extension, or the
  *    last dotted part of a catalog identifier), lower-cased with anything outside `[a-z0-9]`
  *    turned into `_`; a name already taken gets `_2`, `_3`, ... in order of appearance. They
  *    do not depend on which write happened to read a dataset first, or on draft order beyond
  *    that tie-break.
  *  - **Outputs** are one per distinct normalized location, named the same way. Two drafts
  *    writing one location are one output if they agree on its schema (`OutputSchemaConflict`
  *    otherwise); its `derivedFrom` is the union of what the writes read, and `None` ("all
  *    inputs") if any of them said so.
  *  - Every output's `derivedFrom` is rewritten from its draft's own input names to the merged
  *    names.
  *
  * Also merged: input `description`s (distinct ones, first-appearance order, space-separated),
  * `rules` (distinct), and `extensions`/`customRuleTypes` (the earliest draft's value wins on a
  * key). The result is checked with `ContractValidator`; an invalid one is returned as an
  * `InvalidResult` rather than as a contract.
  */
object ContractDraftMerger {
  import MergeConflict._

  def merge(drafts: List[Contract], id: String, version: ContractVersion): Either[List[MergeConflict], Contract] =
    if (drafts.isEmpty) Left(List(NoDrafts))
    else {
      val malformed = drafts.zipWithIndex.flatMap { case (draft, index) => malformations(draft).map(MalformedDraft(index, _)) }
      if (malformed.nonEmpty) Left(malformed)
      else mergeWellFormed(drafts, id, version)
    }

  private def mergeWellFormed(drafts: List[Contract], id: String, version: ContractVersion): Either[List[MergeConflict], Contract] = {
    val inputGroups = groupByLocation(drafts.flatMap(_.inputs))
    val outputGroups = groupByLocation(drafts.flatMap(_.outputs))

    val conflicts =
      inputGroups.collect { case (loc, ds) if distinctSchemas(ds).size > 1 => InputSchemaConflict(loc, distinctSchemas(ds)) } ++
        outputGroups.collect { case (loc, ds) if distinctSchemas(ds).size > 1 => OutputSchemaConflict(loc, distinctSchemas(ds)) }
    if (conflicts.nonEmpty) Left(conflicts)
    else {
      val inputNames = uniqueNames(inputGroups.map(_._1))
      val outputNames = uniqueNames(outputGroups.map(_._1))
      val mergedInputs = inputGroups.map { case (loc, ds) =>
        ds.head.copy(
          name = inputNames(loc),
          format = ds.flatMap(_.format).headOption,
          description = Some(ds.flatMap(_.description).distinct.mkString(" ")).filter(_.nonEmpty)
        )
      }
      // Each draft's own input name -> the merged name of the location it stands for.
      val renamed: List[Map[String, String]] =
        drafts.map(draft => draft.inputs.map(input => input.name -> inputNames(normalize(input.location))).toMap)
      val mergedOutputs = outputGroups.map { case (loc, _) =>
        val members = for {
          (draft, names) <- drafts.zip(renamed)
          output         <- draft.outputs if normalize(output.location) == loc
        } yield output -> names
        val derived = members.map { case (o, names) => o.derivedFrom.map(_.map(names)) }
        members.head._1.copy(
          name = outputNames(loc),
          format = members.flatMap(_._1.format).headOption,
          saveMode = members.flatMap(_._1.saveMode).headOption,
          derivedFrom =
            if (derived.contains(None)) None
            else {
              val used = derived.flatten.flatten.toSet
              Some(mergedInputs.map(_.name).filter(used)) // merged-input order, no duplicates
            }
        )
      }
      val merged = Contract(
        id = id,
        version = version,
        status = drafts.head.status,
        inputs = mergedInputs,
        outputs = mergedOutputs,
        rules = drafts.flatMap(_.rules).distinct,
        extensions = drafts.reverse.foldLeft(Map.empty[String, Any])(_ ++ _.extensions),
        customRuleTypes = drafts.reverse.foldLeft(Map.empty[String, String])(_ ++ _.customRuleTypes)
      )
      val errors = ContractValidator.validate(merged).errors
      if (errors.isEmpty) Right(merged) else Left(errors.map(e => InvalidResult(s"${e.path}: ${e.message}")))
    }
  }

  private def malformations(draft: Contract): List[String] = {
    val inputNames = draft.inputs.map(_.name)
    inputNames.diff(inputNames.distinct).distinct.map(n => s"declares two inputs named '$n'") ++
      draft.outputs.flatMap { output =>
        output.derivedFrom.getOrElse(Nil).distinct.filterNot(inputNames.contains).map(n =>
          s"output '${output.name}' is derivedFrom '$n', which the draft does not declare as an input"
        )
      }
  }

  /** `datasets` grouped by normalized location, groups (and members) in first-appearance order. */
  private def groupByLocation(datasets: List[Dataset]): List[(String, List[Dataset])] = {
    val locations = datasets.map(d => normalize(d.location)).distinct
    locations.map(loc => loc -> datasets.filter(d => normalize(d.location) == loc))
  }

  private def distinctSchemas(datasets: List[Dataset]): List[Schema] = datasets.map(_.schema).distinct

  private def normalize(location: String): String = {
    val slashed = location.trim.replace('\\', '/').stripPrefix("file:")
    if (slashed.length > 1) slashed.stripSuffix("/") else slashed
  }

  /** A name per location: derived from it, with `_2`, `_3`, ... on a clash, in the order given. */
  private def uniqueNames(locations: List[String]): Map[String, String] =
    locations.foldLeft((Map.empty[String, String], Set.empty[String])) { case ((assigned, taken), loc) =>
      val base = baseName(loc)
      val name = Iterator.from(1).map(n => if (n == 1) base else s"${base}_$n").find(!taken(_)).get
      (assigned + (loc -> name), taken + name)
    }._1

  private def baseName(location: String): String = {
    val segment =
      if (location.contains('/')) location.split('/').filter(_.nonEmpty).lastOption.getOrElse("").replaceAll("""\.[A-Za-z0-9]+$""", "")
      else location.split('.').filter(_.nonEmpty).lastOption.getOrElse("") // a catalog identifier: catalog.schema.table
    val sanitized = segment.toLowerCase.replaceAll("[^a-z0-9]+", "_").stripPrefix("_").stripSuffix("_")
    if (sanitized.isEmpty) "dataset" else sanitized
  }
}
