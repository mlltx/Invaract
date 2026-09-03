// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, Dataset, Field}
import com.invaract.ir.{ColumnLineage, ColumnRef}

/** One output column's traced lineage (`ir.ColumnLineage`), enriched with
  * the union of every `sensitivityTags` label declared on any contract
  * input field this column transitively derives from.
  *
  * This is reporting, not enforcement: a non-empty `sensitivityTags` here
  * does not by itself fail verification — `StructuralVerifier` never reads
  * `Field.sensitivityTags` at all. It surfaces, for a human or downstream
  * tooling auditing a transformation, which output columns carry data a
  * contract author flagged as sensitive on the input side, even through
  * several transformation steps that say nothing about sensitivity
  * themselves (a `CASE WHEN` derived from a tagged column carries the same
  * tags forward, exactly like a plain rename would).
  */
case class SensitiveColumnLineage(lineage: ColumnLineage, sensitivityTags: Set[String])

/** Cross-references `ir.Lineage`'s traced column provenance against a
  * `Contract`'s declared input `Field.sensitivityTags`, propagating each
  * tagged input field's labels forward to every output column whose
  * `ColumnLineage.sources` resolves to it — directly, or transitively
  * through however many `Cast`/`Arithmetic`/`Conditional`/... steps sit
  * between them, since `Lineage.trace` has already resolved that chain
  * down to the real `Read` columns before this ever runs.
  *
  * Lives in `spark-adapter`, not `ir`: propagation needs both a `Contract`
  * (from the `contract` module) and traced lineage (from `ir`) together,
  * and `ir` deliberately has no dependency on `contract` (see `ir`'s own
  * module doc — an engine-independent transformation IR shouldn't know
  * what a data contract is). `spark-adapter`, which already depends on
  * both to do `StructuralVerifier`'s contract-vs-plan checking, is where a
  * type that needs both concepts belongs.
  */
object SensitivityLineage {

  /** For each entry in `lineage`, the union of `sensitivityTags` declared
    * on every one of `contract`'s input fields that entry's `sources`
    * resolves to — empty when no source is tagged (the common case,
    * for a contract that declares no sensitive fields at all). Order-
    * preserving and 1:1 with `lineage`.
    */
  def propagate(lineage: List[ColumnLineage], contract: Contract): List[SensitiveColumnLineage] = {
    val taggedFields = taggedInputFields(contract)
    lineage.map(cl => SensitiveColumnLineage(cl, cl.sources.flatMap(tagsFor(_, taggedFields))))
  }

  /** Every (dataset, field) pair among `contract.inputs` whose field
    * declares at least one sensitivity tag — the only fields this
    * propagation ever needs to look up, computed once per call rather than
    * re-scanning every input dataset per source column.
    */
  private def taggedInputFields(contract: Contract): List[(Dataset, Field)] =
    contract.inputs.flatMap(dataset => dataset.schema.fields.filter(_.sensitivityTags.nonEmpty).map(dataset -> _))

  /** `source`'s tags, if `source` resolves — by name, and by
    * location-normalized qualifier (the same matching
    * `StructuralVerifier.locationsMatch` uses for a contract's declared,
    * portable location against Spark's actual reported one; see that
    * method's own doc) — to a tagged input field. A source with no
    * qualifier, or one that doesn't match any tagged input's location at
    * all (e.g. a self-join alias, or a plain untagged column), simply
    * contributes no tags rather than guessing.
    */
  private def tagsFor(source: ColumnRef, taggedFields: List[(Dataset, Field)]): Set[String] =
    source.qualifier match {
      case None => Set.empty
      case Some(qualifier) =>
        taggedFields.collect {
          case (dataset, field) if field.name == source.name && StructuralVerifier.locationsMatch(dataset.location, qualifier) =>
            field.sensitivityTags
        }.flatten.toSet
    }
}
