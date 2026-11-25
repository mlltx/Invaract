// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import com.invaract.contract.LogicalSchema
import com.invaract.ir
import com.invaract.verification.PlanFacts

/** One dataset a checked write read or wrote, as the adapter reported it: where it is, in what format, with which columns,
  * and the catalog table it resolved to. `declaredName` is the dataset's name in the contract, when the contract declares
  * it. `None` for a fact the adapter did not report (a schema an engine does not expose, say), never a guess. */
final case class LineageDataset(
    location: String,
    format: Option[String] = None,
    schema: Option[LogicalSchema] = None,
    catalog: Option[CatalogInfo] = None,
    declaredName: Option[String] = None
)

/** One input column an output column (or the whole output) depends on: `location` is the read it comes from. */
final case class LineageSource(location: String, field: String)

/** How an output column is derived from its sources, in OpenLineage's terms: `kind` is `DIRECT` (the value is computed from
  * the source) or `INDIRECT` (the source decides which rows there are: a filter, a join, a grouping); `subtype` names how
  * (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`; `FILTER`, `JOIN`, `GROUP_BY`). `masking` is not known to Invaract and
  * left out. */
final case class LineageTransformation(kind: String, subtype: String, description: Option[String] = None)

/** One output column's sources, or (with `field` empty) the sources that shape the whole output. */
final case class ColumnLineageEdge(field: Option[String], source: LineageSource, transformation: LineageTransformation)

/** What a checked write read and wrote, carried on its `ContractValidationEvent` for a sink that publishes lineage
  * (OpenLineage). It holds references to what the verification pipeline already built - the plan, the schemas - so
  * carrying it costs the job nothing; column lineage is traced from the plan only when a sink asks (`columnLineage`),
  * on that sink's own thread. Not part of Invaract's own event JSON, whose published schema it does not change.
  *
  * `plan` is the write's translated plan: column lineage exists only where the plan says how each column is computed,
  * so it is empty for a plan Invaract cannot see into (a Python job's own code, a Beam step's function), never guessed.
  */
final case class WriteLineage(
    output: LineageDataset,
    inputs: List[LineageDataset],
    saveMode: Option[String] = None,
    partitionColumns: List[String] = Nil,
    plan: Option[ir.Plan] = None
) {

  /** Each output column's sources, then the sources that shape the whole output (filters, joins, grouping keys). */
  lazy val columnLineage: List[ColumnLineageEdge] = plan.fold(List.empty[ColumnLineageEdge])(WriteLineage.trace)
}

object WriteLineage {

  /** The edges of `plan`'s column lineage, each source resolved to the read it stands for. A source whose read cannot be
    * told (an unqualified name over several reads, a scope shared by reads of different locations) is left out: an edge to
    * the wrong dataset would be worse than none. */
  private[invaract] def trace(plan: ir.Plan): List[ColumnLineageEdge] = {
    val facts = PlanFacts.of(plan)
    val readLocations = facts.reads.map(_.dataset.location).distinct
    def locate(ref: ir.ColumnRef): Option[String] = ref.qualifier match {
      case Some(qualifier) =>
        val location = facts.locationResolver(qualifier)
        if (readLocations.contains(location)) Some(location) else None
      case None => if (readLocations.size == 1) readLocations.headOption else None
    }
    val direct = ir.Lineage.trace(plan).flatMap { column =>
      val transformation = column.derivation match {
        case ir.DerivationKind.Direct => LineageTransformation("DIRECT", "IDENTITY")
        case _ if column.aggregated =>
          LineageTransformation("DIRECT", "AGGREGATION", Some(column.aggregations.toList.map(_.function).sorted.mkString(", ")))
        case ir.DerivationKind.Opaque => LineageTransformation("DIRECT", "TRANSFORMATION", Some("opaque: a function Invaract cannot see into"))
        case _ => LineageTransformation("DIRECT", "TRANSFORMATION")
      }
      column.sources.toList.sortBy(_.toString).flatMap { source =>
        locate(source).map(location => ColumnLineageEdge(Some(column.output.name), LineageSource(location, source.name), transformation))
      }
    }
    def indirect(refs: Iterable[ir.ColumnRef], subtype: String) =
      refs.toList.sortBy(_.toString).flatMap(ref => locate(ref).map(location => ColumnLineageEdge(None, LineageSource(location, ref.name), LineageTransformation("INDIRECT", subtype))))
    val filtered = indirect(facts.filters.flatMap(_.condition.references), "FILTER")
    val joined = indirect(facts.joins.flatMap(_.condition.toList.flatMap(_.references)), "JOIN")
    val grouped = indirect(facts.aggregates.flatMap(_.groupBy.flatMap(_.references)), "GROUP_BY")
    (direct ++ filtered ++ joined ++ grouped).distinct
  }
}
