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
  * the source) or `INDIRECT` (the source decides which rows there are, in what order, or which value a column takes);
  * `subtype` names how (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`; `FILTER`, `JOIN`, `GROUP_BY`, `SORT`, `WINDOW`,
  * `CONDITIONAL`). `masking` is set when the source reaches the column only through a hash or a mask. */
final case class LineageTransformation(kind: String, subtype: String, description: Option[String] = None, masking: Boolean = false)

/** One output column's sources (with `field` that column: its `DIRECT` sources, and the `INDIRECT` ones deciding its
  * value), or (with `field` empty) the sources that shape the whole output. */
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
  lazy val columnLineage: List[ColumnLineageEdge] = plan.fold(List.empty[ColumnLineageEdge])(WriteLineage.trace(_, inputs, output.schema))
}

object WriteLineage {

  /** The edges of `plan`'s column lineage, each source resolved to the read it stands for. `inputs` are the write's reads
    * as the adapter reported them: a read whose columns are known (`schema`) gives a column only a name it has, so a
    * name is not taken from a read that lacks it. A source whose read cannot be told (a scope shared by reads of
    * different locations) is left out: an edge to the wrong dataset would be worse than none. So is a column the write
    * does not have (`written`, the output's columns, when known): a MERGE's plan is its source query, whose columns
    * are not all the table's.
    */
  private[invaract] def trace(plan: ir.Plan, inputs: List[LineageDataset] = Nil, written: Option[LogicalSchema] = None): List[ColumnLineageEdge] = {
    val facts = PlanFacts.of(plan)
    val readLocations = facts.reads.map(_.dataset.location).distinct
    val columnsByLocation = inputs.flatMap(d => d.schema.map(s => d.location -> s.fields.map(_.name).toSet)).toMap
    // ir.Lineage names every source by the scope of the read it comes from (its alias or location)
    def locate(ref: ir.ColumnRef): Option[String] = ref.qualifier.map(facts.locationResolver).filter(readLocations.contains)
    def edge(field: Option[String], source: ir.ColumnRef, transformation: LineageTransformation) =
      locate(source).map(location => ColumnLineageEdge(field, LineageSource(location, source.name), transformation))
    val lineage = ir.Lineage.traceDetailed(plan, read => columnsByLocation.get(read.dataset.location))
    val writtenNames = written.map(_.fields.map(_.name.toLowerCase).toSet)
    val columns = lineage.columns.filter(c => writtenNames.forall(_.contains(c.lineage.output.name.toLowerCase))).flatMap { column =>
      val c = column.lineage
      val name = c.output.name
      val (kind, subtype, description) = c.derivation match {
        case ir.DerivationKind.Direct => ("DIRECT", "IDENTITY", None)
        case _ if c.aggregated        => ("DIRECT", "AGGREGATION", Some(c.aggregations.toList.map(_.function).sorted.mkString(", ")))
        case ir.DerivationKind.Opaque => ("DIRECT", "TRANSFORMATION", Some("opaque: a function Invaract cannot see into"))
        case _                        => ("DIRECT", "TRANSFORMATION", None)
      }
      // each source as it reaches the column: copied unchanged (IDENTITY), computed (the column's kind), or both - a
      // MERGE's UPDATE copies a column its INSERT computes
      val values = column.values.toList.sortBy(_.toString).flatMap { source =>
        val copied = c.derivation == ir.DerivationKind.Direct || column.copied(source)
        val computed = c.derivation != ir.DerivationKind.Direct && (column.transformed(source) || !column.copied(source))
        val ways = (if (copied) List(LineageTransformation("DIRECT", "IDENTITY", None, masking = column.masked(source))) else Nil) ++
          (if (computed) List(LineageTransformation(kind, subtype, description, masking = column.masked(source))) else Nil)
        ways.flatMap(edge(Some(name), source, _))
      }
      val shaping = column.indirect.toList.sortBy(i => (subtypeOf(i.kind), i.source.toString)).flatMap { i =>
        edge(Some(name), i.source, LineageTransformation("INDIRECT", subtypeOf(i.kind)))
      }
      values ++ shaping
    }
    val whole = lineage.indirect.flatMap(i => edge(None, i.source, LineageTransformation("INDIRECT", subtypeOf(i.kind))))
    (columns ++ whole).distinct
  }

  private def subtypeOf(kind: ir.IndirectKind): String = kind match {
    case ir.IndirectKind.Filter      => "FILTER"
    case ir.IndirectKind.Join        => "JOIN"
    case ir.IndirectKind.GroupBy     => "GROUP_BY"
    case ir.IndirectKind.Sort        => "SORT"
    case ir.IndirectKind.Window      => "WINDOW"
    case ir.IndirectKind.Conditional => "CONDITIONAL"
  }
}
