// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.ir

sealed trait JoinType
object JoinType {
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
  case object LeftSemi extends JoinType
  case object LeftAnti extends JoinType
  case object Cross extends JoinType
}

/** A relational operator: something that consumes zero or more datasets and
  * produces one dataset. This is the transformation IR's spine — every node
  * here corresponds to a step in the pipeline the way a person would
  * describe it ("read", "filter", "join", "aggregate"), not to an engine's
  * internal execution or optimization representation.
  */
sealed trait Plan {
  def children: List[Plan]
}

/** The source of a transformation: a dataset read in its entirety.
  * `Read` declares no schema — the IR is not a full type system, and a
  * Read's columns come into existence the moment something downstream
  * references them (see `Lineage`). `alias` lets the same dataset be read
  * twice in one plan (a self-join) with each occurrence individually
  * addressable via `ColumnRef.qualifier`.
  */
case class Read(dataset: DatasetRef, alias: Option[String] = None) extends Plan {
  def children: List[Plan] = Nil
}

/** The sink of a transformation: everything upstream of this node exists to
  * produce `dataset`. Always the root of a complete pipeline.
  *
  * @param format the serialization format actually used ("parquet", "csv",
  *   "json", ...), when the adapter that produced this node could
  *   determine one. `None` doesn't mean "no format" — it means the
  *   translator couldn't identify it (e.g. a write path this IR doesn't
  *   yet model precisely). A contract's declared format can only be
  *   verified against this when it's populated.
  * @param saveMode how this write behaves toward data already at `dataset`
  *   ("append", "overwrite", "ignore", "error"), normalized from the
  *   engine's own mode enum, when determinable — same "`None` means
  *   unknown, not unset" convention as `format`.
  */
case class Write(dataset: DatasetRef, input: Plan, format: Option[String] = None, saveMode: Option[String] = None) extends Plan {
  def children: List[Plan] = List(input)
}

/** Narrows and/or computes the output column set. Unlike SQL's `SELECT *`,
  * `columns` is always the complete output schema — there is no implicit
  * passthrough of unmentioned input columns, which keeps lineage tracing
  * (and contract verification of the output schema) unambiguous. A
  * front-end translating `SELECT *` from a real engine's plan is expected
  * to expand it to explicit columns before constructing this node.
  */
case class Project(input: Plan, columns: List[NamedExpr]) extends Plan {
  def children: List[Plan] = List(input)
}

/** Restricts rows without changing the column set or their meaning. */
case class Filter(input: Plan, condition: Expr) extends Plan {
  def children: List[Plan] = List(input)
}

/** Combines two datasets row-wise. Both sides' columns appear in the
  * output; `condition` is not itself part of the output (see `Lineage`).
  */
case class Join(left: Plan, right: Plan, joinType: JoinType, condition: Option[Expr] = None) extends Plan {
  def children: List[Plan] = List(left, right)
}

/** Groups rows by `groupBy` and collapses each group to one row via
  * `aggregates`. As with `Project.columns`, `aggregates` is always the
  * complete output schema: `groupBy` only defines the partitioning keys and
  * contributes no output columns of its own, exactly as a SQL `GROUP BY`
  * clause doesn't appear in the result unless it's also named in the
  * `SELECT` list. A grouping key that should appear in the output needs its
  * own `NamedExpr` entry in `aggregates` (typically a plain
  * `ColumnReference` to the same column named in `groupBy`).
  */
case class Aggregate(input: Plan, groupBy: List[Expr], aggregates: List[NamedExpr]) extends Plan {
  def children: List[Plan] = List(input)
}

/** Concatenates rows from multiple plans with the same shape. Output column
  * names follow the first branch, matching standard SQL `UNION` semantics.
  */
case class Union(inputs: List[Plan]) extends Plan {
  def children: List[Plan] = inputs
}

/** Orders rows without changing the column set. */
case class Sort(input: Plan, order: List[SortOrder]) extends Plan {
  def children: List[Plan] = List(input)
}

/** Adds columns computed over a window of rows related to each row (by
  * partition and/or order) without collapsing row count the way
  * `Aggregate` does. `windowExprs` entries may wrap an `AggregateCall` (a
  * running/partitioned aggregate) or a plain `FunctionCall` (RANK,
  * ROW_NUMBER, LAG, ...) — the same expression vocabulary as everywhere
  * else in the IR; windowing is a property of this plan node, not a new
  * expression type. Existing input columns pass through unchanged
  * alongside the new windowed columns.
  */
case class Window(
  input: Plan,
  windowExprs: List[NamedExpr],
  partitionBy: List[Expr] = Nil,
  orderBy: List[SortOrder] = Nil
) extends Plan {
  def children: List[Plan] = List(input)
}

/** A plan node a front-end translator could not represent in this IR's
  * vocabulary — an opaque placeholder, not a failure. Any real-world
  * front-end (this one included: see the `spark-adapter` module) will
  * eventually meet a construct with no clean equivalent here (an exotic
  * operator, a vendor extension). Rather than every translator inventing
  * its own ad hoc "give up" representation, `Unsupported` is a first-class,
  * engine-agnostic IR concept: the rest of the tree stays inspectable,
  * `Lineage` degrades to "no known source" for anything that would need to
  * resolve through it, and a translator is expected to pair this node with
  * a diagnostic explaining what it couldn't represent and why.
  */
case class Unsupported(description: String, children: List[Plan] = Nil) extends Plan
