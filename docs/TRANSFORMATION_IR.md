# Transformation Intermediate Representation

This document describes the Invariant transformation IR delivered in Phase
2: an engine-independent representation of a data transformation, precise
enough to establish column-level lineage and, eventually, verify a
transformation against a [contract](CONTRACT_MODEL.md).

Code lives in the `ir/` sbt module (`com.example.ir` package). It has no
dependency on Spark, or on the `contract` module — it is pure Scala data
structures and algorithms over them. That independence is the point: this
is meant to be the thing a Spark logical plan, a SQL AST, or a dbt model
graph all get *translated into*, so that lineage tracing and contract
verification are written once, against this IR, rather than once per
engine.

## Critical principle: semantics, not syntax

The instruction that shaped every design decision in this module: **do not
build an IR that mirrors Spark's Catalyst classes.** Catalyst has dozens of
`Expression` subclasses (`Add`, `Subtract`, `EqualTo`, `GreaterThan`, `And`,
`Or`, `Cast`, ...), an `Alias` that's itself an expression wrapping any
other expression, and attribute resolution via globally unique `exprId`s.
None of that is *transformation semantics* — it's implementation machinery
for one specific execution engine's optimizer.

Three concrete decisions follow from that:

1. **One `FunctionCall` node, not dozens of operator classes.** An
   arithmetic operator, a comparison, a boolean combinator, and a cast are
   all "apply a named function to some arguments" as far as lineage and
   contract verification are concerned — none of them change *which source
   columns* an output value depends on. Collapsing them to
   `FunctionCall(name, args)` is a direct application of the principle: the
   IR only distinguishes node kinds when the distinction actually changes
   what can be proven about the transformation. The one function-like
   exception is `AggregateCall`, because aggregation *does* change something
   semantically load-bearing — cardinality (many input rows collapse into
   one output value) — which lineage tracing must be able to detect.

2. **No `Alias` expression node.** In Catalyst, `Alias` is itself an
   `Expression`, so naming can appear nested anywhere in an expression tree.
   In this IR, naming an output column isn't a computation on values — it's
   metadata about how a plan stage exposes a value downstream. So it lives
   at the plan boundary instead: `NamedExpr(name, expr)` is how `Project`,
   `Aggregate`, and `Window` declare their output columns, and it is not
   itself an `Expr` — it cannot be nested inside a `FunctionCall` the way a
   Catalyst `Alias` could be nested inside anything.

3. **No exprId-based resolution.** Catalyst binds every attribute reference
   to its producer via a globally unique ID, resolved by an analysis pass.
   This IR has no analysis pass and no IDs: a `ColumnRef` is just a name
   plus an optional qualifier (`customer_id` from `raw.orders`), and
   `Lineage` resolves bare names by walking the plan structurally (see
   below). This is a deliberate simplification — it can't disambiguate
   every case a full resolver could (see *Known limitations*) — but it
   keeps the IR itself free of engine-internal bookkeeping.

## The IR

Two small algebras, following the classic logical-plan / expression split
(the one part of the design that *is* shared with virtually every SQL
engine, because it reflects relational algebra itself, not Spark
specifically):

### Expressions (`Expr.scala`) — compute a value

| Node | Represents |
|---|---|
| `ColumnReference(ref: ColumnRef)` | Reads one column. |
| `Literal(value, literalType)` | A constant. `literalType` uses the same logical-type vocabulary as a contract field's `type`. |
| `FunctionCall(name, args)` | Any scalar function: arithmetic, comparison, boolean logic, casts, string/date functions. |
| `AggregateCall(function, arg, distinct)` | An aggregate function (SUM, COUNT, AVG, MIN, MAX, ...). The one expression that changes cardinality. |
| `NamedExpr(name, expr)` | Binds a name to a computed expression — how a plan stage declares an output column. Not an `Expr`. |
| `SortOrder(expr, ascending, nullsFirst)` | One key of a `Sort` or `Window` ordering. |

`AggregateCall` is reused unchanged whether it sits under an `Aggregate`
plan node (grouped aggregation) or a `Window` plan node (running/partitioned
aggregation) — windowing is a property of *where* the expression sits in
the plan, not a different kind of expression.

### Plans (`Plan.scala`) — transform datasets

| Node | Represents |
|---|---|
| `Read(dataset, alias)` | Source: reads a dataset in its entirety. No declared schema — columns come into existence when referenced downstream. `alias` supports self-joins. |
| `Write(dataset, input, format, saveMode)` | Sink: always the root of a complete pipeline. `format` ("parquet", "csv", ...) and `saveMode` ("append", "overwrite", "ignore", "error") are populated when the adapter that produced this node could determine them; `None` otherwise, not "no format"/"no save mode." |
| `Project(input, columns)` | Narrows/computes the output column set. `columns` is always the *complete* output schema — no implicit `SELECT *` passthrough. |
| `Filter(input, condition)` | Restricts rows; column set unchanged. |
| `Join(left, right, joinType, condition)` | Combines two datasets row-wise. Both sides' columns appear in the output. |
| `Aggregate(input, groupBy, aggregates)` | Groups by `groupBy`, collapses each group via `aggregates`. `aggregates` is the complete output schema — `groupBy` contributes no output columns of its own (see below). |
| `Union(inputs)` | Concatenates rows from multiple same-shaped plans. Output names follow the first branch. |
| `Sort(input, order)` | Orders rows; column set unchanged. |
| `Window(input, windowExprs, partitionBy, orderBy)` | Adds columns computed over a window of related rows, without collapsing row count. Input columns pass through unchanged. |

Every `Plan` exposes `children: List[Plan]`, the one generic traversal
mechanism used by both `Lineage` and `PlanPrinter` — neither needs its own
ad hoc per-node-type walk of the tree structure (only of the type-specific
*content*, which genuinely differs per operator).

`JoinType` is `Inner | LeftOuter | RightOuter | FullOuter | LeftSemi |
LeftAnti | Cross`.

### A note on `Aggregate.groupBy`

This one is worth calling out because it wasn't obvious on the first pass.
An early version of `Lineage` treated a plain-column `groupBy` key as
automatically producing an output column with that name — mirroring the
intuition that "you grouped by it, so it's in the result." That's wrong,
and the test suite caught it: writing the natural IR for `SELECT
customer_id, COUNT(*) FROM orders GROUP BY customer_id` produced two
`customer_id` output entries, because the grouping key and the identical
`SELECT`-list entry were both treated as independent output declarations.

The fix follows the same rule already applied to `Project`: `aggregates` is
the *complete* output schema, full stop. `groupBy` is pure partitioning
metadata. A grouping key that should appear in the output needs its own
`NamedExpr` in `aggregates` — exactly as SQL requires the grouping column to
also appear in the `SELECT` list.

## Worked example

The example from this phase's spec:

```scala
val orders = Read(DatasetRef("raw.orders"))
val plan = Write(
  DatasetRef("gold.customer_orders"),
  Project(
    orders,
    List(
      NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
      NamedExpr("lifetime_value", AggregateCall("SUM", ColumnReference(ColumnRef("amount", Some("raw.orders")))))
    )
  )
)
```

`PlanPrinter.render(plan)` — actual output, run via `sbt Test/runMain`:

```
Write(gold.customer_orders)
└─ Project
   ├─ Read(raw.orders)
   ├─ customer_id = raw.orders.customer_id
   └─ lifetime_value = SUM(raw.orders.amount)
```

This differs slightly from the spec's illustrative diagram, which shows
`Read(raw.orders.customer_id)` as a separate leaf under each output column.
That's not how the actual object graph is shaped here: `Read` is a single
shared node (`Project`'s `input`), and per-column provenance is carried by
the *qualified column name* in each `NamedExpr`'s rendered expression
(`raw.orders.customer_id`) rather than by duplicating the Read node once per
column. The same information is present either way — which Read column each
output derives from — just represented once structurally instead of
inlined per branch.

`Lineage.trace(plan)` — actual output:

```
ColumnLineage(customer_id, Set(raw.orders.customer_id), aggregated = false)
ColumnLineage(lifetime_value, Set(raw.orders.amount), aggregated = true)
```

This is the semantic content the spec's diagram is asking for: which source
column each output column traces to, and whether that trace passes through
an aggregation.

## Lineage tracing (`Lineage.scala`)

```scala
case class ColumnLineage(output: ColumnRef, sources: Set[ColumnRef], aggregated: Boolean)

object Lineage {
  def trace(plan: Plan): List[ColumnLineage]
}
```

This is "verified lineage" in the sense of
[MISSION.md §5](../MISSION.md#5-verified-lineage): derived from the
transformation plan itself, not observed at runtime and not merely
declared by metadata.

**Resolution algorithm.** A bare column reference is resolved structurally,
with no symbol table:

1. If the immediate input plan is a `Project`/`Aggregate`/`Window` that
   *declares* a matching output name, resolve against that declaration
   (recursing into the expression, which may itself reference further
   columns).
2. If the immediate input is a pass-through node (`Filter`, `Sort`),
   resolve against its input instead.
3. If the immediate input is a `Read`, the reference is a base case: it
   resolves to itself, qualified by that Read's dataset (or alias).
4. If the immediate input is a `Union`, try every branch and union the
   results — any branch could be the one producing a given row.
5. If the immediate input is a `Join`, try both branches. A qualified
   reference is ruled out on the side whose scope doesn't match. An
   unqualified reference that matches on only one side resolves normally;
   one that matches on **both** sides is genuinely ambiguous, and is
   resolved conservatively by unioning both sides' sources rather than
   guessing or throwing.

Aggregation propagates: if any input to a `FunctionCall` traces through an
`AggregateCall`, the result is marked `aggregated = true`. This is the
signal a future verification engine needs to know that a uniqueness or
row-level constraint doesn't directly apply to that output column.

### Known limitations

- **No schema catalog.** `Read` declares no columns, so `Lineage.trace` on
  a bare `Read` (nothing projected above it) returns an empty list. This
  isn't a missing feature so much as a consequence of not requiring a full
  type/schema system in the IR (see `docs/CONTRACT_MODEL.md` for where
  schemas *are* declared — the contract, not the transformation).
- **Join ambiguity is resolved conservatively, not precisely.** Without a
  real resolver, an unqualified name present on both sides of a `Join` is
  attributed to both, rather than raising an error or requiring
  disambiguation. This favors "don't silently miss a source" over
  "reject invalid SQL."
- **`SELECT *` isn't supported.** `Project.columns` and `Aggregate.aggregates`
  are always complete, explicit output lists. A front end translating a
  real engine's plan (future Phase 1b work) is expected to expand any
  wildcard projection before constructing this IR.

## Rendering (`PlanPrinter.scala`)

`PlanPrinter.render(plan: Plan): String` produces the ASCII tree shown
above. It is intentionally a separate module from `Plan.scala`/`Expr.scala`
— the plan and expression algebras carry no display logic of their own, the
way `Lineage` carries no display logic either. Two independent things
consume the same `children`-based structure.

## What Phase 2 does *not* do yet

- **No translator from a real Spark logical plan into this IR.** That's the
  bridge Phase 1b (the verification engine) needs, and is a substantial
  piece of work on its own — walking Catalyst's `LogicalPlan`/`Expression`
  trees and re-expressing them here.
- **No connection to the `contract` module yet.** `ir` and `contract` are
  independent modules by design; wiring "does this plan's `Lineage.trace`
  output satisfy this `Contract`'s declared outputs and rules" is Phase 1b.
- **No type inference.** Expressions carry no inferred/declared result
  type; `Literal.literalType` is the only type information in the IR today.

## Testing

```bash
cd ir
sbt test
```

21 tests across `PlanSpec` (construction, `children`, `Expr.references`),
`LineageSpec` (the worked example verbatim, a full `GROUP BY` stage,
`Filter`/`Sort` passthrough, `Join` attribution — both unambiguous and
ambiguous — `Window` pass-through-plus-new-columns, and `Union`), and
`PlanPrinterSpec` (rendering of each node kind). All run against real
constructed plans, not mocks.

### Mutation testing

[Stryker4s](https://github.com/stryker-mutator/stryker4s) checks whether
`LineageSpec`'s passing tests actually verify `Lineage`'s resolution logic,
not just execute it — see docs/SPARK_ADAPTER.md's "Mutation testing"
section for the full explanation of what this catches that coverage
can't. `ir/stryker4s.conf` and `build.sbt`'s `strykerMutate` setting scope
this to `Lineage.scala` (the file responsible for column-level provenance
correctness). Run it with:

```bash
cd ir
sbt stryker
# HTML report: target/stryker4s-report/<timestamp>/index.html
```

An initial run scored **44.4%** (8/18 mutants, 80% of the mutants in
actually-covered code). Two survivors are worth knowing about:

- **`l.aggregated || r.aggregated` → `&&`** (`Lineage.scala:120`, `Join`'s
  ambiguous-both-sides resolution) — no test constructs an unqualified
  column name that resolves on both join sides with *different*
  aggregation status, so the `||` could silently become `&&` (or vice
  versa) without any test noticing.
- **`columns.find(_.name == ref.name)` → `!=`** (`Lineage.scala:97`,
  `Project`'s name-matching in `resolveInScope`) — covered by some test,
  but nothing in that test's assertions distinguishes "resolved to the
  correctly-named column" from "resolved to some other column that
  happened to satisfy the assertion anyway."

This is a first pass, scoped deliberately narrow (see `ROADMAP.md` Phase
1c) — `sbt stryker` is not yet wired into CI as a gate, and `mutate` is
not yet widened to the rest of the module.

---

**Last Updated:** 2026-08-21
**Status:** Phase 2 — Transformation IR, initial implementation
