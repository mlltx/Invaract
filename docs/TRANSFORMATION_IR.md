# Transformation Intermediate Representation

This document describes the Invaract transformation IR delivered in Phase
2: an engine-independent representation of a data transformation, precise
enough to establish column-level lineage and, eventually, verify a
transformation against a [contract](CONTRACT_MODEL.md).

Code lives in the `ir/` sbt module (`com.invaract.ir` package). It has no
dependency on Spark, or on the `contract` module — it is pure Scala data
structures and algorithms over them. That independence is the point: this
is meant to be the thing a Spark logical plan, a SQL AST, or a dbt model
graph all get *translated into*, so that lineage tracing and contract
verification are written once, against this IR, rather than once per
engine.

## Critical principle: semantics, not syntax

The instruction that shaped every design decision in this module: **do not
build an IR that mirrors Spark's Catalyst classes one-for-one.** Catalyst
has dozens of `Expression` subclasses (`Add`, `Subtract`, `EqualTo`,
`GreaterThan`, `And`, `Or`, `Cast`, ...), an `Alias` that's itself an
expression wrapping any other expression, and attribute resolution via
globally unique `exprId`s. None of that is *transformation semantics* on
its own — it's implementation machinery for one specific execution
engine's optimizer. The IR distinguishes node kinds only where the
distinction is semantically load-bearing for lineage, contract
verification, or (eventually) fingerprinting/diffing a transformation's
business logic — never merely because Catalyst happens to have a
dedicated class for it.

Concrete decisions that follow from that:

1. **Named categories, not one node per Catalyst class — but not one
   node per *concept* either.** Arithmetic (`+`, `-`, `*`, `/`, `%`, unary
   negation), comparison (`=`, `<=>`, `<`, `<=`, `>`, `>=`), boolean
   combinators (`AND`/`OR`/`NOT`), casts, and conditionals (`CASE WHEN`)
   each get their own node — `Arithmetic`, `Comparison`, `BooleanExpr`,
   `Cast`, `Conditional` — because a future semantic diff needs to say "a
   filter's comparison operator flipped" or "an arithmetic operator
   changed," not just "some function call's arguments differ." Everything
   else that computes a scalar value per row — string/date/math functions,
   null checks, non-aggregate window functions (`RANK`, `ROW_NUMBER`,
   `LAG`, ...) — still collapses into one catch-all, `Function(name,
   args)`, exactly as the original single-`FunctionCall` design did: Spark
   alone exposes dozens of these, and enumerating a node type per built-in
   function would defeat the point of a small IR. `AggregateCall` remains
   its own category too, because aggregation changes something
   semantically load-bearing no other category does — cardinality (many
   input rows collapse into one output value) — which lineage tracing must
   be able to detect. (Named `AggregateCall` rather than `Aggregate` to
   avoid colliding with the `Aggregate` *plan* node in the same Scala
   package; `BooleanExpr` rather than `Boolean` for the same reason against
   `scala.Boolean`.)

2. **`UDF` is not `Function`.** A `Function` node is a claim that this IR
   understands what a named operation computes. A user-defined function's
   body is opaque — Scala/Java/Python/Hive UDFs alike — so it gets its own
   node, `UDF(name, args, engineType)`, that never masquerades as a
   built-in. `name` is populated only when the source engine exposes a
   real, non-generic identifier (a `spark.udf.register(...)`-assigned name,
   for instance) — Spark's own generic placeholder (`"UDF"` for an
   anonymous closure) is treated as "no name available," not a name.

3. **`Alias` exists, but only for a *nested* rename.** In Catalyst, `Alias`
   is itself an `Expression`, so naming can appear nested anywhere in an
   expression tree (e.g. a struct field's name inside `struct(col("a").as(
   "x"))`). At a plan's own output boundary, naming still isn't a
   computation on values — it's metadata about how a plan stage exposes a
   value downstream — so `Project`/`Aggregate`/`Window` still declare their
   output columns via `NamedExpr(name, expr)`, which is not itself an
   `Expr`. But a rename that occurs *mid*-expression (not at that boundary)
   has no other home in the algebra; discarding it (an earlier version of
   both this IR and its Spark translator did exactly that) silently drops
   real information. `Alias(name, expr)` — a genuine `Expr` — exists for
   exactly that case, and only that case: a top-level Catalyst `Alias` a
   translator finds at a `Project`/`Aggregate`/`Window` output list is
   still unwrapped straight into `NamedExpr`, never double-wrapped.

4. **Column identity stays name/qualifier-based, with one narrow, optional
   exception.** Catalyst binds every attribute reference to its producer
   via a globally unique `exprId`, resolved by an analysis pass. This IR
   still has no analysis pass of its own: a `ColumnRef` is a name plus an
   optional qualifier (`customer_id` from `raw.orders`), and `Lineage`
   resolves bare names by walking the plan structurally (see below) — the
   qualifier (a `Read`'s dataset or `alias`) is what disambiguates a
   self-join's two occurrences of the same source, which is the case that
   actually matters in practice (Spark itself requires distinct aliases for
   a self-join to analyze at all). `ColumnRef` additionally carries an
   optional `id: Option[Long]` a front end may populate from real
   per-attribute identity when it has one (Spark's `exprId.id`, exposed as
   a plain, opaque number — never a Catalyst type) — a narrow strengthening
   for the rare case name/qualifier alone can't tell apart, never a
   replacement for the qualifier-based scheme, and inert (`None`) for
   hand-constructed IR and any front end that doesn't have one.

## The IR

Two small algebras, following the classic logical-plan / expression split
(the one part of the design that *is* shared with virtually every SQL
engine, because it reflects relational algebra itself, not Spark
specifically):

### Expressions (`Expr.scala`) — compute a value

| Node | Represents |
|---|---|
| `ColumnReference(ref: ColumnRef)` | Reads one column. |
| `Literal(value, literalType)` | A constant. `literalType` uses the same logical-type vocabulary as a contract field's `type`. `value == null` (type still populated) is a typed SQL `NULL` — fully understood, not `UnknownExpression`. |
| `Alias(name, expr)` | A rename occurring *mid*-expression (e.g. a struct field's name) — not the plan-boundary `NamedExpr` below. |
| `Cast(expr, targetType)` | An explicit type conversion. `targetType` uses the same logical-type vocabulary as `Literal.literalType`. |
| `Arithmetic(operator, operands)` | A numeric operator: `+`, `-`, `*`, `/`, `%`, or unary `NEGATE`. `operands` has one entry for a unary operator, two for binary. |
| `Comparison(operator, left, right)` | A binary comparison: `=`, `<=>`, `<`, `<=`, `>`, `>=`. |
| `BooleanExpr(operator, operands)` | A boolean combinator: `AND`/`OR` (two operands) or `NOT` (one). |
| `Conditional(branches, elseValue)` | `CASE WHEN ... THEN ... [ELSE ...] END`, or a two-way `IF` (a single-branch `Conditional` with an `elseValue`). |
| `Function(name, args)` | The catch-all for any other scalar function: string/date/math functions, null checks, non-aggregate window functions (`RANK`, `ROW_NUMBER`, ...). |
| `UDF(name, args, engineType)` | A user-defined function whose body is opaque to this IR — never conflated with `Function`. `name` is `None` when the engine exposes no real (non-generic) identifier. |
| `AggregateCall(function, arg, distinct)` | An aggregate function (SUM, COUNT, AVG, MIN, MAX, ...). The one expression that changes cardinality. |
| `UnknownExpression(description, sourceType, children)` | A construct a front-end translator couldn't represent — always paired with a diagnostic, never silently dropped. |
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
| `Limit(input, limit, offset)` | Restricts the number of rows; column set unchanged. `offset` is `0` when the source plan has none. |
| `Window(input, windowExprs, partitionBy, orderBy)` | Adds columns computed over a window of related rows, without collapsing row count. Input columns pass through unchanged. |
| `UnknownPlan(description, sourceType, children)` | A plan node a front-end translator couldn't represent — always paired with a diagnostic, never silently dropped. |

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

### Row-level DML (`RowMutation.scala`)

`Write`'s "replace/append the output of a query" shape has no vocabulary
for a row-level DML operation — MERGE, UPDATE, DELETE — since those
mutate existing rows in place rather than producing a fresh output
dataset. `RowMutation(matchCondition, delete, updatedColumns)` captures
exactly the structural facts a contract's DML rules need (see
docs/CONTRACT_MODEL.md's "Interpreted rules"): a MERGE's `ON` condition
as a full `Expr` (so a verifier can read the columns it references),
whether/how an operation deletes rows (`DeleteScope`: `NotApplicable` /
`Unconditional` / `Conditional(condition)` — a sealed trait rather than
`Option[Expr]`, since "no delete happens" and "deletes unconditionally"
are both real states a bare `Option` can't distinguish), and the column
names a standalone UPDATE assigns.

Deliberately **not** a `Plan` node: it doesn't consume/produce datasets
the way `Plan`'s `children: List[Plan]` traversal assumes, and adding a
field to the existing `Write` case class to carry it would have broken
binary compatibility with already-compiled callers (case class
constructors are exact-arity). It's a plain, standalone value produced
by a front-end translator *alongside* the ordinary `Write` node the same
command already translates to — `spark-adapter`'s `RowMutationSupport`
is the Spark-specific extractor; `RuleVerifier` is the consumer. See
that module's own docs (docs/SPARK_ADAPTER.md) for the full story,
including why this deliberately covers only a *standalone* UPDATE/DELETE/
MERGE, not a DSv2 `SupportsRowLevelOperations` connector's rewritten
`ReplaceData`/`WriteDelta` form.

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
ColumnLineage(customer_id, Set(raw.orders.customer_id), Direct, Set())
ColumnLineage(lifetime_value, Set(raw.orders.amount), Computed, Set(AggregationDetail(SUM, false)))
```

This is the semantic content the spec's diagram is asking for: which source
column each output column traces to, whether that trace passes through an
aggregation (and which one), and how directly the output relates to its
source (a plain passthrough vs. a real computation).

## Lineage tracing (`Lineage.scala`)

```scala
sealed trait DerivationKind
object DerivationKind {
  case object Direct extends DerivationKind    // exactly one source column, possibly renamed
  case object Constant extends DerivationKind  // no source columns at all (a literal, or built entirely from literals)
  case object Computed extends DerivationKind  // built from real columns using only operations this IR understands
  case object Opaque extends DerivationKind    // contains a UDF or UnknownExpression anywhere in the tree
}

case class AggregationDetail(function: String, distinct: Boolean = false)

case class ColumnLineage(
  output: ColumnRef,
  sources: Set[ColumnRef],
  derivation: DerivationKind,
  aggregations: Set[AggregationDetail] = Set.empty
) {
  def aggregated: Boolean = aggregations.nonEmpty
}

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

**Aggregation detail.** Every `AggregateCall` an output column's expression
traces through contributes an `AggregationDetail(function, distinct)` to
that column's `aggregations` set — not just a single `aggregated: Boolean`
flag. An output combining more than one aggregate function
(`sum(x) / count(y)`) reports both, since collapsing that to one boolean
loses exactly the detail (which function, over what) a human auditing *how*
a column's meaning changed across a schema-compatible contract revision
would need. `aggregated` remains available as a convenience for "was this
aggregated at all," without inspecting the set.

**Derivation classification.** Alongside `sources`, each `ColumnLineage`
carries a `DerivationKind` — a coarse, human-auditable summary of *how* the
output relates to its sources:

- **`Direct`** — the output is exactly one source column, possibly renamed
  (`id = id`, or a mid-expression `Alias` over a bare reference). No
  computation anywhere along the way.
- **`Constant`** — no source columns whatsoever: a literal, or an
  expression built entirely from literals.
- **`Computed`** — derives from real source columns using only operations
  this IR fully understands (`Cast`/`Arithmetic`/`Comparison`/
  `BooleanExpr`/`Conditional`/`Function`/`AggregateCall`). A `CASE WHEN`
  built from a `Comparison` is the canonical example: every operation
  involved has known, named semantics — auditable, even if it happened
  several plan stages before the column's final declaration.
- **`Opaque`** — a `UDF` or `UnknownExpression` sits *anywhere* along the
  resolved computation, even nested arbitrarily deep beneath
  otherwise-understood operations or behind several layers of passthrough.
  A `CASE WHEN` whose result calls a UDF is opaque overall, not "mostly
  computed" — this IR knows what columns the UDF structurally depends on,
  but not what it actually computes from them, and `Opaque` always wins
  over every other classification once it applies anywhere.

Critically, this is computed *through* the same resolution `sources` already
uses, not by inspecting only a column's outermost declaring expression.
Because Invaract translates Spark's *analyzed* plan rather than the
optimized one (ADR-002), a chain of `.withColumn()` calls produces *nested*
`Project`s — the outermost `Project`'s own declaration for an untouched
column is frequently nothing more than a bare passthrough reference to an
inner `Project`'s real computation (`value_squared = value_squared`, where
the actual `value * value` lives one level down). Classifying only that
outer syntax would misreport the column `Direct`; `Lineage`'s internal
`resolveExpr`/`resolveInScope` walk resolves straight through any number of
such passthrough hops to find the real computation, exactly as it already
does to find `sources`. An unresolvable reference (e.g. one sitting on an
`UnknownPlan`) falls back to `Direct` — the honest, syntax-level answer when
there's nothing further to resolve into.

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
- **No structural verification of lineage against a contract yet.** `ir`
  and `contract` stay independent modules by design (`ir` has no
  dependency on `contract` at all); wiring "does this plan's
  `Lineage.trace` output satisfy this `Contract`'s declared outputs and
  rules" is still Phase 1b. One narrower bridge does exist today, in
  `spark-adapter` rather than here: `SensitivityLineage.propagate` cross-
  references traced lineage against a contract's declared input
  `Field.sensitivityTags`, so a governance-tagged input's labels surface
  on every output column that transitively derives from it (see
  docs/SPARK_ADAPTER.md's "Sensitivity propagation" section) — but this is
  reporting, not enforcement: it never fails verification, and it says
  nothing about outputs/rules generally, only about propagating a specific
  kind of input-declared metadata forward through the same `sources` this
  module already computes.
- **No type inference.** Expressions carry no inferred/declared result
  type; `Literal.literalType` is the only type information in the IR today.

## Testing

```bash
cd ir
sbt test
```

48 tests across `PlanSpec` (12 — construction, `children` (now including
`Limit`), `Expr.references`, `UnknownPlan`/`UnknownExpression` carrying
their `sourceType` and any still-resolvable children), `LineageSpec` (20
— the worked example verbatim, a full `GROUP BY` stage, `Filter`/`Sort`
passthrough, `Join` attribution — both unambiguous and ambiguous,
including mixed aggregation status — `Window`/`Aggregate`/`Union`
resolution when a `Project` sits directly on top of them, literal- and
multi-argument-function-derived outputs, and — new for the expression
algebra rework — `Cast`/`Alias`/`Arithmetic`/`Comparison`/`BooleanExpr`/
`Conditional`/`UDF` all resolving transparently to their operands'
sources, a `Conditional`'s branch *conditions* counting as sources
alongside its values, and an `UnknownExpression`'s still-resolvable
children continuing to contribute sources even though the node itself is
opaque), `PlanPrinterSpec` (13 — rendering of each node kind, exact
branch/continuation-prefix structure at nested depth, `DISTINCT`, empty
`GROUP BY`, `Sort` direction, `Window`'s `PARTITION BY`/`ORDER BY`, and
`Cast`/`Arithmetic`/`BooleanExpr`/`Conditional`/`UDF`/`Alias` rendering in
their natural infix/prefix forms), and `RowMutationSpec` (4 — default
shape, `matchCondition` carrying a full `Expr`, `DeleteScope`'s three
distinct states, `updatedColumns`). All run against real constructed
plans, not mocks. `spark-adapter`'s own test suite (389 tests, including
`ExpressionTranslationSpec`) is the other half of this coverage — real
analyzed Spark plans translated and asserted structurally, not just
hand-constructed IR; see docs/SPARK_ADAPTER.md.

### Mutation testing

[Stryker4s](https://github.com/stryker-mutator/stryker4s) checks whether
this module's passing tests actually verify `Lineage`/`PlanPrinter`'s
logic, not just execute it — see docs/SPARK_ADAPTER.md's "Mutation
testing" section for the full explanation of what this catches that
coverage can't, and CLAUDE.md's "Mutation Testing Requirement" for the
70% bar expected of new/changed code specifically (this module's
whole-module score, below, is the separate CI-blocking bar).
`build.sbt`'s `strykerMutate`/`strykerThresholds*` settings scope this to
the whole module and gate CI at 50% (`stryker4s.conf`'s equivalent keys
were observed not to take effect with this sbt/plugin version
combination). Run it locally with:

```bash
cd ir
sbt stryker
# HTML report: target/stryker4s-report/<timestamp>/index.html
```

An initial run scoped to just `Lineage.scala` scored 44.4% (8/18 mutants).
Adding tests for the real survivors — and widening to the whole module,
which pulled `PlanPrinter.scala` in too — brought it to **86.36%**
(76/100 mutants killed):

- **`l.aggregated || r.aggregated` → `&&`** (`Join`'s ambiguous-both-sides
  resolution) — fixed with a test joining a plain passthrough side against
  an aggregated side on an ambiguous reference, the minimum shape where
  `||` and `&&` actually disagree. The aggregation-detail rework (see
  "Aggregation detail" above) later replaced this boolean `||` with a
  `Set` union (`l.aggregations ++ r.aggregations`) — the exact operator
  this bullet names no longer exists in the current code, but the lesson
  (test a mixed aggregating/non-aggregating join side, not just an
  all-or-nothing one) still applies, and the same test still exercises it.
- **`columns.find(_.name == ref.name)` → `!=`** (`Lineage.scala:97`,
  `Project`'s name-matching in `resolveInScope`) — fixed by asserting the
  resolved *sources*, not just the aggregation flag, on the existing
  windowed-column test (the aggregation flag alone couldn't distinguish
  "resolved to the right column" from "resolved to some other column that
  happened to satisfy the old assertion anyway").
- **`Aggregate`/`Window`/`Union` cases of `resolveInScope`** (previously
  entirely uncovered — no test stacked a `Project` directly on any of
  these) — fixed with three new tests doing exactly that.
- **Exact branch (`├─`/`└─`) and continuation-prefix (`│  `/`   `)
  correctness** (`PlanPrinter.scala`'s `renderChildren`) — every prior
  test only asserted `.contains(...)` on node *content*, never on which
  prefix character preceded it, so a bug that swapped which child counts
  as "last" left every substring still present, just under the wrong
  prefix. Fixed with one test asserting a full rendered string, exactly,
  for a nested two-branch tree.
- **`DISTINCT`, empty `GROUP BY`, `Sort` direction, and `Window`'s
  `PARTITION BY`/`ORDER BY`** — none had a test exercising the "true" or
  "false" side of their respective conditionals (e.g. no test ever
  rendered a `Sort` or `Window` node's label at all). Fixed with four new
  tests.

What's left (12/100 undetected) is 11 `StringLiteral` mutants on
`PlanPrinter`'s pure formatting separators (`", "`, `"\n"`, node-label
text) — the same low-priority, commonly-excluded category discussed in
docs/SPARK_ADAPTER.md — plus one genuinely equivalent mutant
(`Lineage.scala:113`'s `found.isEmpty` forced to `false`: when `found` is
truly empty, the "wrong" branch computes `Provenance(Set.empty, false)`,
which is byte-for-byte the same value the correct `None` produces after
`resolveExpr`'s `.getOrElse` — mathematically unobservable, not a real
gap).

**Follow-up: the expression-algebra rework** (CLAUDE.md's per-PR 70% bar,
scoped to the 5 touched files — `Expr.scala`, `Plan.scala`,
`Identifiers.scala`, `Lineage.scala`, `PlanPrinter.scala` — not a
whole-module run; `RowMutation.scala` was untouched by this change and
excluded from scope):

```bash
cd ir
sbt 'set strykerMutate := Seq("src/main/scala/com/example/ir/Expr.scala", "src/main/scala/com/example/ir/Plan.scala", "src/main/scala/com/example/ir/Identifiers.scala", "src/main/scala/com/example/ir/Lineage.scala", "src/main/scala/com/example/ir/PlanPrinter.scala")' stryker
```

First pass (before writing tests for the new `Cast`/`Arithmetic`/
`Comparison`/`BooleanExpr`/`Conditional`/`UDF`/`Alias`/`Limit`/
`UnknownPlan`/`UnknownExpression` code specifically): 74.34% (84/113).
Two real, addressable gaps, both in genuinely new code, not the
pre-existing 12 above:

- **`Limit`'s rendering was entirely `NoCoverage`** (`PlanPrinter.scala`,
  the `offset > 0` branch and both `s"Limit(...)"` string forms) — no
  test had ever called `PlanPrinter.render` on a `Limit` node at all
  (the one `Limit` test that existed, in `PlanSpec`, only checked
  `.children`). Fixed with a direct `PlanPrinterSpec` test rendering
  `Limit` both with and without a nonzero `offset`.
- **`BooleanExpr`'s unary (`NOT`) and 3+-operand fallback forms, and
  `Arithmetic`'s 3+-operand fallback form, were untested** — the
  combined "Cast/Arithmetic/BooleanExpr/Conditional/UDF/Alias" test only
  exercised `AND` (2 operands) and the binary arithmetic case. Fixed with
  a dedicated test covering `NOT` and a 3-operand case of each.
- Additionally strengthened `LineageSpec`'s new-node-type test: several
  assertions had a `Literal` on one side of a `Comparison`/`Arithmetic`
  (contributing no sources either way), so a mutant that silently
  dropped one operand's contribution to `combine`'s `flatMap` wouldn't
  have been observable. Added three more cases with real columns on
  *both* sides.

Second pass, after those fixes: **84.96%** (96/113) — every remaining
survivor is either the same `StringLiteral`-formatting category as
above, or the same genuinely-equivalent `found.isEmpty` mutant
(unchanged by this rework, just at a shifted line number). No whole-module
rerun was done locally for this follow-up — CI's own whole-module job
(50% break threshold, well below both the prior 86.36% baseline and this
scoped 84.96%) covers that; `RowMutation.scala`'s own coverage is
untouched by this change.

### API compatibility

`Plan`, `Expr`, every case class in `Identifiers.scala` (`DatasetRef`,
`ColumnRef`, ...), `RowMutation`/`DeleteScope`, plus `Lineage.trace` and
`PlanPrinter.render`'s signatures, are this module's binary API surface —
the engine-independent algebra any future front-end (SQL, dbt) is meant
to translate into, per this doc's "Critical principle" above, so keeping
it stable release-to-release matters more here than almost anywhere else
in the codebase. Checked by [MiMa](https://github.com/lightbend/mima) via
`sbt mimaReportBinaryIssues`, CI-enforced on every PR — see CLAUDE.md's
"API Compatibility Requirement" for the full mechanism. `RowMutation` was
added as a wholly new, standalone case class rather than a new field on
the existing `Write` — adding a field to a case class already in the
0.1.0 baseline changes its constructor's arity, which breaks every
already-compiled caller; a new class has no such history to break.

`ColumnLineage` replacing its single `aggregated: Boolean` field with
`derivation: DerivationKind` and `aggregations: Set[AggregationDetail]`
(the "Aggregation detail"/"Derivation classification" section above) is a
real binary-incompatible change to that case class's constructor — but not
one this PR needed a new `mimaBinaryIssueFilters` entry for: `ir/build.sbt`
already carries a transitional, whole-package `ProblemFilters.exclude`
(see that file's own comment) covering every symbol under the pre-rebrand
`com.example.ir.*` namespace this module's `mimaPreviousArtifacts` still
points at, and `ColumnLineage` — like everything else changed alongside
the `com.invaract` rebrand — has no baseline symbol at that old namespace
to compare against at all. This rides on that transitional filter rather
than adding a second one; once the follow-up PR flips
`mimaPreviousArtifacts` to a real `com.invaract` 0.2.0 baseline (per that
file's own "FOLLOW-UP" comment), this shape becomes the new baseline going
forward, and any *future* change to it would need its own filter entry
the ordinary way.

The expression-algebra rework this doc otherwise describes (splitting
`FunctionCall` into `Cast`/`Arithmetic`/`Comparison`/`BooleanExpr`/
`Conditional`/`Function`/`UDF`/`Alias`, renaming `Unsupported`/
`UnsupportedExpr` to `UnknownPlan`/`UnknownExpression`, and adding an `id`
field to `ColumnRef`) **is** exactly the kind of deliberate, documented
MAJOR-version break this section warns about — not an exception to it.
`ir/build.sbt`'s `mimaBinaryIssueFilters` carries the exact
`ProblemFilters.exclude[...]` lines `sbt mimaReportBinaryIssues` reported
against a real local `publishLocal` of this module as it stood
immediately before the change, each with a comment pointing back here,
per CLAUDE.md's "the break is deliberate" path (option 2, not a loosened
`mimaPreviousArtifacts`).

---

**Last Updated:** 2026-08-28
**Status:** Phase 2 — Transformation IR, initial implementation
