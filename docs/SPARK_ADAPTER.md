# Spark Adapter

This document describes the Spark adapter: the bridge from Spark's Catalyst
logical plan into the Invariant [transformation IR](TRANSFORMATION_IR.md).
It's the first concrete front-end for the IR, and the first piece of
ROADMAP.md Phase 1c (Verification Engine) — the IR now has one real source
of transformation plans, not just hand-constructed test fixtures.

Code lives in the `spark-adapter/` sbt module (`com.example.sparkadapter`
package), which depends on `ir` and on Spark (`provided`, same convention as
`plugin`/`runner`).

## Integration point

Spark exposes a query's logical plan through several extension mechanisms.
Before writing any adapter code, this was investigated directly against a
real Spark 3.5.1 session (`spark-shell`, not documentation alone — see
*Empirical findings* below for what that surfaced):

| Mechanism | What it does | Weight |
|---|---|---|
| `Dataset.queryExecution.analyzed` | Direct field access on a `DataFrame` you already hold. | None — no session configuration. |
| `org.apache.spark.sql.util.QueryExecutionListener` | A supported, documented callback invoked after every query a `SparkSession` executes, with no change to how any query is written. | Low — one `spark.listenerManager.register(...)` call. |
| `SparkSessionExtensions` | Inject a custom analyzer/optimizer rule at session-construction time. | High — session must be built with the extension; rules can *rewrite* plans, not just observe them. |

The adapter's core function, `SparkPlanAdapter.translate(plan: LogicalPlan)`,
is agnostic to which of these supplied the plan — it only needs a
`LogicalPlan` value. Two of the three are actually used in this repo:

- **Tests** (`SparkPlanAdapterSpec`) call `df.queryExecution.analyzed`
  directly — simplest option when a `DataFrame` is already in hand.
- **The `runner` integration** (`DemoJobHarness.scala`) registers
  `SparkAdapterListener`, a thin `QueryExecutionListener`, once at
  `SparkSession` construction. This was chosen over
  `SparkSessionExtensions` because the requirement is only to *observe* a
  plan, never to rewrite one — `SparkSessionExtensions` would be strictly
  more mechanism than the job needs.

## Empirical findings

Several design decisions below came directly from running real Spark code
and inspecting the actual Catalyst plan shapes, not from documentation or
memory — Catalyst's node shapes vary across Spark versions and are not
fully specified. The findings that most shaped the adapter:

**`.analyzed` is used, not `.optimizedPlan`.** Given
`spark.read.csv(...).as("o")`:

```
// .analyzed
SubqueryAlias o
+- Relation [id#17,value#18] csv

// .optimizedPlan
Relation [id#17,value#18] csv
```

The optimizer's `EliminateSubqueryAliases` rule removes `SubqueryAlias`
nodes entirely. This IR needs relation aliases to disambiguate self-joins
(`ColumnRef.qualifier` — see [TRANSFORMATION_IR.md](TRANSFORMATION_IR.md)),
so `.analyzed` — resolved, but not yet alias-stripped — is the right input,
not the more heavily rewritten `.optimizedPlan`.

**The write path wraps the query in an internal `WriteFiles` node.**
`df.write.mode("overwrite").parquet(path)` produces:

```
InsertIntoHadoopFsRelationCommand file:/tmp/out.parquet, false, Parquet, ...
+- WriteFiles
   +- Aggregate [id#17], [id#17, sum(value#18) AS lifetime_value#24L]
      +- Relation [id#17,value#18] csv
```

`WriteFiles` (Spark 3.4+) carries no information relevant to this IR, so
it's unwrapped — by checking `getClass.getSimpleName == "WriteFiles"`
rather than importing the class directly, since it's a Spark-internal type
an adapter targeting a different Spark version might not have.

**`Relation [...] csv` in a treeString is a `LogicalRelation`.** The
compact treeString rendering is cosmetic; `.collect { case lr: LogicalRelation => ... }`
confirms the underlying node, and for a CSV/Parquet source its `relation`
field is a `HadoopFsRelation` whose `.location.rootPaths` gives the real
file path used as `DatasetRef.location`.

**`Aggregate.aggregateExpressions` mixes bare and aliased entries.** For
`.groupBy("id").agg(sum("value").as("lifetime_value"))`:

```
Aggregate [id#17], [id#17, sum(value#18) AS lifetime_value#24L]
```

The grouping column comes through as a **bare** `AttributeReference`
(`id#17`), not wrapped in `Alias`, alongside an `Alias`-wrapped aggregate.
Both are `NamedExpression`s (`.name` is defined on the trait), so one
`translateNamed` helper handles both uniformly rather than needing an
`Alias`-only code path.

**`BinaryOperator.symbol` gives clean, uniform operator names** — `Add`,
`And`, `EqualTo`, `GreaterThan`, etc. all expose `.symbol` (`"+"`, `"&&"`,
`"="`, `">"`), letting one `case b: BinaryOperator` cover arithmetic,
comparison, and boolean logic without enumerating Catalyst's dozens of
`Expression` subclasses.

**`count(*)` is `count(1)` under the hood** — its `AggregateFunction` has
exactly one child (a `Literal(1)`), same shape as `SUM`/`AVG`/`MIN`/`MAX`.
No special-casing needed for the common case; only genuinely
multi-argument aggregates (e.g. `COUNT(a, b)`) need the fallback described
below.

## Never throws

`translate` always returns an `ir.Plan`. This matters because a real
adapter will meet constructs it doesn't have a precise translation for —
the question is what happens then, and the answer here is: **degrade, never
crash.**

- An unrecognized **plan** node becomes `ir.Unsupported(description, children)` —
  its own children are still translated and remain inspectable.
- An unrecognized **expression** falls through to a generic translation
  built from Catalyst's own `prettyName`/`children` (available on every
  `Expression`) rather than needing to be enumerated — this alone covers
  most of Spark SQL's built-in functions (string/date functions, `CASE
  WHEN`, `IS NULL`, casts-adjacent operators) without hardcoding them.
- A user-defined function (`ScalaUDF`, `PythonUDF`, Hive UDFs) is
  opaque — its *body* can't be reasoned about — so it's translated as a
  `FunctionCall` over its declared arguments, with a `Diagnostic` flagging
  that lineage tracing can't see inside it.
- A multi-argument aggregate (anything but the single-argument common
  case) is combined into a synthetic `ARGS(...)` wrapper with a
  `Diagnostic`, since `AggregateCall` models exactly one argument.

Every degradation is paired with a `Diagnostic(nodeType, message)`, and
`TranslationResult(plan, diagnostics)` carries the complete list. A
partially understood pipeline is more useful to a verification engine than
an exception that discards everything the adapter *did* understand.

Both `ir.Unsupported` and `ir.UnsupportedExpr` (see
[TRANSFORMATION_IR.md](TRANSFORMATION_IR.md)) were added to the IR itself
as part of this work — a principled, engine-agnostic vocabulary for "could
not translate this," usable by any future front-end, not a Spark-specific
workaround bolted onto the adapter alone.

## Translation coverage

| Spark construct | IR translation |
|---|---|
| `InsertIntoHadoopFsRelationCommand` | `Write(DatasetRef(outputPath), ..., format, saveMode)` — `format` from the write's `FileFormat` via `DataSourceRegister.shortName()` (`None` if it doesn't implement that trait); `saveMode` from the command's own `SaveMode` (`append`/`overwrite`/`error`/`ignore`, normalized to lowercase) |
| `SaveIntoDataSourceCommand` (Delta and other `CreatableRelationProvider`-based `.save(...)` writes) | `Write(DatasetRef(options("path")), ..., format, saveMode)` — `format` from the write's `dataSource` via `DataSourceRegister.shortName()`, the same mechanism as above, just for the other provider trait Spark routes non-`FileFormat` writes through (see "Delta Lake support" below) |
| `CreateDataSourceTableAsSelectCommand` (`.saveAsTable(...)`/`CREATE TABLE ... AS SELECT` against a *new* V1 data source table) | `Write(DatasetRef(storageLocation), ..., format, saveMode)` — `format` straight from `table.provider` (already the clean identifier `DataSourceRegister.shortName()` gives the other two write cases); a third, distinct write shape from both of the above (see "Fail-closed on unverifiable writes" below) |
| `LogicalRelation` (+ `HadoopFsRelation`) | `Read(DatasetRef(rootPath))` — also covers Delta reads (`.load(path)` and catalog table references alike): Delta's read relation is an anonymous subclass of `HadoopFsRelation`, matched here via ordinary subtyping, no dedicated case needed (see "Delta Lake reads" below) |
| `LogicalRelation` (+ `JDBCRelation`) | `Read(DatasetRef("jdbc:<url>/<table>"))` — `JDBCRelation` is `private[sql]` in Spark, so its `jdbcOptions()` accessor is fetched reflectively rather than pattern-matched on the type directly (see `locationOf`'s doc comment); this is a precise identity, not the generic `catalogTable`/`toString` fallback other non-file relations get |
| `SubqueryAlias` over a `Read` | `Read(..., alias = Some(name))` |
| `SubqueryAlias` over anything else | pass-through + `Diagnostic` (no generic aliased-subplan node in the IR) |
| `Project` | `Project(input, columns)` |
| `Filter` | `Filter(input, condition)` |
| `Join` | `Join(left, right, joinType, condition)` |
| `Aggregate` | `Aggregate(input, groupingExpressions, aggregateExpressions)` |
| `Union` | `Union(children)` |
| `Sort` | `Sort(input, order)` |
| `Window` | `Window(input, windowExpressions, partitionSpec, orderSpec)` |
| `GlobalLimit` / `LocalLimit` | transparent pass-through (row count doesn't affect column lineage) |
| `Deduplicate` (`.distinct()`) | transparent pass-through (row count only, no column change) |
| `Repartition` / `RepartitionByExpression` (`.repartition()`, `.coalesce()`) | transparent pass-through (physical partitioning only) |
| `AttributeReference` | `ColumnReference(ColumnRef(name, qualifier))` |
| `Alias` (top-level, in an output list) | `NamedExpr(name, translated child)` |
| `Alias` (nested elsewhere) | translated child, name discarded (no `Alias` expression node in the IR — see design doc) |
| `Literal` | `Literal(value, logicalTypeName)` |
| `Cast` | `FunctionCall("CAST", [child, Literal(targetType, "type")])` |
| `WindowExpression` | translated `windowFunction`; spec discarded (already captured at the `Window` plan node) |
| `AggregateExpression` | `AggregateCall(prettyName, arg, isDistinct)` |
| `BinaryOperator` (arithmetic/comparison/boolean) | `FunctionCall(symbol, [left, right])` |
| `ScalaUDF` / `PythonUDF` / Hive UDFs | `FunctionCall(prettyName, args)` + `Diagnostic` |
| everything else | `FunctionCall(prettyName, children)` (generic) |
| any other plan node | `Unsupported(description, translated children)` + `Diagnostic` |

## Worked example

The example from Phase 2/3's spec (`SELECT id, SUM(value) AS lifetime_value
FROM orders GROUP BY id`, written as `gold.customer_orders`), run against
`demo/input/sample.csv` through the real adapter — actual captured output:

```scala
val agg = orders.groupBy("id").agg(sum("value").as("lifetime_value"))
val result = SparkPlanAdapter.translateAsWrite(agg.queryExecution.analyzed, DatasetRef("gold.customer_orders"))
```

`PlanPrinter.render(result.plan)`:

```
Write(gold.customer_orders)
└─ Aggregate
   ├─ Read(file:/home/user/Invariant/demo/input/sample.csv)
   ├─ GROUP BY id
   ├─ id = id
   └─ lifetime_value = SUM(value)
```

`Lineage.trace(result.plan)`:

```
ColumnLineage(id, Set(.../sample.csv.id), aggregated = false)
ColumnLineage(lifetime_value, Set(.../sample.csv.value), aggregated = true)
```

Zero diagnostics — every construct in this pipeline has a precise
translation.

## Integrated with the test Spark app

`runner/src/main/scala/com/example/runner/DemoJobHarness.scala` registers
`SparkAdapterListener` before running `InvariantPlugin`, and after the
real `outputDf.write.mode("overwrite").parquet(outputPath)` call:

1. Waits (bounded, since `QueryExecutionListener` callbacks run
   asynchronously on Spark's own listener thread) for the listener to
   capture the write's translated IR.
2. Adds a `transformationIR` section to `demo/output/report.json`: the
   rendered plan, the traced lineage, and any diagnostics.
3. Prints the rendered plan to the console.

Actual output from `./dev/test` against the real `InvariantPlugin`
(`value_squared = value * value`, per `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`):

```
Transformation IR (translated from the real Spark logical plan):
Write(file:/home/user/Invariant/demo/output/result.parquet)
└─ Project
   ├─ Read(file:/home/user/Invariant/demo/input/sample.csv)
   ├─ id = id
   ├─ value = value
   └─ value_squared = value * value
```

`demo/output/report.json`'s `transformationIR.lineage`:

```json
[
  { "output": "id", "sources": ["file:.../sample.csv.id"], "aggregated": false },
  { "output": "value", "sources": ["file:.../sample.csv.value"], "aggregated": false },
  { "output": "value_squared", "sources": ["file:.../sample.csv.value"], "aggregated": false }
]
```

Note `value_squared`'s single source: `value` is referenced twice in
`value * value`, and `ColumnLineage.sources` is a `Set`, so the duplicate
collapses — correctly reporting "derives from `value`," not "derives from
`value` twice."

## Diagnostics: plan extraction examples

From `SparkPlanAdapterSpec` (all run against real Spark, not mocked):

- **Filter + Cast** — `df.filter(col("value") > 20).withColumn("value_d", col("value").cast("double"))`
  translates the cast as `FunctionCall("CAST", [ColumnReference(value), Literal("double", "type")])`,
  zero diagnostics.
- **Self-join** — `df.as("cur").join(df.as("arch"), ...)` translates to
  `Join(Read(_, Some("cur")), Read(_, Some("arch")), Inner, Some(...))` —
  both sides individually addressable via `ColumnRef.qualifier`.
- **UDF** — `spark.udf.register("triple", ...)` used in a projection
  produces a `Diagnostic` whose `nodeType` contains `"UDF"`, alongside a
  best-effort `FunctionCall("TRIPLE", [...])` translation — the pipeline
  keeps going.
- **Unsupported construct** — `explode(array(...))` (a `Generate` logical
  node, which has no translation case) produces an `ir.Unsupported` node
  nested exactly where the untranslatable construct sits in the plan, plus
  a `Diagnostic`, rather than an exception.

## Delta Lake support

Delta writes are translated via `SaveIntoDataSourceCommand` (see
"Translation coverage" above) — added after investigating what a real
Delta write actually analyzes to, empirically, not assumed: a fresh
Delta-enabled `SparkSession`
(`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`,
`spark.sql.catalog.spark_catalog=...DeltaCatalog`), a real
`df.write.format("delta").save(path)`, and a `QueryExecutionListener`
logging every analyzed plan Spark produced along the way. The write's
final analyzed plan was `SaveIntoDataSourceCommand`, not a Delta-specific
class at all — Spark's own generic command for any
`CreatableRelationProvider`-based `.save(...)` write, the non-`FileFormat`
counterpart to `InsertIntoHadoopFsRelationCommand`.

**No Delta dependency, compile-time or runtime, is needed to translate
this.** `SaveIntoDataSourceCommand` and `DataSourceRegister` are both
plain, public `org.apache.spark.sql` classes already on this module's
existing `provided` Spark dependency; Delta's `DeltaDataSource` was
confirmed (same investigation) to implement `DataSourceRegister` with
`shortName() == "delta"`, exactly the mechanism `formatOf` already used
for built-in file formats — so `formatOf`'s parameter type was simply
widened from `FileFormat` to `AnyRef` and reused as-is, rather than
writing Delta-specific translation code. `delta-spark` appears only as a
`% "test"` dependency (pinned to 3.2.0, not the latest 3.2.x: a confirmed
real bug in 3.2.1 affects exactly this Scala 2.12 + Spark 3.5.1
combination — see
[delta-io/delta#3737](https://github.com/delta-io/delta/issues/3737)) —
its only job is spinning up a real Delta session to test against, the
same role `com.h2database` plays for the JDBC precedent, never something
the main translation code imports or needs present at runtime for a
non-Delta job to run.

This is also why the single assembled `spark-adapter` jar (see
CLAUDE.md's "What's the product, and what's the test harness" and
ARCHITECTURE.md) needs no Delta-specific variant: nothing about Delta
support depends on Delta being bundled in, or even present, unless a
user's own job actually writes Delta — confirmed directly, not assumed,
by inspecting the assembled jar after adding this support and finding
zero Delta classes in it, same size as before.

An initial attempt at this feature assumed Delta's own command classes
would need recognizing directly, which would have required a real
`delta-spark` compile dependency (`provided`, to keep it out of the
assembled jar) and Delta-specific pattern-matching code — abandoned once
the actual analyzed plan turned out to already be a generic Spark class,
making that unnecessary entirely.

Two real bugs surfaced fixing this, neither about translation itself:

- **`ContractEnforcementRule.verifyOrThrow`'s output-schema derivation**
  only special-cased `InsertIntoHadoopFsRelationCommand`; for any other
  plan (including the new `SaveIntoDataSourceCommand` case) it fell back
  to the command node's *own* `.schema` — empty, since `Command` nodes
  don't produce rows — rather than the schema of the query being written.
  A real Delta write, correctly matching every field its contract
  declared, still failed with `MISSING_OUTPUT_FIELD` on every one of
  them, because the schema being checked against was always empty
  regardless of what was actually written. Fixed by adding the same
  `cmd.query.schema` case `SaveIntoDataSourceCommand` needs.
- **`SparkAdapterListener.onSuccess`** has its own independent "is this a
  write" check, entirely separate from `SparkPlanAdapter.translatePlan`'s
  and `ContractEnforcementRule.verifyOrThrow`'s — also hardcoded to only
  `InsertIntoHadoopFsRelationCommand`. A Delta write correctly translated
  and correctly enforced, yet the listener-based report
  (`demo/output/report.json`'s `transformationIR` section) never
  captured it. Fixed the same way, in a third, separate location — three
  independent places recognizing "is this a write command" was, at the
  time, left as a noted design smell rather than fixed. It has since been
  fixed — see "Write command recognition: a single registry" below.

Both were caught by a real, real-Spark integration test failing (the new
Delta PASS/FAIL pair in `ContractEnforcementRuleSpec` and the translation
test in `SparkPlanAdapterSpec`), not by inspection — consistent with this
module's general testing philosophy.

**Known limitation:** `.save(path)`-style writes are recognized here;
`.saveAsTable(...)` against a *new* V1 data source table is a separate,
later addition (`CreateDataSourceTableAsSelectCommand` — see "Fail-closed
on unverifiable writes" below). DataFrameWriterV2 and SQL `MERGE INTO`
against a Delta (or any DataSourceV2 catalog) table go through a different
plan shape entirely (`AppendData`/`OverwriteByExpression`/Delta's own
`MergeIntoCommand`/similar) that still has no translation — not a silent
gap any more, though: see "Fail-closed on unverifiable writes" for why
these now abort instead of passing through unverified.

## Delta Lake reads

Everything above is the write side; Delta as a contract *input* was a
separate, previously unexplored question, prompted directly by asking
whether read recognition had the same "recognition duplicated across
independent match sites" problem the write side did before "Write command
recognition: a single registry" fixed it.

**Investigated empirically, not assumed — and the answer turned out to be
simpler than expected.** A real Delta-enabled session with an
`injectCheckRule` probe (the same mechanism `ContractEnforcementRule`
actually uses) was run against both `.load(path)` and a catalog table
reference (`spark.table(...)`/`SELECT * FROM tbl`/`SELECT * FROM
delta.\`path\``). Both produce a `LogicalRelation` wrapping
`org.apache.spark.sql.delta.DeltaLog$$anon$2` — and that class is an
**anonymous subclass of Spark's own `HadoopFsRelation`**, not a distinct
relation type the way it first appeared. `locationOf`'s existing
`case h: HadoopFsRelation => h.location.rootPaths...` branch (and the
identical guard in `translatePlan`'s `LogicalRelation` case) already
matches it through ordinary Scala subtyping, and already extracts the
precise physical path — for `.load(path)` *and* for a catalog table
reference alike, confirmed by checking `catalogTable.storage.locationUri`
against what `locationOf` actually returns.

**Net result: zero new code.** No new `translatePlan` case, no location
fallback to improve, and — directly answering the motivating question —
**no registry consolidation needed on the read side.** The write side's
duplication bug was real and specific: three sites recognized *different*
concrete `Command` classes, so one could add support for a class the
others didn't know about. Reads have no analogous risk today: both
consumer sites (`SparkPlanAdapter.translatePlan`'s `LogicalRelation` case
and `ContractEnforcementRule.verifyOrThrow`'s `plan.collect { case lr: LogicalRelation => ... }`
for input-schema collection) gate on the *same* single Spark type, so
they cannot disagree with each other by construction — and Delta's read
relation turned out to already be that type, by inheritance, not a
second type needing its own case.

This does **not** mean the read side can never have the write side's
problem — it means it doesn't have it *today*, for *this* connector. A
future connector whose read genuinely produces something other than
`LogicalRelation` (a `DataSourceV2Relation`, most plausibly — the same
"Known limitations" gap already noted for DataSourceV2 catalog writes)
would need a real second case added in both of those sites, and *that*
would reintroduce the write side's exact risk unless done as a shared
mechanism from the start. Treat that as the trigger for a
`ReadRelationSupport`-style registry, not a reason to build one now for a
shape that doesn't exist yet — see docs/ADDING_A_SPARK_CONNECTOR.md.

**Verified through real enforcement, not just translation in isolation:**
a translation test (`SparkPlanAdapterSpec`) confirms both read shapes
produce a precise `ir.Read` with no fallback diagnostic, and a PASS/FAIL
enforcement pair (`ContractEnforcementRuleSpec`) confirms a contract's
declared input schema is genuinely checked against a real Delta read's
actual schema — including a real, incidental finding along the way: Delta
reports every column nullable on read-back regardless of what was
written, a genuine Delta behavior (not a bug here) that the FAIL test's
contract works around the same way a real contract author would need to.

## Delta Lake operation-surface coverage ledger

`add-spark-connector` was run for Delta twice — once for writes, once for
reads — and each time declared its narrower scope done without stating
what was still untouched. That gap (see
docs/ADDING_A_SPARK_CONNECTOR.md's "coverage ledger" requirement, added
directly because of it) is what this section closes: every row of the
canonical operation surface, investigated empirically against a real
Delta-enabled `SparkSession` via `injectCheckRule` — the exact mechanism
`ContractEnforcementRule` uses — with one of three dispositions per row.
"❓ Not investigated" would be a legitimate answer for a row; a missing
row is not, so every row below has one. **Every 🚫 row also carries a
next step, not just a citation** — see
docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means (and
doesn't)": fail-closed is a safety net for an operation not yet
translated, not a verdict that it shouldn't be. A row landing on 🚫 here
is future work, unless stated otherwise.

### Read

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("delta").load(path)` | ✅ Covered | `LogicalRelation` wraps `DeltaLog$$anon$2`, an anonymous `HadoopFsRelation` subclass matched by ordinary subtyping — see "Delta Lake reads" above. `SparkPlanAdapterSpec`, `ContractEnforcementRuleSpec` PASS/FAIL pair. |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM tbl`) | ✅ Covered | Same relation shape as above, confirmed for both forms — see "Delta Lake reads" above. |
| Time travel / snapshot reads (`versionAsOf`/`timestampAsOf`) | ✅ Covered | Probed empirically: produces the identical `LogicalRelation(relation=HadoopFsRelation)` shape as a plain read. Zero new code needed. |
| Streaming read (as a contract-declared *input*) | ✅ **Covered — closed this pass** | Previously a real false-positive gap: neither `StreamingRelation` (the legacy V1 path Delta itself uses — `.readStream.format("delta").load(path)` analyzes to this, not `StreamingRelationV2`) nor `StreamingRelationV2` (the modern DataSourceV2 path, used by `rate`/Kafka/similar) is a `LogicalRelation`, so `ContractEnforcementRule.verifyOrThrow`'s input-schema collection never saw either, and a contract declaring a streaming source as a required `input` always reported `MISSING_INPUT` even though data was genuinely being read. Closed by teaching both `SparkPlanAdapter`'s translation and `ContractEnforcementRule`'s input-schema collection to recognize both shapes, via two shared, non-duplicated helpers (`streamingRelationLocationOf`/`streamingRelationV2LocationOf`) rather than two independent matches — the exact duplication risk this module's write side already learned from. `StreamingRelation.dataSource.options("path")` and `sourceName` give location/format with no reflection needed (both are plain public spark-sql classes, unlike `WriteToStream`'s sink); `StreamingRelationV2.table` reuses the same `Table.properties()` lookup `AppendData`/`OverwriteByExpression` use below. Verified through real enforcement: a PASS/FAIL pair in `ContractEnforcementRuleSpec` proving a contract's declared input schema is genuinely checked against a real streaming Delta source. |
| Change-data-feed / incremental read (`readChangeFeed`) | ✅ Covered (with a precision caveat) | Probed empirically: produces `LogicalRelation(relation=CDCReader$$DeltaCDFRelation)`, a class distinct from `HadoopFsRelation` — but `translatePlan`'s generic `LogicalRelation` case (not the `HadoopFsRelation`-specific branch) already handles any relation type, producing a correct `ir.Read`. Because this relation has no populated `catalogTable` for a path-based read, it takes the existing "fallback" branch and reports a location diagnostic — the location string is the relation's `toString()`, not a clean physical path. Schema verification is unaffected; only location precision is reduced. **Next step:** none required for correctness; a future enhancement could special-case `DeltaCDFRelation` for a cleaner location string. |

### Write

| Operation | Status | Evidence / next step |
|---|---|---|
| `.save(path)`, all save modes | ✅ Covered | `SaveIntoDataSourceCommand` — see "Delta Lake support" above. |
| `.saveAsTable(...)`, new table | ✅ **Covered — closed this pass** | Non-Delta: `CreateDataSourceTableAsSelectCommand`, translated (see "Translation coverage" above). Delta: `.format("delta").saveAsTable(...)` on a *new* table analyzes to the V2 `ReplaceTableAsSelect`, now a real `WriteCommandSupport` entry. `tableSpec.provider` gives format directly; location is the target's qualified catalog identifier (`"spark_catalog.default.<table>"`), since a not-yet-existing table has no physical path to resolve at analysis time. Verified via a PASS/FAIL pair in `ContractEnforcementRuleSpec`. |
| `.saveAsTable(...)`, existing table (append) | ✅ **Covered — closed this pass** | Analyzes to `AppendData`, now a real `WriteCommandSupport` entry. Location prefers the resolved `DataSourceV2Relation`'s `Table.properties()["location"]` (the physical warehouse path, confirmed empirically) over its qualified identifier. Verified via a PASS/FAIL pair. |
| `.insertInto(...)` | ✅ **Covered — closed this pass** | Same `AppendData` shape, same `WriteCommandSupport` entry, same test. |
| `.writeTo(...)` (DataFrameWriterV2), all sub-ops | ✅ **Covered — closed this pass** | `.append()` → `AppendData`; `.overwrite(cond)` → `OverwriteByExpression`, mapped to the contract's `saveMode: overwrite` uniformly (the delete predicate itself isn't modeled — `StructuralVerifier`'s save-mode check doesn't need it, so no IR extension was needed, contrary to what was first assumed); `.createOrReplace()` → `ReplaceTableAsSelect`, same entry as the new-table `.saveAsTable()` row above. All verified via the same PASS/FAIL pairs. |
| Format-specific DML (`MERGE INTO`/`UPDATE`/`DELETE`) | ✅ **Covered — structurally, deliberately not semantically** | All three (`MergeIntoCommand`/`UpdateCommand`/`DeleteCommand`) confirmed empirically to be Delta-internal classes, matched by reflection (public `target()`/`catalogTable()`/`source()` methods, no compile-time Delta dependency) and recognized as real `WriteCommandSupport` entries. **What this checks:** the operation's *target* against the contract's declared output location and current schema (catching the wrong-table mistake and schema drift) — MERGE's `source` is additionally checked as a contract input. **What this deliberately does not check, and cannot yet:** the actual row-level logic — the merge condition, which columns an `UPDATE` touches, whether a `DELETE` is unconditional. There is no contract vocabulary for that (see docs/CONTRACT_MODEL.md's `rules` field — recorded, not interpreted). Verified through real enforcement: PASS/FAIL pairs for all three, including a FAIL proving the target-schema check and a separate FAIL proving the source-as-input check (which needed a real fix along the way — see below). Full semantic verification (a real `ir.Merge`/`ir.RowMutation` IR node plus contract rules to check it against) is tracked as deliberate future work in ROADMAP.md's "Full semantic DML verification" item, not attempted here — see docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means" for why building the IR node without the rules to consume it would be premature. |
| Streaming write | ✅ **Covered — closed this pass** | Previously the most serious gap found: `WriteToStream` (the streaming write's top-level plan) isn't `Command`-shaped, so `ContractEnforcementRule`'s fail-closed policy — which only gates `Command`-shaped plans — never saw it at all. Confirmed empirically (not assumed): a probe found zero of the plans `injectCheckRule` saw during a real streaming Delta write were `Command`-shaped, and `javap` on Spark's catalyst jar confirmed `WriteToStream` doesn't implement `Command`. This was categorically worse than every other row here: not "fails closed but unverified," but genuinely unenforced — a streaming write committed silently, with no contract check at all. **Closed by adding `WriteToStream` as a real `WriteCommandSupport` entry** (see "Write command recognition: a single registry" below) rather than special-casing it in the fail-closed check: `WriteToStream.inputQuery` gives the schema being written; location comes from a resolved `catalogTable` (`.toTable(...)`, confirmed to carry `storage.locationUri`/`provider`), or from the sink's `name()` when that doesn't throw (a genuine V2 sink), or — since Delta's `DeltaSink` is a legacy V1 `Sink` wrapper whose `name()`/`schema()` unconditionally throw, confirmed empirically — a reflective call to its public `path()` accessor, the same reflection-over-a-class-this-module-has-no-compile-time-dependency-on technique `jdbcLocationOf` already uses for `JDBCRelation`. Verified through real enforcement: a PASS/FAIL pair for `.start(path)`, a PASS test for `.toTable(...)` (the `catalogTable`-populated path), and a test confirming a streaming write to a location unrelated to the active contract is correctly rejected (`OUTPUT_LOCATION_MISMATCH`) — the same behavior batch writes have always had, not special-cased for streaming. All in `ContractEnforcementRuleSpec`. |
| Maintenance operations that touch data (`OPTIMIZE`/`VACUUM`/`RESTORE`/`CLONE`/`CONVERT TO DELTA`) | ✅ Covered by policy classification | `FailClosedCommands.scala`'s `knownSafe` set already includes `VacuumTableCommand`/`OptimizeTableCommand` (rewrites/removes files, doesn't change a table's committed row content) and deliberately excludes `RestoreTableCommand`/`CloneTableCommand`/`ConvertToDeltaCommand` (row-content-changing) — built from a class-by-class enumeration of all 164 `Command` subclasses across Spark 3.5.1 + Delta 3.2.0, reasoned and documented in that file's header comment. Not re-probed individually this pass (the classification predates it); if any one of these is ever doubted, a targeted probe test is cheap to add. |

### A shared pitfall: atomic CTAS/RTAS issues a second, nested write

Closing the `ReplaceTableAsSelect`/`AppendData` rows above surfaced a real
correctness trap, worth documenting for whichever row a future connector
adds next: a *single* `.saveAsTable(...)` call on a brand-new table
produces **two** separate write-shaped plans through `injectCheckRule`,
confirmed empirically — the top-level `ReplaceTableAsSelect` the user's
code actually wrote, *and* an internal, nested `AppendData` against a
`StagedTable` (Spark's own public 2-phase-commit protocol for atomic
CTAS/RTAS — Delta's `StagedDeltaTableV2` implements it). Both are
genuinely visible to `ContractEnforcementRule.verifyOrThrow`, meaning it
runs *twice* for what's one logical write.

The trap: a `StagedTable`'s `Table.properties()` has no `"location"` yet
(the table doesn't physically exist until commit), so a naive
`AppendData` translation would fall back to the table's bare, unqualified
`name()` — a *different* string than `ReplaceTableAsSelect`'s qualified
catalog identifier for the exact same destination. Whichever one a
contract's declared location happened to match, the other invocation
would report `OUTPUT_LOCATION_MISMATCH` and abort an otherwise
contract-satisfying write — caught by a real test failure, not by
inspection. Fixed by having `WriteCommandSupport`'s `AppendData`/
`OverwriteByExpression` case fall back to `DataSourceV2Relation`'s own
`catalog`/`identifier` fields (confirmed populated even for a staged
table) and compute the *same* qualified-identifier string
`ReplaceTableAsSelect`'s case does, via one shared helper
(`qualifiedIdentifier`) — the two now agree by construction, not by
coincidence. Any future connector adding a `WriteCommandSupport` case
for a command that can appear nested inside another (anything using
Spark's staging-catalog protocol) should check for this same trap.

### A second shared pitfall: `plan.collect` doesn't reach a leaf command's own fields

Closing the row-level DML row surfaced a second, distinct trap, just as
worth documenting: `ContractEnforcementRule.verifyOrThrow`'s input-schema
collection (`plan.collect { case lr: LogicalRelation => ... }`) walks the
analyzed plan's `children` — which works for every other write shape here
because their `query`/`target` are genuine children in the tree. Delta's
row-level DML commands are not: `MergeIntoCommand`/`UpdateCommand`/
`DeleteCommand` are effectively leaf nodes in the tree-traversal sense —
`source`/`target` are ordinary case-class fields, never exposed via
`children` — so `plan.collect` on the command itself finds nothing inside
it, confirmed empirically by a real FAIL test never throwing (asserted
`intercept[ContractViolationException]`, got none) rather than assumed to
"just work" the way it does everywhere else. Fixed by having
`ContractEnforcementRule.verifyOrThrow` also walk `WriteCommandSupport`'s
already-extracted `query` field (MERGE's `source`) in addition to the raw
plan — a real, independently traversable `LogicalPlan` unlike the outer
command. Any future connector whose write command hides its "real" query
behind a similarly leaf-shaped node should check for this same trap.

### Delta feature-by-feature confidence pass

The row-level DML ledger row above says "✅ Covered," but covering the
*write-command shape* isn't the same as having tried every Delta table
*feature* against it — those are orthogonal axes (a MERGE is one write
shape; whether its target has schema evolution, generated columns,
deletion vectors, column mapping, liquid clustering, or CHECK constraints
enabled is a property of the target, not the command). Before this pass,
that gap was implicit: "expected to work, never actually tried." This
pass tried each one against a real Delta table, not assumed from
documentation — two were real, found-and-fixed false-rejection bugs; four
are confirmed transparent with a permanent regression test; one is
confirmed untestable in this environment, not silently skipped.

- **Schema evolution (`MERGE` + `spark.databricks.delta.schema.autoMerge.enabled`)
  — real bug, fixed.** `target.schema` at analysis time is the
  *pre-merge* schema — confirmed empirically to not yet include columns
  schema evolution is about to add. A contract requiring a field a
  schema-evolving MERGE would legitimately add was previously rejected
  with `MISSING_OUTPUT_FIELD` for a write that would have satisfied it.
  `deltaRowLevelDml` now checks `MergeIntoCommand.schemaEvolutionEnabled()`
  (public, confirmed via `javap`) and unions the source's new fields into
  `target.schema` as a best-effort approximation — not a full simulation
  of Delta's evolution rules (type widening, nested-struct merging,
  column reordering aren't modeled), with a diagnostic making the
  approximation visible. The same fix also had to distinguish "the
  source legitimately has a field the commit will never write" from "the
  source has an evolved field": confirmed empirically (not assumed) that
  with `autoMerge` disabled, `INSERT *` silently drops a source column
  the target doesn't have — the MERGE succeeds, the table's schema never
  gains it — so `schemaEvolutionEnabled()` being false must keep
  `outputSchema` at `target.schema` alone, not the source's schema.
  `ContractEnforcementRuleSpec` has PASS tests for both directions: one
  proving the evolving case is no longer falsely rejected, and one
  proving a non-evolving MERGE with a source-only extra column doesn't
  falsely *pass* a `rejectUndeclaredFields: true` contract (this second
  test exists specifically because it's the one that kills the mutant a
  naive `isMerge` check-only implementation would leave alive — see
  "Mutation testing" below).

- **Generated columns (`GENERATED ALWAYS AS (...)`) — real bug, fixed.**
  The same class of false-rejection: a generated column is computed by
  Delta at commit time, never supplied by the writer, so
  `AppendData`/`OverwriteByExpression`'s `outputSchema` (previously always
  `cmd.query.schema`) never included it. Confirmed empirically, the hard
  way, that this can't be detected from any DataFrame-facing schema at
  all — not `spark.read.format("delta").load(path).schema`, not
  `spark.table(name).schema`, not even the DSv2 `Table` handle's own
  `.schema()` (confirmed specifically for `DeltaTableV2.schema()`, the
  exact handle these two write shapes resolve their target to) carries
  `delta.generationExpression` metadata key Delta itself sets on a
  generated column's `StructField`. Only Delta's internal
  `Snapshot.schema()` does (reached via `DeltaTableV2.initialSnapshot()`,
  itself reached via the write's own `DataSourceV2Relation.table`).
  `outputSchemaWithGeneratedColumns`/`deltaGeneratedFields` read that
  reflectively (same no-compile-time-dependency, `Try`-wrapped convention
  as `deltaRowLevelDml`) and union the target's generated-only columns
  into `outputSchema` — checking the metadata key directly rather than
  reflecting into Delta's own `GeneratedColumn.isGeneratedColumn` helper,
  since `StructField.metadata` is already a plain public Spark type on
  this module's main classpath. Verified with a PASS test using the
  `io.delta.tables.DeltaTable` builder API (raw `CREATE TABLE ...
  GENERATED ALWAYS AS (...)` SQL DDL fails outright in this Spark 3.5.1 +
  Delta 3.2.0 environment with `[UNSUPPORTED_FEATURE.TABLE_OPERATION]`,
  confirmed empirically, even with explicit `TBLPROPERTIES` for
  reader/writer version — the builder API was the only way found to
  actually create one here) — a contract requiring the generated column
  is satisfied by an append that never supplies it.

  **Superseded**: `outputSchemaWithGeneratedColumns`/`deltaGeneratedFields`
  above were later replaced by a connector-agnostic, reflection-free
  mechanism once an Iceberg investigation found the same underlying
  situation under a different mechanism — see "Generalizing the
  generated-columns fix" in the Iceberg support section below. The
  finding stands (this is *why* the fix was needed); the implementation
  described here no longer exists in the code.

- **Deletion vectors, column mapping mode (`'name'`), liquid clustering
  (`CLUSTER BY`) — confirmed transparent, no fix needed.** Real writes
  and DML against tables with each of these enabled are recognized by
  `WriteCommandSupport` exactly as they would be without the feature —
  correct location, correct schema, no diagnostics. `ContractEnforcementRuleSpec`
  has a permanent PASS test for each, replacing what was previously only
  throwaway probe evidence.

- **CHECK constraints — confirmed orthogonal, no fix needed.** Delta
  enforces these itself, independently, at commit time — Invariant has no
  rule vocabulary for a row-level condition like `CHECK (id >= 0)` (see
  docs/CONTRACT_MODEL.md's `rules` field). A write violating a CHECK
  constraint is recognized by `WriteCommandSupport` identically to a
  satisfying one (no diagnostic — Invariant's structural checks simply
  don't apply here), and is then independently rejected by Delta's own
  `DeltaInvariantViolationException` before commit — confirmed with a
  permanent test asserting both halves: Invariant raises nothing, Delta
  does.

- **Identity columns (`GENERATED ALWAYS AS IDENTITY`) — confirmed
  untestable in this environment, not investigated further.** Spark
  3.5.1's own SQL parser rejects the syntax outright
  (`[PARSE_SYNTAX_ERROR] ... extra input 'IDENTITY'`), confirmed via a
  dedicated probe with no `try`/`catch` that could have masked a
  different failure. This is very likely a Databricks Runtime-only SQL
  extension not present in vanilla OSS Spark 3.5.1's grammar at all, not
  a Delta or Invariant limitation — there is nothing for
  `WriteCommandSupport` to translate because Spark itself never produces
  an analyzed plan to see. Left as ❓ **Not investigated** rather than
  claimed as covered.

**Net assessment:** Delta is not "100% supported" and no single pass makes
it so — but the gap is now fully enumerated instead of implicit, and every
row that isn't ✅ Covered states exactly what would close it, not just
that it's currently rejected — and today, every row *is* ✅ Covered.
Two rows that were genuine, unenforced holes
(streaming writes, and streaming reads as a declared input — the
difference between "fails closed" and "silently unchecked"/"falsely
rejected" is exactly the distinction this ledger exists to keep visible)
are now ✅ Covered, alongside every V2 catalog write shape
(`AppendData`/`OverwriteByExpression`/`ReplaceTableAsSelect`) and
row-level DML (`MERGE`/`UPDATE`/`DELETE`) — the last of these
*structurally* rather than semantically, deliberately: verifying the
actual merge condition/update columns/delete predicate needs both a real
IR extension and contract rules to check it against, neither of which
exist yet, and building the IR half alone would be speculative API
surface nothing could consume. That's real, scoped future work, tracked
explicitly in ROADMAP.md's "Full semantic DML verification" item, not a
silent gap.

## Iceberg support

Added via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md), investigated against a real
Iceberg-enabled session
(`spark.sql.extensions=...IcebergSparkSessionExtensions`,
`spark.sql.catalog.local=...SparkCatalog` with `type=hadoop`, no external
metastore needed for local/test use). Pinned to
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0` — a checked
compatibility issue before pinning, per Phase 0's "any known
compatibility issues" step: 1.10.0 had a confirmed `NoSuchMethodError`
from an Avro 1.11/1.12 API mismatch against Spark 3.5/3.4
([apache/iceberg#14232](https://github.com/apache/iceberg/issues/14232)),
fixed via an Avro 1.12.1 upgrade that landed before 1.11.0. Test-scope
only, same reasoning as `delta-spark` above — no compile-time or runtime
dependency for a non-Iceberg job.

**A JDK-11 CI-only gap found later, not at onboarding time**: this
version's jar is compiled to class file version 61 (Java 17) —
confirmed via a real CI failure (`UnsupportedClassVersionError` on
`SparkCatalog`/`IcebergSource`) on this repo's `ubuntu-latest`/Java-11
matrix leg, and — since this module's suites share one forked JVM —
cascaded into three unrelated suites in the same run, not just
`IcebergConnectorSpec`. A genuine, external constraint of the library
(unlike Delta 3.2.0 above, which loads fine under JDK 11), not fixable
here. `spark-adapter/build.sbt` excludes only `IcebergConnectorSpec`,
only under JDK <17 (`Test / testOptions`'s `Tests.Filter`) — every other
test, in this module and every other, still runs on JDK 11. The
module's own compiled bytecode target (`-target:jvm-1.8`) is unaffected;
this is purely a test-only dependency's own runtime floor.

Iceberg is a "pure" DataSourceV2 connector — its catalog (`SparkCatalog`/
`SparkSessionCatalog`) is the same *kind* of thing Delta's `DeltaCatalog`
is, but unlike Delta, Iceberg's *reads* never happen to be a
`LogicalRelation`-wrapped V1 relation the way Delta's `DeltaLog$$anon$2`
is. That difference is what made this investigation surface two real
gaps that predate Iceberg entirely — general DSv2-connector gaps Delta's
own investigation never hit, because Delta's read/write shapes happened
to avoid them:

- **Batch `DataSourceV2Relation` reads had no translation case at all.**
  `SparkPlanAdapter.Translator.translatePlan` had cases for
  `LogicalRelation` (V1 batch), `StreamingRelation` (V1 streaming), and
  `StreamingRelationV2` (V2 streaming) — nothing for a *batch* V2 catalog
  read, so it fell through to the generic `Unsupported` fallback.
  Confirmed by direct code inspection (not assumed): `StructuralVerifier.collectReads`
  only recognizes `ir.Read` nodes, so any pure-V2 connector's catalog
  reads could never satisfy a contract's declared input — the same class
  of bug streaming reads had before that was fixed (see "A read
  discovered through Delta's own investigation" note below), just for a
  batch read this time. Fixed by adding a `DataSourceV2Relation` case
  reusing `tableLocationAndFormat` (the same `Table.properties()` lookup
  `StreamingRelationV2`'s case and the write side's `AppendData`/
  `OverwriteByExpression` cases already share) — connector-agnostic, not
  Iceberg-specific. `ContractEnforcementRule.verifyOrThrow`'s input-schema
  collection needed the matching case too (both were previously
  duplicated three times across two `plan.collect` sites; adding a fourth
  case to both by hand was the moment that duplication got extracted into
  one shared `recognizedRead: PartialFunction`, reused by both sites, the
  same "one source of truth" reasoning `WriteCommandSupport.combined`
  already applies to write recognition).
- **`CreateTableAsSelect` (explicit-create V2 CTAS) and
  `OverwritePartitionsDynamic` had no `WriteCommandSupport` case.**
  `.writeTo(...).create()` (fails if the table exists, unless
  `ignoreIfExists` — `CREATE TABLE IF NOT EXISTS ... AS SELECT`) is a
  *different* Spark command than `.saveAsTable()`/`.writeTo(...).createOrReplace()`
  (`ReplaceTableAsSelect`, already covered) — genuinely never probed for
  any connector before, Delta included, since nothing had tried a bare
  `.writeTo(...).create()`. Both were Command-shaped, on neither
  `WriteCommandSupport` nor `FailClosedCommands`' safe list, so both were
  already failing closed rather than silently passing — real gaps, now
  closed for every DSv2 connector at once (`createTableAsSelect` reuses
  `ReplaceTableAsSelect`'s own location-resolution helper, renamed
  `v2CreateOrReplaceLocation`; `overwritePartitionsDynamic` reuses
  `AppendData`/`OverwriteByExpression`'s `namedRelationLocationAndFormat`
  directly, since `OverwritePartitionsDynamic` shares the identical
  `NamedRelation table` + `LogicalPlan query` shape via the same
  `V2WriteCommand` supertype).
- **Row-level DML (`MERGE`/`UPDATE`/`DELETE`) goes through a genuinely
  different, more standard mechanism than Delta's.** Confirmed
  empirically: Iceberg implements Spark's own `SupportsRowLevelOperations`
  API, and Spark's `RewriteRowLevelOperation` optimizer-rule family
  rewrites `MERGE`/`UPDATE`/`DELETE` into one of two *stable, public*
  Spark classes — `ReplaceData` (copy-on-write) or `WriteDelta`
  (merge-on-read) — both implementing the shared `RowLevelWrite` trait
  (itself extending `V2WriteCommand`, the same shape `AppendData` uses).
  This is a real, structural difference from Delta's proprietary
  `MergeIntoCommand`/`UpdateCommand`/`DeleteCommand` classes, which
  needed reflection (`deltaRowLevelDml`) precisely because they're
  Delta-internal, undocumented, no-cross-version-guarantee types. The new
  `dsv2RowLevelWrite` case needs no reflection at all — `RowLevelWrite`
  is a real, importable Spark type — making it a genuinely
  connector-agnostic case: any future DSv2 connector using Spark's
  standard row-level-operation API is covered by this same case, not a
  per-connector copy, unlike `deltaRowLevelDml`. Scope mirrors
  `deltaRowLevelDml` deliberately: structural verification only (the
  target's *current* schema against the contract; the merge
  condition/update columns/delete predicate itself is not checked — see
  ROADMAP.md's "Full semantic DML verification" item).
- **A second staged-table trap, this time in the opposite direction from
  Delta's.** `.writeTo(...).create()`/`.saveAsTable()` on a new table
  produces both the outer `CreateTableAsSelect`/`ReplaceTableAsSelect`
  *and* a nested `AppendData` against a `StagedTable` (Spark's public
  2-phase-commit protocol for atomic CTAS/RTAS) — the same "shared
  pitfall" documented above for Delta, whose fix's own doc comment
  claimed the two "now always agree... not by coincidence." That claim
  didn't generalize: confirmed by a real `OUTPUT_LOCATION_MISMATCH` test
  failure, not assumed, that Iceberg's `StagedTable` *does* report a
  `"location"` property pre-commit (Delta's doesn't), so the outer
  command's qualified-identifier resolution and the inner `AppendData`'s
  physical-path resolution disagreed. Fixed properly this time: keyed on
  the `StagedTable` marker interface itself (Spark's own "not committed
  yet" signal), not on whether `properties()` happens to omit
  `"location"` — a staged table's reported location isn't trustworthy
  regardless of whether a given connector happens to populate it, so
  `namedRelationLocationAndFormat` now forces the qualified-identifier
  tier unconditionally whenever `v2.table.isInstanceOf[StagedTable]`,
  before even consulting `tableLocationAndFormat`.
- **`CALL <catalog>.system.<proc>(...)` (Iceberg's maintenance-operation
  mechanism — `rewrite_data_files`/`expire_snapshots`/
  `rollback_to_snapshot`/etc.) is deliberately left unmodeled.** All of
  Iceberg's own SQL-extension commands are `Command`-shaped classes
  found via the same reflective jar-scan technique used for Delta
  (`JarFile` + `Class.forName` + `Command.isAssignableFrom`, this time
  against `iceberg-spark-runtime-3.5_2.12` — 14 classes found). Thirteen
  are genuinely metadata/ref-only (branch/tag create-or-replace/drop,
  partition-spec and identifier-field evolution, write-distribution/
  ordering config, view create/drop/show) and are now on
  `FailClosedCommands`' safe list. The fourteenth, `Call`, represents
  *every* system procedure through one shared class — no structural way
  to tell which procedure a given instance invokes without inspecting
  runtime arguments, and those procedures span genuinely safe
  (`expire_snapshots`) to genuinely row-content-mutating
  (`rollback_to_snapshot`). Safe-listing the class would silently pass
  all of them; left off both lists instead, so every `CALL` fails closed
  today, confirmed by a real test — a real, documented limitation (see
  docs/ADDING_A_SPARK_CONNECTOR.md's "Known limitations"), not an
  oversight.
- **`iceberg-spark-runtime-3.5_2.12:1.11.0`'s own published Maven POM is
  missing a real dependency.** Confirmed empirically, the hard way (a
  `NoClassDefFoundError: scala/jdk/CollectionConverters$` escaping a
  `scala.util.Try` wrapper — `LinkageError` isn't `NonFatal`, so it isn't
  caught): the runtime jar's SQL-extensions parser
  (`IcebergSparkSqlExtensionsParser.isIcebergProcedure`, exercised
  specifically by `CALL` syntax) needs `scala.jdk.CollectionConverters`
  (native in Scala 2.13, backported for 2.12 via
  `scala-collection-compat`), but the jar's own POM doesn't declare that
  dependency. Added as a `% "test"` dependency here, purely so this
  module's own test suite can exercise `CALL`-based operations against a
  real session — a real Invariant user running Iceberg `CALL` procedures
  in their own job would need this on their own runtime classpath too,
  independent of anything `spark-adapter` does.

### Delta feature-by-feature confidence pass, revisited: a read discovered through Delta's own investigation

Streaming reads (`StreamingRelation`/`StreamingRelationV2`) were already
closed before this Iceberg pass — see the Delta operation-surface ledger
above. The batch `DataSourceV2Relation` gap this section opens with is
the same *class* of bug (a read shape with no translation case, silently
never satisfying a contract's `input`), just for the one read shape
Delta's own investigation never exercised, because Delta's batch reads
happen to be `LogicalRelation`-wrapped. Documented here, not folded
silently into the Delta section above, specifically so a future
connector's investigation can search for "batch DataSourceV2Relation"
and find this, rather than rediscovering the same gap a third time.

### Iceberg CALL procedure classification

A follow-up pass targeting the one row the initial Iceberg investigation
left `🚫 Fails closed, deliberately left unmodeled`: `CALL
<catalog>.system.<proc>(...)` procedures. Every procedure — safe and
unsafe alike — analyzes to the *same* Spark class,
`org.apache.spark.sql.catalyst.plans.logical.Call`, so `FailClosedCommands`'
usual technique (match the plan's own class name) can't distinguish them.

**What actually identifies a procedure at the point `injectCheckRule`
sees it**: not `Call`'s own class, and not a name string — the
pre-analysis `CallStatement` node carries a `name: Seq[String]` (e.g.
`Seq("system", "rewrite_data_files")`), but that node is gone by the time
the analyzer resolves it into `Call`, which the check rule only ever sees
post-analysis. What survives is `Call.procedure(): Procedure` — and
`Procedure` itself (confirmed via `javap`) exposes no `name()` either.
But each procedure is a genuinely distinct concrete class (confirmed via
`javap` on `SparkProcedures`' builder registry — `newBuilder("rewrite_data_files")`
instantiates the real `RewriteDataFilesProcedure` class directly, no
dynamic proxy), so `Call.procedure().getClass().getName()` is a stable,
real per-procedure signal — the same reflection-and-class-name-matching
technique `WriteCommandSupport`'s `deltaRowLevelDml` already uses for
Delta's MERGE/UPDATE/DELETE, just one field deeper. `FailClosedCommands.isKnownSafe`
special-cases `Call`'s class name and delegates to this check; any
reflection failure (a future Iceberg version reshaping `Call`) falls back
to `false` — fails closed, never silently safe.

**Classification** (iceberg-spark-runtime 1.11.0's `org.apache.iceberg.spark.procedures`
package has exactly 20 concrete procedure classes — enumerated from the
jar directly, not guessed), grounded in each procedure's own delegate
action class (confirmed via `javap`) and Iceberg's own documentation for
the ones whose semantics weren't obvious from the class name alone:

| Procedure | Disposition | Reasoning |
|---|---|---|
| `rewrite_data_files` | ✅ Safe-listed | Storage compaction, preserves the same logical rows — same category as Delta's `OptimizeTableCommand`, already safe-listed |
| `rewrite_manifests` | ✅ Safe-listed | Metadata-file compaction, never touches data files |
| `rewrite_position_delete_files` | ✅ Safe-listed | Compacts delete files, preserves the same logical deletes |
| `remove_orphan_files` | ✅ Safe-listed | Deletes files unreferenced by any live metadata — same category as Delta's `VacuumTableCommand`, already safe-listed |
| `expire_snapshots` | ✅ Safe-listed | Removes old snapshots/unreachable files, doesn't touch the *current* snapshot |
| `register_table` | ✅ Safe-listed | Catalog-pointer registration from an existing `metadata.json` — no data touched |
| `ancestors_of` | ✅ Safe-listed | Read-only snapshot-lineage introspection |
| `compute_table_stats` | ✅ Safe-listed | Writes auxiliary CBO statistics only, same category as `ANALYZE TABLE` |
| `compute_partition_stats` | ✅ Safe-listed | Same reasoning, partition-scoped |
| `create_changelog_view` | ✅ Safe-listed | Creates a read-only temp view, mutates nothing |
| `rollback_to_snapshot` | 🔧 **Genuinely verified — closed this pass** | See "Verifying `rollback_to_snapshot`" below — no longer blanket-rejected; checked against the contract for real. |
| `rollback_to_timestamp` / `set_current_snapshot` | 🚫 Left unmodeled | Same effect as `rollback_to_snapshot` (changes which snapshot is "current") but not yet wired to `StateChangingCallSupport` — a small, mechanical extension of the same pattern, not attempted this pass |
| `cherrypick_snapshot` / `publish_changes` | 🚫 Left unmodeled | Applies a staged (WAP) snapshot's changes into the current table — content-affecting |
| `fast_forward` | 🚫 Left unmodeled | Advances a branch's current snapshot — content-affecting for that branch's readers |
| `add_files` | 🚫 Left unmodeled | Imports external files as new data — genuinely adds rows |
| `migrate` | 🚫 Left unmodeled | Converts an existing table's format to Iceberg in place — structurally significant, one-time, not routine maintenance |
| `snapshot` | 🚫 Left unmodeled | Iceberg's own docs confirm it never modifies the *source* table, but it creates an entirely new table from data — new persisted content the safe-list mechanism (a silent, unverified no-op) shouldn't quietly wave through |
| `rewrite_table_path` | 🚫 Left unmodeled | Same reasoning as `snapshot` — doesn't touch the source table, but stages new metadata elsewhere; kept conservative rather than silently trusted |

Verified against real CALL statements, not asserted: `IcebergConnectorSpec`'s
`cherrypick_snapshot` fail-closed test (proving a genuinely unsafe,
still-unmodeled procedure is still rejected — `rollback_to_snapshot` is no
longer the example used here, since it now has real support; see
"Verifying `rollback_to_snapshot`" below) and a regression test sampling
five of the ten safe-listed procedures (`rewrite_data_files`,
`rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`,
`ancestors_of`) across compaction, GC, and read-only introspection —
proving they run under an active, otherwise-checking contract instead of
being wrongly rejected.

Mutation testing scoped to `FailClosedCommands.scala`: **88.42%** (of
total), 89.36% (of covered code) — comfortably above the 70% bar.
Mutation testing itself found a real gap along the way, not just
confirmed the score: `isKnownSafeIcebergProcedureCall`'s reflection
fallback (`case _: Throwable => false` — the line that keeps a future
Iceberg version reshaping `Call` failing closed, rather than silently
resolving to "safe") had zero coverage from `IcebergConnectorSpec`'s
real-session tests, since a genuine Iceberg `Call`'s `procedure()`
reflection never actually fails there. Closed with a focused,
session-free unit test (`FailClosedCommandsSpec`, no `SparkSession`
needed) that calls the fallback directly with a plan lacking a
`procedure()` method — killing the mutant for real, not just documenting
around it, since this is the single highest-stakes line in the whole
change (a wrongly-flipped `true` here would silently defeat the entire
feature for any future Iceberg version).

**What this still does not do**: verify the four still-unmodeled
procedures' actual effect against a contract. Six procedures that only
ever move which snapshot is current, never touching a table's schema
(`rollback_to_snapshot`/`rollback_to_timestamp`/`set_current_snapshot`/
`cherrypick_snapshot`/`publish_changes`/`fast_forward`) are done (see
below); `add_files`/`migrate`/`snapshot`/`rewrite_table_path` need a
schema read from a table/path named in a CALL *argument*, which needs
argument parsing/binding this codebase has never done before — rather
than fitting `WriteCommandSupport`'s existing "translate a Spark write"
model.

### Verifying `rollback_to_snapshot`

A pilot, not the full set: the first of the ten unmodeled state-changing
procedures to get real verification instead of a blanket rejection,
scoped deliberately narrow to prove the approach before generalizing (see
ROADMAP.md for the original scoping decision).

**The first design was wrong, and mutation testing caught it, not
inspection.** The initial implementation extracted the *target snapshot's
own* historical schema — via Iceberg's `SparkTable.copyWithSnapshotId(id).schema()`,
confirmed reachable via a real probe (since deleted) that showed the
extracted schema correctly omitting a column added *after* that snapshot
— and checked that against the contract. This turned out to verify
something that doesn't correspond to what the operation actually does:
a real end-to-end test (rollback, then an explicit `refreshTable`) showed
the table's schema *unchanged* — still reflecting the latest evolution,
not the target snapshot's. Independently corroborated by Apache Iceberg's
own issue tracker (apache/iceberg#15165, open and unresolved as of this
writing): a maintainer's own report says `current-schema-id` "always
points to the latest schema" regardless of which snapshot a rollback
makes current. Schema evolution and snapshot rollback are independent in
Iceberg's model — a rollback can never change a table's schema at all, so
comparing against the target snapshot's own schema was answering a
question with no real referent.

**The corrected design** checks the two things a rollback can actually
be judged against, together:

- **Location** — does this CALL even target the contract's declared
  output? Confirmed as a *scoping* gate, not a violation: a state-changing
  operation on a table the active contract doesn't govern is now
  correctly allowed, not swept up by an unrelated contract the way every
  rollback was before this file existed (`StructuralVerifier.verifyStateChange`
  returns a clean pass, skipping the schema check entirely, when location
  doesn't match — deliberately different from `verify`'s `Write` case,
  where a wrong-location write IS a violation, since something was
  genuinely written to the wrong place there).
- **Current schema** — since the operation can't change it, checking the
  table's schema *right now* (via the plain, connector-agnostic
  `Table.columns()`, not Iceberg's `SparkTable`) is exactly checking what
  will still be true afterward. Catches a table already out of compliance
  (e.g. an out-of-band schema change) at the point of a state-changing
  operation on it.

Extraction needs only two reflective hops now (down from three) —
`BaseProcedure.tableCatalog()` (protected, `setAccessible`) and
`TableCatalog.loadTable(Identifier)` (fully public) — since the
snapshot-specific `copyWithSnapshotId` hop is no longer needed at all.

**Verified with real tests, including a real bug the tests themselves
caught**: `IcebergConnectorSpec`'s PASS test (current schema satisfies
the contract → allowed), FAIL test (current schema doesn't → rejected
before touching the table), and a location-scoping test (targets a table
the active contract doesn't govern → allowed). The location-scoping
test's first version passed even though a mutation-testing survivor
proved it wasn't actually distinguishing "scoping worked" from "the
contract happened to be lenient enough to pass anyway" — its contract
only required an already-present, optional field. Fixed by requiring a
field the unrelated table genuinely lacks, so a real schema check (if
scoping were broken) would provably fail. Mutation testing on the three
changed files (`StateChangingCallSupport.scala`, `StructuralVerifier.scala`,
`ContractEnforcementRule.scala`): **100%** (51/51) after that fix — up
from 96.08% (2 survivors) beforehand. `mimaReportBinaryIssues` clean.

### Extending to the five procedures that share `rollback_to_snapshot`'s shape

Generalized the pilot to `rollback_to_timestamp`, `cherrypick_snapshot`,
`publish_changes`, `set_current_snapshot`, and `fast_forward` — every
remaining procedure whose only effect is moving which snapshot is
current, per the "small, mechanical extension" scoping ROADMAP.md set out
when the pilot was first sequenced.

**Investigated before generalizing, not assumed from the pilot alone.**
`javap` against the real `iceberg-spark-runtime-3.5_2.12:1.11.0` jar
confirmed each procedure's declared parameter list, followed by a real
probe (since deleted) exercising each one against a live Iceberg session
to confirm the actual `Call.args` shape and observable table-state
effect — not just the declared parameter names:

- `rollback_to_timestamp` and `cherrypick_snapshot` and `publish_changes`
  each declare exactly two parameters (table, plus a timestamp/snapshot-id/
  WAP-id respectively) — the identical two-argument shape
  `rollback_to_snapshot`'s own extraction already handled, confirmed via
  probe that none of the three can touch a table's schema and that
  `cherrypick_snapshot`/`publish_changes` both move `main`'s current
  snapshot (confirmed via row counts before/after).
- `set_current_snapshot` declares **three** parameters (table,
  `snapshot_id`, `ref`) — `snapshot_id` and `ref` are mutually exclusive,
  confirmed via probe that the real `Call.args` array is always
  3-wide with exactly one of `args(1)`/`args(2)` a non-null `Literal`
  depending on which was supplied. Neither value is read by the
  extraction, the same way `rollback_to_snapshot`'s own snapshot id
  isn't — only `args.head` (the table) is ever needed, and Iceberg's own
  analyzer (`ResolveProcedures`) already guarantees a well-formed,
  fully-bound `args` array by the time this check rule sees the plan, so
  no further shape validation was added.
- `fast_forward` (`FastForwardBranchProcedure`) declares three parameters
  too (table, `branch` — the branch being moved, `to` — the source ref) —
  genuinely different from the other five: confirmed via probe that
  fast-forwarding `"main"` changes the table's default read (row count
  changed), while fast-forwarding any *other* named branch leaves the
  default read completely unchanged. This looked at first like it might
  need special-casing (only check when `branch == "main"`), but doesn't:
  the existing check only asserts an invariant — current schema can't
  move — that holds regardless of which branch a given call targets, not
  a claim about what that specific call changes. A dedicated test proves
  this concretely rather than leaving it as a documentation-only claim:
  fast-forwarding a non-`"main"` branch is still rejected when the
  table's current schema violates the contract, even though that
  specific call's own effect never touches `main` either way.

**Implementation generalized cleanly, not by branching per procedure.**
`StateChangingCallSupport`'s single hardcoded `RollbackToSnapshotProcedure`
class-name check became a `Map[String, String]` from procedure class name
to its CALL-syntax name (used only for the error message via a new
`StateChangeInfo.callName` field) — the extraction logic itself,
verification logic, and `ContractEnforcementRule` wiring are all
unchanged and already generic; no code duplicated per procedure.

**Tests**: one PASS test per newly-recognized procedure in
`IcebergConnectorSpec` (`rollback_to_timestamp`, `cherrypick_snapshot`,
`publish_changes`, `set_current_snapshot` — both its `snapshot_id`-form
and `ref`-form arg bindings — `fast_forward` on `"main"`), a FAIL test
for `rollback_to_timestamp` proving the generalized FAIL path still
works, and the `fast_forward`-on-a-non-`"main"`-branch FAIL test
described above. The old "`cherrypick_snapshot` still fails closed" test
(no longer true) was replaced with an `add_files` fail-closed test, since
`add_files` remains genuinely unmodeled.

Mutation testing scoped to the two changed files: `StateChangingCallSupport.scala`
**80%** (4/5) — the sole survivor is a genuinely equivalent mutant on the
`Call`-class-name guard (mutated to always-true), defended by the
surrounding `try`/`catch`: any real non-`Call` plan lacks a `procedure()`
method, so the outer catch produces an identical `None` either way,
regardless of whether the guard ran — the same category of near-equivalent
guard mutant already accepted for this exact pattern during the original
pilot. `ContractEnforcementRule.scala`: **100%** (10/10).
`mimaReportBinaryIssues` clean (both touched types remain
`private[sparkadapter]`). Full `./dev/build`/`./dev/test`/`./dev/regression`
pipeline passing.

### Iceberg operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.load(path)` (bare filesystem path, unqualified) | 🚫 **Not Iceberg's supported access pattern** | Confirmed empirically: a bare `.format("iceberg").save(path)`/`.load(path)` with no catalog qualification fails hard (`NoClassDefFoundError` reaching for a default `HiveCatalog` no metastore client is on the classpath for), regardless of named-catalog config. Not a gap to close — Iceberg's real path-based mechanism is a path identifier *under* a named Hadoop-type catalog (`` local.`/abs/path` ``, confirmed working, covered by the `AppendData`/`DataSourceV2Relation` rows below). |
| Catalog table read (batch) | ✅ **Covered — closed this pass** | New `DataSourceV2Relation` case in `SparkPlanAdapter`, connector-agnostic. `IcebergConnectorSpec`'s "translates a batch Iceberg catalog read" test, plus a direct-construction no-location fallback test in `SparkPlanAdapterSpec`. |
| Time travel / snapshot reads (`VERSION AS OF`, `snapshot-id` option) | ✅ Covered | Both resolve to the same `DataSourceV2Relation` shape as an ordinary catalog read (confirmed via probe — the time-travel option doesn't change the node type), so the new case covers them without separate handling. No dedicated permanent test beyond the plain catalog-read one — the shape is identical. |
| Streaming read (`readStream`) | ✅ Covered | `StreamingRelationV2`, already generic (closed during the Delta pass). Confirmed for Iceberg via `IcebergConnectorSpec`'s streaming `.toTable()` test (exercises both the read and write halves together). |
| Change-data-feed / incremental read (`start-snapshot-id`) | ✅ Covered | Same `DataSourceV2Relation` shape as time travel — confirmed via probe, no separate case needed. |
| `.save(path)` (bare filesystem path) | 🚫 **N/A — see the `.load(path)` row above** | Same finding, write side. |
| `.saveAsTable(...)`, new table | ✅ Covered | `ReplaceTableAsSelect`, already generic (closed during the Delta pass) — confirmed for Iceberg via probe (`.saveAsTable()` on a new table maps to `ReplaceTableAsSelect` under a V2 catalog, same as Delta). |
| `.saveAsTable(...)`, existing table (append) | ✅ Covered | `AppendData`, already generic. `IcebergConnectorSpec`'s PASS/FAIL pair. |
| `.insertInto(...)` | ✅ Covered | Same `AppendData` shape, confirmed via probe. |
| `.writeTo(...).append()`/`.overwrite(cond)` | ✅ Covered | `AppendData`/`OverwriteByExpression`, already generic. |
| `.writeTo(...).overwritePartitions()` | ✅ **Covered — closed this pass** | New `OverwritePartitionsDynamic` case, connector-agnostic. `IcebergConnectorSpec`'s PASS test. |
| `.writeTo(...).create()` | ✅ **Covered — closed this pass** | New `CreateTableAsSelect` case, connector-agnostic. `IcebergConnectorSpec`'s PASS/FAIL pair plus a direct-inspection `saveMode` test. |
| `.writeTo(...).createOrReplace()`/`.replace()` | ✅ Covered | `ReplaceTableAsSelect`, already generic — confirmed via probe. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | ✅ **Covered — structurally, closed this pass** | New `dsv2RowLevelWrite` case (`ReplaceData`/`WriteDelta`, via the standard `RowLevelWrite` trait — no reflection, unlike Delta's `deltaRowLevelDml`), connector-agnostic. Same structural-only scope as Delta's row-level DML row. `IcebergConnectorSpec`'s PASS/FAIL pair for MERGE, plus PASS tests for UPDATE/DELETE. |
| Streaming write (`writeStream`) | ✅ Covered | `WriteToStream`, already generic (closed during the Delta pass) — confirmed for Iceberg via `IcebergConnectorSpec`'s streaming `.toTable()` test, using the `catalogTable`-populated fallback tier (Iceberg's streaming sink didn't need the reflective `DeltaSink`-style fallback Delta's did). |
| Maintenance operations that touch data (`CALL system.*` procedures) | 🔀 **Partially covered — extended this pass** | Procedure-name-aware classification (see "Iceberg CALL procedure classification" above): 10 of 20 procedures (storage/metadata compaction, GC of unreferenced files/snapshots, catalog registration, stats, read-only introspection) run as verified no-ops. Six procedures — `rollback_to_snapshot`, `rollback_to_timestamp`, `cherrypick_snapshot`, `publish_changes`, `set_current_snapshot`, `fast_forward` (see "Verifying `rollback_to_snapshot`" and "Extending to the five procedures..." below) — are now genuinely verified, not a no-op and not a blanket rejection, via a catalog-level current-schema-plus-location check that holds for all six regardless of how each one moves what's current, closing 16 of 20. The remaining 4 (`add_files`/`migrate`/`snapshot`/`rewrite_table_path`) still fail closed, deliberately: each needs a materially different mechanism — parsing/binding a schema from a table or path named in the CALL's own arguments, not just its target — not attempted this pass. `IcebergConnectorSpec`'s PASS/FAIL/scoping tests for all six genuinely-verified procedures, an `add_files` fail-closed test, and a safe-procedures regression test (5 of the 10 no-ops) prove all three tiers. |
| Iceberg's own metadata/ref DDL (branch/tag/partition-spec/identifier-fields/write-ordering/views) | ✅ Covered by policy classification | 13 classes added to `FailClosedCommands`' safe list, reasoning in that file's own comment. `IcebergConnectorSpec`'s regression test proves none are blocked under an active, otherwise-rejecting contract. |

### Iceberg feature-surface coverage ledger

Iceberg's own distinguishing behaviors beyond its write-command shapes -
per docs/ADDING_A_SPARK_CONNECTOR.md's "The feature surface", a
connector-specific list, not the fixed operation-surface template above.

| Feature | Status | Evidence / next step |
|---|---|---|
| Copy-on-write vs. merge-on-read row-level operations | ✅ Confirmed | Both modes rewrite to the same `RowLevelWrite`-family shape (`ReplaceData` for copy-on-write, `WriteDelta` for merge-on-read) — `dsv2RowLevelWrite` matches the shared trait, so either mode is recognized identically. `IcebergConnectorSpec`'s MERGE/UPDATE/DELETE tests exercise Iceberg's default mode; not separately tested per mode (the mechanism is provably mode-agnostic by construction — it matches the trait both extend, not either concrete class). |
| Staged-table location reporting differs from Delta's | 🔧 **Found and fixed** | See "A second staged-table trap" above. Fixed by keying the location-resolution fallback on the `StagedTable` marker interface itself, not on whether a `"location"` property happens to be absent. |
| Partition evolution (`ADD`/`DROP`/`REPLACE PARTITION FIELD`) | ✅ Confirmed | Metadata-only, safe-listed; `IcebergConnectorSpec`'s regression test exercises `ADD PARTITION FIELD` directly under an active contract. |
| Branching and tagging (named refs to a snapshot) | ✅ Confirmed | Metadata/ref-only, safe-listed; `IcebergConnectorSpec`'s regression test exercises create/drop of both directly. |
| `CALL` system procedures (maintenance) | 🔧 **Found and partially fixed** | See "Iceberg CALL procedure classification", "Verifying `rollback_to_snapshot`", and "Extending to the five procedures..." above, and the operation-surface row above — 10 of 20 procedures reclassified from wrongly-rejected to correctly-allowed no-ops, and 6 of 20 (`rollback_to_snapshot`/`rollback_to_timestamp`/`cherrypick_snapshot`/`publish_changes`/`set_current_snapshot`/`fast_forward`) reclassified from wrongly-rejected to genuinely verified, for 16 of 20 with real disposition; the other 4's correct disposition remains fail-closed, now for a documented per-procedure reason instead of "unmodeled." |
| Iceberg SQL views (`CREATE`/`DROP`/`SHOW ICEBERG VIEWS`) | ✅ Confirmed | Metadata-only (view definitions carry no data of their own, matching this file's existing Spark/Delta view-command entries), safe-listed. Not separately tested beyond the safe-list regression test above — no distinguishing behavior beyond "doesn't touch row content," the same reasoning already applied to Spark's own `ShowViews`/`CreateViewCommand` entries. |
| `iceberg-spark-runtime`'s missing `scala-collection-compat` dependency | 🔧 **Found — a library gap, not an Invariant one** | See above; documented as a real, external finding, not a bug this module can fix (adding it as anything but a test dependency would be exactly the kind of unwanted runtime dependency this module's whole design avoids). |
| Deletion vectors / merge-on-read positional deletes | ✅ **Confirmed — closed this pass** | Real probe (since deleted): a `DELETE` against a real `format-version = 3` table (Iceberg's deletion-vector spec) still produces a plain `ReplaceData` node — the same class `dsv2RowLevelWrite` already matches via the shared `RowLevelWrite` trait. The storage mechanism behind a merge-on-read delete (position-delete file vs. deletion vector) isn't visible at the `LogicalPlan` level this adapter operates on at all, so no code change was needed. `IcebergConnectorSpec`'s new "PASS: a DELETE against a format-version=3 (deletion vector) Iceberg table..." test. |
| Schema evolution on write (`write.spark.accept-any-schema` table property + `mergeSchema` write option) | 🔧 **Found and fixed** | Real bug, but the *opposite* direction from the one predicted before investigating: adding a genuinely new column via `mergeSchema` was already correct (`cmd.query.schema` — the writer's own DataFrame — already includes it, confirmed empirically; unlike Delta's MERGE, `AppendData`'s query *is* the writer-supplied data, not a re-derived plan that could go stale). The real bug is a *narrower* write: with `accept-any-schema` enabled, Iceberg accepts an append missing a column the target already has, NULL-filling it — `outputSchema` (from `query.schema` alone) omitted that column entirely, so a contract requiring it was wrongly `MISSING_OUTPUT_FIELD`-rejected. See "Generalizing the generated-columns fix" below — fixed by the same mechanism that now also covers Delta's generated columns. |
| Identity/generated columns | ✅ **Confirmed — closed this pass** | Real probes (since deleted), not assumed from docs: both Spark's `GENERATED ALWAYS AS` syntax and column `DEFAULT` values are rejected outright by this Iceberg catalog integration — `AnalysisException` (`UNSUPPORTED_FEATURE.TABLE_OPERATION`, "does not support generated columns"/"does not support column default value"), thrown by Spark's own analyzer before any plan is ever produced, regardless of `write.spark.accept-any-schema` (tried explicitly, made no difference). So unlike Delta, there's no Iceberg analog to generated columns reachable through Spark SQL with this connector version — nothing for Invariant to translate, verify, or fix. `IcebergConnectorSpec`'s new "GENERATED ALWAYS AS is rejected outright..." and "a column DEFAULT value is rejected outright..." tests. |

### Generalizing the generated-columns fix: target-only fields, not just generated columns

Investigating the schema-evolution row above found that Delta's
generated-columns fix (`outputSchemaWithGeneratedColumns`/
`deltaGeneratedFields`, described in the "Delta feature-by-feature
confidence pass" section above — reflecting into `DeltaTableV2.initialSnapshot()`
to read the `delta.generationExpression` metadata key) and Iceberg's
narrower-write case are two connector-specific *mechanisms* for the same
underlying situation: a resolved write target can legitimately have
fields the write's own `query` doesn't supply, that will still exist in
the committed row. Confirmed empirically that this generalizes further
than either mechanism alone: `cmd.table.schema()`/`cmd.table.columns()`
(the resolved `NamedRelation`'s current, already-committed schema — a
plain public API, no reflection) already carries a Delta generated
column's *name* even though its `.metadata` (the part that would have
identified it as specifically "generated") is stripped — so detecting
*which* target-only fields are generated was never actually necessary,
only whether the target has fields the query doesn't.

**Why unioning in every target-only field is safe, not just convenient**:
by the time a `DataSourceV2Relation`-based write reaches this check rule
at all, Spark's own analyzer has already validated the write's schema is
acceptable against the target — confirmed empirically (a real probe,
since deleted) that an Iceberg table *without* `accept-any-schema`
rejects a narrower write with `AnalysisException` before it ever
produces an `AppendData` node for this rule to see (verified by a
permanent test, `IcebergConnectorSpec`'s "a narrower append is still
rejected by Spark itself... before reaching Invariant"). So a
genuinely-missing required field (the case `MISSING_OUTPUT_FIELD` exists
to catch) is never silenced by this: either Spark's analyzer already
rejected the write for real (this code never runs), or the target's own
connector has already endorsed the field's absence as valid, meaning
unioning it into `outputSchema` reports what will actually be committed,
not merely what the writer provided.

`outputSchemaWithGeneratedColumns`/`deltaGeneratedFields` (reflective,
Delta-specific) were replaced outright by `outputSchemaWithTargetOnlyFields`
(no reflection, connector-agnostic) — `AppendData`/`OverwriteByExpression`/
`OverwritePartitionsDynamic` all call the new one. `Table.columns()`
(not the deprecated `Table.schema()`) is used for the field list — a
`Column` only guarantees `name()`/`dataType()`/`nullable()`, exactly
what's needed; no other `Column` field (default value, generation
expression, comment) is read. Verified: `IcebergConnectorSpec`'s
existing generated-column-equivalent tests (the narrower-write PASS/FAIL
pair) and the pre-existing Delta generated-column test both still pass
unchanged against the new mechanism. Mutation testing rescoped to
`WriteCommandSupport.scala` after this simplification: **76.92%**
(20/26 non-excluded mutants killed) — the new `outputSchemaWithTargetOnlyFields`/
`unionNewFields` code has zero survivors; all 6 remaining are
pre-existing, already-documented from earlier sub-phases (`catalogTable.isDefined`
×2, the `deltaDmlClassNames` guard, `WriteFiles`/`DeltaSink` near-equivalents).

### Closing Iceberg's last two ❓ feature-surface rows: deletion vectors and generated/default columns

A follow-up pass targeting exactly the two rows the initial Iceberg
investigation left as ❓ Not investigated. Both closed with **no
production code changes** — each was a real question with a real,
testable answer, not a gap needing a fix.

**Deletion vectors** (Iceberg's V3 merge-on-read spec, the successor to
position-delete files): a real probe against a genuine `format-version =
3` table, with a real `DELETE` run against it, confirmed the resulting
plan is still a plain `ReplaceData` — the same class `dsv2RowLevelWrite`
already matches via the shared `RowLevelWrite` trait Iceberg's row-level
API rewrites both copy-on-write and merge-on-read operations into. The
storage representation of the delete (a position-delete file vs. a
deletion vector) is an Iceberg-internal detail below the `LogicalPlan`
level this adapter translates at all — so there was never anything for
this layer to special-case. Closed by adding one permanent test, not by
changing any translation logic.

**Identity/generated columns**: two real probes — Spark's `GENERATED
ALWAYS AS` syntax, and a column `DEFAULT` value (Iceberg's V3
`initial-default`/`write-default` mechanism) — both against this
connector's real catalog integration. Both are rejected outright by
Spark's own analyzer (`AnalysisException`,
`UNSUPPORTED_FEATURE.TABLE_OPERATION`) before any `LogicalPlan` is ever
produced, and setting `write.spark.accept-any-schema` doesn't change
that outcome (tried explicitly). So, unlike Delta, this Iceberg
integration has no generated/default-column concept reachable through
Spark SQL at all — there's no feature here for
`outputSchemaWithTargetOnlyFields` (or anything else) to need to handle.
Closed by adding two permanent tests asserting the rejection, the same
pattern already used for "a narrower append is still rejected by Spark
itself... before reaching Invariant."

Both findings are `IcebergConnectorSpec` tests (19 tests total in that
suite after this pass, up from 17). No `spark-adapter` main source
changed this pass, so CLAUDE.md's mutation-testing requirement (scoped to
changed/added `src/main/scala` files) doesn't apply; `sbt
mimaReportBinaryIssues` stayed clean as expected for a test-only change.

## Fail-closed on unverifiable writes

Every translation gap above — `.saveAsTable()` before
`CreateDataSourceTableAsSelectCommand` was added, Delta's `MERGE INTO`
today, and any future write shape this adapter hasn't been taught yet —
shares the same failure mode: `SparkPlanAdapter` produces `ir.Unsupported`
instead of `ir.Write`, and `ContractEnforcementRule.verifyOrThrow`
previously treated *any* non-`ir.Write` plan as "not a write, nothing to
gate." A write Invariant simply doesn't recognize was, until this change,
indistinguishable from a `SELECT` or a `.count()` — silently let through,
contract or no contract, exactly the way the original Delta gap worked
before `SaveIntoDataSourceCommand` was recognized (see "Delta Lake
support" above).

**The fix:** a plan that's `Command`-shaped (Spark's own marker for
"produces a side effect, not rows") and doesn't translate to `ir.Write` is
now rejected outright — `ContractViolationException` with a
`ViolationType.UnverifiableWrite` violation — rather than silently passed.
Confirmed against a real case: SQL `MERGE INTO` on a Delta table analyzes
to `org.apache.spark.sql.delta.commands.MergeIntoCommand`, which is
neither a recognized write nor exempted (see below), so it's rejected
before touching the target table — verified by asserting the table's rows
are byte-identical before and after the aborted `MERGE INTO`
(`ContractEnforcementRuleSpec`).

**Why not just "any `Command` we don't recognize"?** That was the first
design considered, and it's unsafe: a real, jar-level reflective scan of
every concrete class implementing
`org.apache.spark.sql.catalyst.plans.logical.Command` in Spark 3.5.1's
`spark-sql`/`spark-catalyst` and Delta 3.2.0's `delta-spark` (164 classes
total) found that Spark's own `Command` hierarchy does **not** distinguish
"writes data" from "pure catalog/session metadata" — `SaveIntoDataSourceCommand`
(writes data) and `CreateDataSourceTableCommand` (schema-only, `CREATE
TABLE` with no data) both implement the exact same `LeafRunnableCommand`
trait, with no structural marker separating them. A blanket "reject every
unrecognized `Command`" policy would have also rejected ordinary `CREATE
TABLE`, `ANALYZE TABLE`, `CACHE TABLE`, `SHOW TABLES`, and dozens of other
legitimate DDL/administrative operations the moment a contract was active
— a severe regression, not a safety improvement.

**The actual mechanism:** `FailClosedCommands` (new file) holds an
explicit, documented allowlist of ~100 concrete classes judged — by their
documented SQL semantics, not re-verified execution by execution — to
never change a table's committed row content (schema/namespace/function
DDL, `SHOW`/`DESCRIBE`/`ANALYZE`/`CACHE`, session config, storage
maintenance like `VACUUM`/`OPTIMIZE`). `ContractEnforcementRule.verifyOrThrow`
rejects a `Command`-shaped, non-`ir.Write` plan unless its class is on
that list. Matched by fully-qualified class name (`Set[String]`), not
`classOf[...]`/`isInstanceOf`, for the same reason `SparkPlanAdapter`'s
`jdbcLocationOf`/`unwrapWriteWrapper` use string matching: roughly a sixth
of the safe list lives in `org.apache.spark.sql.delta`, and this module
has no compile-time Delta dependency (see "Delta Lake support" above) —
importing those classes directly would reintroduce exactly the dependency
this module was built to avoid.

Genuinely data-mutating commands the survey found (`DeleteFromTable(WithFilters)`,
`MergeIntoTable`/`DeltaMergeInto`, `UpdateTable`, `LoadData(Command)`,
`TruncateTable(Command/Partition)`, `DropTable(Command)`/
`DropDatabaseCommand` (can delete a managed table's/database's data),
`DropPartitions`/`AlterTableDropPartitionCommand`, `ReplaceTable`, and
Delta's `DeleteCommand`/`UpdateCommand`/`MergeIntoCommand`/`WriteIntoDelta`/
`CloneTableCommand`/`ConvertToDeltaCommand`/`CreateDeltaTableCommand`/
`DeltaReorgTable(Command)`/`DeltaGenerateCommand`/`RestoreTableCommand`)
are deliberately **not** on the safe list — they fail closed until
`SparkPlanAdapter` gains a real translation for them (most don't fit
`ir.Write`'s "write a dataset to a location" shape at all — `MERGE`/
`DELETE`/`UPDATE` are row-level operations the IR wasn't designed to
represent, a real future modeling question, not just a missing case
arm). `InsertIntoDataSourceCommand`/`InsertIntoDataSourceDirCommand` are
real writes with a shape similar to `SaveIntoDataSourceCommand` that could
plausibly be added later; left off for now rather than expanding this
change's scope further. `FailClosedCommands`' own doc comment carries the
full list and the categorization rule ("does it change a table's
committed row content?") so it can be extended consistently.

This is intentionally asymmetric: a safe command missing from the list
costs one unnecessary rejection until someone adds it (annoying, cheap to
fix, loud); a data-mutating command wrongly added to the list would
silently defeat the entire feature (invisible, expensive, exactly the
failure mode this exists to prevent). Every uncertain case in the survey
was left off the list on that basis.

Also added in the same change: `CreateDataSourceTableAsSelectCommand`
(`.saveAsTable(...)`/`CREATE TABLE ... AS SELECT` against a *new* V1 data
source table) is now a real, recognized `ir.Write` — a third distinct
write shape, found via the same reflective survey, that previously fell
through to `Unsupported` exactly like the pre-fix Delta gap. Unlike the
other two write cases, its format comes straight from `table.provider`
(already the clean identifier string, no `DataSourceRegister` lookup
needed). Verified with the same PASS/FAIL enforcement pair pattern as the
Parquet and Delta cases (`ContractEnforcementRuleSpec`) plus a translation
test (`SparkPlanAdapterSpec`).

**Regression coverage:** a dedicated test
(`ContractEnforcementRuleSpec`) runs `CREATE TABLE`, `ANALYZE TABLE`, and
`SHOW TABLES` under an active contract that would reject *any* plan it
actually checked, confirming the fail-closed policy's biggest risk —
blocking legitimate DDL — doesn't happen in practice, not just that the
code compiles.

## Write command recognition: a single registry

Three separate places used to each answer "is this plan a write, and
what does it mean" independently: `SparkPlanAdapter.Translator.translatePlan`
(to translate it), `ContractEnforcementRule.verifyOrThrow` (just to pull
the right output schema), and `SparkAdapterListener.onSuccess` (just to
decide whether to capture a write for `demo/output/report.json`). Three
`case cmd: X =>` matches, hand-kept in lockstep by whoever added a write
shape. Both of this module's real Delta-support bugs (see "Delta Lake
support" above) were exactly that: a write shape added to one match and
missed in another. This wasn't a one-off mistake — it was a structural
hazard built into encoding the same fact three times.

**`WriteCommandSupport`** (new file) replaces all three with one
registry: a `PartialFunction[LogicalPlan, WriteCommandInfo]` per
recognized write shape (`InsertIntoHadoopFsRelationCommand`,
`SaveIntoDataSourceCommand`, `CreateDataSourceTableAsSelectCommand`),
combined via `orElse` into `WriteCommandSupport.combined`. `WriteCommandInfo`
bundles everything a write's translation and verification both need in
one shot — location, the (untranslated) query, format, save mode, and
the output schema `ContractEnforcementRule` checks against — so it's
structurally impossible to add a write shape that translates correctly
but is missing the schema piece the way the original Delta bug did.

All three sites now consult exactly this:

- `SparkPlanAdapter.Translator.translatePlan`: `WriteCommandSupport.combined.lift(plan)` →
  `Some(info)` becomes `ir.Write(DatasetRef(info.location), translatePlan(info.query), info.format, info.saveMode)`,
  reporting `info.diagnostic` if present; `None` falls through to the
  rest of the match (reads, `Project`, `Filter`, ..., the `Unsupported`
  fallback).
- `ContractEnforcementRule.verifyOrThrow`: `WriteCommandSupport.combined.lift(plan).map(_.outputSchema).getOrElse(plan.schema)` —
  one line, replacing the three-case match that used to live here.
- `SparkAdapterListener.onSuccess`: `WriteCommandSupport.combined.isDefinedAt(qe.analyzed)` —
  the entire "is this a write" check, replacing its own three-case match.

**What this means for adding a connector**: most connectors need zero new
entries here at all — the whole point of the Delta investigation was that
`SaveIntoDataSourceCommand` already covers any `CreatableRelationProvider`-based
`.save(...)`, connector-specific or not. When a connector genuinely
introduces a new write-command *shape* Spark doesn't already have a
generic node for, adding support is: implement one more
`PartialFunction[LogicalPlan, WriteCommandInfo]` in `WriteCommandSupport.scala`
following the three existing ones as templates, and chain it into
`combined`. Nothing in `SparkPlanAdapter`, `ContractEnforcementRule`, or
`SparkAdapterListener` needs to change — see
docs/ADDING_A_SPARK_CONNECTOR.md.

**Verified behavior-preserving, not just re-tested:** the full 59-test
suite passed unchanged before and after this refactor (identical
translation output for every existing case), `mimaReportBinaryIssues`
stayed clean, and `./dev/build`/`./dev/test`/`./dev/regression` all still
pass against real `spark-submit`. Mutation testing did surface one real,
new gap the refactor introduced — `SparkAdapterListener.onSuccess`'s
`isDefinedAt` check surviving an "always capture" mutant, because no
existing test asserted the *negative* case (a non-write action leaving
`lastWrite` untouched, only ever tested the positive "a write is
captured" side). Fixed by adding exactly that test rather than leaving it
undetected — see "Mutation testing" below for the resulting score.

## Known limitations

- **No `SparkSessionExtensions`-based capture.** Only a `DataFrame`'s own
  `.queryExecution` or a registered `QueryExecutionListener` are used to
  obtain a plan. This is deliberate (see *Integration point* above), not
  an oversight — the adapter never needed to rewrite a plan.
- **`SubqueryAlias` over a non-`Read` subplan drops the alias** with a
  `Diagnostic`, rather than inventing a generic aliased-subplan IR node for
  a case that didn't come up in practice (see the IR's own *Known
  limitations* in TRANSFORMATION_IR.md).
- **Hive relations are untested.** `HiveTableRelation` has no dedicated
  case (this environment has no Hive metastore to verify against); it
  falls through to the generic `LogicalRelation` handling's
  `catalogTable`-based fallback, unverified against a real Hive session.
  (`JDBCRelation` previously shared this fallback too — a real gap, since
  the fallback's location has no reliable relationship to what a contract
  would declare for a JDBC source — but now gets its own precise
  `url`/`table`-based location; see the Translation coverage table above.)
- **DataFrameWriterV2/SQL `MERGE INTO`/`DELETE`/`UPDATE` writes are not
  recognized** — `.save(path)`-style writes (`SaveIntoDataSourceCommand`)
  and `.saveAsTable(...)` against a new V1 table
  (`CreateDataSourceTableAsSelectCommand`) are (see "Delta Lake support"
  and "Fail-closed on unverifiable writes" above); DataSourceV2 catalog
  writes and Delta's row-level `MERGE`/`DELETE`/`UPDATE` commands use
  different plan shapes with no translation yet. No longer a silent gap,
  though — an active contract now rejects these outright instead of
  passing them through unverified (see "Fail-closed on unverifiable
  writes").
- **JDK 17+ requires `--add-opens` flags for `sbt test` and for the
  non-`spark-submit` fallback in `dev/test`.** Spark reflectively accesses
  JDK-internal classes (`sun.nio.ch.DirectBuffer` via
  `org.apache.spark.storage.StorageUtils`) that JDK 17+'s module system
  closes by default. `spark-submit`'s own launch scripts inject the
  necessary flags automatically, which is why `./dev/test`'s primary path
  needs no changes; `spark-adapter/build.sbt`, `plugin/build.sbt`, and
  `dev/test`'s fallback `java -cp` invocation all now set the same flag
  set explicitly, since this class of failure blocks the "Spark
  integration tests" deliverable outright otherwise, in any environment
  without `spark-submit` on `PATH`.

## Structural verification

`StructuralVerifier.verify(contract, plan, inputSchemas, outputSchema, options): VerificationResult`
(`spark-adapter/src/main/scala/com/example/sparkadapter/StructuralVerifier.scala`)
is ROADMAP.md Phase 1c's structural verifier (its own spec called this
"Phase 4"): the first genuinely useful check, covering every item in
MISSION.md §8's "Structural" class — for both inputs and outputs, not just
the output slice the earlier `ContractVerifier` covered.

Two different sources feed a check:

- **Existence and location** come straight off the `Plan` — `Read`/`Write`
  nodes' `DatasetRef.location`. No Spark-specific data needed; the IR
  already carries this.
- **Schema** (field presence, type, nullability) needs a real Spark
  `StructType` per dataset, supplied by the caller, because the IR
  deliberately carries no schema of its own (`ir.Read` records only which
  columns were *referenced*, not a dataset's full column set — see
  [TRANSFORMATION_IR.md](TRANSFORMATION_IR.md)).

**Location matching** bridges a real gap: a contract declares a portable,
relative location (`"demo/input/sample.csv"`); Spark reports an absolute
`file:` URI at runtime (`"file:/home/user/.../demo/input/sample.csv"` —
confirmed empirically earlier in this doc). Comparing with `==` would fail
every real run for a reason unrelated to actual contract compliance, so
locations are matched by normalized suffix instead (strip the `file:`
scheme; a declared location matches if it equals, or is a path-boundary
suffix of, the actual one).

**Nullability** is checked directionally, not by equality: a contract
requiring non-null (`nullable: false`) is violated by an actual column
that permits nulls; the reverse — contract allows null, actual guarantees
non-null — is a stricter-than-required guarantee, not a violation.

**Unexpected inputs/columns** are opt-in rejectable
(`VerificationOptions(rejectUndeclaredInputs, rejectUndeclaredFields)`),
both off by default — matching how most contract/schema tooling treats an
unlisted extra column: permitted unless a caller opts into strict mode.

The result matches the spec's exact shape:

```json
{
  "status": "PASSED" | "FAILED",
  "contract": "invariant_demo_output@1.0.0",
  "violations": [
    { "type": "UNDECLARED_OUTPUT_COLUMN", "message": "...", "column": "country" }
  ]
}
```

Fourteen violation types, covering inputs and outputs symmetrically:
`MISSING_INPUT`, `UNDECLARED_INPUT`, `MISSING_INPUT_FIELD`,
`UNDECLARED_INPUT_COLUMN`, `INPUT_FIELD_TYPE_MISMATCH`,
`INPUT_FIELD_NULLABILITY_MISMATCH`, and the `OUTPUT_*` equivalents
(`MISSING_OUTPUT`/`OUTPUT_LOCATION_MISMATCH` replace `MISSING_INPUT`'s role
on the output side, since there's exactly one actual `Write` to compare
against the contract's declared output, rather than a set of `Read`s to
match by location). `OUTPUT_FORMAT_MISMATCH` and `OUTPUT_SAVE_MODE_MISMATCH`
are the two asymmetric additions — checked only for outputs, and each only
when both the contract's declared value (`format`/`saveMode`) and the
corresponding actual value from `ir.Write` are known; either side being
unset skips the check.

`runner/DemoJobHarness.scala` runs this against the real demo pipeline on
every `./dev/test`, using `demo/contracts/invariant_output.yaml`. Kept in
its own `contractVerification` report section and console block, separate
from `ExecutionReport.status`: "did the Spark job execute" and "does its
output satisfy the contract" are different questions, and conflating them
would hide which one actually failed.

Actual console output from `./dev/test`:

```
Contract verification: PASSED (invariant_demo_output@1.0.0)
  (no violations)
```

`StructuralVerifierSpec` proves this isn't a rubber stamp: every violation
type fires at least once against real or realistically-constructed
schemas, both `VerificationOptions` toggles are exercised on and off, and
a golden test reproduces the Phase 4 spec's own worked example
(`UNDECLARED_OUTPUT_COLUMN`, column `"country"`) exactly.

Supersedes the earlier `ContractVerifier` (output schema only, no inputs,
no nullability, no undeclared-column rejection) — removed rather than kept
alongside, to avoid two overlapping verifiers in the codebase.

## Contract-aware execution: verify before, not after

Everything above (`SparkAdapterListener`, `StructuralVerifier`) verifies
*after* a write has already executed — useful for reporting, useless for
prevention. `ContractEnforcementRule`
(`spark-adapter/src/main/scala/com/example/sparkadapter/ContractEnforcementRule.scala`)
moves verification *into* the execution lifecycle:

```
Spark application → Logical plan → Invariant → PASS → execute
                                             └─→ FAIL → abort
```

**Why a different Spark mechanism was needed.** `QueryExecutionListener`
(what `SparkAdapterListener` wraps) fires via `onSuccess` — by definition,
after Spark has already run the query. Preventing a write requires a hook
that runs *before* execution and can reject the query. Spark provides
exactly this: `SparkSessionExtensions.injectCheckRule`, a function invoked
on every analyzed plan whose only purpose is validation — it can throw to
reject a query outright. This was confirmed empirically before building on
it (assumptions about Catalyst internals have been wrong before in this
project): a probe registering a check rule that unconditionally threw on
a write command showed the exception propagating out of
`DataFrame.write.parquet(...)` unwrapped, and — critically — the target
file was never created. `output file exists? false`, confirmed directly,
not inferred.

**What triggers a check.** The rule fires on *every* analyzed plan a
session produces — schema-inference reads, `.count()`, intermediate
transformations — not just the final write. Only a plan that
`SparkPlanAdapter.translate`s to an `ir.Write` is verified; everything
else is a silent no-op, confirmed by a dedicated test using a contract
that would fail immediately if it were (wrongly) applied to a non-write
plan.

**Schemas without executing anything.** Every resolved Catalyst
`LogicalPlan` exposes `.schema` derived from its resolved attributes —
available at analysis time, before any physical execution. So
`ContractEnforcementRule` gets both input schemas (via
`plan.collect { case lr: LogicalRelation => ... }`) and the output schema
(`InsertIntoHadoopFsRelationCommand.query.schema`) directly from the
analyzed plan, with no need for a materialized `DataFrame` the way the
post-hoc reporting path used. (`SparkPlanAdapter.locationOf` was promoted
from a private `Translator` method to a public one so both paths share
the same location logic instead of duplicating it.)

**Deterministic, explainable failures.** The task's explicit requirement:
a developer reading the failure should understand what the contract
expected, what the plan contains, why it violates the contract, and how
to correct it. `ContractEnforcementRule.explain` builds exactly this from
one `VerificationResult` — every `Violation` now carries a `remediation`
field alongside its `message` (added in `StructuralVerifier`, populated
at each violation's construction site, since that's where the fix is
obvious). Determinism isn't just claimed: `StructuralVerifier`'s result
list is built without ever iterating a `Set`/`Map` to produce output (only
for membership tests), and a dedicated test runs the identical failing
scenario three times, asserting byte-identical explanation text.

**Live, not just unit-tested.** Beyond `ContractEnforcementRuleSpec`, the
real demo pipeline was run via `spark-submit` against
`demo/contracts/invariant_output_broken_example.yaml` — a contract
requiring a `customer_name` column the real `InvariantPlugin` never
produces:

```
Contract violation: 'invariant_demo_output@1.0.0' rejected this transformation. Write aborted.

What the contract expects:
  input  'orders' at demo/input/sample.csv: id: integer, value: integer
  output 'result' at demo/output/result_broken_example.parquet: id: integer, value: integer, value_squared: integer, customer_name: string

What the plan contains:
  Write(file:/home/user/Invariant/demo/output/result_broken_example.parquet)
  └─ Project
     ├─ Read(file:/home/user/Invariant/demo/input/sample.csv)
     ├─ id = id
     ├─ value = value
     └─ value_squared = value * value

Why it violates the contract (1 violation):
  1. [MISSING_OUTPUT_FIELD] required field 'customer_name' is absent from the actual OUTPUT schema

How to correct it:
  1. Add a 'customer_name' column (type 'string') to the output, or mark it optional in the contract if it isn't always produced.
```

`spark-submit` exited `1`; `demo/output/result_broken_example.parquet` was
never created. This contract is kept in the repo for reference/reproduction
but is not wired into `./dev/test` — the real demo pipeline is expected to
pass its real contract, not this deliberately-broken one.

**Two mechanisms, two moments.** `ContractEnforcementRule` doesn't replace
`SparkAdapterListener` — a check rule can only approve or reject mid-call;
it has no equivalent of "give me the finished result to report on
afterward." `runner/DemoJobHarness.scala` uses both: the check rule decides
whether a write happens at all, and the listener (still registered,
still fed from a write that only proceeded because it already passed
verification) supplies `demo/output/report.json`'s human-facing
`transformationIR` summary.

## Testing

**Cross-platform assertions — a real CI failure, not a hypothetical.**
CI's OS matrix includes `windows-latest`, and this module's development
environment is Linux-only, so a Windows-specific bug in a new test is
invisible locally no matter how thoroughly it's rerun — it can only be
caught by actually reading a failed Windows CI run. Two real patterns
have caused this:

- **Never assert `location.contains(nativePath)`** where `nativePath`
  came from `java.nio.file.Path.toString()`. On Windows that's
  backslash-separated (`C:\Users\...\x`), but Spark always normalizes a
  resolved storage location into a forward-slash `file:` URI regardless
  of platform — the assertion can never match. Assert on the filename
  only (`location.contains("sample.csv")`), the convention every
  location assertion in `SparkPlanAdapterSpec` follows.
- **Never interpolate a native path directly into a SQL string literal**
  (`s"...LOCATION '$path'"`). A Windows path's backslashes collide with
  SQL string-literal escaping and can silently mangle the path into
  something Hadoop then rejects. Normalize with `path.replace('\\', '/')`
  first — Spark/Hadoop accept forward-slash paths on Windows too, so
  this is always safe, not a Windows-only branch.

```bash
cd spark-adapter
sbt test
```

51 tests against a real `local[*]` `SparkSession` (no mocked plans):

- **`SparkPlanAdapterSpec`** (20) — a bare read, the worked example,
  filter+cast, self-join alias disambiguation, union, window, a UDF, an
  unsupported construct, `Sort`, every `JoinType`, a multi-way join chain,
  `COUNT`/`AVG`/`MIN`/`MAX`/`COUNT(DISTINCT ...)`, a multi-argument
  aggregate, `CASE WHEN`/`IS NULL`, `.limit(n)`, `.distinct()`,
  `.repartition()`/`.coalesce()`, format-agnosticism across CSV/JSON/
  Parquet, a real H2 JDBC read, and a full write (format + save mode)
  captured end-to-end through `SparkAdapterListener`.
- **`StructuralVerifierSpec`** (22) — the real demo pipeline passing its
  own contract; every violation type (`MISSING_INPUT`, `UNDECLARED_INPUT`,
  `MISSING_OUTPUT`, `OUTPUT_LOCATION_MISMATCH`, `MISSING_OUTPUT_FIELD`,
  `UNDECLARED_OUTPUT_COLUMN`, `OUTPUT_FIELD_TYPE_MISMATCH`,
  `OUTPUT_FIELD_NULLABILITY_MISMATCH`, `OUTPUT_FORMAT_MISMATCH`,
  `OUTPUT_SAVE_MODE_MISMATCH`, and the input-side equivalents) firing
  correctly; the golden `UNDECLARED_OUTPUT_COLUMN`/`"country"` example; the
  relative-vs-absolute location matching, checked against Spark's real
  reported paths; multi-input `MISSING_INPUT`/`UNDECLARED_INPUT` checked
  independently rather than requiring universal agreement; an absent
  optional field producing no violation; and violation
  messages/remediations naming the correct side.
- **`ContractEnforcementRuleSpec`** (8) — PASS executes and creates
  output; FAIL aborts before any data is written; the explanation contains
  all four required sections; the same violation produces byte-identical
  explanations across three repeated attempts; non-write queries never
  trigger verification even under an always-failing contract;
  `VerificationOptions` thread through the enforcement path;
  `forContract`'s public entry point works directly; `explain` pluralizes
  the violation count and marks optional fields distinctly.
- **`SparkPlanAdapterFuzzSpec`** (1 property, ~200 generated cases per run)
  — random chains of the operations `SparkPlanAdapterSpec` tests
  individually (filter, recomputed columns, sort, aggregate, self-join,
  union, distinct, limit, repartition/coalesce, CASE WHEN), composed in
  random order and depth, asserting `translate`/`render`/`trace` never
  throw and any `Unsupported` node carries a `Diagnostic`. See *Property-
  based fuzzing* below.

### Property-based fuzzing

`SparkPlanAdapterFuzzSpec` (`spark-adapter/src/test/scala/.../SparkPlanAdapterFuzzSpec.scala`)
exists because example-based tests only prove the adapter handles the
examples someone thought to write. The class doc's "never throws" promise
is a claim about *every* plan the adapter might ever see, not just the ~20
hand-picked ones in `SparkPlanAdapterSpec` — and those only ever exercise
one construct at a time, never combinations or nesting.

The spec defines a small `Step` ADT (`FilterStep`, `RecomputeValueStep`,
`SortStep`, `AggregateStep`, `SelfJoinStep`, `UnionSelfStep`,
`DistinctStep`, `LimitStep`, `RepartitionStep`, `CaseWhenStep`) and a
ScalaCheck generator that produces random chains of 1-6 steps. Every step
is designed to preserve a fixed canonical schema
(`id: Int, value: Int, name: String, active: Boolean`) — including
`AggregateStep` (which groups by `id` and re-selects the aggregate back
into `value`) and `SelfJoinStep` (which joins the frame against itself and
re-projects the left side back down to the four canonical columns). That
means the generator never has to track a live schema to decide what's
valid at each step — any sequence of steps composes into a real, resolvable
Spark query — so the randomness lands entirely on plan *shape and depth*,
which is exactly what the hand-written spec doesn't cover.

Each generated case builds the chain against a real `local[*]` session,
takes `.queryExecution.analyzed` (cheap — analysis only, no job ever
executes), and asserts `SparkPlanAdapter.translate`/`PlanPrinter.render`/
`Lineage.trace` all complete without throwing, and that any `Unsupported`
node in the result is paired with a `Diagnostic` (the pairing the class
doc promises). ~200 cases run in a few seconds inside the existing forked
test JVM.

This is validated to actually catch bugs, not just pass by construction:
during development, `translateJoinType`'s `Inner` case was temporarily
changed to throw, and the fuzz spec failed on its very first generated
case — a single-step `List(SelfJoinStep)` chain — with the full analyzed
Catalyst plan and exception in the failure message, enough to reproduce
and fix without needing ScalaCheck's shrinking (which isn't used here,
since `forAll` is called with an explicit `Gen` rather than an `Arbitrary`
instance).

### Mutation testing

Property-based fuzzing (above) answers "does the adapter ever throw?" — a
different, complementary question is "do the tests that pass actually
verify correct behavior, or just execute the code?" [Stryker4s](https://github.com/stryker-mutator/stryker4s)
answers that: it generates small deliberate bugs ("mutants") in a source
file — flip a `==` to `!=`, an `&&` to `||`, `.exists` to `.forall`, delete
a string literal — recompiles, and reruns the real test suite against each
one. A test failing means the mutant is *killed* (good — something
verifies that logic); every test still passing means it *survived*, which
100% line coverage cannot detect.

`build.sbt`'s `strykerMutate` setting scopes this to the whole module
(`src/main/scala/**/*.scala`); `strykerThresholdsBreak` gates it at 70% —
`sbt stryker` exits non-zero below that, which is what makes CI's
`mutation-testing` job (`.github/workflows/test.yml`) fail the build. Both
settings live in `build.sbt`, not `stryker4s.conf`: that config file's
equivalent `mutate`/`thresholds` keys were observed not to take effect
with this sbt/plugin version combination. Run it locally with:

```bash
cd spark-adapter
sbt stryker
# HTML report: target/stryker4s-report/<timestamp>/index.html
```

An initial run scoped to just `StructuralVerifier.scala` scored 50.0%.
Widening to the whole module (`SparkPlanAdapter.scala` and
`ContractEnforcementRule.scala` too) dropped that to **44.79%** — more
files, more untested surface. Adding tests for the real (non-`StringLiteral`)
survivors brought it to **57.06%** (93/177 mutants killed):

- **`exists`/`forall` swaps in the input-matching predicates**
  (`StructuralVerifier.scala:143` and `:157`, the `MISSING_INPUT` and
  `UNDECLARED_INPUT` checks) — fixed with a test declaring two inputs and
  reading two locations that overlap on only one, which is the minimum
  shape where the two quantifiers actually disagree.
- **`field.required` forced to always `true`** (`StructuralVerifier.scala:296`,
  inside `checkSchema`) — fixed with a contract declaring one required and
  one optional field, both absent from the actual schema; only the
  required one may produce a violation.
- **`contextPrefix == "INPUT"` selecting the wrong violation-type/wording
  pair** (`StructuralVerifier.scala:291`) — fixed with a test asserting
  the `datasetNoun` text ("input"/"output") that only that branch controls.
- **Violation-count pluralization and the optional-field marker**
  (`ContractEnforcementRule.scala:121` and `:136`, inside `explain`) —
  `explain` is `private[sparkadapter]`, so these were killed by calling it
  directly with a synthetic one- and two-violation `VerificationResult`,
  no real Spark write needed.

At that point what remained (84/177 undetected) was 79 `StringLiteral`
mutants on message/remediation/type-name text plus the same five real,
already-investigated gaps in `SparkPlanAdapter.scala` (below). Every
`StringLiteral` survivor sits on human-readable text — a test that pins
the exact wording of an error message or the exact spelling of a type
name doesn't verify behavior, it verifies prose, and tends to be the
first assertion a future refactor breaks for no functional reason. Rather
than writing ~79 of those brittle exact-match tests (or leaving the
module's real coverage permanently capped by a mutator category nobody
intends to chase), `build.sbt` now sets
`strykerExcludedMutations := Seq("StringLiteral")`: Stryker4s never
generates that category for this module, so the score reflects only
mutants that change actual behavior — equality/conditional/boolean
logic, method calls, arithmetic, collection operations. This is the same
`StringLiteral`-on-message-text exception CLAUDE.md's "Mutation Testing
Requirement" already names as acceptable to leave undetected with a
documented reason; excluding it here just makes that call explicit and
repo-wide instead of an ad hoc per-PR judgment.

With that exclusion, the module scores **91.94%** (of total) / **93.44%**
(of covered code) — 57/61 mutants killed (numbers as of the
"Write command recognition: a single registry" refactor above; unchanged
in kind, if not exact count, since the initial 91.53%/93.1%/54-59
baseline — the `WriteCommandSupport` extraction itself was fully covered,
and the one genuinely new gap it introduced (`SparkAdapterListener`'s
`isDefinedAt` check surviving an "always capture" mutant — no existing
test asserted the *negative* case, only ever "a write is captured") was
closed with a new test rather than left, so the same five pre-existing
gaps still account for 100% of what's undetected). The five that remain
undetected are the same gaps investigated (not ignored) during the push
to 57.06%, and still hold, just relocated by the refactor — three stayed
in `SparkPlanAdapter.scala` (the code that moved to `WriteCommandSupport.scala`
was the write-shape translation, not these three), two moved into
`WriteCommandSupport.scala` along with `unwrapWriteWrapper`:

- **`JDBCRelation`-guard's near-equivalent always-true mutant**
  (`SparkPlanAdapter.scala:168`) — the `Try`/`toOption` fallback this
  guard feeds absorbs either outcome, so there's no test that could
  distinguish true divergence in behavior from equivalence here.
- **`lr.catalogTable.isEmpty` and `usedFallback`'s Hive-relation branch**
  (`SparkPlanAdapter.scala:247` and `:249`) — no Hive metastore is
  available in this environment to construct a `LogicalRelation` that
  takes this path (see *Known limitations* above).
- **`unwrapWriteWrapper`'s no-wrapper branch**
  (`WriteCommandSupport.scala:155`, two mutants) — Spark 3.4+ always
  inserts the `WriteFiles` wrapper this adapter targets, so the "no
  wrapper present" branch has no reachable real-world trigger under the
  pinned Spark 3.5.1.

Note also that Stryker4s's per-mutant reruns use coverage-based test
selection (only tests observed to execute a mutated line are rerun for
that mutant) — occasionally this can make a mutant "Survive" that the full
suite would actually catch, purely because of how coverage was mapped.
Treat "Survived" as a strong lead to investigate, not an automatic
verdict.

This module's whole-module score (91.94%) now clears the same 70% bar
CLAUDE.md's "Mutation Testing Requirement" sets for new/changed code —
the two are no longer at different levels, though the whole-module number
stays the CI-blocking gate and the per-PR incremental check (below) stays
the mechanism that actually enforces the new-code bar on every push.

#### Delta feature-by-feature confidence pass

Scoped `sbt stryker --mutate "src/main/scala/com/example/sparkadapter/WriteCommandSupport.scala"`
(this sub-phase's only changed file) scored **73.08%** (19/26 non-excluded
mutants killed) after the schema-evolution fix, generated-columns fix,
and the `/simplify` pass that followed (extracting `unionNewFields` and
replacing the `GeneratedColumn`/`Protocol` reflection chain with a direct
`StructField.metadata` check — see "Delta feature-by-feature confidence
pass" above). One of the two real survivors in the new generated-columns
code was killed by adding a direct-inspection test
(`WriteCommandSupport reports no diagnostic for AppendData into a Delta
table with no generated columns`, mirroring the existing path-based-DML
direct-inspection test) proving `outputSchemaWithGeneratedColumns`'s
"nothing found" branch stays silent, not just that the resulting schema
value happens to come out the same either way.

The other new-code survivor — `deltaGeneratedFields`'s
`table.getClass.getName != "...DeltaTableV2"` guard, forced to skip the
check — is the same class of near-equivalent mutant already documented
above for `SparkPlanAdapter`'s `JDBCRelation` guard: the surrounding
`Try`/`getOrElse(Seq.empty)` absorbs either outcome for any table type
this module could realistically encounter (a non-`DeltaTableV2` object
simply doesn't have an `initialSnapshot()` method, so skipping the guard
just trades one safe empty result for another reached via a caught
`NoSuchMethodException`), so there's no test that could distinguish true
behavioral divergence from equivalence without constructing an
artificial object purpose-built to defeat the guard. The remaining five
survivors (`catalogTable.isDefined` ×2, the `WriteFiles`-unwrap pair, and
`DeltaSink`'s format check) predate this sub-phase and are already
accounted for above/in ROADMAP.md.

#### Iceberg support

Scoped `sbt stryker --mutate "{WriteCommandSupport.scala,SparkPlanAdapter.scala,ContractEnforcementRule.scala,FailClosedCommands.scala}"`
(all four files this sub-phase touched) scored **80.65%** (of total) /
**81.97%** (of covered code) — 50/61 non-excluded mutants killed.

Two real survivors were in this pass's own new code, both killed rather
than left:

- **`CreateTableAsSelect`'s `cmd.ignoreIfExists` branch** (deciding
  between `saveMode = "error"`/`"ignore"`) survived forced to both `true`
  and `false` — no existing test distinguished a bare
  `.writeTo(...).create()` from `CREATE TABLE IF NOT EXISTS ... AS
  SELECT`. Killed with a new direct-inspection test capturing both real
  plan shapes and asserting `saveMode` for each
  (`IcebergConnectorSpec`'s "CreateTableAsSelect's saveMode reflects
  ignoreIfExists").
- **The new `DataSourceV2Relation` read case's no-location diagnostic
  branch** (`SparkPlanAdapter.scala`) survived because every real test
  exercising it happened to resolve a genuine location (a real Iceberg
  catalog table always does) — the mutant only diverges when the
  location is genuinely absent, a case no real Iceberg session in this
  test suite naturally produces. Killed with a directly-constructed
  minimal `Table` (implementing just `name()`/`schema()`/`capabilities()`
  — `properties()` has an empty-map default) wrapped in a real
  `DataSourceV2Relation` via `DataSourceV2Relation.create(...)`, no
  session needed at all (`SparkPlanAdapterSpec`'s "falls back to a
  DataSourceV2Relation's name() with a diagnostic when its Table reports
  no location") — the same technique that would be needed to kill
  `StreamingRelationV2`'s still-surviving analogous mutant (see below),
  left for a future pass since it predates this one.

The remaining 12 survivors all predate this sub-phase and are already
documented (here or in ROADMAP.md's earlier sub-phases):
`deltaDmlClassNames`'s guard, the `WriteFiles`-unwrap pair, `DeltaSink`'s
format check, `deltaRowLevelDml`'s `catalogTable.isDefined` diagnostic
branch ×2, `deltaGeneratedFields`'s `DeltaTableV2` guard, the
`JDBCRelation` near-equivalent, `StreamingRelationV2`'s own no-location
branch (the streaming counterpart to the batch case just fixed above —
not addressed this pass, since closing it isn't part of what Iceberg's
investigation required), `StreamingRelation`'s path-option check, the
Hive-metastore-unavailable `NoCoverage` gap, and `LogicalRelation`'s
`usedFallback` branch.

#### Incremental checking in CI

That 70%-on-new-code bar was originally a manual step (run a scoped
`sbt stryker` locally, read the report). `.github/workflows/test.yml`'s
`mutation-testing-ir`/`mutation-testing-spark-adapter` jobs (see "CI
mutation-testing wall-clock" below for why there are two, not one) now
automate it for every PR: a "Mutation test changed files" step diffs
against the PR's base commit
(`github.event.pull_request.base.sha`), filters to each module's changed
`src/main/scala/**/*.scala` files, and runs `sbt stryker` scoped to just
those — via a brace-expansion glob for multiple files
(`--mutate "{FileA.scala,FileB.scala}"`, confirmed to work with
Stryker4s's glob matcher) — with `--thresholds.high 90 --thresholds.low 80
--thresholds.break 70` passed on the CLI. These happen to match
`spark-adapter`'s own whole-module thresholds now (`ir`'s are higher, at
80/60/50, reflecting its higher whole-module score) — the CLI override
still exists to pin every module's incremental, changed-files-only run to
the same 70% new-code bar regardless of what each module's own
whole-module thresholds happen to be set to, without touching either
module's `build.sbt`.
A module with no changed files under `src/main/scala` is skipped entirely
rather than run with an empty `--mutate` (which Stryker4s tolerates —
exits 0 reporting "0 mutant(s)" — but still pays the full test-runner
startup cost for nothing).

This is real incrementality, not Stryker4s's own feature: Stryker4s itself
has no diff/since mode (unlike StrykerJS), so this is a small CI-level
wrapper around the same `--mutate` scoping mechanism used throughout this
section, not a built-in capability. It only runs on `pull_request` events
— a bare `push` has no unambiguous "changed relative to what" to diff
against.

#### CI mutation-testing wall-clock

Two real, previously-untried levers found while investigating why CI's
mutation-testing job was taking 35-40+ minutes (an earlier session
investigated *local* `sbt test` speed — session reuse, shuffle
partitions, codegen — and found no real lever there; this is a
different, CI-specific question).

**`--concurrency` works as an explicit CLI flag** (`sbt "stryker
--concurrency 4"`), even though the equivalent `stryker4s.conf`/
`build.sbt` settings don't take effect with this plugin version (see
`spark-adapter/stryker4s.conf`'s own comment) — confirmed via the log
line changing from "Creating 2 test-runners" to "Creating 4
test-runners". Same category of quirk this repo already documented for
`--mutate`/`--thresholds`: config-file settings silently no-op, CLI
flags work. Added to every `sbt stryker` invocation in both mutation-
testing CI jobs below.

**The single `mutation-testing` job was split into two parallel jobs**
(`mutation-testing-ir`, `mutation-testing-spark-adapter`). `ir` and
`spark-adapter` are independent modules with independent test suites —
nothing about mutating one depends on the other finishing — so running
them sequentially on one runner was pure wasted wall-clock. Splitting
lets GitHub schedule them concurrently, cutting wall-clock from
`ir_time + spark_adapter_time` to roughly `max(ir_time, spark_adapter_time)`.
Since `spark-adapter` is far larger and slower (Delta + Iceberg +
everything else) and `ir` has zero Spark dependency at all (confirmed by
grepping `ir/build.sbt`), this also lets `ir`'s job skip the entire Spark
cache/download/configure sequence `spark-adapter`'s job still needs —
`ir`'s own run is now ~1-2 minutes, mostly hidden under `spark-adapter`'s
much larger cost rather than adding to it serially. `summary`'s `needs:`
list and result-check condition were updated to the two new job names.

**What this doesn't change**: `spark-adapter`'s own whole-module run is
still the real bottleneck (~30-40 min) — it runs the full test suite once
per generated mutant against real Delta- and Iceberg-backed Spark
sessions, which is genuine, unavoidable work, not overhead. These two
levers reduce wasted time around that core cost (serialization,
under-utilized cores), not the cost itself.

### API compatibility

This module's real, intended public surface is deliberately narrow:
`ContractEnforcementRule.forContract` (the installation entry point),
`SparkAdapterListener` (the reporting entry point), and the result/error
types both produce — `TranslationResult`, `Diagnostic`,
`ContractViolationException`, `VerificationResult`, `Violation`,
`VerificationOptions`, `ViolationType`. That list was arrived at by
grepping every cross-module reference before deciding what stays public,
not by guessing (see CLAUDE.md's "What's the product, and what's the
test harness" review that prompted it). `SparkPlanAdapter` and
`StructuralVerifier` — the raw Catalyst-to-IR translator and the raw
verification function — are `private[sparkadapter]`: nothing outside
this module ever called either directly, since a real user gets both
automatically via the installed extension.

This surface is checked by [MiMa](https://github.com/lightbend/mima) via
`sbt mimaReportBinaryIssues`, CI-enforced on every PR — see CLAUDE.md's
"API Compatibility Requirement" for the full mechanism (no Maven Central
release exists yet to compare against, so CI publishes the PR's base
branch to the runner's local Ivy cache first and diffs the PR's head
against that instead; a module that doesn't exist yet at the base commit
— true of all three, for this repo's own PR #1, whose base predates them
entirely — is skipped gracefully rather than special-cased).

One real limitation worth knowing, discovered while narrowing
`SparkPlanAdapter`/`StructuralVerifier`: `private[sparkadapter]` is a
Scala-compiler-enforced restriction, not a JVM one. `javap` on the
compiled classes shows they stay `public final class` in raw bytecode
either way (confirmed directly, not assumed) — Scala's qualified-private,
when the qualifier is the symbol's own containing package, doesn't
correspond to a bytecode access-flag change for a top-level object.
MiMa compares bytecode, so it cannot see this narrowing as a change at
all — `mimaReportBinaryIssues` reports zero issues for it, not because
an exclusion covers it, but because nothing detectable to MiMa actually
changed. The narrowing is still worth doing: it stops real Scala code
from depending on either class by accident, which is the failure mode
that actually matters. It just isn't a MiMa-enforced guarantee the way
the module's real public API is.

To see the translation adapter running against the actual demo pipeline:

```bash
./dev/test
# Console output includes "Transformation IR (translated from the real
# Spark logical plan):" followed by the rendered plan.
# demo/output/report.json's "transformationIR" section has the full
# rendered plan, lineage, and diagnostics.
```

---

**Last Updated:** 2026-08-23
**Status:** Spark adapter — initial implementation (ROADMAP.md Phase 1c, Spark Adapter sub-phase)
