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
| Format-specific DML (`MERGE INTO`/`UPDATE`/`DELETE`) | 🚫 Fails closed | `MERGE INTO` confirmed via an existing FAIL-CLOSED test (`MergeIntoCommand`, real Spark analysis — see "Fail-closed on unverifiable writes" below). `UPDATE`/`DELETE` not individually re-probed this pass; both are explicitly named as deliberately-excluded, `Command`-shaped classes in `FailClosedCommands.scala`'s header comment, so they follow the same default-reject path by construction. **Next step:** genuinely the hardest row here to close — `ir.Write` models "replace/append the output of a query," not "apply a row-level predicate-conditioned mutation," so covering this for real needs an IR extension (something like `ir.Merge`/`ir.RowMutation`) before a `WriteCommandSupport` case is even meaningful, not just a new case against the existing shape. Left as a known, larger limitation rather than a near-term "add one case" item. |
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

**Net assessment:** Delta is not "100% supported" and no single pass makes
it so — but the gap is now fully enumerated instead of implicit, and every
remaining 🚫 row states what would actually close it, not just that it's
currently rejected. Two rows that were genuine, unenforced holes
(streaming writes, and streaming reads as a declared input — the
difference between "fails closed" and "silently unchecked"/"falsely
rejected" is exactly the distinction this ledger exists to keep visible)
are now ✅ Covered, alongside every V2 catalog write shape
(`AppendData`/`OverwriteByExpression`/`ReplaceTableAsSelect`). What
remains at 🚫 is exactly one row: row-level DML (`MERGE`/`UPDATE`/
`DELETE`), which needs a real IR extension before it's even attemptable —
not a "not supported, and that's fine" resting state, a deliberately
scoped-out piece of larger future work.

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

#### Incremental checking in CI

That 70%-on-new-code bar was originally a manual step (run a scoped
`sbt stryker` locally, read the report). `.github/workflows/test.yml`'s
`mutation-testing` job now automates it for every PR: a "Mutation test
changed files" step diffs against the PR's base commit
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
