# Spark Adapter

This document describes the Spark adapter: the bridge from Spark's Catalyst
logical plan into the Invaract [transformation IR](TRANSFORMATION_IR.md).
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
   ├─ Read(file:/home/user/Invaract/demo/input/sample.csv)
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
`SparkAdapterListener` before running `InvaractPlugin`, and after the
real `outputDf.write.mode("overwrite").parquet(outputPath)` call:

1. Waits (bounded, since `QueryExecutionListener` callbacks run
   asynchronously on Spark's own listener thread) for the listener to
   capture the write's translated IR.
2. Adds a `transformationIR` section to `demo/output/report.json`: the
   rendered plan, the traced lineage, and any diagnostics.
3. Prints the rendered plan to the console.

Actual output from `./dev/test` against the real `InvaractPlugin`
(`value_squared = value * value`, per `plugin/src/main/scala/com/example/plugin/InvaractPlugin.scala`):

```
Transformation IR (translated from the real Spark logical plan):
Write(file:/home/user/Invaract/demo/output/result.parquet)
└─ Project
   ├─ Read(file:/home/user/Invaract/demo/input/sample.csv)
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

## Connector support

Full per-connector write-ups — including the operation-surface and
feature-surface coverage ledgers required by
`.claude/skills/add-spark-connector/SKILL.md` Phase 11 — live under
[`docs/connectors/`](connectors/), one file per connector, not inline
here. This keeps this file's cross-cutting architecture content (write
command recognition, fail-closed policy, structural verification,
testing strategy) from growing unbounded as connectors are added — see
`docs/ADDING_A_SPARK_CONNECTOR.md` for why that split happened and what
belongs on which side of it.

| Connector | Notes | Ledgers |
|---|---|---|
| [Delta Lake](connectors/delta.md) | Row-level DML (`MERGE`/`UPDATE`/`DELETE`) translated via `deltaRowLevelDml`; schema-evolution and generated-column bugs found and fixed. | [Operation](connectors/delta.md#delta-lake-operation-surface-coverage-ledger) · [Feature](connectors/delta.md#delta-feature-by-feature-confidence-pass) |
| [Iceberg](connectors/iceberg.md) | Catalog-based DSv2 connector; `CALL` procedures classified individually (safe-list vs. fails-closed). | [Operation](connectors/iceberg.md#iceberg-operation-surface-coverage-ledger) · [Feature](connectors/iceberg.md#iceberg-feature-surface-coverage-ledger) |
| [Parquet](connectors/parquet.md) | Not a separate library — Spark's own bundled `FileFormat`, already on `provided`. | [Operation](connectors/parquet.md#parquet-operation-surface-coverage-ledger) · [Feature](connectors/parquet.md#parquet-feature-surface-coverage-ledger) |
| [CSV](connectors/csv.md) | Same shape as Parquet — Spark's own bundled `FileFormat`. | [Operation](connectors/csv.md#csv-operation-surface-coverage-ledger) · [Feature](connectors/csv.md#csv-feature-surface-coverage-ledger) |
| [Hive](connectors/hive.md) | `HiveTableRelation` reads plus three newly-translated write shapes; a static-partition `INSERT` false-rejection bug found and fixed. | [Operation](connectors/hive.md#hive-operation-surface-coverage-ledger) · [Feature](connectors/hive.md#hive-feature-surface-coverage-ledger) |
| [Avro](connectors/avro.md) | Reuses existing generic write cases with zero new `WriteCommandSupport` code; one general (non-Avro-specific) location bug found and fixed. | [Operation](connectors/avro.md#avro-operation-surface-coverage-ledger) · [Feature](connectors/avro.md#avro-feature-surface-coverage-ledger) |
| [ClickHouse](connectors/clickhouse.md) | Redirected from an infeasible BigQuery attempt; one new `deleteFromTable` write case; tested against a real standalone-binary server (no Docker in this environment). | [Operation](connectors/clickhouse.md#clickhouse-operation-surface-coverage-ledger) · [Feature](connectors/clickhouse.md#clickhouse-feature-surface-coverage-ledger) |

## Fail-closed on unverifiable writes

Every translation gap above — `.saveAsTable()` before
`CreateDataSourceTableAsSelectCommand` was added, Delta's `MERGE INTO`
today, and any future write shape this adapter hasn't been taught yet —
shares the same failure mode: `SparkPlanAdapter` produces `ir.Unsupported`
instead of `ir.Write`, and `ContractEnforcementRule.verifyOrThrow`
previously treated *any* non-`ir.Write` plan as "not a write, nothing to
gate." A write Invaract simply doesn't recognize was, until this change,
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
- **Hive support is closed — see "Hive support" below.** This bullet
  previously said `HiveTableRelation` fell through to the generic
  `LogicalRelation` handling's `catalogTable`-based fallback; verified
  against a real embedded-Derby Hive session, that turned out to be
  imprecise about the mechanism (worse than described, in fact —
  `HiveTableRelation` is not `LogicalRelation`-wrapped at all, so it fell
  all the way through to the fully generic `Unsupported` translation, not
  even the `catalogTable` fallback) but correct about the conclusion (a
  real, previously-untested gap). Both the read side and two real write-
  side false-rejection bugs are now fixed; one known, documented,
  unfixed location-resolution gap remains for CTAS with no pre-existing
  physical path — see "Hive support" for the full writeup and both
  coverage ledgers.
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
  "contract": "invaract_demo_output@1.0.0",
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
every `./dev/test`, using `demo/contracts/invaract_output.yaml`. Kept in
its own `contractVerification` report section and console block, separate
from `ExecutionReport.status`: "did the Spark job execute" and "does its
output satisfy the contract" are different questions, and conflating them
would hide which one actually failed.

Actual console output from `./dev/test`:

```
Contract verification: PASSED (invaract_demo_output@1.0.0)
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
Spark application → Logical plan → Invaract → PASS → execute
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
`demo/contracts/invaract_output_broken_example.yaml` — a contract
requiring a `customer_name` column the real `InvaractPlugin` never
produces:

```
Contract violation: 'invaract_demo_output@1.0.0' rejected this transformation. Write aborted.

What the contract expects:
  input  'orders' at demo/input/sample.csv: id: integer, value: integer
  output 'result' at demo/output/result_broken_example.parquet: id: integer, value: integer, value_squared: integer, customer_name: string

What the plan contains:
  Write(file:/home/user/Invaract/demo/output/result_broken_example.parquet)
  └─ Project
     ├─ Read(file:/home/user/Invaract/demo/input/sample.csv)
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

### Dry-run mode: inferring a contract instead of enforcing one

`ContractEnforcementRule.dryRun` (`ContractInference.scala`) answers a
different question from everything above: "I don't have a contract for
this job yet — what would one covering it actually look like?" Installed
via the same `injectCheckRule` mechanism as `forContract`, but with no
contract to enforce, so it only observes: never throws, never blocks a
write. On a recognized write, it builds a `Contract` from that write's
actual location/format/save-mode/schema and the schemas of every input the
write's plan reads — the same `collectInputSchemas` helper `verifyOrThrow`
itself uses, factored out specifically so the two can never disagree about
what counts as an input — and hands it to a caller-supplied callback.
`runner/DemoJobHarness` wires this up behind a `--dry-run` flag: no
contract is loaded at all, and the inferred contract is printed via
`ContractParser.write` (the new inverse of `ContractParser.parse`/
`parseFile`) instead of being enforced. See docs-site's "Dry-run mode"
guide for the user-facing walkthrough.

**Structure only, never business rules.** `rules` is always empty on an
inferred contract — there is no way to observe "this MERGE must always
match on customer_id" from watching one execution, the way `RuleType`'s
vocabulary expresses it. Every inferred field is marked `required: true`
(it genuinely was present in this run) with `nullable` taken directly from
Spark's own tracked nullability, not guessed. Row-level DML (MERGE/UPDATE/
DELETE) is out of scope for the same reason `WriteCommandInfo`'s DML cases
already document: those operations have no single "new output" to build a
dataset schema from.

**A real bug this caught.** The first implementation inferred a write's
raw `WriteCommandInfo.location` verbatim — for a local path, that includes
Spark's own `file:` scheme prefix (`file:/tmp/...`). `StructuralVerifier.locationsMatch`
only strips that prefix from the *actual* side of a comparison, expecting
a contract's *declared* location to already be scheme-less (the form every
hand-authored contract in `demo/contracts/` uses). An inferred contract
that skipped this normalization would therefore fail its own
`OUTPUT_LOCATION_MISMATCH` check the moment it was used with
`forContract` against the exact write it came from — confirmed by a real
round-trip test (`ContractInferenceSpec`, "an inferred contract ...
passes real enforcement of the write it came from") failing this way
before the fix. `ContractInference.normalizeLocation` strips the prefix
inferred locations the same way `locationsMatch` expects.

## Notification sinks

Everything above answers "does this write satisfy its contract" and, if
not, aborts it. `com.example.sparkadapter.notification`
(`spark-adapter/src/main/scala/com/example/sparkadapter/notification/`)
answers a different, additive question: how does an external system find
out that a check happened at all — PASS or FAIL — or that a write actually
completed? This is opt-in observability, not a new enforcement mechanism:
with no sink configured, nothing here runs, and every code path above is
unaffected.

**Two events, two moments, matching the two mechanisms already documented
above.**

- `ContractValidationEvent` — published by `ContractEnforcementRule` for
  every check it performs (the `ir.Write` branch, the state-changing-CALL
  branch, and both fail-closed branches — `UnverifiableWrite` and
  `InvalidContract`), always *before* a FAILED result's
  `ContractViolationException` is thrown. This is "the contract was
  evaluated, with this result," at analysis time — a FAILED event here
  means the write never executed.
- `WriteEvent` — published by `SparkAdapterListener`'s `onSuccess`, the
  same post-execution observation point `demo/output/report.json`'s
  `transformationIR` section already uses. This is "the write actually
  completed," strictly later than (and independent of) the check above —
  a write `ContractEnforcementRule` rejects never reaches this event,
  since Spark never executes it.

Both carry `metadata: Map[String, Any]`, copied verbatim from the active
contract's own `Contract.extensions` bag (see docs/CONTRACT_MODEL.md) —
whatever a contract author already recorded there (owner, team, upstream
system, anything ODCS or this project doesn't itself interpret) rides
along on every event, without a second, parallel metadata vocabulary.

**Both events also carry `applicationId: Option[String]`** — the owning
`SparkSession`'s `sparkContext.applicationId`, so a consumer aggregating
events from many concurrent jobs (or many runs of the same job over time)
can group by run without inventing its own correlation ID.
`ContractValidationEvent` gets it from the `SparkSession` captured by
`ContractEnforcementRule.forContract`'s closure at rule-installation time;
`WriteEvent` gets it from `qe.sparkSession` on the `QueryExecution`
`SparkAdapterListener.onSuccess` is handed. Always `Some` in practice for
both — there is no code path that constructs either event without a live
`SparkSession` — but kept `Option` rather than a bare `String` since a
`NotificationSink` implementation should not have to trust that no future
call site will ever construct one without a session in hand.

**`WriteEvent` additionally carries `durationMs: Long` and three
connector-dependent `Option[Long]` fields — `rowCount`, `bytesWritten`,
`fileCount`.** `durationMs` comes straight from the `durationNs` argument
Spark's own `QueryExecutionListener.onSuccess` callback already provides,
converted to milliseconds — always populated, no connector dependency.

The other three come from `qe.executedPlan.metrics` — Spark's own
`SQLMetric` map on the executed physical plan, read for the well-known
keys `numOutputRows`/`numOutputBytes`/`numFiles`. This was **confirmed
empirically, not assumed**, to behave differently across connectors:

- **Populated** for ordinary V1 writes (`InsertIntoHadoopFsRelationCommand`
  → `DataWritingCommandExec`) — plain Parquet/CSV/JSON/ORC writes via
  `DataFrameWriter.save`/`.parquet`/etc. all take this path, so
  `rowCount`/`bytesWritten`/`fileCount` are `Some` for the demo harness's
  own writes and are asserted as such in
  `SparkAdapterListenerSpec`.
- **Absent (`None`)** for Delta writes (`SaveIntoDataSourceCommand`/
  `AppendDataExecV1`) and Iceberg writes (`AppendDataExec`) — neither
  connector's write command populates these particular `SQLMetric` keys on
  the node this listener inspects, confirmed by a real local Delta write in
  `SparkAdapterListenerSpec` asserting all three are `None`. This is *not*
  a bug or an oversight to fix later by digging harder into the same
  mechanism — it is why these fields are honestly typed `Option[Long]`
  rather than `Long`, and why the docs-site guide below tells a reader not
  to expect them for Delta/Iceberg outputs. Getting equivalent numbers for
  those two connectors needs connector-specific investigation — see
  ROADMAP.md for status — not a different `SQLMetric` key.

**Configuration is a plain `.properties` file, deliberately not YAML and
deliberately not part of the contract document.** Sink configuration (an
endpoint, a file path, possibly credentials) is a deployment-environment
concern that varies independently of the contract, and `java.util.Properties`
needs no new dependency — this module has no direct dependency on
SnakeYAML (`contract`'s own YAML parser does, but nothing here reaches it
for this). `NotificationConfig.load` recognizes `sink.enabled`,
`sink.class` (a `NotificationSink` implementation's fully-qualified class
name, needing a public no-arg constructor), and `sink.property.<name>`
(passed to that sink's `configure` with the prefix stripped). See
docs-site's "Notification sinks" guide for the full worked example and key
reference.

**Reflective sink loading fails loudly at setup, quietly at publish time —
deliberately asymmetric, the same pattern `ContractEnforcementRule`'s own
fail-closed design uses elsewhere in this module.**
`NotificationSinkFactory.create` throws immediately for a misconfigured
sink (missing/misspelled class, no no-arg constructor, `configure`
rejecting its properties) — the same "fail loudly, at setup time"
treatment `ContractParser.parseFile` gives a malformed contract, since a
typo'd sink class silently producing zero events forever is worse than a
job that won't start. Once built, though, every sink is wrapped in
`SafeNotificationSink`, which catches and logs (never rethrows) any
exception a `publish` call raises — a broken or slow sink (a network call
timing out, an implementation bug) must never turn an otherwise-successful
contract check or write into a job failure, since notification is a
best-effort side channel, not part of enforcement.

**Four built-in sinks ship in `spark-adapter` itself, all dependency-free:**
`LoggingNotificationSink` (one JSON line per event via SLF4J, at INFO),
`FileNotificationSink` (appends one JSON line per event to a configured
`path` — what `./dev/test`/`./dev/regression` use to prove this against a
real Spark job; see below), `HttpNotificationSink` (POSTs each
event's JSON to a configured `url`, via `java.net.http.HttpClient` — part
of the JDK since Java 11, so this needs no new dependency either), and
`HadoopFsNotificationSink` (below).

Requests from `HttpNotificationSink`
are sent with `HttpClient.sendAsync`, not blocking whatever thread
`publish` was called on; a connection failure or non-2xx response is
logged at WARN once the async response lands, since by then there's no
call stack left for `SafeNotificationSink` to catch an exception on.
Tested against a real, local `com.sun.net.httpserver.HttpServer` (also
JDK-bundled) rather than a mocked `HttpClient` — the same "real thing over
a mock" discipline this module's Spark-facing specs already use.

**`HadoopFsNotificationSink`: one sink for S3, GCS, and HDFS, not three.**
Built on `org.apache.hadoop.fs.{FileSystem, Path}` — the exact same
abstraction Spark's own `DataFrameWriter` already dispatches every write
through, where the URI scheme (`s3a://`, `gs://`, `hdfs://`, `abfs://`,
plain `file://`) picks the concrete implementation at runtime via Hadoop's
own configuration-driven lookup, never a compile-time link to a vendor
SDK. This needs no new dependency in `spark-adapter` itself:
`hadoop-client-api`/`hadoop-client-runtime` (confirmed via
`sbt Test/dependencyTree` to be the shaded 3.3.4 artifacts Spark 3.5.7
actually resolves — not the classic unshaded `hadoop-common`) already
carry `FileSystem`/`Path` transitively through the existing `provided`
`spark-core`/`spark-sql` dependencies. It costs a real user nothing extra
either, for a structural reason, not a coincidence: if a contract's own
`location:` already points at `s3a://`/`gs://`, the job's runtime
classpath already carries `hadoop-aws`/the GCS connector for that write to
succeed at all — this sink piggybacks on exactly that, the same way
`FileNotificationSink` piggybacks on `java.io.FileWriter` already being
part of the JDK.

Configuration: `sink.property.path` (required, treated as a directory
prefix, not a single file) and `sink.property.hadoop.<key>` passthrough
into the `Configuration` this sink builds — Hadoop's own configuration
keys, not a second vocabulary. **One JSON object per event, not an
appended log** — deliberately, not an oversight:
`FileSystem.append` is not reliably supported across implementations, and
S3A in particular has never supported real append (S3 objects are
immutable) — an append-based design that works for `FileNotificationSink`'s
local files would silently misbehave the moment `sink.property.path`
pointed at `s3a://`. Writing each event as its own object under the
configured prefix, named `<timestamp>-<eventType>-<uuid>.json` to avoid
collisions, needs nothing more than `create`, universally supported.

Tested against a real `file://` `LocalFileSystem` — Hadoop's own built-in
`FileSystem` implementation, exercising the identical API surface
`hdfs://`/`s3a://`/`gs://` all implement, which is precisely the point of
the abstraction (Hadoop itself tests every `FileSystem` implementation
for consistent semantics against the same contract). Deliberately
*not* tested against a real HDFS cluster in this repository: Hadoop's own
`MiniDFSCluster` would need to be added as a dependency, and it pulls in
the classic, unshaded `hadoop-common`/`hadoop-hdfs` artifacts alongside
the shaded `hadoop-client-api`/`hadoop-client-runtime` Spark 3.5.7 already
resolves — real risk of reopening exactly the kind of transitive
classpath conflict this module's own `build.sbt` has extensively
documented fighting for Netty/Jackson/Arrow, for a test whose real
assertions are about this sink's own logic (path resolution, one-file-
per-event, the `hadoop.*` passthrough), not about Hadoop's `FileSystem`
contract itself. Real S3/GCS backends are further out of reach for this
repository's test environment entirely (no cluster, no cloud credentials)
— confirmed structurally correct via the shared `FileSystem` contract, not
separately verified end-to-end, and documented as such rather than forced
into a false claim of full verification.

**A fourth sink, Kafka, deliberately ships as a separate module/artifact
instead — `notification-kafka/`, not part of `spark-adapter`.** Unlike an
HTTP client, there's no JDK-bundled Kafka client, and unlike Delta/
Iceberg's narrow set of internal case classes (which `WriteCommandSupport`
reaches via reflection precisely so `spark-adapter` never needs those
libraries as a real dependency), `KafkaProducer`'s API is broad enough
that reflecting the whole thing would be unidiomatic — this needed a real,
unscoped `kafka-clients` dependency somewhere. Putting that somewhere in
its own module (mirroring how Spark itself ships `spark-sql-kafka-0-10`
as a wholly separate artifact from `spark-sql`, not bundled into it) means
a user who wants Kafka builds `notification-kafka`'s own assembled jar
(`cd notification-kafka && sbt assembly`) and adds it to their classpath;
a user who doesn't never resolves `kafka-clients` at all — `spark-adapter`'s
own `build.sbt` declares no dependency on it whatsoever, a stronger
guarantee than even the `test`-scoped connector dependencies (Delta,
Iceberg, Hive, Avro, ClickHouse) get, since those are still resolved to
build/test `spark-adapter` itself.

`KafkaNotificationSink.configure` treats `sink.property.topic` specially
(the destination topic) and passes every *other* `sink.property.*` key
straight through as a Kafka producer config
(`sink.property.bootstrap.servers`, `sink.property.security.protocol`,
...) — Kafka's own producer configuration keys, not a second vocabulary
this sink would otherwise have to invent and keep in sync with Kafka's.
Tested against `MockProducer` — `kafka-clients`' own officially-supported
test double for exactly this purpose, substituted via a `private[kafka]`
constructor the reflective `NotificationSinkFactory` path never uses —
rather than a hand-rolled mock or standing up a real broker.

A real destination beyond these four (a different message queue, a metrics
system, cloud pub/sub) is ordinary user code implementing
`NotificationSink` — nothing here requires it to live in this module, this
repository, or even the JVM ecosystem's dependency-free constraints these
four happen to satisfy. `NotificationJson.toJson` is public specifically
so such a sink can reuse this module's event serialization instead of
reinventing it, whether or not it lives in the same jar.

**API shape: additive overloads, not new parameters on existing
signatures**, per CLAUDE.md's "API Compatibility Requirement" — adding a
parameter to `ContractEnforcementRule.forContract`'s existing signature
(even with a default) changes its compiled descriptor, a binary break for
any already-compiled caller. Instead: `forContract(contract, options,
sink: NotificationSink)` is a new overload; the original
`forContract(contract, options = ...)` is untouched, delegating to the new
one with no sink. `SparkAdapterListener` similarly gained two new
auxiliary constructors (`(sink, contractRef, metadata)` and the
`(sink, contract: Option[Contract])` convenience) alongside its original,
still-present zero-arg constructor — every existing `new
SparkAdapterListener()` call site in this repo (and any real user's code)
keeps compiling and keeps its exact prior behavior unchanged. Confirmed by
`sbt mimaReportBinaryIssues` staying clean.

**Live-demonstrated against a real Spark job, not just unit-tested.**
`demo/notify.properties` configures a `FileNotificationSink` writing to
`demo/output/events.jsonl`; `./dev/test` passes it to `DemoJobHarness` and
asserts both a `CONTRACT_VALIDATION` and a `WRITE` event actually landed
in that file from a real `spark-submit` run. `./dev/regression` goes
further, using two more instances of the same config
(`demo/regression-notify-{pass,fail}.properties`) to prove the asymmetry
that matters most: the PASS case's events file contains both a PASSED
`ContractValidationEvent` and a `WriteEvent` (the write really happened),
while the FAIL case's contains a FAILED `ContractValidationEvent` and *no*
`WriteEvent` at all (the write never executed) — the same "no output file
on disk" proof the FAIL case's enforcement assertion already relies on,
now extended to the notification side of the same rejection.

## DML rule verification

Every check above (`StructuralVerifier`, and `ContractEnforcementRule`'s
row-level-DML structural checks — target location/schema, MERGE's source
as an input) verifies the *shape* of what's written. It has never checked
a contract's `rules` — recorded by `contract` since Phase 1a, never
interpreted (see docs/CONTRACT_MODEL.md's "What Phase 1 Does *Not* Do
Yet"). `RuleVerifier`
(`spark-adapter/src/main/scala/com/example/sparkadapter/RuleVerifier.scala`)
closes the first slice of that gap: the three DML rule types
`ContractRule.interpret` decodes (see docs/CONTRACT_MODEL.md's
"Interpreted rules") — `merge_condition`, `forbid_unconditional_delete`,
`allowed_update_columns` — checked against a real Spark MERGE/UPDATE/
DELETE.

**Extraction is a separate, parallel path from `WriteCommandSupport`, not
a change to it.** `RowMutationSupport`
(`spark-adapter/src/main/scala/com/example/sparkadapter/RowMutationSupport.scala`)
matches the exact same Delta `UpdateCommand`/`DeleteCommand`/
`MergeIntoCommand` classes (plus DSv2's plain `DeleteFromTable`)
`WriteCommandSupport.deltaRowLevelDml`/`deleteFromTable` already
recognize, but extracts a different fact: `ir.RowMutation`, not
`WriteCommandInfo`. This was a deliberate design choice, not an
oversight — adding a field to `WriteCommandInfo` (or to `ir.Write`
itself) to carry this would have been a binary-incompatible change to an
already-published case class constructor; a second, independent
extractor is the MiMa-safe way to add a new fact these commands carry,
mirroring `StateChangingCallSupport`'s existing relationship to
`WriteCommandSupport`.

**What's extracted, confirmed empirically, not assumed:**

- **MERGE's `ON` condition** (`MergeIntoCommand.condition()`, a plain
  `Expression`, always present) — translated via a new
  `SparkPlanAdapter.translateExprStandalone` entry point (a throwaway
  `Translator` instance; the only public/package-private way another file
  in this module can reach `Translator.translateExpr`, which is otherwise
  `private` to `SparkPlanAdapter`'s object body, not just
  `private[sparkadapter]`).
- **UPDATE's assigned columns.** `UpdateCommand.updateExpressions()` is
  always aligned 1:1 with `target.output` — confirmed by reading Delta
  3.2.0's own source
  (`PreprocessTableUpdate.toCommand`/`UpdateExpressionsSupport.generateUpdateExpressions`),
  not assumed: a column the SQL `SET` clause doesn't mention gets back
  its *original* `target.output` attribute (Delta's own `defaultExpr`
  fallback) as that column's entry. So `updatedColumns` is exactly the
  columns where `updateExpressions(i)` is not `semanticEquals` to
  `target.output(i)` — genuinely changed, not Delta's own passthrough.
- **Whether a DELETE is unconditional.** Delta's `DeleteCommand.condition()`
  is `Option[Expression]` (`None` = unconditional). DSv2's plain
  `DeleteFromTable.condition` is different — confirmed via Spark 3.5.1's
  own parser (`AstBuilder.visitDeleteFromTable`): a bare `DELETE FROM t`
  with no `WHERE` sets `condition` to `Literal.TrueLiteral`, never `None`.
  `RowMutationSupport.deleteScopeOf` normalizes both into the same
  `DeleteScope`.

**Iceberg (and any DSv2 `SupportsRowLevelOperations` connector) is now
covered too, for two of the three rules unconditionally and the third
for its common case.** `WriteCommandSupport.dsv2RowLevelWrite`'s
`ReplaceData`/`WriteDelta` nodes are Spark's own *rewritten* form of the
operation, not the original command — a first pass judged them not to
carry a clean fact to extract and deferred this deliberately (see
ROADMAP.md's "Full semantic DML verification" item's history). A real
investigation (a throwaway probe against a live Iceberg 1.11.0 session,
since deleted, plus reading Spark 3.5.1's
`RewriteDeleteFromTable`/`RewriteUpdateTable`/`RewriteMergeIntoTable`
source) found it was more extractable than assumed:

- **`RowLevelWrite.condition`** (present on both `ReplaceData` and
  `WriteDelta`, copy-on-write and merge-on-read alike) is, per Spark's
  own rewrite rules, exactly the original predicate — a MERGE's `ON`
  clause, or a DELETE/UPDATE's `WHERE` clause (`Literal.TrueLiteral` if
  absent). No reflection needed, unlike Delta: `RowLevelWrite`/
  `ReplaceData`/`WriteDelta`/`RowLevelOperation` are real, stable, public
  Spark connector-API types.
- **`RowLevelWrite.operation.command()`** (`org.apache.spark.sql.connector.write.RowLevelOperation.Command`)
  reliably reports which of MERGE/UPDATE/DELETE a plan represents,
  confirmed against real captured plans for all three, both write
  strategies — this is what lets `merge_condition`/
  `forbid_unconditional_delete` be checked identically to Delta's, and is
  also what makes the UPDATE gap below precise rather than a guess.
- **UPDATE's assigned columns are extractable for copy-on-write
  (`ReplaceData`) only.** Its rewritten `query` is a `Project` where
  *every* target column is wrapped `Alias(If(matchCondition, assignedExpr,
  originalAttr), name)` — confirmed empirically: an untouched column
  produces `if (cond) id else id AS id` (the identical attribute on both
  branches), a genuinely reassigned one `if (cond) (doubled + 1) else
  doubled AS doubled`. So a column changed iff its `If`'s two branches
  aren't semantically equal (`RowMutationSupport.updatedColumnsOfReplaceData`).
  Merge-on-read's `WriteDelta` rewrites UPDATE to a structurally
  different `Expand`-based plan (one row-operation-tagged output row per
  insert/delete, confirmed via the same probe) with no equivalent
  per-column pairing — deliberately not attempted, see the fail-closed
  behavior below for what happens instead of silently reporting zero
  changed columns.

**Recognized-but-unextractable is not the same as inapplicable — fail
closed, don't silently skip.** `RowMutationSupport.classify` returns one
of three things: `None` (`plan` isn't row-level DML at all — most
writes; a DML rule is simply inapplicable, same as before), `Some(Extracted(kind,
mutation))` (recognized and successfully extracted — the normal path,
above), or `Some(Unverifiable(kind))` — genuinely `kind`-shaped DML (a
real MERGE/UPDATE/DELETE) that this module could not extract facts for.
Two concrete cases reach `Unverifiable` today: a future Delta version
renaming a reflected method (this already fell through to the general
`UnverifiableWrite` fail-closed policy for the *write as a whole*; now
distinguished for rule-checking specifically), and Iceberg's
merge-on-read UPDATE, above. Before this existed, a contract's
`allowed_update_columns` rule against a merge-on-read UPDATE would
execute, report no violation, and provide no protection — the exact
silent gap this closes. `ContractEnforcementRule.verifyOrThrow` checks
`RuleVerifier.appliesTo(rule, kind)` before treating an `Unverifiable`
classification as a problem, so an operation kind the active contract
declares no rule for still passes normally — a merge-on-read UPDATE
under a contract that only declares `forbid_unconditional_delete` isn't
spuriously rejected. The new `RULE_UNVERIFIABLE_DML` violation type gets
the same abort-before-any-data-is-written treatment as every other
violation.

**Each rule only constrains the DML shape it names.** A `merge_condition`
rule is silently inapplicable (not violated) to a mutation with no match
condition; `forbid_unconditional_delete` to one with no delete;
`allowed_update_columns` to one that updates no columns — the same
"declared but not every check is always relevant" relationship
`StructuralVerifier`'s own `VerificationOptions` toggles have. This is
distinct from `Unverifiable` above: `Extracted(kind, mutation)` with a
kind-mismatched rule is *inapplicable* (nothing to check); `Unverifiable`
with a kind-matched rule is a real gap that fails closed.

**Wired into `ContractEnforcementRule.verifyOrThrow` alongside, not
instead of, `StructuralVerifier`.** In the `ir.Write` branch,
`RowMutationSupport.classify(plan)` is `None` for every write shape that
isn't row-level DML — a no-op for the vast majority of writes a contract
governs — `Extracted` feeds `RuleVerifier.verify`, and `Unverifiable`
feeds the `appliesTo` check above. Either way, any resulting violations
are appended to `StructuralVerifier`'s before the combined pass/fail
decision, so a rule violation (or an unverifiable one) gets the exact
same abort-before-any-data-is-written guarantee and four-part
`explain()` treatment every other violation type gets.

**Live-tested against real Delta and real Iceberg (both copy-on-write and
merge-on-read), PASS and FAIL, per rule type**
(`ContractEnforcementRuleSpec`, `IcebergConnectorSpec`): a MERGE matching
on the declared column executes normally, one missing a declared column
is aborted before touching the table; a filtered DELETE executes
normally, an unconditional one is aborted; a copy-on-write UPDATE
assigning only allowed columns executes normally, one assigning a
disallowed column is aborted; a merge-on-read UPDATE under an
`allowed_update_columns` rule is aborted with `RULE_UNVERIFIABLE_DML`
(and, distinctly, executes normally when no such rule is declared —
proving the fail-closed check doesn't over-reject). Every FAIL case
asserts the target table's rows are byte-identical before and after the
aborted attempt, the same discipline every other enforcement test in
this file uses. `RuleVerifierSpec` covers the pure-Scala logic directly
(no Spark session needed, since `RowMutation`/`ContractRule` are both
plain data) — every rule type's inapplicable case, PASS, and FAIL, an
unrecognized/malformed rule contributing no violations, and
`RuleVerifier.appliesTo`'s kind-matching truth table.

**`merge_condition` checks genuine equality pairing, not just "the column
is referenced somewhere."** The first version of this check
(`RuleVerifier.checkMergeCondition`) only confirmed each declared column
appeared *anywhere* in the MERGE's `ON` condition — a real gap, since a
range check (`t.customer_id > 0`), a literal comparison (`t.customer_id =
'ACME'`), or an `OR` branch (`t.id = s.id OR t.region = s.region`, which
only requires *one* side to hold) all reference a column without the
MERGE actually matching target against source on it. `RuleVerifier.equalityPairedColumns(expr)`
closes this: it recursively flattens top-level `&&` conjuncts (Catalyst's
own `And.symbol`, confirmed via `SparkPlanAdapter.translateExpr`'s
`BinaryOperator` case — not the SQL keyword `"AND"`) and only counts a
column as matched if it's a bare operand of a top-level `=`/`<=>`
(null-safe equality) comparison against another bare column reference —
`t.customer_id = s.customer_id`, or even `t.customer_id = s.cust_id`
(the declared name only has to appear on *one* side, since a contract
could reasonably be authored against either the target's or the source's
naming). An extra, non-equality conjunct beyond the declared columns
(an additional partition-pruning predicate, say) is still tolerated, not
flagged — checking more than required was never the failure this rule
guards against.

Still a structural approximation, not full predicate logic:
`equalityPairedColumns` doesn't descend into `||`, `NOT`, or `CASE WHEN`
(no De Morgan-equivalence reasoning), and doesn't distinguish target- from
source-side qualifiers — two columns on the *same* side compared to each
other would still count as a pairing. Both are documented, deliberate
scope limits (see ROADMAP.md's "Full semantic DML verification" item),
not oversights.

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
- **`ContractEnforcementRuleSpec`** (this list of counts predates most of
  the connector work below and is stale on the total — see each
  connector's own doc, e.g. docs/connectors/delta.md, for what's actually
  covered today) — PASS executes and creates output; FAIL aborts before
  any data is written; the explanation contains all four required
  sections; the same violation produces byte-identical explanations
  across repeated attempts; non-write queries never trigger verification
  even under an always-failing contract; `VerificationOptions` thread
  through the enforcement path; `forContract`'s public entry point works
  directly; `explain` pluralizes the violation count and marks optional
  fields distinctly; and, per rule type (`merge_condition`/
  `forbid_unconditional_delete`/`allowed_update_columns`), a real PASS and
  a real FAIL against a live Delta session — see "DML rule verification"
  above.
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
- **`lr.catalogTable.isEmpty`, the `LogicalRelation` fallback's final
  catch-all branch** (`SparkPlanAdapter.scala:319`) — previously
  attributed to "no Hive metastore available to exercise it"; re-checked
  against a real Hive-enabled session once "Hive support" (above) added
  one, and it's still `NoCoverage`, for a different and more precise
  reason now confirmed rather than assumed: a genuine Hive-native table
  read never reaches this branch at all — it takes the dedicated
  `HiveTableRelation` case instead (`HiveTableRelation` is its own
  top-level `LeafNode`, not `LogicalRelation`-wrapped). This branch is
  for some *other*, non-`HadoopFsRelation`/non-`JDBCRelation`/non-Hive
  `BaseRelation` wrapped in a `LogicalRelation` with no populated
  `catalogTable` — no such relation kind is exercised anywhere in this
  module's test suite, Hive included.
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

#### Mutation testing: the Parquet streaming-sink fix

`WriteCommandSupport.scala` rescoped after the Parquet connector pass's
`FileStreamSink` fix (see "Parquet support" above): **80.0%** (24/30
non-excluded mutants killed). Every survivor investigated by hand, not
just cited by percentage:

- **4 pre-existing, untouched by this pass**: `unwrapWriteWrapper`'s
  already-documented no-wrapper branch (two mutants — Spark 3.4+ always
  inserts the `WriteFiles` wrapper, so this branch has no reachable
  real-world trigger under the pinned version), and two guards inside
  `deltaRowLevelDml` (`catalogTable.isDefined`, which only changes
  diagnostic *message wording*, not the fallback value itself, and
  `!deltaDmlClassNames.contains(...)`) — neither touched by this pass's
  actual change.
- **1 killed**: the `EqualityOperator` (`==`→`!=`) mutant on
  `streamSinkFormatOf`'s new `FileStreamSink` guard, killed by
  `ParquetConnectorSpec`'s direct-construction test asserting
  `format = Some("parquet")`.
- **1 real survivor in the new code, confirmed genuinely equivalent, not
  left unexamined**: the same guard's `ConditionalExpression`-to-`true`
  mutant. Checked by hand: for a real `FileStreamSink` (the only sink
  this branch is ever reached for once `DeltaSink` is ruled out by the
  preceding `if`), forcing the guard to unconditionally `true` produces
  the *identical* result the real condition already does — the branch
  was already being taken. For any other sink that could theoretically
  reach this position, the reflective `fileFormat` field lookup
  (`reflectivePrivateField`) fails either way (no such field exists),
  producing `None` either way. No test — real or hypothetical — could
  ever distinguish the two, the same class of provable equivalence as
  the already-documented `JDBCRelation`-guard survivor above.

This pass's first fix attempt (special-casing `FileStreamSink` inside
`streamSinkLocationAndFormat`'s `sink.name()` guard, rather than
extending `reflectiveSinkPath`) would have added exactly this kind of
unkillable, equivalent-mutant code for no behavioral benefit — a manually
applied mutation of that guard produced no observable test failure,
which is what prompted re-diagnosing the bug's real location (see
"Parquet support" above) rather than writing a test to force a kill of
what turned out to be dead code. Removed outright rather than kept and
documented as equivalent: the correct fix needed no such guard at all.

#### Mutation testing: Hive connector support

Scoped to the three files this pass changed (`WriteCommandSupport.scala`,
`SparkPlanAdapter.scala`, `ContractEnforcementRule.scala`): **80.0%** (of
total) / **81.01%** (of covered code). `ContractEnforcementRule.scala`
has zero survivors — its new `HiveTableRelation` case in `recognizedRead`
is fully killed. Every survivor investigated by hand, not just cited by
percentage; ten predate this pass entirely (re-surfaced only because
scoping covers the whole file, not because this pass touched them):

- **9 pre-existing, untouched by this pass**: `deltaRowLevelDml`'s class-
  name guard and its `catalogTable.isDefined` diagnostic branch (×2),
  `unwrapWriteWrapper`'s no-wrapper branch (×2), `streamSinkFormatOf`'s
  `FileStreamSink` guard, and — in `SparkPlanAdapter.scala` —
  `StreamingRelation`'s path-option check, `StreamingRelationV2`'s no-
  location branch, and the `JDBCRelation` near-equivalent. All already
  documented above; none touched by this pass's actual changes.
- **1 pre-existing, but its own documented *reasoning* corrected**:
  `lr.catalogTable.isEmpty` (`SparkPlanAdapter.scala:319`, `NoCoverage`)
  was previously attributed to "no Hive metastore available to exercise
  it" — now that one exists (this pass's own test session), it's
  confirmed to still be `NoCoverage`, for the real reason: a genuine Hive
  table read never reaches this branch at all, since it takes the new
  dedicated `HiveTableRelation` case instead. This branch is for some
  *other* relation kind (non-`HadoopFsRelation`/`JDBCRelation`/Hive)
  wrapped in a `LogicalRelation` with no `catalogTable` — genuinely not
  exercised by anything in this suite. See the corrected bullet earlier
  in this section.
- **1 real, closed mutant, found then killed, not left**: `insertIntoHiveTable`'s
  `saveMode = Some(if (overwrite) "overwrite" else "append")` ternary —
  every test that reached this code exercised the `overwrite` branch
  (`INSERT OVERWRITE`, or a `.saveAsTable()` append that happens to
  default to it); nothing asserted the `append` case explicitly. Killed
  by adding a dedicated translation test for a plain `.insertInto()`
  asserting `saveMode.contains("append")`.
- **3 confirmed genuinely equivalent, same reasoning as the pre-existing
  `JDBCRelation`/`deltaRowLevelDml` guards above**: `createHiveTableAsSelect`'s,
  `insertIntoHiveTable`'s, and `insertIntoHiveDir`'s own
  `plan.getClass.getName != <expected>` type guards. Each feeds a
  `scala.util.Try { ... }.toOption` block reflectively calling methods
  that only exist on the expected class — mutating the guard to always
  proceed makes the reflection throw for any other plan (caught by
  `Try`, producing `None`, the same result the guard's own `if (...)
  None` branch already gives). No test could ever distinguish the two.
- **1 confirmed unreachable via any real Spark SQL, not just untested**:
  `insertIntoHiveDir`'s own `saveMode = Some(if (overwrite) "overwrite"
  else "append")` ternary. Unlike `InsertIntoHiveTable` (where plain
  `INSERT INTO` genuinely sets `overwrite = false`), Hive/Spark SQL's
  grammar has only one form of this statement — `INSERT OVERWRITE
  [LOCAL] DIRECTORY ...` — there is no `INSERT INTO ... DIRECTORY`
  syntax to parse `overwrite = false` from, so the `else` branch has no
  real caller to write a test against.
- **1 new, real, and left — a defensive guard genuinely unreachable in
  this environment**: `SparkPlanAdapter`'s new `htr.tableMeta.storage.locationUri.isEmpty`
  check (mirrors every other "no precise location" diagnostic guard in
  this file). Every real Hive table created via `CREATE TABLE` in a live
  session already has a resolved storage location by the time it's
  readable — the same practical unreachability `CreateDataSourceTableAsSelectCommand`'s
  and `SaveIntoDataSourceCommand`'s analogous "no location" diagnostics
  already have, just never previously exercised for Hive specifically.

Net: every survivor in code this pass actually added or changed is
either a confirmed equivalent mutant, a confirmed real gap that's now
closed, or a confirmed environment-unreachable defensive branch — none
left uninvestigated. `mimaReportBinaryIssues` is clean (no public
signature changed — the new write/read cases are additions inside
existing private matchers and one new `private[sparkadapter]` helper).

#### Mutation testing: Avro connector support

Scoped to the one file this pass changed:
`sbt stryker --mutate "src/main/scala/com/example/sparkadapter/WriteCommandSupport.scala"`
— **76.74%** (33/43 non-excluded mutants killed; 145 generated, 102
excluded as `StringLiteral`). Clears the 70% break threshold. All 10
survivors fall **outside** the code this pass added or changed
(`createDataSourceTableAsSelect`'s new `SessionCatalog.defaultTablePath`
fallback, lines 144–165) — confirmed by cross-referencing every
survivor's reported line number against the diff, not assumed from the
percentage alone:

- `WriteCommandSupport.scala:859` / `:923` / `:828` — `insertIntoHiveTable`/
  `insertIntoHiveDir`/`createHiveTableAsSelect`'s own class-name type
  guards, already documented as genuinely equivalent mutants in the Hive
  mutation-testing subsection above (a `scala.util.Try` reflection block
  that produces the identical `None` result whether the guard or the
  reflective call itself rejects a non-matching plan).
- `WriteCommandSupport.scala:691` (×2) — `deltaRowLevelDml`'s
  `catalogTable.isDefined` diagnostic branch, already documented above as
  a pre-existing survivor.
- `WriteCommandSupport.scala:293` — `streamSinkFormatOf`'s `FileStreamSink`
  guard, already documented above.
- `WriteCommandSupport.scala:974` (×2) — the `WriteFiles`-unwrapping
  guard in `unwrapWriteWrapper`, already documented above.
- `WriteCommandSupport.scala:680` — `deltaRowLevelDml`'s own class-name
  guard, already documented above.
- `WriteCommandSupport.scala:940` — `insertIntoHiveDir`'s `saveMode`
  ternary, already documented above as unreachable via any real Spark SQL
  grammar (no `INSERT INTO ... DIRECTORY` syntax exists to parse
  `overwrite = false` from).

None of these ten are new: every one was already investigated and
documented (as a confirmed-equivalent mutant, a pre-existing gap, or a
grammar-unreachable branch) during Hive's own mutation-testing pass
above — re-surfacing here only because this run's scope is the whole
file, not because this pass's actual change touched any of them.
**Zero real Survived/NoCoverage mutants exist in the code this pass
added** — the `createDataSourceTableAsSelect` location fix has no
uncovered branch of its own. `mimaReportBinaryIssues` is clean (no
public signature changed — the fix is entirely internal to an existing
`private[sparkadapter]` match case; `WriteCommandInfo`'s own shape is
unchanged).

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

