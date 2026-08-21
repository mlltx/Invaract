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
- **The `runner` integration** (`PluginRunner.scala`) registers
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
| `InsertIntoHadoopFsRelationCommand` | `Write(DatasetRef(outputPath), ...)` |
| `LogicalRelation` (+ `HadoopFsRelation`) | `Read(DatasetRef(rootPath))` |
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

`runner/src/main/scala/com/example/runner/PluginRunner.scala` registers
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

## Contract verification (structural)

`ContractVerifier.verify(contract, actualSchema, lineage): VerificationResult`
(`spark-adapter/src/main/scala/com/example/sparkadapter/ContractVerifier.scala`)
is the first slice of ROADMAP.md Phase 1c's verification algorithm: it
checks a `Contract`'s declared output against a real Spark job's actual
output schema and traced lineage. For each contract-declared field it
checks presence, type compatibility (Spark's `DataType.typeName` and this
project's contract type vocabulary already share the same strings —
`"integer"`, `"long"`, `"string"`, ... — so this is a direct comparison),
and, for required fields, that `Lineage.trace` can account for it at all.
See the class doc for exactly what this does and does not check (no
nullability, no `rules` interpretation, extra columns not flagged).

`runner/PluginRunner.scala` runs this against the real demo pipeline on
every `./dev/test`, using `demo/contracts/invariant_output.yaml` — a real
contract for `InvariantPlugin`'s actual output (`id`, `value`,
`value_squared`, all `integer`). The result is kept in its own
`contractVerification` report section and console block, separate from
`ExecutionReport.status`: "did the Spark job execute" and "does its
output satisfy the contract" are different questions, and conflating them
would hide which one actually failed.

Actual console output from `./dev/test`:

```
Contract verification: PASS (invariant_demo_output)
  ✓ id: type 'integer' matches; sources: file:.../sample.csv.id
  ✓ value: type 'integer' matches; sources: file:.../sample.csv.value
  ✓ value_squared: type 'integer' matches; sources: file:.../sample.csv.value
```

`ContractVerifierSpec` proves this isn't a rubber stamp: alongside the
real pipeline passing its own contract, two deliberately broken contracts
(a required field the output doesn't have; a field with the wrong
declared type) are checked against the same real output and genuinely
fail, naming the specific field and reason.

## Testing

```bash
cd spark-adapter
sbt test
```

13 tests against a real `local[*]` `SparkSession` (no mocked plans):

- **`SparkPlanAdapterSpec`** (9) — a bare read, the worked example,
  filter+cast, self-join alias disambiguation, union, window, a UDF, an
  unsupported construct, and a full write captured end-to-end through
  `SparkAdapterListener`.
- **`ContractVerifierSpec`** (4) — the real demo pipeline's actual output
  passing its real contract; two deliberately broken contracts genuinely
  failing; a passing check reporting its traced source, not just a
  boolean.

To see it running against the actual demo pipeline:

```bash
./dev/test
# Console output includes "Transformation IR (translated from the real
# Spark logical plan):" followed by the rendered plan.
# demo/output/report.json's "transformationIR" section has the full
# rendered plan, lineage, and diagnostics.
```

---

**Last Updated:** 2026-08-21
**Status:** Spark adapter — initial implementation (ROADMAP.md Phase 1c, Spark Adapter sub-phase)
