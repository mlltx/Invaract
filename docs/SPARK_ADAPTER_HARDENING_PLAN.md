# Spark Adapter Hardening Plan

This is the record of a battle test of `spark-adapter` and the plan for closing every gap it
found. It is written so that someone who was not part of the test can implement each item
without re-deriving anything: every item states what was observed, why it happens, what to
build, which tests prove it, and which documents change.

- **Tested at:** commit `71554c6` (`main`), Spark 3.5.9, Delta 3.3.3, Scala 2.12.18, JDK 21.
- **Baseline:** `./dev/build` passed on that commit, including the whole `spark-adapter` suite.
- **Method:** about 300 adversarial jobs run against a real `local[4]` Spark session with the
  enforcement rule installed, plus about 30 real `spark-submit` runs of PySpark jobs that know
  nothing about Invaract. Three people were played:
  - **the Spark expert** — a data engineer who uses every corner of Spark (subqueries, CTEs,
    streaming, DDL, RDDs, deep plans, concurrency);
  - **the newcomer** — an entry-level user following the docs: dry run, copy the draft, turn
    enforcement on;
  - **the platform admin** — the team that attaches Invaract and an organizational policy to
    jobs it does not own, and needs to know the attachment holds.

Everything in "Observed" was seen in a real run, not inferred from reading code, unless it is
marked *(by reading the code)*. Fixes marked **prototyped** were tried against the real session
and behaved as described.

## How to read an item

Each item has a severity:

| Severity | Meaning |
|---|---|
| **S1** | A check silently does not happen: something the contract or the platform forbids goes through, or a report states something false. |
| **S2** | A legitimate job cannot run under enforcement (a false rejection or a crash) and the user has no way round it. |
| **S3** | Misleading messages, friction, or an operational rough edge. |

## Summary

| ID | Sev | Who is hit | Gap | Size |
|---|---|---|---|---|
| [H-01](#h-01-drop-database--cascade-passes) | S1 | expert, admin | `DROP DATABASE … CASCADE` passes and deletes declared outputs | small |
| [H-02](#h-02-reads-inside-subqueries-are-invisible) | S1 | expert, admin | Reads inside `IN`/`EXISTS`/scalar subqueries are invisible | medium |
| [H-03](#h-03-a-read-of-several-paths-shows-only-the-first-and-a-read-of-part-of-an-input-does-not-match-it) | S1 + S2 | everyone | A multi-path or glob read shows only its first path; a read of a partition folder or file of a declared input is `MISSING_INPUT` | large |
| [H-04](#h-04-an-rdd-backed-dataframe-passes-rejectundeclaredinputs) | S1 | admin | `createDataFrame(df.rdd)` passes `rejectUndeclaredInputs` | small |
| [H-05](#h-05-alter-table-on-a-declared-output-is-never-checked) | S1 | expert, admin | `ALTER TABLE` (add/drop/rename column, set location, rename) on a declared output is never checked | large |
| [H-06](#h-06-a-rejected-write-counts-as-the-output-having-been-written) | S1 | admin | A rejected write still counts as "the output was written" | small |
| [H-07](#h-07-the-job-can-detach-invaract-or-loosen-its-settings) | S1 | admin | A misspelt class name, or the job's own `SparkSession.builder.config(...)`, removes Invaract or loosens it, silently | large |
| [H-08](#h-08-a-misspelt-sparkinvaract-setting-is-ignored) | S1 | admin, newcomer | A misspelt `spark.invaract.*` setting is ignored | small |
| [H-09](#h-09-a-deep-plan-crashes-the-job-with-stackoverflowerror) | S2 | expert | A deep plan crashes a plain `count()` with `StackOverflowError` | medium |
| [H-10](#h-10-drop-table-truncate-and-drop-partition-cannot-be-used-at-all) | S2 | everyone | `DROP TABLE IF EXISTS`, `TRUNCATE`, `DROP PARTITION` cannot be used at all; `DROP VIEW` and `CLEAR CACHE` are rejected | large |
| [H-11](#h-11-every-foreachbatch-stream-is-rejected) | S2 | expert | Every `foreachBatch` stream is rejected | medium |
| [H-12](#h-12-a-jdbc-write-can-never-match-its-declared-output) | S2 | expert | A JDBC write can never match a declared output | small |
| [H-13](#h-13-insert-overwrite-directory-is-rejected) | S2 | expert | `INSERT OVERWRITE DIRECTORY … USING` is rejected | small |
| [H-14](#h-14-lineage-is-lost-through-views-ctes-and-plain-copies) | S1 | admin | Column lineage and sensitivity tags are lost through temp views, views, CTEs and plain copies | medium |
| [H-15](#h-15-contract-and-policy-files-must-be-local-files) | S2 | admin | The contract, policy, location map and sink config must be local files | medium |
| [H-16](#h-16-one-write-publishes-two-or-three-validation-events) | S3 | admin | One write publishes two or three validation events | medium |
| [H-17](#h-17-messages-and-warnings-that-mislead-a-newcomer) | S3 | newcomer | Six messages and warnings that mislead | small each |
| [H-18](#h-18-documentation-gaps) | S3 | everyone | Documentation gaps, including the limits of what Invaract on Spark can see | medium |

[What was tested and held](#what-was-tested-and-held) lists what the battle test did not break,
and [Not fixed, by decision](#not-fixed-by-decision) lists what it found that this plan
deliberately leaves alone.

## Order of work

Do the items in this order. Each is one pull request unless it says otherwise.

1. **H-01, H-06, H-08, H-12, H-13** — small, independent, no design risk. Do these first to
   learn the module's test and CI conventions.
2. **H-09** — makes every later item safer to test on big plans.
3. **H-14** — changes the IR the adapter emits; do it before H-02 and H-03, which build on it.
4. **H-02**, then **H-03**, then **H-04**.
5. **H-11** — needs H-03.
6. **H-10**, then **H-05** — they share the "which declared output is this table" helper H-10
   introduces.
7. **H-07**, **H-15**, **H-16**.
8. **H-17**, **H-18** — H-18 last, because it documents the final behavior of everything above.

H-02, H-03 and H-14 change the IR produced for some queries, so the
[fingerprint](SEMANTIC_LINEAGE_FINGERPRINTING.md) of those jobs changes. Ship the three in one
release and say so in its notes (see "Rules for every item", rule 9).

## Rules for every item

These restate what `CLAUDE.md` requires, as a checklist. An item is not done until all of
them hold. If a step fails and the item does not tell you what to do, stop and report it; do
not work round it.

1. **Write the failing test first.** Each item names its tests. Write them, run them, and see
   them fail for the reason the item describes before changing any main code. If a test
   passes before the fix, the test is wrong or the gap has already been closed: stop and check.
2. **Tests use a real Spark session.** Extend `ConnectorSpecBase`
   (`spark-adapter/src/test/scala/com/invaract/sparkadapter/ConnectorSpecBase.scala`) or follow
   `ContractEnforcementRuleSpec`: build one `SparkSession` in `beforeAll`, call
   `injectContractCheck`, switch contracts with `withContract(yaml, options) { ... }`. Never
   mock a plan. A test that needs the real `--conf` path follows
   `InvaractSparkSessionExtensionSpec`; one that needs its own JVM (environment variables, exit
   status) follows `DeclaredOutputsSpec` and `MissingOutputJob`.
3. **Assert both directions.** Every "X is now rejected" test has a partner "the legitimate
   neighbour of X still passes", and the reverse. A rejection test asserts the violation type
   (`e.result.violations.map(_.violationType)`) and that the forbidden effect did not happen
   (the file or table is unchanged), not only that something was thrown.
4. **Mutation testing.** For every file you add or change under a module's `src/main/scala`,
   run from that module's directory
   `sbt 'stryker --mutate "src/main/scala/com/invaract/.../YourFile.scala"'` and reach 70%.
   Write the tests so a flipped comparison, a negated condition, a swapped `&&`/`||` or an
   off-by-one would each fail one of them; the items name the boundaries that matter.
5. **A new file under `spark-adapter/src/main/scala` goes into a mutation shard.** Add its path
   to one `files:` list of the `mutation-testing-spark-adapter` job in
   `.github/workflows/test.yml`. `shard-6` and `shard-9` hold four files each and are the
   smallest multi-file shards today; use `shard-6` for the first new file, `shard-9` for the
   second, and keep alternating. The `mutation-shard-drift-check` job fails otherwise.
6. **Coverage.** `cd spark-adapter && sbt coverage test coverageReport` must stay above the
   thresholds in `spark-adapter/build.sbt` (92% statements, 88% branches).
   `verification-core` has its own (92% and 89%). Never lower a threshold to get a pass.
7. **Binary compatibility.** Everything this plan adds to `spark-adapter` is
   `private[sparkadapter]` unless the item says "public". Do not change the signature of any
   existing public method or case class. If CI's `api-compatibility` job fails, you changed
   something public by accident: undo it.
8. **Real job.** Run `./dev/test` (exit 0, `demo/output/report.json` status `PASS`) and
   `./dev/regression`. Both must pass after every item.
9. **Documents.** Each item lists the pages to change. Besides those: an item that changes
   what a capability's note says updates
   `spark-adapter/src/main/resources/invaract-capabilities-spark.yaml` and then runs
   `./dev/capabilities` to regenerate the pages built from it; an item that changes the IR for
   some query shape adds a line to the "Fingerprints change when" list this plan asks H-14 to
   start in `docs-site/src/content/docs/guides/fingerprinting-transformations.mdx`. Finish with
   `cd docs-site && npm run build`. Read `docs-site/DOCUMENTATION.md` before editing a page
   there, and quote only output you produced by running something.
10. **Do not widen the safe list to make a test pass.** `FailClosedCommands.knownSafe` gains an
    entry only where an item says so, with the reason in a comment next to it.
11. **Do not change `verification-core`, `contract`, `ir` or `adapter-testkit` unless the item
    names the file.** Most fixes are deliberately kept inside `spark-adapter` so that the Beam
    and Python adapters and the conformance kit are untouched.
12. **No new violation type.** Every rejection in this plan uses an existing `ViolationType`,
    so `reference/violation-types.md` gains wording, not rows.

### Facts about Spark 3.5.9 this plan relies on

All of these were checked against the running session or with `javap` on Spark's jars. If one
turns out not to hold on the version you build against, stop and report it.

- The check rule sees the **analyzed** plan. Several statements are still catalyst commands at
  that point and become `...execution.command...` classes only later, so a safe list or a
  match keyed on the execution class never fires for them. What the rule actually sees:

  | Statement | Class the rule sees | Fields, in order |
  |---|---|---|
  | `DROP TABLE [IF EXISTS] t` (any table, existing or not) | `catalyst.plans.logical.DropTable` | `child: ResolvedIdentifier(catalog, identifier)`, `ifExists`, `purge` |
  | `DROP VIEW v` (persistent) | `execution.command.DropTableCommand` | `tableName`, `ifExists`, `isView = true`, `purge` |
  | `TRUNCATE TABLE t [PARTITION (...)]` (parquet/csv/hive table) | `execution.command.TruncateTableCommand` | `tableName`, `partitionSpec: Option[Map]` |
  | `TRUNCATE TABLE t` (Delta, Iceberg) | `catalyst.plans.logical.TruncateTable` | `table: ResolvedTable` (Delta then refuses it itself) |
  | `ALTER TABLE t DROP PARTITION (...)` | `execution.command.AlterTableDropPartitionCommand` | `tableName`, `specs`, `ifExists`, `purge`, `retainData` |
  | `ALTER TABLE t ADD PARTITION (...) [LOCATION p]` | `execution.command.AlterTableAddPartitionCommand` | `tableName`, `partitionSpecsAndLocs: Seq[(Map, Option[String])]`, `ifNotExists` |
  | `ALTER TABLE t ADD COLUMNS` (parquet table) | `execution.command.AlterTableAddColumnsCommand` | `table`, `colsToAdd: Seq[StructField]` |
  | `ALTER TABLE t CHANGE COLUMN` (parquet table) | `execution.command.AlterTableChangeColumnCommand` | `tableName`, `columnName`, `newColumn: StructField` |
  | `ALTER TABLE t ADD COLUMNS / RENAME COLUMN / DROP COLUMN / ALTER COLUMN / REPLACE COLUMNS` (Delta, Iceberg) | `catalyst.plans.logical.AddColumns`, `RenameColumn`, `DropColumns`, `AlterColumn`, `ReplaceColumns` | all extend `AlterTableCommand`: `table: ResolvedTable`, `changes: Seq[TableChange]` |
  | `ALTER TABLE t SET LOCATION p` (parquet table) | `execution.command.AlterTableSetLocationCommand` | `tableName`, `partitionSpec`, `location` |
  | `ALTER TABLE t RENAME TO u` (parquet table) | `execution.command.AlterTableRenameCommand` | `oldName`, `newName`, `isView` |
  | `DROP DATABASE d [CASCADE]` | `catalyst.plans.logical.DropNamespace` | `namespace: ResolvedNamespace`, `ifExists`, `cascade` |
  | `CLEAR CACHE` | `execution.command.ClearCacheCommand$` (an object; note the `$`) | — |
  | `INSERT OVERWRITE DIRECTORY p USING fmt SELECT …` | `execution.command.InsertIntoDataSourceDirCommand` (`storage`, `provider`, `query`, `overwrite`), then a nested `InsertIntoHadoopFsRelationCommand` for the same path | |
  | `df.write.jdbc(url, table, props)` | `execution.datasources.SaveIntoDataSourceCommand` | `query`, `dataSource: JdbcRelationProvider`, `options: Map(driver, url, dbtable)`, `mode` |
  | `df.writeStream.foreachBatch(f).start()` | `WriteToStream` whose `sink` class is `ForeachBatchSink`; then, per micro-batch, `WriteToMicroBatchDataSourceV1`, then a bare `LogicalRDD` (the batch DataFrame), then whatever `f` writes | |

- `QueryPlan.subqueries` gives the plans of the subquery expressions in one node;
  `collectWithSubqueries(pf)` is `collect` that also descends into them.
- `CatalogV2Util.applySchemaChanges(schema, changes, tableProvider, statementType)` returns the
  schema an `AlterTableCommand`'s `changes` produce.
- `org.apache.spark.sql.catalyst.optimizer.InlineCTE(alwaysInline = true).apply(plan)` replaces
  every CTE reference in `plan` by the CTE's body.
- Spark only logs a warning when a class in `spark.sql.extensions` cannot be loaded
  (`WARN SparkSession: Cannot use <class> to configure session extensions.`) and carries on.
  It **fails the application** when a class in `spark.plugins` cannot be loaded.
- Settings a job passes to `SparkSession.builder.config(k, v)` replace the same key given by
  `spark-submit --conf`.
- Spark's `META-INF/services` loading of `SparkSessionExtensionsProvider` is **not** used when
  the session is created from PySpark (tried: the provider was on the classpath and never
  called). Do not build on it.
- A `DriverPlugin`'s `init(sc, ctx)` runs before any `SparkSession` exists, `ctx.conf()` is the
  live `SparkConf`, and a change made to `spark.sql.extensions` there is honoured by the
  session created afterwards, from Scala and from PySpark (tried with a prototype plugin).

---

## H-01 `DROP DATABASE … CASCADE` passes

**Severity S1.** Small.

### Observed

Under a contract whose declared output is the table `dbx.gov_t`:

```sql
DROP DATABASE dbx CASCADE   -- passes; dbx.gov_t and its rows are gone
DROP TABLE IF EXISTS dbx.nope  -- rejected, UNVERIFIABLE_WRITE (the table does not even exist)
```

`docs/SPARK_ADAPTER.md` ("Fail-closed on unverifiable writes") says dropping a database is
deliberately left off the safe list because it "can delete a managed table's/database's data".

### Cause

`FailClosedCommands.knownSafe` (`spark-adapter/.../FailClosedCommands.scala`) lists
`org.apache.spark.sql.catalyst.plans.logical.DropNamespace`. That is the class the rule sees
for `DROP DATABASE`, with or without `CASCADE`. The class the document names,
`DropDatabaseCommand`, never reaches the rule.

### Fix

1. In `FailClosedCommands`, remove `DropNamespace` from `knownSafe`.
2. In `isKnownSafe`, before the set lookup, add
   `case d: org.apache.spark.sql.catalyst.plans.logical.DropNamespace => !d.cascade`
   (turn the `if`/`else` into a `match`). Without `CASCADE` Spark itself refuses to drop a
   database that holds a table, so the non-cascade form cannot delete data.
3. Give the rejection its own words. Change `unverifiable(commandClassName: String)` to
   `unverifiable(plan: LogicalPlan)` and return, for a cascading `DropNamespace`:
   - problem: `DROP DATABASE ... CASCADE deletes every table in the database, so Invaract cannot check it against the contract`
   - remediation: `Drop the tables this job is allowed to remove one at a time, then drop the empty database without CASCADE.`

   Every other plan keeps today's text (H-17 rewrites that text). Update the caller in
   `ContractEnforcementRule.verifyOrThrow`.

### Tests

In `FailClosedCommandsSpec`:
- `DROP DATABASE d` on an empty database passes under a contract.
- `DROP DATABASE d CASCADE` on a database holding a table with rows is rejected with
  `UNVERIFIABLE_WRITE`, and the table still has its rows afterwards.
- `DROP DATABASE IF EXISTS no_such_db CASCADE` is rejected too (state this in the test name: the
  statement is judged, not what it would find).

Then add one **statement-level** regression test that runs each of these under a contract and
asserts `UNVERIFIABLE_WRITE` or a write violation, never a pass:
`DROP DATABASE … CASCADE`, `LOAD DATA … INTO TABLE`, `RESTORE TABLE … TO VERSION AS OF 0` (Delta),
`TRUNCATE TABLE` and `DROP TABLE` on a table the contract does not declare. The existing tests
check class names; this one checks SQL text, which is what would have caught this gap. (H-10
later changes what `DROP TABLE` and `TRUNCATE` do for a *declared* output; keep these two on an
undeclared table.)

### Documents

- `docs/SPARK_ADAPTER.md`, "Fail-closed on unverifiable writes": say that the list is keyed by
  the class of the analyzed plan, name `DropNamespace`, and correct the sentence about
  `DropDatabaseCommand`.
- `FailClosedCommands`' own comment block: same correction.

---

## H-02 Reads inside subqueries are invisible

**Severity S1.** Medium. Do after H-14.

### Observed

Contract: one input `orders`, `rejectUndeclaredInputs=true`. `sec` is a table the contract does
not declare.

| Query written to the declared output | Result | Should be |
|---|---|---|
| `SELECT id, (SELECT max(id) FROM sec) AS value FROM orders` | passes | `UNDECLARED_INPUT` |
| `SELECT id, value FROM orders WHERE id IN (SELECT id FROM sec)` | passes | `UNDECLARED_INPUT` |
| `SELECT id, value FROM orders o WHERE EXISTS (SELECT 1 FROM sec WHERE sec.id = o.id)` | passes | `UNDECLARED_INPUT` |
| `SELECT 1 AS id, (SELECT max(value) FROM orders) AS value` (default options) | `MISSING_INPUT` for `orders` | passes |

The translated plan shows the subquery as a function with no arguments:
`Filter(INSUBQUERY(o.id, LISTQUERY()))`, `value = SCALARSUBQUERY()`. So besides the input
checks, the fingerprint does not change when the subquery changes, and no plan-shape rule
(`forbid_cross_join`, …) sees inside it.

### Cause

A subquery's plan hangs off an expression, not off the plan's `children`.
`SparkPlanAdapter.Translator.translateExpr` falls through to the generic
`ir.Function(prettyName, children)` for `ScalarSubquery`, `ListQuery`, `Exists` and
`InSubquery`, and the plan is not among `children`. `ContractEnforcementRule.collectInputSchemas`
and `collectInputFormats` use `plan.collect`, which does not descend into expressions either.

### Fix

Spark's own optimizer turns `IN`/`EXISTS` into a semi join and a scalar subquery into an outer
join. The translation does the same: a subquery becomes the right side of a join placed under
the node that uses it. No IR class changes.

All changes are in `SparkPlanAdapter.scala` and `ContractEnforcementRule.scala`.

1. **Attach subquery plans.** In `Translator`, rename the body of `translateNonWritePlan` to
   `translateNode` and make `translateNonWritePlan` this:

   ```scala
   private def translateNonWritePlan(plan: LogicalPlan): ir.Plan = {
     val node = translateNode(plan)
     val subqueries = plan.subqueries.map(translatePlan).toList
     if (subqueries.isEmpty) node else attachSubqueries(node, subqueries)
   }

   /** Each subquery joins the input of the node that uses it, as Spark's optimizer rewrites it. */
   private def attachSubqueries(node: ir.Plan, subqueries: List[ir.Plan]): ir.Plan = {
     def joined(base: ir.Plan): ir.Plan =
       subqueries.foldLeft(base)((acc, s) => ir.Join(acc, s, ir.JoinType.LeftSemi, Some(ir.Literal(true, "boolean"))))
     node match {
       case p: ir.Project   => p.copy(input = joined(p.input))
       case f: ir.Filter    => f.copy(input = joined(f.input))
       case a: ir.Aggregate => a.copy(input = joined(a.input))
       case w: ir.Window    => w.copy(input = joined(w.input))
       case s: ir.Sort      => s.copy(input = joined(s.input))
       case j: ir.Join      => j.copy(left = joined(j.left))
       case other           => joined(other)
     }
   }
   ```

   The condition is the literal `true` on purpose: `PlanRuleVerifier` treats a join with **no**
   condition as a cross join (`forbid_cross_join`), and this is not one.
   Check the field names of `ir.Project`/`Filter`/`Aggregate`/`Window`/`Sort`/`Join` in
   `ir/src/main/scala/com/invaract/ir/Plan.scala` before writing the `copy` calls.

2. **Give a scalar subquery's value its source.** In `translateExpr`, before the generic
   `case e: Expression` fallback, add

   ```scala
   case s: org.apache.spark.sql.catalyst.expressions.ScalarSubquery =>
     translateExpr(s.plan.output.head)
   ```

   The attribute resolves, through the join added in step 1, to the subquery's own output, so
   lineage follows it. Leave `InSubquery`, `ListQuery` and `Exists` on the generic fallback:
   they filter rows and contribute no value.

3. **Find reads in subqueries everywhere a plan is searched.** Replace `collect` with
   `collectWithSubqueries` at these four places and nowhere else:
   - `ContractEnforcementRule.collectInputSchemas` (both calls),
   - `ContractEnforcementRule.collectInputFormats`,
   - `SparkPlanAdapter.computeAliasDisambiguation`: the `subqueryAliases` collect and the
     `bareLeafOccurrences` collect,
   - `CsvOptionsSupport`'s `plans.flatMap(_.collect { ... })`.

### Tests

New `SubqueryReadsSpec` (extends `ConnectorSpecBase`; register temp views over parquet files):
- the three undeclared-read queries above are rejected with `UNDECLARED_INPUT`, the message
  names the subquery's table, and the output directory is not created;
- each of the three passes when `sec` is declared as an input too;
- the fourth query (declared input read only in a subquery) passes with default options;
- a subquery nested in a subquery (`IN (SELECT id FROM a WHERE id IN (SELECT id FROM sec))`) is
  rejected for `sec`;
- `forbid_cross_join`: a query with an `IN` subquery and no cross join passes; the same query
  whose subquery contains `FROM a CROSS JOIN b` is rejected with `RULE_CROSS_JOIN_VIOLATION`;
- lineage: for `SELECT id, (SELECT max(amount) FROM sec) AS m FROM orders`, `Lineage.trace`
  gives `m` the source `sec.amount`, and `id` only `orders.id`;
- with `computeFingerprint`, two jobs that differ only in the subquery's table get different
  fingerprints (capture both through a `TestNotificationSink`).

In `ExpressionTranslationSpec`: the rendered plan of an `IN` query contains a `Join` whose
right side is a `Read` of the subquery's table.

Boundaries to cover for mutation testing: `subqueries.isEmpty` (a plan with none must come out
exactly as before: assert equality with the pre-change rendering for one existing fixture),
and each arm of `attachSubqueries` (a subquery in a `SELECT` list, a `WHERE`, a `HAVING`, an
`ORDER BY`, a join's `ON`).

### Documents

- `docs/SPARK_ADAPTER.md`, "Translation coverage": add rows for subquery expressions.
- `docs-site/.../guides/enforcing-transformation-rules.mdx`: one sentence that rules also see
  joins, filters and aggregations inside subqueries.
- Fingerprint list (rule 9): "a query with a subquery".

---

## H-03 A read of several paths shows only the first, and a read of part of an input does not match it

**Severity S1** (first half) and **S2** (second half). Large. Do after H-02.

### Observed

Contract: input `orders` at `…/in/orders`. `orders2` is another dataset with the same columns.

| Read | Options | Result | Should be |
|---|---|---|---|
| `spark.read.parquet(orders, orders2)` | strict | passes | `UNDECLARED_INPUT` for `orders2` |
| `spark.read.parquet("…/in/orders*")` (matches both) | strict | passes | `UNDECLARED_INPUT` for `orders2` |
| `spark.read.parquet(orders2, orders)` | strict | `MISSING_INPUT` + `UNDECLARED_INPUT` | `UNDECLARED_INPUT` only |
| `spark.read.parquet("…/in/part/dt=2024-01-01")`, contract declares `…/in/part` | default | `MISSING_INPUT` | passes |
| the same with `.option("basePath", "…/in/part")` | default | `MISSING_INPUT` | passes |
| one file inside the declared folder | default | `MISSING_INPUT` | passes |
| `spark.read.parquet("…/in/orders/*.parquet")` | default | `MISSING_INPUT` | passes |

The first two are silent passes. The last four make the commonest way to read a partitioned
dataset (`f"{base}/dt={ds}"`) impossible to put under a contract; the troubleshooting page says
so ("A partition's folder") and offers no way round it.

### Cause

`SparkPlanAdapter.locationOf` takes `h.location.rootPaths.headOption`. A read of several paths,
and a glob (which Spark expands into one root path per match), keeps only the first. And a
location is matched by equality or suffix, so a folder or file *inside* a declared input is a
different place.

### Fix

Two rules, both applied where the adapter names a file read:

- **Every root path is a read.** A relation with several root paths yields one `ir.Read` per
  distinct location.
- **A read inside a declared input is a read of that input.** When a root path is not itself a
  declared input but lies under one, it is reported at the declared input's location.

The second rule needs the contract's input locations, so the translator is told them.

1. **New file `ReadLocations.scala`** (`private[sparkadapter] object ReadLocations`), pure
   functions, no Spark session:

   ```scala
   /** `actual` if it is a declared location or lies under none; otherwise the part of it that is the
     * longest declared location it lies under. */
   def attribute(actual: String, declared: Seq[String]): String

   /** Every distinct location a file relation reads, attributed. */
   def of(roots: Seq[String], declared: Seq[String]): List[String]
   ```

   `attribute` works on `Locations.canonical` forms (`com.invaract.contract.Locations`):
   - If some `d` in `declared` satisfies `LocationMatching.matches(d, actual)`, return `actual`
     unchanged. (An input declared as a partition folder keeps working exactly as today.)
   - Otherwise split `canonical(actual)` into segments. For every proper prefix made of whole
     segments (so `/a/orders2` is never a prefix of anything under `/a/orders`), keep those that
     some declared location matches by `LocationMatching.matches`. If the segments *after* the
     prefix contain `..`, treat the prefix as not matching. Return the **longest** kept prefix;
     if none, return `actual` unchanged.
   - A prefix keeps its scheme and authority: for `s3://bucket/a/b/c` the prefixes are
     `s3://bucket/a/b`, `s3://bucket/a`, `s3://bucket`.

   `LocationMatching` is `private[invaract]`, which `com.invaract.sparkadapter` can use.

2. **`SparkPlanAdapter`.**
   - Add `def translate(plan: LogicalPlan, declaredInputs: Seq[String]): TranslationResult` and
     make the existing `translate(plan)` call it with `Nil`. `Translator` takes
     `declaredInputs` as a second constructor argument.
   - Add `def locationsOf(lr: LogicalRelation, declaredInputs: Seq[String]): List[String]`:
     for a `HadoopFsRelation` with at least one root path,
     `ReadLocations.of(h.location.rootPaths.map(_.toString), declaredInputs)`; for anything
     else `List(locationOf(lr))`. Keep `locationOf` as it is (other code uses it).
   - In `translateNode`'s `case lr: LogicalRelation`: compute `locationsOf`. With one location,
     build the `ir.Read` exactly as today from that location. With several, build
     `ir.Union(primary :: extras)` where `primary` is today's `ir.Read` for the first location
     (same alias, same catalog identity) and each extra is
     `ir.Read(ir.DatasetRef(location))` with no alias. Lineage keeps attributing columns to the
     first location, as it does today; the extras exist so the input checks see them.
   - In the `SubqueryAlias` case, add an arm before `case other`: when `sa.child` is a
     `LogicalRelation` and the translation is an `ir.Union` whose first input is an `ir.Read`,
     put the alias on that first `ir.Read` only and do not report the "dropped alias"
     diagnostic.

3. **`ContractEnforcementRule`.**
   - `verifyOrThrow` and `inferOutcome` call `SparkPlanAdapter.translate(plan, declaredInputs)`
     with `contract.inputs.map(_.location)` (`Nil` in dry-run, which has no contract).
   - Turn `recognizedRead` into a function `LogicalPlan => List[(String, StructType)]` taking
     `declaredInputs`, returning one pair per location from `locationsOf` for a
     `LogicalRelation` and one pair as today for the other four cases. `collectInputSchemas`,
     `collectInputFormats` and `CsvOptionsSupport` use it, so a schema, a format and CSV
     options are keyed by the same location string the `ir.Read` carries. If they disagree, an
     input's schema silently stops being checked: add the assertion in the tests below.
   - `CheckpointRegistry.record` is unaffected.

4. **Dry-run drafts.** A draft must name the dataset, not today's partition folder, or
   tomorrow's run fails. In `inferOutcome` only, post-process each read location with
   `ReadLocations.datasetRoot(root, isFile)`: drop the last segment when the root path is a
   file (`h.location.inputFiles` contains it; compare as `new Path(x).toUri.toString`), then
   drop trailing segments of the form `name=value`. Several root paths that reduce to the same
   location collapse to one input. Implement `datasetRoot` in `ReadLocations` as a pure
   function over strings and a boolean.

### Tests

`ReadLocationsSpec` (plain unit test, no Spark):
- exact match returns the input unchanged, including when a longer declared location also
  matches a prefix;
- `/data/in/orders/dt=1/p.parquet` with declared `in/orders` gives `/data/in/orders`; with
  declared `/data/in/orders` the same; with both `orders` and `in/orders/dt=1` declared gives
  `/data/in/orders/dt=1` (longest wins);
- `/data/in/orders2/x` with declared `in/orders` is unchanged (segment boundary);
- `/data/in/orders` with declared `in/orders/dt=1` is unchanged (a parent is not inside a child);
- `s3a://b/in/orders/x` with declared `s3://b/in/orders` gives `s3://b/in/orders`;
- a remainder containing `..` is unchanged;
- no declared locations: unchanged;
- `of` removes duplicates and keeps first-seen order;
- `datasetRoot`: file under partition folders, partition folders only, neither, a `name=value`
  that is the only segment.

`ReadLocationsEnforcementSpec` (real session): each row of the table above, plus
- both datasets declared: the two-path read passes, strict;
- the schema of the second path is checked: declare `orders2` with a field it does not have and
  expect `MISSING_INPUT_FIELD` naming `orders2`'s location;
- a read of `…/in/orders2` is still `UNDECLARED_INPUT` when only `orders` is declared;
- an aliased two-path read through a temp view still has lineage to the first location;
- dry-run on a job reading `…/in/part/dt=2024-01-01` drafts the input at `…/in/part`, and that
  draft passes when the job is rerun reading `dt=2024-01-02`.

### Documents

- `docs-site/.../reference/contract-format.mdx`, "Location matching": add the inside-an-input
  rule for inputs, and that outputs are unchanged (a write into a sub-folder of a declared
  output is still a mismatch; a test above the fix must show this stays true).
- `docs-site/.../troubleshooting/common-problems.mdx`: rewrite the "A partition's folder" bullet.
- `docs-site/.../guides/dry-run-mode.mdx`: a draft names the dataset folder.
- `docs/SPARK_ADAPTER.md`: "Structural verification", location matching paragraph.
- Fingerprint list: "a read of several paths".

---

## H-04 An RDD-backed DataFrame passes `rejectUndeclaredInputs`

**Severity S1.** Small. Do after H-03.

### Observed

```scala
val sec = spark.read.parquet(secret)                       // not declared
orders.join(spark.createDataFrame(sec.rdd, sec.schema), "id")
  .select("id", "value").write.parquet(out)                // passes with rejectUndeclaredInputs=true
```

The translated plan holds `UnknownPlan(RDDRelation(RDD-backed relation, 2 column(s)))` and a
diagnostic, and nothing acts on it.

### Cause

`InputChecker` judges `ir.Read` nodes only. An RDD-backed relation is deliberately not a
lineage boundary (a `foreachBatch` micro-batch is one too), so it is neither a read nor a
reason to doubt.

### Fix

With `rejectUndeclaredInputs` on, the platform has asked that every input be visible. A source
Invaract cannot see into then makes the write unverifiable.

In `ContractEnforcementRule.verifyOrThrow`, in the `case w: ir.Write` branch, next to the
existing `PartitioningSupport.refusal` block and before `VerificationPipeline.verifyWrite`:

```scala
if (options.rejectUndeclaredInputs && StructuralVerifier.collectUnknownPlans(translated.plan).exists(_.sourceType == "RDDRelation")) {
  VerificationPipeline.requireValidContract(contract, sink, applicationId, job)
  VerificationPipeline.rejectUnverifiableWrite(contract, OpaqueSources.rddBacked, translated.plan, sink, applicationId, job)
}
```

`StructuralVerifier.collectUnknownPlans` exists and is `private[invaract]`. Put the
constant `"RDDRelation"` in one place: `SparkPlanAdapter` already writes it when it builds the
node, so add `val RddRelationSourceType = "RDDRelation"` there and use it in both.
`OpaqueSources.rddBacked` is an `UnverifiableOperation`:

- name `RDDRelation`
- problem `this write reads a DataFrame built from an RDD (createDataFrame(rdd), rdd.toDF), and Invaract cannot see what the RDD was computed from`
- remediation `Read the data with spark.read or spark.table so its source is visible, or run without rejectUndeclaredInputs if inputs outside the contract are acceptable for this job.`

Do not include the checkpoint boundary types here: `CheckpointVerificationAuditSpec` already
fixes how an unresolved checkpoint behaves under this option.

### Tests

In `ContractEnforcementRuleSpec`:
- the job above is rejected with `UNVERIFIABLE_WRITE` when `rejectUndeclaredInputs = true`, and
  the output is not written;
- the same job passes with default options (unchanged behavior);
- a strict job with no RDD-backed relation passes (the condition's other half);
- a strict job whose RDD-backed relation came through `.checkpoint()` and was resolved by the
  registry passes (it is no longer an `RDDRelation` once resolved).

### Documents

- `docs-site/.../reference/configuration.md`: in the "Reject undeclared inputs" row, add that
  an input Invaract cannot see into (a DataFrame built from an RDD) blocks the write.
- `docs-site/.../reference/violation-types.md`: add this cause under `UNVERIFIABLE_WRITE`.

---

## H-05 `ALTER TABLE` on a declared output is never checked

**Severity S1.** Large. Do after H-10.

### Observed

Contract: output table `d2_d` (Delta) or `d2_p` (parquet), fields `id`, `value`,
`rejectUndeclaredFields=true`. Each statement passes, with no validation event:

```sql
ALTER TABLE d2_d ADD COLUMNS (email string)      -- an undeclared column appears
ALTER TABLE d2_d RENAME COLUMN value TO amount   -- a required field disappears
ALTER TABLE d2_d DROP COLUMN id                  -- a required field disappears
ALTER TABLE d2_p SET LOCATION '/…/in/secret'     -- the output now serves other data
ALTER TABLE d2_p RENAME TO d2_renamed            -- the output no longer exists
ALTER TABLE t ADD PARTITION (dt='z') LOCATION '/…/elsewhere'   -- rows appear with no write
```

Afterwards `d2_d` had the schema `amount:int, email:string` against a contract that requires
`id:int, value:int`.

### Cause

These classes are on `FailClosedCommands.knownSafe` because they do not change rows. They do
change what the contract describes: the output's columns and where it lives.

### Fix

Treat them as what `StateChangingCallSupport` already is for Iceberg procedures: an operation
with no query that leaves a table in a new state, checked with
`VerificationPipeline.verifyStateChange`. That method already does the right scoping: a table
no declared output matches is "not this contract's concern" and passes; a table that is a
declared output has the resulting schema checked with the same rules and messages as a write.

1. **New file `SchemaDdlSupport.scala`**, `private[sparkadapter] object SchemaDdlSupport`:

   ```scala
   sealed trait SchemaDdl { def statement: String; def location: String; def tableNames: List[String] }
   /** The table's columns after the statement. */
   final case class NewSchema(statement: String, location: String, tableNames: List[String], schema: StructType) extends SchemaDdl
   /** The table stops being where, or what, the contract says: SET LOCATION, RENAME away, ADD PARTITION ... LOCATION. */
   final case class Displaced(statement: String, location: String, tableNames: List[String], problem: String) extends SchemaDdl
   /** RENAME TO: `target*` is the new name, `schema` the renamed table's columns. */
   final case class RenamedOnto(statement: String, sourceLocation: String, sourceNames: List[String],
                                targetLocation: String, targetNames: List[String], schema: StructType) extends SchemaDdl

   def extract(plan: LogicalPlan): Option[SchemaDdl]
   ```

   `extract` matches (see the table under "Facts about Spark 3.5.9"):
   - `cmd: AlterTableCommand` with `cmd.table` a `ResolvedTable rt` →
     `NewSchema(…, CatalogV2Util.applySchemaChanges(rt.table.schema, cmd.changes, Option(rt.table.properties.get("provider")), "ALTER TABLE"))`.
     Location: `SparkPlanAdapter.tableLocationAndFormat(rt.table)._1`, else the table name
     below. Table name:
     `LocationMatching.fromParts(rt.catalog.name() +: rt.identifier.namespace() :+ rt.identifier.name())`,
     which is how `StateChangingCallSupport` already names an Iceberg table (see its
     `StateChangeInfo.tableName`). H-10 puts both in a helper (`TableTargets`); use it.
   - `AlterTableAddColumnsCommand(table, colsToAdd)` → `NewSchema` with the catalog table's
     schema plus `colsToAdd`.
   - `AlterTableChangeColumnCommand(table, columnName, newColumn)` → `NewSchema` with that one
     field replaced.
   - `AlterTableSetLocationCommand(table, None, _)` → `Displaced`, problem
     `repoints the table at a different location`. With a partition spec (`Some`) → `Displaced`,
     problem `repoints a partition at data no write produced`.
   - `AlterTableAddPartitionCommand` → `Displaced` only if some entry of `partitionSpecsAndLocs`
     has a location (`_._2.isDefined`), problem `attaches data no write produced`; otherwise
     `None`.
   - `AlterTableRenameCommand(old, new, isView = false)` → `RenamedOnto`. `isView = true` → `None`.
   - catalyst `SetTableLocation` and `RenameTable` (the Delta/Iceberg forms; their first field
     is a `ResolvedTable`) → the same two results.
   - anything else → `None`.

   For the V1 commands, read the table with
   `SparkSession.active.sessionState.catalog.getTableMetadata(tableName)` inside a `Try`. Its
   location is `storage.locationUri`, its names come from
   `CatalogIdentitySupport.fromCatalogTable`. For a rename target that does not exist yet, the
   location is `SparkSession.active.sessionState.catalog.defaultTablePath(newName)` (the way
   `WriteCommandSupport.createDataSourceTableAsSelect` already finds a new table's location).
   If the `Try` fails, return `None`: the statement then takes today's path.

2. **`ContractEnforcementRule.verifyOrThrow`**, in the non-write branch, after
   `StateChangingCallSupport.extract` finds nothing and before the fail-closed test:

   - `NewSchema` → `VerificationPipeline.verifyStateChange(contract, statement, location, tableNames, SparkSchemas.toLogicalSchema(schema), SQLConf.get.caseSensitiveAnalysis, options, sink, applicationId, job)`.
   - `Displaced` → if `OutputChecker.matchOutput(contract.outputs, location :: tableNames)` is
     defined, `VerificationPipeline.rejectUnverifiableWrite` with name = the statement, problem
     = `<statement> <problem>, and '<output name>' is an output this contract declares`,
     remediation = `Write the output with a query Invaract can check, or change the contract's declared location in the same change.`
     Otherwise do nothing (pass).
   - `RenamedOnto` → first the source: if it matches a declared output, reject as `Displaced`
     with problem `renames the table away`. Then the target: `verifyStateChange` with the
     target's location and names and the schema.

   The safe-list entries stay: a statement on a table the contract does not declare still
   passes, through `verifyStateChange`'s own scoping or the `None` case.

3. **Dry-run** (`inferOutcome`) is unchanged: these statements produce no draft.

### Tests

New `SchemaDdlSupportSpec` on a Delta-enabled session (copy the session set-up from
`ContractEnforcementRuleSpec`; Delta column rename and drop need the table properties
`delta.columnMapping.mode=name`, `delta.minReaderVersion=2`, `delta.minWriterVersion=5`):

For a declared output, with `rejectUndeclaredFields = true`:
- `ADD COLUMNS (email string)` → `UNDECLARED_OUTPUT_COLUMN`, and the table's schema is unchanged
  afterwards; on a parquet table and on a Delta table;
- the same statement passes with default options, and when the contract declares `email`;
- `RENAME COLUMN value TO amount` → `MISSING_OUTPUT_FIELD` for `value`;
- `DROP COLUMN id` → `MISSING_OUTPUT_FIELD` for `id`;
- `DROP COLUMN` of a column the contract marks `required: false` passes;
- `CHANGE COLUMN id id int COMMENT 'x'` passes (the schema is the same);
- `SET LOCATION` → `UNVERIFIABLE_WRITE`, and `DESCRIBE` shows the old location;
- `RENAME TO other` → `UNVERIFIABLE_WRITE`, and the table still has its name;
- `ADD PARTITION … LOCATION` → `UNVERIFIABLE_WRITE`; `ADD PARTITION` without a location passes;
- `ALTER TABLE staging RENAME TO <declared output name>` where `staging` has an extra column →
  `UNDECLARED_OUTPUT_COLUMN`; where `staging` has exactly the declared columns → passes.

For a table the contract does not declare: every statement above passes and takes effect.

With a `TestNotificationSink`: a checked statement on a declared output publishes exactly one
`ContractValidationEvent` (`PASSED` or `FAILED`); one on an undeclared table publishes one
`PASSED` event with no violations (this is what `verifyStateChange` does today for an Iceberg
procedure on an undeclared table; assert it so the behavior is deliberate).

### Documents

- `docs-site/.../concepts/fail-closed.mdx`: a section "Statements that change a table without
  writing to it", with the table of what is checked and what is refused.
- `docs-site/.../reference/connector-support.mdx`: add a row group "Table changes".
- `docs/SPARK_ADAPTER.md`: a section next to "Fail-closed on unverifiable writes".
- `invaract-capabilities-spark.yaml`: extend the note of `write.stateChange` ("Nine Iceberg
  CALL procedures …") to include these statements, then `./dev/capabilities`.

---

## H-06 A rejected write counts as the output having been written

**Severity S1.** Small.

### Observed

Contract with outputs `result` and `audit`. The job writes `result` correctly, and writes
`audit` without a required column, catching the exception (as the install guide's own
"Handling a rejection" example does). `audit` is never created. At the end of the job
`DeclaredOutputs.finishAll` reports nothing: no `MISSING_OUTPUT` for `audit`, and with
`spark.invaract.failOnMissingOutput=true` the job would exit 0.

### Cause

In `ContractEnforcementRule.verifyOrThrow`, `written(w)` is the first statement of the
`case w: ir.Write` branch, before `VerificationPipeline.verifyWrite`. A write is recorded
whether or not it passes.

### Fix

Move `written(w)` to after `VerificationPipeline.verifyWrite(...)` returns, immediately before
the `plan match { case _: CreateDataSourceTableAsSelectCommand => … }` block. Nothing else
changes. A write that passes the check and then fails while Spark runs it is still recorded;
that failure reaches the job as an exception of its own, so say so in the method's comment
rather than adding machinery for it.

### Tests

In `DeclaredOutputsSpec` (which already drives `forContract` and `finishAll`):
- a job whose only attempt at `audit` is rejected and swallowed: `finishAll` returns one
  `MISSING_OUTPUT` naming `audit`;
- the same job followed by a correct write of `audit`: `finishAll` returns nothing;
- a job whose writes all pass: nothing (existing test; keep it green).

The first test must fail before the fix. It is also the test that kills the mutant "record
before verify".

### Documents

`docs-site/.../guides/installing-the-enforcement-rule.mdx`, "Every declared output must be
written": one sentence, "A write that was blocked does not count."

---

## H-07 The job can detach Invaract, or loosen its settings

**Severity S1.** Large. This is the gap the platform admin cares about most.

### Observed

A platform attaches Invaract with `spark-submit --conf` to a PySpark job it does not own:

| What happened | Result |
|---|---|
| the class name in `spark.sql.extensions` was misspelt, or the jar was missing | one `WARN` from Spark; every write passed |
| the job's code has `.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")`, as the Delta and Iceberg quick-starts tell people to write | Invaract was dropped, silently; every write passed |
| the job's code has `.config("spark.invaract.dryRun", "true")` | nothing was enforced |
| the job's code has `.config("spark.invaract.contract", "<its own file>")` | the job's own contract was enforced |
| the job's code has `.config("spark.invaract.orgPolicy", "<an empty policy>")` | the organizational policy was gone |
| the job calls `spark.conf.set("spark.invaract.dryRun", "true")` before its first query | nothing was enforced |

`invaract-capabilities-spark.yaml` declares `config.zeroCodeInstall: supported`; Beam declares
the equivalent `partial` for this very reason.

### What can and cannot be promised

A job's code runs inside the driver with every permission the driver has, so no in-process
check can stop an author who sets out to defeat it. What can be promised, and what this item
delivers:

1. Invaract is never missing **by accident**: a misspelt name or missing jar stops the job, and
   a job that sets its own extension list keeps Invaract.
2. A setting the platform **pins** cannot be changed through any Spark configuration call.
3. The documents say exactly this.

### Fix

**Part A: a driver plugin that keeps the extension installed.**

New file `InvaractSparkPlugin.scala`, public class
`com.invaract.sparkadapter.InvaractSparkPlugin extends org.apache.spark.api.plugin.SparkPlugin`:

- `executorPlugin()` returns `null`.
- `driverPlugin()` returns a `DriverPlugin` whose `init(sc, ctx)`:
  1. reads `ctx.conf().get("spark.sql.extensions", "")`, splits on commas, trims;
  2. if `com.invaract.sparkadapter.InvaractSparkSessionExtension` is not in the list, appends
     it, writes the list back with `ctx.conf().set("spark.sql.extensions", …)`, and logs at
     `WARN`: `Invaract: spark.sql.extensions did not name Invaract (the job's own configuration may have replaced it); added it.`
  3. applies the pinned settings of Part B: for each pinned key, if `ctx.conf()` holds a
     different value, logs at `WARN` naming the key (not the values) and sets the pinned value;
  4. returns an empty map.

Put steps 1–2 in a pure function `withInvaract(current: String): String` so it can be unit
tested: empty string, a list without Invaract, a list with it (unchanged, no duplicate), a
list with spaces around the commas.

A platform then attaches with **one** setting that fails closed:

```bash
spark-submit --jars invaract-spark-adapter-<version>.jar \
  --conf spark.plugins=com.invaract.sparkadapter.InvaractSparkPlugin \
  --conf spark.invaract.contract=/path/to/contract.yaml  my-job.py
```

If the jar is missing or the name is misspelt, Spark refuses to start the application.
`spark.sql.extensions=…InvaractSparkSessionExtension` keeps working exactly as before.

`InvaractSparkSessionExtension` must tolerate being listed twice (the plugin adds it only when
absent, but a user may list it and a platform may too). Today a double listing runs every
check twice. In `InvaractSparkSessionExtension.apply`, keep a
`java.util.WeakHashMap[SparkSessionExtensions, java.lang.Boolean]` (synchronized) and inject
the check rule only the first time a given `SparkSessionExtensions` object is seen.

**Part B: pinned settings.**

New file `PinnedSettings.scala`, `private[sparkadapter] object PinnedSettings`:

- The environment variable `INVARACT_PINNED_CONF`, when set, names a `.properties` file whose
  keys are spelt as on Spark (`spark.invaract.contract=…`, `spark.invaract.dryRun=false`).
- `def load(env: Map[String, String]): Map[String, String]`: empty when the variable is unset;
  the file's entries when set; an `IllegalStateException` naming the variable and the path when
  the file cannot be read; an `IllegalArgumentException` when a key does not start with
  `spark.invaract.`. Pure apart from reading the file.
- `lazy val current: Map[String, String] = load(sys.env)`, plus
  `private[sparkadapter] def withPinsForTest[T](pins: Map[String, String])(body: => T): T` that
  swaps a `@volatile` override for the duration of `body`. Do not read a JVM system property:
  job code can set one.

In `SparkConfigSource.apply`, look a key up in the pins first and in `session.conf` second:

```scala
ConfigSource.prefixed(Prefix)(key => PinnedSettings.get(key).orElse(session.conf.getOption(key)))
```

Every Invaract setting is read through `SparkConfigSource` *(by reading the code:
`AttachSetup`, `VerificationSetup`, `DeclaredOutputs`)*, except the two prefix families in
`DryRunReporter` (`spark.invaract.job.metadata.*`, `spark.invaract.contract.metadata.*`), which
call `session.conf.getAll`; leave those unpinned and say so in the reference page.

Why an environment variable: it is the one input a job's Spark calls cannot overwrite, and
every scheduler can set it for the driver (`spark.yarn.appMasterEnv.INVARACT_PINNED_CONF`,
`spark.kubernetes.driverEnv.INVARACT_PINNED_CONF`, `spark-env.sh`, a Databricks cluster's
environment variables).

**Part C: say what is promised.** In `invaract-capabilities-spark.yaml`, keep
`config.zeroCodeInstall: supported` and replace its note with: attached by
`spark.plugins=com.invaract.sparkadapter.InvaractSparkPlugin` (fails the job if Invaract cannot
be loaded, and survives a job that sets its own `spark.sql.extensions`) or by
`spark.sql.extensions`; settings in the file named by `INVARACT_PINNED_CONF` cannot be changed
by the job's Spark configuration. Run `./dev/capabilities`.

### Tests

- `InvaractSparkPluginSpec` (unit): `withInvaract` cases above.
- `PinnedSettingsSpec` (unit): unset; a readable file; a missing file; a key without the
  prefix; `get` on a pinned and an unpinned key.
- `SparkConfigSourceSpec`: with `withPinsForTest`, a pinned `dryRun=false` wins over a session
  conf `dryRun=true`; an unpinned key still comes from the session; a pinned key absent from
  the session is found.
- A job in its own JVM, following `DeclaredOutputsSpec`/`MissingOutputJob`: start a
  `SparkSession` with `spark.plugins` set and `.config("spark.sql.extensions", "some.Other")`
  in the builder, with `INVARACT_PINNED_CONF` in the child process's environment pinning the
  contract and `dryRun=false`, while the builder sets `spark.invaract.dryRun=true`. Assert that
  a write to an undeclared location throws `ContractViolationException`.
- The same job with `spark.plugins` naming a class that does not exist: the process exits
  non-zero before any write.
- `InvaractSparkSessionExtensionSpec`: with the extension listed twice, one write publishes the
  same number of validation events as with it listed once.

The PySpark half was proven by hand with a prototype plugin (a job that overrode
`spark.sql.extensions` was still enforced). Add it to `./dev/regression` only if that script
already runs a PySpark job; otherwise leave it as the manual check below.

Manual check, to run once and paste into the pull request: a PySpark job whose builder sets
`spark.sql.extensions` to another class, submitted with `--conf spark.plugins=…`, must reject a
write to an undeclared path.

### Documents

- `docs-site/.../guides/installing-the-enforcement-rule.mdx`: lead with the `spark.plugins`
  form; keep `spark.sql.extensions` as the alternative; add a caution box listing the three
  accidents the plugin prevents.
- New guide `docs-site/.../guides/locking-settings-for-a-platform.mdx`: pinned settings, how to
  set the variable on YARN, Kubernetes and standalone, and the plain statement of what is and
  is not promised (the three points above).
- `docs-site/.../reference/configuration.md`: a section "Pinned settings".
- `docs-site/.../troubleshooting/common-problems.mdx`: "The job runs but nothing is checked".
- `docs/SPARK_ADAPTER.md`: "Fully code-free installation"; `ARCHITECTURE.md` ADR-008: an
  amendment recording the plugin and why the service-loader route was rejected (PySpark does
  not use it).
- `CLAUDE.md`, "External Attachability Requirement": mention the plugin and pinned settings
  where it describes the baseline installation.

---

## H-08 A misspelt `spark.invaract.*` setting is ignored

**Severity S1.** Small.

### Observed

`--conf spark.invaract.rejectUndeclaredInput=true` (missing `s`),
`--conf spark.invaract.dryrun=true`, `--conf spark.invaract.orgpolicy=/p.yaml`: no error, no
warning, and none of the three took effect. An admin who mistypes the strict-inputs flag
believes it is on. Beam already refuses a misspelt option; a wrong *value* for a Spark flag is
already refused.

### Fix

New `private[sparkadapter] object SparkSettingNames` (put it in `SparkConfigSource.scala`):

```scala
/** The spark.invaract.* keys in `conf` that Invaract does not read, each with the nearest key it does read. */
def unknown(conf: Map[String, String]): List[(String, Option[String])]
```

Known keys: `SparkConfigSource.Prefix + name` for every `name` in `InvaractConf.All`, plus any
key starting with `DryRunReporter.JobMetadataConfPrefix` or
`DryRunReporter.ContractMetadataConfPrefix`. The nearest key uses
`com.invaract.contract.Suggestions.distance`, compared without regard to case, and is offered
when the distance is 3 or less.

Call it once per session, at the top of `InvaractSparkSessionExtension.checkRuleFor` and of
both `ContractEnforcementRule.forContract` overloads and `dryRun`, with `session.conf.getAll`:

- enforcement: throw `IllegalArgumentException`:
  `Invaract does not read 'spark.invaract.rejectUndeclaredInput'. Did you mean 'spark.invaract.rejectUndeclaredInputs'? (Settings: <docs URL of reference/configuration>)`,
  one line per unknown key;
- dry-run: log the same text at `WARN` and carry on (dry-run never stops a job).

`InvaractConf.TrustedSteps` is in `All` and is not used on Spark; it stays "known" (setting it
is harmless and the name is real).

### Tests

`SparkConfigSourceSpec`: no Invaract keys; only known keys; each prefix family; one unknown key
with a near neighbour (the suggestion is that neighbour); one unknown key with none (no
suggestion); a key differing only in case; distance exactly 3 (suggested) and 4 (not).
`InvaractSparkSessionExtensionSpec`: a session with `spark.invaract.rejectUndeclaredInput=true`
fails its first query with a message containing both spellings; the same key under
`spark.invaract.dryRun=true` does not fail.

### Documents

`docs-site/.../reference/configuration.md` (intro: a setting Invaract does not know is refused
on Spark too) and a troubleshooting entry.

---

## H-09 A deep plan crashes the job with `StackOverflowError`

**Severity S2.** Medium.

### Observed

```scala
val df = (1 to 1500).foldLeft(orders)((d, i) => d.withColumn("value", $"value" + i))
df.count()
```

Without a contract: works. With any contract active: `java.lang.StackOverflowError`, from
`SparkPlanAdapter.Translator.translatePlan ← SparkPlanAdapter.translate ←
ContractEnforcementRule.verifyOrThrow`. The same at 1,000 columns. The job is not writing
anything; a loop of `withColumn` is ordinary in feature pipelines.

### Cause

`verifyOrThrow` translates **every** analyzed plan (it has to, to remember what a later
`.checkpoint()` was made from). `translatePlan` recurses once per plan node with larger frames
than Spark's own analyzer, so it runs out of stack on plans Spark handles. Nothing catches the
error. Log-only dry-run (`ContractEnforcementRule.dryRun`) has the same exposure;
`DryRunReporter` already guards against it.

### Fix

New file `DeepPlans.scala`, `private[sparkadapter] object DeepPlans`:

```scala
val LargeStackBytes: Long = 512L * 1024 * 1024

/** `body` on the calling thread; `Left` if it ran out of stack. */
def attempt[A](body: => A): Either[StackOverflowError, A]

/** `body` on a new thread with a `LargeStackBytes` stack; `Left` if even that ran out. Any other
  * throwable `body` throws is rethrown on the calling thread, unwrapped. */
def onLargeStack[A](body: => A): Either[StackOverflowError, A]
```

`onLargeStack` creates `new Thread(null, runnable, "invaract-deep-plan", LargeStackBytes)`,
starts it, joins it, and rethrows what it caught. Spark keeps the active session in an
inheritable thread-local, so `SparkSession.active` and `SQLConf.get` work on the new thread;
the test below proves it.

In `ContractEnforcementRule`, split `verifyOrThrow`'s body into two private methods:
`translateStage(...)` (everything up to and including `recordCheckpointOrigin`; no events, no
exceptions other than programming errors) returning the translation, and
`checkStage(translation, ...)` (the `translated.plan match { … }`). Then:

```scala
DeepPlans.attempt(translateStage(...)) match {
  case Right(t) => DeepPlans.attempt(checkStage(t, ...)).left.foreach(_ => tooDeep(...))
  case Left(_)  => DeepPlans.onLargeStack { checkStage(translateStage(...), ...) }.left.foreach(_ => tooDeep(...))
}
```

`tooDeep`: if `analyzedPlan.isInstanceOf[Command]` or
`WriteCommandSupport.combined.isDefinedAt(analyzedPlan)` (both look only at the root node),
call `VerificationPipeline.rejectUnverifiableWrite` with name `DeeplyNestedPlan`, problem
`this write's plan is too deeply nested for Invaract to verify`, remediation
`Shorten the plan before writing: call .checkpoint() or .localCheckpoint() part-way through, or write an intermediate table and read it back.`,
and `ir.UnknownPlan("a plan too deeply nested to render")` as the plan. Otherwise log one
`WARN` per session (guard with an `AtomicBoolean`) and return: a plan that is not a write is
never blocked.

`ContractViolationException` thrown inside `onLargeStack` must reach the caller as itself, not
wrapped; the test asserts it.

Wrap `inferOrIgnore`'s body in `DeepPlans.attempt` and log a `WARN` on `Left`, so log-only
dry-run can never throw.

### Tests

`DeepPlansSpec` (unit):
- a recursive function 200 deep returns `Right` from `attempt`;
- one 10,000,000 deep returns `Left` from `attempt`, and (if it fits in 512 MB; choose the depth
  by trying) `Right` from `onLargeStack`;
- one with no base case returns `Left` from `onLargeStack`;
- an `IllegalStateException` thrown inside `onLargeStack` arrives as that exception;
- inside `onLargeStack`, `SparkSession.active` is the session active on the calling thread.

In `ContractEnforcementRuleSpec`:
- the 1,500-`withColumn` `count()` passes under a contract. Start the test with the same count
  and **no** contract inside `assume(Try(...).isSuccess)`, so it is skipped rather than failed
  on a JVM where Spark itself cannot analyze the plan;
- a write of the 1,000-`withColumn` DataFrame to the declared output passes, and one to an
  undeclared path is rejected with `OUTPUT_LOCATION_MISMATCH` (a deep plan is still checked);
- `ContractEnforcementRule.inferOrIgnore` on a 3,000-deep expression plan does not throw.

### Documents

Troubleshooting entry "A write is rejected as too deeply nested"; one sentence in
`docs/SPARK_ADAPTER.md`, "Never throws".

---

## H-10 `DROP TABLE`, `TRUNCATE` and `DROP PARTITION` cannot be used at all

**Severity S2.** Large.

### Observed

The newcomer's path, with a job that begins `spark.sql("DROP TABLE IF EXISTS top_customers")`
and then rebuilds that table:

1. dry-run reports the statement `SKIPPED_UNRECOGNIZED` ("enforcement would reject it");
2. enforcing the draft dry-run produced rejects the job at that line, `UNVERIFIABLE_WRITE`;
3. the message tells the *user* to "add its class to FailClosedCommands' known-safe list … and
   open an issue/PR".

The same job with that one line removed passes. There is no setting or contract field that
lets the statement through. The same holds for `TRUNCATE TABLE t` before an `INSERT`, and for
`ALTER TABLE t DROP IF EXISTS PARTITION (dt='…')` before rewriting a partition: both everyday
ways to make a job re-runnable. Also rejected, though they delete no data:

- `DROP TABLE IF EXISTS x` when `x` does not exist;
- `DROP VIEW v` on a persistent view (the rule sees `DropTableCommand` with `isView = true`;
  `DropView` is on the safe list but is not what arrives);
- `CLEAR CACHE` (`ClearCacheCommand$` is not on the list; `UNCACHE TABLE` is).

### Fix

A contract lists what a job may change. So: **removing data from a declared output is allowed
when the contract lets that output be overwritten; removing data from anything else is
refused, with a message a user can act on.**

1. **Safe-list the two that delete nothing.** In `FailClosedCommands`:
   - add `"org.apache.spark.sql.execution.command.ClearCacheCommand$"` to `knownSafe` (comment:
     drops cached copies only);
   - in `isKnownSafe`, `case d: org.apache.spark.sql.execution.command.DropTableCommand => d.isView`
     (a view holds no data).

2. **New file `DestructiveDdlSupport.scala`**:

   ```scala
   final case class DestructiveDdl(statement: String, location: String, tableNames: List[String], removesEverything: Boolean)
   def extract(plan: LogicalPlan): Option[DestructiveDdl]
   ```

   | Plan | `removesEverything` | Location and names |
   |---|---|---|
   | `DropTable(ResolvedIdentifier(catalog, ident), _, _)` | true | names from `CatalogIdentitySupport.fromV2(catalog, ident)`; location: the table's, if it exists (load it as below), else `sessionState.catalog.defaultTablePath` for a session-catalog table, else the name |
   | `DropTableCommand(name, _, isView = false, _)` | true | from the catalog table |
   | `TruncateTableCommand(name, None)` | true | from the catalog table |
   | `TruncateTableCommand(name, Some(_))` | false | from the catalog table |
   | `TruncateTable(ResolvedTable)` | true | as H-05 does for a `ResolvedTable` |
   | `AlterTableDropPartitionCommand(name, …)` | false | from the catalog table |
   | catalyst `DropPartitions(ResolvedTable, …)`, `TruncatePartition(ResolvedTable, …)` | false | as for a `ResolvedTable` |

   Put "names and location of a V1 table", "of a `ResolvedTable`" and "of a
   `ResolvedIdentifier`" in one small helper object (`TableTargets`) that H-05 then reuses.
   Every catalog lookup is inside a `Try`; when one fails, use the table's name as the location.

3. **`ContractEnforcementRule.verifyOrThrow`**, non-write branch, after
   `StateChangingCallSupport` and before the fail-closed test:

   ```
   DestructiveDdlSupport.extract(plan) match {
     case Some(ddl) => judge(ddl)
     ...
   ```

   `judge`, with `names = ddl.location :: ddl.tableNames`:
   - `OutputChecker.matchOutput(contract.outputs, names)` is `None` →
     `rejectUnverifiableWrite` with name = the statement,
     problem `<statement> removes data from '<table>', which this contract does not declare as an output`,
     remediation `Declare '<table>' as an output of the contract if this job owns it (the job must then write it), or remove the statement.`
   - it is `Some(output)` and `output.saveMode` is defined and is not `overwrite` (compare
     without regard to case) → reject with
     `Violations.outputSaveModeMismatch(expected = <declared>, actual = "overwrite", writeLocation = ddl.location)`
     through `VerificationPipeline.reject` (`private[invaract]`; it publishes the `FAILED` event
     and throws). Render the plan as `ir.UnknownPlan(ddl.statement)`.
   - it is `Some(output)`, `ddl.removesEverything`, and a rule about deletes applies: use
     `RuleVerifier.anyRuleAppliesTo` with `MutationKind.Delete` (read its parameters at
     `verification-core/.../RuleVerifier.scala`) and, when it is true, reject with
     `Violations.unconditionalDeleteRule()` if the contract declares
     `forbid_unconditional_delete`, otherwise with
     `Violations.unverifiableDml("DELETE", Some(ddl.location))` (a custom delete rule this
     statement cannot be judged against).
   - otherwise → allowed. Call `VerificationPipeline.requireValidContract` first, then
     `VerificationPipeline.publishValidation(contract, VerificationResult.of("<id>@<version>", Nil), sink, applicationId, job)`
     so the event log shows it. **Do not** call `written(...)`: dropping or truncating an output
     is not writing it, and a job that drops a declared output and never rebuilds it must still
     end with `MISSING_OUTPUT`.

4. **Dry-run.** In `inferOutcome`, a plan `DestructiveDdlSupport.extract` recognizes returns
   `Skipped(InferenceStatus.SkippedUnsupported, "<statement>: allowed under enforcement when '<table>' is a declared output that may be overwritten")`
   instead of `SkippedUnrecognized`, placed before the fail-closed case. The newcomer's dry run
   then no longer reports a blocker for a job that will pass.

### Tests

New `DestructiveDdlSupportSpec` (parquet tables on the default catalog, one Delta table):

Declared output, no `saveMode` or `saveMode: overwrite`:
- `DROP TABLE IF EXISTS t` (existing) passes and the table is gone;
- `DROP TABLE IF EXISTS t` when `t` does not exist yet, with the output declared **by path**
  (the warehouse path, as a dry-run draft declares it) passes: this is the newcomer's case and
  depends on `defaultTablePath`;
- the same with the output declared by table name (`spark_catalog/default/t`) passes;
- `TRUNCATE TABLE t` passes and the table is empty;
- `ALTER TABLE t DROP IF EXISTS PARTITION (dt='a')` passes and only that partition is gone;
- one `PASSED` validation event each.

Declared output with `saveMode: append`: each of the three is rejected with
`OUTPUT_SAVE_MODE_MISMATCH` and the data is intact.

Declared output with the rule `forbid_unconditional_delete`: `DROP TABLE` and `TRUNCATE TABLE`
are rejected with `RULE_UNCONDITIONAL_DELETE`; `DROP PARTITION` and
`TRUNCATE TABLE … PARTITION` pass.

Not a declared output: each is rejected with `UNVERIFIABLE_WRITE`, the message contains the
table's name and the word `Declare`, and the data is intact. Includes
`DROP TABLE IF EXISTS no_such_table`.

Safe list: `DROP VIEW v` (persistent) passes and the view is gone; `CLEAR CACHE` passes;
`DROP TABLE t` on a real table is **not** let through by the `isView` test (this kills the
mutant that negates it).

End to end, in `DeclaredOutputsSpec`: a job that drops a declared output and never writes it
ends with `MISSING_OUTPUT`; one that drops then rewrites it ends clean.

Update `DryRunReporterSpec` for the new status of a recognized statement.

Then rerun the newcomer's path by hand and paste the result into the pull request: a PySpark
job that does `DROP TABLE IF EXISTS x`, rebuilds `x` with `saveAsTable`, run once with
`spark.invaract.dryRun=true` and a file sink, then with the summary's `mergedContractYaml` as
`spark.invaract.contract` and `spark.invaract.failOnMissingOutput=true`, must exit 0 both times.

### Documents

- `docs-site/.../concepts/fail-closed.mdx`: a section on removing data, with the decision table.
- `docs-site/.../troubleshooting/common-problems.mdx`, "A write is rejected with
  `UNVERIFIABLE_WRITE`": the three cases and what to do.
- `docs-site/.../guides/dry-run-mode.mdx`: the status table.
- `docs/SPARK_ADAPTER.md` and `docs/connectors/hive.md` ("`DROP TABLE` on an EXTERNAL table — a
  deliberate false rejection": now allowed for a declared output; correct it).

---

## H-11 Every `foreachBatch` stream is rejected

**Severity S2.** Medium. Do after H-03.

### Observed

```scala
stream.writeStream.foreachBatch { (batch: DataFrame, _: Long) =>
  batch.write.mode("append").parquet(declaredOutput)
}.start()
```

is rejected when the stream starts: `OUTPUT_LOCATION_MISMATCH … the plan writes to
'ForeachBatchSink'`, with the advice to "update the contract's declared output location to
'ForeachBatchSink'". This is the standard way to upsert a stream into Delta, and no contract
can let it through. Dry-run drafts an output located at `ForeachBatchSink`.

With the outer check forced open (a second output declared at the literal location
`ForeachBatchSink`), the write inside the function was rejected with `MISSING_INPUT`: the
batch's own plan reads the individual new files (`…/in/orders/part-00000-….parquet`), not the
declared folder.

What the rule sees during such a stream is in the table under "Facts about Spark 3.5.9". The
important part: the micro-batch plan and the batch DataFrame's `LogicalRDD` carry the **same
output attribute ids**, so `CheckpointRegistry` already ties the batch DataFrame back to the
micro-batch plan, and with it to the files it read.

### Fix

1. **The outer `WriteToStream` is not itself a write** when its sink is a `ForeachBatchSink`:
   the function's own writes are each checked. In `WriteCommandSupport`, make the
   `writeToStream` partial function not match such a plan (guard:
   `ws.sink.getClass.getSimpleName != "ForeachBatchSink"`), and export
   `def isForeachBatch(plan: LogicalPlan): Boolean`. `WriteToStream` is not a `Command`, so the
   plan then falls to "not a write" in `verifyOrThrow` and to `None` in `inferOutcome`, which is
   what is wanted in both. Log once at `INFO`:
   `Invaract: a foreachBatch stream is checked through the writes its function makes.`
2. **The inner write's input** then resolves through the registry to a read of files inside the
   declared input, which H-03's rule attributes to that input. No further code.
3. **`.foreach(writer)`, `console`, `memory`, `noop`** stay rejected: they hand rows to code or
   to a sink with no location. Only their message changes (H-17).

### Tests

In `ParquetConnectorSpec` (it already runs streams with `Trigger.AvailableNow`), with a file
stream source declared as the contract's input:
- `foreachBatch` writing the batch to the declared output passes, and the output has the rows;
- `foreachBatch` writing to an undeclared path: the stream's `exception` is a
  `ContractViolationException` with `OUTPUT_LOCATION_MISMATCH` naming that path (not
  `ForeachBatchSink`), and the path does not exist;
- with `rejectUndeclaredInputs`: a function that joins the batch with an undeclared table is
  rejected with `UNDECLARED_INPUT`;
- a function that writes nothing: the stream runs; with a declared output, the job ends with
  `MISSING_OUTPUT` (through `DeclaredOutputs.finishAll`);
- dry-run on the first case produces one draft, whose output is the real path and whose input
  is the declared folder (not a file, not `ForeachBatchSink`);
- a Delta sink inside the function (`batch.write.format("delta").mode("append").save(path)`)
  passes: add to the Delta section of `ContractEnforcementRuleSpec`.

`SparkPartitioningSpec` has a test named "…one into foreachBatch states nothing, so a
partitioned output refuses it". After this change the outer stream is not judged at all, so
that half of the test no longer applies: replace it with "a `foreachBatch` stream is not judged
as a write; a partitioned write inside its function is checked like any batch write", and keep
the memory-sink half.

If the source is not a file source (Kafka, `rate`), the micro-batch plan's leaf may not
translate to a read of the declared input, and the inner write is then `MISSING_INPUT` (or, with
`rejectUndeclaredInputs`, `UNVERIFIABLE_WRITE` from H-04). Add one test with the `rate` source
that records what happens, and document the result; do not try to fix it in this item.

### Documents

- `docs-site/.../reference/connector-support.mdx`: the `WriteToStream` row; add `foreachBatch`.
- `invaract-capabilities-spark.yaml`: `write.streaming` summary and note; `./dev/capabilities`.
- `docs/SPARK_ADAPTER.md`, streaming sections; `docs-site/.../guides/partitioning-outputs.mdx`
  (it mentions `foreachBatch`).

---

## H-12 A JDBC write can never match its declared output

**Severity S2.** Small.

### Observed

`df.write.mode("overwrite").jdbc(url, "OUT_J", props)` under a contract declaring the table:

```
OUTPUT_LOCATION_MISMATCH … the plan writes to
'Map(driver -> org.apache.derby.jdbc.EmbeddedDriver, url -> jdbc:derby:memory:probe2;create=true, dbtable -> OUT_J)'
```

A read of the same table is located at `jdbc:jdbc:derby:memory:probe2;create=true/OUT_J`
(`jdbc:` + the URL + `/` + the table). The user documents never say how a JDBC table is
written in a contract.

### Cause

`WriteCommandSupport.saveIntoDataSource` uses `cmd.options("path")` and, when there is none,
the options map's `toString`.

### Fix

In `saveIntoDataSource`, between "has a path" and the fallback: when the options hold both
`url` and `dbtable`, the location is `s"jdbc:${url}/${dbtable}"`, with no diagnostic. This is
the string `SparkPlanAdapter.jdbcLocationOf` builds for a read; move that one line into a
shared `def jdbcLocation(url: String, table: String): String` and call it from both, so they
cannot drift. `cmd.options` is case-insensitive already. The format is `jdbc` (the provider's
short name), as today.

Never put the options map in a location or a message again: it can hold a password. Change the
fallback's location to `s"${cmd.dataSource.getClass.getSimpleName}(no path)"`.

### Tests

In `ContractEnforcementRuleSpec` or a new `JdbcConnectorSpec`, with embedded Derby
(`org.apache.derby.jdbc.EmbeddedDriver`, `jdbc:derby:memory:<name>;create=true`; it is on the
test classpath):
- a write to the declared table (`location: "jdbc:<url>/<TABLE>"`, `format: jdbc`) passes and
  the rows are there;
- a write to another table is rejected with `OUTPUT_LOCATION_MISMATCH`, and the message does
  not contain `driver` or `Map(`;
- read the table back as an input and write a parquet output: the read and the write of the
  same table have the same location string;
- a `SaveIntoDataSourceCommand` with neither `path` nor `url` (use `format("noop")`) gets the
  new fallback text.

### Documents

- `docs-site/.../reference/connector-support.mdx`: JDBC row, Writes ✅.
- `docs-site/.../reference/contract-format.mdx`: under "Location matching", how a JDBC table is
  written (`jdbc:` followed by the connection URL, `/`, the table), with one real example from
  the test, and the remark that the URL's own `jdbc:` makes the prefix appear twice.
- New `docs/connectors/jdbc.md` with the two coverage ledgers
  (`docs/ADDING_A_SPARK_CONNECTOR.md`, Phase 11), filled in honestly: reads and `save`-style
  writes covered; everything else "not investigated".

---

## H-13 `INSERT OVERWRITE DIRECTORY` is rejected

**Severity S2.** Small.

### Observed

`INSERT OVERWRITE DIRECTORY '<declared output>' USING parquet SELECT …` is rejected with
`UNVERIFIABLE_WRITE` (`InsertIntoDataSourceDirCommand`). The Hive form of the statement is
already recognized (`insertIntoHiveDir`).

### Fix

In `WriteCommandSupport`, add `insertIntoDataSourceDir`, modelled on `insertIntoHiveDir` but
with a direct match (the class is in `spark-sql`):

```scala
case cmd: org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand =>
  WriteCommandInfo(location = <cmd.storage.locationUri, else the same fallback insertIntoHiveDir uses>,
                   query = cmd.query, format = Some(cmd.provider),
                   saveMode = Some(if (cmd.overwrite) "overwrite" else "append"),
                   outputSchema = cmd.query.schema, diagnostic = …)
```

Chain it into `combined`. The nested `InsertIntoHadoopFsRelationCommand` for the same path is
checked too, as for `saveAsTable`.

### Tests

In `ParquetConnectorSpec`: to the declared output passes and the files exist; to another
directory is rejected with `OUTPUT_LOCATION_MISMATCH` and nothing is written; a `USING csv`
statement against an output declared `format: parquet` is rejected with
`OUTPUT_FORMAT_MISMATCH`. Remove `InsertIntoDataSourceDirCommand` from any test or document
that lists it as failing closed (`FailClosedCommands`' comment, `docs/SPARK_ADAPTER.md`, and the
dry-run guide's `SKIPPED_UNRECOGNIZED` example, which uses this very statement: replace the
example with a statement that is still unrecognized, taken from a real run).

### Documents

`docs/connectors/parquet.md` and `csv.md` operation ledgers; the dry-run guide example above;
the "Recognized Spark write commands" table in `reference/connector-support.mdx`.

---

## H-14 Lineage is lost through views, CTEs and plain copies

**Severity S1** (a report states something false). Medium. **Prototyped.** Do before H-02.

### Observed

`orders.email` is tagged `pii` in the contract. The lineage and tags reported for the same
logic written five ways:

| How the job is written | `e2 = upper(email)` traced to | Tags |
|---|---|---|
| `orders.select($"id", upper($"email").as("e2"))` | `orders.email` | `pii` |
| `orders.createOrReplaceTempView("o")`; `SELECT id, upper(email) AS e2 FROM o` | nothing; derivation "Constant" | none |
| `SELECT a.id, b.email FROM o a JOIN o2 b ON …` (two temp views) | nothing | none |
| `WITH c AS (SELECT id, email FROM …) SELECT id, upper(email) AS e2 FROM c`, then `.write` | no lineage at all | none |
| `spark.read.parquet(orders).filter(…).write…` (a copy; no `select`) | no lineage at all | none |

So the commonest SQL style in PySpark (`createOrReplaceTempView` + `spark.sql`) and the
simplest way to move personal data (a copy) both report no sensitive columns. The contract
format reference says tags propagate "through however many computation steps sit in between".
Everything that consumes lineage is affected: sensitivity reporting, dry-run's "contributes to
an output column" note, role consistency, and static data-quality proofs.

### Cause

Three separate holes in `SparkPlanAdapter.Translator`:

1. `View` (the node Spark wraps a temp or persistent view in) has no case, so it becomes an
   opaque `ir.UnknownPlan`, through which `Lineage` resolves nothing.
2. `WithCTE`/`CTERelationDef`/`CTERelationRef` have no case either. They survive in a write's
   query when the query was built with `spark.sql("WITH …")` and written with `.write`.
3. `ir.Read` declares no columns (a documented property of the IR), so a plan with no
   projection above its reads has no output columns for `Lineage` to report. The IR's own
   document says a front end "is expected to expand any wildcard projection"; the Spark
   adapter does not.

### Fix

All in `SparkPlanAdapter.scala`. With these three changes the table above reports
`orders.email` and `pii` in every row (tried).

1. In `translateNode` (today's `translateNonWritePlan`), next to the `Deduplicate` case:

   ```scala
   case v: View => translatePlan(v.child)
   ```

2. Next to it:

   ```scala
   case w: WithCTE =>
     scala.util.Try(InlineCTE(alwaysInline = true).apply(w)).toOption.filterNot(_.isInstanceOf[WithCTE]) match {
       case Some(inlined) => translatePlan(inlined)
       case None          => <the existing generic UnknownPlan fallback for `w`>
     }
   ```

   (`org.apache.spark.sql.catalyst.plans.logical.{View, WithCTE}`,
   `org.apache.spark.sql.catalyst.optimizer.InlineCTE`.) The `filterNot` is what stops a plan
   that could not be inlined from recursing for ever. Extract the generic fallback into a
   small method so both this and `case other` use it.

3. Give every write an explicit column list:

   ```scala
   /** A projection of `query`'s own columns over `translated`, unless it already ends in one. */
   def withOutputProjection(query: LogicalPlan, translated: ir.Plan): ir.Plan = translated match {
     case _: ir.Project | _: ir.Aggregate => translated
     case other                           => ir.Project(other, query.output.map(translateNamed).toList)
   }
   ```

   Use it in `translatePlan`'s write case
   (`ir.Write(…, withOutputProjection(info.query, translatePlan(info.query)), …)`) and in
   `translateAsWrite`.

### What this changes in existing tests

The prototype was run against the whole `spark-adapter` suite; the result is recorded in
[Appendix A](#appendix-a-existing-tests-touched-by-the-h-14-prototype). Update each listed
expectation deliberately: read the new rendering, confirm it is the old plan with a `Project`
of the write's own columns on top (or a view/CTE node gone), and only then change the expected
value. A difference of any other kind is a bug in your change.

### Tests

New cases in `SparkPlanAdapterSpec` and a `LineageThroughViewsSpec`:
- each row of the table: `SensitivityLineage.propagate` gives `e2` (or `email`) the source
  `orders.email` and the tag `pii`;
- a persistent view (`CREATE VIEW v AS SELECT id, email AS mail FROM t`): `mail` traces to
  `t.email`;
- a CTE referenced twice (`FROM c a JOIN c b ON …`): both sides trace to the base table;
- a nested CTE; a CTE inside a view;
- copy, filter-only, sort-and-limit, `distinct`, `union` of two reads: every output column has
  a source;
- a write that already ends in a `select` renders exactly as before (the first arm of
  `withOutputProjection`): compare with a stored rendering;
- `MISSING_INPUT`/`UNDECLARED_INPUT` behavior through a view and through a CTE is unchanged
  (reads were already found): one test each, so a later change cannot regress it.

### Documents

- `docs/SPARK_ADAPTER.md`, "Translation coverage": rows for `View`, `WithCTE`, and the output
  projection; remove "`SubqueryAlias` over anything else … didn't come up in practice" if no
  longer accurate for views.
- `docs-site/.../guides/fingerprinting-transformations.mdx`: start a short list "Fingerprints
  change when Invaract learns to see more of a job", with this item's three shapes (a job that
  reads through a view, uses a CTE, or writes without a `select`). Read
  `docs/SEMANTIC_LINEAGE_FINGERPRINTING.md` §10 first and follow what it says about a change of
  this kind; if it requires bumping `FingerprintHasher.CurrentVersion`, stop and ask, because
  that is a change to the `fingerprint` module.
- `docs-site/.../reference/contract-format.mdx`, "Sensitivity tags": add views and CTEs to the
  list of what tags propagate through.

---

## H-15 Contract and policy files must be local files

**Severity S2.** Medium.

### Observed

`--conf spark.invaract.contract=file:///…/contract.yaml` fails with
`Contract file not found: file:/…/contract.yaml`: even a `file:` URI is not understood, so
`s3://`, `hdfs://`, `abfss://` and `dbfs:/` cannot work either *(by reading the code:
`ContractParser.parseFile` and `java.nio.file.Paths`; the same for `orgPolicy`,
`orgPolicyOverlays`, `locationMap` and `notifyConfig`)*. In cluster deploy mode the driver runs
on a machine the submitter does not control, so a platform must ship each file with `--files`
and name it by its bare file name. The install guide's own example uses `--master yarn` with an
absolute path and does not mention this.

### Fix

Make the five file settings accept any Hadoop file system URI on Spark, and keep plain paths
working exactly as today.

1. New file `RemoteFiles.scala` in `spark-adapter`,
   `private[sparkadapter] object RemoteFiles`:

   ```scala
   /** Whether `value` names a file through a URL scheme (`s3a://…`, `hdfs://…`, `file:/…`, `dbfs:/…`). */
   def isRemote(value: String): Boolean
   /** A local copy of `value`, fetched with the session's Hadoop configuration; `value` itself if it is a plain path. */
   def localize(value: String, session: SparkSession): String
   ```

   `isRemote`: matches `^[A-Za-z][A-Za-z0-9+.-]+:/` and is not a `registry://` or `ref://`
   reference (two letters or more, so a Windows drive `C:/` is a plain path).
   `localize`: for a remote value, open it with
   `new Path(value).getFileSystem(session.sparkContext.hadoopConfiguration)`, copy it to
   `Files.createTempFile("invaract-", "-" + <file name>)` (`deleteOnExit`), and return that
   path. On failure throw `IllegalArgumentException`:
   `'<setting>' names '<value>', which could not be read: <cause>`.

2. In `SparkConfigSource.apply`, wrap the lookup: for the names `InvaractConf.Contract`,
   `OrgPolicy`, `LocationMap`, `NotifyConfig`, return `localize(value)`; for
   `OrgPolicyOverlays`, split on commas, localize each, and rejoin. Cache each localized value
   per session (a `ConcurrentHashMap` keyed by the raw value) so a file is fetched once.
   `registry://` contracts are untouched.

3. Improve the plain-path error. In `ContractParser.parseFile`'s "Contract file not found"
   message (module `contract`; this is the one permitted change there), append:
   `If the job runs in cluster mode, ship the file with --files and name it by its file name.`

`HadoopFsNotificationSink` already writes through the Hadoop file system API; copy its way of
obtaining the configuration.

### Tests

`RemoteFilesSpec`: `isRemote` for a plain absolute path, a relative path, `file:/x`,
`file:///x`, `s3a://b/k`, `hdfs://nn:8020/x`, `dbfs:/x`, `C:/x`, `registry://a@1`, `ref://a`;
`localize` of a `file:` URI returns a readable copy with the same bytes; of a plain path returns
it unchanged; of a missing remote file throws naming the setting.
`InvaractSparkSessionExtensionSpec`: a session whose contract, org policy and location map are
all given as `file://` URIs enforces correctly; overlays given as a comma-separated list of
URIs all apply.

### Documents

- `docs-site/.../guides/installing-the-enforcement-rule.mdx`: a section "Where the contract
  file can live" (local path, any Hadoop URI, or `--files` + file name in cluster mode), and
  fix the `--master yarn` example.
- `docs-site/.../reference/configuration.md`: note on the five file settings.
- A troubleshooting entry for "Contract file not found" in cluster mode.

---

## H-16 One write publishes two or three validation events

**Severity S3.** Medium.

### Observed

`ContractValidationEvent`s published for one statement, all `PASSED`:

| Statement | Events |
|---|---|
| `df.write.parquet(path)` | 1 |
| `df.write.saveAsTable(newTable)` | 2 |
| `df.write.mode("append").saveAsTable(existingTable)` | 2 |
| `CREATE TABLE t USING delta AS SELECT …` | 3 |
| `INSERT INTO deltaTable SELECT …`, `MERGE INTO …` | 1 |

The two events of one `saveAsTable` carry **different** `eventId`s, so a consumer that follows
the documented advice to deduplicate on `eventId` still counts two. Counts of checks, and the
`checksPassed` of `JobSummaryEvent`, are inflated by how the job happens to be written.

### Cause

Spark analyzes the outer command and then the command(s) it runs inside; each reaches the rule
and each is a recognized write of the same output. This is already known for dry-run
(`ContractEnforcementRule.dryRun`'s comment) and for the catalog claim (`TableCreations`).

### Fix

Keep checking every plan (a nested plan that fails must still be blocked). Mark, do not drop:
add to `ContractValidationEvent` a field that lets a consumer tell a repeat from a new write.

This changes a public case class in `verification-core`, so it is the one item with a
deliberate binary break. Follow the "The break is deliberate" route in `CLAUDE.md`: add the
field **last**, with a default, add the `ProblemFilters` lines MiMa prints to
`mimaBinaryIssueFilters` in `verification-core/build.sbt` and `spark-adapter/build.sbt` with a
comment, and bump both modules' minor versions as their `build.sbt` comments describe for
every earlier field addition.

1. `ContractValidationEvent` gains `writeKey: Option[String] = None`: the canonical location of
   the write (`Locations.canonical`) plus `#` plus a per-write counter. `NotificationJson`
   writes it as `"writeKey"`; add it as an optional property to the event JSON Schema,
   `docs-site/public/schemas/notification/v1/event.schema.json`, and to any copy of that schema
   the test helper `EventSchema` validates against (search the repository for the file name).
2. In `spark-adapter`, a small `private[sparkadapter] object WriteKeys` keeps, per thread, the
   key of the write being analyzed. Spark analyzes the nested commands of one statement on the
   same thread, inside the outer command's execution, so: when `verifyOrThrow` checks a write
   whose canonical location equals the current thread's last key location **and** that key was
   set less than 60 seconds ago, reuse the key; otherwise mint a new one
   (`<location>#<AtomicLong.incrementAndGet>`). Pass it down to `VerificationPipeline` through
   a new optional parameter on a **new overload** of `verifyWrite` (do not change the existing
   signature).
3. Document: "Count writes by `writeKey`, not by event."

If, while implementing step 2, the nested commands turn out not to share a thread (check with a
test that records `Thread.currentThread().getId` in the rule for the three multi-event
statements above), stop and report: the key then needs a different carrier.

### Tests

For each row of the table, with a `TestNotificationSink`: the number of distinct `writeKey`s
is 1. Two consecutive `df.write.mode("append").parquet(samePath)` calls produce 2 distinct
keys (this is the case the 60-second-and-same-thread rule must not merge: the first write has
finished, so its key must be cleared when its check ends at the outermost level; write the
test first and let it drive that detail). Sixteen threads each doing a `saveAsTable` to their
own table produce 16 distinct keys. `EventSchema.assertValid` (used by `TestNotificationSink`)
accepts the new field.

### Documents

`docs-site/.../guides/notification-sinks.mdx`: the field, and the counting advice; the event
examples quoted in `dry-run-mode.mdx` and the notification guide gain the field only if you
regenerate them from a real run.

---

## H-17 Messages and warnings that mislead a newcomer

**Severity S3.** Six small changes; one pull request, or fold each into the item it touches.

| # | Observed | Change |
|---|---|---|
| a | `UNVERIFIABLE_WRITE`'s advice is for a contributor: "add its class to FailClosedCommands' known-safe list … and open an issue/PR". A user cannot do that. | In `FailClosedCommands.unverifiable`, new remediation: `Invaract cannot check '<class>'. If the statement only reads or manages metadata, please report it: <issue URL>. Otherwise write this output with a statement Invaract recognizes (see <docs URL of reference/connector-support>).` Keep the contributor guidance in `docs/SPARK_ADAPTER.md`, not in the message. |
| b | `OUTPUT_FIELD_NULLABILITY_MISMATCH` advises "Filter or coalesce nulls out of 'id'". `df.filter($"id".isNotNull)` was tried and is still rejected: Spark does not mark a column non-null because of a filter until it optimizes the plan, and the check runs before that. `coalesce($"id", lit(0))` passes. | In `Violations` (`verification-core`; permitted change), reword: `Give '<field>' a value that cannot be null, for example coalesce(<field>, <default>); filtering nulls out does not change the column's declared nullability. Or set nullable: true in the contract if nulls are expected.` Apply the same wording to the input-side variant only if it carries the same advice. |
| c | Enforcing a contract that dry-run itself drafted logs one warning per field: `Field is marked required but also nullable; a required field should not be nullable` (14 lines for a 14-field draft). `required` (the column exists) and `nullable` (its values may be null) are different things, and dry-run always drafts `required: true, nullable: true` for file data. | In `ContractValidator` (`contract`; permitted change), remove this warning. Update `ContractValidatorTest`'s assertion for it to assert its absence. Check `docs/CONTRACT_MODEL.md` and `reference/contract-format.mdx` for a sentence describing the warning and remove it. |
| d | When an organizational policy rejects the contract, **every** query fails, including `df.count()`, with "Contract violation: … rejected this transformation. Write aborted." | In `VerificationSetup.enforceOrgPolicy`'s rejection (`verification-core`; permitted change), use a headline that fits: `Contract '<id>@<version>' does not meet the organizational policy, so Invaract will not run this job's queries.` Find where `VerificationPipeline.explain` builds the headline and give it an optional override rather than a second copy of the explanation. |
| e | `spark.invaract.notifyConfig` naming a file with `sink.class` but no `sink.enabled=true` publishes nothing, silently. | In `AttachSetup.strictSink`/`lenientSink`, when the file loads and the sink is disabled, log once at `WARN`: `'<setting>' names '<path>', but sink.enabled is not true in it, so no events are published.` |
| f | `type: int` in a contract is a type mismatch against an `integer` column ("declares type 'int' but … has type 'integer'"), although `array<int>` is accepted. The reference documents this; newcomers copy types from `DESCRIBE TABLE`, which prints `int` and `bigint`. | In `SchemaChecker.typeMismatch` (`verification-core`; permitted change), after the exact comparison fails and the declared type has no `<`: if `LogicalType.parse(written)` gives a scalar type whose `typeName` equals `actual.typeName`, it is a match. Do **not** apply this when the declared type is the bare word `decimal`, `dec` or `numeric` (a bare decimal must keep not matching a column with a precision, as the reference says). In `ContractValidator`, stop warning for the aliases the parser accepts. |

### Tests

One per row: (a) the message contains the connector-support URL and not `FailClosedCommands`;
(b) message text, plus an enforcement test showing `filter(isNotNull)` rejected and `coalesce`
accepted, so the advice stays true; (c) a `required: true, nullable: true` field produces no
warning, and a dry-run draft enforced on its own job logs no `AttachSetup` warning at all;
(d) a `count()` under a failing policy throws with the new headline and without "Write
aborted"; (e) the warning appears, and does not appear for an enabled sink or with no
`notifyConfig`; (f) `int`/`bigint`/`smallint`/`tinyint`/`real` match `integer`/`long`/`short`/
`byte`/`float`; `int` does not match `long`; bare `decimal` still does not match
`decimal(38,18)`; `decimal(10,2)` still matches only itself.

Rows b, d, e and f change `verification-core`: run its own suite, mutation check and coverage,
and run `beam-adapter`'s suite too (`cd beam-adapter && sbt test`), since it shares the code.

### Documents

`reference/violation-types.md` (a, b), `reference/contract-format.mdx` "Known types" (f: remove
"a bare top-level `type:` must use the keyword … `integer`, not `int`"),
`guides/notification-sinks.mdx` (e).

---

## H-18 Documentation gaps

**Severity S3.** Do last.

1. **PySpark.** The Spark install guide never mentions Python, and most Spark jobs are PySpark.
   Add a section "PySpark jobs" to `guides/installing-the-enforcement-rule.mdx`: the same
   `spark-submit` line with a `.py` file (take it from the H-07 manual check), what a rejection
   looks like in a Python traceback (the four-part explanation is at the top of the
   `Py4JJavaError`; quote a real one, trimmed), and how to catch it
   (`except Exception as e: if "Contract violation" in str(e)`).

2. **Jobs that already set `spark.sql.extensions`.** Say that the value is a comma-separated
   list and show Invaract next to Delta's and Iceberg's extensions; point to the plugin (H-07)
   as the way not to depend on it.

3. **A page on what Invaract on Spark cannot see.** New
   `docs-site/src/content/docs/concepts/what-invaract-cannot-see.mdx`. Each line below was
   observed passing under a contract that should have stopped it, and is a property of checking
   a query plan, not a defect to fix:

   | The job does | Why it is invisible |
   |---|---|
   | `df.rdd.saveAsTextFile(path)`, `saveAsObjectFile` | an RDD action has no query plan |
   | `df.foreachPartition { … write files … }`, a `ForeachWriter` body | arbitrary code on the executors |
   | `df.collect()` then `java.io`, or `df.toPandas().to_csv(…)` | the data has left Spark |
   | `df.collect()` then `toDF` again | Invaract sees literal rows, not where they came from |
   | `FileSystem.rename` or `hadoop fs -mv` onto a declared output | not a Spark query |
   | `CREATE VIEW v AS SELECT * FROM <input>` | a view stores no data; who may read it is the catalog's access control |
   | a relative declared location (`out/result`) matched by `/anywhere/out/result` | by design; the `absolute locations` organizational policy closes it |
   | an output column the contract does not declare, with default options | by design; `rejectUndeclaredFields`, which a policy can force on, closes it |
   | a contract that names a **view** as an input | a read through a view is reported as a read of the tables under it; declare those |

   End the page with the settings a platform should pin (H-07) to close the ones that can be
   closed: `rejectUndeclaredInputs`, `rejectUndeclaredFields`, and a policy requiring absolute
   locations. Link the page from `concepts/fail-closed.mdx`, the install guide and the FAQ's
   "Does a passing contract mean my job's output is definitely correct?".

4. **`reference/connector-support.mdx`.** After H-11 to H-13 the table is out of date: update
   the write-command table, the JDBC row and the streaming row from the tests, not from memory.

5. **`CLAUDE.md`.** It states Spark 3.5.1 in "Quick Summary" and "Versions and Compatibility";
   the build uses 3.5.9 (`spark-adapter/src/main/resources/supported-versions.properties`).
   Correct both, or point them at that file.

6. **`docs/SPARK_ADAPTER.md`, "Known limitations".** Remove the limitations this plan closes and
   add the ones it leaves (see "Not fixed, by decision").

Finish with `cd docs-site && npm run build` and `cd contributor-docs && npm run build`.

---

## What was tested and held

So that nobody spends time re-testing these: each was tried and behaved correctly.

- **Writes to a place the contract does not declare** were blocked for `parquet`, `json`,
  `csv`, `text`, `orc`, Delta by path, `saveAsTable`, `CREATE TABLE … AS SELECT` with and
  without `LOCATION`, `INSERT INTO`, `INSERT … VALUES`, `insertInto`, `writeTo(...).create()`,
  file and table streaming sinks, and a write through `spark.newSession()`.
- **Hidden reads** were found through a join, a `UNION`, `EXCEPT`, a CTE, a temp view, a
  persistent view, `spark.table`, typed `Dataset` operations (`as[...]`, `map`), `.cache()`,
  `.checkpoint()`, `.localCheckpoint()`, a broadcast hint, a window, `LATERAL VIEW`, a
  `binaryFile` read, a recursive read of a parent folder and `SELECT … FROM parquet.\`path\``.
- **A job cannot overwrite its own declared input** (`MISSING_INPUT` + location mismatch).
- **Ordinary commands pass under a contract:** `SET`, `RESET`, `USE`, every `SHOW`/`DESCRIBE`,
  `EXPLAIN`, temp and global temp views, `CACHE`/`UNCACHE`, `REFRESH`, `ANALYZE`, `MSCK REPAIR`,
  `CREATE DATABASE`/`TABLE`, temporary functions, `COMMENT ON`, table properties, Delta
  `DESCRIBE HISTORY`/`DETAIL`, `OPTIMIZE`, `VACUUM`, and the `spark.catalog` API.
- **Robustness:** 2,000-column projections, a 500-way union, eight chained self-joins, pivot,
  cube, grouping sets, generators, higher-order functions, nested types, JSON functions, window
  frames, UDFs, typed operations, sampling, hints, unpivot, lateral subqueries, inline tables,
  odd literals (binary, `NaN`, intervals, maps) and Delta time travel all verified without
  error with every option switched on.
- **Concurrency:** eight threads each doing 24 writes (paths, `saveAsTable`, checkpointed) in
  one session: no error, 256 events, all `PASSED`.
- **Session handling:** `spark.newSession()` is enforced; `spark.conf.set("spark.invaract.…")`
  after the first query has no effect on the session, on a new session, or on a stream.
- **Location forms:** trailing slash, `file://`, `file:`, a non-normalized write path, and case
  differences behave as the reference says.
- **Errors a newcomer hits first** are clear: no contract set, contract file missing, invalid
  YAML (the message names the tab character and the line).
- **Dry run to enforcement:** a realistic PySpark job (CSV in, join, aggregate, a partitioned
  parquet output and a `saveAsTable`) passed under the contract its own dry run drafted, once
  the `DROP TABLE IF EXISTS` line was taken out (H-10).
- One apparent failure, `SHOW CURRENT NAMESPACE` throwing "Couldn't find a valid constructor",
  reproduces **without** Invaract once the session has a cached table: it is Spark's, not ours.

## Not fixed, by decision

- **Reading and writing outside a query plan** (RDD actions, `foreachPartition`, `collect`).
  Invisible by nature; documented in H-18.
- **A contract cannot name a view as its input.** Declaring the tables under the view works.
  Supporting the view's name as well would need an `ir.Read` to answer to two identities; it
  is a change to the IR and is left for a separate design.
- **`console`, `memory`, `noop` and `.foreach(writer)` sinks stay rejected.** They have no
  location to check. H-17a improves the message.
- **Sensitivity tags are reported, not enforced.** Already stated in the contract format
  reference; H-14 makes the report correct, which any future enforcement rule depends on.
- **Kafka, `rate` and other non-file sources inside `foreachBatch`.** H-11 records what happens
  and documents it.
- **Duplicate checks when the extension is listed twice** are removed by H-07; nothing else.
- **Spark Connect, Iceberg- and Hive-specific forms of the statements in H-05 and H-10, and
  cluster managers other than `local`** were not exercised in this test. Where an item matches
  a catalyst class (`AlterTableCommand`, `DropTable`, `TruncateTable`) the Iceberg form is
  covered by construction; add one Iceberg test per item in `IcebergConnectorSpec` to confirm,
  and record anything that differs in `docs/connectors/iceberg.md`.

## Appendix A: existing tests touched by the H-14 prototype

The three changes of H-14 were applied as written above and the whole `spark-adapter` suite
was run (`sbt test`, 871 tests). **869 passed and 2 failed**, both in `SparkPlanAdapterSpec`,
both for the expected reason:

| Test | Why it fails | Change to make |
|---|---|---|
| `translates a Delta write via SaveIntoDataSourceCommand, not falling through to Unsupported` | it matches `Write(DatasetRef(location), Read(_, None, _), …)`; the write's query is now `Project(Read(…), [id, value])` | match `Write(DatasetRef(location), Project(Read(_, None, _), columns), …)` and assert `columns.map(_.name) == List("id", "value")` |
| `translates a .saveAsTable() write via CreateDataSourceTableAsSelectCommand, not falling through to Unsupported` | the same | the same |

The rendering the two tests printed, for reference:

```
Write(/tmp/…/delta_write_test, format=delta, saveMode=overwrite)
└─ Project
   ├─ Read(file:/tmp/…/sample.csv)
   ├─ id = id
   └─ value = value
```

No other suite failed: enforcement, connectors, dry-run, fingerprint and conformance tests all
passed with the prototype in place. If your implementation of H-14 breaks any test other than
these two, the difference is in your change, not in the design.

The prototype was then removed; nothing of it is in the repository.
