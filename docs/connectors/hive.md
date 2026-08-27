# Hive support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Added via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md), against a real Hive-enabled session —
`SparkSession.Builder.enableHiveSupport()` with an embedded, per-test
Derby metastore (`javax.jdo.option.ConnectionURL` pointed at a temp
directory), no external Hive install or metastore service needed.
`org.apache.spark %% spark-hive % 3.5.1` was added to `spark-adapter/build.sbt`
as a `% "test"` dependency only — Spark's own first-party Hive
integration module, not an external connector library, but split out of
`spark-sql` into its own artifact for exactly the reason every other
connector in this document stays test-scoped: a job that never enables
Hive support shouldn't carry its metastore-client dependency footprint.

Unlike Delta/Iceberg's onboarding, this pass found the operation surface
genuinely under-covered rather than confirmed-by-analogy — a reflective
scan of `spark-hive`'s own jar for `Command` subclasses (the same
technique that found Delta's `MergeIntoCommand`) found exactly three
classes, and Phase 2's operation-surface probing against a real session
found a fourth real gap on the *read* side that predates this pass
entirely.

## `HiveTableRelation` reads — zero new dependency, one real bug fixed

The single most consequential finding: `HiveTableRelation` (the read-side
shape for a genuinely Hive-native table — any non-Parquet/ORC SerDe, or a
Parquet/ORC table with `spark.sql.hive.convertMetastore{Parquet,Orc}`
explicitly disabled) is **not** `LogicalRelation`-wrapped the way Delta's
read shape turned out to be (see "Delta Lake reads" above) — it's its own
top-level `LeafNode`, confirmed empirically via a real `injectCheckRule`
probe's full `treeString` output, not assumed from the class name. Before
this pass, `SparkPlanAdapter.Translator.translatePlan` had no case for
it at all, so it fell all the way through to the generic `Unsupported`
fallback — worse than an imprecise location (the fate of every other
unrecognized relation kind wrapped in `LogicalRelation`): a Hive-native
table could never satisfy a contract's declared input, full stop. This
corrects this document's own previous "Known limitations" bullet, which
described the fallback mechanism slightly wrong (it named the
`LogicalRelation`/`catalogTable` fallback) while correctly flagging the
gap as real and untested.

**Zero new dependency needed to fix it.** `HiveTableRelation` itself
lives in `org.apache.spark.sql.catalyst.catalog` — plain, public
`spark-catalyst`, already on this module's `provided` Spark dependency,
not the separate `spark-hive` artifact. `SparkPlanAdapter` now imports it
directly and pattern-matches on it like any other Catalyst type, no
reflection needed (unlike every Hive-specific case on the write side
below). `hiveTableRelationLocationOf` resolves
`tableMeta.storage.locationUri`, falling back to the table's qualified
identifier with a diagnostic if unset (not observed in practice — every
real Hive table has a resolved storage location by the time it's
readable). `ContractEnforcementRule.recognizedRead` got the matching
case, so a Hive-native table now also satisfies a contract's declared
`input` the same way every other read shape does.

A Parquet/ORC Hive table with metastore conversion **on** (the default)
was confirmed, separately, to still resolve to the pre-existing
`LogicalRelation`/`HadoopFsRelation` case — the conversion setting, not
the on-disk file format, determines which shape appears; both are tested
in `HiveConnectorSpec`.

## Write shapes — three real classes, all newly translated

The reflective jar scan found exactly three concrete `Command` subclasses
in `spark-hive`, all genuinely data-writing, none needing a
`FailClosedCommands` safe-list entry (they're translated writes instead —
see "Write command recognition: a single registry" below):

- **`CreateHiveTableAsSelectCommand`** — `.format("hive").saveAsTable(...)`
  and `CREATE TABLE ... STORED AS ... AS SELECT ...`. Confirmed
  empirically to be used for *both* a genuinely new table and an append
  onto an existing one (Hive's own command doesn't distinguish the two
  the way `CreateDataSourceTableAsSelectCommand` does) — its `run()`
  internally creates or verifies the table, then issues a second, nested
  `InsertIntoHiveTable` to perform the actual write, the same
  "one logical call, two Command-shaped plans" pitfall Parquet/Delta/
  Iceberg's CTAS already document (see "A shared pitfall" below Delta's
  ledger). `format` comes from `tableDesc.provider` (`"hive"`); location
  from `tableDesc.storage.locationUri`.
- **`InsertIntoHiveTable`** — `.insertInto(...)`, `INSERT [OVERWRITE]
  [INTO] TABLE ...` against an existing table, and the nested write the
  command above issues internally. `overwrite: Boolean` maps directly to
  the contract's `append`/`overwrite` save modes — confirmed via a real
  `INSERT OVERWRITE` test.
- **`InsertIntoHiveDirCommand`** — `INSERT [OVERWRITE] [LOCAL] DIRECTORY
  '<path>' [ROW FORMAT ...] SELECT ...`, Hive's directory-export write.
  Found *only* by the reflective scan — it isn't a catalog-table write at
  all, so none of the standard `.save`/`.saveAsTable`/`.insertInto` probes
  in Phase 2 would ever have triggered it, the same lesson Delta's
  `MergeIntoCommand` taught the first time this process was run. Writes
  to an arbitrary filesystem path outside the catalog; `format` is
  reported as `"hive"` uniformly (there's no `CatalogTable` to read a
  provider string from).

All three are matched by fully-qualified class name and read via public-
method reflection — the same convention `WriteCommandSupport.deltaRowLevelDml`
uses, for a related but distinct reason: these three *are* stable,
documented Spark classes (not undocumented internals the way Delta's
`MergeIntoCommand` is), it's just that `spark-hive` is a separate,
optional, and much heavier artifact this module deliberately keeps off
its `provided`/compile classpath, so a hard import isn't available.

## A real, found-and-fixed false-rejection bug: static-partition INSERT

The same false-rejection bug class Delta's generated columns and DSv2's
target-only fields already taught this module to watch for, found again
under a third, independent mechanism: `INSERT INTO t PARTITION(dt=
'2024-01-01') SELECT ...` supplies the partition value as a literal in
the `PARTITION` clause — confirmed empirically (not assumed) that this
column **never appears in the query's own schema at all** (`InsertIntoHiveTable.query.schema`
is missing it entirely, and `outputColumnNames` excludes it too). A
contract requiring that column would have been falsely rejected with
`MISSING_OUTPUT_FIELD` for a write that genuinely produces it.

A **dynamic**-partition insert (`INSERT INTO t SELECT ..., dt`) does
*not* have this problem — confirmed separately that the value comes from
the `SELECT` itself, already part of `query.schema`, so the fix below is
a no-op there.

Fixed in `WriteCommandSupport.insertIntoHiveTable` by reusing the
existing `unionNewFields` helper (previously private to the DSv2 target-
only-fields fix and Delta's schema-evolution fix) to union any of
`table.schema`'s fields not already present in `query.schema` into
`outputSchema`, with a diagnostic. Safe for the same reason the DSv2 fix
is safe: by the time this command reaches the check rule at all, Spark's
own analyzer has already validated the write's column count/types
against the target (a genuinely missing *data* column fails analysis
before any plan is produced here) — the union can only ever add a field
Spark itself has already endorsed as legitimately absent from the query.

A related, second finding surfaced fixing this: the unioned partition-
column field comes from `CatalogTable.schema`, whose columns
(partition columns included) are always reported nullable — Hive's
classic DDL has no `NOT NULL` column constraint to preserve in the first
place, the same "every field nullable" finding independently confirmed
for Hive's ordinary data columns below. A contract declaring a static-
partition column `required: true` would still (correctly) fail
`OUTPUT_FIELD_NULLABILITY_MISMATCH` — not a bug, a second, orthogonal
instance of a finding this document already tracks for Parquet/CSV.

Both directions verified in `HiveConnectorSpec`: a PASS test proving the
static-partition case is no longer falsely rejected, a second PASS
proving the (already-correct) dynamic-partition case, and a FAIL test
proving a genuinely missing *data* field is still correctly caught
alongside the partition-column fix.

## Known limitation: CTAS with no pre-existing physical path

**Found, not fixed** — the same class of gap Parquet's own `.saveAsTable()`-
on-a-brand-new-table test already documents as out of scope, confirmed
here to also apply to Hive, plus one Hive-specific extension of it.

`CreateHiveTableAsSelectCommand.tableDesc.storage.locationUri` is
populated with the table's real physical path only when **appending
onto an existing table** — the analyzer resolves `tableDesc` by looking
up the existing catalog entry. It is `None`, falling back to the
qualified catalog identifier (`spark_catalog.default.<table>`), both for
a genuinely **new** table (nothing to look up yet — the same "no explicit
path given" gap `CreateDataSourceTableAsSelectCommand` has, which
Parquet's tests sidestep via `.option("path", ...)`, a knob Hive's
`.saveAsTable()` has no equivalent of) **and**, confirmed empirically as
a real, Hive-specific surprise, for `.mode("overwrite")` onto an
**existing** table — overwrite is treated as replace-like, so the
analyzer builds `tableDesc` fresh rather than consulting the existing
entry, even though the table (and its real location) already exists.

The nested `InsertIntoHiveTable`, built during `run()` after the table
has been created or verified, always resolves the real physical path —
so for both the new-table and overwrite-onto-existing cases, the outer
and inner commands disagree on location, and a contract targeting the
real physical path is rejected at the *outer* check before the inner,
genuinely-correct one ever runs.

**Append mode is the one case confirmed to agree by construction** — the
PASS/FAIL pair in `HiveConnectorSpec` for the "two nested Command plans"
scenario deliberately uses append, matching the one case verified safe.
The overwrite-mode gap has its own dedicated test
(`HiveConnectorSpec`'s "known limitation" test) proving the outer
command's location really is the qualified identifier, not the physical
path — a standing regression check on the gap itself, not just a
disclosed limitation. **Next step, if closed later**: no `SparkSession`
reference is threaded through `WriteCommandSupport` today (it's a pure
`LogicalPlan → WriteCommandInfo` function), so resolving this would need
either passing one through (a real API shape change) or accepting the
qualified-identifier form as an equally valid contract `location` for
this one write shape — neither attempted here, consistent with the
Parquet precedent of leaving the analogous gap as documented future work
rather than a scope-widening fix.

## Feature surface

- **Bucketed tables (`CLUSTERED BY ... INTO n BUCKETS`)** — confirmed
  transparent, no fix needed. Bucketing metadata lives entirely in
  `InsertIntoHiveTable.bucketSpec`, never touches the translated schema,
  location, or format on either the read or write side.
  `HiveConnectorSpec`'s permanent test.
- **A real Hive UDF** (not Spark's own `ScalaUDF`) — confirmed
  transparent. `isOpaqueUdf`'s existing `n.endsWith("HiveGenericUDF")`
  check (already in the codebase, previously untested against a real
  Hive UDF for lack of a metastore) correctly recognizes
  `org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper`, translating it
  as an opaque `FunctionCall` with a diagnostic — no fix needed, just the
  first real confirmation. `HiveConnectorSpec`'s permanent test.
- **Nullability on read-back** — confirmed, the same practical
  consequence as Parquet's/CSV's own version of this finding, for an
  independent reason: classic Hive DDL has no `NOT NULL` column
  constraint at all, so every column — data or partition — is always
  nullable in the catalog's own schema, regardless of what's actually
  written. `HiveConnectorSpec`'s PASS/FAIL pair (a `required: false`
  contract passes; `required: true` is correctly rejected as
  `OUTPUT_FIELD_NULLABILITY_MISMATCH`).

## Fail-closed policy — confirmed, not extended

No `FailClosedCommands` changes were needed. Hive's own maintenance/DDL
surface reuses generic Spark commands this module already classified
during Delta/Iceberg's onboarding — confirmed for real against a Hive
table, not left as a theoretical carryover:

- `CreateTableCommand` (plain `CREATE TABLE`, no `AS SELECT`) — already
  safe-listed, confirmed safe for a Hive-format table too.
- `AnalyzeTableCommand`, `ShowTables`, `RepairTableCommand` (`MSCK REPAIR
  TABLE`), `AlterTableAddPartitionCommand` — already safe-listed,
  confirmed for real against a partitioned Hive table.
- `LoadDataCommand` (`LOAD DATA [LOCAL] INPATH ... INTO TABLE`) and
  `TruncateTableCommand` — already correctly *excluded* (documented as
  genuinely data-mutating, deliberately unmodeled) — confirmed for real:
  both are rejected, and the target table's data is left unchanged.
- `MergeIntoTable`/`UpdateTable` — confirmed empirically to be real,
  `Command`-shaped plans for a plain Hive table's `MERGE INTO`/`UPDATE`
  (unlike Iceberg, which rewrites these into `ReplaceData`/`WriteDelta`
  before they'd ever reach this point — Hive tables don't implement
  `SupportsRowLevelOperations`, so the rewrite never applies). Already
  correctly excluded from the safe list (this document's own exclusion
  list already named both). Confirmed for real: Invariant's own
  `UnverifiableWrite` rejection fires *before* Spark's own `MERGE INTO
  TABLE is not supported temporarily`/`UPDATE TABLE is not supported
  temporarily` error ever would. `DELETE FROM` against a plain Hive table
  is rejected by Spark itself before producing any `Command`-shaped plan
  at all — genuinely nothing for this policy to classify.

## `.writeTo()` and streaming writes — N/A, confirmed by Spark itself

Both confirmed to be rejected by Spark before producing any analyzable
plan Invariant could see, the same pattern already documented for
Parquet/CSV: `.writeTo(...).append()` against a Hive table fails with
`Cannot write into v1 table` (the default `spark_catalog` isn't a
`SupportsWrite`-capable V2 catalog for V1-provider-backed tables); a
streaming `.writeStream...toTable(...)` against a Hive table fails with
`The input source(...) is different from the table ...'s data source
provider(hive)` — Hive isn't a valid streaming sink format at all.
`HiveConnectorSpec`'s two tests.

## Hive operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("hive").load(path)` | 🚫 **N/A — no such mechanism exists** | Hive is catalog-only; there is no path-based `.load()` form for it (Hive tables are always addressed by catalog identifier). Nothing to translate. |
| Catalog table read (`spark.table(...)`/`SELECT * FROM t`) | ✅ **Covered — closed this pass** | `HiveTableRelation`, a real, previously-untranslated read shape — see above. `HiveConnectorSpec`'s translation test and PASS enforcement test. Parquet/ORC-with-conversion reads confirmed to still use the pre-existing `LogicalRelation` case, also tested. |
| Time travel / snapshot reads | 🚫 **N/A — no such mechanism exists** | Hive tables have no versioning/snapshot concept. Nothing to translate. |
| Streaming read (`readStream`) | 🚫 **N/A — no such mechanism exists** | No streaming source implementation exists for the Hive table format. Not attempted; genuinely nothing to test against. |
| Change-data-feed / incremental read | 🚫 **N/A — no such mechanism exists** | No CDC mechanism exists for Hive tables. Nothing to translate. |
| `.save(path)` | 🚫 **N/A — rejected by Spark itself** | `df.write.format("hive").save(path)` is rejected outright by Spark (`Hive data source can only be used with tables`) — confirmed via the Spark documentation this module's Phase 0 investigation checked; Hive is `.saveAsTable()`-only by design. Not a translation gap. |
| `.saveAsTable(...)`, new table | ✅ **Covered — closed this pass, with a known limitation** | `CreateHiveTableAsSelectCommand` + nested `InsertIntoHiveTable`, both real `WriteCommandSupport` entries. Translation-level test passes; enforcement has a documented, tested gap when no physical path pre-exists — see "Known limitation" above. Next step: thread a `SparkSession` reference through `WriteCommandSupport` (a real API shape change) or accept the qualified identifier as an equally valid declared `location`, neither attempted here. |
| `.saveAsTable(...)`, existing table (append) | ✅ **Covered — closed this pass** | Same two commands, confirmed to agree on the real physical path for append mode specifically. `HiveConnectorSpec`'s PASS/FAIL pair. |
| `.insertInto(...)` | ✅ **Covered — closed this pass** | `InsertIntoHiveTable`. `HiveConnectorSpec`'s PASS/FAIL pair, plus a dedicated translation test confirming `INSERT OVERWRITE`'s save mode. |
| `.writeTo(...)` (DataFrameWriterV2) | 🚫 **N/A — rejected by Spark itself, not an Invariant gap** | `Cannot write into v1 table` — the default `spark_catalog` isn't a `SupportsWrite` V2 catalog for Hive tables. `HiveConnectorSpec`'s test. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | 🚫 **Fails closed (MERGE/UPDATE), N/A (DELETE)** | `MergeIntoTable`/`UpdateTable` are real `Command`-shaped plans, correctly rejected by the existing generic exclusion (no new code needed) — `HiveConnectorSpec`'s enforcement test. `DELETE FROM` is rejected by Spark itself before any plan is produced. Next step, if ever pursued: would need the same kind of structural-only treatment `deltaRowLevelDml`/`dsv2RowLevelWrite` give Delta/DSv2 MERGE — not attempted, since plain Hive tables have no ACID/row-level-mutation storage layer in vanilla OSS Spark to make this meaningful. |
| Streaming write | 🚫 **N/A — rejected by Spark itself** | No valid streaming sink format for Hive tables — confirmed via a real rejected `.toTable()` call. `HiveConnectorSpec`'s test. |
| Maintenance operations that touch data | ✅ **Covered by policy classification** | `LoadDataCommand` (`LOAD DATA INPATH`) and `TruncateTableCommand` confirmed, for real against a Hive table, to already be correctly excluded from `FailClosedCommands`' safe list — both fail closed, with a dedicated test proving the target table's data is left unchanged. `INSERT ... DIRECTORY` (`InsertIntoHiveDirCommand`) is a real, translated write, not a maintenance op — see above. |

## Hive feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| Static-partition `INSERT ... PARTITION(...)` | 🔧 **Found and fixed** | The partition column is missing from the query's own schema entirely — real false-rejection bug, fixed via `unionNewFields` in `WriteCommandSupport.insertIntoHiveTable`. `HiveConnectorSpec`'s PASS test (the fix) and FAIL test (a genuinely missing data field is still caught). |
| Dynamic-partition `INSERT ... SELECT ..., col` | ✅ Confirmed — already correct | The partition column comes from the `SELECT` itself, already part of `query.schema` — no fix needed. `HiveConnectorSpec`'s PASS test. |
| Bucketed tables (`CLUSTERED BY ... INTO n BUCKETS`) | ✅ Confirmed transparent | Bucketing metadata never affects the translated schema/location/format on either read or write. `HiveConnectorSpec`'s test. |
| Real Hive UDFs (`HiveSimpleUDF`/`HiveGenericUDF`) | ✅ Confirmed transparent | The existing `isOpaqueUdf` suffix check, previously untested against a real Hive UDF, correctly recognizes `GenericUDFUpper`. `HiveConnectorSpec`'s test. |
| Nullability on read-back | ✅ Confirmed — independently, same practical consequence as Parquet/CSV | Every Hive column (data or partition) is always nullable in the catalog schema — classic Hive DDL has no `NOT NULL` constraint to preserve. `HiveConnectorSpec`'s PASS/FAIL pair. |
| Metastore-conversion toggle (`spark.sql.hive.convertMetastoreParquet`) | ✅ Confirmed — determines which operation-surface row applies, not a bug | With conversion on (default), a Parquet/ORC Hive table's reads/writes use the pre-existing `LogicalRelation`/`InsertIntoHadoopFsRelationCommand` cases; with it off, the same table uses `HiveTableRelation`/`InsertIntoHiveTable` instead. Both confirmed correct. `HiveConnectorSpec`'s tests for both settings. |

Net assessment: Hive is not "100% supported," the same honest framing
every other connector in this document gets — but every operation-surface
row has a disposition and every disposition but one is ✅ Covered, and
that one row's gap is a tested, documented limitation with a concrete
next step, not a silent one. Two real bugs were found and fixed (the
`HiveTableRelation` read gap, the static-partition schema gap); one real
gap was found and left open with a standing regression test proving it's
still there (CTAS/overwrite location resolution); everything else was a
genuine confirmation, not an assumption carried over from Delta/Iceberg/
Parquet's own onboarding. `HiveConnectorSpec`: 26 tests.

