# Parquet support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Added via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md), but a different shape of investigation
than Delta/Iceberg: **Parquet is not a separate connector library at
all.** `org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat`
is Spark's own built-in `FileFormat`, already on this module's `provided`
`spark-sql` dependency — nothing to add to `build.sbt`, not even as a
`% "test"` dependency. It registers no catalog of its own (`.saveAsTable`/
`spark.table(...)`/SQL DDL against a plain `USING parquet` table go
through Spark's default `spark_catalog`, the same one CSV/JSON use), has
no row-level DML, no versioning/time-travel, and — confirmed by the same
reflective jar-scan technique used for Delta/Iceberg, this time against
`spark-sql`/`spark-catalyst` themselves with no separate connector jar to
scan — adds no SQL-extension `Command` classes of its own. Every Parquet
operation routes through generic Spark commands `WriteCommandSupport`/
`FailClosedCommands` already recognize (`InsertIntoHadoopFsRelationCommand`,
`CreateDataSourceTableAsSelectCommand`, `AppendData`/`OverwriteByExpression`/
`ReplaceTableAsSelect`/`CreateTableAsSelect`, `WriteToStream`) — most of
them literally *because* they were built and tested against Parquet
first (the demo harness's own `./dev/test` writes Parquet; the original
`ContractEnforcementRuleSpec` PASS/FAIL pair predates Delta/Iceberg
entirely).

So this pass was mostly an audit — real, empirical confirmation that
those generic mechanisms cover Parquet specifically for every row of "The
operation surface," not assumed from the Delta/Iceberg precedent — plus
two real findings along the way, one a genuine bug.

**A real bug found and fixed: streaming writes to a plain Parquet sink
resolved to a useless, non-matching location.** `WriteCommandSupport`'s
`WriteToStream` case (built for Delta) tries, in order: `sink.name()`
(tier 2), then a reflective call to a public `path()` accessor (tier 3),
then falls back to the sink's own `toString`. The first investigation
attempt assumed Spark's own built-in `FileStreamSink` — the sink
`.writeStream.format("parquet"/"csv"/"json"/"orc"/"text")` uses, not
Parquet-specific — reached tier 2 successfully, since its `toString()`
renders as a descriptive `"FileSink[<path>]"` string; a direct-construction
probe (constructing a real `FileStreamSink` by hand, no live query needed)
corrected that: `FileStreamSink.name()` throws the exact same `Sink`-trait
default `IllegalStateException` `DeltaSink.name()` does — it does *not*
override `name()` with a non-throwing implementation, despite `toString()`
appearing to suggest otherwise. Tier 2 already correctly fails for it, the
same as for Delta. **The real bug was in tier 3**: `reflectiveSinkPath`
only ever tried a *public method* named `path` — exactly what `DeltaSink`
exposes, but `FileStreamSink.path` (confirmed via `javap`) is a `private
final` field with no public accessor at all, so tier 3 found nothing
either, and every plain `FileFormat`-based streaming write fell all the
way to the last-resort `ws.sink.toString` — never a contract's real
declared physical path, unconditionally. Caught by a real
`ParquetConnectorSpec` PASS test failing with `OUTPUT_LOCATION_MISMATCH`
against the write's own real output directory — not inspection, and the
first (wrong) root-cause theory was itself caught the same way: a
manually-applied mutation of the originally-added tier-2 guard produced
no observable test failure, which is exactly the "equivalent mutant"
signature — the trigger to re-investigate rather than write a test to
force a false kill.

Fixed in `WriteCommandSupport.reflectiveSinkPath` (see that file's own doc
comment): extended from "public method" to "declared field" — the same
no-compile-time-dependency-tolerant convention as everywhere else in this
file, since `FileStreamSink`, while a plain public class already on this
module's `provided` Spark dependency, exposes neither `path` nor
`fileFormat` as a public method. The format fix is a genuine bonus, not
just a location fix: `streamSinkFormatOf` previously always returned
`None` for any non-Delta sink, `FileStreamSink` included — a new branch
reflects into the private `fileFormat` field and reuses
`SparkPlanAdapter.formatOf`'s existing `DataSourceRegister.shortName()`
mechanism, so any `FileFormat`-based streaming write (Parquet, CSV, JSON,
ORC, text — all share this one sink class) gets a real, precise format
for free, not just Parquet. Verified via a direct-construction test in
`ParquetConnectorSpec` (a real `FileStreamSink`/`WriteToStream` built by
hand and translated synchronously, no live streaming query or
`QueryExecutionListener` timing involved) asserting both the real
physical path and `format = Some("parquet")` in one deterministic
assertion, plus the existing streaming-query-based PASS/FAIL pair, plus
confirmation the full `spark-adapter` suite (140 tests, all 7 specs
including the pre-existing Delta streaming-write tests) still passes
unchanged.

**A genuinely new operation-surface finding, not a bug: `.saveAsTable()`
append onto an *existing* plain-Parquet table is a third instance of the
"one call, two nested Command-shaped plans" pattern** already documented
above for Delta/Iceberg's `StagedTable`-based atomic CTAS/RTAS — but via
a different mechanism. Confirmed empirically:
`CreateDataSourceTableAsSelectCommand.run()` itself detects the target
table already exists and, rather than creating a new one, internally
executes a second, nested `InsertIntoHadoopFsRelationCommand` to perform
the actual insert — both visible to `injectCheckRule`, meaning
`ContractEnforcementRule.verifyOrThrow` runs *twice* for one logical
write, the same risk profile as the `StagedTable` case. Unlike that case,
this one needed **no fix**: both plans resolve to the identical physical
location (the existing table's real storage path, confirmed via `cmd.table.storage.locationUri`
on the outer command and `cmd.outputPath` on the nested one), so a
satisfying write passes both checks and a violating write is rejected at
the first (outer) command, before the nested insert ever runs — verified
by a real PASS/FAIL pair (`ParquetConnectorSpec`), the FAIL half
additionally asserting the target table's row count is unchanged (proof
the nested insert genuinely never executed, not just that an exception
was thrown somewhere). Documented here as a known trap for whichever
future connector or write shape reaches this pattern next, per the
existing "shared pitfall" sections' own stated purpose — a genuinely new
table created via `.saveAsTable()`/`.writeTo(...).create()` *without* an
explicit path option has a related, unresolved version of this same risk
(the outer command's location falls back to the qualified catalog
identifier per the existing "No storage location on new table"
diagnostic, while the nested insert's location is the real physical
path — the two don't match) — left out of scope for this pass since
every existing precedent test (Delta's, Iceberg's, and this pass's own)
sidesteps it with an explicit path, so it was never exercised for real;
flagged here rather than left implicit.

## Parquet operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("parquet").load(path)`/`.parquet(path)` | ✅ Covered | Pre-existing generic `LogicalRelation`+`HadoopFsRelation` handling, in place since before Delta/Iceberg existed. `SparkPlanAdapterSpec`'s CSV/JSON/Parquet read-format test; `ContractEnforcementRuleSpec`'s original Parquet PASS/FAIL pair. |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM t`) | ✅ Covered | Same `LogicalRelation` shape via the default `spark_catalog`, confirmed via `injectCheckRule` probe (`SubqueryAlias` over `Relation ... parquet`). `ParquetConnectorSpec`'s `.insertInto()` test and `StructuralVerifierSpec`'s existing demo-plan tests exercise a catalog table read/write. |
| Time travel / snapshot reads | 🚫 **N/A, not a real gap** | Plain Parquet (`USING parquet`, no table-format layer) has no versioning concept at all — no `VERSION AS OF`/`TIMESTAMP AS OF` equivalent exists for it in Spark's own SQL grammar. Not future work; there is nothing to translate. |
| Streaming read (`readStream`) | ✅ Covered | Generic `StreamingRelation` handling (built for Delta), confirmed empirically for Parquet's own `FileSource` — a plain public spark-sql class, simpler than Delta's case (no anonymous-subclass wrinkle). `ParquetConnectorSpec`'s "PASS: a streaming Parquet source satisfies a contract's declared input schema." |
| Change-data-feed / incremental read | 🚫 **N/A, not a real gap** | No CDC mechanism exists for plain Parquet — this is a table-format feature (Delta's `readChangeFeed`), not something a bare file format has an equivalent of. Nothing to translate. |
| `.save(path)`, all four save modes | ✅ Covered | `InsertIntoHadoopFsRelationCommand` — the original, Parquet-first write case (see "Translation coverage" above). Confirmed each mode (`append`/`overwrite`/`ignore`/`error`) produces this same command via a real `injectCheckRule` probe. `ContractEnforcementRuleSpec`'s original PASS/FAIL pair. |
| `.saveAsTable(...)`, new table | ✅ Covered | `CreateDataSourceTableAsSelectCommand`, `table.provider` defaulting to `"parquet"` — the original test case for this write shape, predating Delta/Iceberg. `SparkPlanAdapterSpec`'s existing `.saveAsTable()` test. |
| `.saveAsTable(...)`, existing table (append) | ✅ **Covered — closed this pass** | A real, newly-found structural trap (see above): confirmed via `injectCheckRule` to produce two nested `Command`-shaped plans (`CreateDataSourceTableAsSelectCommand` + a nested `InsertIntoHadoopFsRelationCommand`), both independently checked — verified not to cause a false-pass or false-reject. `ParquetConnectorSpec`'s PASS/FAIL pair (the FAIL half also asserts the nested insert never ran). |
| `.insertInto(...)` | ✅ Covered | Same `InsertIntoHadoopFsRelationCommand` shape as `.save(path)`. `ParquetConnectorSpec`'s translation test via `SparkAdapterListener`. |
| `.writeTo(...)` (DataFrameWriterV2) — `.append()`/`.overwrite(cond)`/`.overwritePartitions()` against an *existing* table | 🚫 **N/A — rejected by Spark itself, not an Invariant gap** | Confirmed empirically: Spark's own analyzer refuses all three with `AnalysisException: Cannot write into v1 table` — a permanent architectural constraint (plain Parquet never implements the V2 `SupportsWrite` capability under the default `useV1SourceList`), independent of Hive support, never reaching an analyzable plan. `ParquetConnectorSpec`'s test proving rejection and zero committed rows. |
| `.writeTo(...).create()`, new table | ✅ Covered, with a caveat | With the format made explicit (`.using("parquet")`), reuses the exact same `CreateDataSourceTableAsSelectCommand` path `.saveAsTable()` already uses — confirmed via `injectCheckRule`, no new plan shape. `ParquetConnectorSpec`'s translation test. **Caveat, not a gap**: without `.using(...)`, `.create()` defaults toward Hive-table creation and fails with `NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT` — a genuine, pre-existing Spark limitation of the non-Hive default session catalog, confirmed via a real probe, unrelated to Invariant. |
| `.writeTo(...).createOrReplace()`/`.replace()` | 🚫 **N/A — rejected by Spark itself, not an Invariant gap** | Confirmed empirically, for both new and existing tables, format made explicit or not: `AnalysisException: ... does not support REPLACE TABLE AS SELECT`. The default `spark_catalog` is not a `StagingTableCatalog` for V1-provider-backed tables the way Delta's/Iceberg's own catalogs are — never reaches an analyzable plan. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | 🚫 **N/A — not a Parquet capability at all** | Confirmed empirically: Spark itself rejects all three against a plain Parquet table (`MERGE INTO TABLE is not supported temporarily`/`UPDATE TABLE is not supported temporarily`/`AnalysisException` for `DELETE`) — genuinely different from Delta/Iceberg's 🚫 rows elsewhere in this document, which are real, connector-supported operations Invariant hasn't translated yet. Plain Parquet has no row-level-DML capability whatsoever in vanilla Spark; there is nothing to translate. `ParquetConnectorSpec`'s regression test proving rejection and an unchanged row count. |
| Streaming write | ✅ **Covered — real bug found and fixed this pass** | See above. `WriteToStream`'s existing case, generic since Delta — but its reflective path lookup (tier 3) only tried a public `path()` method, never finding `FileStreamSink`'s private `path` field, so every plain `FileFormat`-based streaming write fell to a useless `toString`-based location. Fixed in `WriteCommandSupport.reflectiveSinkPath`/`streamSinkFormatOf`. `ParquetConnectorSpec`'s direct-construction translation test plus its PASS/FAIL enforcement pair. |
| Maintenance operations that touch data | 🚫 **N/A — no such mechanism exists** | Plain Parquet has no connector-level maintenance command (no `OPTIMIZE`/`VACUUM`/compaction SQL) — any compaction a user performs is just an ordinary read-and-overwrite job, already covered by the `.save(path)` row above. Nothing to classify. |

## Parquet feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| Nullability on read-back | ✅ **Confirmed — and reattributed** | Every field reports `nullable = true` after a Parquet write+read round-trip, regardless of the original schema's nullability — confirmed directly against plain Parquet, no Delta/Iceberg involved. This is the *same* behavior this document previously described only under "Delta Lake reads" (and separately, independently, under Iceberg's own ledger) — now confirmed to be inherited from Parquet's own reader, not something either connector does itself, since both store data as Parquet under the hood. `ParquetConnectorSpec`'s two permanent tests: one confirming the behavior directly (schema comparison pre/post write), one proving a `required: true` field sourced from a Parquet read is correctly rejected as `OUTPUT_FIELD_NULLABILITY_MISMATCH` (the practical contract-authoring consequence). |
| Schema merging (`mergeSchema=true` across heterogeneous files) | ✅ Confirmed transparent | The analyzed plan's schema genuinely includes the merged (union) column set — confirmed via a real contract that would `MISSING_OUTPUT_FIELD`-reject if the extra column weren't present. No `WriteCommandSupport`/`StructuralVerifier` change needed. `ParquetConnectorSpec`'s PASS test. |
| Partition column discovery (`partitionBy`) | ✅ Confirmed transparent | Partition columns are genuinely present, with the correct name and type, in both the read-back schema and the write's own verified schema — no "generated columns"-style gap the way Delta's onboarding found, since (unlike a Delta generated column) a partition column is always part of `query.output` for a `FileFormat` write: Spark needs its actual value to route each row to the right directory. `ParquetConnectorSpec`'s PASS test. |
| Corrupt/malformed file in the read path | ✅ Confirmed orthogonal | A directory containing a corrupt file fails entirely within Spark's own Parquet-reading machinery (schema/footer resolution, confirmed to occur at variable points depending on file layout — sometimes during DataFrame construction, sometimes only once a job runs) — never producing an analyzable write-command plan for `ContractEnforcementRule` to see, and never committing output. No fix needed; nothing for Invariant to check here differently. `ParquetConnectorSpec`'s test. |
| Legacy timestamp/date rebase mode (`spark.sql.parquet.datetimeRebaseModeInRead`/`...Write`, `int96RebaseModeIn{Read,Write}`) | ✅ **Confirmed — closed as a follow-up** | Real probe (since deleted), then permanent tests: a pre-Gregorian-calendar date/timestamp written under `LEGACY` and read back under `CORRECTED` round-trips with its `DateType`/`TimestampType` schema completely unchanged — rebase mode only ever affects the encoded *value*, never a field's declared type or nullability. A write sourced from such a read, under a contract declaring `date`/`timestamp` types, passes cleanly. Also confirmed the strictest setting (`EXCEPTION`) never blocks *analysis* — schema resolution succeeds regardless of rebase mode; `EXCEPTION` exists to guard genuinely ambiguous files (no rebase metadata tag, i.e. written by pre-2.4.6 Spark), which a modern-Spark-written file never triggers. `ParquetConnectorSpec`'s two new tests. |
| Writer-side storage optimizations (bloom filters, dictionary encoding, column statistics) | ✅ **Confirmed — closed as a follow-up** | Real probe (since deleted), then a permanent test: `parquet.bloom.filter.enabled#<col>`, `parquet.enable.dictionary`, and `parquet.enable.summary-metadata` writer options have zero effect on the analyzed column set, and disabling dictionary encoding specifically doesn't affect `streamSinkFormatOf`/`formatOf`'s format detection either — these are physical storage-layer decisions Parquet's reader resolves entirely below the `LogicalPlan` level this adapter translates at, the same reasoning already established for Delta's/Iceberg's own storage-layer rows. `ParquetConnectorSpec`'s new test. |

Net assessment: Parquet's operation surface is fully enumerated, not
partially covered — every row above is either ✅ Covered or a genuinely
reasoned 🚫 N/A (a real Spark-level constraint, not "Invariant hasn't
gotten to this yet" the way a Delta/Iceberg 🚫 row means), the only
connector so far where the *whole* row-level-DML and DataSourceV2-catalog-write
axes are architecturally inapplicable rather than future work. Two real,
previously-unknown findings came out of what looked like a purely
confirmatory pass: a genuine streaming-write location bug (fixed, and
fixed connector-agnostically — every `FileFormat`-based streaming sink
benefits, not just Parquet's), and a documented-but-not-fixed nested-write
trap for a path-less new-table `.saveAsTable()`/`.writeTo(...).create()`,
flagged as real future work rather than left implicit. Both feature-surface
rows left ❓ after the initial pass (rebase mode, storage optimizations)
were closed in a same-day follow-up — real probes, not assumed transparent
by analogy to Delta/Iceberg's own storage-layer rows, each converted to a
permanent test before its probe was deleted. Every row of both ledgers now
has a real disposition: no ❓ remaining in either.

