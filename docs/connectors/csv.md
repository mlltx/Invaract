# CSV support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Fourth connector onboarded via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md). Like Parquet, CSV is not a separate
connector library at all: `org.apache.spark.sql.execution.datasources.csv.CSVFileFormat`
is Spark's own built-in `FileFormat`, already on this module's `provided`
`spark-sql` dependency — nothing added to `build.sbt`, not even as a
`% "test"` dependency. It shares Parquet's exact operation-surface
architecture (default `spark_catalog`, no catalog of its own, no
row-level DML, no versioning, no SQL-extension `Command` classes, the
same `FileStreamSink` streaming-sink class), so this pass empirically
confirmed — not assumed by analogy — that every operation-surface row
routes through the exact same generic mechanisms Parquet's pass already
proved out (`InsertIntoHadoopFsRelationCommand`,
`CreateDataSourceTableAsSelectCommand`, `WriteToStream`/`FileStreamSink`,
including the private-field-reflection fix from Parquet's pass — a real
direct-construction test confirms it generalizes to `CSVFileFormat`
specifically, not just assumed connector-agnostic from its own doc
comment). **No bugs found, no new structural patterns, zero
`src/main/scala` changes** — this pass is a pure confirmatory audit of
the operation surface plus real, substantive new ground on the feature
surface, since CSV — unlike Parquet — is a plain text format with no
native schema.

**What's genuinely new for CSV: the feature surface, not the operation
surface.** CSV has real, CSV-specific behaviors with no Parquet analog,
each confirmed with a real probe and codified into a permanent test:

- **Schema inference default.** Without `inferSchema=true` (or an
  explicit schema), every column reads back as `StringType` — confirmed
  directly, and confirmed to have the correct practical consequence: a
  contract declaring a numeric type for a field sourced from a plain
  (no-inferSchema) CSV read is correctly rejected as
  `OUTPUT_FIELD_TYPE_MISMATCH`, not silently passed. With
  `inferSchema=true`, real types (`IntegerType` for small whole numbers,
  not `LongType` — a real gotcha this pass's own tests initially got
  wrong, see below) are resolved and satisfy a contract declaring them.
- **Header handling.** `header=true`/`header=false` is transparent:
  without a header, CSV falls back to positional `_c0`/`_c1`/... column
  names, which are just ordinary column names as far as translation and
  verification are concerned — a contract can declare and satisfy them
  like any other field name.
- **Malformed-record modes.** `FAILFAST` confirmed to fail only at
  execution (task/job failure reading the bad record), never at
  analysis — the same "orthogonal to Invariant's structural check"
  pattern already confirmed for Parquet's corrupt-file case.
  `DROPMALFORMED` confirmed to silently exclude bad rows from what's
  written, with the analyzed schema unaffected, so a contract is
  satisfied against the remaining good rows.
- **`columnNameOfCorruptRecord`.** Confirmed to be transparent: declaring
  it just adds an ordinary extra `StringType` column to the analyzed
  schema — Invariant sees it like any other field, no special handling
  needed, and a contract that declares it is satisfied normally.
- **Nullability on read-back.** Every field reports `nullable = true`
  after a CSV write+read round-trip, regardless of the original schema's
  nullability — confirmed independently for CSV specifically (not
  inherited from Parquet's reader the way Delta's/Iceberg's own version
  of this finding was, since CSV is a genuinely separate text-based
  reader, not built on Parquet under the hood). Same practical
  consequence for contract authors as Parquet's/Delta's/Iceberg's own
  version of this finding.
- **Date/timestamp parsing with a custom `dateFormat`.** Confirmed
  transparent under `PERMISSIVE` mode: an unparseable date becomes
  `null` in the row, not a thrown exception or an analysis-time failure —
  the schema stays `DateType` either way, and a contract declaring it is
  satisfied.

**A real test-writing bug, caught and fixed before landing, not a product
bug.** The first draft of this pass's two streaming tests declared
`type: long` in their contracts for fields sourced from an
`inferSchema=true` CSV read — but CSV's schema inference resolves small
whole numbers to `IntegerType`, not `LongType` (confirmed directly by
the dedicated inferSchema test, which correctly declares `type:
integer`). Both streaming tests failed on first run with a real
`ContractViolationException` (`INPUT_FIELD_TYPE_MISMATCH`/
`OUTPUT_FIELD_TYPE_MISMATCH`, expected `integer` got `long`) — correctly
caught by the engine doing its job, not a false pass. Fixed by declaring
`type: integer` in both tests' contracts, matching what CSV's real
inferred schema actually produces.

## CSV operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("csv").load(path)`/`.csv(path)` | ✅ Covered | Same generic `LogicalRelation`+`HadoopFsRelation` handling as Parquet, confirmed for CSV specifically. `SparkPlanAdapterSpec`'s existing CSV read-format test; `CsvConnectorSpec`'s tests throughout exercise real CSV reads. |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM t`) | ✅ Covered | Same `LogicalRelation` shape via the default `spark_catalog`, confirmed via `injectCheckRule` probe and `CsvConnectorSpec`'s `.insertInto()` test (which reads the target table back through the catalog). |
| Time travel / snapshot reads | 🚫 **N/A, not a real gap** | Plain CSV (`USING csv`, no table-format layer) has no versioning concept at all — the same architectural constraint as Parquet. Nothing to translate. |
| Streaming read (`readStream`) | ✅ Covered | Generic `StreamingRelation`/`FileSource` handling, confirmed empirically for CSV. `CsvConnectorSpec`'s "PASS: a streaming CSV source satisfies a contract's declared input schema." |
| Change-data-feed / incremental read | 🚫 **N/A, not a real gap** | No CDC mechanism exists for plain CSV — a table-format-only feature, the same constraint as Parquet. Nothing to translate. |
| `.save(path)`, all four save modes | ✅ Covered | `InsertIntoHadoopFsRelationCommand`, confirmed for CSV via real probe (all four modes: `append`/`overwrite`/`ignore`/`error`) during Phase 3's operation-surface probe. |
| `.saveAsTable(...)`, new table | ✅ Covered | `CreateDataSourceTableAsSelectCommand`, `table.provider = "csv"`. Confirmed via probe; exercised throughout `CsvConnectorSpec`'s setup code. |
| `.saveAsTable(...)`, existing table (append) | ✅ Covered | Same nested-double-write pattern Parquet's pass documented (`CreateDataSourceTableAsSelectCommand` delegating to a nested `InsertIntoHadoopFsRelationCommand`), confirmed independently for CSV via a real PASS/FAIL pair — not assumed to generalize from Parquet. `CsvConnectorSpec`'s PASS/FAIL pair (the FAIL half also asserts the nested insert never ran). |
| `.insertInto(...)` | ✅ Covered | Same `InsertIntoHadoopFsRelationCommand` shape. `CsvConnectorSpec`'s translation test via `SparkAdapterListener`. |
| `.writeTo(...)` (DataFrameWriterV2) — `.append()`/`.overwrite(cond)` against an *existing* table | 🚫 **N/A — rejected by Spark itself, not an Invariant gap** | Confirmed empirically for CSV specifically: `AnalysisException: Cannot write into v1 table` — the same V1/V2 architectural constraint as Parquet (plain CSV never implements the V2 `SupportsWrite` capability under the default `useV1SourceList`). `CsvConnectorSpec`'s test proving rejection and zero committed rows. |
| `.writeTo(...).create()`, new table | ✅ Covered | With the format made explicit (`.using("csv")`), reuses the exact same `CreateDataSourceTableAsSelectCommand` path `.saveAsTable()` already uses — confirmed via `injectCheckRule`, no new plan shape. `CsvConnectorSpec`'s translation test. |
| `.writeTo(...).createOrReplace()`/`.replace()` | 🚫 **N/A — rejected by Spark itself, not an Invariant gap** | Confirmed empirically for CSV, for both new and existing tables, format made explicit: `AnalysisException: ... does not support REPLACE TABLE AS SELECT` — the default `spark_catalog` is not a `StagingTableCatalog` for V1-provider-backed tables. Unlike Parquet's pass (which only confirmed this via probe), `CsvConnectorSpec` has a dedicated permanent test for this row. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | 🚫 **N/A — not a CSV capability at all** | Confirmed empirically: Spark itself rejects all three against a plain CSV table, the same messages as Parquet's (`MERGE INTO TABLE is not supported temporarily`/`UPDATE TABLE is not supported temporarily`/`AnalysisException` for `DELETE`). `CsvConnectorSpec`'s regression test proving rejection and an unchanged row count. |
| Streaming write | ✅ Covered | `WriteToStream`/`FileStreamSink`, using the exact fix Parquet's pass made to `WriteCommandSupport.reflectiveSinkPath`/`streamSinkFormatOf` — confirmed to generalize to CSV specifically via a direct-construction test (`FileStreamSink` built with a real `CSVFileFormat`, translated synchronously), not assumed from the fix's own "connector-agnostic" doc comment. `CsvConnectorSpec`'s direct-construction test plus its streaming PASS/FAIL pair. |
| Maintenance operations that touch data | 🚫 **N/A — no such mechanism exists** | Plain CSV has no connector-level maintenance command, the same constraint as Parquet. Nothing to classify. |

## CSV feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| Schema inference default (no `inferSchema`, no explicit schema) | ✅ Confirmed — genuinely CSV-specific | Every column defaults to `StringType` when `inferSchema` is left off — a real gotcha with no Parquet analog (Parquet is self-describing). Confirmed a contract declaring a numeric type against such a read is correctly rejected as `OUTPUT_FIELD_TYPE_MISMATCH`. `CsvConnectorSpec`'s two tests (StringType default + rejection; `inferSchema=true` resolving real types and satisfying a typed contract). |
| Header handling (`header=true`/`false`) | ✅ Confirmed transparent | Without a header, CSV falls back to positional `_c0`/`_c1`/... names — ordinary column names as far as translation/verification are concerned, no special handling needed. `CsvConnectorSpec`'s test. |
| Malformed-record modes (`PERMISSIVE`/`DROPMALFORMED`/`FAILFAST`) | ✅ Confirmed orthogonal | `FAILFAST` fails only at execution, never at analysis — the same pattern as Parquet's corrupt-file case. `DROPMALFORMED` silently excludes bad rows; the analyzed schema is unaffected, and a contract is satisfied against the remaining rows. `CsvConnectorSpec`'s two tests. |
| `columnNameOfCorruptRecord` | ✅ Confirmed transparent | An ordinary extra `StringType` column in the analyzed schema — no special handling needed, a contract declaring it is satisfied normally. `CsvConnectorSpec`'s test. |
| Nullability on read-back | ✅ Confirmed — independently, not inherited from Parquet | Every field reports `nullable = true` after a CSV write+read round-trip regardless of declared nullability — confirmed directly for CSV's own (non-Parquet-based) reader, the same practical consequence as Parquet's/Delta's/Iceberg's own version of this finding. `CsvConnectorSpec`'s test. |
| Date/timestamp parsing with a custom `dateFormat` | ✅ Confirmed transparent | An unparseable date under `PERMISSIVE` mode becomes `null` in the row, not a thrown exception or analysis-time failure — schema stays `DateType`, contract satisfied. `CsvConnectorSpec`'s test. |

Net assessment: like Parquet, CSV's operation surface is fully enumerated
with no ❓ rows — every row is either ✅ Covered (via mechanisms Parquet's
pass already proved out and this pass independently confirmed generalize
to CSV) or a genuinely reasoned 🚫 N/A, the second connector where the
whole row-level-DML and DataSourceV2-catalog-write axes are
architecturally inapplicable rather than future work. No bugs found and
no `src/main/scala` changes were needed — CLAUDE.md's mutation-testing
requirement (scoped to changed/added `src/main/scala` files) doesn't
apply to this pass. The real substance was on the feature surface: six
CSV-specific behaviors (schema-inference default, header handling,
malformed-record modes, the corrupt-record column, nullability, and
date parsing), each confirmed with a real probe and codified into a
permanent test, plus one `createOrReplace()`/`.replace()` row given a
dedicated permanent test where Parquet's own pass had only probed it.
`CsvConnectorSpec`: 19 tests.

