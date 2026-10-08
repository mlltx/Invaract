# Engine research: BigQuery and Apache Beam

The BigQuery and Beam rows in `docs/MULTI_ENGINE_ADAPTERS.md` (write modes, types) were written from memory
of public documentation and marked unverified. This document replaces memory with primary sources, corrects
those tables where they were wrong, and settles the three questions an adapter's author has to answer before
writing code: **where can the engine be gated**, **what can be known about a job at that point**, and **how does
CI get real-engine evidence without secrets**. It is research, not a design for either adapter; no adapter code
exists, and neither adapter starts without a go-ahead.

## How this was checked, and what could not be

Read directly from source, on the day of writing (`master` of each repository, so a later reader should
re-check anything that matters, especially anything marked *Beam* or *BigQuery API* below):

| Source | What it settles |
|---|---|
| `apache/beam` `sdks/java/core/.../schemas/Schema.java` | the Beam schema type system (`Schema.TypeName`) |
| `apache/beam` `sdks/java/io/google-cloud-platform/.../bigquery/BigQueryUtils.java` | Beam's own mapping to and from BigQuery types |
| `apache/beam` `.../bigquery/BigQueryIO.java` | `WriteDisposition`, what a `BigQueryIO` transform exposes about itself |
| `apache/beam` `sdks/java/core/.../Pipeline.java`, `PipelineRunner.java`, `values/PCollection.java` | how a pipeline is traversed, how a runner is chosen, schemas, boundedness |
| `apache/beam` `.../BigQueryIOWriteTest.java` | how Beam itself tests BigQuery writes without a service |
| `googleapis/googleapis` `google/cloud/bigquery/v2/{standard_sql,job_config,job_stats,table_schema}.proto` | BigQuery's type kinds, dispositions, dry run, what a dry run reports |

**Not checked.** The environment's egress policy blocks `beam.apache.org` and the Google Cloud documentation
hosts, so nothing below rests on those pages. Where a statement comes from general knowledge and not from the
sources above it is marked **unverified**; those are the ones to confirm first when an adapter is started, ideally
against a real project.

## Findings that correct the existing tables

### Write modes

| BigQuery (query job) | Beam `BigQueryIO.Write` | Canonical | Notes |
|---|---|---|---|
| `WRITE_TRUNCATE` | `WRITE_TRUNCATE` | `overwrite` | BigQuery: the table's schema becomes the query result's, constraints are removed. Beam: **only supported with the `FILE_LOADS` write method** |
| `WRITE_TRUNCATE_DATA` | (none) | `overwrite` | **Missing from the old table.** Replaces the data but keeps the existing table's schema and constraints, so the schema to verify is the *table's*, not the query's |
| `WRITE_APPEND` | `WRITE_APPEND` | `append` | |
| `WRITE_EMPTY` | `WRITE_EMPTY` | `error` (approximate) | Fails only if the table *contains data*; an existing empty table succeeds, which `ErrorIfExists` would not. Beam's check "may occur long before data is written, and does not guarantee exclusive access" |

The **default differs by job kind** in the BigQuery API: a query job defaults to `WRITE_EMPTY`, a load job to
`WRITE_APPEND` (`job_config.proto`). An adapter that reads an unset disposition must apply the default of the
job kind it is looking at, not one global default, or it will report `error` for loads and `append` for queries.
Beam's `BigQueryIO.Write` defaults to `WRITE_EMPTY`.

The canonical vocabulary is unchanged (`append`, `overwrite`, `ignore`, `error`); the approximation on
`WRITE_EMPTY` is the thing to document in each adapter's capability note, not a reason for a fifth name.

### Types

| Neutral type | BigQuery | Beam schema | Correction or confirmation |
|---|---|---|---|
| `string`, `boolean`, `long`, `double` | `STRING`, `BOOL`, `INT64`, `FLOAT64` | `STRING`, `BOOLEAN`, `INT64`, `DOUBLE` | Confirmed. A BigQuery column schema also spells these `INTEGER`, `FLOAT`, `BOOLEAN` (legacy names), and a nested one `RECORD`: normalise on read |
| `byte`, `short`, `integer`, `float` | none (`INT64` only, `FLOAT64` only) | `BYTE`, `INT16`, `INT32`, `FLOAT` | **New.** Beam has the narrow types; BigQuery has none. A Beam `INT32` output is `integer`, which a contract declaring `long` will correctly not match |
| `decimal(p,s)` | `NUMERIC` default `(38,9)`; declared `NUMERIC(p,s)` needs `1 ≤ p-s ≤ 29`, `0 ≤ s ≤ 9`. `BIGNUMERIC(p,s)` needs `1 ≤ p-s ≤ 38`, `0 ≤ s ≤ 38` | `DECIMAL`, **no precision or scale in the type** | **Refined.** A parameterised `NUMERIC(p,s)` is `decimal(p,s)`; a `BIGNUMERIC(p,s)` is `decimal(p,s)` only when `p ≤ 38`, otherwise (and unparameterised, which is `(76.76, 38)`) `OtherType("BIGNUMERIC", ...)`. Beam can only say `OtherType("DECIMAL", ...)`: its own BigQuery reader maps both `NUMERIC` and `BIGNUMERIC` to the same `DECIMAL` |
| `date` | `DATE` | logical type `SqlTypes.DATE` | Confirmed |
| `time` | `TIME` | logical type `SqlTypes.TIME` | Confirmed |
| `timestamp_ntz` | `DATETIME` | logical type `SqlTypes.DATETIME` | **Resolved** (was "decide per type"): the local date-time logical type |
| `timestamp` | `TIMESTAMP` | `DATETIME` (the primitive; a Joda instant) and `SqlTypes.TIMESTAMP` | **Resolved**: Beam's primitive `DATETIME` is an instant, so it is `timestamp`, not `timestamp_ntz`. Beam itself maps it to BigQuery `TIMESTAMP` |
| `timestamp` (picoseconds) | `TIMESTAMP` with `timestamp_precision = 12` (6 is the default) | `Timestamp.MILLIS`/`MICROS`/`NANOS` logical types, or `STRING` for picos, by a reader option | **New.** A 12-digit BigQuery timestamp is not the neutral microsecond `timestamp`: report `OtherType` |
| `json` | `JSON` | none: Beam's own reader maps `JSON` and `GEOGRAPHY` to `STRING` | **Confirmed and sharpened.** On Beam a JSON or geography column from BigQuery is a `string`, so a contract declaring `json` against a Beam pipeline fails as a type mismatch. That is correct and should be said in the Beam adapter's notes |
| `geography` | `GEOGRAPHY` | none (as above) | as above |
| `interval` | `INTERVAL` | none | Confirmed by absence in `Schema.TypeName` and `BigQueryUtils` |
| `binary` | `BYTES` | `BYTES` | Confirmed |
| `array` | `ARRAY` | `ARRAY`, `ITERABLE` | Confirmed; Beam's `ITERABLE` is an array that need not fit in memory. A BigQuery array of arrays is not allowed (**unverified**), so a nested-array contract on BigQuery is a mismatch by construction |
| `struct` | `STRUCT` (`RECORD` in a table schema) | `ROW` | Confirmed |
| `map` | none | `MAP` | Confirmed: `standard_sql.proto` has no map kind. Beam's BigQuery reader can *infer* a map from a repeated record whose two fields are named `key` and `value` (`inferMaps` option); an adapter must not do this silently |
| no neutral type | `RANGE` | `ITERABLE` aside, custom logical types | Beam's own BigQuery reader throws on `RANGE` ("unsupported"), so a `RANGE` column is `OtherType` on BigQuery and an error on Beam |

Beam's nullability is a property of the field, not the type (`Field.withNullable`), and carries straight to
`LogicalField.nullable`.

## Where each engine can be gated

`EnforcementPoint` has three values (`in-engine-blocking`, `pre-submit-gate`, `observe-only`). Both engines are
**not** `in-engine-blocking`: neither lets an extension abort a write from inside, the way Spark's check rule does.

### Apache Beam: a pre-submit gate, and a real one

- **The graph is available before anything runs.** `Pipeline.traverseTopologically(PipelineVisitor)` walks every
  transform and `PCollection` of a constructed pipeline; `PCollection.isBounded()` says batch or streaming
  (read/write streaming capabilities), and `PCollection.hasSchema()`/`getSchema()` gives the row schema when the
  collection has one.
- **A `BigQueryIO` transform describes itself.** Its `DisplayData` carries `table`, `schema` (as JSON),
  `createDisposition` and `writeDisposition` on the write side, and `table`, `query`, `useLegacySql` on the read
  side (`populateDisplayData` in `BigQueryIO.java`). Display data is a public, stable API, so an adapter need
  not reflect on private fields.
- **It can be attached with no change to the job's source.** `PipelineRunner.fromOptions` instantiates the class
  named by `--runner` through a `fromOptions(PipelineOptions)` factory. A wrapper runner that verifies the
  pipeline and then delegates to the configured real runner is therefore attachable with `--runner=...` alone,
  which is what the External Attachability Requirement asks for. The limit: a job that sets its runner in code
  bypasses it, which the capability note must say. (How the wrapper learns the delegate is a design question:
  a custom option registered through a `PipelineOptionsRegistrar` is the obvious route; **unverified**.)
- **Where it cannot see.** A `PCollection` of plain Java objects with no schema has no output schema to verify:
  that is an *unverifiable* write, and fail-closed, not a pass. A custom `DoFn` is opaque to lineage, the way a
  Spark UDF is. A write method that never goes through `BigQueryIO` (a custom sink) is an operation the adapter
  has no translation for: `UNVERIFIABLE_WRITE`.

Declared enforcement point: `pre-submit-gate`.

### BigQuery: a pre-flight check for clients that opt in, and an observer for the rest

- **There is no hook inside the service.** A statement or load submitted by any client other than the one that
  carries the check is not seen. Writes by the Storage Write API, streaming inserts and loads are different
  paths again. The service cannot be asked to refuse a job on a contract's behalf.
- **What a pre-flight can know without running the job.** A job submitted with `dryRun` returns "a mostly empty
  response with some processing statistics" (`job_config.proto`), and those statistics include
  `referencedTables`, the result `schema`, `statementType` (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`,
  `CREATE_TABLE_AS_SELECT`, `TRUNCATE_TABLE`, `LOAD_DATA`, `EXPORT_DATA`, `CALL`, `SCRIPT`, ...) and
  `ddlTargetTable` (`job_stats.proto`). A `SCRIPT` runs child statements the dry run does not enumerate, and
  `TRUNCATE_TABLE` is BigQuery's own "changes data but is not a write": both are the fail-closed case. The job
  configuration itself carries `destinationTable`, `writeDisposition` and `createDisposition`. That is enough for:
  declared inputs read, output location, output schema (type and nullability), format (always a BigQuery
  table), save mode, catalog registration (always registered), row-level DML recognised as a write, and
  fail-closed on any `statementType` the adapter does not recognise.
- **What it cannot know.** A dry run returns no plan: no filters, joins, groupings, column derivations or
  function calls. Plan-shape rules, column lineage, sensitivity propagation, static data quality, role
  consistency, boundaries and the fingerprint all need the statement analysed. The honest declaration is
  `unsupported` for those (a contract that relies on one is then rejected as `UNSUPPORTED_CONTRACT_FEATURE`, which
  is the point of the declaration), until and unless a SQL analyzer is added as a deliberate piece of work.
  (BigQuery's dialect is GoogleSQL; the open-source ZetaSQL analyzer exists, but whether it is a workable
  dependency here is **unverified** and not assumed.)
- **Zero-code attach exists only for observation**, if at all: reading the service's own job history after the
  fact (**unverified**: the audit-log and `INFORMATION_SCHEMA` routes were not checked). A gate that a platform
  team can switch on for a job it does not own is not available, and the declaration must say so rather than
  claim `config.zeroCodeInstall`.

Declared enforcement point: `pre-submit-gate` for a client that calls the check, with the bypass stated; the
observation route, if built, is `observe-only` and a different adapter mode.

## How CI gets real-engine evidence

The conformance kit judges an adapter by running neutral jobs *on the real engine* through the real
enforcement path. Each engine needs an answer to "what runs in CI, with no credentials".

- **Beam: a real, hermetic runner.** `DirectRunner` (`runners/direct-java`) runs a pipeline in-process, so a
  `ConformanceAdapter` can build each neutral job as a real Beam pipeline, run it through the wrapper, and judge
  the outcome, exactly as Spark's does on `local[*]`. File-based transforms (`TextIO`, `ParquetIO`) cover the
  parts that need no service. For `BigQueryIO`, Beam's own write tests (`BigQueryIOWriteTest`) run against
  in-repo fakes (`FakeBigQueryServices`, `FakeDatasetService`, `FakeJobService`) under `TestPipeline`; whether
  those fakes are consumable from a published artifact, or would have to be re-created, is **unverified** and
  decides how far the `BigQueryIO` scenarios can go without a service.
- **BigQuery: no local engine was found.** No official emulator was identified. The options are (a) a recorded
  fixture layer: dry-run responses and table schemas captured once from a real project and replayed, which
  proves the adapter's *reading* of the real response shape (the shapes are defined in the protos above) but not
  that the service still returns them; (b) a credentialed job that runs the same scenarios against a real
  project, on a schedule and not on every pull request, kept outside the required checks because it needs
  secrets and costs money. The honest combination is (a) as the required check and (b) as the periodic proof,
  with the capability page saying which of the two backs each "verified".

## What this means for the order of work

1. **Beam first.** It has a real in-process runner, a graph visible before submission, schemas, boundedness and
   a zero-code attach point. Every capability has a way to be verified in CI, so it is the adapter that can
   exercise the whole kit and the skill's process.
2. **BigQuery second, as a deliberately partial adapter.** Structural checks only, fail-closed everywhere else,
   with the fixture layer as its required CI evidence. That is a feature of the kit (an adapter declares what it
   cannot do and is held to it), not a shortfall.
3. **Draft declarations** (to be confirmed by the adapter's own work, not taken as given):

| Capability | Beam | BigQuery |
|---|---|---|
| `read.batch` / `write.batch` | supported (`BigQueryIO`, file IO) | supported (query, load) |
| `read.streaming` / `write.streaming` | supported (`isBounded`) | not-applicable (a job is bounded) |
| `write.rowLevelDml` | not-applicable | partial (`statementType` `UPDATE`/`DELETE`/`MERGE`; the rule inputs need the statement) |
| `check.location`, `check.schema`, `check.format`, `check.saveMode`, `check.catalogRegistration`, `check.inputExistence` | supported | supported |
| `check.nestedTypes` | supported (`ROW`/`ARRAY`/`MAP`) | supported (`STRUCT`/`ARRAY`) |
| `rules.planShape`, `rules.dml` | partial (transform graph, not SQL) | unsupported |
| `analysis.fingerprint`, `analysis.functionCatalog`, lineage, sensitivity, static data quality, role consistency | partial: opaque `DoFn`s, SQL transforms | unsupported (no plan) |
| `config.zeroCodeInstall` | partial (`--runner`; a job that sets its runner in code bypasses it) | unsupported |
| `failClosed.unverifiableWrites` | supported (a sink that is not a recognised write) | supported (an unrecognised `statementType`) |
| enforcement point | `pre-submit-gate` | `pre-submit-gate`, caller-cooperative |

## Still to confirm against the engines

Everything marked **unverified** above, plus: Beam's `PipelineOptionsRegistrar` route for a wrapper runner's
delegate option; whether Beam's BigQuery fakes ship in a consumable artifact; the exact `DisplayData` keys on the
Beam read/write transforms in the Beam version the adapter will target (the file is `master`); BigQuery's
array-of-arrays rule; and the audit-log or `INFORMATION_SCHEMA` route for observation. Each is a small, concrete
check that belongs at the start of the adapter that needs it, recorded in that adapter's friction log.
