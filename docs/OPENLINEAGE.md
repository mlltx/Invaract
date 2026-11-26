# OpenLineage: Design

Invaract publishes what it verified as [OpenLineage](https://openlineage.io) run events, so the lineage tools a data
platform already runs (Marquez, DataHub, Atlan, Collibra, OpenMetadata, ...) show, for every run of every job, which
datasets it read and wrote, their columns, how each output column was derived, and whether the write passed its contract.
MISSION.md §5: OpenLineage stays the ecosystem's interoperability layer for lineage; Invaract adds the verification that
gives those relationships confidence. This document is the design and the record of how it was built.

## 1. Goals and non-goals

Goals:

1. **One implementation, every engine.** The mapping, the delivery and the settings live in `verification-core`; an adapter
   gets OpenLineage by doing what it already does (calling `VerificationPipeline` and `AttachSetup`). Spark, Beam (Java and
   Python) and plain Python jobs publish it from the first commit, and an adapter added later gets it by default.
2. **Attached by configuration alone** (CLAUDE.md's External Attachability Requirement): neutral settings in
   `InvaractConf`, spelled by each engine its own way (`spark.invaract.openLineageUrl`, `--invaractOpenLineageUrl`,
   `--invaract_open_lineage_url`, `INVARACT_OPEN_LINEAGE_URL`).
3. **Checked by the conformance kit** on every adapter: a new capability, `reporting.openLineage`, which every adapter
   declares, and scenarios that hold each adapter's events to the OpenLineage specification and to what the adapter
   declares (column lineage only from an adapter that declares it).
4. **Cheap and safe to turn on.** Nothing on the job's own thread but handing a reference to a bounded queue; mapping,
   serialization and delivery happen on one background thread. A slow, failing or unreachable endpoint never fails,
   blocks or slows the job, and memory stays bounded however long the endpoint is down.
5. **Correct to the specification**, held to it by tests: every event the tests produce is validated against the official
   OpenLineage JSON Schema (pinned, vendored), dataset names follow the OpenLineage naming conventions, and Invaract's own
   facets have published schemas.
6. **Honest.** A fact Invaract does not know is left out, never guessed: no column lineage from a plan Invaract cannot see
   into (a Python job's own code, a Beam step's function), no completion where the adapter saw none.

Non-goals (this version):

- Replacing an engine's own OpenLineage integration (`openlineage-spark`). The two can run side by side; section 9 says how
  they relate.
- A Kafka transport. HTTP covers Marquez and every hosted backend; a file transport covers a log-shipping pipeline.
  Kafka can follow as an opt-in module the way `notification-kafka` did.
- Consuming OpenLineage (reading other tools' lineage to verify against).

## 2. What is published

### 2.1 Runs

One OpenLineage **run per Invaract run**: a Spark application, a Beam pipeline submission, a Python job's run. The
specification requires each run to have one `START` and one terminal event (`COMPLETE`, `FAIL` or `ABORT`):

| When | OpenLineage event | Carries |
|---|---|---|
| The run's first Invaract event | `START` | job and run facets |
| Each contract check of a write | `RUNNING` | that write's inputs and output, with their facets; on a refused write, the `errorMessage` run facet |
| Each completed write (Spark, Python) | `RUNNING` | the output, with `outputStatistics`, `datasetVersion`, `lifecycleStateChange` |
| The run ends | `COMPLETE`, or `FAIL` when a write was refused | the run's totals (`invaract_run` facet) |

`RUNNING` events let a backend show a long Spark application's lineage while it runs, and a `RUNNING` event is never
needed for correctness: OpenLineage backends merge the inputs and outputs of every event of a run. For fewer requests,
`emission=endOfRun` sends only `START` and the terminal event, the terminal one carrying every dataset seen.

The run ends when the adapter says so - `NotificationSink.endRun(job)`, a neutral signal every wrapping sink passes on and
every other sink ignores (a `JobSummaryEvent` or `DryRunSummaryEvent` ends it too) - or, failing that, when the process
ends (a JVM shutdown hook, bounded by `shutdownTimeoutMs`). A run that saw a refused write ends `FAIL`; otherwise
`COMPLETE`. Where each adapter ends it:

| Adapter | Ends the run |
|---|---|
| Spark | at application end: `DeclaredOutputs`, already the end-of-application hook for `check.outputsWritten`, calls `endRun` after any `MISSING_OUTPUT` it reports (and for a job discarded unjudged) |
| Beam Java | `InvaractRunner.run`, in a `finally`: the pipeline was refused, or checked and handed to its runner (for a blocking runner, finished) |
| Beam Python | `PortableCheck.run`, in a `finally`: the checker's verdict is the end of Invaract's part of the run |
| Python I/O | the run's end (`finish`/`published`, or the checker's `close` at end of input), after the `JobSummaryEvent` |

The kit holds every adapter to it (section 7): a run with no terminal event within 30 seconds of the job returning is a
divergence. A process killed outright (`SIGKILL`, `os._exit`) runs no hook: its run has no
terminal event, which OpenLineage backends show as still running - the honest picture of a run nobody saw end.

**Run identity.** OpenLineage requires a UUID. The run's id is a name-based UUID (version 5) of the engine and Invaract's
own run id (`JobInfo.runId`: Spark's application id, Beam's job id, the Python run's id), so every event of a run, from
every process that publishes for it, carries the same id, and the same run published twice is the same run.
Once a run has ended, a completed write reported for it afterwards (Spark reports one from a listener thread, which can
trail the end) is dropped rather than reopening the run, which OpenLineage's one-terminal-event rule forbids. A new check
under the same Invaract run id - a process whose engine gives its runs no id of their own runs one after another under the
same key - is a new run: its id is the UUID of the key and its generation (`<runId>#1`, `#2`, ...), still deterministic.

**Job identity.** `namespace` (setting `openLineageNamespace`, else the configuration file's `namespace`, else the
`OPENLINEAGE_NAMESPACE` environment variable, else `default`, OpenLineage's own default) and `name`: the job's stable id
(`jobId`), else the engine's name for the job, else the contract's id. A configured `jobName` wins over all three.

**Parent run.** When the job runs inside an orchestrator that publishes OpenLineage itself (Airflow, Dagster), the
`parent` run facet links the two: from the configuration file (`parent.runId`, `parent.job.namespace`,
`parent.job.name`) or from the `OPENLINEAGE_PARENT_ID` environment variable (`<namespace>/<name>/<runId>`, the form
Airflow's integration sets).

### 2.2 Datasets and their names

Dataset names follow the [OpenLineage naming conventions](https://openlineage.io/docs/spec/naming), so the same dataset
published by Invaract, by Spark's integration and by a warehouse's own integration is one node in the graph:

| Location | namespace | name |
|---|---|---|
| `/data/out/report`, `file:/data/out/report` | `file` | `/data/out/report` |
| a relative path (`data/out/report`) | `file` | the absolute path, resolved against the working directory |
| `C:\data\out`, `file:/C:/data/out` (Windows) | `file` | `C:/data/out`: forward slashes, the drive kept |
| `hdfs://nn:8020/warehouse/t` | `hdfs://nn:8020` | `/warehouse/t` |
| `s3://bucket/key`, `s3a://`, `s3n://` | `s3://bucket` | `key` |
| `gs://bucket/key` | `gs://bucket` | `key` |
| `abfss://container@account.dfs.core.windows.net/path` | the same, up to the host | `/path` |
| `postgresql://host:5432/db` and a table | `postgres://host:5432` | `db.schema.table` |
| an opaque JDBC URL (`h2:mem:bank`, `oracle:thin:@db:1521:orcl`) and a table | the URL, credentials stripped (`h2:mem:bank`) | the table, as openlineage-spark names it |
| `bigquery:project.dataset.table` (Beam) | `bigquery` | `project.dataset.table` |
| any other `scheme://authority/path` | `scheme://authority` | `/path` |

Credentials in a location (`s3://key:secret@bucket`, `postgresql://user:pass@host`) are always removed. A dataset that
resolved to a catalog table also carries the `symlinks` facet naming the table (`TABLE`), and `catalog` when the catalog
is known.

### 2.3 Facets

Standard facets (OpenLineage 2-0-2, the versions pinned in `verification-core/src/test/resources/openlineage/`):

| Facet | On | From |
|---|---|---|
| `schema` | inputs, outputs | the schemas the adapter reports (`CheckedWrite.inputSchemas`, `.outputSchema`; `WriteEvent.schema`), nested fields for structs |
| `columnLineage` | outputs | `ir.Lineage.traceDetailed` over the write's plan (section 11), each source column resolved to its read (`PlanFacts.locationResolver`). `DIRECT` edges per output column (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`, `masking` for a hash, `mask` or a count); `INDIRECT` edges per output column (`CONDITIONAL`: a `CASE WHEN`'s tests; `WINDOW`: a window's partitioning and ordering) and for the whole output (`FILTER`, `JOIN`, `GROUP_BY`, `SORT`), every one resolved through the plan to the read columns it comes from. A read's reported columns bound what it can be a source of, and only the columns the write has get lineage |
| `dataQualityAssertions` | outputs | the contract check: one assertion for the contract as a whole, one per violation (its type, column, expected, actual), and the static data-quality results |
| `outputStatistics` | outputs of completed writes | `WriteEvent`'s rows, bytes and files, where the engine reports them |
| `datasetVersion` | outputs of completed writes | the Delta version or Iceberg snapshot the write committed |
| `lifecycleStateChange` | outputs | `OVERWRITE` for an overwrite, `CREATE` for a write that creates its table |
| `storage` | inputs, outputs | the format (`parquet`, `delta`, ...) and the storage layer for table formats |
| `symlinks`, `catalog` | inputs, outputs | the catalog table a dataset resolved to |
| `processing_engine` | run | the engine and its version, and Invaract's version as the adapter |
| `parent` | run | section 2.1 |
| `errorMessage` | run | a refused write's explanation (`VerificationPipeline.explain`) |
| `jobType` | job | `BATCH`, integration `INVARACT`, job type `JOB` |

Invaract's own facets, each with a JSON Schema published by the docs site
(`docs-site/public/schemas/openlineage/1-0-0/`):

- `invaract_contract` (output dataset): the contract (`id@version`), the dataset's name in it, the verdict (`PASSED`,
  `FAILED`), every violation (type, message, column, expected, actual, remediation), the inputs it could not verify, and
  the transformation fingerprint when one was computed.
- `invaract_run` (run): the adapter, how it attached (`enforce`, `dryRun`), and on the terminal event the run's totals
  (checks passed and failed, violations, writes).

### 2.4 Scope settings

What is published is configurable, for cost and for privacy: `facets.disabled` (a comma-separated list of facet names,
for instance `columnLineage,schema`), `schema.maxFields` and `columnLineage.maxFields` (larger schemas and lineage are cut to
that many fields), `failuresOnly=true` (only runs with a refused write publish at
all - for a team that wants lineage tools to show problems only), `emission=perCheck|endOfRun`.

## 3. Settings

Three neutral settings in `InvaractConf`, so the conformance kit checks every adapter reaches them:

| Setting | Meaning |
|---|---|
| `openLineageUrl` | The OpenLineage HTTP endpoint's base URL (`http://marquez:5000`): publishing is on, with every default |
| `openLineageConfig` | A `.properties` file with every setting below; publishing is on when it names a transport |
| `openLineageNamespace` | The job namespace |

The configuration file (keys without a prefix):

| Key | Default | Meaning |
|---|---|---|
| `transport` | `http` when a URL is set | `http`, `file` or `console` |
| `url` | `openLineageUrl` | HTTP base URL |
| `endpoint` | `api/v1/lineage` | path under `url` |
| `apiKeyEnv` | `OPENLINEAGE_API_KEY` | environment variable holding a bearer token; never the token itself |
| `header.<Name>` | | extra request headers |
| `compression` | `none` | `gzip` |
| `timeoutMs` | `5000` | per request |
| `path` | | the `file` transport's file (one event per line) |
| `namespace`, `jobName`, `parent.*` | | section 2.1 |
| `queue.capacity` | `1000` | events held for delivery; beyond it the newest are dropped, counted and logged. A queued event holds its plan until it is published, so this bounds memory: a thousand events of a 200-column job are about 100 MB at worst (an endpoint that hangs), a few MB for an ordinary one |
| `retry.maxAttempts` | `3` | per event, for a failure another try could fix (connection, timeout, 408, 429, 5xx) |
| `retry.initialBackoffMs`, `retry.maxBackoffMs` | `200`, `5000` | |
| `circuit.failures` | `5` | consecutive failed deliveries that stop delivery attempts... |
| `circuit.cooldownMs` | `30000` | ...for this long; events meanwhile are dropped (or dead-lettered), counted and logged |
| `deadLetter.path` | | a file that keeps what could not be delivered, one event per line, for replay |
| `maxEventBytes` | `1048576` | an event larger than this sheds `columnLineage`, then `schema`, before it is dropped |
| `shutdownTimeoutMs` | `5000` | how long the process end waits to close runs and deliver what is queued |
| `facets.disabled`, `schema.maxFields`, `columnLineage.maxFields`, `failuresOnly`, `emission` | | section 2.4 |

A setting that cannot be used (an unknown `transport`, a number that is not one, an `apiKeyEnv` naming a variable that is
not set when one was asked for explicitly) fails the job at start-up under enforcement, as a broken notification sink
does, and is said once and ignored in dry-run mode, whose promise is never to fail a job.

OpenLineage publishing composes with `notifyConfig`: both can be on, and they never share a failure.

## 4. Architecture

```
adapter ─ VerificationPipeline.verifyWrite ──► ContractValidationEvent (+ WriteLineage: references only)
        ─ WriteEvent / JobSummaryEvent ──────┐
                                             ▼
                              OpenLineageSink.publish  (job's thread: enqueue a reference, O(1))
                                             │ bounded queue
                                             ▼
                              worker thread: OpenLineageMapper (run state, facets, naming)
                                             → OpenLineageJson → Transport (http | file | console)
                                                 retry, circuit breaker, dead letter
```

- `WriteLineage` (`verification-core`): the write's plan, its output and inputs (location, format, schema, catalog), as
  references the pipeline already holds. Carried on `ContractValidationEvent.lineage`, not serialized into Invaract's own
  event JSON (whose schema is unchanged). Column lineage is computed from it lazily, on the worker thread.
- `OpenLineageSink` (`com.invaract.verification.openlineage`): a `NotificationSink`. `AttachSetup` builds it from the
  settings and combines it with the `notifyConfig` sink, if any, so every adapter that already uses `AttachSetup` publishes
  OpenLineage with no change of its own.
- `OpenLineageMapper`: pure functions from Invaract's events to OpenLineage events, with the per-run state (started,
  totals, held events) kept in a bounded map. Only the worker thread touches it.
- `OpenLineageNaming`: section 2.2.
- `JsonText` (`verification-core`'s notification package): an RFC 8259 writer, written by hand (no JSON library at
  runtime); `NotificationJson` now escapes through it too, which fixed control characters and non-finite numbers in
  Invaract's own events.
- Column lineage follows the adapter's declaration: an event carries the plan for `columnLineage` only when the adapter
  declares `lineage.columnLevel` supported or partial (`VerificationPipeline.tracesColumns`), so an adapter whose plans
  are opaque never claims column lineage it does not trace.
- A write refused before it is checked in full (Python I/O judges a write before it is staged) is refused through
  `VerificationPipeline.rejectWrite`, whose event carries the write's lineage as `verifyWrite`'s does.
- An adapter installed in code with a sink of its own (Spark's `forContract`, Beam's `InvaractBeam.enforce`) still gets
  OpenLineage from configuration alone: `AttachSetup.withOpenLineage` joins the OpenLineage sink the settings ask for,
  unless the sink already carries it.
- Dry run publishes each inferred write as an output; `ContractInferenceEvent` carries no plan, so a dry run's events
  have no inputs and no column lineage.
- `Transport`: `HttpTransport` (`java.net.http`, optional gzip), `FileTransport` (appends one line per event, under a
  lock, so concurrent processes interleave whole lines), `ConsoleTransport` (SLF4J).

## 5. Performance

What turning it on costs the job:

- On the job's thread: building `WriteLineage` from references the pipeline already holds, and one `offer` to a bounded
  queue. No mapping, no JSON, no I/O. Measured in section 8.
- Off the job's thread: column lineage (`Lineage.trace`, already trampolined for deep plans), JSON, delivery. One daemon
  thread per sink.
- Memory: at most `queue.capacity` events, each one a reference to data the job already built.
- At the job's end: at most `shutdownTimeoutMs` (default five seconds) waiting for delivery - only when there is still
  something to deliver.

## 6. Hardening

- Never throws into the job: the sink is wrapped (`SafeNotificationSink`), and the worker catches everything.
- A slow endpoint is bounded by `timeoutMs`; a down one by the circuit breaker; a flood by the queue's capacity. Every drop
  is counted, and said at WARN once per minute at most, never once per event.
- Secrets: a bearer token only from an environment variable; credentials removed from every dataset name and from every
  URL a log line shows.
- Text: every string the job controls (column names, messages, locations) is escaped; property tests hold the JSON
  well-formed for any string, control characters and unpaired surrogates included.
- Size: `maxEventBytes`, then shedding, then dropping with a WARN.
- Concurrency: one worker; state per run in a bounded map (oldest runs closed first when it is full).

## 7. The conformance kit

A capability every adapter declares, `reporting.openLineage` ("publishes OpenLineage run events through configuration
alone"), and scenarios that attach a real job through configuration (`ConformanceAdapter.attached`) with
`openLineageUrl` set to an endpoint the kit serves, then hold what arrives to:

- every event's required fields (`eventType`, `eventTime`, `producer`, `schemaURL`, a UUID run id, the job) - the events
  themselves are built by the core, whose tests hold every one to the official OpenLineage JSON Schema;
- one `START` and one terminal event per run, in that order, all with the run's id;
- the namespace `openLineageNamespace` sets;
- the inputs and the output named for the locations the job read and wrote (section 2.2), the output's `schema` facet
  equal to the columns written;
- the contract's verdict on `dataQualityAssertions` and `invaract_contract`: `PASSED` for an allowed write; for a
  refused one, `FAILED`, the violation, and a `FAIL` terminal event with `errorMessage`;
- `columnLineage` exactly when the adapter declares `lineage.columnLevel` supported or partial, and its edges naming real
  input columns;
- a job that is refused still publishes; an unreachable endpoint does not fail the job.

Three scenarios (`openlineage-run`, `openlineage-refused`, `openlineage-unreachable`), attached through
`Attachment.OpenLineage`; `OpenLineageConformance` judges them. `BrokenAdapterSpec` gains the broken adapters they exist to
catch: one that never sets the endpoint on its engine, one that loses the namespace, and one that never ends its run.

## 8. Test plan and results

**Core (`verification-core`, `com.invaract.verification.openlineage`, about 100 tests).** `JsonTextSpec` (property tests:
any string, control characters and lone surrogates included, renders as JSON a strict parser reads back unchanged);
`OpenLineageNamingSpec`; `OpenLineageSettingsSpec` (every key, every refusal); `OpenLineageMapperSpec` (every event and
facet, validated against the official OpenLineage 2-0-2 JSON Schema and each facet against its own versioned schema, the
spec vendored at release 1.55.0; Invaract's facets against the schemas the docs site publishes); `OpenLineageSchemaSpec`
(the validator itself catches what it should); `OpenLineageDeliverySpec` (ordering, retries and backoff, the circuit
breaker, dead letter, shedding, a full queue, bounded close, interrupts, many threads); `OpenLineageTransportSpec` (a
real HTTP server: statuses, no redirects, gzip, headers, timeouts, credentials never in a message; concurrent file
writers interleave whole lines); `OpenLineageAttachSpec` (through `AttachSetup` as every adapter attaches it; a real JVM
whose shutdown hook ends its run).

**Kit.** `OpenLineageConformanceTest` breaks each rule of the judge one at a time; `BrokenAdapterSpec` holds the three
scenarios to the mistakes above. The reference adapter conforms.

**Adapters, through the kit's scenarios on real jobs.** Spark (`SparkConformanceSpec`), Beam Java
(`BeamConformanceSpec`), Beam Python (`BeamPythonConformanceSpec`, a real Python pipeline and the checker), Python I/O
(`PythonIoConformanceSpec`, and `PythonIoPackageConformanceSpec` with real Python jobs and the package): all three
OpenLineage scenarios conform, and are on each adapter's baseline. Plus `SparkOpenLineageSpec` (a real application
attached by `spark.sql.extensions`, ended by `spark.stop()`; one installed in code with `forContract`) and a Beam test of a
check built in code.

**Mutation testing** (Stryker4s, scoped to the new files): `verification-core`'s OpenLineage files and `JsonText`/
`WriteLineage` scored 83% (of covered code 85%) before the targeted tests were added for the real survivors (run
eviction, the held-event cap, statistics, dataset-level lineage, credentials holding `@`, a daemon worker, interrupted
callers), which also found and fixed an empty user part (`hdfs://@nn`) left in a namespace. What survives is in the
rate-limited WARN summary's arithmetic and log text, and two equivalent mutants (a boundary the map's own invariant
never reaches; a shortcut whose fallback computes the same answer).

**Performance** (`OpenLineagePerformanceSpec`, `SparkOpenLineageSpec`; a laptop-class container):

| Measure | Result |
|---|---|
| A check of a 200-column job (two inputs joined, filtered), OpenLineage off vs on | 41-112 us vs 68-169 us across runs; the difference is within run-to-run noise (-50 to +57 us) |
| Publishing one event, on the job's thread | about 1 us (an `offer`), whatever the endpoint does |
| 1,000 events against an endpoint answering in 1 s | 0-14 ms on the job's thread |
| 50,000 events against a stalled endpoint, `queue.capacity=1000` | 1,001 kept, 48,999 dropped and counted; 0.1-1.4 us each |
| The worker, 200-column events with column lineage | 450-760 events/s; about 100 KB each uncompressed |
| A Spark job of 20 writes, OpenLineage off vs on (endpoint answering in 20 ms) | 2,960 ms vs 2,939 ms |

The worst case for memory is a wide job writing many times while the endpoint hangs: each queued event holds what
describes its write, which is why `queue.capacity` defaults to 1,000, not 10,000.

## 9. Alongside openlineage-spark

`openlineage-spark` publishes one run per Spark SQL execution, with the plan's lineage as Spark sees it; Invaract publishes
one run per application with what it verified. Both can be on: they are different jobs in the graph (set
`openLineageNamespace` and `jobName` to keep them apart, or the same `parent` to group them under one orchestrator run),
and the datasets are the same nodes, since both follow the naming conventions. That is measured, not assumed (section 11):
on every job of the cross-check corpus the two name every dataset alike, except a table a write creates in a catalog
(`CREATE TABLE AS SELECT`), which Invaract names by its catalog identifier, as its location does not exist yet.

## 10. Progress

- [x] Design (this document)
- [x] Core: model, naming, mapping, JSON, transports, delivery, settings
- [x] Kit: capability, scenarios, broken adapters, declarations and baselines
- [x] Adapters: Spark, Beam Java, Beam Python, Python I/O
- [x] Hardening and performance, measured
- [x] User documentation (`docs-site`: "Publish Lineage to OpenLineage", the configuration reference, the capability pages)
- [ ] Gates on CI: coverage (blocked locally by Maven Central rate-limiting the scoverage reporter), the native checker build
- [x] Spark lineage cross-checked against openlineage-spark and Spline on real jobs (section 11), and the gaps it found
  closed or listed

## 11. Cross-checking Spark lineage

Unit tests can only hold lineage to what their author expected. `lineage-crosscheck/` holds it to two independent tools
instead, on real Spark jobs: each job of its corpus (`Corpus.scala`, 53 jobs: projections, conditions, filters, sorts,
joins of every kind, aggregates, windows, rollups, pivots, unions, nested data, SQL with views, CTEs and subqueries,
UDFs, the Dataset API, caching and checkpoints, partitioned and appending writes, catalog tables, Delta Lake, Iceberg and
JDBC) runs once in a session with all three capturing it:

| Tool | What it is held to |
|---|---|
| Invaract | the job's contract declares its output; Invaract publishes OpenLineage to a file |
| openlineage-spark 1.55.0 | the reference for dataset naming and for the transformation types of the column lineage facet |
| Spline agent 2.4.0 | an independent model of the analyzed plan: every written attribute traced back to the read attributes it comes from |

Each tool's report of the write is reduced to one model (`Captured`: inputs, output, direct edges with their subtypes,
indirect edges per field or for the whole output) and compared (`Comparison`). Every difference is either fixed or
explained in `Corpus.scala`, as `Gap` (Invaract's), `Reference` (the other tool misses or adds something) or `Choice` (a
convention); the spec fails on a difference with no explanation and on an explanation for a job that now agrees. The
exact differences are checked in (`crosscheck-baseline.txt`), so any change in Invaract's lineage - or a reference's, on an
upgrade - is seen, and docs-site's "Spark Column Lineage Coverage" page is generated from the run (`Report`), with
Invaract's known gaps listed. `CROSSCHECK_UPDATE=true sbt test` rewrites both. CI runs it (`lineage-crosscheck`) whenever
what Spark's lineage is built from changes.

What the first runs found, and what was done:

| Found | Done |
|---|---|
| A column of one join side attributed to a read on the other that merely lacked a qualifier (`b <- orders.b`, where `orders` has no `b`) | `ir.Lineage.trace(plan, readColumns)`: a read gives only a name it has, from the schemas the adapter already reports |
| An indirect edge named a column of the filter's own computation, not of a read (`orders.spark_grouping_id`; a filter on `x = amount * 2` as `orders.x`) | indirect lineage moved into `ir.Lineage.traceDetailed`, resolved through the plan like direct lineage |
| `explode`, `ROLLUP`/`CUBE`/`GROUPING SETS` were opaque (`UnknownPlan`): no lineage through them | new IR nodes `Generate` and `Expand`, translated, traced, fingerprinted and printed |
| No `SORT`, `WINDOW` or `CONDITIONAL` lineage; no grouping for `distinct`/`dropDuplicates`; `SELECT DISTINCT` untranslated | each traced; deduplication translated as Spark's optimizer does, a grouping |
| An `IN (subquery)` condition never named the subquery's column | the subquery's columns are the `IN`'s arguments |
| No `masking` | set for a hash, `mask` or a count |
| A path-based Delta MERGE/UPDATE/DELETE was located at its plan's text (`Relation [none#0L,none#1L] parquet`), so a correct MERGE was refused, and any two tables of one shape matched one contract | located at the table's own path |
| A MERGE's lineage named columns of its source the table does not have, and not the table it reads | edges only for columns the write has; the target listed among the inputs |
| An opaque JDBC URL (H2, SQLite, Oracle thin) named apart from openlineage-spark | named as it names them, credentials stripped |
| A DataFrame made from an RDD had no inputs | the directories the RDD's file scans read are its inputs (no columns) |

What stays a difference is on the generated page, each with its reason. Most are openlineage-spark reading Spark's
*optimized* plan (an inferred `isnotnull` filter on every join key; `IN`/`EXISTS` rewritten into semi joins), or a
reference missing what Invaract follows (subqueries, CTEs, `localCheckpoint`, rollup keys). Invaract's own gaps: a `USING`
join's key attributed to both sides; a MERGE traced by column name, not through its clauses; a table created by its own
write named by catalog identifier.

## 12. Production-readiness review

A review of the whole feature as something a platform team would run on every job, against the questions that decide
that: is the lineage correct, is it complete enough and are its gaps known, can a team move to it from what they run
today, can it hurt a job, and can it be operated. Each finding, and what was done:

| # | Area | Finding | Status |
|---|---|---|---|
| 1 | Correctness | Column lineage was only ever compared with its own author's expectations | **Fixed**: the cross-check (section 11) holds it to openlineage-spark and Spline on real jobs, in CI |
| 2 | Correctness | Wrong edges: a column attributed to a read that lacks it; an indirect edge to a column of the plan's own computation; a MERGE's source columns the table does not have | **Fixed** (section 11) |
| 3 | Correctness | A path-based Delta MERGE/UPDATE/DELETE was located at its plan's text: a correct MERGE refused, two tables of one shape matching one contract | **Fixed**: located at the table's path |
| 4 | Correctness | Dataset names with backslashes on Windows, and a Windows run publishing only its terminal event | **Fixed** (string normalisation that cannot throw) |
| 5 | Correctness | A completed-write report arriving after a run's end reopened the run under the same id | **Fixed** (run generations, section 2.1) |
| 6 | Completeness | `explode`, rollups, deduplication, sorts, windows, conditions, `IN` subqueries, masking, RDD-backed inputs were missing | **Fixed** (section 11) |
| 7 | Completeness | Remaining gaps | **Documented**, generated from the cross-check: a `USING` join's key attributed to both sides; a MERGE traced by name, not through its clauses; a table a write creates in a catalog named by its identifier |
| 8 | Migration | Dry-run mode (the observe-only mode a team starts in) published no inputs, schemas or column lineage | **Fixed**: a dry run publishes the lineage enforcement would, on every adapter |
| 9 | Migration | A job launched by Airflow carries its parent and root runs in `spark.openlineage.parent*`/`rootParent*`; Invaract ignored them | **Fixed**: read as they are (`ConfigSource.environment`); the root run is published too (`ParentRunFacet` 1-2-0) |
| 10 | Migration | Dataset identity with openlineage-spark was assumed | **Measured**: identical on every corpus job but the CTAS case (row 7); opaque JDBC URLs now named as openlineage-spark names them |
| 11 | Migration | No path from openlineage-spark or Spline | **Added**: the docs-site guide "Move from openlineage-spark or Spline", with a settings mapping |
| 12 | Safety | Publishing must never slow or fail a job | **Held**: off the job's thread, bounded queue, retries, circuit breaker, size shedding, bounded shutdown (sections 5, 6); measured (section 8) |
| 13 | Safety | Secrets | **Held**: the API key is read from an environment variable named in the settings, never the settings themselves; headers the client or transport set are refused; credentials are stripped from every dataset name, opaque JDBC URLs (`user/password@`, `user=`/`password=` properties) included |
| 14 | Operability | The publisher's own health is visible only in the log (a summary of dropped, failed and skipped events, at most once a minute) | **Follow-up**: no metrics source (Spark's `MetricsSystem`, a counter an orchestrator can scrape) yet |
| 15 | Operability | A process killed outright cannot end its run | **Documented**: the backend shows it running; nothing in the process can prevent it |
| 16 | Operability | A streaming query is checked once, when it starts, so its run carries one write's lineage, not one per micro-batch | **Documented** (as for enforcement, docs/SPARK_ADAPTER.md); the cross-check's `streaming` job shows what is published |
| 17 | Evolution | Invaract's own facets need a versioning rule | **Set**: the schemas are published under a versioned path (`schemas/openlineage/1-0-0/`). A new optional field is a new minor version under a new path (`1-1-0/`); a removed or changed field a new major version; a published version is never edited, so every event's `_schemaURL` keeps validating |
| 18 | Transports | No Kafka or Kinesis transport, which openlineage-java has | **Follow-up**: HTTP, file and console only; the migration guide says how to reach Kafka meanwhile |

Gates every change to this feature now passes: the OpenLineage 2-0-2 schema on every event, the conformance kit's three
scenarios on every adapter, mutation testing (at or above 70% on every file changed, section 8), MiMa, coverage, the
performance specs, and the lineage cross-check with its baseline and generated page.
