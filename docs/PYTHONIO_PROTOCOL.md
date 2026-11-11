# The Python I/O Checker Protocol

How the `invaract` Python package (`pythonio-adapter/python`) and its checker (`PythonIoCheck`, `pythonio-adapter`) talk
to each other. The package does no checking: it starts the checker once per run and describes to it every read and write
the job makes; the checker answers each with a verdict. This document is the reference for anyone changing either side,
or writing another client for the checker. Why the adapter is built this way is `docs/PYTHON_IO_ADAPTER_DESIGN.md`
(section 4); what was built and found is `docs/PYTHONIO_ADAPTER.md`.

**The schema.** Every message is described by one JSON Schema (2020-12),
[`docs-site/public/schemas/pythonio/v1/protocol.schema.json`](../docs-site/public/schemas/pythonio/v1/protocol.schema.json),
published at `https://mlltx.github.io/Invaract/schemas/pythonio/v1/protocol.schema.json`. Its `$defs` name each request
(`OpenRequest`, `ReadRequest`, `WriteRequest`, `UnverifiableWriteRequest`, `WrittenRequest`, `FinishRequest`,
`PublishedRequest`) and each
reply (`OpenReply`, `OkReply`, `FinishReply`, `RefusedReply`, `NotCheckedReply`), and every field has a description.
This document says what the schema cannot: the order of messages, what the checker does with each, and how the protocol
changes.

## Transport

- The package starts the checker as a child process: the native program a platform wheel carries, `java -jar` on the jar
  the pure-Python wheel carries, or the command in `INVARACT_CHECKER` (`_checker.command`).
- Requests go to the checker's standard input and replies come back on its standard output: one JSON object per line,
  UTF-8, each line ended by `\n`. Every request gets exactly one reply, in order; the package waits for each reply before
  sending the next request.
- The checker's log goes to its standard error, which is the job's own.
- The package closes the checker's standard input when the run ends, and waits for it to exit: the checker delivers its
  events (to the notification sink the settings name) before it exits.

A request describes data: names, formats, columns and their types, how many empty values each column holds. It never
carries any of the data.

## A run, message by message

A run is one `open`, then any number of `read`s and `write`/`written` pairs, in the order the job makes them, then one
`finish`, and, when `finish`'s reply says to publish, one `published` once the package has put the outputs in place.
With `INVARACT_LINT` on, a `scan` follows `open`, and another may come just before `finish` ([`scan`](#scan)). This
is the recorded case `passes` (`pythonio-adapter/src/test/resources/protocol/passes`), a job that reads
two inputs and writes two outputs; `>` is a request, `<` its reply:

```text
> {"op": "open", "protocol": 1, "settings": {"contract": "${CASE}/contract.yaml"}, "job": {"name": "orders_job.main", "runId": "run-1", "pythonVersion": "3.11.17", "libraries": {"pandas": "2.3.3", "pyarrow": "25.0.1"}}}
< {"contract": {"id": "orders_report", "inputs": [...], "outputs": [...], "version": "1.0.0"}, "mode": "enforce", "ok": true, "protocol": 1}
> {"op": "read", "name": "orders", "schema": [{"name": "order_id", "type": "int64", "nullable": true, "nulls": 0}, {"name": "region", "type": "string", "nullable": true, "nulls": 3}, {"name": "amount", "type": "int64", "nullable": true, "nulls": 0}]}
< {"ok": true}
> {"op": "read", "name": "customers", "schema": [{"name": "customer_id", "type": "int64", "nullable": true, "nulls": 0}]}
< {"ok": true}
> {"op": "write", "name": "report", "format": "parquet", "saveMode": "overwrite", "schema": [{"name": "region", "type": "string", "nullable": true}, {"name": "total", "type": "int64", "nullable": true}, {"name": "lines", "type": "list<item: struct<sku: string, qty: int32>>", "nullable": true}]}
< {"ok": true}
> {"op": "written", "name": "report", "nulls": {"region": 0, "total": 0, "lines": 7}}
< {"ok": true}
> {"op": "write", "name": "audit", "format": "csv", "saveMode": "overwrite", "schema": [{"name": "order_id", "type": "int64", "nullable": true}, {"name": "status", "type": "string", "nullable": true}]}
< {"ok": true}
> {"op": "written", "name": "audit", "nulls": {"order_id": 0, "status": 12}}
< {"ok": true}
> {"op": "finish", "completed": true}
< {"ok": true, "publish": true}
```

(The `open` reply's contract is shortened here. The recorded case is older than the package's `format` on each read,
which the checker treats as unknown, and than `published`, which a package now sends next: the recorded case
`published-write-events` shows it.)

### `open`

Sent when the run starts: `@invaract.job`'s call, or `with invaract.run()`. It carries the protocol version the package
speaks, the Invaract settings by their neutral names (the package reads each from `INVARACT_<NAME>` and forwards it
unchanged, `_settings`), and the job's details, which become every event's `job`.

The checker reads the settings, resolves the contract (a file, or `registry://`), the location map and the
organisational policy, and refuses, before the job's code runs, a contract asking for a check this adapter cannot make
(`UNSUPPORTED_CONTRACT_FEATURE`). Its reply says the mode:

- `enforce`, with `contract`: what the package needs of the contract. Each dataset's resolved location, format, save
  mode, partitioning and CSV options, and its declared columns (an input's are the columns the package reads, with their
  types, which a CSV or database read is typed as; an output's are the ones written not-null when required). With
  `runtimeDataQuality` on, each output also gives its data checks, which the package evaluates on the data it stages.
- `dry-run`: no contract (`dryRun` set). Every read and write must carry its `location`, every write is let through, and
  the checker infers a draft contract from what the job does.

With `"check": true` the `open` starts a check (`invaract.check`) instead of a run: every declared input counts as read,
with its declared columns, so one write is judged alone; nothing is published, and the settings that send events are not
forwarded. A check needs a contract.

### `read`

Sent for each `invaract.read`, after the package has read the data and before it hands it to the job. The checker refuses
a name the contract does not declare, a `location` (from the job's `location=`) that is not the input's, a `format` that is
not the input's declared format, columns that disagree with the input's, CSV options that disagree with its `csv:`, and a
missing catalog registration its `catalog:` requires. The read is then part of every later write's plan: the checker
holds each write's inputs to the contract.

A read whose format already disagrees with the contract is sent **without** `schema`, before anything is read: the
checker refuses it on its format. Were nothing to disagree after all, it answers not-checked, so columns the package did
not read are never taken on trust.

### `write` and `written`

Each `invaract.write` is two requests. Some things are known before the data is written; whether a required column holds
an empty value is known only after, since pandas marks every column nullable whatever it holds.

1. `write`: the table described before any of it is staged. The checker judges everything but the empty values (the
   location, format, save mode, catalog, partitioning, CSV options, the inputs read, the columns and their types, each
   column taken to hold none), so a write that cannot pass is refused before the job spends time staging it. It publishes
   an event only when it refuses.
2. `written`: sent once the write is staged, with the empty values each column held, counted batch by batch as the data
   was written. A required column holding one is refused. With `runtimeDataQuality` on, `checks` reports, for each of the
   output's data checks in the order `open` gave them, how many rows broke it and some of their values. This is the
   verdict on the whole write, and its event.

A table the package cannot describe (one that offers no Arrow stream) is sent as a `write` with `unverifiable` and no
columns, and is refused (`UNVERIFIABLE_WRITE`).

### `scan`

Sent when `INVARACT_LINT` is `report` or `enforce`: once right after `open`, before the job reads anything, with what
invaract-lint found in the job's own code (the modules loaded when the run started, or `INVARACT_LINT_PATHS`); and once
more just before `finish`, for the job's own modules it imported while it ran, when there are any. It carries the
`phase` (`start` or `finish`), whether to `enforce`, the `scanner` and its version, the `files` scanned, every finding
(invaract-lint's `Finding.to_dict()`, the ones a `# invaract: allow` marker keeps included, as `allowed: true`), the
`errors` for files it could not read, and `durationMs`:

```text
> {"op": "scan", "phase": "start", "enforce": true, "scanner": "invaract-lint 0.1.0", "files": ["jobs/clean.py", "jobs/helpers.py"], "findings": [{"id": "aaaaaaaaaaaaaaaa", "code": "IVR001", "category": "read", "rule": "pandas-read", "path": "jobs/helpers.py", "line": 5, ..., "allowed": true, "reason": "a lookup table shipped with the job", ...}], "errors": [], "durationMs": 7}
< {"ok": true, "status": "EXCEPTIONS"}
```

The checker publishes a `CODE_SCAN` event for it. Enforcing a contract, it refuses the run for each finding not
`allowed`, one `UNGOVERNED_DATA_ACCESS` violation each (the first 50, and one more counting the rest), and answers
not-checked when a file could not be read or none was scanned. A dry run is never refused for a scan. The reply's `status`
is the event's: `FINDINGS`, `NOT_SCANNED`, `EXCEPTIONS` or `CLEAN` (recorded cases `code-scan-*`).

### `finish`

Sent once, when the job returns (`completed: true`) or raises (`completed: false`). A completed run that never wrote a
declared output is refused (`MISSING_OUTPUT`). In a dry run, the checker merges the drafts into one for the job and
publishes it. The reply's `publish` tells the package whether to publish what it staged: when it is true the run waits
for `published`; otherwise (or when `finish` is refused) the run ends here, with its `JOB_SUMMARY`.

### `published`

Sent once, after a `finish` whose reply said to publish, when the package has put the outputs in place. It lists each
output published, by name, with what the package knows of it: the rows it staged, the files and bytes put in place, the
Delta version or Iceberg snapshot its commit made, and how long staging and publishing it took. An output it did not
publish is not listed: `ignore` on an output that holds data, or an output put back after a later one failed to publish
(the package publishes the outputs in the order first written, and if one fails, puts back those already published by a
rename; a table's commit, a copy to object storage or a database's transaction cannot be put back, and stays listed).

The checker publishes a `WRITE` event for each output listed, built from what it checked of the output's first staged
write (location, format, save mode, catalog, partition columns), its columns as written (a required column not nullable,
any other nullable if any write of the output said so), and what `published` says; then the
run's `JOB_SUMMARY` (its validation events by verdict, their violations, and its writes), and delivers the run's events.
A name the run did not stage is answered not-checked; the run ends either way. After `published`, every request is
answered not-checked.

A package interrupted before `finish` or `published` (killed, or a publish that crashed) never sends them: when its
standard input closes, the checker ends the run itself, with its `JOB_SUMMARY`, and delivers the events.

## Replies

| Reply | Means | The package |
|---|---|---|
| `{"ok": true, ...}` | The request passed. `open`'s reply carries the protocol version, the mode and the contract; `scan`'s, the scan's `status`; `finish`'s, `publish`. | goes on |
| `{"ok": false, "status": "refused", "violations": [...], "explanation": "..."}` | The contract refused it. `violations` lists the kinds, `explanation` is Invaract's standard message. | raises `ContractViolation` (exit status 1) |
| `{"ok": false, "status": "not-checked", "reason": "..."}` | It could not be checked: a malformed request, one out of order (a `write` before `open`, a `written` with no `write`, a `published` before `finish`), a contract that cannot be read, an unknown setting, another protocol version. | raises `CheckNotRun` (exit status 2) |

A refusal or a not-checked reply ends the run: the package raises, discards everything staged, sends `finish` with
`completed: false` so the checker delivers its events, and publishes nothing. (A not-checked reply to `published` comes
after the outputs are in place: the package raises, and the run is over.) A checker that stops, or answers with a
line that is not JSON, is a `CheckNotRun` too. Nothing is ever published unchecked.

## Events

The checker, not the package, publishes every event, to the sink the `notifyConfig` setting names (the package never
sees a sink). Which event each request leads to:

| Request | Events |
|---|---|
| `open` | `CONTRACT_VALIDATION FAILED` when the contract is refused when the run opens (an organisational policy it breaks, a check this adapter cannot make) |
| `read`, `write` | `CONTRACT_VALIDATION FAILED` when refused; nothing when they pass |
| `written` | `CONTRACT_VALIDATION`, `PASSED` or `FAILED`; in a dry run, `CONTRACT_INFERENCE` |
| `scan` | `CODE_SCAN`; and `CONTRACT_VALIDATION FAILED` when an enforced scan refuses the run (`UNGOVERNED_DATA_ACCESS`) |
| `finish` | `CONTRACT_VALIDATION FAILED` for an output never written (`MISSING_OUTPUT`); in a dry run, `DRY_RUN_SUMMARY`; and, when the run ends here, `JOB_SUMMARY` |
| `published` | `WRITE` for each output listed, then `JOB_SUMMARY` |

Every event is a notification event as `docs-site/public/schemas/notification/v1/event.schema.json` describes it, with
`job.engine` `pythonio`; the package's tests validate each one a test reads against that schema. What each field of a
Python job's events holds is in the user guide (docs-site, "Configure a Notification Sink", "Events from a Python job").

## Columns and types

A column is `{"name", "type", "nullable", "nulls"}`. `type` is pyarrow's own name for the column's type, `str(field.type)`:
`int64`, `large_string`, `timestamp[us, tz=UTC]`, `decimal128(10, 2)`, `list<item: struct<sku: string, qty: int32>>`,
`dictionary<values=string, indices=int8, ordered=0>`. The checker maps it to the contract's types with
verification-core's `ArrowSchemas`, the mapping the Beam Python adapter uses too; a name it cannot map is not checked.
`nullable` is Arrow's flag. `nulls`, where the data has been seen, is how many empty values the column holds, and is what
a required column is judged on.

## Catalogs, locations and dry runs

**`catalog`.** A read or write of a catalog table also says which catalog it is registered in, as the contract's
`catalog:` requirement names one, and the checker puts it on the plan's read or write, where the requirement is checked as
on every engine (the `catalog-*` cases). The package sends it for an Iceberg table, whose location
(`<catalog>.<namespace>.<table>`) names it: `{"technology": "iceberg", "catalogName": "lake", "namespace": ["sales"],
"table": "report"}`; and for a database table: `{"technology": "jdbc", "catalogName": <database>, "namespace":
[<schema>], "table": <table>}`. Files and Delta tables, named by their path, carry none, so a requirement on them is
never met. The catalog service's own address is never sent, since a catalog URI can hold credentials.

**`location` with a contract in force.** A job migrated from a dry run may still say where it reads and writes
(`location=`). The package then sends it, and the checker checks it like any other location, so an argument left behind
cannot point a read or a write somewhere the contract does not declare (`location-given-agrees`,
`read-location-disagrees-refused`, `write-location-disagrees-refused`).

**Dry runs** (`dryRun`). There is no contract, so every `read` and `write` must carry the job's own `location` and
`format`. Every write is let through; each staged one gets a draft contract, published as a `CONTRACT_INFERENCE` event
with its self-check, and `finish` merges them into one draft for the job, named after it (`DRY_RUN_SUMMARY`), as Spark's
and Beam's dry runs do.

## Versions

`open` says the protocol version the package speaks (`protocol`), and its reply says the one the checker speaks. A
checker that gets another version answers not-checked; a package whose checker replies with another version, or with
none (a checker from before the field), raises `CheckNotRun`. So a package and a checker from different releases
(`INVARACT_CHECKER` naming an old checker, say) stop the run rather than read each other's messages wrongly. The checker
takes an `open` without `protocol` as version 1, which is the version of every package before the field was sent.

The version changes only when a message changes in a way an older package or checker would misread: a field removed or
renamed, a field's type or meaning changed, a reply the other side cannot tell apart. Adding an optional field leaves it
as it is: each side ignores fields it does not know. A new version gets a new schema file beside this one
(`schemas/pythonio/v2/protocol.schema.json`), and the old one stays published.

## How the protocol is held to the schema

- **Every request the package sends, and every reply it receives, in every test** of the package's pytest suite is
  validated against the schema (`tests/conftest.py`'s `protocol_checked`), and every event a test reads against the
  notification events' schema.
- **Every recorded case** (`src/test/resources/protocol`, run against the JVM by `PythonIoProtocolCasesSpec` and against
  the native program by `native/cases.py`): each reply must be one the schema allows for its request, and each request the
  schema rejects must be one the checker answered not-checked, except the few `tests/test_protocol.py` names
  (`TOLERATED`), each with the reason the checker accepts it.
- The docs build compiles the schema in Ajv's strict mode and validates every recorded reply with it
  (`docs-site/scripts/check-schemas.mjs`), a second implementation of JSON Schema.

## Changing the protocol

1. Change the checker's side: `PythonIoRequests.scala` (parsing a request) and `PythonIoCheck.scala` (answering it, and
   the events it publishes).
2. Change the package's side: every request is built in `invaract/_protocol.py`, and nowhere else.
3. Change the schema, and this document where the change is one of order or meaning.
4. Record a case for it: a directory under `src/test/resources/protocol` with `contract.yaml` and `requests.jsonl`, then
   `python3 native/cases.py <a copy of the case's parent> --record -- java -jar target/scala-2.12/invaract-pythonio-checker-0.1.0.jar`
   (after `sbt assembly`) to write its `replies.jsonl`. Read what it wrote before committing it.
5. Run the checker's suite (`sbt test`) and the package's (`python -m pytest` in `python/`).
6. If an older package or checker would misread the change, raise the version (`Requests.Version`, `_protocol.VERSION`,
   the schema's `const`s) and publish a new schema file.
