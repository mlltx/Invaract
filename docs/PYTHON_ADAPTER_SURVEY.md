# Native Python adapter survey

> **Status: survey. No adapter code exists.** This document helps decide whether and how to let Invaract check plain Python
> data jobs: scripts that read and write files themselves, with no data-processing engine underneath. It is not about Beam's
> Python SDK (Beam's library for writing pipelines in Python), which has its own adapter (docs/BEAM_PYTHON_ADAPTER.md).
> Engine surface surveyed: CPython 3.11.17 (the standard Python interpreter) running scripts that do their file work through
> the standard library (`open`, `csv`, `json`, `pathlib`, `shutil`, `os`, `sqlite3`) and through the two libraries almost
> every Python data script uses for columnar files, pyarrow 25.0.1 and pandas 2.3.3. Linux kernel 6.18.
> Written: 2026-10-26. Evidence checked: 2026-10-26. Source of each claim: section 10, "Evidence log".

## The short version

**What it would be.** A plain Python job has no plan to inspect beforehand. It is a script, and its reads and writes happen
as each line runs. So Invaract would sit *inside* the Python interpreter (the program that runs the script). When the
interpreter starts, Invaract would install a hook (a function Python calls whenever the script is about to do something, such
as open a file). It would also wrap the handful of library functions that write data files. Each time the job is about to
write, Invaract would compare that write with the data contract and refuse it, before a byte lands, if it breaks the
contract. This is the way Spark is checked, one write at a time, rather than the way Beam is checked, the whole job before it
starts. As everywhere, it checks the *shape* of a job: what it reads, where it writes, and which columns and types come out.
It never checks what the team's own code does to individual values.

**How much of the suite it is expected to pass.** Of the suite's 55 scenarios:
- 25 by doing the check;
- 2 only partly: one passes only because of how the kit's job is laid out, and one is expected to diverge;
- 15 by refusing safely;
- 13 skipped because they do not apply.

Details in section 7.

**Can it be switched on without changing the job's code?** *Partly.* A platform team installs one Python package in the
environment the job runs in, and sets `INVARACT_CONTRACT=<contract>` (plus any other Invaract settings) as environment
variables. The job's code and its command stay exactly as they are. Probe 5 showed this on an unmodified job. Section 5 has
the details, and the ways a job can escape it.

**Classifying outputs: a data asset or a readiness signal.** A contract can already label each output `type: DATA_ASSET`
(the data product itself) or `type: CONTROL`, which the contract model defines to include "a readiness/reconciliation
signal". Nothing checks an output's label today, on any engine. D13 proposes the check, and it suits plain Python well:
- the adapter confirms the job wrote to each declared place;
- it confirms that what landed there matches the label: a table with columns for a data asset, a marker or small status
  file for a readiness signal;
- it refuses a readiness signal for a data asset that was not written, and checked, earlier in the same job.

It is an engine-neutral change, so it needs a decision of its own (decision 9).

**The biggest risks.**
1. **The checking code runs in Java, so every machine that runs a governed Python job needs Java.** For Beam that was only the
   machine that submits jobs. Here it is every worker, and slim Python container images usually have no Java (UNVERIFIED).
   The alternative is a checker compiled to a program that needs no Java (D9).
2. **Some writes are invisible to Python.** pyarrow writes files from compiled C++ code. Python's hook never sees those writes,
   so a pyarrow write function Invaract has not wrapped goes through unchecked (probe 6: three of them did). On Linux the
   operating system can be told to refuse writes outside the contract's places, and probe 7 showed it stopping exactly those
   writes. It does not check columns, though, and other systems have nothing like it (D8).
3. **Python does not record which data went into which write.** The adapter knows what the job read *before* a write, not
   what that write was built from. So in a job with several outputs, a missing input can go unnoticed, and one scenario is
   expected to diverge from the suite (D4).
4. **Switched on through the environment, it fails open (lets the job run unchecked) when the package is missing.** If a job runs in a Python environment
   the platform did not prepare, nothing is checked and nothing says so. A small launcher command closes this for platforms
   that want it (section 5).

**Decisions I need from you.** (Each is explained in section 4; a one-line answer is enough.)
1. Build it, as a hook inside the interpreter that checks each write as it is made, sharing the existing Java checking code?
   *Recommend yes, starting on Linux.*
2. Run the Java checker as one helper process per job, which needs Java on every worker, and turn to a no-Java build only if
   platforms object? *Recommend yes.*
3. Write each output the contract declares to a temporary file first, and move it into place only once checked, so a refused
   write never damages the old data? *Recommend yes.*
4. Count every read before a write as feeding it. Then, in a separate change to the conformance kit, stop requiring the
   second violation in scenario 20 from an adapter that cannot know which reads feed which write? *Recommend yes.*
5. Offer the Linux operating-system restriction on writes (Landlock) as an opt-in setting at first, and make it the default
   later? *Recommend yes.*
6. Read `trustedSteps` on this engine as places and programs a platform trusts not to carry governed data (log folders,
   caches, `gzip`)? *Recommend yes.*
7. Wrap the standard library, pyarrow, pandas and Polars in the first version, and cover DuckDB and every other writer by
   reading files back from disk plus the operating-system restriction? *Recommend yes.*
8. Name it the `python` adapter, in a new `python-adapter` module with its own Python package, and share the pyarrow type
   mapping with the Beam Python adapter? *Recommend yes.*
9. Check each output against its declared kind, as an engine-neutral addition that every adapter declares a position on,
   built before the Python adapter? The kinds are a data asset or a readiness signal, with the signal naming the data assets
   it announces. *Recommend yes; the open questions on it are at the end of D13.*
10. When a job starts, should Invaract remove a stale readiness marker for each signal the contract declares, so
    downstream jobs never see yesterday's "ready" for today's run? *Recommend yes, as an opt-in setting.*

## The four requirements, one by one

The requirements this adapter is judged on first:
1. confirm the job wrote where its contract says;
2. have each output declare what it is (a data asset or a readiness signal), and confirm that;
3. switch it on without changing the job;
4. capture the schema of the files written.

Probes 10 to 13 were run for this section.

### 1. The job wrote where it said it would

**Verdict: yes, in two halves.** Nothing lands outside the declared places, and every declared place is written in this run.

**Nothing lands outside the declared places.**
- *Writes that go through Python* are seen before they happen, and refused: `open`, the `csv` and `json` modules, pandas'
  CSV, Excel and Parquet writers, fsspec on local files, renames, moves and touches (probes 2, 3 and 10).
- *Writes made inside compiled code* are not seen by Python at all. That covers pyarrow, Polars and DuckDB (probes 2, 6
  and 10). There are two answers:
  - A wrapper on each catalogued function sees the write before it happens: pyarrow's write functions, and Polars'
    `write_parquet`, `write_csv` and the other Python methods.
  - DuckDB's `COPY ... TO 'path'` is inside SQL text, so no wrapper can sensibly read it. On Linux, the operating-system
    restriction refuses every write outside the declared places, whatever makes it. Probe 13 showed it stopping pyarrow,
    Polars and DuckDB writes, a shell command, and a Python child running in its own virtual environment.

**Every declared place is written in this run.** This is checked when the job ends, as on Spark. It uses what the hook
recorded, plus a look at the files at each declared place that changed since the job started, which also catches writers
Python cannot see (probe 12).

**Details that decide what "the place it said" means:**
- *Write to a temporary file, then rename it into place* is a common, careful pattern. The rename names the destination
  (`os.rename ('data/out/.report.tmp', 'data/out/report.parquet')`, probe 10), and counts as the write landing there. A
  rename onto a declared output is not refused, contrary to the first draft of D7.
- *Paths are compared once `..` and symbolic links (shortcuts to another folder) are resolved.* A write through
  `linked/out/x.csv` is a write to `real/out/x.csv` (probe 10).
- *Libraries write their own temporary files* in the system temporary folder (`tempfile`, and openpyxl for Excel files,
  probe 10). That folder is trusted by default.
- *Cloud storage (S3, GCS)* was not probed: there are no credentials here (UNVERIFIED).
  - Writes through fsspec are Python code and can be wrapped.
  - The boto3 and google-cloud-storage clients would need wrappers of their own.
  - The operating-system restriction does not cover network writes.

### 2. Each output says what it is, and Invaract confirms it

The contract labels each output, and a readiness signal names the asset it announces (D13 proposes the `readinessFor`
field). Probe 12 built a small working prototype of the checks, with this contract:
- `report`: a data asset, the folder `data/out/report`, columns `id` and `amount`, both int64;
- `report_ready`: a readiness signal for `report`, the file `data/out/report/_SUCCESS`.

The job wrote its report with Polars, which Python cannot see.

| Case | What the job did | Result |
|---|---|---|
| a | wrote the report, then touched `_SUCCESS` | allowed: "every asset it names was written in this run and matches" |
| b | touched `_SUCCESS` first, then wrote the report | signal refused: "its asset 'report' was not written in this run" |
| c | wrote the report with DuckDB, `id` as text, then touched `_SUCCESS` | signal refused: "its asset 'report' has id: string, the contract says int64" |
| d | wrote nothing new; yesterday's report and marker were there; re-touched `_SUCCESS` | signal refused: "its asset 'report' was not written in this run" |
| e | put only an empty file at the report's place, then touched `_SUCCESS` | signal refused: "its asset 'report' holds an empty file" |
| f | wrote the report, then a pyarrow table as `_SUCCESS` | caught only at exit, after it landed: "readiness location holds a table, not a marker"; exit status 3 |

**How it works.** The readiness signal is the gate downstream jobs rely on, so that is where Invaract looks hardest:
- It sees all three ways a marker is written: created, opened for writing, or re-touched (`os.utime` only, probe 9).
- At that moment, it opens the asset's files *as they are on disk*. Each must have been written in this run, be a data file,
  and have the declared columns.
- Because it reads the files themselves, this works whichever library wrote them, DuckDB included (case c).

**What it does not do.**
- **It stops a new signal; it cannot withdraw an old one.** In case d, yesterday's `_SUCCESS` is still there. Downstream jobs
  should check the marker's date, or Invaract should remove a stale marker for a declared signal when the job starts. That
  is a choice to make (decision 10).
- **A table written as the marker by a writer Python cannot see** (case f) is caught only when the job ends. With a wrapper
  on that function, it would be refused before it landed.
- **"Written in this run" uses file modification times.** On a network file system whose clock differs from the machine's,
  or on an object store, that needs checking (UNVERIFIED).

### 3. Switched on without changing the job

| Route | How | Covers (probe) | Escapes (probe) |
|---|---|---|---|
| **Installed package** (recommended) | the package's `.pth` file, plus `INVARACT_*` environment variables | the job, its child Python processes, `multiprocessing` with both `fork` and `spawn`, `python -I` (probes 5, 13) | `python -S`; a job that runs in a Python environment without the package, such as one it builds itself (probe 13) |
| **Environment only, no install** | `PYTHONPATH` pointing at a shared folder holding a `sitecustomize.py` (a file Python runs at start-up if it finds one) | the same, without touching the job's environment (probe 13) | `python -I`, which ignores `PYTHONPATH`. It also hides the distribution's own `sitecustomize.py` (Debian's Python has one) unless it imports it in turn (probe 13) |
| **Launcher** | `invaract-python run job.py` | the same as the package, and refuses to start if Invaract cannot install itself | nothing extra (PREDICTED) |
| **Operating-system restriction** (Linux, opt-in) | set when the interpreter starts; it cannot be undone and every child inherits it | writes outside the declared places, from any library, any child process, a shell command or a job's own virtual environment (probe 13) | nothing on Linux. It cannot classify outputs or check columns: it is the floor under the other routes |

Two more points:
- **Long-lived processes** (a notebook's kernel, a scheduler worker that runs many tasks in one process) start before a task's
  settings exist. The package should read `INVARACT_*` at the first write rather than at start-up (not probed).
- **Java is needed wherever the job runs**, for the checker (D9). It is the main practical cost of this whole route.

### 4. The schema of the files written

There are two sources, and the recommendation is to use both:

- **From the call, before anything is written** (wrappers). This is what allows a refusal before the data lands.
  - pyarrow tables: exact.
  - pandas: its own types are vague (`object` for text, probe 11). pandas' own conversion to pyarrow gives the exact types,
    `id: int64; name: string; amount: int64; at: timestamp[ns]; tier: dictionary<...>`.
  - The `csv` module: names only.
- **From the file, after it is written** (read back from disk). This is the truth, whatever wrote it. Probe 11 wrote eight
  files and read them back:

| File | Written by | Read back |
|---|---|---|
| `pandas.parquet` | pandas | `id: int64; name: string; amount: int64; at: timestamp[ns]; tier: dictionary<values=string, ...>` |
| `pandas_index.parquet` | pandas, `id` as the index | `name: string; ...; id: int64` (the index became a column, moved to the end) |
| `polars.parquet` | Polars | `id: int64; tags: large_list<element: large_string>` |
| `duckdb.parquet` | DuckDB `COPY` | `id: int64 not null; name: string not null; d: date32[day] not null` |
| `fast.avro` | fastavro | `id: long; name: ['null', 'string']` |
| `pandas.feather` | pyarrow | as the Parquet file |
| `pandas.csv` | pandas | header only: `id, name, amount, at, tier` |
| `pandas.jsonl` | pandas | keys of the first record only: `id, name, amount, at, tier` |

**What this means.**
- Parquet, Avro and Feather files carry their exact schema, nested types and "may not be empty" included. The pyarrow type
  mapping Invaract already has covers what came back: `large_list`, `large_string`, `dictionary`, `date32`
  (`PyArrowSchemas.scala`, lines 59 to 108).
- CSV and JSON carry column names, never types. Their types can come only from the data the job handed over (pandas
  types), and the contract should say whether that is good enough or whether such a write is refused when it declares
  types.
- Two surprises a contract author will meet:
  - pandas writes a non-default index as an extra column;
  - pandas never marks a column "may not be empty", while DuckDB marks most of them.
- The same read-back gives **dry-run mode** a real schema for every file the job wrote, so draft contracts are accurate
  whichever library the team uses.

### 5. When the job writes to a database: Postgres and BigQuery

A database write does not go through Python's file opening, so the file mechanisms above do not apply. Probes 14 to 17 ran
a real Postgres 16 server, and the real BigQuery client library (3.46.2) against a recording stand-in for BigQuery, since
there are no Google credentials here.

**What the hook sees** (probes 14 and 17b):

| How the job writes | What the hook sees |
|---|---|
| Postgres through psycopg2, psycopg 3, or pandas `to_sql` (SQLAlchemy) | **nothing**: these drivers talk to the server from compiled code |
| Postgres through pg8000 (a pure-Python driver) | only the connection: `socket.connect (..., ('127.0.0.1', 55432))`. No SQL, no table |
| BigQuery through `google-cloud-bigquery` | **every request, before it is sent**: `http.client.send` carries `POST /bigquery/v2/projects/demo/datasets/gold/tables/report/insertAll` and the body `{"rows": [...]}` |

**So the work falls to three other places.**

1. **A wrapper on the database client, which sees each statement or request before it runs.**
   - *Postgres* (probe 16). Wrapping `psycopg2.connect` so that every cursor is a checking one covered:
     - the job's own SQL;
     - pandas `to_sql`.

     A SQL parser (sqlglot) named each statement's target table and kind. Results:
     - `INSERT INTO gold.other` was refused (an undeclared table);
     - `DELETE FROM gold.report` with no `WHERE` was refused;
     - `INSERT INTO gold.report` was allowed.

     Postgres's own `EXPLAIN` (it describes what a statement would do without running it) also names the tables read and
     their filters (probe 15): `WRITE INSERT gold.report; READ gold.orders filter=(orders.amount > 0)`. `EXPLAIN` cannot
     explain `TRUNCATE` or `CREATE TABLE AS`; the parser names their targets.
     - **The gap.** A job that picks its own cursor type (`cursor(cursor_factory=RealDictCursor)`) got past the wrapper,
       and its insert into `gold.other` landed.
   - *BigQuery* (probe 17). Every write goes through one Python client object, and its requests say exactly what is
     written:

     | Call | What the request states |
     |---|---|
     | `load_table_from_dataframe` | `destinationTable` (`demo.gold.report`), `writeDisposition` (`WRITE_TRUNCATE`), the `schema`, `sourceFormat: PARQUET` |
     | `insert_rows_json` | the table in the path (`.../tables/report/insertAll`), and the rows |
     | `query("INSERT INTO gold.report SELECT ...")` | the SQL text; BigQuery itself runs it |
     | `query(..., destination=..., write_disposition=WRITE_APPEND)` | `destinationTable` and `writeDisposition` |

     For SQL that BigQuery runs, BigQuery has its own **dry run**: the same query with `dry_run=True` runs nothing. The
     client then reads the statement type and the tables it reads from the answer (`QueryJob.statement_type`,
     `referenced_tables` and `ddl_target_table`, `google/cloud/bigquery/job/query.py`, lines 1258 to 1307). What a real
     BigQuery dry run returns for each statement type was not seen here (UNVERIFIED: no credentials).
2. **The database's own permissions** are the floor, as the operating-system restriction is for files. A Postgres role
   allowed only `INSERT` on `gold.report` was refused everything else by Postgres itself (probe 15):
   - `insert into gold.other`: `ERROR: permission denied for table other`;
   - `delete from gold.report` and `truncate gold.report` were refused the same way.

   BigQuery's equivalent is the job's service account (IAM). A platform team applies either through the credentials it
   gives the job, without touching the job.
3. **The database is the truth about the schema.** Reading the table back gives its exact columns, types and "may not be
   empty":
   - Postgres's `information_schema.columns`: `id bigint NO; amount bigint YES` (probe 16);
   - BigQuery's `get_table`: `('id', 'INTEGER', 'REQUIRED'), ('amount', 'INTEGER', 'NULLABLE')` (probe 17).

   One surprise: pandas' load into BigQuery sent every column as `NULLABLE` (probe 17), the same as with Parquet files.

**What it means for the four requirements.**
- **Where it wrote.** A table is located by its name. That would be `<database>/<schema>/<table>` for Postgres (with the
  server's address as its catalog location), and `<project>/<dataset>/<table>` for BigQuery, as the Beam adapters already
  write it. The wrapper refuses an undeclared table before the statement runs. Permissions refuse it even when the wrapper
  is bypassed.
- **What it is.**
  - A data asset is a load or insert into a governed table.
  - A readiness signal is usually a row in a status table, such as `INSERT INTO control.load_status VALUES ('report',
    'READY', now())`.
  - The wrapper sees both statements, in order, and whether each succeeded. So the rule "no ready row unless the asset was
    written and checked in this run" works the same as for files.
  - Updates, deletes and merges are visible too. That makes Invaract's existing rules on row-level changes (for example
    `forbid_unconditional_delete`) checkable for these jobs, which they are not for files.
- **Save mode.**
  - BigQuery states it outright (`writeDisposition`).
  - For Postgres it follows from the statements: `INSERT` is append, `TRUNCATE` then `INSERT` is overwrite, `CREATE TABLE
    AS` is "error if it exists".
  - pandas' `if_exists="replace"` drops and re-creates the table.
- **The schema.**
  - Before the write: from a BigQuery load's `schema`, or the `INSERT`'s column list and the table's own columns.
  - After the write: from the table itself, exactly.
- **Switching it on.** The wrappers come in the same package as everything else, so there is nothing new to install.
  Permissions are a second, independent switch the platform already holds.

**What stays out of reach.**
- SQL hidden inside a stored procedure or function (`CALL load_report()`): the wrapper sees only the call. It must be
  refused, or trusted by name.
- Drivers the package does not wrap: asyncpg, or a database driver for another engine.
- Another program run by the job (`psql`, the `bq` command): refused unless trusted.
- BigQuery multi-statement scripts, where a dry run may say little (UNVERIFIED).

## How to read this document

- **What "conforming" means here.** Invaract has a *conformance suite*: 55 small, engine-neutral jobs, called *scenarios*,
  that every adapter is run against. An *adapter* is the part of Invaract that knows one engine. It declares what it can and
  cannot do (its *declaration*), and each scenario must come out the way that declaration promises. **Refusing safely is
  conforming.** If an adapter says "I cannot check this", the suite expects it to refuse any contract that depends on it,
  rather than let the job through unchecked. So a ❌ below is a limit to understand, not a failing grade.
- **Forecast marks.**
  - ✅ expected to do the check and produce what the suite expects.
  - ⚠️ only partly, or with a workaround, or depends on something not yet verified.
  - ❌ cannot do the check, so the suite expects a safe refusal.
  - ➖ does not apply.
- **Confidence.** **High**: shown by a probe I ran. **Medium**: each fact it rests on was probed, or a source says so, but the
  whole has not been assembled. **Low**: inferred. Nothing here has been run as an adapter.
- **"Probe".** A throwaway program I wrote and ran against real Python 3.11.17, pyarrow 25.0.1 and pandas 2.3.3 to see what
  they reveal. The programs are not committed; what they printed is quoted below. Each probe's checking was a toy written for
  the probe, not Invaract's checker. The probes show what can be *seen* and *stopped*, not Invaract's real verdicts.
- **Words.** Each technical term is explained where it first appears, and again in the glossary at the end.

## 1. What Invaract asks of an engine

### The contract, and the check

A *data contract* is a small YAML file saying what a job may read, where it must write, and what the written data must look
like:

```yaml
id: orders_report
version: "1.0.0"
inputs:
  - { name: orders, location: data/in/orders.csv, format: csv }
outputs:
  - name: report
    location: data/out/report.parquet
    format: parquet
    schema:
      fields:
        - { name: id, type: long, required: true }
        - { name: amount, type: long }
```

Parquet is a common file format that stores a table together with its column names and types. The output's *schema* is
its list of column names and types; `required: true` means the column may never be empty. Invaract
checks a job against the contract **before the job writes anything**. If the job reads something undeclared, writes somewhere
else, or produces the wrong columns, the write is refused with a message saying what was checked, what failed and how to fix
it. On Spark the check runs inside Spark just before each write. On Beam it runs once, just before the whole job is handed to
the engine. Here it would run inside Python just before each write.

### The engine-neutral model (the "IR")

Every engine describes a job in its own way. Spark has a *query plan*: a tree of the operations a query performs, worked out
before it runs. Beam has a *pipeline graph*: the job's steps, with arrows for the data flowing between them. Invaract translates
that description into one small engine-independent description of reads, filters, joins, groupings and writes, called the *IR*
(intermediate representation). All the checking code works on the IR, so it is written once for every engine.

The Spark adapter builds the IR from Spark's query plan; the Beam adapters build it from the pipeline graph. Plain Python has
neither (section 2), so this adapter would build a small IR *at each write*, from what it has seen so far:

```
Read data/in/orders.csv  ─┐
                          ├─►  ?? the team's Python code  ─►  Write data/out/report.parquet (parquet, overwrite: id long not null, amount long)
Read (any other read)    ─┘
```

The middle box is *opaque*: Invaract knows a step is there, not what it does. The same is true of a Beam job's own functions.
The reads are every dataset the job opened before this write. The write carries everything the writing function was handed.

### Capabilities, declarations and scenarios

A *capability* is one thing the checking code can do: "check that a write lands where the contract says", "check the
columns' types", "enforce a required filter". Each adapter's declaration gives every capability (31 today) one of four
statuses: `supported`, `partial` (some shapes only, with a note saying which), `unsupported`, or `not-applicable` (the
engine has no such thing). Some capabilities are marked *blocks if unsupported*: if a contract relies on one the adapter
cannot do, the job is refused (`UNSUPPORTED_CONTRACT_FEATURE`) rather than passed with that part unchecked. The scenarios
then hold each adapter to its own declaration.

## 2. The engine in plain words

There is no engine. A plain Python data job is a script, a file of Python code that the interpreter (CPython, the standard
Python program) runs from top to bottom:

```python
import csv, sys
import pyarrow as pa, pyarrow.parquet as pq

src = sys.argv[1] if len(sys.argv) > 1 else "data/in/orders.csv"
with open(src, newline="") as f:                              # read: happens now
    rows = [r for r in csv.DictReader(f) if int(r["amount"]) > 0]
schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("amount", pa.int64())])
table = pa.Table.from_pylist([{"id": int(r["id"]), "amount": int(r["amount"])} for r in rows], schema=schema)
pq.write_table(table, "data/out/report.parquet")              # write: happens now
```

Spark and Beam *describe* a job first and *run* it afterwards, and there is a moment between the two at which Invaract can
look. A script has no such moment. Nothing describes the job before it runs: no plan, and no *DAG* (directed acyclic graph,
the usual shape of a job description, steps joined by one-way arrows). The read happens when its line runs, then the
filtering, then the write. Seen from inside the interpreter, the job is a sequence of events in time:

```
time ──►
 open("data/in/orders.csv", "r")      csv rows read    filter (Python code)    pq.write_table(table, "data/out/report.parquet")
        ▲ a read Invaract can record                                               ▲ a write Invaract can stop
```

Where it can be stopped: at each write, while the script runs, by code that runs inside the same interpreter. A Python
exception raised there aborts the write, and the job's own error handling sees an ordinary error.

**Libraries change the picture.** The standard library's file work goes through Python's own `open`, which Python lets a hook
watch. pyarrow, the library behind nearly all Parquet in Python (pandas uses it too), does its file work in compiled C++ code,
which Python cannot watch (probe 2). The adapter therefore watches in two ways: a hook for everything that goes through
Python, and wrappers around the library functions that write. Section 3 shows each.

## 3. How the adapter would get hold of a job

Four things the engine produces were examined. The first was rejected as the main source; the other three together are the
design.

### The script's syntax tree (rejected as the main source)

- **What it is.** The script's text, parsed into a tree of its statements and expressions, which is what Python itself does
  before running it. It is called the *AST* (abstract syntax tree). It is like a sentence diagram of the code: it shows that
  line 11 calls `pq.write_table` with two arguments, but not what those arguments will hold when the line runs.
- **How we get hold of it.** `ast.parse(source)`. It is available *before* the job runs, so in principle a job could be
  refused before it starts. That would be a pre-submit gate, like Beam's.
- **What it contains.** Every call the script makes and its arguments *as written*: a fixed path such as
  `'data/out/report.parquet'`, a mode such as `'w'`, a header list such as `fieldnames=['rows']`.
- **What it leaves out.** Anything decided as the script runs: a path read from the command line (`sys.argv`), an environment
  variable, a configuration file, a loop, a function in another module, a table whose schema is built three lines earlier.
  Most real jobs take their paths from their command line or configuration, so most real jobs would have to be refused as
  uncheckable. That is why this is not the main source.
- **A real example.** Captured by probe 1 (`ast.parse` of the job above, printing each call), 2026-10-26:

```
line 11: pq.write_table(table, 'data/out/report.parquet')
    args: ["Name(id='table', ctx=Load())", "Constant(value='data/out/report.parquet')"]
line 6: open(src, newline='')
    args: ["Name(id='src', ctx=Load())"]
line 13: open('data/out/summary.csv', 'w', newline='')
    args: ["Constant(value='data/out/summary.csv')", "Constant(value='w')"]
line 14: csv.DictWriter(f, fieldnames=['rows'])
```

Reading it line by line:
- **line 11:** the write's destination is a fixed text (`Constant`), so it could be checked. Its data is `Name(id='table')`, a
  variable, so its columns cannot be read from the tree.
- **line 6:** the read's path is the variable `src`, filled from the command line. The tree cannot say what the job reads.
- **lines 13 and 14:** a CSV write whose destination, mode and header are all fixed text. This shape is checkable, but it is
  the exception.

### Runtime audit events (the hook)

- **What it is.** The interpreter raises an *audit event* for sensitive actions (shown on 3.11; Python has had them since 3.8
  by PEP 578, UNVERIFIED for the other versions here): opening a file, renaming or
  deleting one, starting another program, connecting to a database. A *hook* (a function registered with
  `sys.addaudithook`) is called with each event's name and details, and can stop the action by raising an error. It is like a
  building's door sensors: they report every door that opens, not what is carried through it.
- **How we get hold of it.** `sys.addaudithook(hook)`, called once when the interpreter starts (section 5 shows how that
  happens without changing the job). Events arrive *while the job runs*, at the moment of each action and before it happens,
  so a write can be refused before it starts (probe 3b). There is no function to remove a hook once it is added (probe 5
  listed `sys`'s audit functions: only `addaudithook` and `audit`).
- **What it contains.** For a file opened through Python (`open`, `pathlib`, `csv` and `json` on an opened file, pandas'
  CSV reader and writer, `shutil.copy`):
  - its path;
  - its mode, which says whether it is being read, replaced (`w`), appended to (`a`) or created only if absent (`x`);
  - the operating system's flags, which say the same.

  Renames, deletes and copies (`os.rename`, `os.remove`, `shutil.copyfile`) name both places. A `sqlite3` database names its
  file, and starting another program names the program.
- **What it leaves out.**
  - Everything written in compiled code: pyarrow's reads and writes raise no event at all (probe 2).
  - What data passes through an open file: no columns, no format.
  - What a `sqlite3` statement does to the database.

  The cost: columns and formats must come from the libraries themselves (next artefact), and pyarrow needs its own treatment.
- **A real example.** Captured by probe 2, 2026-10-26. A script did one of each kind of read and write while a hook printed
  every event (Python's own reads of its library files filtered out):

```
-- csv write via open()
  EVENT open ('data/in/orders.csv', 'w', 524865)
-- csv read via open()
  EVENT open ('data/in/orders.csv', 'r', 524288)
-- pyarrow parquet write to a path
-- pyarrow parquet read of a path
-- pyarrow parquet write to a folder (dataset)
-- pyarrow csv write to a path
-- pandas read_csv
  EVENT open ('data/in/orders.csv', 'r', 524288)
-- pandas to_csv
  EVENT open ('data/out/c.csv', 'w', 524865)
-- pandas to_parquet
  EVENT open ('data/out/d.parquet', 'w', 524865)
-- shutil.copy
  EVENT shutil.copyfile ('data/out/f.txt', 'data/out/g.txt')
-- os.rename / os.remove
  EVENT os.rename ('data/out/g.txt', 'data/out/h.txt', -1, -1)
  EVENT os.remove ('data/out/h.txt', -1)
-- sqlite3 insert
  EVENT sqlite3.connect ('data/out/db.sqlite',)
-- subprocess
  EVENT subprocess.Popen ('true', ['true'], None, None)
-- os.open + os.write (low level)
  EVENT open ('data/out/i.bin', None, 524353)
```

Reading it line by line:
- **`open (..., 'w', 524865)`** is a file opened to be replaced. The number is the operating system's flags, and 524865
  includes "truncate", which empties the file *at that moment*. This matters for D3.
- **The four pyarrow lines have no event under them.** pyarrow's reads and writes are invisible to the hook.
- **pandas** is visible where it opens files through Python: its CSV reader and writer, and, surprisingly, its Parquet writer,
  which opens the file in Python and hands it to pyarrow.
- **`shutil.copyfile`, `os.rename`, `os.remove`** name the places they change. **`sqlite3.connect`** names the database file;
  the `INSERT` itself raises nothing. **`subprocess.Popen`** names the program started.
- **`os.open`**, Python's lowest-level way to open a file, is seen too, with flags instead of a mode.

### The arguments of the library calls Invaract wraps

- **What it is.** Invaract replaces a library's write function (`pyarrow.parquet.write_table`, `csv.DictWriter`, ...) with a
  *wrapper*: a function of the same name that looks at its arguments, checks them, and then calls the original. The
  arguments are the most precise description of a write that exists, because they are what the library is about to write.
- **How we get hold of it.** By assigning the wrapper in place of the original in the library's module when the interpreter
  starts, before the job imports it. Each wrapped call arrives *while the job runs*, before anything is written (probe 3).
  The list of functions wrapped is the adapter's *catalogue*, like the Beam adapters' list of read and write steps.
- **What it contains.**
  - For a pyarrow write: the destination, and the table's full schema, with every column's name, exact type, nested types,
    and whether it may be empty (`not null`).
  - For pandas: the same, because pandas' Parquet write goes through `pyarrow.parquet.write_table` (probe 6).
  - For a `csv.DictWriter`: the column names (`fieldnames`), but no types, because a CSV file holds only text.
  - For a pyarrow read: the dataset's location and its schema.
- **What it leaves out.**
  - Any write function not in the catalogue. Probe 6 showed three pyarrow writes going through unseen and unrefused, because
    they write from compiled code and were not wrapped.
  - The types of a CSV or JSON write.
  - Anything about how the columns were computed.

  The cost: the catalogue must be kept complete for the libraries it claims, and anything outside it needs the next artefact
  as a safety net.
- **A real example.** Captured by probe 3, 2026-10-26: the job from section 2, run with a probe package that wraps
  `pq.write_table`, watches `open`, and holds a toy contract (output `data/out/report.parquet`, `id` an int64 that is
  required, `amount` an int64):

```
### a) conforming job
INVARACT-PROBE: read data/in/orders.csv
INVARACT-PROBE: pq.write_table data/out/report.parquet schema='id: int64 not null\namount: int64'
INVARACT-PROBE: schema ok at data/out/report.parquet: {'id': ('int64', False), 'amount': ('int64', True)}; reads so far: ['data/in/orders.csv']
job: wrote data/out/report.parquet 2 rows
exit status 0

### b) wrong location                      (data/out/elsewhere.parquet held the text "previous" beforehand)
INVARACT-PROBE: pq.write_table data/out/elsewhere.parquet schema='id: int64 not null\namount: int64'
INVARACT-PROBE: REFUSED OUTPUT_LOCATION_MISMATCH: write to data/out/elsewhere.parquet, contract says data/out/report.parquet
PermissionError: Invaract refused: OUTPUT_LOCATION_MISMATCH: write to data/out/elsewhere.parquet, contract says data/out/report.parquet
exit status 3                              (elsewhere.parquet still holds "previous")

### c) id written as text
INVARACT-PROBE: pq.write_table data/out/report.parquet schema='id: string not null\namount: int64'
INVARACT-PROBE: REFUSED OUTPUT_FIELD_TYPE_MISMATCH id: string != int64
exit status 3                              (report.parquet still has: id: int64 not null; amount: int64)
```

Reading it:
- **a)** The hook recorded the read of `data/in/orders.csv`. The wrapper saw the destination and the exact schema (`id`
  required, `amount` optional) before writing. These are the facts the contract above needs.
- **b)** A write to the wrong place was refused before pyarrow opened the file, and the old file was untouched.
- **c)** A column of the wrong type was refused the same way, and yesterday's good report survived.

### What each kind of read and write states (probes 2, 3, 6)

| How the job reads or writes | Seen by | Where | Columns | Format | Save mode |
|---|---|---|---|---|---|
| `open()` then `csv.writer` / `csv.DictWriter` | hook + wrapper | path | names only (`fieldnames`, or the first row) | csv | from the mode: `w` overwrite, `a` append, `x` error |
| `open()` then `json.dump`, or `f.write(...)` | hook | path | none | json from `json.dump`; none from a raw write | from the mode |
| `pyarrow.parquet.write_table` | wrapper only | path | exact, nested types and `not null` included | parquet | overwrite |
| `pyarrow.parquet.write_to_dataset`, `pyarrow.dataset.write_dataset`, `pyarrow.csv.write_csv`, `pyarrow.feather.write_feather` | **nothing unless wrapped** (probe 6) | path or folder | exact once wrapped | from the function | from `existing_data_behavior`, or overwrite |
| pandas `to_parquet` | hook + the pyarrow wrapper | path | exact, but every column may be empty unless the job passes `schema=` (probe 8) | parquet | overwrite |
| pandas `to_csv`, `to_json` | hook (+ a pandas wrapper, for columns) | path | names and pandas types once wrapped | csv, json | from `mode=` |
| `pyarrow.parquet.read_table`, pandas `read_parquet` | wrapper only | path or folder | exact | parquet | |
| `open()` for reading, `csv.reader`, pandas `read_csv` | hook (+ wrapper for the header) | path | names | csv | |
| `pathlib.Path.touch()`, `os.utime` (a marker such as `_SUCCESS`) | hook | path | none | | a new marker raises `open` with create flags; re-touching an existing one raises **only** `os.utime` (probe 9) |
| `shutil.copy`, `os.rename`, `os.remove` | hook | both paths | none | | a rename or move onto a declared output is the write landing there (probe 10); a delete of one is refused (D7) |
| Polars `write_*`, DuckDB `COPY ... TO` | **nothing** (probe 10); Polars methods can be wrapped, DuckDB's SQL cannot | path | read back from the file (probe 11) | parquet, csv, ... | refused outside the declared places only by the operating-system restriction (probe 13) |
| `sqlite3.connect` | hook | database file | none | | refused unless declared and trusted (D7) |
| another program (`subprocess`) | hook | the program's name | none | | refused unless trusted (D7) |
| compiled code outside the catalogue (database drivers, other C libraries) | **nothing** (UNVERIFIED for drivers) | | | | refused only by the operating-system restriction (D8) |

### The operating system's own restriction (Linux Landlock)

- **What it is.** Landlock is a feature of the Linux kernel (the core of the operating system) that lets a running program
  give up rights it does not need, for good. Here that means "from now on, this process may create or change files only
  beneath these folders". It is like handing in every key except the ones for the rooms you have business in. It sees every
  write, whichever library or language makes it.
- **How we get hold of it.** Three system calls, made through Python's `ctypes` when the interpreter starts, listing the
  contract's output folders plus any trusted ones. From then on, a write anywhere else fails with "Permission denied" inside
  whatever code attempts it (probe 7). Available on this machine (Landlock version 7, kernel 6.18). Whether a given production
  host has it is UNVERIFIED.
- **What it contains.** Nothing to inspect. It is a wall, not a window: it refuses writes outside the listed folders. It does
  not report them to Invaract, and it cannot check columns, formats or save modes.
- **What it leaves out.**
  - Everything about a write's content.
  - Any check finer than a folder: a single declared file inside a busy folder needs that folder's write rights.
  - Every operating system other than Linux.
- **A real example.** Captured by probe 7, 2026-10-26. Writes were allowed only beneath `data/out/allowed`, then a script tried
  three pyarrow writes, two of which Python's hooks cannot see:

```
INVARACT-PROBE: writes now allowed only beneath data/out/allowed
job: wrote data/out/allowed/report.parquet
job: data/out/sneaky.csv: PermissionError: [Errno 13] Failed to open local file 'data/out/sneaky.csv'. Detail: [errno 13] Permission denied
job: data/out/sneaky_ds: PermissionError: [Errno 13] Cannot create directory 'data/out/sneaky_ds'. Detail: [errno 13] Permission denied
```

Reading it: the permitted write landed. The two that probe 6 had shown slipping past Python (`pyarrow.csv.write_csv`,
`pq.write_to_dataset`) were refused by the operating system, with pyarrow's own "Permission denied" error. (A first run of
this probe also allowed `/tmp`, which holds the scratch folder, and so proved nothing. It was rerun without it.)

## 4. Key design decisions

### D1. Where does the check run?

- **Options.**
  - (a) Inside the interpreter, at each write, through the hook and the wrappers (section 3).
  - (b) Before the script runs, by reading its syntax tree.
  - (c) After the run, by looking at what files changed.
- **Recommendation.** (a). Its enforcement point (where the check can stop a job) is `in-engine-blocking`, the same as
  Spark's.
- **Why.** (b) cannot see most real jobs' paths (probe 1), so it would refuse nearly everything. (c) can only report damage
  already done. (a) sees each write with its real destination and data, and can stop it before it lands (probe 3).
- **What it costs.**
  - Like Spark, it judges one write at a time. A declared output the job never writes is found only when the interpreter
    exits, after the other writes have happened, and reported then (`check.outputsWritten` partial, as on Spark).
  - A job can catch Invaract's error and carry on. The refused write still does not happen, and the adapter can set a failing
    exit status at the end regardless (probe 3e).
- **What would change my mind.** Jobs that are mostly fixed paths (generated scripts, notebooks exported as scripts). Then a
  syntax-tree pre-check could be added *in front of* (a), to refuse some jobs before they start. It would not replace (a).

### D2. What is the adapter's description of a job?

- **Options.**
  - (a) Build an IR at each write: every dataset read so far, then one opaque step (the Python code), then the write with
    everything the writing function was handed.
  - (b) Try to follow data through the program, tagging each table as it is read and tracing it to the write.
- **Recommendation.** (a).
- **Why.** It uses only facts the probes showed are available, and the checking code already handles an opaque step with
  reads beneath it: the Beam adapters rely on exactly that. (b) breaks as soon as the code makes a new table from an old one,
  which is almost every line of a data job.
- **What it costs.** No filter, join or grouping is ever visible, so those rules are refused safely (section 7). Which reads
  fed which write is a guess (D4).

### D3. How is a refused write kept from damaging the old data?

- **Options.**
  - (a) Check at `open` only: the location and save mode there, the columns when the CSV writer is made.
  - (b) *Stage* every write to a declared output: open a temporary file next to it, check when it is closed, and move it into
    place only if the check passes.
  - (c) Check only writes whose columns are known before the file is opened (pyarrow), and refuse the rest.
- **Recommendation.** (b) for writes through Python's `open`. pyarrow and pandas Parquet writes need no staging, because the
  wrapper checks them before the file is opened.
- **Why.** Opening a file to replace it empties it at that moment (probe 2's flags). In probe 3d a CSV write to the declared
  place with the wrong header was refused, *after* `open` had already emptied yesterday's report: the file was left empty.
  Probe 4 staged the same write. The refusal then left the old report intact, and a correct write was moved into place on
  close.
- **What it costs.**
  - Invaract must replace Python's `open` for declared outputs (`builtins.open`, which is the same object as `io.open` and is
    what `pathlib` calls).
  - Appending to a staged file means copying the existing file first.
  - A reader sees the old file until the write finishes, which is usually better.
- **What would change my mind.** Very large appends, where the copy costs too much. Those could be checked at `open` instead,
  with the risk stated in the declaration.

### D4. Which reads count, and which write do they feed?

- **Options.**
  - (a) Every dataset read the process made before a write counts as feeding it.
  - (b) Only the reads made since the previous write.
  - (c) Tag the objects a read returns, and check whether the written object carries the tag.
- **Recommendation.** (a). Then, in a kit change of its own, stop requiring per-write read attribution of an adapter that
  cannot know which reads feed which write. That change would add a capability for it, or key scenario 20's second
  violation to an existing one.
- **Why.**
  - Python does not record which data went into which write.
  - (a) never invents a missing input for a valid job.
  - (b) would refuse every job that reads everything first and then writes twice.
  - (c) loses the tag at the first computed column.
- **What it costs.**
  - Scenario 18 conforms only because the kit's job reads each input just before its write.
  - Scenario 20 is expected to diverge: the stray write is blocked, but its second violation (`MISSING_INPUT`) is not
    reported, because the orders were read earlier.
  - With the strict option (`rejectUndeclaredInputs`), a job that writes two outputs from two inputs can have the second
    write refused for reading the first one's input.
- **Also: which reads count.** Probe 6's "reads so far" included the two `.pth` files Python reads at start-up. A dataset read
  is therefore any read through a catalogued reader, or any plain `open` of a file outside:
  - the interpreter's own folders (`sys.prefix`, the installed libraries);
  - the script's own code (`.py` files);
  - the places a platform names as trusted (D6).
- **What would change my mind.** If the maintainers prefer no kit change, the adapter ships with a documented divergence, and
  its write-up cannot be marked complete.

### D5. How do Python's names become contract locations?

- **Recommendation.**
  - A location is the path the job named, made absolute against the job's working folder. A relative declaration (such as
    `data/out/report.parquet`) then matches it by its ending, the existing rule in `LocationMatching`.
  - A file read from inside a folder the contract declares counts as reading that folder. This needs a small rule in the
    adapter, because the shared matching compares whole locations and a file inside a folder is not one.
  - A remote path pyarrow understands (`s3://bucket/x`) is used as given.
- **What it costs.** Checking that the file-inside-a-folder rule cannot let a read of `orders_backup/` count as `orders/`. It
  matches on whole folder names, the same `/`-boundary care the shared rule takes.

### D6. How do Python's types map to the contract's?

- **Recommendation.**
  - pyarrow schemas use the pyarrow type mapping the Beam Python adapter already has (`PyArrowSchemas`). It covers nested
    types, time zones and types with no neutral equivalent, so it moves to a place both adapters share. The Python side sends
    the schema as structured data, rather than as printed text, for the new adapter.
  - pandas writes go through pyarrow and are mapped the same way.
  - CSV writes have names and no types, so a contract that declares types for a CSV output has the write refused as
    unverifiable, never passed. JSON and raw text have neither.
- **Why.** CSV and JSON hold text. Whatever type a reader later infers is not something the job states.
- **What it costs.** pandas marks every column as possibly empty unless the job passes `schema=` to `to_parquet` (probe 8). A
  contract with `required: true` columns written through pandas is then refused until the team adds that schema, which the
  user guide must say plainly.

### D7. How are writes described, and what is refused as unverifiable?

- **Recommendation.**
  - Format comes from the function that writes (`write_table` is parquet, the `csv` module or `to_csv` is csv, `json.dump` is
    json). A raw `f.write` states none, so a contract that declares a format for that output has the write refused.
  - Save mode comes from the open mode: `w` is overwrite, `a` append, `x` error. `pq.write_table` always replaces the file.
    `write_to_dataset` follows its `existing_data_behavior` option.
  - Refused as unverifiable (`UNVERIFIABLE_WRITE`):
    - a delete of a declared output (a rename or move onto one is followed instead: the destination is where the write
      landed, probe 10);
    - a `sqlite3` database that is not a declared output, whose statements are not translated at first;
    - another program started by the job, unless the platform trusts it.
- **Why.** Each of these changes data in a way Invaract cannot read. Failing closed (refusing what cannot be checked, rather
  than letting it through) is Invaract's rule everywhere.
- **What would change my mind.** Many jobs that call harmless programs. `trustedSteps` exists for that.

### D8. What cannot be seen, and the policy

- **Recommendation.**
  - Writes inside compiled code that the catalogue does not wrap cannot be seen by Python at all (probe 6), and no Python-side
    policy can refuse them.
  - On Linux, offer the operating-system restriction (probe 7): writes outside the contract's output folders and the trusted
    places fail at the kernel. Make it opt-in at first, because jobs write to temporary folders and caches the platform will
    need to list. Make it the default once the trusted places are understood.
  - Elsewhere, the declaration says plainly that such writes are not seen.
- **Why.** Without the restriction, a compiled library is an unguarded door. With it, the door is locked, though Invaract
  cannot say who tried it: the job fails with the library's own "Permission denied" rather than Invaract's message.
- **What it costs.** A Linux-only guarantee, and a list of trusted places to maintain. Network writes (a database driver, an
  HTTP upload) are not covered either way. Landlock can also restrict network connections (UNVERIFIED in this survey).

### D9. How does Python reach the Java checker?

- **Options.**
  - (a) One helper process per job: Java, started in the background when the interpreter starts, and asked about each write
    over a pipe.
  - (b) `java -jar` once per write. That is about 0.9 s each, measured by the Beam Python adapter, so too slow for a script
    that writes many files.
  - (c) The checker compiled ahead of time into a program that needs no Java (GraalVM "native image"), shipped in the Python
    package.
- **Recommendation.** (a) first, (c) if Java on the workers is a real obstacle.
- **Why.** (a) reuses the existing checker and pays the Java start-up once, in parallel with the job's own start.
- **What it costs.** Java 11+ on every machine that runs a governed job. That is a much larger ask than Beam's "where jobs are
  submitted", and it is the biggest risk in this survey. If the helper cannot start, every write to a declared output is
  refused ("the check could not run").
- **What would change my mind.** A survey of the platforms' worker images. If most lack Java, start with (c).

### D10. How results are reported

- **Recommendation.**
  - The checker publishes the same validation events as every adapter, through the same notification sinks (the places events are sent: a log, a file, an HTTP
    endpoint, Kafka). The engine is
    named `python`, its version is the interpreter's (`3.11.17`), and `engineDetails` carries the script's path and the
    pyarrow and pandas versions.
  - The job's own command line is not recorded, because it can hold secrets.
  - A refused write raises `ContractViolation`, a kind of `PermissionError` (Python's standard error for "not allowed"), so a
    job's existing error handling treats it as a failed write. The process ends with a failing status even if the job catches
    it.

### D11. How CI proves it without secrets

- **Recommendation.**
  - A conformance adapter that writes each scenario as a small Python script and runs it with the real interpreter, the real
    package and the real checker. This is the same way the Beam Python adapter runs real Python pipelines in CI.
  - Recorded write descriptions as fast unit-test fixtures for the Scala side.
  - A matrix over Python 3.9 to 3.13, and over pyarrow and pandas releases. Each release re-runs probe 6's check that every
    catalogued function is still wrapped and no new write path appeared.
  - Nothing needs a cloud account.

### D12. Language, packaging and naming

- **Recommendation.**
  - A new sbt module, `python-adapter`: the Scala checker, its declaration and its conformance tests.
  - A Python package (`invaract-python`, PREDICTED name) holding:
    - the hook, the wrappers and the staging;
    - the `.pth` file that starts them;
    - the launcher (section 5);
    - the client for the helper process.
  - The pyarrow type mapping moves out of `beam-adapter` so both Python adapters use it.
- **What it costs.** More Python code than the Beam Python package, which was ~150 lines with no decisions in it. Here the
  hook, the catalogue and the staging *are* decisions. They need their own tests, lint and mutation testing (`mutmut`),
  because the repository's Scala gates do not reach them.

### D13. Classifying each output: a data asset or a readiness signal

The requirement: the contract confirms that the job wrote to each place it declared, and the team declares *what* it wrote
or did there. Two kinds matter most:
- a **data asset**: the governed table or files other teams consume;
- a **readiness signal**: a marker such as `_SUCCESS` or `READY`, or a small status file, that tells downstream jobs the asset
  is complete.

**What exists today.**
- The first half is there: `check.location` refuses a write to an undeclared place (`OUTPUT_LOCATION_MISMATCH`), and
  `check.outputsWritten` reports a declared output never written (`MISSING_OUTPUT`).
- The second half is declared but never checked. Every output may carry `type: DATA_ASSET` or `type: CONTROL`, and CONTROL's
  definition includes "a readiness/reconciliation signal" (`contract/.../ContractModel.scala`, `DatasetType`). But the role
  check covers inputs only. In `docs/CONTRACT_MODEL.md`'s words, "a declared output's type carries no role-consistency check
  of its own".
- CONTROL also covers processing calendars and watermarks, so a readiness signal cannot be told apart from other control
  data, and nothing records which asset a signal announces.

**Options.**
- (a) Keep `type: CONTROL` and add an output-only field naming the assets the signal announces, for example
  `readinessFor: [report]`.
- (b) Add `READINESS` as a fourth `type`.
- (c) Leave the contract as it is and check only that a `DATA_ASSET` output receives tabular data.

**Recommendation.** (a), with three checks, each a blocking violation:
1. **A data asset must receive data.** The write to a `DATA_ASSET` output must be a write with columns, such as a Parquet
   table. A touch, an empty file or a raw text write there is refused: "declared a data asset, but the job wrote a marker".
2. **A readiness signal must not receive the data.** A table written to a readiness output is refused. A marker, an empty
   file or a small status file passes. A status file may declare a `schema` like any output, and is then checked against it.
3. **A readiness signal comes after its asset.** The signal is refused unless every asset it names was written earlier in the
   same job and passed its check. A refused or never-written asset therefore never gets a "ready" marker. This is the
   check that protects downstream consumers most.

**Why (a).** `type` is a closed set, and contract parsers hard-fail on a value they do not know. A fourth type would make
every new contract unreadable by older Invaract releases. A new optional field is ignored by them. (c) leaves out the
ordering check, which is the valuable one.

**What plain Python can see** (probes 2, 3 and 9):
- The kind of each write is exact. A catalogued table writer (pyarrow, pandas, the `csv` module) means data. A touch, an
  `open` closed with nothing written, or a raw write of a few bytes means a marker or status. With staging (D3), the adapter
  also sees the final size and which writer produced the file.
- Order is exact, because a script runs one line at a time and the adapter checks every write in the same process. When the
  marker is written, it already knows whether the report was written and passed.
- **The trap probe 9 found.** Re-touching a marker that already exists raises only an `os.utime` event, with no `open`. The
  adapter must treat `os.utime` on a readiness location as a write of that signal. Otherwise a stale marker could be
  re-signalled unseen.
- A data asset written through compiled code the catalogue does not wrap is never seen as written, so its readiness signal is
  refused. That fails closed, as it should.

**Forecast for this adapter:** supported, High confidence for the facts, Medium for the whole, since no adapter exists. Other
engines differ, which is why this needs a capability every adapter declares:
- Spark checks writes one at a time in order, so the ordering check fits it (Medium).
- A Beam pipeline's steps run in parallel unless the job orders them, so a marker step could start before the asset's write
  finishes. Beam would likely declare the ordering check partial or unsupported (Low, UNVERIFIED).

**What it costs.** It is an engine-neutral change, made in its own pull request before the Python adapter:
- the contract field and its validation;
- the checks in `verification-core`, and two new violation types (for example `OUTPUT_ROLE_MISMATCH` and
  `READINESS_BEFORE_ASSET`);
- a new `Capability`, so that every adapter takes a position;
- new conformance scenarios: an asset written as a marker, a readiness signal written as a table, a signal before its asset,
  and a signal after its asset;
- the user documentation.

**Open questions for you.**
- May a readiness signal be only an empty marker, or also a small status file, or a row in a control table?
- Is a signal written by a *different* process than the asset (an orchestrator's next task, say) in scope? One process cannot
  see another's checks, so that would need Invaract to keep a durable record of checked writes. Until it exists, such a
  signal would have to be refused or left unchecked.
- Are there other things a team should declare it "did" at an output, such as deleting it, or publishing by moving a staged
  folder into place?

## 5. Switching it on without touching the job: external attachability

**Verdict: Partly.**

**The mechanism.** When Python starts, it reads every `.pth` file in its library folder (`site-packages`) and runs any line
that begins with `import` (`site.py`, lines 200 to 201). Installing the package drops one such file. The import does nothing
unless an Invaract setting is present in the environment. When one is, it adds the hook, the wrappers and, on Linux if asked,
the restriction. Then the job starts as usual. Probe 5 ran the unmodified job of section 2 this way:

```
pip install invaract-python                       # once, into the environment the job runs in (PREDICTED name)
INVARACT_CONTRACT=contracts/orders_report.yaml \
INVARACT_NOTIFY_CONFIG=/etc/invaract/notify.properties \
python report_job.py data/in/orders.csv           # the job's own command, unchanged
```

With no Invaract setting, the job ran exactly as before (exit status 0). With the setting and a wrong output, the write was
refused and the process ended with status 3. A child Python process started by the job was checked too, because it inherits
the environment and the same `.pth` file (probe 5).

Settings are environment variables rather than command-line flags because a script owns its own command line: its argument
parser would reject flags it does not know.

**For platforms that want a missing install to fail closed**, a launcher does the same and refuses to start the job if
Invaract cannot be installed:

```
invaract-python run report_job.py data/in/orders.csv     # PREDICTED
```

**The settings.** Each is `INVARACT_` followed by the neutral name in capitals with underscores:

| Setting | What it does | How it is given here | Possible? |
|---|---|---|---|
| `contract` | which contract to enforce (a file, or `registry://id@version`) | `INVARACT_CONTRACT` | Yes |
| `dryRun` | observe and infer a contract instead of enforcing | `INVARACT_DRY_RUN=true` | Yes |
| `notifyConfig` | where validation events are sent | `INVARACT_NOTIFY_CONFIG` | Yes |
| `registryUrl` / `registryClientClass` | where `registry://` contracts are fetched, and with what client | `INVARACT_REGISTRY_URL` / `INVARACT_REGISTRY_CLIENT_CLASS` | Yes |
| `locationMap` | maps `ref://` names in a contract to real locations | `INVARACT_LOCATION_MAP` | Yes |
| `orgPolicy` / `orgPolicyOverlays` | organisation-wide rules layered on the contract | `INVARACT_ORG_POLICY` / `INVARACT_ORG_POLICY_OVERLAYS` | Yes |
| `rejectUndeclaredInputs` / `rejectUndeclaredFields` | stricter checking | `INVARACT_REJECT_UNDECLARED_INPUTS` / `..._FIELDS` | Yes (inputs: see D4) |
| `computeFingerprint` / `staticDataQuality` / `roleConsistency` | optional analyses | `INVARACT_COMPUTE_FINGERPRINT` ... | Yes, but each is declared unsupported, so a run with one is refused |
| `failOnMissingOutput` | a declared output never written makes the process fail | `INVARACT_FAIL_ON_MISSING_OUTPUT=true` | Yes, as on Spark |
| `trustedSteps` | what a platform trusts not to carry governed data | `INVARACT_TRUSTED_STEPS=/var/log/*,gzip` | Yes, read as places and programs (decision 6) |

The Python side passes them to the Java checker unchanged, so they are read by the same engine-neutral settings code
(`ConfigSource`, `AttachSetup`) as on every adapter. The opt-in operating-system restriction (D8) is the one new switch. It
belongs to this adapter rather than being a neutral setting, because only an in-process adapter on Linux has it.

**How a job can get around it, and what is not possible.**
- `python -S` skips the `.pth` files, so nothing is installed. A job needing only the standard library then runs unchecked.
  (In probe 5, `-S` also hid pyarrow from that job, so it failed for its own reason.) `python -I` does *not* get around it
  (probe 5).
- A job run in a Python environment the platform did not prepare (its own virtual environment, a private copy of Python's library folder) has no package, and runs
  unchecked without a word. The launcher closes this.
- A job that deliberately restores the original functions, or writes through `ctypes` or its own compiled code, escapes the
  wrappers. The hook stays: no function removes one. The operating-system restriction, where on, stays as well. Invaract's
  checks are meant to catch mistakes, not a job written to evade them.
- A machine without Java cannot check anything. Writes to declared outputs are then refused.

## 6. What it would look like to a user

**The contract** is the one in section 1.

**The job** is the one in section 2, unchanged.

**Turning it on** (what the platform team does): install the package in the job's environment and set `INVARACT_CONTRACT`,
as in section 5.

**A passing run.** The job runs as before. Captured from probe 3a (the probe's own wording; the adapter's would differ):

```
INVARACT-PROBE: read data/in/orders.csv
INVARACT-PROBE: pq.write_table data/out/report.parquet schema='id: int64 not null\namount: int64'
job: wrote data/out/report.parquet 2 rows
```

A PASSED validation event goes to the configured sink.

**A blocked run (PREDICTED).** The job is pointed at the wrong output. Invaract's standard message, as the Beam adapter prints
it today, adapted to this adapter's plan:

```
Traceback (most recent call last):
  File "/srv/jobs/orders/report_job.py", line 11, in <module>
    pq.write_table(table, out)
invaract_python.ContractViolation: Contract violation: 'orders_report@1.0.0' rejected this transformation. Write aborted.
What the contract expects:
  input  'orders' at data/in/orders.csv
  output 'report' at data/out/report.parquet: id: long, amount: long (optional)
What the plan contains:
  Write(/srv/jobs/orders/data/out/elsewhere.parquet, format=parquet, mode=overwrite)
    ?? Python code (report_job.py)
      Read(/srv/jobs/orders/data/in/orders.csv, format=csv)
Why it violates the contract (1 violation):
  1. [OUTPUT_LOCATION_MISMATCH] contract declares output location 'data/out/report.parquet' but the plan writes to '/srv/jobs/orders/data/out/elsewhere.parquet'
How to correct it:
  1. Write to 'data/out/report.parquet' instead, or update the contract's declared output location to '/srv/jobs/orders/data/out/elsewhere.parquet' if this location change is intentional.
```

Nothing is written, any file already at that place is untouched, and the process exits with a failing status.

**Dry-run mode (PREDICTED).** With `INVARACT_DRY_RUN=true` and no contract, every write goes through. For each one Invaract
publishes a draft contract: the reads seen so far, and the write with its location, format, save mode and columns. Parquet
drafts carry types, CSV drafts only names, and JSON drafts none, each saying so. A summary merging the drafts is published
when the interpreter exits.

**Where the events go.** To the sink named by `INVARACT_NOTIFY_CONFIG` (a log, a file, an HTTP endpoint, Kafka), as on every
adapter. Each event names the engine `python`, its version, and the script.

## 7. How much of the suite it is expected to pass

### 7.1 The capability declaration, forecast

| Capability | What it means | Would declare | Why | Confidence |
|---|---|---|---|---|
| `read.batch` | reads are recognised | partial | Reads through Python's `open` raise an event (probe 2), and pyarrow and pandas reads are wrapped. A read inside other compiled code is not seen. | High |
| `write.batch` | writes are recognised and checked before they happen | partial | The same, for writes. Probe 6 showed three unwrapped pyarrow writes going through unseen. | High |
| `read.streaming`, `write.streaming` | the same for streams | not-applicable | Plain Python has no streaming construct; a loop of reads and writes is checked write by write. | High |
| `write.rowLevelDml`, `rules.dml` | row-level changes, and rules about them | unsupported | `sqlite3` statements raise no event (probe 2) and are not translated at first. A database the contract does not declare is refused. | High |
| `write.stateChange` | schema-only changes | unsupported | `sqlite3` schema changes are not translated; they are refused as unverifiable. | Medium |
| `check.inputExistence` | declared inputs read; undeclared reads refused | partial | Every read is recorded, but each write is assumed to be built from every earlier read (D4). | High |
| `check.location` | writes land in the right place | supported | Every recognised write names its destination before it happens (probe 3b). | High |
| `check.outputsWritten` | every declared output written | partial | Found only at exit, after the other writes, as on Spark. | Medium |
| `check.schema` | written columns and types match | partial | Exact for Parquet through pyarrow or pandas (probe 3c). Names only for the `csv` module. None for JSON and raw text, which are refused when the output declares columns. | High |
| `check.nestedTypes` | lists, structs and maps | supported | pyarrow schemas state them, and the shared pyarrow mapping already handles them. | Medium |
| `check.format` | the file format | partial | From the writing function; a raw `f.write` states none. | Medium |
| `check.saveMode` | append, overwrite, error | supported | Every recognised write states one: the open mode, or the function's own behaviour. | Medium |
| `check.catalogRegistration` | the output is a registered table | unsupported | Plain Python has no table catalog, so the requirement can never be met; such a contract is refused. | High |
| `rules.planShape` | required filter, join, group-by columns; no cross join | unsupported | Filters, joins and groupings are Python code. | High |
| `rules.custom` | an organisation's own rule types | unsupported | They judge row-level changes, which are not translated. | High |
| `analysis.*`, `lineage.columnLevel` | data-quality proofs, role consistency, fingerprints, function catalog, sensitivity, column lineage (which input columns each output column comes from) | unsupported | Each needs to see how columns are computed, which is Python code. | High |
| `lineage.boundaryResolution` | seeing past a checkpoint or cache | not-applicable | There is none; saving intermediate data is an ordinary write and read. | High |
| `policy.organizational`, `config.locationRefs`, `config.contractRegistry`, `reporting.notifications` | organisation policy, `ref://` locations, a contract registry, events | supported | Shared Java code, configured through the environment. | Medium |
| `config.zeroCodeInstall` | switched on without changing the job | partial | An installed package plus environment variables (probe 5). `python -S`, an unprepared environment, or deliberate evasion escapes it. Java is needed on the worker. | High |
| `reporting.dryRunInference` | draft contracts from real jobs | partial | Drafts are only as precise as the writer: types for Parquet, names for CSV. | Medium |
| `failClosed.unverifiableWrites` | refusing what cannot be checked | partial | Deletes, `sqlite3` and other programs are refused; renames and moves are followed (probe 2 shows each raises an event). Writes inside unwrapped compiled code are refused only by the Linux restriction (probe 7). | High |

**Enforcement point:** `in-engine-blocking`. Each write is checked inside the interpreter just before it happens, and a
refused one never lands. The job does run, and writes made before a refusal stand, as on Spark.

### 7.2 Scenario by scenario

#### Where data is read and written, and whether the contract itself is sound: strong. Every location is seen at the moment it is opened.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 2 | `wrong-output-location` | A job writes its report to a different folder than the contract promises, say a test folder in production. Invaract stops it before anything is written. | does the checks, write blocked: `OUTPUT_LOCATION_MISMATCH` | ✅ | The destination is the path the job passes to the write. A wrong one is refused before any byte is written, and an existing file there is left untouched (probe 3b). | High |
| 15 | `declared-input-not-read` | The contract says the job is built from the orders data, but the job never reads it. Invaract reports that the job is not using what the contract says it uses. | does the checks, write blocked: `MISSING_INPUT` | ✅ | Every read the job makes before the write is recorded (probe 2, probe 3a), so a declared input never opened is seen to be missing. | Medium |
| 16 | `undeclared-input-rejected` | With the strict option on, a job reads a dataset the contract never mentions. Invaract stops it, so nobody quietly pulls in an unapproved source. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | The second input's read is recorded like the first. This needs the adapter to tell dataset reads apart from the interpreter's own reads of its library files, which probe 6 showed are recorded too (D4). | Medium |
| 17 | `derived-from-excuses-an-input-the-output-is-not-built-from` | A contract has two outputs built from different inputs. The job writes both; the write of the first does not read the second one's input, and that is fine: Invaract does not demand it of that write. | does the checks, write allowed | ✅ | Reads that come before a write are counted for it, and an extra read never causes `MISSING_INPUT`, so this write passes. | Medium |
| 18 | `derived-from-requires-every-source-of-the-output` | The job's write of one output skips the input that output is built from and reads the other output's source instead. Invaract still reports the missing input. | does the checks, write blocked: `MISSING_INPUT` | ⚠️ | Conforms only because of how the kit's job is laid out: each extra write reads its input just after the main write. A real job that reads everything first would have the customers read counted for the report, and the missing input would go unnoticed. Python does not say which reads fed which write (D4). | Medium |
| 19 | `derived-from-rejects-a-read-of-another-outputs-source` | With the strict option on, a job's write of one output reads an input that belongs to the other output. Invaract stops it as an unapproved source for this output. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | Both inputs are read before the report is written, so the strict option sees the other output's source among the report's reads. | Medium |
| 20 | `an-undeclared-second-write-is-blocked` | A job writes its report and its audit as the contract says, then also copies the customers somewhere else. Invaract allows the declared writes and stops the extra one: a job writes only where its contract says. | does the checks, write blocked: `MISSING_INPUT`, `OUTPUT_LOCATION_MISMATCH` | ⚠️ | Expected to diverge. The stray write is blocked (`OUTPUT_LOCATION_MISMATCH`), but by then the job has read the orders for its first write, so the adapter will not also report the stray write's `MISSING_INPUT` that the kit expects. Closing this needs a decision about the kit (D4). | Medium |
| 21 | `declared-output-never-written` | A contract promises two outputs, a report and an audit. The job writes the report, correctly, but never the audit. Invaract refuses the job: an output the contract says the job writes, it must write. | does the checks, write blocked: `MISSING_OUTPUT` | ✅ | As on Spark: found when the interpreter exits, reported then, and with `failOnMissingOutput` the process exits with a failure status. Probe 3e showed that an exit hook can set the exit status. | Medium |
| 22 | `unrecognized-step-keeps-the-reads-beneath-it-visible` | A job passes its data through a step Invaract cannot interpret (a custom or unusual operation). The data still came from the declared input, and Invaract still sees that. | does the checks, write allowed | ✅ | Everything between the reads and the write is opaque Python code anyway, so an unusual step changes nothing about which reads are seen. | Medium |
| 23 | `unrecognized-step-does-not-hide-an-undeclared-read` | A job hides an unapproved source behind a step Invaract cannot interpret, hoping it will not be noticed. Invaract still finds it and stops the write. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | As for 22: the unapproved read is recorded when it is opened, whatever the code does with it afterwards. | Medium |
| 24 | `rejoining-branches-keep-the-read-beneath-them-visible` | A job sends its data down two paths and merges them back together, over and over, before writing it. The data still came from the declared input, and Invaract still sees that, however many times the paths split and rejoin. | does the checks, write allowed | ✅ | Python has no graph whose branches could hide a read. The adapter keeps a flat list of reads, so splitting and merging data changes nothing. | Medium |
| 25 | `rejoining-branches-do-not-hide-an-undeclared-read` | A job joins in an unapproved source and then sends the data down paths that split and rejoin, over and over. Invaract still finds the unapproved source beneath them and stops the write. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | As for 24: the unapproved read is on the list. | Medium |
| 54 | `untranslatable-write-fails-closed` | A job does something that changes data but that Invaract cannot understand. Invaract assumes the worst and stops it, rather than letting an unchecked change through. | does the checks, write blocked: `UNVERIFIABLE_WRITE` | ✅ | The kit's job would delete the output with `os.remove`, or change it through `sqlite3`. Both raise runtime events (probe 2) that the adapter refuses as unverifiable. | Medium |
| 55 | `invalid-contract` | The contract itself is broken, for example it contradicts itself. Invaract rejects it up front rather than checking jobs against nonsense. | does the checks, write blocked: `INVALID_CONTRACT` | ✅ | The contract is checked when the interpreter starts, by the shared code, before the job's own code runs. | Medium |

#### Columns and types: exact for Parquet written through pyarrow or pandas, names only for the csv module, none for raw text.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 1 | `conforming-write` | A team's job reads the declared input and writes the declared output with the declared columns. Invaract lets it run and records that the check passed. | does the checks, write allowed | ✅ | The read's columns come from the pyarrow read the job calls. The written columns, types and empty-or-not come from the table handed to the Parquet write, before it is written (probe 3a). | Medium |
| 3 | `missing-output-field` | The contract promises a customer_name column but the job's output no longer has it, which would break whoever reads the data. Invaract stops the write. | does the checks, write blocked: `MISSING_OUTPUT_FIELD` | ✅ | The table handed to a Parquet write lists every column, so a missing one is seen before the write. | Medium |
| 4 | `output-type-mismatch` | The contract says id is a number but the job turns it into text, which would break whoever reads the report. Invaract stops the write. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | The table states each column's type exactly. A text `id` was refused and the old file kept (probe 3c). | High |
| 5 | `extended-type-not-produced` | The contract promises a special type (time, JSON, interval or geography) but the job produces something else, perhaps because its engine has no such type. Invaract treats that as a mismatch and never waves it through. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | pyarrow has a time type and a duration type, and no JSON or geography type. A column of another type is a mismatch, never a pass, through the existing pyarrow type mapping. | Medium |
| 12 | `output-nullability-mismatch` | The contract says a column must always have a value, but the job can produce empty values there. Invaract stops it. | does the checks, write blocked: `OUTPUT_FIELD_NULLABILITY_MISMATCH` | ✅ | pyarrow marks each column as allowed to be empty or not (`not null`), and probe 3a read that flag before the write. | High |
| 13 | `undeclared-output-column-allowed-by-default` | A job adds an extra column the contract does not list. By default Invaract allows extras, because most teams add columns over time. | does the checks, write allowed | ✅ | Extra columns are visible in the table's schema; the shared check allows them by default. | Medium |
| 14 | `undeclared-output-column-rejected` | A strict team turns on the no-extra-columns option. A job that adds an unlisted column, perhaps by accident or perhaps personal data, is stopped. | does the checks, write blocked: `UNDECLARED_OUTPUT_COLUMN` | ✅ | The same schema shows the extra column, so the strict option catches it. | Medium |
| 30 | `nested-type-matches` | A column holding a list or a record (a list of tags, say) has the structure the contract describes. Invaract lets it through. | does the checks, write allowed | ✅ | pyarrow states nested types (`list<item: string>`), and the existing pyarrow type mapping already turns them into the contract's array, struct and map types. | Medium |
| 31 | `nested-type-mismatch` | The list of tags the job produces holds numbers where the contract says text. Invaract looks inside the structure, not just at the word list, and stops it. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | As for 30: a list of numbers and a list of text are different types in the schema. | Medium |

#### Formats and save modes: from the function that writes. Catalogs: none in plain Python, refused safely.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 26 | `output-format-matches` | The contract says the output is Parquet and the job writes Parquet. Invaract lets it through. | does the checks, write allowed | ✅ | The format comes from the function that writes: `pq.write_table` is Parquet. | Medium |
| 27 | `output-format-mismatch` | The contract says Parquet but the job writes CSV, which readers cannot parse the same way. Invaract stops it. | does the checks, write blocked: `OUTPUT_FORMAT_MISMATCH` | ✅ | The kit's CSV write would go through pyarrow's CSV writer, so the columns are still known and only the format differs. That writer must be wrapped: unwrapped, it is invisible (probe 6). | Medium |
| 28 | `output-save-mode-matches` | The contract says the job must add to the table (append) and it does. Invaract lets it through. | does the checks, write allowed | ✅ | `pq.write_table` always replaces the file, which is `overwrite`. | Medium |
| 29 | `output-save-mode-mismatch` | The contract says append, but the job overwrites the table and wipes its history. Invaract stops it. | does the checks, write blocked: `OUTPUT_SAVE_MODE_MISMATCH` | ✅ | The contract says append, and `pq.write_table` replaces the file. `open(..., "a")` and the `existing_data_behavior` option of `write_to_dataset` say append where a job means it. | Medium |
| 10 | `catalog-required-output-unregistered` | The contract says the output must be a registered table other teams can find, but the job writes to a bare folder. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Plain Python has no catalog of tables (a shared list of named tables), so a requirement for one can never be met. The kit expects a safe refusal, which is correct: the requirement is not silently waived. | High |
| 11 | `catalog-required-output-registered` | The job writes to the registered table the contract requires. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Plain Python has no catalog of tables (a shared list of named tables), so a requirement for one can never be met. The kit expects a safe refusal, which is correct: the requirement is not silently waived. | High |

#### Transformation-shape rules and analyses: refused safely, as declared.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 32 | `required-filter-column-present` | The contract requires every job to filter on a date column so nobody scans the whole history. The job filters on it. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 33 | `required-filter-column-missing` | Same rule, but the job never filters on the date column. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 34 | `forbid-cross-join-allows-a-keyed-join` | The contract bans accidental all-pairs joins, which can explode a table. The job joins on a matching column. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 35 | `forbid-cross-join-blocks-a-cartesian-product` | The job joins two tables with no condition, pairing every row with every row. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 36 | `required-join-columns-present` | The contract requires joins to use customer_id. The job joins on it. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 37 | `required-join-columns-missing` | The job joins on some other column, so the result could mix up customers. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 38 | `required-group-by-present` | The contract requires totals per customer. The job groups by customer. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 39 | `required-group-by-wrong-keys` | The job groups by region, then renames the column to customer so the output looks right. Invaract notices the real grouping and stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 42 | `static-data-quality-proves-a-constant` | The contract says total must be between 0 and 100 and the job sets it to a fixed 1. Invaract can prove that from the job alone, and says so. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 43 | `static-data-quality-cannot-prove-a-passthrough` | The contract gives a range for a column that is simply copied from an input. Invaract cannot know the input's values, so it reports not guaranteed, and does not block. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 44 | `static-data-quality-violation-blocks` | The job sets a column to a fixed value the contract's range rules out, such as -5 where 0 to 100 is allowed. Invaract proves it can never be valid and stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 45 | `role-source-contributing-conforms` | A dataset the contract calls a source feeds the output, as expected. Invaract finds that consistent. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |
| 46 | `role-control-contributing-contradicts` | A dataset the contract calls a control (a lookup or switch that should not supply data) ends up in the output. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | The adapter cannot do this check, so the kit expects a safe refusal before anything is written. It only has to call the shared check at start-up, the same check the Beam adapters already use. | Medium |

#### Skipped: not applicable to plain Python, or depends on something declared out of reach.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 6 | `row-level-delete-checked-as-write` | A job deletes rows from a table the contract does not mention. Invaract treats the delete as a write to that table and stops it. | skipped: 'write.rowLevelDml' is declared unsupported: sqlite3 UPDATE and DELETE are not translated; a sqlite3 database the contract does not declare is refused as unverifiable. | ➖ | sqlite3 changes to rows are not translated at first (D7), so the row-level change scenarios are skipped. | High |
| 7 | `row-level-delete-own-table-is-not-an-input` | A job deletes rows from its own output table. Invaract does not complain that the table counts as an unapproved input, because a delete has to look at the rows it changes. | skipped: 'write.rowLevelDml' is declared unsupported: sqlite3 UPDATE and DELETE are not translated; a sqlite3 database the contract does not declare is refused as unverifiable. | ➖ | sqlite3 changes to rows are not translated at first (D7), so the row-level change scenarios are skipped. | High |
| 8 | `unconditional-delete-forbidden` | The contract forbids deleting every row at once, and a job runs a delete with no condition. Invaract stops it. | skipped: 'write.rowLevelDml' is declared unsupported: sqlite3 UPDATE and DELETE are not translated; a sqlite3 database the contract does not declare is refused as unverifiable. | ➖ | sqlite3 changes to rows are not translated at first (D7), so the row-level change scenarios are skipped. | High |
| 9 | `filtered-delete-allowed` | Same contract, but the job deletes only the rows matching a condition. Invaract lets it through. | skipped: 'write.rowLevelDml' is declared unsupported: sqlite3 UPDATE and DELETE are not translated; a sqlite3 database the contract does not declare is refused as unverifiable. | ➖ | sqlite3 changes to rows are not translated at first (D7), so the row-level change scenarios are skipped. | High |
| 40 | `fingerprint-flags-nothing-for-a-deterministic-job` | With fingerprinting on, a job that gives the same answer on every run is reported as repeatable. | skipped: 'analysis.fingerprint' is declared unsupported: A fingerprint of reads and writes alone would miss the logic between them. | ➖ | No fingerprint is computed: one built from the reads and the write alone would leave out the logic between them. | High |
| 41 | `fingerprint-flags-a-generated-id` | A job adds a random unique id to every row. Invaract reports that column as different on every run, so teams know a re-run will not match. | skipped: 'analysis.fingerprint' is declared unsupported: A fingerprint of reads and writes alone would miss the logic between them. | ➖ | No fingerprint is computed: one built from the reads and the write alone would leave out the logic between them. | High |
| 47 | `lineage-passthrough-cast-and-constant` | For each output column Invaract can say where it came from: copied from this input column, converted from that one, or a fixed value that came from nowhere. | skipped: 'lineage.columnLevel' is declared unsupported: Needs to see how each output column is computed. | ➖ | Column lineage needs to see how each column is computed, which is Python code. | High |
| 48 | `lineage-through-join-and-filter` | A column that came through a join still traces back to the right input, and filtering rows does not add false sources. | skipped: 'lineage.columnLevel' is declared unsupported: Needs to see how each output column is computed. | ➖ | Column lineage needs to see how each column is computed, which is Python code. | High |
| 49 | `sensitivity-follows-a-cast` | An input column is tagged sensitive, say personal data. Output columns built from it, even converted to another type, carry the tag; unrelated ones do not. | skipped: 'analysis.sensitivityPropagation' is declared unsupported: Needs column-level lineage. | ➖ | Needs column lineage. | High |
| 50 | `boundary-filter-is-seen-through` | A job saves intermediate results midway (a checkpoint or cache). A filter applied before that point still counts for the rules. | skipped: 'lineage.boundaryResolution' does not apply to this engine: Plain Python has no checkpoint or cache; saving intermediate data is an ordinary write and read. | ➖ | Plain Python has no checkpoint or cache; saving data midway is an ordinary write and read. | High |
| 51 | `boundary-does-not-excuse-a-missing-input` | Saving intermediate results midway does not let a job get away with never reading a declared input. | skipped: 'lineage.boundaryResolution' does not apply to this engine: Plain Python has no checkpoint or cache; saving intermediate data is an ordinary write and read. | ➖ | Plain Python has no checkpoint or cache; saving data midway is an ordinary write and read. | High |
| 52 | `streaming-write-is-checked-like-a-batch-write` | A continuously running job (a stream) is checked like a one-off job: its source is the declared input and its destination the declared output. | skipped: 'read.streaming' does not apply to this engine: Plain Python has no streaming construct; a loop that keeps reading is a series of ordinary reads. | ➖ | Plain Python has no streaming construct of its own. | High |
| 53 | `streaming-write-to-the-wrong-location` | A streaming job writes to the wrong destination. Invaract stops it before it starts. | skipped: 'read.streaming' does not apply to this engine: Plain Python has no streaming construct; a loop that keeps reading is a series of ordinary reads. | ➖ | Plain Python has no streaming construct of its own. | High |

### 7.3 Claims no scenario can check

| Capability | What a test must show | How this adapter's test would show it | Forecast |
|---|---|---|---|
| `policy.organizational` | a contract that breaks an organisational policy is rejected when enforcement is set up | a script started with `INVARACT_ORG_POLICY` and a breaking contract fails before its first line runs | ✅ Medium |
| `config.zeroCodeInstall` | enforcement installed through configuration alone blocks a violating write | probe 5, made permanent: the unchanged job, the package installed, `INVARACT_CONTRACT` set, the wrong write refused | ✅ High |
| `config.locationRefs` | a `ref://` location is resolved through configuration and checked | a job run with `INVARACT_LOCATION_MAP` | ✅ Medium |
| `config.contractRegistry` | a `registry://` contract is fetched and enforced | a job run against the stub registry the other adapters' tests use | ✅ Medium |
| `reporting.dryRunInference` | dry-run lets a real write through and reports an inferred contract | a job run with `INVARACT_DRY_RUN=true` | ✅ Medium |
| `write.stateChange` | a schema-only change is checked | declared unsupported: `sqlite3` schema changes are refused as unverifiable, not checked | ❌ |
| `rules.custom` | a custom rule type blocks a write | declared unsupported | ❌ |

### 7.4 Totals

**25 of 55 by doing the check · 2 partly · 15 by refusing safely · 13 skipped.** For a user, the checks that matter most would
hold from the first version:
- reads and writes in the declared places;
- the written columns, types and required columns for Parquet, which is what most contracts describe;
- formats and save modes;
- a refused write that leaves the old data untouched.

The transformation-shape rules and the analyses are refused clearly rather than checked. The two partial rows come from one
limit: plain Python does not record which reads a write was built from.

## 8. What it would take

1. **Settle the unverified items** (small). These are: Java on the platforms' Python worker images; Landlock on their hosts;
   audit events on Python 3.9 to 3.13; database drivers and HTTP clients. This settles D8 and D9.
2. **Decide scenario 20** (small). This is a kit change in its own pull request, following D4, or an accepted divergence.
3. **The Scala checker** (medium). It is a new `python-adapter` module, a long-running helper that takes one write
   description at a time and answers with a verdict. It builds the IR of D2 and calls the shared verification, with the
   pyarrow mapping moved to a shared place. It is proven by recorded write descriptions and covered by the coverage and
   mutation gates. This is the first useful piece, since any language could then send writes to it.
4. **The Python package** (medium). This covers:
   - the `.pth` start-up, the hook and the catalogue's wrappers;
   - staging (D3) and the helper's client;
   - the launcher, and the attach test of probe 5 made permanent.

   It gets pytest tests, lint and `mutmut`.
5. **Conformance** (medium). A conformance adapter writes each scenario as a script and runs it with the real interpreter.
   It also needs the declaration, a baseline file and the capability matrix row. This is where the forecast is checked.
6. **The Linux restriction** (small to medium). It is opt-in (D8), with the trusted places, and tested with the pyarrow
   writes of probe 6.
7. **Release** (small). This means the PyPI package and the checker jar.
8. **Output classification** (medium, engine-neutral, best done before step 3). This is D13: the contract field, the three
   checks, the capability, the scenarios and the docs, in its own pull request. It also raises Spark's and Beam's positions on
   the new capability.

## 9. Risks and open questions

| Risk or question | Why it matters | How to settle it |
|---|---|---|
| Java on the machines that run Python jobs | without it, nothing is checked and declared outputs cannot be written | `java -version` in the platforms' worker images; if missing, D9 (c) |
| Writes inside compiled code the catalogue does not wrap | they pass unseen on every system without the Linux restriction | keep the catalogue complete per release (D11); make the restriction the default on Linux once proven |
| Which reads feed which write | missing inputs can go unnoticed in multi-output jobs; scenario 20 diverges | the kit decision in D4 |
| An environment the platform did not prepare | the job runs unchecked and nothing says so | the launcher (section 5); a platform check that the package is importable |
| Staging changes how files appear (atomically, at close) and costs a copy on append | could surprise a job that reads its own output while writing it | measure on real jobs; fall back to checking at `open` for appends (D3) |
| The interpreter's own reads look like dataset reads | the strict input option would refuse every job | the filter in D4, tested on the conformance runs |
| Library internals change between releases | a wrapped function renamed, or a new write path added, is a new unguarded door | the release matrix of D11 re-runs probe 6's check |

## 10. Evidence log

### Probes I ran

All on 2026-10-26, CPython 3.11.17, pyarrow 25.0.1, pandas 2.3.3 (the repository's Beam Python environment,
`/opt/invaract-beam-python`), Linux 6.18 on x86_64. They are not committed.

1. **Probe 1, the syntax tree.** `ast.parse` of the section 2 job, printing every call to `open`, `pq.write_table`,
   `csv.DictWriter` and `csv.DictReader` with its arguments. Output quoted in section 3: the read's path is a variable
   (`Name(id='src')`) and the written table is a variable (`Name(id='table')`).
2. **Probe 2, which actions raise audit events.** A hook printing the events `open`, `os.*`, `shutil.*`, `sqlite3.*`,
   `subprocess.*`, `socket.connect`, `urllib.Request` and `ctypes.*` (Python's reads of its own library files filtered) around
   one of each kind of read and write. Output quoted in section 3. pyarrow's Parquet read and write, its dataset write and
   its CSV write raised no event. pandas' `to_parquet` raised an `open` of the destination for writing.
3. **Probe 3, blocking inside the interpreter.** A probe package with a hook (refuse a write-mode `open` outside the
   declared output), a wrapper on `pq.write_table` (check destination and schema first), a wrapper on `csv.DictWriter`, and an
   exit hook setting status 3 after any refusal. Run with `PYTHONPATH` on the section 2 job and on variants:
   - (a) conforming: written, exit status 0.
   - (b) wrong destination: refused, the old file kept.
   - (c) `id` as text: refused, the old report kept.
   - (d) a CSV with the wrong header to the declared place: refused when the writer was made, *but the old file had already
     been emptied by `open`*.
   - (e) a job catching the refusal: the write did not happen, the job "finished normally", and the exit status was 3.
4. **Probe 4, staged writes.** A probe replacing `builtins.open` and `io.open` for the declared output with a temporary file
   checked on close. The wrong-header CSV of 3(d) was refused and `report.csv` still held its old contents. A correct CSV was
   checked and moved into place.
5. **Probe 5, attaching without touching the job.** A fresh virtual environment with an `invaract.pth` file
   (`import invaract_probe`), the probe active only when its environment variable is set.
   - The unmodified job with no variable: exit status 0, as before.
   - With the variable and a wrong output: refused, exit status 3.
   - `python -I`: still refused.
   - `python -S`: no `.pth` files ran (no "installed" line); the job then failed on its own missing pyarrow.
   - A child Python process started by the job: installed and refused too.
   - `dir(sys)` lists only `addaudithook` and `audit`: there is no function to remove a hook.
6. **Probe 6, libraries through the probe.** With the probe of probe 3 attached:
   - pandas `to_parquet` to the declared place went through the `pq.write_table` wrapper with its schema (`id: int64`,
     `amount: int64`, both optional).
   - pandas `to_csv` elsewhere was refused by the hook.
   - `pyarrow.csv.write_csv`, `pq.write_to_dataset` and `pyarrow.feather.write_feather` to undeclared places were **written**,
     unseen.
   - The "reads so far" list included the two `.pth` files Python read at start-up.
7. **Probe 7, Linux Landlock.** `landlock_create_ruleset` reported version 7. Writes restricted to `data/out/allowed` through
   `ctypes` system calls (444 to 446), then three pyarrow writes: the allowed one landed, and `pyarrow.csv.write_csv` and
   `pq.write_to_dataset` elsewhere failed with `[Errno 13] Permission denied`. A first run that also allowed `/tmp`, where the
   probe ran, wrote all three. That was my mistake and proves nothing. A plain `open` outside the allowed folder was refused
   the same way.
8. **Probe 8, pandas and required columns.** `df.to_parquet` wrote `id: int64; amount: int64` (both optional).
   `df.to_parquet(..., schema=pa.schema([pa.field('id', pa.int64(), nullable=False), ...]))` wrote `id: int64 not null`.

9. **Probe 9, readiness markers.** The same event-printing hook as probe 2, around four ways of writing a marker:
   - `pathlib.Path(...).touch()` of a new `_SUCCESS` raised `os.utime` and an `open` with create flags (524353);
   - touching it again raised **only** `os.utime ('data/out/report/_SUCCESS', None, None, -1)`;
   - `open("data/out/READY", "w").close()` raised `open (..., 'w', 524865)` and left a 0-byte file;
   - a small `json.dump` status file raised `open (..., 'w', 524865)` and left 26 bytes.

10. **Probe 10, where writes land.** The event-printing hook of probe 2, with Polars 2.1.0, DuckDB 2.0.0, openpyxl 3.1.5 and
    fsspec 2026.9.0 installed in a scratch environment:
    - "write a temp file, then `os.replace`" raised `open` of the temp file, then `os.rename (.report.tmp, report.parquet)`;
    - a `tempfile` then `shutil.move` raised `shutil.move` and `os.rename` naming the destination;
    - writes through a symbolic link and through `..` raised `open` with the path as written, unresolved;
    - Polars `write_parquet` and `write_csv`, DuckDB `COPY ... TO` and `pyarrow.dataset.write_dataset` raised **nothing**;
    - pandas `to_excel` raised `open` of the destination, plus openpyxl's own temporary files in `/tmp`;
    - fsspec on a local file raised `open`.
11. **Probe 11, schemas from calls and from files.** pandas, Polars, DuckDB, pyarrow (Feather) and fastavro wrote eight files,
    then a reader took each schema from the file itself: the Parquet footer, the Avro header, the Arrow file, or the first
    line of a CSV or JSON file. The output is quoted in "The four requirements", section 4.
12. **Probe 12, classifying outputs.** A prototype (a hook for `open`, `os.utime` and `os.rename` on the readiness location,
    plus reading the asset's files from disk) with a two-output contract. Six cases, quoted in "The four requirements",
    section 2. A `touch` of a new marker raised `os.utime` first and then `open`, so a real adapter must count one signal,
    not two.
13. **Probe 13, how far switching it on reaches.**
    - With the package's `.pth`: `multiprocessing` writes under both `fork` and `spawn` were refused.
    - With `PYTHONPATH` and a `sitecustomize.py` only, on the system Python: the write was refused, the distribution's own
      `/usr/lib/python3.11/sitecustomize.py` was hidden, and `python -I` wrote unchecked.
    - A child running in a virtual environment the job created wrote unchecked.
    - With the operating-system restriction set in the parent, every write outside the allowed folder was refused with
      "Permission denied": the same child, `sh -c "echo x > ..."`, Polars `write_parquet` and DuckDB `COPY`. A Polars write
      inside the allowed folder landed.

14. **Probe 14, database writes and the hook.** The event watcher of probe 2 (socket, HTTP and file events) around inserts into a
    local Postgres 16 through psycopg2 2.9.13, psycopg 3.3.6 (binary), pg8000 1.31.5 and pandas `to_sql` through SQLAlchemy
    2.1.6. All four rows landed:
    - psycopg2, psycopg 3 and `to_sql` raised no event;
    - pg8000 raised `socket.getaddrinfo` and `socket.connect` to `127.0.0.1:55432`, and nothing about the SQL.
15. **Probe 15, Postgres's own view.**
    - `EXPLAIN (FORMAT JSON, VERBOSE)` gave, for example, `WRITE INSERT gold.report; READ gold.orders filter=(orders.amount
      > 0)`. A `DELETE` with no `WHERE` showed a scan with no filter. `TRUNCATE` cannot be explained, and `CREATE TABLE AS`
      showed only its read.
    - sqlglot 30.25.0 named the target and kind of all five statements.
    - A role granted only `INSERT` on `gold.report` got `permission denied` for an insert into `gold.other`, a `DELETE`
      and a `TRUNCATE`.
16. **Probe 16, a psycopg2 wrapper.** `psycopg2.connect` wrapped so that each connection's `cursor_factory` is a checking
    cursor (`execute`, `executemany`, `copy_expert`), using sqlglot.
    - It refused an insert into `gold.other`, the same through pandas `to_sql`, and a `DELETE` with no `WHERE`.
    - It allowed inserts into `gold.report`.
    - A cursor created with `cursor_factory=RealDictCursor` bypassed it, and its insert landed.
    - `information_schema.columns` then gave `id bigint NO; amount bigint YES`.
17. **Probe 17, the BigQuery client.**
    - The real `google-cloud-bigquery` 3.46.2 ran against a recording HTTP session standing in for BigQuery, so its
      answers, the dry run's included, were made up by the probe.
    - The requests carried: the destination table, `writeDisposition`, the schema (every column `NULLABLE`, from pandas)
      and `sourceFormat: PARQUET` for a load; the table path and rows for a streaming insert; the SQL text for a query; and
      `dryRun: true` for a dry run.
    - A separate real call without credentials (17b) showed the hook receiving `http.client.send` with the request line
      `POST /bigquery/v2/projects/demo/datasets/gold/tables/report/insertAll` and the JSON body, before sending. BigQuery
      refused the call, since it had no credentials.

### Sources

- `contract/src/main/scala/com/invaract/contract/ContractModel.scala`, `DatasetType`: `DATA_ASSET`, `SOURCE`, `CONTROL`, with
  CONTROL defined to include "a readiness/reconciliation signal"; `docs/CONTRACT_MODEL.md`, "Input and Output Types" and
  "Role-consistency checks": only inputs are checked.
- `/usr/lib/python3.11/site.py`, lines 200 to 201: a `.pth` line starting with `import` is executed at start-up.
- `pathlib.py` (3.11), line 1044: `Path.open` calls `io.open`; probe: `io.open is builtins.open` is `True`.
- `pyarrow/parquet/core.py` (25.0.1), line 47: `_resolve_filesystem_and_path` is imported by name. Patching it in
  `pyarrow.fs` therefore would not reach the Parquet writer, so there is no single Python choke point for pyarrow writes.
  `pyarrow/_csv.pyx`, line 1600: `write_csv` is compiled (Cython).
- `docs/BEAM_PYTHON_ADAPTER.md`, D4: the Java checker answers in about 0.9 s per run.
- `verification-core`: `LocationMatching` (exact or `/`-boundary suffix matching), `InvaractConf.All` (the settings),
  `VerificationPipeline.requireOutputsWritten` and the Spark declaration's `check.outputsWritten` note (found at exit).
- `adapter-testkit`: `Scenarios.scala` (the 55 scenarios; scenario 20's expected `MISSING_INPUT` for a stray write),
  `Conformance.expectationFor` (partial is held to the neutral result), `AttestedClaims.requirements`.
- `docs-site/.../using-invaract-with-apache-beam.mdx`: the rejection message format used in section 6.

### Unverified

| Claim | The cheapest check |
|---|---|
| Slim Python worker images (`python:3.11-slim`, common Airflow and serverless images) have no Java | `docker run python:3.11-slim java -version`, and the same on the platforms' own images |
| Audit hooks behave as in probe 2 on Python 3.9, 3.10, 3.12 and 3.13 | run probe 2 on each |
| Database drivers (`psycopg2`, `pymysql`) and HTTP clients raise usable events, or none | probe 2 with each installed, against a local server |
| Landlock is available on the platforms' Linux hosts and container runtimes (inheritance by child processes is settled: probe 13) | `landlock_create_ruleset` version check on each |
| Writes to S3 or GCS (fsspec, boto3, google-cloud-storage) can be located by wrappers | a probe against a local S3 emulator |
| "Written in this run" by modification time is reliable on the platforms' shared and network file systems | probe 12 on each, comparing the file's time with the machine's clock |
| Reading the settings at the first write works for long-lived processes (notebook kernels, scheduler workers) | probe 5 with the variable set after start-up |
| What a real BigQuery dry run returns for INSERT, MERGE, CREATE TABLE AS and scripts | one dry run of each with a test project's credentials |
| psycopg 3, pg8000 and asyncpg can be wrapped as psycopg2 was | probe 16 for each driver |
| pandas-gbq goes through the `google-cloud-bigquery` client | read pandas-gbq's source; probe 17 with it installed |
| Landlock's network rules can refuse database and HTTP connections | probe 7 with `LANDLOCK_ACCESS_NET_CONNECT_TCP` |
| A hook cannot be removed by any means, `ctypes` included | read CPython's `sys.addaudithook` implementation; try to clear it through `ctypes` |
| Wrapping pyarrow lazily when the job first imports it (an import hook) works for `from pyarrow.parquet import write_table` | a probe with an import hook and that import form |
| The helper Java process starts within the job's own start-up time | time the assembled checker as a long-running process |
| Staging keeps file permissions and ownership that jobs rely on | probe 4 on a file with non-default permissions |
| Landlock's write rights cover `os.utime`, so a re-touch of a marker outside the allowed folders is refused | probe 7 with an `os.utime` call |
| Beam cannot order a marker step after an asset's write without the job saying so | read Beam's `Wait.on` documentation and a probe pipeline |

## Glossary

- **Interpreter (CPython)**: the program that runs a Python script, line by line.
- **Engine**: the system that runs a data job (Spark, Beam). Plain Python has none; the interpreter plays its part.
- **Data contract / schema**: the YAML file a job is checked against / the column names and types of a dataset.
- **Query plan, pipeline graph, DAG**: an engine's description of a job made before it runs. Plain Python has none of these.
- **IR**: Invaract's engine-neutral description of a job's reads, filters, joins, groupings and writes.
- **Opaque step**: a step Invaract knows is there but cannot see into (here, the team's Python code).
- **Capability / declaration / status**: one thing an adapter can or cannot do / its full list / supported, partial,
  unsupported or not-applicable.
- **Adapter**: the part of Invaract that knows one engine and translates its jobs into the IR.
- **Conformance suite / scenario**: the small engine-neutral jobs every adapter runs; an adapter conforms when each comes out
  as its declaration says.
- **AST (syntax tree)**: a script's code parsed into a tree of statements, available before it runs.
- **Audit event / hook**: an action Python announces (opening a file, starting a program) / a function called with each one,
  which can stop it.
- **Wrapper / catalogue**: a function put in place of a library's own to check its arguments first / the list of functions
  wrapped.
- **Compiled code**: code turned into machine instructions (pyarrow's C++), which Python's hooks cannot see into.
- **Data asset / readiness signal**: an output that is the governed data product itself (`type: DATA_ASSET`) / a
  marker or small status file telling downstream jobs an asset is complete (today `type: CONTROL`; D13).
- **Staging**: writing to a temporary file and moving it into place only once checked.
- **Landlock**: a Linux feature that lets a process give up file and network rights for good.
- **`.pth` file**: a small file in Python's library folder that Python reads, and may run a line of, at start-up.
- **Enforcement point**: where the check runs; here `in-engine-blocking`, inside the interpreter before each write.
- **Fail closed**: refuse what cannot be checked, rather than let it through.
- **Attach**: switch Invaract on for a job, ideally without changing its code.
- **Lineage**: which input columns each output column comes from.
- **Sink / source**: where a write lands / where a read comes from. A *notification sink* is where Invaract's events go.
- **Runner / transform** (Beam words, for comparison): the engine a Beam pipeline runs on / one step of a pipeline.
- **`OUTPUT_LOCATION_MISMATCH`**: a write lands somewhere other than the contract's output.
- **`MISSING_OUTPUT_FIELD`** / **`OUTPUT_FIELD_TYPE_MISMATCH`** / **`OUTPUT_FIELD_NULLABILITY_MISMATCH`**: a declared output
  column is missing, has the wrong type, or may be empty where the contract says it may not.
- **`UNDECLARED_OUTPUT_COLUMN`**: with `rejectUndeclaredFields`, the output has a column the contract does not declare.
- **`MISSING_INPUT`** / **`UNDECLARED_INPUT`**: a declared input is not read / an undeclared one is.
- **`MISSING_OUTPUT`**: a declared output is never written.
- **`OUTPUT_FORMAT_MISMATCH`** / **`OUTPUT_SAVE_MODE_MISMATCH`**: the write's format or save mode differs from the contract's.
- **`UNVERIFIABLE_WRITE`**: an action changes data in a way Invaract cannot check, so it is refused.
- **`UNSUPPORTED_CONTRACT_FEATURE`**: the contract relies on something this adapter declares it cannot check, so it is refused.
- **`INVALID_CONTRACT`**: the contract itself is malformed.

## Retrospective

_Not yet: no adapter exists._
