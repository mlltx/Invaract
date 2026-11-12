# Working on the `invaract` Python Package

A contributor's guide to `pythonio-adapter/python`, the package Python jobs read and write through. It covers how the
package is laid out, how a read and a write flow through it, its conventions, how to run its tests, and how to make the
changes that come up most often. How to *use* the package is the user guide (docs-site, "Use Invaract with Python Jobs",
and the "Python Package API" reference). How the package talks to its checker is `docs/PYTHONIO_PROTOCOL.md`. The
adapter's design, and what was built when, is `docs/PYTHON_IO_ADAPTER_DESIGN.md` and `docs/PYTHONIO_ADAPTER.md`.

## The one rule

**The package never decides whether something passes; the checker does, and nothing is published unless it said yes.**
The package describes each read and write to the checker, stages each output where no reader sees it, and publishes them
all only when the run finished and every answer was yes. Any change that could let data reach an output's location
without a passing `written` and `finish` is wrong, however convenient. When the package cannot do something it was
asked (a format it does not write, a location it does not understand, an extra that is not installed), it raises
`CheckNotRun` and publishes nothing; it never falls back to something close.

## Layout

The public interface is `invaract/__init__.py` alone (`job`, `run`, `read`, `write`, `check`, `reads`, `writes`, `Run`,
`ContractViolation`, `CheckNotRun`), typed and documented there; every module whose name starts with `_` is internal.
From the top down:

| Module | Does | Uses |
|---|---|---|
| `__init__` | The public functions, with their type hints and the docstrings the user documentation follows. | `_run` |
| `errors` | `ContractViolation` (the checker refused; exit status 1) and `CheckNotRun` (no check could be made; exit status 2). | |
| `_run` | A run's lifecycle (`Run`: open, read, write, publish or discard, finish), `invaract.check`, `@invaract.job`. The only module that talks to the checker. | everything below |
| `_protocol` | Every request the package sends, built in one place, and the protocol version. | |
| `_checker` | The checker process: finding its command (`INVARACT_CHECKER`, the bundled native program, the bundled jar with Java), starting it, one request and reply at a time. | `_settings` |
| `_settings` | The `INVARACT_*` settings forwarded to the checker by their neutral names, and the package's own variables (`INVARACT_CHECKER`, `INVARACT_LINT*`). | |
| `_codescan` | `INVARACT_LINT`: which files are the job's own code, scanning them with invaract-lint (an optional dependency, `invaract[lint]`) at the run's start and, for modules imported since, at its finish, and the `scan` request. | `_protocol`, `_settings` |
| `_frames` | Tables in and out: what `write` is handed, as an Arrow stream; what `read` returns, as the frame library asked for; counting empty values. | `_pandas` |
| `_pandas` | pandas' own conversions: a frame written as the contract declares where that changes no value, named index levels, `dtype_backend`, the notes a refusal carries. | `_types` |
| `_checks` | Runtime data quality: each output's data checks, evaluated over every batch staged. | |
| `_types` | The contract's column types as Arrow types, for data the package builds (CSV, JSON, Excel, database columns, pandas conversions). | |
| `_datasets` | A dataset by its location and format: how it is read, the catalog it is registered in, and which `Output` writes it. The one place that dispatches on format and location. | the modules below |
| `_outputs` | `Output`, what every kind of output is to a run; `Layout`, its partitioning; `Published`, what its publish put in place. | |
| `_files`, `_formats`, `_csv` | Plain files: staging and publishing them (`RenamedOutput` locally and on HDFS; `CopiedOutput` on object storage, with `_SUCCESS` last); reading and writing JSON lines, ORC, Avro, Feather, Excel; CSV options. | `_storage` |
| `_storage` | Where a location lives: local, HDFS, S3, GCS, Azure, by its URI. | |
| `_tables` | Delta Lake and Iceberg tables, staged uncommitted and committed in one commit at publish. | `_storage` |
| `_jdbc`, `_postgres`, `_sqlite` | Database tables (`jdbc`): what the databases share, and each database, staged in a table or file no reader sees and moved into place in one transaction. | |
| `_optional` | Importing a library an extra installs, with the `pip install "invaract[<extra>]"` hint when it is missing. | |

There are no import cycles: `_run` uses the rest, `_datasets` uses the modules that read and write each kind of
dataset, and those share `_frames`, `_types`, `_outputs`, `_storage` and `_optional`. No module imports an optional
library (pandas, Polars, deltalake, pyiceberg, fastavro, openpyxl, the ADBC drivers) when the package is imported: each
is imported where it is first needed, through `_optional.require`, and `_pandas.is_frame` recognises a pandas frame by
its type without importing pandas, so `import invaract` needs pyarrow alone.

## How a read and a write flow

**`invaract.read("orders")`**: `_run.current()` finds the run → `Run.read` looks up the declared input in the contract
`open` returned, and works out the format (the job's `format=`, else the contract's, else the location's extension) →
`_datasets.read_table` (or `read_batches`, with `chunksize`) reads it, only the declared columns, typed as declared where
the format holds no types → `_protocol.read` describes it, with each column's empty values → the checker answers → the
table is returned as the frame kind asked for (`_frames.as_frame`).

**`invaract.write(frame, "report")`**: `Run.write` turns the frame into an Arrow schema and a stream of batches
(`_frames.stream`; a pandas frame is first conformed to the declared types by `_pandas.to_arrow`) → `Run._target` works
out where and how it goes → `_protocol.write`, the pre-check, before anything is staged → `Run._output` makes the output
on its first write (`_datasets.output`) → `Output.stage` writes the batches where no reader sees them, counting each
column's empty values and stopping at a required column's first one, while `_checks` evaluates any data checks →
`_protocol.written`, the verdict on the whole write.

**The run's end** (`Run.__exit__`): if the job returned and nothing was refused, `finish`; if its reply says to publish,
`Run._publish` calls every output's `publish` in the order first written, then tells the checker what each put in place
(`_protocol.published`: the rows `Run` counted as they were staged, the time staging and publishing took, and the
output's own `Published`), and the checker publishes a `WRITE` event for each and the run's `JOB_SUMMARY`; then every
output's `forget_previous`. If an output fails to publish, those already published are put back where their storage
allows (`restore`), the checker is told what stays published, and the failure is raised. Otherwise, and whenever anything
fails before publishing, every output's `discard` and a `finish` with `completed: false`, each step taken even if an
earlier one fails (`Run._end_unpublished`).

## Conventions

- **Which error.** `ContractViolation` is raised only from a checker's refusal (`Run._ask`). `CheckNotRun` is for
  anything that stops the check being made or the write being made as checked. `ValueError` is for a wrong argument
  (`TypeError` for a frame that cannot be one table: two columns of one name, a column of mixed kinds), raised before
  anything is checked. `FileExistsError` is for an `error` save mode meeting data. Every message says what
  the job did and what to do about it, in the job's terms (`invaract.write('report')`, not a module name).
- **An `Output`** (`_outputs.Output`) stages data so no reader sees it (`stage`), makes it the output's data in one step
  as far as the storage allows (`publish`), keeps what it replaced until every output of the run has published
  (`forget_previous`), and can always be undone before publishing (`discard`). `stage` returns each column's empty values,
  and writes a required column not-null (`_frames.with_required`), so readers see the contract's schema. `publish` returns
  what it put in place (`_outputs.Published`: files, bytes, a Delta version or Iceberg snapshot, each where the storage
  says) or `None` when it put nothing there, which is what the output's `WRITE` event reports. `restore` undoes a
  publish where the storage can (a rename, `RenamedOutput`) and says whether it did; the default, for a commit or a copy
  that cannot be taken back, is `False`.
- **Staging is named after the run** (`run_id`), so two runs never collide and a crashed run's leftovers are recognisable.
- **Docstrings** say what a function gives or does, in the terms of the job and the contract, and why where it is not
  obvious; the decisions they cite (decision 3, D5, milestone 6) are in `docs/PYTHON_IO_ADAPTER_DESIGN.md` and
  `docs/PYTHONIO_ADAPTER.md`.
- **The public interface's type hints** are checked against the values the package takes (`tests/test_api.py`); a new
  argument or value updates both, and the user documentation.

## Running the tests

Every test runs a real checker; none mocks it. Build the checker jar once, then run the suite:

```bash
cd pythonio-adapter && sbt assembly          # the checker: target/scala-2.12/invaract-pythonio-checker-0.1.0.jar
cd python
python -m venv .venv && . .venv/bin/activate
pip install -e ".[test,avro,excel]" duckdb     # pytest, jsonschema, pandas, Polars, fastavro, openpyxl
python -m pytest
```

The tests find the jar themselves (with `JAVA_HOME`'s Java, or `java` on the `PATH`); `INVARACT_TEST_CHECKER` names
another checker, such as a native build (`docs/PYTHONIO_ADAPTER.md`, "Milestone 0", and CI's `pythonio-native-checker`
job show how one is built). Some suites need more, and are skipped
without it:

| Suite | Needs | Set |
|---|---|---|
| `test_tables.py` (Delta, Iceberg, S3 through moto) | `pip install -e ".[delta,iceberg]" "moto[server]" boto3` | `INVARACT_TEST_TABLES=true` makes a missing library fail the suite rather than skip it |
| `test_postgres.py` | A PostgreSQL server you can create tables in, and `pip install -e ".[postgres]"` | `INVARACT_TEST_POSTGRES=postgresql://user@host:port/database` |
| `test_sqlite.py` | `pip install -e ".[sqlite]"` | |
| `test_storage.py` | Emulators or a cluster: fake-gcs-server, Azurite, a Hadoop single-node cluster | `INVARACT_TEST_GCS`, `INVARACT_TEST_AZURE`, `INVARACT_TEST_HDFS` (each `http://host:port`, `hdfs://host:port`); `INVARACT_TEST_STORAGE_REQUIRED=true` fails rather than skips |

Every test validates each request and reply against the protocol's JSON Schema, and each event it reads against the
events' schema (`tests/conftest.py`), so a test that passes has also kept to both.

**In CI** (`.github/workflows/test.yml`): `pythonio-python` runs the suite against the native checker with the oldest
supported libraries, the oldest with the table extras, and the newest (the last two against a PostgreSQL server);
`pythonio-storage` runs `test_storage.py` against real storage services; `pythonio-native-checker` builds the native
checker and runs every conformance scenario as a real Python job through the package (`tests/conformance_job.py`);
`release-python.yml` builds every wheel and runs the example job with each.

## Common changes

**Add a file format.** Give it a reader and a writer in `_formats` (a writer has `write_batch` and `close`), add it to
`_formats.FORMATS` and, for reading, to `DATASET` (pyarrow's datasets read it) or `FOREIGN` (read file by file), and its
extension to `_BY_EXTENSION`. If it needs a library, add an extra to `pyproject.toml` and import it with
`_optional.require`. Then tests in `test_formats.py`, a row in the user guide's formats table and the API reference's,
and, if no other engine names it, a word with the contract's format list.

**Add a database.** A module beside `_postgres` and `_sqlite` with the functions `_jdbc`'s docstring lists, named in
`_datasets._database`, an extra for its driver, and a `test_<database>.py` that runs against a real server in CI.

**Add a kind of output** (a new table format, a new store). Subclass `_outputs.Output`: `stage` writes where no reader
sees it and returns each column's empty values (`_frames.take` does the counting); `publish` puts it in place in one step
as far as the storage allows and returns `_outputs.Published` (or `None` when it put nothing there); `discard` deletes what
is staged; `restore` undoes a publish only if the storage really can (leave the default `False` otherwise: the run then
reports the output as published, which is the truth). Return it from `_datasets.output`, and test it in a run that fails
part way through publishing (`tests/test_run.py`'s `_failing_publish`).

**Add a kind of storage.** `_storage.filesystem` gives pyarrow's file system for a location's scheme; a scheme with no
rename publishes plain files by copying (`_files.output` decides which). Delta and Iceberg reach storage through their
own libraries, with each library's standard settings: never add an Invaract setting for a credential.

**Add a setting.** Settings are engine-neutral and read by the checker, so a new one starts in verification-core's
`InvaractConf` (CLAUDE.md, "External Attachability Requirement"). Here, add it to `_settings.SETTINGS` (the checker's
`PythonIoSettingsSpec` fails until the two agree) and to the protocol schema's `Settings` (`tests/test_protocol.py`
fails until they agree).

**Change what a request says.** See `docs/PYTHONIO_PROTOCOL.md`, "Changing the protocol": the checker, `_protocol`, the
schema and a recorded case, in one change.

**Add a public argument.** `__init__.py` (the signature, its type hint and its docstring), `_run`, a test of the
argument, `tests/test_api.py` if it takes fixed values, and the user guide and API reference.

## Releasing

`release-python.yml` builds the wheels (one per platform with the native checker, one pure-Python with the jar) and the
lint's, and publishes them on a `python-v<version>` tag; docs/RELEASING.md, "The Python packages", has the steps. The
version is `invaract.__version__`.
