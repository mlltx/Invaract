# invaract-lint

Finds the reads and writes a Python job makes outside the [Invaract](https://github.com/mlltx/Invaract) package. The package
checks every read and write that goes through it; this lint is the guard against the ones that do not
(docs/PYTHON_IO_ADAPTER_DESIGN.md, section 7). It has no dependencies.

```bash
pip install invaract-lint
invaract-lint jobs/                        # or: python -m invaract_lint jobs/
```

On the example job as it was before it adopted Invaract (`examples/orders_job` beside the package, and
`tests/test_cli.py`'s `BEFORE`):

```
jobs/orders/build.py:2:5: IVR002 `paid[['order_id', 'status']].to_parquet` writes data outside invaract: write it with invaract.write(<frame>, <name>), or mark the line `# invaract: allow`
jobs/orders/load.py:4:14: IVR001 `pd.read_parquet` reads data outside invaract: read it with invaract.read(<name>), or mark the line `# invaract: allow`
jobs/orders/load.py:8:12: IVR001 `pd.read_csv` reads data outside invaract: read it with invaract.read(<name>), or mark the line `# invaract: allow`
jobs/orders/main.py:8:5: IVR002 `paid.merge(load.customers(), on='customer_id').to_parquet` writes data outside invaract: write it with invaract.write(<frame>, <name>), or mark the line `# invaract: allow`
4 read(s) or write(s) outside invaract
```

It exits 0 when it finds nothing, 1 when it finds something not marked, and 2 when a file cannot be read or parsed.

| Code | Flags |
|---|---|
| `IVR001` | a read: `pandas.read_*`, Polars' `read_*` and `scan_*`, pyarrow's `parquet`/`csv`/`feather`/`json`/`orc`/`ipc`/`dataset` readers, `duckdb.read_*`, numpy's `load*`, and other libraries' readers |
| `IVR002` | a write: those libraries' writers; a frame's `to_csv`, `to_parquet`, ... and `write_*` given a destination, `sink_*`; `open` and the other openers (`gzip.open`, `zipfile.ZipFile`, `tarfile.open`, `os.open`, ...) in a mode that writes, or one not spelled out; a `Path`'s `write_text`, `write_bytes`, `open("w")`, `touch`, `rename`; copies, moves and deletes (`shutil`, `os.rename`, `os.remove`, ...) |
| `IVR003` | a connection: database drivers' `connect`, `sqlalchemy.create_engine`, cloud and storage clients (`boto3`, `s3fs`, `gcsfs`, `adlfs`, `fsspec`, pyarrow's filesystems), HTTP clients |
| `IVR004` | SQL that reads or writes files, passed to `sql`, `execute` or `query`: `COPY`, `ATTACH`, `EXPORT DATABASE`, `VACUUM INTO`, `read_parquet(...)` and the like, a quoted file after `FROM`; and SQL not spelled out that is run on a DuckDB or SQLite connection |
| `IVR005` | another program started: `subprocess`, `os.system`, `os.exec*` |
| `IVR006` | code the scan cannot follow: `exec`, `eval`, `getattr` and imports by a name built at run time, native code loaded with `ctypes` |

It reads the code as Python does (`resolver.py`): a name means what the nearest scope that binds it says, so
`import pandas as pd`, `from pandas import read_csv as rc`, `reader = pd.read_csv`, `getattr(pd, "read_csv")` and
`importlib.import_module("pandas")` are all followed, and a parameter or a function of the job's own that happens to be
called `open` is not mistaken for the builtin. A function passed or stored without being called
(`map(pd.read_csv, paths)`) is found where it is referred to. A mode, a flag or a SQL statement kept in a variable is
followed to the text it was assigned.

What is certainly not data at rest is left alone: a buffer in memory (`io.BytesIO()`), the console and the null device,
a scratch file in the temporary folder, a database in memory, a frame turned into text (`df.to_csv()` with no
destination), a file opened for reading, a name used only as a type (an annotation, `isinstance`), and the word `copy`
in a SQL value, comment or column name.

**Keeping a call on purpose.** Mark its line (any line of the call) with `# invaract: allow`, ideally with the reason:

```python
lookup = pd.read_csv("lookup/regions.csv")  # invaract: allow - a table shipped with the job, not a dataset
```

**What it cannot follow, it reports.** It never runs the code, so a call reached through a name built at run time
(`getattr(df, "to_" + kind)`), an import by such a name, `exec` and `eval`, and native code are each a finding of their own
(`IVR006`): the scan fails closed, and a reviewer decides. What it does not see at all is what such a line then goes on to
do, SQL handed to a function that runs it on a connection made elsewhere, and code the job fetches and runs. The
`invaract` package's strict mode (`INVARACT_STRICT`) watches the run for those, using this package's `catalogue.py` as its
list of what writes.

**As data**, `--format json` prints the whole scan as one JSON object: its `status` (`findings`, `not-scanned`,
`exceptions` or `clean`), the `files` scanned, `counts` per category split into `findings` and `exceptions`, every finding
(the ones a marker keeps included, as `allowed: true`) and the `errors` for files it could not parse. Each finding has:

| Field | What it says |
|---|---|
| `path`, `line`, `column`, `endLine`, `endColumn` | Where the call starts and ends (`endColumn` is just after its last character). |
| `code`, `category`, `rule`, `detail` | `IVR001`-`IVR006` and its word (`read`, `write`, `connect`, `sql`, `process`, `dynamic`); the rule that matched (`pandas-read`, `frame-write`, `open-write-mode`, `database-connect`, `file-sql`, ...); and what made it count when the name alone does not (`mode 'w'`, `COPY`). |
| `call`, `resolved`, `scope` | The call as written, the name it stands for through the imports (`pandas.read_csv`; `null` for a frame's method), and the function or class it is in (`Loader.load`, or `<module>`). |
| `snippet`, `target` | The whole call on one line, and the path or URL it names when the code spells one out (never for a connection or SQL). A password in a URL is masked in both. |
| `allowed`, `reason` | Whether a `# invaract: allow` marker keeps it, and what the marker says after it. |
| `id` | Identifies the call across runs: built from the file, the rule, the function and the call, not the line. |

From Python, `scan_source`/`scan_file` return every finding and `check_source`/`check_file` the ones not marked; `scan(paths)`
returns a `Scan` with `status`, `counts()` and `to_dict()`.

**When a job runs**, the `invaract` package can run this scan itself as the run starts, publish the result as a `CODE_SCAN`
event, and, with `INVARACT_LINT=enforce`, refuse the job for a call no marker explains: see the package's user guide,
"Scan the job's code when it starts". Install the two together with `pip install invaract[lint]`.

**In CI**, `--format github` prints each finding as an annotation on the pull request's diff:

```yaml
- run: pip install invaract-lint
- run: invaract-lint --format github jobs/
```

**As a pre-commit hook:**

```yaml
repos:
  - repo: local
    hooks:
      - id: invaract-lint
        name: invaract-lint
        entry: invaract-lint
        language: python
        additional_dependencies: ["invaract-lint"]
        types: [python]
```

`--exclude GLOB` (repeatable) skips files or folders by name or path; hidden folders, virtual environments and `build`/`dist`
are always skipped.

Tests: `python -m pytest` here. Mutation testing: `mutmut run` here (`[tool.mutmut]` in `pyproject.toml`).
