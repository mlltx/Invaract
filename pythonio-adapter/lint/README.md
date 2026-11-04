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

It exits 0 when it finds nothing, 1 when it finds something, and 2 when a file cannot be read or parsed.

| Code | Flags |
|---|---|
| `IVR001` | a read: `pandas.read_*`, Polars' `read_*` and `scan_*`, pyarrow's `parquet`/`csv`/`feather`/`json`/`orc`/`ipc`/`dataset` readers, `duckdb.read_*` |
| `IVR002` | a write: pyarrow's writers; a frame's `to_csv`, `to_parquet`, ... and `write_*` given a destination, `sink_*`; `open` in a mode that writes (or one not spelled out); `Path.write_text`, `write_bytes`, `open("w")` |
| `IVR003` | a connection: database drivers' `connect`, `sqlalchemy.create_engine`, BigQuery and Cloud Storage clients, `boto3`, `s3fs`, `gcsfs`, `adlfs`, `fsspec` |
| `IVR004` | SQL that reads or writes files, passed to `sql`, `execute` or `query`: `COPY`, `ATTACH`, `EXPORT DATABASE`, `read_parquet(...)` and the like, a quoted path after `FROM` |

Calls are matched by the name they resolve to through the file's imports (`import pandas as pd`, `from pyarrow import parquet
as pq`, `from pandas import read_csv`), so an alias is no way around it. Frames turned into text (`df.to_csv()` with no
destination) and files opened for reading (`open("config.yaml")`) are not flagged.

**Keeping a call on purpose.** Mark its line (any line of the call) with `# invaract: allow`, ideally with the reason:

```python
lookup = pd.read_csv("lookup/regions.csv")  # invaract: allow - a table shipped with the job, not a dataset
```

**What it does not do.** It checks only that such calls are present, which a syntax tree answers reliably; it does not try to
work out what they do. Names reached dynamically (`getattr(pd, "read_csv")`, `__import__`) are not followed: it guards the
ordinary way of writing a job, not one built to get around it.

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
