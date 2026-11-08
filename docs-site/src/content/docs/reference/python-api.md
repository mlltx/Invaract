---
title: Python Package API
description: Every function, argument, return value and exception of the invaract Python package, with the formats, locations and extras it supports.
sidebar:
  order: 1.6
---

The `invaract` package's whole public interface is the names on this page: `import invaract`, then
`invaract.<name>`. Modules whose names start with `_` are the package's own and can change in any release. How to adopt
the package in a job, step by step, is [Use Invaract with Python Jobs](/guides/using-invaract-with-python/).

The package ships type hints (it is marked `py.typed`), so an editor completes these arguments, and a type checker such
as mypy flags a value the package does not take, such as `as_="numpy"` or `save_mode="replace"`.

## Runs

Every `read` and `write` belongs to a run. A run starts Invaract's checker, has every read and write checked as it is
made, and when it ends publishes every output the job staged, or none if anything was refused or the job failed.
One run at a time per process; runs do not nest.

### `@invaract.job(contract=None, *, name=None)`

Decorates a job's entry point: each call of the function is one run. Use it as `@invaract.job("contract.yaml")`,
`@invaract.job()` (the contract then comes from `INVARACT_CONTRACT`), or bare `@invaract.job`.

| Argument | |
|---|---|
| `contract` | The default contract: a file path, or `registry://<id>@<version>`. `INVARACT_CONTRACT` overrides it. |
| `name` | The job's name in the events the run publishes. Default: the function's module and name, such as `orders_job.main`. |

When the function returns, the run publishes what the job wrote. When it raises, nothing is published and the
exception propagates. The outputs are published in the order the job first wrote them; if one fails to publish, those
already put in place by a rename are put back, and the failure is raised (see
[Files, tables and object storage](/guides/using-invaract-with-python/#files-tables-and-object-storage)).

### `invaract.run(contract=None, *, name=None)`

The same run as a context manager, for a script with no entry-point function:

```python
with invaract.run("contract.yaml"):
    orders = invaract.read("orders")
    invaract.write(summarise(orders), "report")
```

The arguments are `job`'s. The default name is the script's (or its module's, under `python -m`). It returns an
`invaract.Run`; a job has no need to use the object itself.

## Reading

### `invaract.read(name, as_="pandas", *, location=None, format=None, dtype_backend=None, chunksize=None, csv=None)`

Reads the input the contract declares as `name`, from the contract's location, and checks it against the contract
before returning it. Only the columns the contract lists are read, when it lists any. A CSV, JSON or Excel input, or a
database table's text columns, are read as the contract's types.

| Argument | |
|---|---|
| `name` | The input's name in the contract. |
| `as_` | What to return: `"pandas"` (a DataFrame), `"polars"` (a DataFrame), `"polars-lazy"` (a LazyFrame) or `"arrow"` (a `pyarrow.Table`). |
| `dtype_backend` | For `as_="pandas"` only, as pandas' own readers take it: `"numpy_nullable"` (`Int64`, `boolean`, `string` columns: an integer column with a missing value stays an integer) or `"pyarrow"` (`ArrowDtype` columns). Default: pandas' default dtypes. |
| `chunksize` | A number of rows, 1 or more: return an iterator of frames of that many rows each, the last one shorter, as pandas' `read_csv(chunksize=)` does, for an input larger than memory. The input is read twice, once to count its empty values (checked before any of it is returned) and once for the chunks, and is never held whole. |
| `csv` | How a CSV input is laid out, by the contract's option names: `{"delimiter": ";", "header": False, "encoding": "ISO-8859-1"}`. For a job with no contract yet; otherwise the contract's `csv:` options are used, and options that disagree with them are refused. See [CSV Options](/guides/csv-options/). |
| `location`, `format` | Where, and in which format, the job reads. For a job being migrated with `INVARACT_DRY_RUN=true`, which has no contract to say so. With a contract in force, a value that disagrees with it is refused (a `format` before anything is read). |

Returns the frame, or with `chunksize`, an iterator of frames.

### `@invaract.reads(*names, as_="pandas", dtype_backend=None)`

Decorates a function so each input in `names` is read with `invaract.read` and passed in as the keyword argument of
the same name, unless the caller passes it (as a test can):

```python
@invaract.reads("customers")
def customers(customers):
    return customers
```

## Writing

### `invaract.write(frame, name, *, location=None, format=None, save_mode=None, partition_by=None, partition_overwrite=None, csv=None)`

Checks `frame` against the output the contract declares as `name`, stages it where no reader sees it, checks it again
once staged, and publishes it when the run ends, with the contract's format, save mode and partitioning. A required
column that holds an empty value is refused.

| Argument | |
|---|---|
| `frame` | Anything that offers an Arrow stream: a pandas, Polars or DuckDB frame, a Polars LazyFrame (run batch by batch into staging), a `pyarrow.Table` or `RecordBatchReader`. A pandas frame's row index is not written; see [pandas frames](/guides/using-invaract-with-python/#pandas-frames) for how its columns are written as the contract declares them. |
| `name` | The output's name in the contract. |
| `partition_by` | The columns to partition by, in order. Default: the output's declared partition keys. Partitioning otherwise than the contract declares is refused. See [Partition Outputs](/guides/partitioning-outputs/). |
| `partition_overwrite` | What an overwrite of a partitioned output replaces: `"static"` (every partition) or `"dynamic"` (only those written). Default: the output's declared `partitionOverwriteMode`, else `"static"`. |
| `csv` | How a CSV output is laid out, as for `read`. |
| `location`, `format`, `save_mode` | Where, in which format and with which save mode (`"overwrite"`, `"append"`, `"error"` or `"ignore"`) the job writes. For a job being migrated with `INVARACT_DRY_RUN=true`; with a contract in force, a value that disagrees with it is refused. With none, from anywhere, a write is Parquet, overwriting. |

Writing the same output twice in one run publishes both writes together (a plain-file output as two files); both must be
laid out the same way.

### `@invaract.writes(name)`

Decorates a function so what it returns is written with `invaract.write` as the output `name`, and still returned:

```python
@invaract.writes("audit")
def audit(paid):
    return paid[["order_id", "status"]]
```

### `invaract.check(frame, name, *, contract=None, csv=None)`

Checks `frame` exactly as `invaract.write` would check it as the output `name` (its columns, their types, its empty
values and, with `INVARACT_RUNTIME_DATA_QUALITY` on, its values) and writes nothing: for a notebook, or a job's own
tests. It returns `None` when the write would pass, and raises `ContractViolation` when it would be refused.

It works with or without a run in progress, and is not one of the run's writes. `contract` names the contract when
`INVARACT_CONTRACT` does not (inside a run, the run's own contract is the default). A check publishes no events.

## Errors and exit statuses

Every refusal and every failure to check raises one of two exceptions. Either way, nothing the run wrote is published.

| Exception | Raised when | Exit status, if uncaught |
|---|---|---|
| `invaract.ContractViolation` (a `PermissionError`) | The contract refused a read, a write or the run. | 1 |
| `invaract.CheckNotRun` (a `RuntimeError`) | The check could not be made: no checker for the platform, a checker that stopped, a contract that cannot be read, an unknown setting, a format or location the package cannot read or write, an extra that is not installed. | 2 |

The two exit statuses let a scheduler tell a run its contract refused from one that could not be checked; any other
uncaught exception ends the job with Python's usual status 1.

`ContractViolation` has two attributes:

| Attribute | |
|---|---|
| `violations` | The kind of each violation, in order, such as `["OUTPUT_FIELD_TYPE_MISMATCH"]`. Every kind is in [Violation Types](/reference/violation-types/). |
| `explanation` | Invaract's explanation, also the exception's message: what the contract expects, what the job did, why that violates the contract, and how to correct it. For a pandas frame, notes on how pandas held a column it could not write as declared follow. |

A wrong argument (an unknown `as_`, a `chunksize` of 0, `csv=` for a dataset that is not CSV) raises `ValueError`
before anything is checked. `read` or `write` with no run in progress raises `RuntimeError`.

## Formats and locations

A dataset's format and location come from the contract (or, in a dry run, from the job's `format=` and `location=`).
Without a format, the location's extension names one (`.csv`; `.json`, `.jsonl` and `.ndjson`; `.orc`; `.avro`;
`.feather` and `.arrow`; `.xlsx`), a `postgresql://` or `sqlite:///` location is `jdbc`, and anything else is Parquet.

| Format | Location | Extra to install |
|---|---|---|
| `parquet`, `csv`, `json` (JSON lines), `orc`, `feather` | A file or a folder of files: a local or mounted path, `hdfs://...`, or object storage (`s3://`, `gs://`, `az://`, `abfss://`) | none |
| `avro` | as above | `avro` |
| `excel` | as above | `excel` |
| `delta` | The table's path or URI | `delta` |
| `iceberg` | `<catalog>.<namespace>.<table>`, the catalog configured as pyiceberg configures any | `iceberg` |
| `jdbc` | `postgresql://[<user>@]<host>[:<port>]/<database>/[<schema>.]<table>` (the schema is `public` when not given; the password comes from `PGPASSWORD` or `~/.pgpass`, never the location) | `postgres` |
| `jdbc` | `sqlite:///<path to the database file>/<table>` (`sqlite:////` for an absolute path) | `sqlite` |

Install an extra with the package: `pip install "invaract[pandas,delta]"`. The `pandas` and `polars` extras install
those libraries. How each kind of output is staged and published is in
[Files, tables and object storage](/guides/using-invaract-with-python/#files-tables-and-object-storage).

## Events

With `INVARACT_NOTIFY_CONFIG` set, a run publishes a `CONTRACT_VALIDATION` event for each check, a `WRITE` event for
each output it publishes, and a `JOB_SUMMARY` when it ends: see
[Events from a Python job](/guides/notification-sinks/#events-from-a-python-job). `invaract.check` publishes none.

## Settings

A run reads every setting from the job's environment as `INVARACT_<NAME>`, so a platform can configure a job it does
not own: `INVARACT_CONTRACT`, `INVARACT_DRY_RUN`, `INVARACT_NOTIFY_CONFIG`, `INVARACT_ORG_POLICY` and the rest. Every
setting, with its meaning, is in [Configuration Settings](/reference/configuration/); `INVARACT_CHECKER`, the only
setting of the Python package alone, is in its [Python-only settings](/reference/configuration/#python-only-settings).
