# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract for Python jobs: read and write by the names a data contract gives, and publish only what the contract allows.

    import invaract

    @invaract.job()                                   # or @invaract.job("contract.yaml"); INVARACT_CONTRACT overrides it
    def main():
        orders = invaract.read("orders")              # a pandas DataFrame; as_="polars", "polars-lazy" or "arrow"
        invaract.write(report(orders), "report")      # checked now, staged, published when the run ends

A run starts Invaract's checker (docs/PYTHONIO_ADAPTER.md), describes every read and write to it (names, columns, types,
empty values; never the data), and when the job returns publishes every staged output, or none if anything was refused.
A refusal raises ``ContractViolation`` (exit status 1); a check that could not be made raises ``CheckNotRun`` (exit status 2).
Every setting comes from the environment as ``INVARACT_<NAME>`` (``invaract._settings``).
"""
import functools

from . import _run
from ._run import Run, job
from .errors import CheckNotRun, ContractViolation

__version__ = "0.1.0"
__all__ = ["job", "run", "read", "write", "reads", "writes", "Run", "ContractViolation", "CheckNotRun"]


def run(contract=None, *, name=None):
    """A run, for a script with no entry-point function: ``with invaract.run():``."""
    return Run(contract, name)


def read(name, as_="pandas", *, location=None, format=None, dtype_backend=None):
    """Reads the input the contract declares as ``name``, from the contract's location, as a pandas DataFrame (``as_``:
    ``"pandas"``, ``"polars"``, ``"polars-lazy"`` or ``"arrow"``). Only the columns the contract lists are read, if it lists
    any; a CSV input's columns are read as the contract's types.

    ``dtype_backend``, for a pandas read, is what pandas' own readers take: ``"numpy_nullable"`` gives ``Int64``,
    ``boolean`` and ``string`` columns (an integer column with a missing value stays an integer), ``"pyarrow"`` gives
    ``ArrowDtype`` columns; without it, pandas' default dtypes.

    ``location`` and ``format`` are for migrating a job with no contract yet (``INVARACT_DRY_RUN=true``): they say where the
    job reads. With a contract in force, a ``location`` that disagrees with it is refused.
    """
    return _run.current("read").read(name, as_, location, format, dtype_backend)


def write(frame, name, *, location=None, format=None, save_mode=None, partition_by=None, partition_overwrite=None):
    """Checks ``frame`` (anything offering an Arrow stream: a pandas, Polars or DuckDB frame, a Polars LazyFrame, a pyarrow
    Table or RecordBatchReader) against the output the contract declares as ``name``, stages it, and publishes it at the
    run's end with the contract's format, save mode and partitioning. A required column that holds an empty value is
    refused.

    ``location``, ``format`` and ``save_mode`` are for migrating a job with no contract yet (``INVARACT_DRY_RUN=true``). With
    a contract in force, one that disagrees with it is refused. Without either, a write is Parquet, overwriting.

    ``partition_by`` (column names, in order) and ``partition_overwrite`` (``"static"``: an overwrite replaces every
    partition; ``"dynamic"``: only those written) default to the output's declared partition keys and
    ``partitionOverwriteMode``; a write that partitions otherwise than the contract declares is refused.

    A pandas frame is written as the contract declares where pandas cannot hold the declared type and no value changes (an
    integer column with a missing value, which pandas holds as ``float64``, is written as an integer); a named index level
    the contract declares as a column (a ``groupby`` result's keys) is written as that column. A column that cannot be
    written as declared is refused, with a note saying why in pandas' terms.
    """
    _run.current("write").write(frame, name, location, format, save_mode, partition_by, partition_overwrite)


def reads(*names, as_="pandas", dtype_backend=None):
    """Decorates a function so each input in ``names`` is read and passed in as the keyword argument of the same name (unless
    the caller passes it, as a test can)."""

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for name in names:
                if name not in kwargs:
                    kwargs[name] = read(name, as_, dtype_backend=dtype_backend)
            return fn(*args, **kwargs)

        return wrapper

    return decorate


def writes(name):
    """Decorates a function so what it returns is written as the output ``name``, and still returned."""

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            write(result, name)
            return result

        return wrapper

    return decorate
