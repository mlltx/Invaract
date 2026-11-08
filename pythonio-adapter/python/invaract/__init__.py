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

The functions here are the whole public interface; every module whose name starts with ``_`` is the package's own.
"""
from __future__ import annotations

import functools
import os
from typing import Any, Callable, Literal, Mapping, Optional, Sequence, TypeVar, Union

from . import _run
from ._run import Run, job
from .errors import CheckNotRun, ContractViolation

__version__ = "0.1.0"
__all__ = ["job", "run", "read", "write", "check", "reads", "writes", "Run", "ContractViolation", "CheckNotRun"]

#: A contract: a file path, or ``registry://<id>@<version>``.
Contract = Union[str, "os.PathLike[str]"]
#: A location a job names (``location=``): a path, or a URI (``s3://...``, ``postgresql://...``).
Location = Union[str, "os.PathLike[str]"]
#: What ``invaract.read`` returns a frame as.
FrameKind = Literal["pandas", "polars", "polars-lazy", "arrow"]
#: A CSV dataset's options, by the contract's names (Spark's): ``{"delimiter": ";", "header": False}``.
CsvOptions = Mapping[str, Union[str, bool]]

_F = TypeVar("_F", bound=Callable[..., Any])


def run(contract: Optional[Contract] = None, *, name: Optional[str] = None) -> Run:
    """A run, for a script with no entry-point function: ``with invaract.run():``."""
    return Run(contract, name)


def read(
    name: str,
    as_: FrameKind = "pandas",
    *,
    location: Optional[Location] = None,
    format: Optional[str] = None,
    dtype_backend: Optional[Literal["numpy_nullable", "pyarrow"]] = None,
    chunksize: Optional[int] = None,
    csv: Optional[CsvOptions] = None,
) -> Any:
    """Reads the input the contract declares as ``name``, from the contract's location, as a pandas DataFrame (``as_``:
    ``"pandas"``, ``"polars"``, ``"polars-lazy"`` or ``"arrow"``). Only the columns the contract lists are read, if it lists
    any; a CSV input's columns are read as the contract's types.

    ``dtype_backend``, for a pandas read, is what pandas' own readers take: ``"numpy_nullable"`` gives ``Int64``,
    ``boolean`` and ``string`` columns (an integer column with a missing value stays an integer), ``"pyarrow"`` gives
    ``ArrowDtype`` columns; without it, pandas' default dtypes.

    ``chunksize`` (a number of rows) returns the input as frames of that many rows each, one at a time, as pandas'
    ``read_csv(chunksize=)`` does, for an input larger than memory: it is read twice, once to count its empty values (which
    are checked before any of it is handed over) and once for the chunks, and never held whole.

    ``csv`` (Spark's CSV option names: ``{"delimiter": ";", "header": False, "encoding": "ISO-8859-1"}``) is how a CSV input is
    laid out, for a job with no contract yet; the contract's ``csv:`` options are used otherwise, and ones that disagree with
    them are refused.

    ``location`` and ``format`` are for migrating a job with no contract yet (``INVARACT_DRY_RUN=true``): they say where, and
    in which format, the job reads. With a contract in force, one that disagrees with it is refused (a ``format`` before
    anything is read).

    Returns the frame (or, with ``chunksize``, an iterator of frames). Raises ``ContractViolation`` when the contract
    refuses the read, and ``CheckNotRun`` when it could not be checked.
    """
    return _run.current("read").read(name, as_, location, format, dtype_backend, chunksize, csv)


def write(
    frame: Any,
    name: str,
    *,
    location: Optional[Location] = None,
    format: Optional[str] = None,
    save_mode: Optional[Literal["overwrite", "append", "error", "ignore"]] = None,
    partition_by: Optional[Sequence[str]] = None,
    partition_overwrite: Optional[Literal["static", "dynamic"]] = None,
    csv: Optional[CsvOptions] = None,
) -> None:
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

    A CSV output is written with its contract's ``csv:`` options (delimiter, header, encoding, quote, escape, nullValue,
    dateFormat, timestampFormat); ``csv`` gives them for a job with no contract yet, and ones that disagree with the contract's
    are refused.

    Raises ``ContractViolation`` when the contract refuses the write, and ``CheckNotRun`` when it could not be checked;
    either way nothing the run wrote is published.
    """
    _run.current("write").write(frame, name, location, format, save_mode, partition_by, partition_overwrite, csv)


def check(frame: Any, name: str, *, contract: Optional[Contract] = None, csv: Optional[CsvOptions] = None) -> None:
    """Checks ``frame`` against the output the contract declares as ``name`` exactly as ``invaract.write`` would (its columns,
    their types, empty values, and with ``INVARACT_RUNTIME_DATA_QUALITY`` on, its values), and writes nothing: for a notebook,
    or a job's own tests. Raises ``ContractViolation`` when the write would be refused; returns ``None`` when it would pass.

    It works with or without a run in progress, and does not count as one of the run's writes. ``contract`` names the
    contract when ``INVARACT_CONTRACT`` does not. A check publishes no events.
    """
    _run.check(frame, name, contract, csv)


def reads(*names: str, as_: FrameKind = "pandas", dtype_backend: Optional[Literal["numpy_nullable", "pyarrow"]] = None) -> Callable[[_F], _F]:
    """Decorates a function so each input in ``names`` is read and passed in as the keyword argument of the same name (unless
    the caller passes it, as a test can)."""

    def decorate(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for name in names:
                if name not in kwargs:
                    kwargs[name] = read(name, as_, dtype_backend=dtype_backend)
            return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorate


def writes(name: str) -> Callable[[_F], _F]:
    """Decorates a function so what it returns is written as the output ``name``, and still returned."""

    def decorate(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            write(result, name)
            return result

        return wrapper  # type: ignore[return-value]

    return decorate
