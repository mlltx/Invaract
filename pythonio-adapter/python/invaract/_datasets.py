# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A dataset by its location and format: how it is read, the catalog it is registered in, and the output that writes it.

- Plain files (``_formats.FORMATS``): read through pyarrow's datasets or file by file (``_formats.Files``), written by
  ``_files``; a location is a file or a folder of files, local, on HDFS or on object storage (``_storage``).
- Delta Lake and Iceberg tables (``_tables``).
- Database tables (``jdbc``): PostgreSQL (``_postgres``) and SQLite (``_sqlite``), by the location's scheme.
"""
import os

import pyarrow as pa

from . import _csv, _files, _formats, _jdbc, _postgres, _sqlite, _storage, _tables, _types
from .errors import CheckNotRun

#: Every format a run reads and writes.
FORMATS = _formats.FORMATS + _tables.TABLE_FORMATS + (_jdbc.FORMAT,)
#: The format of a dataset that neither the contract, the job nor the location's extension names.
DEFAULT_FORMAT = "parquet"


def format_of(location):
    """The format ``location`` names: ``jdbc`` for a database table, else the one its extension names, else Parquet."""
    if _database(location) is not None:
        return _jdbc.FORMAT
    return _formats.by_extension(os.fspath(location)) or DEFAULT_FORMAT


def catalog_of(location, fmt):
    """The catalog registration the checker is told a dataset has (the protocol's ``catalog``): an Iceberg or a database
    table's; ``None`` for files and Delta tables, which are named by their path."""
    fmt = fmt.lower()
    if fmt == _jdbc.FORMAT:
        database = _database(location)
        return database.catalog_of(location) if database is not None else None
    if fmt == "iceberg":
        return _tables.catalog_of(location)
    return None


def read_table(location, fmt, fields, partition_by=(), csv=None):
    """The dataset at ``location`` in format ``fmt``. Only the columns the contract lists are read, when it lists any, and,
    for a format that holds no types of its own (CSV, JSON, Excel, a database's text), each as the contract's type. A
    declared column the data lacks is left for the checker to refuse. A folder partitioned Hive-style (``region=north/``)
    gives its keys as columns: those in ``partition_by`` typed as the contract declares them, any other typed as pyarrow
    infers it from the folder names."""
    fmt = fmt.lower()
    if fmt == _jdbc.FORMAT:
        return _required_database(location).read(location, fields)
    if fmt == "iceberg":
        return _tables.read_iceberg(location, _names(fields))
    dataset, finish = _dataset(location, fmt, fields, partition_by, csv)
    return finish(dataset.to_table(columns=_present(fields, dataset.schema.names) or None))


def read_batches(location, fmt, fields, partition_by=(), csv=None):
    """The dataset at ``location``, as ``read_table`` reads it, a batch at a time: ``(schema, batches)``, where
    ``batches(columns)`` reads the dataset afresh and yields its record batches of ``columns`` (by default the columns
    ``schema`` holds) one by one, without holding the whole dataset in memory."""
    fmt = fmt.lower()
    if fmt == _jdbc.FORMAT:
        return _required_database(location).batches(location, fields)
    if fmt == "iceberg":
        return _tables.iceberg_batches(location, _names(fields))
    dataset, finish = _dataset(location, fmt, fields, partition_by, csv)
    columns = _present(fields, dataset.schema.names) or dataset.schema.names
    schema = finish(pa.schema([dataset.schema.field(c) for c in columns]).empty_table()).schema

    def batches(only=None):
        return (finish(b) for b in dataset.to_batches(columns=list(only if only is not None else columns)))

    return schema, batches


def output(location, fmt, mode, run_id, layout, csv=None):
    """The output that writes ``location`` in format ``fmt`` with save mode ``mode`` (``_outputs.Output``); ``csv``, the CSV
    options a CSV output is written with."""
    fmt = fmt.lower()
    if fmt == _jdbc.FORMAT:
        return _required_database(location).output(location, mode, run_id, layout)
    if fmt == "delta":
        return _tables.DeltaOutput(location, mode, run_id, layout)
    if fmt == "iceberg":
        return _tables.IcebergOutput(location, mode, run_id, layout)
    return _files.output(location, fmt, mode, run_id, layout, csv)


def _database(location):
    """The module of the database ``location`` names (``_postgres`` or ``_sqlite``), or ``None`` for any other location."""
    if _postgres.is_postgres(location):
        return _postgres
    if _sqlite.is_sqlite(location):
        return _sqlite
    return None


def _required_database(location):
    database = _database(location)
    if database is None:
        raise CheckNotRun(f"Invaract reads and writes jdbc only as PostgreSQL (postgresql://...) and SQLite (sqlite:///...) "
                          f"tables, not {location}")
    return database


def _names(fields):
    return [f["name"] for f in fields]


def _present(fields, names):
    """The declared columns the data has, in the contract's order."""
    return [n for n in _names(fields) if n in names]


def _unchanged(data):
    return data


def _dataset(location, fmt, fields, partition_by, csv=None):
    """``(dataset, finish)``: what is at ``location`` as a pyarrow dataset (a Delta table's current version; a file or a
    folder of files, a CSV's columns typed as the contract declares them, read with its CSV options), or as
    ``_formats.Files`` for a format pyarrow's datasets do not read, and what a table or batch read from it still needs
    (``_csv.read_format``)."""
    import pyarrow.dataset as ds

    if fmt == "delta":
        return _tables.delta_dataset(location), _unchanged
    types = _types.scalars(fields)
    if fmt in _formats.FOREIGN:
        return _formats.Files(location, fmt, fields, types), _unchanged
    finish = _unchanged
    if fmt == "csv":
        file_format, finish = _csv.read_format(fields, csv or {})
    elif fmt in _formats.DATASET:
        file_format = _formats.DATASET[fmt]
    else:
        raise ValueError(f"invaract cannot read format {fmt!r}: it reads {', '.join(FORMATS[:-1])} and {FORMATS[-1]}")
    partitioning = (ds.partitioning(pa.schema([(k, types.get(k) or pa.string()) for k in partition_by]), flavor="hive")
                    if partition_by else "hive")
    filesystem, path = _storage.filesystem(location)
    return ds.dataset(path, format=file_format, partitioning=partitioning, filesystem=filesystem), finish
