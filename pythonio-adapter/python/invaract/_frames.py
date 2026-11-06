# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Tables in and out: what a job hands ``invaract.write`` as an Arrow stream, what a read returns in the frame library the job
asked for, and each as the checker is told about it (a column's pyarrow type name, Arrow's nullable flag, its empty values)."""
import re

import pyarrow as pa

from . import _pandas

#: What ``invaract.read`` can return.
FRAME_KINDS = ("pandas", "polars", "polars-lazy", "arrow")


def describe(schema, nulls=None):
    """The columns as the checker reads them: ``str(type)`` is pyarrow's own name for a type, which the checker maps."""
    return [
        {"name": f.name, "type": str(f.type), "nullable": f.nullable, **({"nulls": nulls[f.name]} if nulls is not None else {})}
        for f in schema
    ]


def stream(frame):
    """The frame as ``(schema, batches)``, its schema known before any batch is produced; ``None`` for an object that offers
    no Arrow stream, whose contents cannot be described. A Polars LazyFrame runs batch by batch as the batches are taken."""
    if type(frame).__name__ == "LazyFrame" and hasattr(frame, "collect_schema"):
        import polars as pl

        schema = pl.DataFrame(schema=frame.collect_schema()).to_arrow().schema
        return schema, (batch for part in _lazy_batches(frame) for batch in part.to_arrow().cast(schema).to_batches())
    if _pandas.is_frame(frame):
        # pandas' own stream carries the row index as a column (__index_level_0__) once a filter or sort has made it anything
        # but 0, 1, 2, ...; the index is pandas' bookkeeping, not data the contract declares, so it is left out (_pandas)
        table, _ = _pandas.to_arrow(frame)
        return table.schema, iter(table.to_batches())
    if hasattr(frame, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(frame)
        return reader.schema, iter(reader)
    # a Polars or DuckDB older than the Arrow stream (Polars 1.0, DuckDB 1.0): the library's own conversion to Arrow
    convert = _OWN_CONVERSION.get(type(frame).__module__.split(".")[0])
    if convert is not None and callable(getattr(frame, convert, None)):
        converted = getattr(frame, convert)()
        if isinstance(converted, pa.Table):
            return converted.schema, iter(converted.to_batches())
        return converted.schema, iter(converted)
    return None


_OWN_CONVERSION = {"polars": "to_arrow", "duckdb": "arrow"}


def _lazy_batches(frame):
    if hasattr(frame, "collect_batches"):
        return frame.collect_batches()
    return [frame.collect()]


def null_counts(batch):
    return {name: batch.column(i).null_count for i, name in enumerate(batch.schema.names)}


def with_required(schema, required):
    """``schema`` with each column in ``required`` made not-null: what an output is written as, so readers see the contract's
    schema (decision 3)."""
    return pa.schema([f.with_nullable(False) if f.name in required else f for f in schema], metadata=schema.metadata)


def take(schema, target, batches, required, write):
    """Hands each batch, as ``target``, to ``write`` and returns the empty values each column held. A required column's first
    empty value ends it: the checker refuses that write, so nothing more is written."""
    nulls = dict.fromkeys(schema.names, 0)
    for batch in batches:
        counts = null_counts(batch)
        for column, n in counts.items():
            nulls[column] += n
        if any(counts.get(column) for column in required):
            break
        write(pa.RecordBatch.from_arrays(batch.columns, schema=target))
    return nulls


def as_frame(table, kind, dtype_backend=None):
    """The table read, as the frame library ``kind`` names (a pandas frame in the dtypes ``dtype_backend`` names)."""
    if kind == "arrow":
        return table
    if kind == "pandas":
        return _pandas.as_pandas(table, dtype_backend)
    if kind in ("polars", "polars-lazy"):
        import polars as pl

        frame = pl.from_arrow(table)
        return frame.lazy() if kind == "polars-lazy" else frame
    raise ValueError(f"invaract.read cannot return as_={kind!r}: give one of {', '.join(FRAME_KINDS)}")


_SIMPLE_TYPES = {
    "string": pa.string(), "boolean": pa.bool_(), "tinyint": pa.int8(), "byte": pa.int8(), "smallint": pa.int16(),
    "short": pa.int16(), "int": pa.int32(), "integer": pa.int32(), "bigint": pa.int64(), "long": pa.int64(),
    "float": pa.float32(), "real": pa.float32(), "double": pa.float64(), "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"), "timestamp_ltz": pa.timestamp("us", tz="UTC"), "timestamp_ntz": pa.timestamp("us"),
    "binary": pa.binary(),
}
_DECIMAL = re.compile(r"^(?:decimal|dec|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def arrow_type(contract_type):
    """The Arrow type a CSV column of the contract's ``contract_type`` is read as; ``None`` for one a CSV file cannot hold
    (nested types), which is then read as the reader infers it."""
    t = contract_type.strip().lower()
    if t in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[t]
    m = _DECIMAL.match(t)
    return pa.decimal128(int(m.group(1)), int(m.group(2))) if m else None


def read_table(location, fmt, fields, partition_by=(), csv=None):
    """The dataset at ``location`` in format ``fmt``: a file or a folder of files, local or on object storage, or a Delta or
    Iceberg table. Only the columns the contract lists are read, when it lists any, and, for CSV, each as the contract's
    type. A declared column the data lacks is left for the checker to refuse. A folder partitioned Hive-style
    (``region=north/``) gives its keys as columns: those in ``partition_by`` typed as the contract declares them, any other
    typed as pyarrow infers it from the folder names."""
    from . import _tables

    declared = [f["name"] for f in fields]
    if fmt == "jdbc":
        from . import _postgres

        return _postgres.read(location, fields)
    if fmt == "delta":
        return _tables.read_delta(location, declared)
    if fmt == "iceberg":
        return _tables.read_iceberg(location, declared)
    dataset, finish = _file_dataset(location, fmt, fields, partition_by, csv)
    return finish(dataset.to_table(columns=_present(declared, dataset.schema.names) or None))


def read_batches(location, fmt, fields, partition_by=(), csv=None):
    """The dataset at ``location``, as ``read_table`` reads it, a batch at a time: ``(schema, batches)``, where ``batches(columns)``
    reads the dataset afresh and yields its record batches of ``columns`` (by default the columns ``schema`` holds) one by
    one, without holding the whole dataset in memory."""
    from . import _tables

    declared = [f["name"] for f in fields]
    if fmt == "jdbc":
        from . import _postgres

        return _postgres.batches(location, fields)
    if fmt == "iceberg":
        return _tables.iceberg_batches(location, declared)
    dataset, finish = (_tables.delta_dataset(location), _same) if fmt == "delta" else _file_dataset(location, fmt, fields, partition_by, csv)
    columns = _present(declared, dataset.schema.names) or dataset.schema.names
    schema = finish(pa.schema([dataset.schema.field(c) for c in columns]).empty_table()).schema
    return schema, lambda only=None: (finish(b) for b in dataset.to_batches(columns=list(only if only is not None else columns)))


def chunks(schema, batches, size):
    """``batches`` as tables of ``size`` rows each, the last one shorter (as pandas' ``chunksize`` gives them)."""
    pending = schema.empty_table()
    for batch in batches:
        pending = pa.concat_tables([pending, pa.Table.from_batches([batch]).cast(schema)])
        while pending.num_rows >= size:
            yield pending.slice(0, size)
            pending = pending.slice(size)
    if pending.num_rows:
        yield pending


def count_nulls(schema, batches):
    """How many empty values each column of ``schema`` holds, from one pass over ``batches()``."""
    nulls = dict.fromkeys(schema.names, 0)
    for batch in batches(schema.names):
        for name, n in null_counts(batch).items():
            nulls[name] += n
    return nulls


def _present(declared, names):
    return [n for n in declared if n in names]


def _same(data):
    return data


def _file_dataset(location, fmt, fields, partition_by, csv=None):
    """``(dataset, finish)``: a file or folder of Parquet or CSV files as a pyarrow dataset (a CSV's columns typed as the
    contract declares them, read with its CSV options), and what a table or batch read from it still needs (``_csv``)."""
    import pyarrow.dataset as ds

    from . import _storage

    types = {f["name"]: arrow_type(f["type"]) for f in fields}
    finish = _same
    if fmt == "csv" and csv:
        from . import _csv

        file_format, finish = _csv.read_format(fields, csv)
    elif fmt == "csv":
        import pyarrow.csv as csv

        file_format = ds.CsvFileFormat(convert_options=csv.ConvertOptions(column_types={n: t for n, t in types.items() if t is not None}))
    elif fmt == "parquet":
        file_format = "parquet"
    else:
        raise ValueError(f"invaract cannot read format {fmt!r}: it reads parquet, csv, delta and iceberg")
    partitioning = (ds.partitioning(pa.schema([(k, types.get(k) or pa.string()) for k in partition_by]), flavor="hive")
                    if partition_by else "hive")
    return ds.dataset(_storage.uri(location), format=file_format, partitioning=partitioning), finish
