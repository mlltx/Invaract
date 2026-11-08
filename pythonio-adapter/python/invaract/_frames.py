# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Tables in and out: what a job hands ``invaract.write`` as an Arrow stream, what a read returns in the frame library the job
asked for, and how many empty values each column holds (which ``_protocol.columns`` tells the checker)."""
import pyarrow as pa

from . import _pandas

#: What ``invaract.read`` can return.
FRAME_KINDS = ("pandas", "polars", "polars-lazy", "arrow")
#: A frame library's own conversion to Arrow, for a release older than the Arrow stream (Polars 1.0, DuckDB 1.0).
_OWN_CONVERSION = {"polars": "to_arrow", "duckdb": "arrow"}


def stream(frame, fields=(), dry_run=False):
    """The frame as ``(schema, batches, notes)``, its schema known before any batch is produced; ``None`` for an object that
    offers no Arrow stream, whose contents cannot be described. A Polars LazyFrame runs batch by batch as the batches are
    taken. A pandas frame is converted as ``_pandas.to_arrow`` converts it, to the types ``fields`` (the output's declared
    columns) declare, and ``notes`` are what that says of its columns; every other frame holds its own types, and has none."""
    if _pandas.is_frame(frame):
        # pandas' own stream carries the row index as a column (__index_level_0__) once a filter or sort has made it anything
        # but 0, 1, 2, ...; the index is pandas' bookkeeping, not data the contract declares, so it is left out (_pandas)
        table, notes = _pandas.to_arrow(frame, fields, dry_run=dry_run)
        return table.schema, iter(table.to_batches()), notes
    if type(frame).__name__ == "LazyFrame" and hasattr(frame, "collect_schema"):
        import polars as pl

        schema = pl.DataFrame(schema=frame.collect_schema()).to_arrow().schema
        return schema, (batch for part in _lazy_batches(frame) for batch in part.to_arrow().cast(schema).to_batches()), []
    if hasattr(frame, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(frame)
        return reader.schema, iter(reader), []
    convert = _OWN_CONVERSION.get(type(frame).__module__.split(".")[0])
    if convert is not None and callable(getattr(frame, convert, None)):
        converted = getattr(frame, convert)()
        if isinstance(converted, pa.Table):
            return converted.schema, iter(converted.to_batches()), []
        return converted.schema, iter(converted), []
    return None


def _lazy_batches(frame):
    if hasattr(frame, "collect_batches"):
        return frame.collect_batches()
    return [frame.collect()]


def null_counts(batch):
    """How many empty values each column of ``batch`` (a record batch or a table) holds."""
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
