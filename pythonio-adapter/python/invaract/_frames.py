# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Tables in and out: what a job hands ``invaract.write`` as an Arrow stream, what a read returns in the frame library the job
asked for, and each as the checker is told about it (a column's pyarrow type name, Arrow's nullable flag, its empty values)."""
import re

import pyarrow as pa

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
    if type(frame).__module__.split(".")[0] == "pandas" and type(frame).__name__ == "DataFrame":
        # pandas' own stream carries the row index as a column (__index_level_0__) once a filter or sort has made it anything
        # but 0, 1, 2, ...; the index is pandas' bookkeeping, not data the contract declares, so it is left out
        table = pa.Table.from_pandas(frame, preserve_index=False)
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


def as_frame(table, kind):
    """The table read, as the frame library ``kind`` names."""
    if kind == "arrow":
        return table
    if kind == "pandas":
        return table.to_pandas()
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


def read_table(location, fmt, fields):
    """The dataset at ``location`` (a file or a folder of files) in format ``fmt``: only the columns the contract lists, when it
    lists any, and, for CSV, each read as the contract's type. A declared column the data lacks is left for the checker to
    refuse."""
    import pyarrow.dataset as ds

    declared = [f["name"] for f in fields]
    if fmt == "csv":
        import pyarrow.csv as csv

        types = {f["name"]: arrow_type(f["type"]) for f in fields}
        file_format = ds.CsvFileFormat(convert_options=csv.ConvertOptions(column_types={n: t for n, t in types.items() if t is not None}))
    elif fmt == "parquet":
        file_format = "parquet"
    else:
        raise ValueError(f"invaract cannot read format {fmt!r}: it reads parquet and csv")
    dataset = ds.dataset(location, format=file_format)
    columns = [n for n in declared if n in dataset.schema.names] if declared else None
    return dataset.to_table(columns=columns)
