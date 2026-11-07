# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The file formats besides Parquet and CSV (decided 2026-11-06, docs/PYTHONIO_ADAPTER.md), each named as Spark names it so
one contract reads the same on every engine:

- ``json``: JSON lines, one object per line, as Spark reads and writes ``json``. A column the contract types is read as
  that type (a date from ``yyyy-MM-dd`` text, a timestamp from ISO 8601 text). Written as Spark writes it: an empty value
  left out of its line, dates as ``yyyy-MM-dd``, timestamps as ISO 8601, decimals as numbers, binary as base64.
- ``orc`` and ``feather`` (Arrow's own file format, which pandas' ``to_feather`` writes): through pyarrow, typed as the files
  say, as Parquet is.
- ``avro``: through fastavro (``pip install "invaract[avro]"``), as Spark's ``avro`` source lays it out: one record per row,
  an optional column a union with ``null``, dates, timestamps and decimals as Avro's logical types.
- ``excel``: ``.xlsx`` workbooks through openpyxl (``pip install "invaract[excel]"``): the first sheet, its first row naming
  the columns. A column the contract types is read as that type. A timestamp with a time zone is written in UTC, as Excel
  holds no time zone; binary and nested columns cannot be written.

A location is a file or a folder of files, partitioned Hive-style or not, as for Parquet. Reading JSON, Avro and Excel goes
file by file (``Files``): pyarrow's datasets do not read them.
"""
import base64
import datetime
import decimal
import io
import json
import math
import re

import pyarrow as pa

from .errors import CheckNotRun

#: Formats pyarrow's datasets read (``_frames._file_dataset``), by the name pyarrow gives each.
DATASET = {"parquet": "parquet", "orc": "orc", "feather": "feather"}
#: Formats read file by file here.
FOREIGN = ("json", "avro", "excel")
FORMATS = ("parquet", "csv", "json", "orc", "avro", "feather", "excel")
#: The extension of a file in each format, where it is not the format's name.
_EXTENSIONS = {"excel": "xlsx"}
#: The format a location's extension names, for a read or write with neither a contract nor a format to say it.
_BY_EXTENSION = {".csv": "csv", ".json": "json", ".jsonl": "json", ".ndjson": "json", ".orc": "orc", ".avro": "avro",
                 ".feather": "feather", ".arrow": "feather", ".xlsx": "excel"}
#: The most rows a sheet holds, its header row besides.
EXCEL_ROWS = 1_048_575


def extension(fmt):
    return _EXTENSIONS.get(fmt, fmt)


def by_extension(location):
    """The format the extension of ``location`` names, or ``None``."""
    lowered = str(location).lower()
    return next((fmt for ext, fmt in _BY_EXTENSION.items() if lowered.endswith(ext)), None)


def _need(module, extra, fmt):
    try:
        return __import__(module, fromlist=["_"])
    except ImportError as e:
        raise CheckNotRun(f'Invaract needs the {module} package to read or write {fmt}: pip install "invaract[{extra}]"') from e


# ---- reading, file by file ---------------------------------------------------------------------------------------------

class Files:
    """A file, or a folder of files partitioned Hive-style or not, in a format pyarrow's datasets do not read: what
    ``_frames`` reads a pyarrow dataset through (``schema``, ``to_table``, ``to_batches``). A partition key the contract
    lists is typed as it declares it, any other read as text."""

    def __init__(self, location, fmt, fields, types):
        import pyarrow.dataset as ds

        from . import _storage

        self._fmt, self._fields, self._types = fmt, fields, types
        self._fs, base = _storage.filesystem(location)
        listing = ds.dataset(base, format="parquet", schema=pa.schema([]), filesystem=self._fs)
        paths = sorted(f.path for f in listing.get_fragments())
        keys = []
        for path in paths:
            for segment in path[len(base.rstrip("/")):].strip("/").split("/")[:-1]:
                key = segment.partition("=")[0]
                if "=" in segment and key not in keys:
                    keys.append(key)
        self._keys = pa.schema([(k, types.get(k) or pa.string()) for k in keys])
        if keys:
            partitioning = ds.partitioning(self._keys, flavor="hive")
            listing = ds.dataset(base, format="parquet", schema=pa.schema([]), filesystem=self._fs, partitioning=partitioning)
            self._files = sorted((f.path, ds.get_partition_keys(f.partition_expression)) for f in listing.get_fragments())
        else:
            self._files = [(p, {}) for p in paths]
        self._schema = None

    @property
    def schema(self):
        if self._schema is None:
            if self._files:
                self._schema = self._read(*self._files[0]).schema
            else:
                declared = [pa.field(f["name"], self._types.get(f["name"]) or pa.string()) for f in self._fields]
                self._schema = pa.schema([f for f in declared if f.name not in self._keys.names] + list(self._keys))
        return self._schema

    def to_table(self, columns=None):
        tables = [self._read(path, keys) for path, keys in self._files]
        if not tables:
            table = self.schema.empty_table()
        else:
            table = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")
        return table.select(columns) if columns is not None else table

    def to_batches(self, columns=None):
        for path, keys in self._files:
            table = self._read(path, keys)
            yield from (table.select(columns) if columns is not None else table).to_batches()

    def _read(self, path, keys):
        with self._fs.open_input_file(path) as stream:
            table = _READERS[self._fmt](stream, self._fields, self._types)
        # a partition key comes from the folder names, where the file holds it too (or JSON's reader made it up, empty)
        table = table.select([c for c in table.column_names if c not in self._keys.names])
        for field in self._keys:
            table = table.append_column(field, pa.array([keys.get(field.name)] * table.num_rows, field.type))
        return table


def _read_json(stream, fields, types):
    import pyarrow.json as pj

    # a date is text in a JSON file, which pyarrow reads only as a number of days: read as text, then cast
    explicit = pa.schema([(n, pa.string() if pa.types.is_date(t) else t) for n, t in types.items() if t is not None])
    data = stream.read()
    if not data.strip():
        # a file of no lines (an output of no rows), which pyarrow will not read: the contract's columns, empty
        return pa.schema([(n, t) for n, t in types.items() if t is not None]).empty_table()
    table = pj.read_json(pa.BufferReader(data), parse_options=pj.ParseOptions(explicit_schema=explicit, unexpected_field_behavior="infer"))
    for name, t in types.items():
        if t is not None and pa.types.is_date(t) and name in table.column_names:
            i = table.schema.get_field_index(name)
            table = table.set_column(i, pa.field(name, t), table.column(i).cast(t))
    return table


def _read_avro(stream, fields, types):
    fastavro = _need("fastavro", "avro", "avro")
    reader = fastavro.reader(stream)
    schema = _arrow_of_avro(reader.writer_schema)
    return pa.Table.from_pylist(list(reader), schema=schema)


def _arrow_of_avro(schema):
    """The Arrow schema of an Avro record schema (as fastavro gives it, parsed)."""
    return pa.schema([pa.field(f["name"], *_avro_type(f["type"])) for f in schema["fields"]])


def _avro_type(avro):
    """``(Arrow type, nullable)`` of an Avro type."""
    if isinstance(avro, list):
        others = [t for t in avro if t != "null"]
        if len(others) != 1:
            raise CheckNotRun(f"Invaract reads an Avro union only of one type and null, not {avro}")
        return _avro_type(others[0])[0], len(others) != len(avro)
    if isinstance(avro, str):
        return {"null": pa.null(), "boolean": pa.bool_(), "int": pa.int32(), "long": pa.int64(), "float": pa.float32(),
                "double": pa.float64(), "bytes": pa.binary(), "string": pa.string()}[avro], False
    logical, kind = avro.get("logicalType"), avro["type"]
    if logical == "decimal":
        return pa.decimal128(avro["precision"], avro.get("scale", 0)), False
    if logical == "date":
        return pa.date32(), False
    if logical in ("timestamp-millis", "timestamp-micros"):
        return pa.timestamp("us", tz="UTC"), False
    if logical in ("local-timestamp-millis", "local-timestamp-micros"):
        return pa.timestamp("us"), False
    if kind == "record":
        return pa.struct([pa.field(f["name"], *_avro_type(f["type"])) for f in avro["fields"]]), False
    if kind == "array":
        return pa.list_(pa.field("element", *_avro_type(avro["items"]))), False
    if kind == "map":
        return pa.map_(pa.string(), _avro_type(avro["values"])[0]), False
    if kind == "enum":
        return pa.string(), False
    if kind == "fixed":
        return pa.binary(), False
    return _avro_type(kind)


def _read_excel(stream, fields, types):
    openpyxl = _need("openpyxl", "excel", "excel")
    book = openpyxl.load_workbook(io.BytesIO(stream.read()), read_only=True, data_only=True)
    try:
        rows = [r for r in book.worksheets[0].iter_rows(values_only=True) if any(v is not None for v in r)]
    finally:
        book.close()
    if not rows:
        return pa.table({})
    names = [str(n) if n is not None else f"_c{i}" for i, n in enumerate(rows[0])]
    columns = {}
    for i, name in enumerate(names):
        values = [r[i] if i < len(r) else None for r in rows[1:]]
        t = types.get(name)
        columns[name] = _cells(name, values, t) if t is not None else _guess(values)
    return pa.table(columns)


def _cells(name, values, t):
    """A sheet's column as the contract's type ``t``."""
    def one(v):
        if v is None:
            return None
        if pa.types.is_date(t) and isinstance(v, datetime.datetime):
            return v.date()
        if pa.types.is_timestamp(t) and isinstance(v, datetime.datetime) and t.tz is not None and v.tzinfo is None:
            return v.replace(tzinfo=datetime.timezone.utc)
        if pa.types.is_decimal(t) and isinstance(v, float):
            return decimal.Decimal(repr(v))
        if pa.types.is_integer(t) and isinstance(v, float) and v.is_integer():
            return int(v)
        if pa.types.is_floating(t) and isinstance(v, int) and not isinstance(v, bool):
            return float(v)
        if pa.types.is_string(t) and not isinstance(v, str):
            return str(v)
        return v

    try:
        return pa.array([one(v) for v in values], t)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError) as e:
        raise CheckNotRun(f"Invaract cannot read the sheet's column {name!r} as {t}: {e}") from e


def _guess(values):
    try:
        return pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return pa.array([None if v is None else str(v) for v in values], pa.string())


_READERS = {"json": _read_json, "avro": _read_avro, "excel": _read_excel}


# ---- writing -----------------------------------------------------------------------------------------------------------

def writer(stream, fmt, schema):
    """A writer of ``fmt`` (not Parquet or CSV) to the binary ``stream``, which it closes with itself."""
    return {"json": JsonWriter, "orc": OrcWriter, "avro": AvroWriter, "feather": FeatherWriter, "excel": ExcelWriter}[fmt](stream, schema)


class OrcWriter:
    def __init__(self, stream, schema):
        try:
            import pyarrow.orc as orc
        except ImportError as e:
            raise CheckNotRun("this pyarrow has no ORC support (pyarrow on Windows has none), so Invaract cannot write orc") from e
        self._stream, self._out, self._schema, self._written = stream, orc.ORCWriter(stream), schema, False

    def write_batch(self, batch):
        self._out.write(pa.Table.from_batches([batch]))
        self._written = True

    def close(self):
        try:
            if not self._written:
                # an ORC file holds its schema only once something is written: an output of no rows writes an empty table
                self._out.write(self._schema.empty_table())
            self._out.close()
        finally:
            self._stream.close()


class FeatherWriter:
    """Arrow's file format, compressed with LZ4 as pyarrow's ``write_feather`` compresses it by default."""

    def __init__(self, stream, schema):
        options = pa.ipc.IpcWriteOptions(compression="lz4") if pa.Codec.is_available("lz4") else None
        self._stream, self._out = stream, pa.ipc.new_file(stream, schema, options=options)

    def write_batch(self, batch):
        self._out.write_batch(batch)

    def close(self):
        try:
            self._out.close()
        finally:
            self._stream.close()


class JsonWriter:
    """JSON lines as Spark writes them (module docstring)."""

    def __init__(self, stream, schema):
        self._stream = stream

    def write_batch(self, batch):
        self._stream.write("".join(_json_line(row) for row in batch.to_pylist()).encode("utf-8"))

    def close(self):
        self._stream.close()


def _json_line(row):
    return _json_object(row) + "\n"


def _json_object(row):
    return "{" + ",".join(f"{json.dumps(k)}:{_json_value(v)}" for k, v in row.items() if v is not None) + "}"


def _json_value(v):
    if isinstance(v, bool) or v is None:
        return json.dumps(v)
    if isinstance(v, float):
        # Spark writes the numbers JSON has no literal for as text
        return json.dumps("NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")) if not math.isfinite(v) else json.dumps(v)
    if isinstance(v, decimal.Decimal):
        return format(v, "f")
    if isinstance(v, datetime.datetime):
        return json.dumps(_iso(v))
    if isinstance(v, datetime.date):
        return json.dumps(v.isoformat())
    if isinstance(v, bytes):
        return json.dumps(base64.b64encode(v).decode("ascii"))
    if isinstance(v, dict):
        return _json_object(v)
    if isinstance(v, list):
        if v and all(isinstance(e, tuple) and len(e) == 2 for e in v):
            # a map, as to_pylist gives it: its keys as the object's
            return _json_object({str(k): e for k, e in v})
        return "[" + ",".join("null" if e is None else _json_value(e) for e in v) + "]"
    if isinstance(v, (datetime.time, datetime.timedelta)):
        return json.dumps(str(v))
    return json.dumps(v)


def _iso(v):
    """A timestamp as Spark writes it: to the millisecond when it has nothing finer, ``Z`` for UTC."""
    text = v.isoformat(timespec="milliseconds" if v.microsecond % 1000 == 0 else "microseconds")
    return text[:-6] + "Z" if text.endswith("+00:00") else text


class AvroWriter:
    """Avro as Spark's ``avro`` source lays it out (module docstring); the record is named ``topLevelRecord``, as Spark
    names it."""

    def __init__(self, stream, schema):
        fastavro = _need("fastavro", "avro", "avro")
        from fastavro.write import Writer

        self._stream = stream
        self._names = iter(range(1, 1_000_000))
        self._out = Writer(stream, fastavro.parse_schema(self._record("topLevelRecord", schema)))

    def _record(self, name, fields):
        return {"type": "record", "name": name, "fields": [self._field(f) for f in fields]}

    def _field(self, field):
        if not _AVRO_NAME.match(field.name):
            raise CheckNotRun(f"Avro names a column with letters, digits and _, not {field.name!r}: rename it to write avro")
        t = self._type(field.type)
        return {"name": field.name, "type": ["null", t], "default": None} if field.nullable else {"name": field.name, "type": t}

    def _type(self, t):
        if pa.types.is_boolean(t):
            return "boolean"
        if pa.types.is_integer(t):
            return "long" if t.bit_width == 64 or (pa.types.is_unsigned_integer(t) and t.bit_width == 32) else "int"
        if pa.types.is_float32(t) or pa.types.is_float16(t):
            return "float"
        if pa.types.is_float64(t):
            return "double"
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            return "string"
        if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
            return "bytes"
        if pa.types.is_date(t):
            return {"type": "int", "logicalType": "date"}
        if pa.types.is_timestamp(t):
            return {"type": "long", "logicalType": "timestamp-micros" if t.tz is not None else "local-timestamp-micros"}
        if pa.types.is_decimal(t):
            return {"type": "bytes", "logicalType": "decimal", "precision": t.precision, "scale": t.scale}
        if pa.types.is_struct(t):
            return self._record(f"record{next(self._names)}", [t.field(i) for i in range(t.num_fields)])
        if pa.types.is_list(t) or pa.types.is_large_list(t):
            item = self._type(t.value_type)
            return {"type": "array", "items": ["null", item] if t.value_field.nullable else item}
        if pa.types.is_map(t) and pa.types.is_string(t.key_type):
            item = self._type(t.item_type)
            return {"type": "map", "values": ["null", item] if t.item_field.nullable else item}
        raise CheckNotRun(f"Invaract cannot write a column of type {t} as avro")

    def write_batch(self, batch):
        for row in batch.to_pylist():
            self._out.write(_avro_row(row))

    def close(self):
        try:
            self._out.flush()
        finally:
            self._stream.close()


_AVRO_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _avro_row(value):
    """A row as fastavro takes it: a map (pairs, as to_pylist gives it) as a dict."""
    if isinstance(value, dict):
        return {k: _avro_row(v) for k, v in value.items()}
    if isinstance(value, list):
        if value and all(isinstance(e, tuple) and len(e) == 2 for e in value):
            return {k: _avro_row(v) for k, v in value}
        return [_avro_row(e) for e in value]
    return value


class ExcelWriter:
    """One sheet: a header row naming the columns, then a row per row (module docstring)."""

    def __init__(self, stream, schema):
        openpyxl = _need("openpyxl", "excel", "excel")
        for field in schema:
            t = field.type
            if pa.types.is_nested(t) or pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
                raise CheckNotRun(f"Invaract cannot write the column {field.name!r} ({t}) to an Excel sheet: a cell holds no {t}")
        self._stream, self._rows = stream, 0
        self._book = openpyxl.Workbook(write_only=True)
        self._sheet = self._book.create_sheet()
        self._sheet.append(list(schema.names))

    def write_batch(self, batch):
        self._rows += batch.num_rows
        if self._rows > EXCEL_ROWS:
            raise CheckNotRun(f"an Excel sheet holds at most {EXCEL_ROWS:,} rows besides its header; this output has more")
        for row in batch.to_pylist():
            self._sheet.append([_cell(v) for v in row.values()])

    def close(self):
        try:
            # openpyxl writes a zip archive, which wants a stream it can seek in
            buffer = io.BytesIO()
            self._book.save(buffer)
            self._stream.write(buffer.getvalue())
        finally:
            self._stream.close()


def _cell(v):
    if isinstance(v, datetime.datetime) and v.tzinfo is not None:
        return v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return v
