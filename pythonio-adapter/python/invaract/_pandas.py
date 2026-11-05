# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""pandas frames, written as their contract declares them, and read in the dtypes a job asks for (decided 2026-11-05,
docs/PYTHONIO_ADAPTER.md).

pandas has no way to hold some of the types a contract declares: an integer column with a missing value is ``float64``, a
date is a ``datetime64`` at midnight, a decimal is an ``object`` column of ``Decimal`` values of whatever precision they
happen to have, and a column with no values has no type at all. So a pandas frame's columns are converted to the types the
contract declares for them, **when no value changes**: the conversion is made, converted back, and kept only if every value
comes back as it was. ``1.0`` and a missing value become a ``long``; ``1.5`` does not, and the write is refused as a type
mismatch, as it was. A conversion is only between values of one kind (numbers, text, times), and never adds or removes a
time zone, which would say something about the data the frame does not. Polars, DuckDB and Arrow frames hold the declared
types themselves, so a mismatch there is the job's, and they are written as they are.

When a column is left unconverted, the note saying why (in pandas' terms, with the fix) is added to the refusal.

A named index level the contract declares as a column (a ``groupby`` result's keys) is written as that column; any other
index is pandas' bookkeeping and is not written.
"""
import datetime
import decimal

import pyarrow as pa
import pyarrow.compute as pc

from . import _frames

#: ``dtype_backend`` values ``invaract.read`` takes, as pandas' own readers do.
DTYPE_BACKENDS = ("numpy_nullable", "pyarrow")


def is_frame(frame):
    return type(frame).__module__.split(".")[0] == "pandas" and type(frame).__name__ == "DataFrame"


def to_arrow(frame, fields=()):
    """``(table, notes)``: ``frame`` as an Arrow table, each column the contract declares in ``fields`` converted to the
    declared type where no value changes, and a note for each declared column that was not."""
    declared = {f["name"]: f for f in fields}
    frame = _with_index_columns(frame, declared)
    try:
        table = pa.Table.from_pandas(frame, preserve_index=False)
    except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
        raise _mixed_column(frame, e) from e
    notes = []
    for i, name in enumerate(table.column_names):
        field = declared.get(name)
        target = _frames.arrow_type(field["type"]) if field else None
        if target is None:
            continue
        column, note = conform(table.column(i), target)
        if note:
            notes.append(f"'{name}' ({frame.dtypes.iloc[i]} in pandas) is not written as {field['type']}: {note}"
                         .replace("{name}", name))
        elif column is not table.column(i):
            table = table.set_column(i, table.schema.field(i).with_type(column.type), column)
    return table, notes


def conform(column, target):
    """``(column, note)``: ``column`` as ``target`` if converting it changes no value; else ``column`` itself and why not
    (``None`` when it already is what the contract declares)."""
    if _matches(column.type, target):
        return column, None
    if pa.types.is_dictionary(column.type):
        # a pandas category: its values, which are what is written
        column = column.cast(column.type.value_type)
    source = column.type
    if column.null_count == len(column):
        # nothing to change: missing values, or no rows at all, are missing values of any type
        return pa.chunked_array([pa.nulls(len(column), target)], target), None
    if _kind(source) != _kind(target):
        return column, _UNLIKE.get(_kind(target), "convert it in the job")
    if pa.types.is_timestamp(target) and (_zone(source) is None) != (target.tz is None):
        return column, _NAIVE if _zone(source) is None else _AWARE
    try:
        # Arrow's checked conversion refuses a value that does not fit (a decimal's precision, an integer's range); one
        # that fits but rounds (a float as a decimal or a narrower float) is found by converting back
        converted = _cast(column, target, safe=True)
        changed = _changed(converted, column)
    except _ARROW_ERRORS:
        return column, _changes(source, target, _example(column, target))
    if pc.any(changed).as_py():
        return column, _changes(source, target, pc.filter(column, changed)[0].as_py())
    return converted, None


def as_pandas(table, dtype_backend=None):
    """``table`` as a pandas frame: in pandas' default dtypes, or, as pandas' own readers take it, ``numpy_nullable``
    (``Int64``, ``boolean``, ``string``: an integer column with a missing value stays an integer) or ``pyarrow``
    (``ArrowDtype``)."""
    import pandas as pd

    if dtype_backend is None:
        return table.to_pandas()
    if dtype_backend == "pyarrow":
        return table.to_pandas(types_mapper=pd.ArrowDtype)
    nullable = {
        pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
        pa.uint8(): pd.UInt8Dtype(), pa.uint16(): pd.UInt16Dtype(), pa.uint32(): pd.UInt32Dtype(), pa.uint64(): pd.UInt64Dtype(),
        pa.bool_(): pd.BooleanDtype(), pa.float32(): pd.Float32Dtype(), pa.float64(): pd.Float64Dtype(),
        pa.string(): pd.StringDtype(), pa.large_string(): pd.StringDtype(),
    }
    return table.to_pandas(types_mapper=nullable.get)


def check_dtype_backend(dtype_backend, as_):
    if dtype_backend is None:
        return
    if dtype_backend not in DTYPE_BACKENDS:
        raise ValueError(f"invaract.read cannot take dtype_backend={dtype_backend!r}: give one of {', '.join(DTYPE_BACKENDS)}")
    if as_ != "pandas":
        raise ValueError(f"dtype_backend is for a pandas read, not as_={as_!r}")


# what a column is, and what it can become


def _matches(source, target):
    """The column is already what the checker takes for ``target``: its own type, text of either width or a pandas category
    of text, or a timestamp of another unit with the same time zone (or none)."""
    if source == target:
        return True
    if pa.types.is_string(target):
        return _kind(source) == "text"
    if pa.types.is_timestamp(target) and pa.types.is_timestamp(source):
        return source.tz == target.tz
    return False


def _kind(t):
    if pa.types.is_dictionary(t):
        return _kind(t.value_type)
    if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t):
        return "number"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "text"
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return "time"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary"
    return None


def _zone(t):
    return t.tz if pa.types.is_timestamp(t) else None


_ARROW_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)


def _cast(column, target, safe):
    """``column`` as ``target``. Integers become decimals by way of a decimal wide enough for any 64-bit integer, which
    Arrow requires."""
    if pa.types.is_integer(column.type) and pa.types.is_decimal(target):
        column = column.cast(pa.decimal128(20, 0), safe=safe)
    return column.cast(target, safe=safe)


def _changed(converted, column):
    """Which values of ``column`` do not come back as they were from ``converted``."""
    return pc.fill_null(pc.not_equal(_back(converted, column.type), column), False)


def _example(column, target):
    """A value of ``column`` that does not fit ``target``, for the note; ``None`` if none can be told apart."""
    try:
        changed = _changed(_cast(column, target, safe=False), column)
        if not pc.any(changed).as_py() and pa.types.is_decimal(target):
            # an unchecked decimal keeps a value too wide for its precision: the widest values are the ones
            changed = pc.greater_equal(pc.abs(column.cast(pa.float64(), safe=False)), 10.0 ** (target.precision - target.scale))
        return pc.filter(column, changed)[0].as_py() if pc.any(changed).as_py() else None
    except _ARROW_ERRORS:
        return None


def _back(converted, source):
    """``converted`` as ``source`` again. A decimal becomes a float by way of its text, the nearest float to it: Arrow's own
    conversion is not exact (``19.99`` comes back ``19.990000000000002``)."""
    if pa.types.is_decimal(converted.type) and pa.types.is_floating(source):
        return converted.cast(pa.string()).cast(source)
    return converted.cast(source, safe=False)


_UNLIKE = {
    "number": "it does not hold numbers; convert it in the job, for example pd.to_numeric(df['{name}'])",
    "text": "it does not hold text; convert it in the job, for example df['{name}'].astype('string')",
    "time": "it does not hold dates or times; convert it in the job, for example pd.to_datetime(df['{name}'])",
    "boolean": "it does not hold true and false; convert it in the job, for example df['{name}'].astype('boolean')",
}
_NAIVE = ("its datetimes have no time zone, and the contract's type is a moment in UTC: say which zone they are in, for "
          "example df['{name}'].dt.tz_localize('UTC'), or declare the field timestamp_ntz")
_AWARE = ("its datetimes have a time zone, and the contract's type has none: df['{name}'].dt.tz_convert(None) gives them in "
          "UTC without it, or declare the field timestamp")


def _changes(source, target, example):
    value = "a value" if example is None else _shown(example)
    if pa.types.is_floating(source) and pa.types.is_integer(target):
        why = (f"{value} is not a whole number, or is out of range (a whole-number column with missing values is float64 "
               "in pandas, and is written as an integer as it is); round it in the job if that is intended")
    elif pa.types.is_integer(target) or pa.types.is_floating(target):
        why = f"{value} does not fit {target} exactly; declare a wider type, or check the data"
    elif pa.types.is_decimal(target):
        why = (f"{value} does not fit {target} exactly; round it to the declared scale in the job "
               f"(df['{{name}}'].round({target.scale})), or build the column from decimal.Decimal values")
    elif pa.types.is_date(target):
        why = f"{value} has a time of day; drop it in the job if that is intended (df['{{name}}'].dt.normalize())"
    else:
        why = f"{value} would change as {target}"
    return why


def _shown(value):
    """A value as the job would see it printed: a date, time or decimal as its text, anything else as Python writes it."""
    return str(value) if isinstance(value, (datetime.date, decimal.Decimal)) else repr(value)


def _with_index_columns(frame, declared):
    """``frame`` with each named index level the contract declares, and the frame does not already hold, as a column."""
    levels = [n for n in frame.index.names if n is not None and n in declared and n not in frame.columns]
    return frame.reset_index(level=levels) if levels else frame


def _mixed_column(frame, error):
    """A column pandas holds as ``object`` with values of more than one kind cannot be one column of a file: which one."""
    for name in frame.columns:
        if frame[name].dtype != object:
            continue
        try:
            pa.array(frame[name], from_pandas=True)
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            kinds = sorted({type(v).__name__ for v in frame[name].dropna()})
            return TypeError(f"invaract.write: column '{name}' holds values of more than one kind ({', '.join(kinds)}), which "
                             f"pandas keeps in one object column and a file cannot: give it one type in the job, for "
                             f"example df['{name}'].astype('string')")
    return error
