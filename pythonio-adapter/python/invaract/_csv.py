# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""CSV files laid out as their contract's ``csv:`` options say (decided 2026-11-06, docs/PYTHONIO_ADAPTER.md).

The options, and what each means, are Spark's CSV options, so one contract reads the same on every engine: ``delimiter``,
``header``, ``encoding``, ``quote``, ``escape``, ``nullValue``, ``dateFormat`` and ``timestampFormat`` (Java
``DateTimeFormatter`` patterns, ``dd/MM/yyyy``, translated here to the ``strptime`` patterns Arrow takes). A read or write uses
the options its dataset declares, with any a job gives (``csv=``, as a migrating job says how it read the file) in their place;
whatever it used is reported to the checker, which refuses options that differ from the declared ones. With no options at
all, a CSV file is read and written as before: comma-separated, a header line, UTF-8, pyarrow's own text for each value.
"""
import csv as _pycsv
import io
import re

import pyarrow as pa
import pyarrow.compute as pc

from . import _types
from .errors import CheckNotRun

NAMES = ("delimiter", "header", "encoding", "quote", "escape", "nullValue", "dateFormat", "timestampFormat")


def options(declared, given):
    """The options a read or write uses, as text: the dataset's declared ones, with any the job gives in their place."""
    used = {k: str(v) for k, v in (declared or {}).items()}
    for name, value in (given or {}).items():
        if name not in NAMES and name != "sep":
            raise ValueError(f"invaract does not know the CSV option {name!r}: it takes {', '.join(NAMES)}")
        used["delimiter" if name == "sep" else name] = ("true" if value else "false") if isinstance(value, bool) else str(value)
    return used


def character(options, name, default=None):
    """The one character option ``name`` stands for (``\\t`` is a tab), or ``default`` when it is not given."""
    text = options.get(name)
    if text is None:
        return default
    text = "\t" if text == "\\t" else text
    if len(text) != 1:
        raise CheckNotRun(f"Invaract reads and writes CSV with a one-character {name}, not {text!r}")
    return text


def header(options):
    return options.get("header", "true").strip().lower() == "true"


_PATTERN = re.compile(r"'[^']*'|([A-Za-z])\1*")
_FIELDS = {
    "yyyy": "%Y", "uuuu": "%Y", "yy": "%y", "uu": "%y", "MM": "%m", "M": "%m", "MMM": "%b", "MMMM": "%B", "dd": "%d", "d": "%d",
    "HH": "%H", "H": "%H", "hh": "%I", "h": "%I", "mm": "%M", "m": "%M", "ss": "%S", "s": "%S", "a": "%p",
    "EEE": "%a", "EEEE": "%A", "XXX": "%z", "XX": "%z", "X": "%z", "Z": "%z", "ZZZ": "%z",
}


def strptime_pattern(pattern):
    """The ``strptime`` pattern for a Java ``DateTimeFormatter`` pattern (``dd/MM/yyyy`` is ``%d/%m/%Y``); ``CheckNotRun`` for
    one with a field Arrow cannot read (fractions of a second, week numbers, zone names)."""
    out, at = [], 0
    for m in _PATTERN.finditer(pattern):
        out.append(pattern[at:m.start()].replace("%", "%%"))
        token = m.group(0)
        if token.startswith("'"):
            out.append(token[1:-1].replace("%", "%%") if len(token) > 2 else "'")
        elif token in _FIELDS:
            out.append(_FIELDS[token])
        else:
            raise CheckNotRun(f"Invaract cannot read or write the date pattern {pattern!r}: it does not take '{token}'")
        at = m.end()
    out.append(pattern[at:].replace("%", "%%"))
    return "".join(out)


# reading


def read_format(fields, options):
    """``(file format, finish)``: a pyarrow dataset CSV format reading with ``options``, and what turns a table or batch it
    reads into the contract's types (dates and timestamps in a declared pattern are read as text, then parsed)."""
    import pyarrow.csv as csv
    import pyarrow.dataset as ds

    types = _types.scalars(fields)
    patterns = {}
    for name, t in types.items():
        if t is not None and pa.types.is_date(t) and "dateFormat" in options:
            patterns[name] = (strptime_pattern(options["dateFormat"]), t)
        elif t is not None and pa.types.is_timestamp(t) and "timestampFormat" in options:
            patterns[name] = (strptime_pattern(options["timestampFormat"]), t)
    column_types = {n: (pa.string() if n in patterns else t) for n, t in types.items() if t is not None}
    read = {"encoding": options["encoding"]} if "encoding" in options else {}
    if not header(options):
        names = [f["name"] for f in fields]
        if names:
            read["column_names"] = names
        else:
            read["autogenerate_column_names"] = True
    parse = {"delimiter": character(options, "delimiter", ","), "quote_char": character(options, "quote", '"')}
    if "escape" in options:
        parse["escape_char"] = character(options, "escape")
    convert = {"column_types": column_types}
    if "nullValue" in options:
        convert.update(null_values=[options["nullValue"]], strings_can_be_null=True)
    file_format = ds.CsvFileFormat(read_options=csv.ReadOptions(**read), parse_options=csv.ParseOptions(**parse),
                                   convert_options=csv.ConvertOptions(**convert))

    def finish(data):
        for name, (fmt, t) in patterns.items():
            if name in data.schema.names:
                i = data.schema.get_field_index(name)
                parsed = pc.strptime(data.column(i), format=fmt, unit="us", error_is_null=False)
                if pa.types.is_date(t):
                    parsed = parsed.cast(t)
                elif t.tz is not None:
                    parsed = pc.assume_timezone(parsed, t.tz) if parsed.type.tz is None else parsed.cast(t)
                data = data.set_column(i, pa.field(name, parsed.type), parsed)
        return data

    return file_format, finish


# writing


class Writer:
    """Writes record batches as CSV text laid out as ``options`` say, to a binary stream it closes with itself."""

    def __init__(self, stream, schema, options):
        self._stream = stream
        self._options = options
        self._text = io.TextIOWrapper(stream, encoding=options.get("encoding", "UTF-8"), newline="")
        quote = character(options, "quote", '"')
        escape = character(options, "escape")
        self._csv = _pycsv.writer(self._text, delimiter=character(options, "delimiter", ","), quotechar=quote,
                                  escapechar=None if escape == quote else escape, doublequote=escape is None or escape == quote,
                                  quoting=_pycsv.QUOTE_MINIMAL, lineterminator="\n")
        self._null = options.get("nullValue", "")
        if header(options):
            self._csv.writerow(schema.names)

    def write_batch(self, batch):
        columns = [self._text_of(batch.column(i)) for i in range(batch.num_columns)]
        self._csv.writerows(zip(*columns))

    def _text_of(self, column):
        t = column.type
        if pa.types.is_date(t) and "dateFormat" in self._options:
            text = pc.strftime(column.cast(pa.timestamp("s")), format=strptime_pattern(self._options["dateFormat"]))
        elif pa.types.is_timestamp(t) and "timestampFormat" in self._options:
            text = pc.strftime(column, format=strptime_pattern(self._options["timestampFormat"]))
        else:
            text = column.cast(pa.string())
        return [self._null if v is None else v for v in text.to_pylist()]

    def close(self):
        self._text.flush()
        self._text.detach()
        self._stream.close()
