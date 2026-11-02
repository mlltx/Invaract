# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Finds the calls in a Python job that read or write data outside the invaract package.

It checks only that such a call is *present*, which the syntax tree answers reliably; it never tries to work out what the
call does with its data (docs/PYTHON_IO_ADAPTER_DESIGN.md, section 7). A call the team means to keep is marked on its line
with ``# invaract: allow``, which shows up in review.

What it looks for, by the name the call resolves to through the file's imports:

- reads (``IVR001``): ``pandas.read_*``, ``polars.read_*`` and ``scan_*``, pyarrow's ``parquet``, ``csv``, ``feather``,
  ``json``, ``orc``, ``ipc`` and ``dataset`` readers, ``duckdb.read_*``;
- writes (``IVR002``): pyarrow's writers, a frame's ``to_csv``, ``to_parquet``, ... and ``write_*``, ``sink_*`` given a
  destination, ``open`` in a mode that writes, ``Path.write_text`` and ``write_bytes``;
- connections (``IVR003``): database drivers' ``connect``, ``sqlalchemy.create_engine``, cloud storage clients and
  filesystems;
- SQL that reads or writes files (``IVR004``): ``COPY``, ``ATTACH``, ``EXPORT DATABASE``, ``read_parquet(...)`` and a quoted
  path after ``FROM``, in a string passed to ``sql``, ``execute`` or ``query``.

Names reached dynamically (``getattr(pd, "read_csv")``, ``__import__``) are not followed: the lint is a guard against the
ordinary way of writing a job, not against one built to evade it.
"""
import ast
import io
import re
import tokenize
from dataclasses import dataclass

READ = "IVR001"
WRITE = "IVR002"
CONNECT = "IVR003"
SQL = "IVR004"

ALLOW = re.compile(r"#\s*invaract:\s*allow\b")

_PYARROW_IO = r"pyarrow\.(?:parquet|csv|feather|json|orc|ipc|dataset)"

#: Calls by the qualified name they resolve to, each with what it does.
_MODULE_CALLS = [
    (re.compile(r"pandas\.read_\w+"), READ),
    (re.compile(r"polars\.(?:read|scan)_\w+"), READ),
    (re.compile(_PYARROW_IO + r"\.(?:read_\w+|open_\w+|dataset|ParquetFile|ParquetDataset)"), READ),
    (re.compile(r"duckdb\.(?:read_\w+|from_csv_auto|from_parquet)"), READ),
    (re.compile(_PYARROW_IO + r"\.(?:write_\w+|new_\w+|ParquetWriter|CSVWriter|RecordBatchFileWriter|RecordBatchStreamWriter)"), WRITE),
    (re.compile(r"(?:sqlite3|psycopg2|psycopg|pymysql|pyodbc|cx_Oracle|oracledb|duckdb|mysql\.connector|snowflake\.connector)\.connect"), CONNECT),
    (re.compile(r"sqlalchemy\.create_engine|google\.cloud\.bigquery\.Client|google\.cloud\.storage\.Client"), CONNECT),
    (re.compile(r"boto3\.(?:client|resource)|s3fs\.S3FileSystem|gcsfs\.GCSFileSystem|adlfs\.AzureBlobFileSystem"), CONNECT),
    (re.compile(r"fsspec\.(?:open|open_files|filesystem)"), CONNECT),
]

#: Methods of a frame that write once they are given somewhere to write (pandas' ``to_*``, Polars' ``write_*``); without one,
#: most of them return the text instead.
_FRAME_WRITES = {
    "to_csv", "to_parquet", "to_json", "to_excel", "to_sql", "to_feather", "to_orc", "to_hdf", "to_pickle", "to_stata", "to_xml",
    "write_csv", "write_parquet", "write_json", "write_ndjson", "write_ipc", "write_ipc_stream", "write_avro", "write_excel",
    "write_database", "write_delta", "write_iceberg",
}
#: Keyword arguments that name a destination.
_DESTINATIONS = {
    "path", "path_or_buf", "excel_writer", "buf", "con", "name", "file", "target", "table_name", "connection", "uri",
    "workbook",
}
#: Methods that always write: Polars' streaming sinks, a ``Path``'s writers.
_ALWAYS_WRITE = {"sink_parquet", "sink_csv", "sink_ipc", "sink_ndjson", "write_text", "write_bytes"}
#: Functions that open a file, with the mode as their second argument.
_OPENERS = {"open", "io.open", "codecs.open"}
#: Methods that run SQL given as their first argument.
_SQL_METHODS = {"sql", "execute", "executemany", "query"}
_FILE_SQL = re.compile(r"\b(?:COPY|ATTACH|EXPORT\s+DATABASE)\b|\bread_(?:parquet|csv|json)\w*\s*\(|\bFROM\s+'", re.IGNORECASE)

_WHAT = {
    READ: "reads data outside invaract: read it with invaract.read(<name>)",
    WRITE: "writes data outside invaract: write it with invaract.write(<frame>, <name>)",
    CONNECT: "opens a database or storage connection outside invaract",
    SQL: "runs SQL that reads or writes files outside invaract",
}


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    col: int
    code: str
    call: str

    @property
    def message(self):
        return f"`{self.call}` {_WHAT[self.code]}, or mark the line `# invaract: allow`"

    def __str__(self):
        return f"{self.path}:{self.line}:{self.col}: {self.code} {self.message}"


def check_source(source, path="<source>"):
    """The calls in ``source`` that read or write data outside invaract and are not marked ``# invaract: allow``, in order.
    Raises ``SyntaxError`` for source that is not Python."""
    tree = ast.parse(source, filename=path)
    allowed = _allowed_lines(source)
    imports = _imports(tree)
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        code = _classify(node, imports)
        if code is None:
            continue
        if any(line in allowed for line in range(node.lineno, (node.end_lineno or node.lineno) + 1)):
            continue
        findings.append(Finding(path, node.lineno, node.col_offset + 1, code, ast.unparse(node.func)))
    return sorted(findings, key=lambda f: (f.line, f.col))


def check_file(path):
    with open(path, "rb") as f:
        source = f.read()
    return check_source(source.decode("utf-8"), path)


def _allowed_lines(source):
    lines = set()
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT and ALLOW.search(token.string):
            lines.add(token.start[0])
    return lines


def _imports(tree):
    """What each name bound by an import stands for, as a dotted path: ``pd`` -> ``pandas``, ``pq`` -> ``pyarrow.parquet``,
    ``read_csv`` -> ``pandas.read_csv``."""
    names = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    names[alias.asname] = alias.name
                else:
                    top = alias.name.split(".")[0]
                    names[top] = top
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                names[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return names


def _qualified(node, imports):
    """The dotted path an expression stands for through the imports (``pq.write_table`` -> ``pyarrow.parquet.write_table``),
    or ``None`` when it does not start at an import."""
    if isinstance(node, ast.Name):
        return imports.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _qualified(node.value, imports)
        return f"{base}.{node.attr}" if base else None
    return None


def _classify(call, imports):
    name = _qualified(call.func, imports)
    if name is not None:
        for pattern, code in _MODULE_CALLS:
            if pattern.fullmatch(name):
                return code
        if name in _OPENERS:
            return _open_mode(call)
    if isinstance(call.func, ast.Name) and call.func.id == "open" and "open" not in imports:
        return _open_mode(call)
    if isinstance(call.func, ast.Attribute):
        method = call.func.attr
        if method in _ALWAYS_WRITE:
            return WRITE
        if method in _FRAME_WRITES and (call.args or any(k.arg in _DESTINATIONS for k in call.keywords)):
            return WRITE
        if method == "open" and name is None:
            # Path(...).open(mode): the mode is its first argument
            return _mode_writes(call.args[0] if call.args else _keyword(call, "mode"))
        if method in _SQL_METHODS and call.args and _FILE_SQL.search(_text(call.args[0])):
            return SQL
    return None


def _open_mode(call):
    return _mode_writes(call.args[1] if len(call.args) > 1 else _keyword(call, "mode"))


def _mode_writes(mode):
    """A mode that writes, or one the source does not spell out; ``None`` for no mode (read) or a read mode."""
    if mode is None:
        return None
    if isinstance(mode, ast.Constant) and isinstance(mode.value, str):
        return WRITE if set(mode.value) & set("wax+") else None
    return WRITE


def _keyword(call, name):
    return next((k.value for k in call.keywords if k.arg == name), None)


def _text(node):
    """The literal text of a string or f-string argument (the parts written in the source)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return ""
