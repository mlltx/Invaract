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

Each ``Finding`` says where the call is (its start and end, the function or class it is in), what it is (the call as written,
the name it resolves to, the rule that matched, the literal path or mode it names), and whether a ``# invaract: allow`` marker
keeps it, with the reason the marker gives: enough for a report to locate, group and count findings across files and runs
(``Finding.id`` stays the same when lines above the call move). ``check_source`` returns the findings not marked;
``scan_source`` returns every one, marked or not.

Names reached dynamically (``getattr(pd, "read_csv")``, ``__import__``) are not followed: the lint is a guard against the
ordinary way of writing a job, not against one built to evade it.
"""
import ast
import hashlib
import io
import re
import tokenize
from dataclasses import dataclass
from typing import Optional

READ = "IVR001"
WRITE = "IVR002"
CONNECT = "IVR003"
SQL = "IVR004"

ALLOW = re.compile(r"#\s*invaract:\s*allow\b(?P<reason>.*)")

#: What each code is about, as a word a report can group by.
CATEGORIES = {READ: "read", WRITE: "write", CONNECT: "connect", SQL: "sql"}

_PYARROW_IO = r"pyarrow\.(?:parquet|csv|feather|json|orc|ipc|dataset)"

#: Calls by the qualified name they resolve to, each with what it does and the rule's name (``Finding.rule``).
_MODULE_CALLS = [
    (re.compile(r"pandas\.read_\w+"), READ, "pandas-read"),
    (re.compile(r"polars\.(?:read|scan)_\w+"), READ, "polars-read"),
    (re.compile(_PYARROW_IO + r"\.(?:read_\w+|open_\w+|dataset|ParquetFile|ParquetDataset)"), READ, "pyarrow-read"),
    (re.compile(r"duckdb\.(?:read_\w+|from_csv_auto|from_parquet)"), READ, "duckdb-read"),
    (re.compile(_PYARROW_IO + r"\.(?:write_\w+|new_\w+|ParquetWriter|CSVWriter"
                              r"|RecordBatchFileWriter|RecordBatchStreamWriter)"), WRITE, "pyarrow-write"),
    (re.compile(r"(?:sqlite3|psycopg2|psycopg|pymysql|pyodbc|cx_Oracle|oracledb|duckdb"
                r"|mysql\.connector|snowflake\.connector)\.connect"), CONNECT, "database-connect"),
    (re.compile(r"sqlalchemy\.create_engine"), CONNECT, "sqlalchemy-engine"),
    (re.compile(r"google\.cloud\.bigquery\.Client|google\.cloud\.storage\.Client"), CONNECT, "cloud-client"),
    (re.compile(r"boto3\.(?:client|resource)|s3fs\.S3FileSystem|gcsfs\.GCSFileSystem|adlfs\.AzureBlobFileSystem"), CONNECT,
     "storage-client"),
    (re.compile(r"fsspec\.(?:open|open_files|filesystem)"), CONNECT, "fsspec"),
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
#: A ``Path``'s writers, whose place is the ``Path`` itself.
_PATH_WRITES = {"write_text", "write_bytes"}
#: Methods that always write: Polars' streaming sinks, a ``Path``'s writers.
_ALWAYS_WRITE = {"sink_parquet", "sink_csv", "sink_ipc", "sink_ndjson"} | _PATH_WRITES
#: Functions that open a file, with the mode as their second argument (the builtin ``open`` resolves to ``"open"``).
_OPENERS = {"open", "io.open", "codecs.open"}
#: Mode letters that write.
_WRITE_MODE = frozenset("wax+")
#: Methods that run SQL given as their first argument.
_SQL_METHODS = {"sql", "execute", "executemany", "query"}
_FILE_SQL = re.compile(r"\b(?:COPY|ATTACH|EXPORT\s+DATABASE)\b|\bread_(?:parquet|csv|json)\w*\s*\(|\bFROM\s+'", re.IGNORECASE)

_WHAT = {
    READ: "reads data outside invaract: read it with invaract.read(<name>)",
    WRITE: "writes data outside invaract: write it with invaract.write(<frame>, <name>)",
    CONNECT: "opens a database or storage connection outside invaract",
    SQL: "runs SQL that reads or writes files outside invaract",
}


#: The longest ``Finding.snippet`` and ``Finding.target``; longer text is cut, with an ellipsis.
_LONGEST = 200
#: A password in a URL (``postgresql://user:secret@host``): never copied into a finding, which may be published.
_PASSWORD = re.compile(r"(\w[\w+.-]*://[^/\s:@'\"]+):[^@\s/'\"]+@")


@dataclass(frozen=True)
class Finding:
    """A call that reads or writes data outside invaract.

    ``path``, ``line`` and ``col`` (1-based) are where the call starts, ``end_line`` and ``end_col`` where it ends. ``code``
    is ``IVR001`` to ``IVR004`` (``category`` says it in a word), and ``rule`` names the rule that matched (``pandas-read``,
    ``open-write-mode``, ...). ``call`` is the called expression as written, ``resolved`` the name it stands for through the
    file's imports (``pandas.read_csv`` for ``pd.read_csv``; ``None`` for a method of a value, such as ``df.to_parquet``), and
    ``scope`` the function or class it is in (``Loader.load``; ``<module>`` at the top level). ``end_col`` is the column just
    after the call's last character. ``snippet`` is the whole call on one line, as Python reads it (so reformatting the source
    does not change it), and ``target`` the path or URL it names when the source spells it out; ``detail`` says what
    made it count when the name alone does not (the mode of an ``open``, the SQL keyword). ``allowed`` is true when a
    ``# invaract: allow`` marker on one of its lines keeps it, and ``reason`` is what the marker says after it. A password
    in a URL is never copied into ``snippet`` or ``target``.
    """

    path: str
    line: int
    col: int
    code: str
    call: str
    end_line: int = 0
    end_col: int = 0
    resolved: Optional[str] = None
    scope: str = "<module>"
    rule: str = ""
    snippet: str = ""
    target: Optional[str] = None
    detail: Optional[str] = None
    allowed: bool = False
    reason: Optional[str] = None

    @property
    def category(self):
        return CATEGORIES[self.code]

    @property
    def message(self):
        return f"`{self.call}` {_WHAT[self.code]}, or mark the line `# invaract: allow`"

    @property
    def id(self):
        """A short identifier that stays the same while the call does: from the file, the rule, the function it is in and
        the call as written, not its line, so a finding keeps its identifier when lines above it are added or removed. Two
        identical calls in the same function share it."""
        key = "\0".join((self.path, self.code, self.rule, self.scope, self.snippet))
        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

    def to_dict(self):
        """The finding as JSON-ready data, by the names reports and events use."""
        return {
            "id": self.id, "code": self.code, "category": self.category, "rule": self.rule, "path": self.path,
            "line": self.line, "column": self.col, "endLine": self.end_line, "endColumn": self.end_col, "call": self.call,
            "resolved": self.resolved, "scope": self.scope, "snippet": self.snippet, "target": self.target,
            "detail": self.detail, "allowed": self.allowed, "reason": self.reason, "message": self.message,
        }

    def __str__(self):
        return f"{self.path}:{self.line}:{self.col}: {self.code} {self.message}"


def check_source(source, path="<source>"):
    """The calls in ``source`` that read or write data outside invaract and are not marked ``# invaract: allow``, in order.
    Raises ``SyntaxError`` for source that is not Python."""
    return [f for f in scan_source(source, path) if not f.allowed]


def scan_source(source, path="<source>"):
    """Every call in ``source`` that reads or writes data outside invaract, marked ``# invaract: allow`` or not
    (``Finding.allowed``), in order. Raises ``SyntaxError`` for source that is not Python."""
    tree = ast.parse(source, filename=path)
    allowed = _allowed_lines(source)
    imports = _imports(tree)
    findings = []
    for node, scope in _calls(tree):
        match = _classify(node, imports)
        if match is None:
            continue
        code, rule, detail = match
        reasons = [allowed[line] for line in range(node.lineno, node.end_lineno + 1) if line in allowed]
        findings.append(Finding(
            path, node.lineno, node.col_offset + 1, code, ast.unparse(node.func),
            end_line=node.end_lineno, end_col=node.end_col_offset + 1, resolved=_qualified(node.func, imports),
            scope=".".join(scope) or "<module>", rule=rule, snippet=_short(" ".join(ast.unparse(node).split())),
            target=_target(node, code, rule), detail=detail, allowed=bool(reasons), reason=next((r for r in reasons if r), None),
        ))
    return sorted(findings, key=lambda f: (f.line, f.col))


def check_file(path):
    """``check_source`` of the file at ``path``, read as Python reads it (UTF-8, or the encoding its first lines declare)."""
    return [f for f in scan_file(path) if not f.allowed]


def scan_file(path):
    """``scan_source`` of the file at ``path``, read as ``check_file`` reads it."""
    with tokenize.open(path) as f:
        source = f.read()
    return scan_source(source, path)


def _allowed_lines(source):
    """The lines a ``# invaract: allow`` marker is on, each with the reason written after it (``""`` for none)."""
    lines = {}
    if "invaract" not in source:
        return lines  # no marker can be there: skip tokenizing
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            marker = ALLOW.search(token.string)
            if marker:
                lines[token.start[0]] = marker.group("reason").strip(" \t-:,;")
    return lines


def _calls(tree):
    """Every call in ``tree``, with the names of the functions and classes it is inside, outermost first."""
    stack = [(tree, ())]
    while stack:
        node, scope = stack.pop()
        if isinstance(node, ast.Call):
            yield node, scope
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            scope = scope + (node.name,)
        stack.extend((child, scope) for child in ast.iter_child_nodes(node))


def _imports(tree):
    """What each name bound by an import stands for, as a dotted path: ``pd`` -> ``pandas``, ``pq`` -> ``pyarrow.parquet``,
    ``read_csv`` -> ``pandas.read_csv``. The builtin ``open`` stands for ``open`` unless an import rebinds it."""
    names = {"open": "open"}
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
    """``(code, rule, detail)`` for a call that reads or writes data outside invaract, else ``None``."""
    name = _qualified(call.func, imports)
    if name is not None:
        for pattern, code, rule in _MODULE_CALLS:
            if pattern.fullmatch(name):
                return code, rule, None
        if name in _OPENERS:
            return _opened(_argument(call, 1, "mode"), "open-write-mode")
    if isinstance(call.func, ast.Attribute):
        method = call.func.attr
        if method in _ALWAYS_WRITE:
            return WRITE, "always-writes", None
        if method in _FRAME_WRITES and (call.args or any(k.arg in _DESTINATIONS for k in call.keywords)):
            return WRITE, "frame-write", None
        if method == "open" and name is None:
            # Path(...).open(mode): the mode is its first argument
            return _opened(_argument(call, 0, "mode"), "path-open-write-mode")
        if method in _SQL_METHODS and call.args:
            keyword = _FILE_SQL.search(_text(call.args[0]))
            if keyword:
                return SQL, "file-sql", " ".join(keyword.group(0).split())
    return None


def _opened(mode, rule):
    """A write when ``mode`` writes, or when the source does not spell it out; ``None`` for no mode (read) or a read mode."""
    if mode is None:
        return None
    if isinstance(mode, ast.Constant) and isinstance(mode.value, str):
        return (WRITE, rule, f"mode {mode.value!r}") if _WRITE_MODE.intersection(mode.value) else None
    return WRITE, rule, "mode not spelled out"


def _argument(call, position, keyword):
    """The argument at ``position``, or else the one passed as ``keyword``; ``None`` when neither is given."""
    if len(call.args) > position:
        return call.args[position]
    return next((k.value for k in call.keywords if k.arg == keyword), None)


def _target(call, code, rule):
    """The path or URL a read or write names: the first of its arguments written as a string (``pq.write_table(t,
    "out.parquet")``; an ``open``'s first only), else a destination keyword's (``path=``); ``None`` when the source does not spell one out, for a connection (whose
    argument may be credentials) and for SQL (whose text is the query, not a place)."""
    if code not in (READ, WRITE):
        return None
    # an open's second argument is its mode, never a place
    positional = call.args[:1] if rule == "open-write-mode" else call.args
    candidates = positional + [k.value for k in call.keywords if k.arg in _DESTINATIONS]
    if rule == "path-open-write-mode" or (rule == "always-writes" and call.func.attr in _PATH_WRITES):
        # Path('a').write_text(...): the place is the Path, not the first argument (the text, or the mode)
        owner = call.func.value
        candidates = owner.args[:1] if isinstance(owner, ast.Call) else []
    literal = next((c.value for c in candidates if isinstance(c, ast.Constant) and isinstance(c.value, str)), None)
    return _short(literal) if literal is not None else None


def _short(text):
    """``text`` with any password in a URL masked, cut to ``_LONGEST`` characters."""
    text = _PASSWORD.sub(r"\1:***@", text)
    return text if len(text) <= _LONGEST else text[:_LONGEST - 1] + "\u2026"


def _text(node):
    """The literal text of a string or f-string argument (the parts written in the source)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return ""
