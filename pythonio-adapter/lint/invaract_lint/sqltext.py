# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Whether a piece of SQL reads or writes files: the statements and table functions that do, found in the SQL's own words
and never inside its quoted values, its quoted names or its comments.

DuckDB, PostgreSQL, SQLite and MySQL each let SQL reach files: ``COPY ... TO``/``FROM``, ``ATTACH``, ``EXPORT DATABASE``,
``VACUUM INTO``, ``SELECT ... INTO OUTFILE``, ``LOAD DATA INFILE``, table functions such as ``read_parquet(...)``, and, in
DuckDB, a quoted file where a table would be (``FROM 'orders/*.parquet'``). A column called ``copy``, the word ``attach``
in a value, or a date after ``EXTRACT(... FROM ...)`` is none of those.

SQL put together at run time is read with ``UNKNOWN`` where each part the source does not spell out goes. ``file_access``
still finds the statements above by their own words (``COPY`` whatever follows it), and ``built`` says when a part that is
not spelled out stands where a whole statement or a table would be, which is where SQL that reaches files can hide.
"""
import re

#: Stands in SQL for a part the source does not spell out.
UNKNOWN = "\x01"
#: Stands in SQL built up from itself (``sql = sql + " AND ..."``) for the text it had before: more of the same statement.
EARLIER = "\x02"
#: A part of SQL filled in at run time: ``UNKNOWN``, or a ``{}``, ``{name}``, ``%s`` or ``%(name)s`` still to be formatted.
_GAP = r"(?:\x01|\{\w*\}|%(?:\(\w+\))?s)"

#: A statement that reaches files by its first words, each with what a finding calls it.
_STATEMENTS = [
    # COPY <table, query or gap> ... TO/FROM, the direction itself perhaps left to run time; not a column called copy
    (re.compile(r"COPY\s+(?:" + _GAP + r"|(?:\(|[\w.]+).*?(?:\b(?:TO|FROM)\b|" + _GAP + "))", re.IGNORECASE | re.DOTALL), "COPY"),
    (re.compile(r"ATTACH\s+(?:DATABASE\b|IF\b|[?$:]|\x00|" + _GAP + ")", re.IGNORECASE), "ATTACH"),
    (re.compile(r"(?:EXPORT|IMPORT)\s+DATABASE\b", re.IGNORECASE), "EXPORT DATABASE"),
    (re.compile(r"VACUUM\b.*?\bINTO\b", re.IGNORECASE | re.DOTALL), "VACUUM INTO"),
    (re.compile(r"LOAD\s+DATA\b.*?\bINFILE\b", re.IGNORECASE | re.DOTALL), "LOAD DATA INFILE"),
]
#: Words anywhere in a statement that reach files.
_ANYWHERE = [
    (re.compile(r"\bINTO\s+(?:OUTFILE|DUMPFILE)\s*(?:\x00|" + _GAP + ")", re.IGNORECASE), "INTO OUTFILE"),
    (re.compile(r"\b((?:read_(?:parquet|csv|json|ndjson|text|blob|xlsx)\w*|parquet_scan|delta_scan|iceberg_scan|sniff_csv))\s*\(",
                re.IGNORECASE), None),
]
#: A quoted value that names a file, a folder of files or a URL: what ``FROM '...'`` must hold to be a read of a file.
_FILE = re.compile(r".*(?:" + _GAP + r"|://|[/\\*]|\.(?:parquet|csv|tsv|json|jsonl|ndjson|txt|gz|zst|bz2|xlsx|db|duckdb|sqlite|arrow|feather|orc|avro)$)",
                   re.IGNORECASE | re.DOTALL)
_FROM = re.compile(r"\b(?:FROM|JOIN)\s*\x00(\d+)\x00", re.IGNORECASE)
#: A statement that starts with a part not spelled out, and a table that is one.
_BUILT_STATEMENT = re.compile(_GAP)
_BUILT_TABLE = re.compile(r"\b(?:FROM|JOIN)\s+" + _GAP, re.IGNORECASE)

#: One token of SQL: a comment, a quoted value, a quoted name, or anything else.
_TOKEN = re.compile(r"--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|`[^`]*`", re.DOTALL)


def file_access(sql):
    """What in ``sql`` reads or writes files (``"COPY"``, ``"read_parquet"``, ``"FROM 'orders.parquet'"``), or ``None``."""
    statements, values = _statements(sql)
    for statement in statements:
        for pattern, name in _STATEMENTS:
            if pattern.match(statement):
                return name
        for pattern, name in _ANYWHERE:
            found = pattern.search(statement)
            if found:
                return name or found.group(1).lower()
        for found in _FROM.finditer(statement):
            value = values[int(found.group(1))]
            if _FILE.match(value):
                return f"FROM '{value.replace(UNKNOWN, '...')}'"
    return None


def built(sql):
    """Where ``sql`` leaves to run time what could reach files: ``"statement"`` when a whole statement, or its first
    words, are not spelled out (it may be a ``COPY``); ``"table"`` when what follows a ``FROM`` or ``JOIN`` is not (in
    DuckDB it may be a file); ``None`` when every gap is somewhere else (a value compared against, a column, a limit)."""
    statements, _ = _statements(sql)
    if any(_BUILT_STATEMENT.match(statement) for statement in statements):
        return "statement"
    if any(_BUILT_TABLE.search(statement) for statement in statements):
        return "table"
    return None


def _statements(sql):
    """The statements of ``sql``, each with its comments and quoted names blanked and each quoted value replaced by its
    number between two NULs, and the quoted values by number."""
    values = []

    def blank(match):
        token = match.group(0)
        if token.startswith("'"):
            values.append(token[1:-1])
            return f"\x00{len(values) - 1}\x00"  # a quoted value: kept apart, so its words are never read as SQL
        # a quoted name stays a name, with none of its words; a comment is nothing
        return " " if token.startswith(("--", "/*")) else " _ "

    bare = _TOKEN.sub(blank, sql)
    return [statement.replace(EARLIER, " ").strip() for statement in bare.split(";")], values
