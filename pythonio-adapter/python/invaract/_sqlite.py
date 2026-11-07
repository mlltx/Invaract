# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""SQLite tables, through ADBC (``adbc-driver-sqlite``, ``pip install "invaract[sqlite]"``): reading them, and outputs staged
during the run in a file of their own and moved into place in one transaction only when the whole run passed (decided
2026-11-06, docs/PYTHONIO_ADAPTER.md).

A table's location names the database file and the table, as SQLAlchemy names a database:
``sqlite:///<relative path>/<table>`` (``sqlite:///data/shop.db/orders``) or ``sqlite:////<absolute path>/<table>``. Its
format is ``jdbc``, as for PostgreSQL; it is registered in the ``jdbc`` catalog under the file's name without its extension,
the ``main`` schema, and the table.

**Types.** SQLite stores a value as an integer, a real, text or a blob, whatever a column says. Booleans are written as 0
and 1; dates, timestamps (ISO 8601, a time zone written as an offset) and decimals (exactly, as text) as text. A read casts
each column the contract types back to its type (a timestamp with no offset, written by another program, is taken to be in
the contract's time zone).

**Staging and publishing.** An output's rows are written (ADBC's bulk ingest) to a private database file on the job's
local disk, so the target database is neither locked nor touched during the run, nor created by a run that is refused.
Publishing, in one transaction on the target: the staged file attached, the table created if it does not exist (its
columns as the staged data's, a required column ``NOT NULL``), emptied for an ``overwrite``, the staged rows inserted, and
committed. ``error`` refuses a table that holds rows; ``ignore`` publishes nothing to one that does. A SQLite table is not
written partitioned.
"""
import os
import re
import shutil
import sqlite3
import tempfile
import urllib.parse

import pyarrow as pa
import pyarrow.compute as pc

from . import _files, _frames
from .errors import CheckNotRun

SCHEME = "sqlite"
FORMAT = "jdbc"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_STAGED = "invaract_staged"


def is_sqlite(location):
    return str(location).lower().startswith(SCHEME + "://") if location else False


def parse(location):
    """``(database file path, table)`` from a table's location."""
    text = str(location)
    rest = text[len(SCHEME) + 3:] if is_sqlite(text) else ""
    database, _, table = rest[1:].rpartition("/") if rest.startswith("/") else ("", "", "")
    if not database or not table:
        raise CheckNotRun(f"a SQLite location is sqlite:///<path to the database file>/<table> (sqlite://// for an absolute "
                          f"path), not {location!r}")
    if not _IDENTIFIER.match(table):
        raise CheckNotRun(f"Invaract names SQLite tables as plain identifiers, not {table!r} (in {location!r})")
    return database, table


def catalog_of(location):
    """The protocol's ``catalog`` for a table: the ``jdbc`` catalog, the file's name without its extension, ``main`` and the table."""
    database, table = parse(location)
    return {"technology": "jdbc", "catalogName": os.path.splitext(os.path.basename(database))[0], "namespace": ["main"], "table": table}


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _dbapi():
    try:
        import adbc_driver_sqlite.dbapi as dbapi
    except ImportError as e:
        raise CheckNotRun('Invaract needs the adbc-driver-sqlite package to read or write SQLite tables: '
                          'pip install "invaract[sqlite]"') from e
    return dbapi


def _read_only(database):
    return _dbapi().connect("file:" + urllib.parse.quote(os.path.abspath(database)) + "?mode=ro")


def _columns(cursor, table):
    cursor.execute(f"PRAGMA table_info({_quote(table)})")
    return [row[1] for row in cursor.fetchall()]


def read(location, fields):
    """The table at ``location``: the columns the contract lists (all of them when it lists none), each cast to the
    contract's type where it declares one."""
    database, query = _select(location, fields)
    with _read_only(database) as conn, conn.cursor() as cursor:
        cursor.execute(query())
        data = cursor.fetch_arrow_table()
    return typed(data, fields)


def batches(location, fields):
    """``(schema, batches)`` of the table at ``location``, as ``_frames.read_batches`` gives them."""
    database, query = _select(location, fields)
    with _read_only(database) as conn, conn.cursor() as cursor:
        cursor.execute(query() + " WHERE 0")
        schema = typed(cursor.fetch_arrow_table(), fields).schema

    def read(only=None):
        conn = _read_only(database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query(only))
                reader = cursor.fetch_record_batch()
                try:
                    for batch in reader:
                        yield typed(pa.Table.from_batches([batch]), fields).to_batches()[0] if batch.num_rows else batch
                finally:
                    reader.close()
        finally:
            conn.close()

    return schema, read


def _select(location, fields):
    """``(database, query)``: ``query(columns)`` selects ``columns`` (by default, those the contract lists that the table
    has, or all of them when it lists none)."""
    database, table = parse(location)
    if not os.path.isfile(database):
        raise FileNotFoundError(f"there is no SQLite database {database} (for {location})")
    with _read_only(database) as conn, conn.cursor() as cursor:
        present = _columns(cursor, table)
    if not present:
        raise FileNotFoundError(f"there is no SQLite table {table} in {database}")
    declared = [f["name"] for f in fields]
    columns = [c for c in declared if c in present] if declared else present

    def query(selected=None):
        return f"SELECT {', '.join(_quote(c) for c in (columns if selected is None else selected))} FROM {_quote(table)}"

    return database, query


def typed(data, fields):
    """``data`` with each column the contract types cast to that type from what SQLite held."""
    types = {f["name"]: _frames.arrow_type(f["type"]) for f in fields}
    return pa.table({c: _cast(data[c], types[c]) if types.get(c) is not None else data[c] for c in data.column_names})


def _cast(column, t):
    if column.type == t:
        return column
    if pa.types.is_decimal(t) and pa.types.is_floating(column.type):
        # through its shortest text, as a real holds no decimal exactly
        column = column.cast(pa.string())
    if pa.types.is_timestamp(t) and t.tz is not None and pa.types.is_string(column.type):
        try:
            return column.cast(t)
        except pa.ArrowInvalid:
            # text with no offset: in the contract's time zone
            return pc.assume_timezone(column.cast(pa.timestamp(t.unit)), t.tz)
    return column.cast(t)


def storable(schema, batch):
    """``batch`` as SQLite stores it (the module docstring): decimals and timestamps with a time zone as text."""
    columns = []
    for field, column in zip(schema, batch.columns):
        t = field.type
        if pa.types.is_decimal(t):
            column = column.cast(pa.string())
        elif pa.types.is_timestamp(t) and t.tz is not None:
            column = pc.strftime(column.cast(pa.timestamp(t.unit, "UTC")), format="%Y-%m-%dT%H:%M:%SZ")
        columns.append(column)
    return pa.RecordBatch.from_arrays(columns, names=schema.names)


def _affinity(t):
    """The column type a published table declares for Arrow type ``t``: one whose affinity keeps the value as written."""
    if pa.types.is_integer(t) or pa.types.is_boolean(t):
        return "INTEGER"
    if pa.types.is_floating(t):
        return "REAL"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "BLOB"
    return "TEXT"


class SqliteOutput:
    """One output: staged in a private database file, moved into the target's table at publish."""

    def __init__(self, location, mode, run_id, layout=_files.FLAT):
        if layout.keys:
            raise CheckNotRun(f"Invaract does not write a SQLite table partitioned ({layout.describe()}): {location}")
        self.target = str(location)
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        self.database, self.table = parse(location)
        self._folder = None
        self._schema = None
        self._required = set()

    @property
    def _staging(self):
        return os.path.join(self._folder, "staged.db")

    def has_data(self):
        if not os.path.isfile(self.database):
            return False
        conn = sqlite3.connect("file:" + urllib.parse.quote(os.path.abspath(self.database)) + "?mode=ro", uri=True)
        try:
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (self.table,)).fetchone()
            return bool(exists) and conn.execute(f"SELECT EXISTS (SELECT 1 FROM {_quote(self.table)})").fetchone()[0] == 1
        finally:
            conn.close()

    def stage(self, schema, batches, required):
        target = _frames.with_required(schema, required)
        if self._schema is None:
            self._folder = tempfile.mkdtemp(prefix=f"invaract-{self.run_id}-")
            self._schema = target
        elif list(target.names) != self._schema.names:
            raise CheckNotRun(f"every write of {self.target} in one run has the same columns: {self._schema.names}, not {target.names}")
        self._required |= set(required)
        conn = _dbapi().connect(self._staging)
        try:
            with conn.cursor() as cursor:
                # the staged table made first, so that a write of no rows still publishes the table's columns
                cursor.adbc_ingest(_STAGED, pa.Table.from_batches([storable(target, pa.RecordBatch.from_pylist([], schema=target))]),
                                   mode="create_append")

                def write(batch):
                    cursor.adbc_ingest(_STAGED, storable(target, batch), mode="create_append")

                nulls = _frames.take(schema, target, batches, required, write)
            conn.commit()
            return nulls
        finally:
            conn.close()

    def publish(self):
        if self._schema is None:
            return
        target, columns = _quote(self.table), ", ".join(_quote(c) for c in self._schema.names)
        os.makedirs(os.path.dirname(os.path.abspath(self.database)), exist_ok=True)
        conn = sqlite3.connect(self.database, isolation_level=None)
        try:
            conn.execute("ATTACH DATABASE ? AS invaract_staging", (self._staging,))
            conn.execute("BEGIN IMMEDIATE")
            try:
                exists = conn.execute("SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?", (self.table,)).fetchone()
                if exists and self.mode == "ignore" and conn.execute(f"SELECT EXISTS (SELECT 1 FROM main.{target})").fetchone()[0]:
                    conn.execute("ROLLBACK")
                    return
                if not exists:
                    definitions = ", ".join(f"{_quote(f.name)} {_affinity(f.type)}" + (" NOT NULL" if f.name in self._required else "")
                                            for f in self._schema)
                    conn.execute(f"CREATE TABLE main.{target} ({definitions})")
                elif self.mode == "overwrite":
                    conn.execute(f"DELETE FROM main.{target}")
                conn.execute(f"INSERT INTO main.{target} ({columns}) SELECT {columns} FROM invaract_staging.{_quote(_STAGED)}")
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            try:
                conn.execute("DETACH DATABASE invaract_staging")
            except sqlite3.Error:
                pass
            finally:
                conn.close()
                self.discard()

    def forget_previous(self):
        pass

    def discard(self):
        if self._folder is not None:
            shutil.rmtree(self._folder, ignore_errors=True)
            self._folder = None
