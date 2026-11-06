# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""PostgreSQL tables, through ADBC (``adbc-driver-postgresql``, ``pip install "invaract[postgres]"``): reading them, and
outputs staged during the run and moved into place in one transaction only when the whole run passed (decided 2026-11-04,
docs/PYTHONIO_ADAPTER.md).

A table's location is ``postgresql://[user@]host[:port]/<database>/<schema>.<table>`` (``postgres://`` too; ``<table>`` alone
is in the ``public`` schema; a query string, ``?sslmode=require``, is passed on). Its format is ``jdbc``, the name Spark and
Beam give a dataset reached through JDBC; it is registered in the ``jdbc`` catalog under its database, schema and table. Credentials never go in a location (one with a password is refused): they come from libpq's own sources, as any
PostgreSQL client's do (``PGPASSWORD``, ``PGUSER``, ``~/.pgpass``, ``PGSSLMODE``, ...).

**Staging and publishing.** Each output has its own connection and transaction for the run. Its rows are copied (ADBC's
bulk ingest, ``COPY``) into a session-temporary table, which no other session sees and which the server drops with the
session whatever happens. Publishing, in that one transaction: the table created if it does not exist (its columns as the
staged data's, a required column ``NOT NULL``), emptied for an ``overwrite`` (``TRUNCATE``, which readers wait on rather
than see half done), the staged rows inserted, and committed. A refused or failed run rolls back, and nothing reaches the
table. ``error`` refuses a table that holds rows; ``ignore`` publishes nothing to one that does. A Postgres table is not
written partitioned.
"""
import re
import urllib.parse

import pyarrow as pa

from . import _files, _frames
from .errors import CheckNotRun

SCHEMES = frozenset({"postgresql", "postgres"})
FORMAT = "jdbc"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def is_postgres(location):
    return urllib.parse.urlsplit(str(location)).scheme.lower() in SCHEMES if location else False


def parse(location):
    """``(server uri, database, schema, table)`` from a table's location; the uri is what the driver connects to."""
    parts = urllib.parse.urlsplit(str(location))
    if parts.password is not None:
        raise CheckNotRun(f"a PostgreSQL location must not hold a password (PGPASSWORD or ~/.pgpass give it): {_redacted(parts)}")
    segments = [s for s in parts.path.split("/") if s]
    if parts.scheme.lower() not in SCHEMES or not parts.hostname or len(segments) != 2:
        raise CheckNotRun(f"a PostgreSQL location is postgresql://host[:port]/<database>/<schema>.<table>, not {location!r}")
    database, identifier = segments
    schema, _, table = identifier.rpartition(".")
    schema = schema or "public"
    for name in (schema, table):
        if not _IDENTIFIER.match(name):
            raise CheckNotRun(f"Invaract names PostgreSQL schemas and tables as plain identifiers, not {name!r} (in {location!r})")
    server = urllib.parse.urlunsplit(("postgresql", parts.netloc, f"/{database}", parts.query, ""))
    return server, database, schema, table


def _redacted(parts):
    return urllib.parse.urlunsplit((parts.scheme, f"{parts.username}:***@{parts.hostname}" + (f":{parts.port}" if parts.port else ""),
                                    parts.path, parts.query, ""))


def catalog_of(location):
    """The protocol's ``catalog`` for a table: the ``jdbc`` catalog, its database, schema and table (the server's address,
    which can hold a user name, is not sent)."""
    _, database, schema, table = parse(location)
    return {"technology": "jdbc", "catalogName": database, "namespace": [schema], "table": table}


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _connect(server):
    try:
        import adbc_driver_postgresql.dbapi as dbapi
    except ImportError as e:
        raise CheckNotRun('Invaract needs the adbc-driver-postgresql package to read or write PostgreSQL tables: '
                          'pip install "invaract[postgres]"') from e
    return dbapi.connect(server)


def _exists(cursor, schema, table):
    cursor.execute(f"SELECT to_regclass('{_quote(schema)}.{_quote(table)}') IS NOT NULL")
    return bool(cursor.fetchone()[0])


def _has_rows(cursor, schema, table):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {_quote(schema)}.{_quote(table)})")
    return bool(cursor.fetchone()[0])


def read(location, fields):
    """The table at ``location``: the columns the contract lists (all of them when it lists none), each cast to the
    contract's type where it declares one (the driver gives some types, ``numeric`` among them, as text)."""
    server, columns, query = _select(location, fields)
    with _connect(server) as conn, conn.cursor() as cursor:
        cursor.execute(query(columns))
        data = cursor.fetch_arrow_table()
    return _typed(data, fields)


def batches(location, fields):
    """``(schema, batches)`` of the table at ``location``, as ``_frames.read_batches`` gives them: ``batches(columns)`` runs
    the query afresh and yields its record batches, typed as ``read`` types them, as the server sends them."""
    server, columns, query = _select(location, fields)
    with _connect(server) as conn, conn.cursor() as cursor:
        cursor.execute(query(columns) + " WHERE false")
        schema = _typed(cursor.fetch_arrow_table(), fields).schema

    def read(only=None):
        conn = _connect(server)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query(list(only if only is not None else columns)))
                reader = cursor.fetch_record_batch()
                try:
                    for batch in reader:
                        yield _typed(pa.Table.from_batches([batch]), fields).to_batches()[0] if batch.num_rows else batch
                finally:
                    reader.close()
        finally:
            conn.close()

    return schema, read


def _select(location, fields):
    """``(server, columns, query)``: the columns of the table at ``location`` to read (those the contract lists, all of them
    when it lists none), and the query that selects given ones."""
    server, _, schema, table = parse(location)
    declared = [f["name"] for f in fields]
    with _connect(server) as conn, conn.cursor() as cursor:
        if not _exists(cursor, schema, table):
            raise FileNotFoundError(f"there is no PostgreSQL table {schema}.{table} in {location}")
        cursor.execute(f"SELECT * FROM {_quote(schema)}.{_quote(table)} WHERE false")
        present = [d[0] for d in cursor.description]
    columns = [c for c in declared if c in present] if declared else present
    return server, columns, lambda selected: f"SELECT {', '.join(_quote(c) for c in selected)} FROM {_quote(schema)}.{_quote(table)}"


def _typed(data, fields):
    """``data`` with each column the contract types cast to that type."""
    types = {f["name"]: _frames.arrow_type(f["type"]) for f in fields}
    return pa.table({c: data[c].cast(types[c]) if types.get(c) is not None and data[c].type != types[c] else data[c]
                     for c in data.column_names})


class PostgresOutput:
    """One output's connection and open transaction: staged into a temporary table, moved into place at publish."""

    def __init__(self, location, mode, run_id, layout=_files.FLAT):
        if layout.keys:
            raise CheckNotRun(f"Invaract does not write a PostgreSQL table partitioned ({layout.describe()}): {location}")
        self.target = str(location)
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        server, _, self.schema, self.table = parse(location)
        self._staging = f"invaract_{run_id}_{self.table}"[:63]
        self._conn = _connect(server)
        self._columns = None
        self._required = set()

    def has_data(self):
        with self._conn.cursor() as cursor:
            return _exists(cursor, self.schema, self.table) and _has_rows(cursor, self.schema, self.table)

    def stage(self, schema, batches, required):
        target = _frames.with_required(schema, required)
        with self._conn.cursor() as cursor:
            if self._columns is None:
                cursor.adbc_ingest(self._staging, pa.Table.from_batches([], schema=target), mode="create", temporary=True)
                self._columns = list(target.names)
            elif list(target.names) != self._columns:
                raise CheckNotRun(f"every write of {self.target} in one run has the same columns: {self._columns}, not {target.names}")
            self._required |= set(required)

            def write(batch):
                cursor.adbc_ingest(self._staging, batch, mode="append", temporary=True)

            return _frames.take(schema, target, batches, required, write)

    def publish(self):
        if self._columns is None:
            self.discard()
            return
        target, staging = f"{_quote(self.schema)}.{_quote(self.table)}", f"pg_temp.{_quote(self._staging)}"
        columns = ", ".join(_quote(c) for c in self._columns)
        with self._conn.cursor() as cursor:
            exists = _exists(cursor, self.schema, self.table)
            skip = exists and self.mode == "ignore" and _has_rows(cursor, self.schema, self.table)
        if skip:
            self.discard()
            return
        with self._conn.cursor() as cursor:
            if not exists:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(self.schema)}")
                cursor.execute(f"CREATE TABLE {target} (LIKE {staging})")
                for column in sorted(self._required):
                    cursor.execute(f"ALTER TABLE {target} ALTER COLUMN {_quote(column)} SET NOT NULL")
            elif self.mode == "overwrite":
                cursor.execute(f"TRUNCATE {target}")
            cursor.execute(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {staging}")
            cursor.execute(f"DROP TABLE {staging}")
        self._conn.commit()
        self._close()

    def forget_previous(self):
        pass

    def discard(self):
        """Rolls back the run's transaction: nothing staged reaches the table, and the temporary table goes with the session."""
        if self._conn is not None:
            try:
                self._conn.rollback()
            finally:
                self._close()

    def _close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
