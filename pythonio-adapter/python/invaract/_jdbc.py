# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Database tables, the format Spark and Beam call ``jdbc``: PostgreSQL (``_postgres``) and SQLite (``_sqlite``), told apart
by their location's scheme (``_datasets``). What the two share is here: how a table is named, and how one is read a batch at
a time.

Each database is a module ``_datasets`` uses through the same functions, so adding a database is adding one more module
that has them, and naming it in ``_datasets._database``:

- ``is_<database>(location)``: whether ``location`` names a table of this database (by its scheme);
- ``catalog_of(location)``: the catalog the checker is told the table is registered in (``technology: jdbc``, the
  database, its schema, the table);
- ``read(location, fields)`` and ``batches(location, fields)``: the table read whole, or a batch at a time, each column as
  the contract types it (``typed``, with the database's own conversion): ``read`` and ``batches`` here, given how to connect
  and the query (``selector``);
- ``output(location, mode, run_id, layout)``: an ``_outputs.Output`` that stages into a table no reader sees and moves it
  into place in one transaction at publish.
"""
import re

from . import _types

#: The format of a database table, whichever database holds it.
FORMAT = "jdbc"

#: A schema or table name Invaract writes into SQL: a plain identifier, so it never needs more than quoting.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def quote(name):
    """``name`` as a quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def typed(data, fields, cast):
    """``data`` (a table or a record batch, as a database's driver gives it) with each column the contract types made that
    type by ``cast(column, arrow type)``, the database's own conversion; any other column as it is."""
    types = _types.scalars(fields)
    names = data.schema.names
    columns = [data.column(i) if types.get(name) in (None, data.column(i).type) else cast(data.column(i), types[name])
               for i, name in enumerate(names)]
    return type(data).from_arrays(columns, names=names)


def selector(table, present, fields):
    """``query(columns)``: the SQL that selects ``columns`` from ``table`` (quoted, schema and all) - by default the columns the
    contract lists in ``fields`` that the table has (``present``), or all of them when it lists none."""
    declared = [f["name"] for f in fields]
    default = [c for c in declared if c in present] if declared else list(present)

    def query(columns=None):
        return f"SELECT {', '.join(quote(c) for c in (default if columns is None else columns))} FROM {table}"

    return query


def read(connect, query, typed):
    """The whole result of ``query()``, made ``typed``, on a connection of its own."""
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(query())
        return typed(cursor.fetch_arrow_table())


def batches(connect, query, typed):
    """``(schema, batches)``, as ``_datasets.read_batches`` gives them: the schema of ``query()``'s result made ``typed``
    (asked for with no rows), and ``batches(columns)``, which streams it (``stream``)."""
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(query() + " WHERE 1 = 0")
        schema = typed(cursor.fetch_arrow_table()).schema
    return schema, stream(connect, query, typed)


def stream(connect, query, typed):
    """``batches(columns)``, as ``_datasets.read_batches`` gives it: each call connects afresh (``connect()``), runs
    ``query(columns)`` and yields its record batches as the server sends them, each made ``typed``, then disconnects."""

    def batches(only=None):
        conn = connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query(only))
                reader = cursor.fetch_record_batch()
                try:
                    for batch in reader:
                        yield typed(batch)
                finally:
                    reader.close()
        finally:
            conn.close()

    return batches
