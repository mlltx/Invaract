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
- ``read(location, fields)`` and ``batches(location, fields)``: the table read whole, or a batch at a time (``stream``),
  each column as the contract types it (``typed``, with the database's own conversion);
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
