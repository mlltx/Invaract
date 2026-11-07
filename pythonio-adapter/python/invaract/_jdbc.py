# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Database tables, the format Spark and Beam call ``jdbc``: PostgreSQL (``_postgres``) and SQLite (``_sqlite``), told apart
by their location's scheme (``_datasets``). What the two share is here: how a table is named, and how one is read a batch at
a time."""
import re

#: The format of a database table, whichever database holds it.
FORMAT = "jdbc"

#: A schema or table name Invaract writes into SQL: a plain identifier, so it never needs more than quoting.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def quote(name):
    """``name`` as a quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


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
