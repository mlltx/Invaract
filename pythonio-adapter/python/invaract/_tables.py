# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Delta Lake and Iceberg tables: reading them, and outputs whose data is staged during the run and committed, in one atomic
commit per output, only when the whole run passed (docs/PYTHONIO_ADAPTER.md, milestone 6).

- **Delta** (``deltalake``, ``pip install "invaract[delta]"``): a write's Parquet files are written into the table's folder,
  where no reader sees them until a commit names them. Publishing commits them (creating the table if it does not exist);
  discarding deletes them. The location is the table's path or URI.
- **Iceberg** (``pyiceberg``, ``pip install "invaract[iceberg]"``): a write's data is held until publishing, which writes and
  commits it in one transaction (creating the table, and its namespace, if they do not exist). The location is
  ``<catalog>.<namespace>.<table>``; the catalog is configured as pyiceberg configures any (``PYICEBERG_CATALOG__<NAME>__*``
  or ``.pyiceberg.yaml``).

Save modes: ``overwrite`` replaces the table's rows, ``append`` adds to them, ``error`` refuses a table that holds rows, and
``ignore`` publishes nothing to one that does.
"""
import json
import os
import shutil
import tempfile
import time

import pyarrow as pa

from . import _frames, _storage
from .errors import CheckNotRun

TABLE_FORMATS = ("delta", "iceberg")


def _require(module, extra):
    try:
        return __import__(module, fromlist=["_"])
    except ImportError as e:
        raise CheckNotRun(f"Invaract needs the {module} package to read or write {extra} tables: "
                          f'pip install "invaract[{extra}]"') from e


# ---- Delta -----------------------------------------------------------------------------------------------------------

def _delta_table(location):
    """The Delta table at ``location``, or ``None`` if there is none."""
    deltalake = _require("deltalake", "delta")
    from deltalake.exceptions import TableNotFoundError

    try:
        return deltalake.DeltaTable(_storage.uri(location))
    except TableNotFoundError:
        return None


def read_delta(location, columns):
    table = _delta_table(location)
    if table is None:
        raise FileNotFoundError(f"there is no Delta table at {location}")
    # to_pyarrow_dataset, not to_pyarrow_table: the latter leaves a process using S3 unable to exit (deltalake 1.6)
    dataset = table.to_pyarrow_dataset()
    return dataset.to_table(columns=[c for c in columns if c in dataset.schema.names] if columns else None)


class DeltaOutput:
    """A Delta table output. Its Parquet files are staged where no reader sees them: in the table's own folder when it is local
    (no commit names them yet), on local disk when the table is on object storage (uploaded at publish, so a refused run never
    touches the bucket: deleting there leaves "folder" markers behind). Publishing commits them, once."""

    def __init__(self, location, mode, run_id):
        self.target = _storage.uri(location)
        self.mode = mode
        self.run_id = run_id
        self._fs, self._root = _storage.filesystem(location)
        self._remote = _storage.on_object_storage(self.target)
        self._staging = tempfile.mkdtemp(prefix=f"invaract-{run_id}-") if self._remote else self._root
        self._actions = []
        self._schema = None

    def has_data(self):
        table = _delta_table(self.target)
        return table is not None and len(table.file_uris()) > 0

    def stage(self, schema, batches, required):
        _require("deltalake", "delta")
        from deltalake.transaction import AddAction
        import pyarrow.fs as fs
        import pyarrow.parquet as pq

        target = _frames.with_required(schema, required)
        name = f"part-{self.run_id}-{len(self._actions) + 1:05d}.parquet"
        os.makedirs(self._staging, exist_ok=True)
        path = os.path.join(self._staging, name)
        rows = 0
        writer = pq.ParquetWriter(path, target, filesystem=fs.LocalFileSystem())
        try:
            def write(batch):
                nonlocal rows
                rows += batch.num_rows
                writer.write_batch(batch)

            nulls = _frames.take(schema, target, batches, required, write)
        finally:
            writer.close()
        stats = {"numRecords": rows, "minValues": {}, "maxValues": {}, "nullCount": {n: nulls[n] for n in target.names}}
        self._actions.append(AddAction(name, os.path.getsize(path), {}, int(time.time() * 1000), True, json.dumps(stats)))
        self._schema = target
        return nulls

    def publish(self):
        if not self._actions:
            return
        table = _delta_table(self.target)
        if table is not None and self.mode == "ignore" and len(table.file_uris()) > 0:
            self.discard()
            return
        if self._remote:
            import pyarrow.fs as fs

            for action in self._actions:
                fs.copy_files(os.path.join(self._staging, action.path), f"{self._root}/{action.path}",
                              source_filesystem=fs.LocalFileSystem(), destination_filesystem=self._fs)
        if table is None:
            from deltalake.schema import Schema
            from deltalake.transaction import create_table_with_add_actions

            create_table_with_add_actions(self.target, Schema.from_arrow(self._schema), self._actions, mode="error")
        else:
            table.create_write_transaction(self._actions, "overwrite" if self.mode == "overwrite" else "append", self._schema)
        self._actions = []
        self.discard()

    def discard(self):
        """Deletes what is staged and not committed: the local staging folder, or the files in the table's own folder."""
        if self._remote:
            shutil.rmtree(self._staging, ignore_errors=True)
        else:
            for action in self._actions:
                try:
                    os.remove(os.path.join(self._staging, action.path))
                except OSError:
                    pass
        self._actions = []

    def forget_previous(self):
        pass


# ---- Iceberg ---------------------------------------------------------------------------------------------------------

def _iceberg_identifier(location):
    """``(catalog, identifier)`` from ``<catalog>.<namespace>.<table>``."""
    catalog, _, identifier = str(location).partition(".")
    if not catalog or "." not in identifier:
        raise CheckNotRun(f"an Iceberg location is <catalog>.<namespace>.<table>, not {location!r}")
    return catalog, identifier


def catalog_of(location, fmt):
    """The catalog registration the checker is told a dataset has, as the protocol's ``catalog``: an Iceberg table's catalog,
    namespace and table (its location names them); ``None`` for files and Delta tables, which are named by their path. The
    catalog service's own address is not sent: a catalog URI can hold credentials."""
    if fmt != "iceberg" or not location:
        return None
    catalog, _, identifier = str(location).partition(".")
    namespace, _, table = identifier.rpartition(".")
    if not catalog or not namespace or not table:
        return None  # not an Iceberg location: reading or writing it is refused with the reason
    return {"technology": "iceberg", "catalogName": catalog, "namespace": namespace.split("."), "table": table}


def _iceberg_table(location):
    """``(catalog, identifier, table)``, the table ``None`` if it does not exist."""
    _require("pyiceberg", "iceberg")
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError

    name, identifier = _iceberg_identifier(location)
    catalog = load_catalog(name)
    try:
        return catalog, identifier, catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog, identifier, None


def _iceberg_has_rows(table):
    snapshot = table.current_snapshot() if table is not None else None
    return snapshot is not None and int((snapshot.summary or {}).get("total-records", 0) or 0) > 0


def read_iceberg(location, columns):
    _, _, table = _iceberg_table(location)
    if table is None:
        raise FileNotFoundError(f"there is no Iceberg table {location}")
    present = [c for c in columns if c in table.schema().column_names] if columns else []
    return table.scan(selected_fields=tuple(present) or ("*",)).to_arrow()


class IcebergOutput:
    def __init__(self, location, mode, run_id):
        _iceberg_identifier(location)
        self.target = str(location)
        self.mode = mode
        self.run_id = run_id
        self._batches = []
        self._schema = None

    def has_data(self):
        return _iceberg_has_rows(_iceberg_table(self.target)[2])

    def stage(self, schema, batches, required):
        """Holds the batches until publishing: pyiceberg writes a whole table at once, inside the transaction it commits."""
        _require("pyiceberg", "iceberg")
        target = _frames.with_required(schema, required)
        self._schema = target
        return _frames.take(schema, target, batches, required, self._batches.append)

    def publish(self):
        if self._schema is None:
            return
        data = pa.Table.from_batches(self._batches, schema=self._schema)
        catalog, identifier, table = _iceberg_table(self.target)
        if table is None:
            catalog.create_namespace_if_not_exists(identifier.rsplit(".", 1)[0])
            transaction = catalog.create_table_transaction(identifier, schema=self._schema)
            transaction.append(data)
            transaction.commit_transaction()
        elif not (self.mode == "ignore" and _iceberg_has_rows(table)):
            with table.transaction() as transaction:
                (transaction.overwrite if self.mode == "overwrite" else transaction.append)(data)
        self.discard()

    def discard(self):
        self._batches = []
        self._schema = None

    def forget_previous(self):
        pass
