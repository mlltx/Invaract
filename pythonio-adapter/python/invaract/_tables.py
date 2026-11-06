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

**Partitioned tables** (a ``_files.Layout`` with keys): a new table is created partitioned by the keys (Delta's partition
columns; an Iceberg identity partition spec), and a table that exists must already be partitioned so, or the write is not
checked. An ``overwrite`` replaces every partition (``static``) or only those written (``dynamic``), in the same one commit:
for Delta, the new files added and the old files of the written partitions removed (deltalake's ``RemoveAction``, absent from
older releases, which then refuse a dynamic overwrite rather than do it in pieces); for Iceberg,
``dynamic_partition_overwrite``.
"""
import json
import os
import shutil
import tempfile
import time
import urllib.parse

import pyarrow as pa

from . import _files, _frames, _storage
from .errors import CheckNotRun

#: How a null partition value is spelt in a folder name, as Hive, Spark and pyarrow spell it.
NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"

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
    dataset = delta_dataset(location)
    return dataset.to_table(columns=[c for c in columns if c in dataset.schema.names] if columns else None)


def delta_dataset(location):
    """The Delta table at ``location`` as a pyarrow dataset of its current version."""
    table = _delta_table(location)
    if table is None:
        raise FileNotFoundError(f"there is no Delta table at {location}")
    # to_pyarrow_dataset, not to_pyarrow_table: the latter leaves a process using S3 unable to exit (deltalake 1.6)
    return table.to_pyarrow_dataset()


class DeltaOutput:
    """A Delta table output. Its Parquet files are staged where no reader sees them: in the table's own folder when it is local
    (no commit names them yet), on local disk when the table is on object storage (uploaded at publish, so a refused run never
    touches the bucket: deleting there leaves "folder" markers behind). Publishing commits them, once."""

    def __init__(self, location, mode, run_id, layout=_files.FLAT):
        self.target = _storage.uri(location)
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        existing = _delta_table(location)
        if existing is not None:
            columns = tuple(existing.metadata().partition_columns)
            if columns != tuple(layout.keys):
                raise CheckNotRun(f"the Delta table at {location} is {_files.Layout(columns, 'static').describe()}, "
                                  f"but the write is {layout.describe()}")
            if layout.keys and layout.overwrite == "dynamic" and mode == "overwrite":
                import deltalake.transaction as transaction

                if not hasattr(transaction, "RemoveAction"):
                    raise CheckNotRun("Invaract overwrites a Delta table's partitions dynamically in one commit, which needs a "
                                      "deltalake release with deltalake.transaction.RemoveAction: upgrade deltalake")
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
        os.makedirs(self._staging, exist_ok=True)
        self._schema = target
        if self.layout.keys:
            return self._stage_partitioned(schema, target, batches, required)
        name = f"part-{self.run_id}-{len(self._actions) + 1:05d}.parquet"
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
        return nulls

    def _stage_partitioned(self, schema, target, batches, required):
        """Each batch split by the partition keys' values, each part written to its own Hive-style folder as a file whose
        Add action carries those values (the keys stay out of the file, as Delta keeps them)."""
        from deltalake.transaction import AddAction
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        keys = list(self.layout.keys)
        missing = [k for k in keys if k not in schema.names]
        if missing:
            raise CheckNotRun(f"Invaract cannot partition by {', '.join(missing)}: the table has no such column")
        data_schema = pa.schema([f for f in target if f.name not in keys], metadata=target.metadata)

        def write(batch):
            table = pa.Table.from_batches([batch])
            for values in table.select(keys).group_by(keys).aggregate([]).to_pylist():
                mask = None
                for key in keys:
                    match = pc.is_null(table[key]) if values[key] is None else pc.equal(table[key], pa.scalar(values[key], table.schema.field(key).type))
                    mask = match if mask is None else pc.and_(mask, match)
                part = table.filter(mask)
                text = {k: _partition_text(values[k], table.schema.field(k).type) for k in keys}
                folder = "/".join(f"{k}={NULL_PARTITION if text[k] is None else urllib.parse.quote(text[k], safe='')}" for k in keys)
                name = f"{folder}/part-{self.run_id}-{len(self._actions) + 1:05d}.parquet"
                path = os.path.join(self._staging, *name.split("/"))
                os.makedirs(os.path.dirname(path), exist_ok=True)
                data = part.drop_columns(keys).cast(data_schema)
                pq.write_table(data, path)
                stats = {"numRecords": data.num_rows, "minValues": {}, "maxValues": {},
                         "nullCount": {c: data[c].null_count for c in data.column_names}}
                self._actions.append(AddAction(name, os.path.getsize(path), text, int(time.time() * 1000), True, json.dumps(stats)))

        return _frames.take(schema, target, batches, required, write)

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
        keys = list(self.layout.keys) or None
        if table is None:
            from deltalake.schema import Schema
            from deltalake.transaction import create_table_with_add_actions

            create_table_with_add_actions(self.target, Schema.from_arrow(self._schema), self._actions, mode="error", partition_by=keys)
        elif keys and self.mode == "overwrite" and self.layout.overwrite == "dynamic":
            # one commit: the new files, and the old files of the partitions written removed
            written = {json.dumps(a.partition_values, sort_keys=True) for a in self._actions}
            removals = [_remove_action(row) for row in _delta_files(table) if json.dumps(row["partition_values"], sort_keys=True) in written]
            table.create_write_transaction(self._actions + removals, "append", self._schema, partition_by=keys)
        else:
            table.create_write_transaction(self._actions, "overwrite" if self.mode == "overwrite" else "append", self._schema, partition_by=keys)
        self._actions = []
        self.discard()

    def discard(self):
        """Deletes what is staged and not committed: the local staging folder, or the files in the table's own folder (a
        partition folder made only for them is left, empty, as Delta leaves them)."""
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


def _partition_text(value, arrow_type):
    """A partition value as Delta records it: its text (``2026-11-04``, ``true``), or ``None`` for a null."""
    if value is None:
        return None
    return pa.scalar(value, arrow_type).cast(pa.string()).as_py()


def _delta_files(table):
    """The table's current data files, each as ``{"path", "size", "partition_values"}`` with the values as text."""
    actions = pa.table(table.get_add_actions(flatten=False))
    partition = actions.column("partition")
    types = {f.name: f.type for f in partition.type}
    return [
        {"path": path, "size": size, "partition_values": {k: _partition_text(v, types[k]) for k, v in (values or {}).items()}}
        for path, size, values in zip(actions.column("path").to_pylist(), actions.column("size_bytes").to_pylist(), partition.to_pylist())
    ]


def _remove_action(row):
    from deltalake.transaction import RemoveAction

    return RemoveAction(row["path"], True, int(time.time() * 1000), row["size"], row["partition_values"])


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


def iceberg_batches(location, columns):
    """``(schema, batches)`` of the Iceberg table at ``location``, as ``_frames.read_batches`` gives them."""
    _, _, table = _iceberg_table(location)
    if table is None:
        raise FileNotFoundError(f"there is no Iceberg table {location}")
    present = tuple(c for c in columns if c in table.schema().column_names) if columns else tuple(table.schema().column_names)

    def batches(only=None):
        return iter(table.scan(selected_fields=tuple(only if only is not None else present)).to_arrow_batch_reader())

    return table.scan(selected_fields=present).to_arrow_batch_reader().schema, batches


def _iceberg_keys(table):
    """The columns an Iceberg table is partitioned by, in order, as identity partitions; a field with another transform
    (``bucket``, ``day``, ...) is named with it, so it never equals a layout's plain column."""
    from pyiceberg.transforms import IdentityTransform

    schema = table.schema()
    return tuple(schema.find_column_name(f.source_id) if isinstance(f.transform, IdentityTransform)
                 else f"{f.transform}({schema.find_column_name(f.source_id)})" for f in table.spec().fields)


class IcebergOutput:
    def __init__(self, location, mode, run_id, layout=_files.FLAT):
        _iceberg_identifier(location)
        self.target = str(location)
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        # a table that exists keeps its partitioning: a write laid out otherwise is not checked rather than mis-written
        table = _iceberg_table(location)[2]
        if table is not None and _iceberg_keys(table) != tuple(layout.keys):
            raise CheckNotRun(f"the Iceberg table {location} is {_files.Layout(_iceberg_keys(table), 'static').describe()}, "
                              f"but the write is {layout.describe()}")
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
            if self.layout.keys:
                with transaction.update_spec() as spec:
                    for key in self.layout.keys:
                        spec.add_identity(key)
            transaction.append(data)
            transaction.commit_transaction()
        elif not (self.mode == "ignore" and _iceberg_has_rows(table)):
            with table.transaction() as transaction:
                if self.mode != "overwrite":
                    transaction.append(data)
                elif self.layout.keys and self.layout.overwrite == "dynamic":
                    transaction.dynamic_partition_overwrite(data)
                else:
                    transaction.overwrite(data)
        self.discard()

    def discard(self):
        self._batches = []
        self._schema = None

    def forget_previous(self):
        pass
