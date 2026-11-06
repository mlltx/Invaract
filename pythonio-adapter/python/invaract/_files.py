# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Outputs of plain files (Parquet, CSV and the formats in ``_formats``): staged during the run, published only when the
whole run passed. How depends on
where the location lives:

- **A local or mounted file system** (``LocalFileOutput``): staged in a hidden folder beside the location and renamed into
  place, which is atomic per output.
- **HDFS** (``hdfs://``, ``viewfs://``; ``HadoopFileOutput``): the same, on HDFS itself through pyarrow's Hadoop file system,
  whose rename is atomic too.
- **Object storage** (S3, GCS, Azure; ``ObjectStorageFileOutput``): there is no rename, so the files are staged on the job's
  local disk (a refused run never touches the bucket) and copied into place at publish, as Spark and Hadoop do: the old
  ``_SUCCESS`` marker removed, the new files uploaded, the old ones deleted (overwrite), and ``_SUCCESS`` written last.
  A reader that waits for ``_SUCCESS`` sees a complete output; one that does not can see old and new files together while
  the publish runs, and a crash part way leaves them so, without ``_SUCCESS`` (decided 2026-11-03, docs/PYTHONIO_ADAPTER.md).

**Partitioned outputs** (a ``Layout`` with keys) are written Hive-style, one folder per value of each key in order
(``region=north/day=2026-11-04/part-....parquet``), the key columns kept in the folder names and out of the files, as Spark
writes them. An ``append`` adds the run's files to their folders. An ``overwrite`` replaces, by the layout's mode, the whole
output (``static``) or only the partition folders the run wrote (``dynamic``), each folder renamed into place (or, on object
storage, its old files deleted); a dynamic overwrite is atomic per partition, not per output (decided 2026-11-04).

Any other location (a URI scheme Invaract does not know) is refused, never written somewhere else.
"""
import collections
import os
import posixpath
import shutil
import tempfile

from . import _formats, _frames, _storage
from .errors import CheckNotRun

#: Schemes of file systems whose rename is atomic, through pyarrow's Hadoop file system.
HADOOP = frozenset({"hdfs", "viewfs"})
SUCCESS = "_SUCCESS"


class Layout(collections.namedtuple("Layout", ["keys", "overwrite"])):
    """How an output lays out its data: the columns it is partitioned by, in order (none: not partitioned), and what an
    ``overwrite`` replaces, ``static`` (every partition) or ``dynamic`` (only those written)."""

    __slots__ = ()

    def describe(self):
        return f"partitioned by {', '.join(self.keys)}" if self.keys else "not partitioned"


FLAT = Layout((), "static")


def output(location, fmt, mode, run_id, layout=FLAT):
    kind = _storage.scheme(location)
    if kind in (None, "file"):
        return LocalFileOutput(location, fmt, mode, run_id, layout)
    if kind in HADOOP:
        return HadoopFileOutput(location, fmt, mode, run_id, layout)
    if kind in _storage.OBJECT_STORAGE:
        return ObjectStorageFileOutput(location, fmt, mode, run_id, layout)
    raise CheckNotRun(f"Invaract cannot write files to {location}: it writes to local and mounted file systems, HDFS "
                      "(hdfs://, viewfs://) and object storage (s3://, gs://, abfss://, ...)")


def writer(path, fmt, schema, filesystem=None):
    """A writer of ``fmt`` (``_formats.FORMATS``) at ``path``, on ``filesystem`` (pyarrow's) or the local one."""
    if fmt not in ("parquet", "csv"):
        import pyarrow as pa

        return _formats.writer(pa.OSFile(path, "wb") if filesystem is None else filesystem.open_output_stream(path), fmt, schema)
    if fmt == "csv" and getattr(fmt, "options", None):
        from . import _csv

        return _csv.Writer(open(path, "wb") if filesystem is None else filesystem.open_output_stream(path), schema, fmt.options)
    if fmt == "csv":
        import pyarrow.csv as csv

        if filesystem is None:
            return csv.CSVWriter(path, schema)
        stream = filesystem.open_output_stream(path)
        return _Closing(csv.CSVWriter(stream, schema), stream)
    import pyarrow.parquet as pq

    return pq.ParquetWriter(path, schema, filesystem=filesystem)


class _Closing:
    """A CSV writer over a stream it opened, closing the stream with it."""

    def __init__(self, inner, stream):
        self._inner = inner
        self._stream = stream

    def write_batch(self, batch):
        self._inner.write_batch(batch)

    def close(self):
        self._inner.close()
        self._stream.close()


def _stage_into(path, fmt, schema, batches, required, filesystem, remove):
    """Writes the batches to a new file at ``path`` with ``not null`` on each required column, and returns the empty values
    each column held (``_frames.take``); a write that fails part way leaves no file."""
    target = _frames.with_required(schema, required)
    try:
        out = writer(path, fmt, target, filesystem)
        try:
            return _frames.take(schema, target, batches, required, out.write_batch)
        finally:
            out.close()
    except BaseException:
        remove(path)
        raise


def _stage_partitioned(base, prefix, fmt, schema, batches, required, keys, filesystem, remove_prefixed):
    """``_stage_into`` for a partitioned output: each batch written under ``base`` in Hive-style folders by ``keys``, as files
    named ``<prefix>-<batch>-<n>.<extension>``; a write that fails part way leaves none of them."""
    import pyarrow as pa
    import pyarrow.dataset as ds

    missing = [k for k in keys if k not in schema.names]
    if missing:
        raise CheckNotRun(f"Invaract cannot partition by {', '.join(missing)}: the table has no such column")
    target = _frames.with_required(schema, required)
    partitioning = ds.partitioning(pa.schema([target.field(k) for k in keys]), flavor="hive")
    written = [0]
    # pyarrow's dataset writer lays out the folders; a format it does not write, or writes only one way (CSV with
    # options), is written to a local scratch folder as Arrow files first, then each file copied into the format by ``writer``
    direct = fmt in ("parquet", "feather") or (fmt == "csv" and not getattr(fmt, "options", None))
    into, into_fs, kind = (base, filesystem, fmt) if direct else (tempfile.mkdtemp(prefix=f"invaract-{prefix}-"), None, "ipc")

    def write(batch):
        written[0] += 1
        ds.write_dataset(pa.Table.from_batches([batch]), into, format=kind, partitioning=partitioning, filesystem=into_fs,
                         basename_template=f"{prefix}-{written[0]:05d}-{{i}}.{_formats.extension(fmt)}", existing_data_behavior="overwrite_or_ignore")

    try:
        nulls = _frames.take(schema, target, batches, required, write)
        if not direct:
            _convert(into, base, fmt, filesystem)
        return nulls
    except BaseException:
        remove_prefixed(base, prefix)
        raise
    finally:
        if not direct:
            _remove(into)


def _convert(scratch, base, fmt, filesystem):
    """Each Arrow file under the local folder ``scratch`` written as ``fmt`` at the same place under ``base``."""
    import pyarrow as pa

    for rel in _local_files(scratch):
        destination = posixpath.join(base, rel) if filesystem is not None else os.path.join(base, *rel.split("/"))
        if filesystem is None:
            os.makedirs(os.path.dirname(destination), exist_ok=True)
        else:
            filesystem.create_dir(posixpath.dirname(destination), recursive=True)
        with pa.ipc.open_file(os.path.join(scratch, *rel.split("/"))) as reader:
            out = writer(destination, fmt, reader.schema, filesystem)
            try:
                for i in range(reader.num_record_batches):
                    out.write_batch(reader.get_batch(i))
            finally:
                out.close()


def _leaves(files):
    """The partition folders (relative, ``/``-separated) that hold ``files`` (relative paths); ``""`` for files at the top."""
    return sorted({posixpath.dirname(f) for f in files})


# ---- local and mounted file systems ------------------------------------------------------------------------------------

class LocalFileOutput:
    """Its staging folder is beside its location, so that publishing is a rename on the same file system."""

    def __init__(self, location, fmt, mode, run_id, layout=FLAT):
        self.target = _storage.uri(location)
        self.fmt = fmt
        parent, base = os.path.split(self.target)
        self.staging = os.path.join(parent, f".{base}.invaract-staging-{run_id}")
        self.previous = os.path.join(parent, f".{base}.invaract-previous-{run_id}")
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        self.files = 0

    def has_data(self):
        return _has_data(self.target)

    def stage(self, schema, batches, required):
        os.makedirs(self.staging, exist_ok=True)
        self.files += 1
        prefix = f"part-{self.run_id}-{self.files:05d}"
        if self.layout.keys:
            return _stage_partitioned(self.staging, prefix, self.fmt, schema, batches, required, self.layout.keys, None,
                                      lambda base, p: _remove_prefixed_local(base, p))
        return _stage_into(os.path.join(self.staging, f"{prefix}.{_formats.extension(self.fmt)}"), self.fmt, schema, batches, required, None, _remove)

    def publish(self):
        if not os.path.isdir(self.staging):
            return
        if self.mode == "append":
            staged = _local_files(self.staging)
            for rel in staged:
                if os.path.exists(os.path.join(self.target, rel)):
                    raise FileExistsError(f"Invaract will not replace {os.path.join(self.target, rel)} when appending")
            for rel in staged:
                destination = os.path.join(self.target, rel)
                os.makedirs(os.path.dirname(destination), exist_ok=True)
                os.rename(os.path.join(self.staging, rel), destination)
            self.discard()
        elif self.mode == "ignore" and _has_data(self.target):
            self.discard()
        elif self.mode == "overwrite" and self.layout.keys and self.layout.overwrite == "dynamic":
            self._replace_partitions(_leaves(_local_files(self.staging)))
        else:
            if os.path.lexists(self.target):
                os.rename(self.target, self.previous)
            try:
                os.rename(self.staging, self.target)
            except BaseException:
                if os.path.lexists(self.previous):
                    os.rename(self.previous, self.target)
                raise

    def _replace_partitions(self, leaves):
        """A dynamic overwrite: each partition folder the run wrote takes the place of the one there (moved aside to
        ``previous`` until the run is over); one that fails part way puts back the folders already replaced."""
        done = []
        try:
            for leaf in leaves:
                destination, aside = os.path.join(self.target, leaf), os.path.join(self.previous, leaf)
                if os.path.lexists(destination):
                    os.makedirs(os.path.dirname(aside), exist_ok=True)
                    os.rename(destination, aside)
                done.append(leaf)
                os.makedirs(os.path.dirname(destination), exist_ok=True)
                os.rename(os.path.join(self.staging, leaf), destination)
        except BaseException:
            for leaf in reversed(done):
                destination, aside = os.path.join(self.target, leaf), os.path.join(self.previous, leaf)
                if os.path.lexists(destination) and not os.path.lexists(os.path.join(self.staging, leaf)):
                    os.rename(destination, os.path.join(self.staging, leaf))
                if os.path.lexists(aside):
                    os.rename(aside, destination)
            raise
        self.discard()

    def forget_previous(self):
        _remove(self.previous)

    def discard(self):
        _remove(self.staging)


def _local_files(root):
    """Every file under ``root``, as a ``/``-separated path relative to it, in order."""
    found = []
    for folder, _, names in os.walk(root):
        for name in names:
            found.append(os.path.relpath(os.path.join(folder, name), root).replace(os.sep, "/"))
    return sorted(found)


def _remove_prefixed_local(base, prefix):
    for rel in _local_files(base) if os.path.isdir(base) else []:
        if posixpath.basename(rel).startswith(prefix):
            os.remove(os.path.join(base, rel))


def _has_data(path):
    """Whether a location holds data: a file, or a folder with anything in it that is not hidden (``.``) or metadata (``_``)."""
    if os.path.isfile(path):
        return True
    return os.path.isdir(path) and any(not f.startswith((".", "_")) for f in os.listdir(path))


def _remove(path):
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.lexists(path):
        os.remove(path)


# ---- pyarrow file systems (HDFS, object storage) -----------------------------------------------------------------------

def _exists(fs, path):
    import pyarrow.fs as pafs

    return fs.get_file_info(path).type != pafs.FileType.NotFound


def _listing(fs, path, recursive=False):
    import pyarrow.fs as pafs

    return [i for i in fs.get_file_info(pafs.FileSelector(path, allow_not_found=True, recursive=recursive))
            if i.type == pafs.FileType.File or not recursive]


def _fs_has_data(fs, path):
    """``_has_data`` on a pyarrow file system."""
    import pyarrow.fs as pafs

    info = fs.get_file_info(path)
    if info.type == pafs.FileType.File:
        return True
    return info.type == pafs.FileType.Directory and any(
        not posixpath.basename(i.path).startswith((".", "_")) for i in _listing(fs, path))


def _fs_files(fs, root):
    """Every file under ``root`` on ``fs``, relative to it, in order."""
    root = root.rstrip("/")
    return sorted(posixpath.relpath(i.path, root) for i in _listing(fs, root, recursive=True))


class HadoopFileOutput:
    """``LocalFileOutput`` on HDFS: staged beside the location and renamed into place, through pyarrow's Hadoop file system
    (which needs Hadoop's client libraries where the job runs, as any pyarrow HDFS access does)."""

    def __init__(self, location, fmt, mode, run_id, layout=FLAT, filesystem=None):
        self.target = _storage.uri(location)
        self.fmt = fmt
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        self.fs, self.path = filesystem or _storage.filesystem(location)
        parent, base = posixpath.split(self.path.rstrip("/"))
        self.staging = posixpath.join(parent, f".{base}.invaract-staging-{run_id}")
        self.previous = posixpath.join(parent, f".{base}.invaract-previous-{run_id}")
        self.files = 0

    def has_data(self):
        return _fs_has_data(self.fs, self.path)

    def stage(self, schema, batches, required):
        self.fs.create_dir(self.staging, recursive=True)
        self.files += 1
        prefix = f"part-{self.run_id}-{self.files:05d}"
        if self.layout.keys:
            return _stage_partitioned(self.staging, prefix, self.fmt, schema, batches, required, self.layout.keys, self.fs, self._remove_prefixed)
        return _stage_into(posixpath.join(self.staging, f"{prefix}.{_formats.extension(self.fmt)}"), self.fmt, schema, batches, required, self.fs, self._remove_file)

    def publish(self):
        if not _exists(self.fs, self.staging):
            return
        if self.mode == "append":
            staged = _fs_files(self.fs, self.staging)
            for rel in staged:
                if _exists(self.fs, posixpath.join(self.path, rel)):
                    raise FileExistsError(f"Invaract will not replace {posixpath.join(self.path, rel)} when appending")
            for rel in staged:
                destination = posixpath.join(self.path, rel)
                self.fs.create_dir(posixpath.dirname(destination), recursive=True)
                self.fs.move(posixpath.join(self.staging, rel), destination)
            self.discard()
        elif self.mode == "ignore" and self.has_data():
            self.discard()
        elif self.mode == "overwrite" and self.layout.keys and self.layout.overwrite == "dynamic":
            self._replace_partitions(_leaves(_fs_files(self.fs, self.staging)))
        else:
            if _exists(self.fs, self.path):
                self.fs.move(self.path, self.previous)
            try:
                self.fs.move(self.staging, self.path)
            except BaseException:
                if _exists(self.fs, self.previous):
                    self.fs.move(self.previous, self.path)
                raise

    def _replace_partitions(self, leaves):
        """``LocalFileOutput._replace_partitions`` on HDFS."""
        done = []
        try:
            for leaf in leaves:
                destination, aside = posixpath.join(self.path, leaf), posixpath.join(self.previous, leaf)
                if _exists(self.fs, destination):
                    self.fs.create_dir(posixpath.dirname(aside), recursive=True)
                    self.fs.move(destination, aside)
                done.append(leaf)
                self.fs.create_dir(posixpath.dirname(destination), recursive=True)
                self.fs.move(posixpath.join(self.staging, leaf), destination)
        except BaseException:
            for leaf in reversed(done):
                destination, aside = posixpath.join(self.path, leaf), posixpath.join(self.previous, leaf)
                if _exists(self.fs, destination) and not _exists(self.fs, posixpath.join(self.staging, leaf)):
                    self.fs.move(destination, posixpath.join(self.staging, leaf))
                if _exists(self.fs, aside):
                    self.fs.move(aside, destination)
            raise
        self.discard()

    def forget_previous(self):
        if _exists(self.fs, self.previous):
            self.fs.delete_dir(self.previous)

    def discard(self):
        if _exists(self.fs, self.staging):
            self.fs.delete_dir(self.staging)

    def _remove_file(self, path):
        if _exists(self.fs, path):
            self.fs.delete_file(path)

    def _remove_prefixed(self, base, prefix):
        for rel in _fs_files(self.fs, base):
            if posixpath.basename(rel).startswith(prefix):
                self.fs.delete_file(posixpath.join(base, rel))


class ObjectStorageFileOutput:
    """Staged on local disk, copied into place Spark-style at publish, ``_SUCCESS`` written last (module docstring)."""

    def __init__(self, location, fmt, mode, run_id, layout=FLAT):
        self.target = _storage.uri(location)
        self.fmt = fmt
        self.mode = mode
        self.run_id = run_id
        self.layout = layout
        self.fs, self.path = _storage.filesystem(location)
        self.path = self.path.rstrip("/")
        self.staging = tempfile.mkdtemp(prefix=f"invaract-{run_id}-")
        self.files = 0

    def has_data(self):
        return _fs_has_data(self.fs, self.path)

    def stage(self, schema, batches, required):
        os.makedirs(self.staging, exist_ok=True)
        self.files += 1
        prefix = f"part-{self.run_id}-{self.files:05d}"
        if self.layout.keys:
            return _stage_partitioned(self.staging, prefix, self.fmt, schema, batches, required, self.layout.keys, None,
                                      lambda base, p: _remove_prefixed_local(base, p))
        return _stage_into(os.path.join(self.staging, f"{prefix}.{_formats.extension(self.fmt)}"), self.fmt, schema, batches, required, None, _remove)

    def publish(self):
        import pyarrow.fs as pafs

        staged = _local_files(self.staging) if os.path.isdir(self.staging) else []
        if not staged or (self.mode == "ignore" and self.has_data()):
            self.discard()
            return
        marker = f"{self.path}/{SUCCESS}"
        uploaded = {f"{self.path}/{rel}" for rel in staged}
        if self.mode == "append":
            clashes = sorted(p for p in uploaded if _exists(self.fs, p))
            if clashes:
                raise FileExistsError(f"Invaract will not replace {clashes[0]} when appending")
            old = []
        elif self.layout.keys and self.layout.overwrite == "dynamic":
            # only the partitions the run wrote are replaced
            old = [i.path for leaf in _leaves(staged) for i in _listing(self.fs, f"{self.path}/{leaf}", recursive=True)]
        else:
            old = [i.path for i in _listing(self.fs, self.path, recursive=True)]
        # 1. no reader that waits for _SUCCESS takes the output for complete while it changes
        if _exists(self.fs, marker):
            self.fs.delete_file(marker)
        # 2. the new files, beside the old ones (nothing is deleted before they are all there)
        for rel in staged:
            pafs.copy_files(os.path.join(self.staging, *rel.split("/")), f"{self.path}/{rel}",
                            source_filesystem=pafs.LocalFileSystem(), destination_filesystem=self.fs)
        # 3. an overwrite's old files
        for path in old:
            if path not in uploaded and path != marker:
                self.fs.delete_file(path)
        # 4. complete
        with self.fs.open_output_stream(marker):
            pass
        self.discard()

    def forget_previous(self):
        pass

    def discard(self):
        shutil.rmtree(self.staging, ignore_errors=True)
