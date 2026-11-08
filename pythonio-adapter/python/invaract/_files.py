# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Outputs of plain files (Parquet, CSV and the formats in ``_formats``): staged during the run, published only when the
whole run passed. How depends on where the location lives:

- **A local or mounted file system, or HDFS** (``hdfs://``, ``viewfs://``, through pyarrow's Hadoop file system;
  ``RenamedOutput``): staged in a hidden folder beside the location and renamed into place, which is atomic per output.
- **Object storage** (S3, GCS, Azure; ``CopiedOutput``): there is no rename, so the files are staged on the job's local
  disk (a refused run never touches the bucket) and copied into place at publish, as Spark and Hadoop do: the old
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
import contextlib
import os
import posixpath
import shutil
import tempfile

import pyarrow as pa

from . import _csv, _formats, _frames, _outputs, _storage
from .errors import CheckNotRun

#: Schemes of file systems whose rename is atomic, through pyarrow's Hadoop file system.
HADOOP = frozenset({"hdfs", "viewfs"})
SUCCESS = "_SUCCESS"


def output(location, fmt, mode, run_id, layout=_outputs.FLAT, csv=None):
    """The output that writes ``fmt`` files at ``location``; ``csv``, the options a CSV output is written with."""
    kind = _storage.scheme(location)
    if kind in (None, "file"):
        target = _storage.uri(location)
        return RenamedOutput(target, target, LocalFiles(), fmt, mode, run_id, layout, csv)
    if kind in HADOOP:
        filesystem, path = _storage.filesystem(location)
        return RenamedOutput(_storage.uri(location), path.rstrip("/"), ArrowFiles(filesystem), fmt, mode, run_id, layout, csv)
    if kind in _storage.OBJECT_STORAGE:
        return CopiedOutput(location, fmt, mode, run_id, layout, csv)
    raise CheckNotRun(f"Invaract cannot write files to {location}: it writes to local and mounted file systems, HDFS "
                      "(hdfs://, viewfs://) and object storage (s3://, gs://, abfss://, ...)")


# ---- the file systems an output is staged and published on -------------------------------------------------------------

class LocalFiles:
    """The operations an output needs, on the local file system (or one mounted there). Paths below a root are relative and
    ``/``-separated, whatever the platform."""

    #: The pyarrow file system a writer is given: none, the local one.
    arrow = None
    path = os.path

    def join(self, base, rel):
        return os.path.join(base, *rel.split("/"))

    def exists(self, path):
        return os.path.lexists(path)

    def files(self, root):
        """Every file under ``root``, relative to it, in order; none when there is no ``root``."""
        found = []
        for folder, _, names in os.walk(root):
            for name in names:
                found.append(os.path.relpath(os.path.join(folder, name), root).replace(os.sep, "/"))
        return sorted(found)

    def size(self, path):
        return os.path.getsize(path)

    def has_data(self, path):
        """Whether ``path`` holds data: a file, or a folder with anything in it that is not hidden (``.``) or metadata
        (``_``)."""
        if os.path.isfile(path):
            return True
        return os.path.isdir(path) and any(not f.startswith((".", "_")) for f in os.listdir(path))

    def make_dir(self, path):
        os.makedirs(path, exist_ok=True)

    def move(self, source, destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        os.rename(source, destination)

    def remove(self, path):
        """Deletes the file or folder at ``path``, if there is one (a link, not what it links to)."""
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.lexists(path):
            os.remove(path)

    def output_stream(self, path):
        return pa.OSFile(path, "wb")


class ArrowFiles:
    """``LocalFiles`` on a pyarrow file system (HDFS, object storage)."""

    path = posixpath

    def __init__(self, filesystem):
        self.arrow = filesystem

    def join(self, base, rel):
        return posixpath.join(base, rel)

    def exists(self, path):
        import pyarrow.fs as pafs

        return self.arrow.get_file_info(path).type != pafs.FileType.NotFound

    def listing(self, path, recursive=False):
        """What is in the folder ``path``: every file under it, or, not ``recursive``, everything directly in it."""
        import pyarrow.fs as pafs

        return [i for i in self.arrow.get_file_info(pafs.FileSelector(path, allow_not_found=True, recursive=recursive))
                if i.type == pafs.FileType.File or not recursive]

    def files(self, root):
        root = root.rstrip("/")
        return sorted(posixpath.relpath(i.path, root) for i in self.listing(root, recursive=True))

    def size(self, path):
        return self.arrow.get_file_info(path).size

    def has_data(self, path):
        import pyarrow.fs as pafs

        info = self.arrow.get_file_info(path)
        if info.type == pafs.FileType.File:
            return True
        return info.type == pafs.FileType.Directory and any(
            not posixpath.basename(i.path).startswith((".", "_")) for i in self.listing(path))

    def make_dir(self, path):
        self.arrow.create_dir(path, recursive=True)

    def move(self, source, destination):
        self.arrow.create_dir(posixpath.dirname(destination), recursive=True)
        self.arrow.move(source, destination)

    def remove(self, path):
        import pyarrow.fs as pafs

        kind = self.arrow.get_file_info(path).type
        if kind == pafs.FileType.Directory:
            self.arrow.delete_dir(path)
        elif kind != pafs.FileType.NotFound:
            self.arrow.delete_file(path)

    def output_stream(self, path):
        return self.arrow.open_output_stream(path)


# ---- writing files -----------------------------------------------------------------------------------------------------

def writer(path, fmt, schema, files, csv=None):
    """A writer of ``fmt`` (``_formats.FORMATS``) at ``path`` on ``files``; ``csv``, the options a CSV file is written with."""
    if fmt == "parquet":
        import pyarrow.parquet as pq

        return pq.ParquetWriter(path, schema, filesystem=files.arrow)
    if fmt == "csv" and csv:
        return _csv.Writer(files.output_stream(path), schema, csv)
    if fmt == "csv":
        import pyarrow.csv as pacsv

        stream = files.output_stream(path)
        return _Closing(pacsv.CSVWriter(stream, schema), stream)
    return _formats.writer(files.output_stream(path), fmt, schema)


class _Closing:
    """A writer over a stream it does not close itself, closing the stream with it."""

    def __init__(self, inner, stream):
        self._inner = inner
        self._stream = stream

    def write_batch(self, batch):
        self._inner.write_batch(batch)

    def close(self):
        try:
            self._inner.close()
        finally:
            self._stream.close()


def _convert(scratch, base, fmt, csv, files):
    """Each Arrow file under the local folder ``scratch`` written as ``fmt`` at the same place under ``base`` on ``files``."""
    local = LocalFiles()
    for rel in local.files(scratch):
        destination = files.join(base, rel)
        files.make_dir(files.path.dirname(destination))
        with pa.ipc.open_file(local.join(scratch, rel)) as reader:
            out = writer(destination, fmt, reader.schema, files, csv)
            try:
                for i in range(reader.num_record_batches):
                    out.write_batch(reader.get_batch(i))
            finally:
                out.close()


def _folders(files):
    """Every folder that holds ``files`` (relative paths), and every folder above it: ``""`` (the top) for all of them."""
    found = {""}
    for f in files:
        parts = posixpath.dirname(f).split("/") if posixpath.dirname(f) else []
        found.update("/".join(parts[:i]) for i in range(1, len(parts) + 1))
    return found


def _leaves(files):
    """The partition folders (relative, ``/``-separated) that hold ``files`` (relative paths); ``""`` for files at the top."""
    return sorted({posixpath.dirname(f) for f in files})


# ---- outputs -----------------------------------------------------------------------------------------------------------

class _FileOutput(_outputs.Output):
    """Files staged in the folder ``staging`` on ``staged_on`` (``LocalFiles`` or ``ArrowFiles``), one file (or, partitioned,
    one file per partition) per write."""

    def __init__(self, target, fmt, mode, run_id, layout, csv, staged_on, staging):
        super().__init__(target, mode, layout)
        self.fmt = fmt
        self.csv = csv
        self.run_id = run_id
        self.staging = staging
        self._staged_on = staged_on
        self._writes = 0

    def stage(self, schema, batches, required):
        self._staged_on.make_dir(self.staging)
        self._writes += 1
        prefix = f"part-{self.run_id}-{self._writes:05d}"
        target = _frames.with_required(schema, required)
        if self.layout.keys:
            return self._stage_partitioned(prefix, schema, target, batches, required)
        return self._stage_file(f"{prefix}.{_formats.extension(self.fmt)}", schema, target, batches, required)

    def _stage_file(self, name, schema, target, batches, required):
        """Writes the batches to a new file ``name`` in ``staging``, as ``target`` (``schema`` with each required column not
        null), and returns the empty values each column held (``_frames.take``); a write that fails part way leaves no file."""
        files = self._staged_on
        path = files.join(self.staging, name)
        try:
            out = writer(path, self.fmt, target, files, self.csv)
            try:
                return _frames.take(schema, target, batches, required, out.write_batch)
            finally:
                out.close()
        except BaseException:
            files.remove(path)
            raise

    def _stage_partitioned(self, prefix, schema, target, batches, required):
        """``_stage_file`` for a partitioned output: each batch written in Hive-style folders by the layout's keys, as files
        named ``<prefix>-<batch>-<n>.<extension>``; a write that fails part way leaves none of them."""
        import pyarrow.dataset as ds

        files, fmt = self._staged_on, self.fmt
        _outputs.require_columns(schema, self.layout.keys)
        partitioning = ds.partitioning(pa.schema([target.field(k) for k in self.layout.keys]), flavor="hive")
        # pyarrow's dataset writer lays out the folders; a format it does not write, or writes only one way (CSV with
        # options), is written to a local scratch folder as Arrow files first, then each file copied into the format by ``writer``
        direct = fmt in ("parquet", "feather") or (fmt == "csv" and not self.csv)
        into, into_fs, kind = (self.staging, files.arrow, fmt) if direct else (tempfile.mkdtemp(prefix=f"invaract-{prefix}-"), None, "ipc")
        written = 0

        def write(batch):
            nonlocal written
            written += 1
            ds.write_dataset(pa.Table.from_batches([batch]), into, format=kind, partitioning=partitioning, filesystem=into_fs,
                             basename_template=f"{prefix}-{written:05d}-{{i}}.{_formats.extension(fmt)}",
                             existing_data_behavior="overwrite_or_ignore")

        try:
            nulls = _frames.take(schema, target, batches, required, write)
            if not direct:
                _convert(into, self.staging, fmt, self.csv, files)
            return nulls
        except BaseException:
            for rel in files.files(self.staging):
                if posixpath.basename(rel).startswith(prefix):
                    files.remove(files.join(self.staging, rel))
            raise
        finally:
            if not direct:
                LocalFiles().remove(into)

    def discard(self):
        self._staged_on.remove(self.staging)

    def _staged(self):
        """``(files, published)``: the files staged (relative to ``staging``), and what publishing them puts in place."""
        files = self._staged_on.files(self.staging) if self._staged_on.exists(self.staging) else []
        size = sum(self._staged_on.size(self._staged_on.join(self.staging, rel)) for rel in files)
        return files, _outputs.Published(files=len(files), bytes=size)


class RenamedOutput(_FileOutput):
    """Staged in a hidden folder beside its location (``path`` on ``files``), so that publishing is a rename on the same file
    system; what a publish replaces is moved aside to ``previous`` until the whole run is published, so that ``restore`` can
    put it back if a later output fails to publish.

    Each way of publishing (``_append``, ``_replace_partitions``, ``_replace_folder``) returns the function that undoes it,
    and calls that function itself if it fails part way: a publish is all done, or not done at all."""

    def __init__(self, target, path, files, fmt, mode, run_id, layout=_outputs.FLAT, csv=None):
        parent, base = files.path.split(path)
        super().__init__(target, fmt, mode, run_id, layout, csv, files, files.path.join(parent, f".{base}.invaract-staging-{run_id}"))
        self.path = path
        self.previous = files.path.join(parent, f".{base}.invaract-previous-{run_id}")
        self._files = files
        #: what undoes the last publish, for ``restore``; ``None`` before a publish, or when it put nothing in place
        self._undo = None

    def has_data(self):
        return self._files.has_data(self.path)

    def publish(self):
        staged, published = self._staged()
        if not staged or (self.mode == "ignore" and self.has_data()):
            self.discard()
            return None
        if self.mode == "append":
            self._undo = self._append(staged)
        elif self.mode == "overwrite" and self.layout.dynamic:
            self._undo = self._replace_partitions(_leaves(staged))
        else:
            self._undo = self._replace_folder()
        self.discard()
        return published

    def restore(self):
        """Puts the location back as it was before ``publish``."""
        undo, self._undo = self._undo, None
        if undo is not None:
            undo()
        return True

    def forget_previous(self):
        self._files.remove(self.previous)

    def _append(self, staged):
        """Moves each staged file into place beside the files already there; undone by removing the files it added, then
        the folders it made for them (the output's own, a partition's)."""
        files = self._files
        for rel in staged:
            if files.exists(files.join(self.path, rel)):
                raise FileExistsError(f"Invaract will not replace {files.join(self.path, rel)} when appending")
        # deepest first, so that each folder is empty when it is removed
        made = sorted((d for d in _folders(staged) if not files.exists(self._at(d))), reverse=True)
        added = []

        def undo():
            for rel in reversed(added):
                files.remove(files.join(self.path, rel))
            for folder in made:
                files.remove(self._at(folder))

        with _undone_on_failure(undo):
            for rel in staged:
                files.move(files.join(self.staging, rel), files.join(self.path, rel))
                added.append(rel)
        return undo

    def _replace_partitions(self, leaves):
        """A dynamic overwrite: each partition folder the run wrote takes the place of the one there, which is moved aside
        to ``previous``; undone by moving those back."""
        files, replaced = self._files, []

        def undo():
            for leaf in reversed(replaced):
                files.remove(files.join(self.path, leaf))
                if files.exists(files.join(self.previous, leaf)):
                    files.move(files.join(self.previous, leaf), files.join(self.path, leaf))
            files.remove(self.previous)

        with _undone_on_failure(undo):
            for leaf in leaves:
                if files.exists(files.join(self.path, leaf)):
                    files.move(files.join(self.path, leaf), files.join(self.previous, leaf))
                replaced.append(leaf)
                files.move(files.join(self.staging, leaf), files.join(self.path, leaf))
        return undo

    def _replace_folder(self):
        """Every other publish: the staged folder takes the place of the whole location, which is moved aside to
        ``previous``; undone by moving it back."""
        files = self._files

        def undo():
            files.remove(self.path)
            if files.exists(self.previous):
                files.move(self.previous, self.path)

        if files.exists(self.path):
            files.move(self.path, self.previous)
        with _undone_on_failure(undo):
            files.move(self.staging, self.path)
        return undo

    def _at(self, rel):
        """The path of ``rel`` (``/``-separated, ``""`` for the top) under the location."""
        return self._files.join(self.path, rel) if rel else self.path


@contextlib.contextmanager
def _undone_on_failure(undo):
    """Runs ``undo`` if the block fails, then lets the failure go on."""
    try:
        yield
    except BaseException:
        undo()
        raise


class CopiedOutput(_FileOutput):
    """Staged on local disk, copied into place Spark-style at publish, ``_SUCCESS`` written last (module docstring)."""

    def __init__(self, location, fmt, mode, run_id, layout=_outputs.FLAT, csv=None):
        filesystem, path = _storage.filesystem(location)
        super().__init__(_storage.uri(location), fmt, mode, run_id, layout, csv, LocalFiles(),
                         tempfile.mkdtemp(prefix=f"invaract-{run_id}-"))
        self.path = path.rstrip("/")
        self._store = ArrowFiles(filesystem)

    def has_data(self):
        return self._store.has_data(self.path)

    def publish(self):
        import pyarrow.fs as pafs

        store = self._store
        staged, published = self._staged()
        if not staged or (self.mode == "ignore" and self.has_data()):
            self.discard()
            return None
        marker = f"{self.path}/{SUCCESS}"
        uploaded = {f"{self.path}/{rel}" for rel in staged}
        if self.mode == "append":
            clashes = sorted(p for p in uploaded if store.exists(p))
            if clashes:
                raise FileExistsError(f"Invaract will not replace {clashes[0]} when appending")
            old = []
        elif self.layout.dynamic:
            # only the partitions the run wrote are replaced
            old = [i.path for leaf in _leaves(staged) for i in store.listing(f"{self.path}/{leaf}", recursive=True)]
        else:
            old = [i.path for i in store.listing(self.path, recursive=True)]
        # 1. no reader that waits for _SUCCESS takes the output for complete while it changes
        store.remove(marker)
        # 2. the new files, beside the old ones (nothing is deleted before they are all there)
        for rel in staged:
            pafs.copy_files(self._staged_on.join(self.staging, rel), f"{self.path}/{rel}",
                            source_filesystem=pafs.LocalFileSystem(), destination_filesystem=store.arrow)
        # 3. an overwrite's old files
        for path in old:
            if path not in uploaded and path != marker:
                store.arrow.delete_file(path)
        # 4. complete
        with store.output_stream(marker):
            pass
        self.discard()
        return published
