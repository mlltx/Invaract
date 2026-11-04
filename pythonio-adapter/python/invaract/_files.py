# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Outputs of plain Parquet or CSV files: staged during the run, published only when the whole run passed. How depends on
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

Any other location (a URI scheme Invaract does not know) is refused, never written somewhere else.
"""
import os
import posixpath
import shutil
import tempfile

from . import _frames, _storage
from .errors import CheckNotRun

#: Schemes of file systems whose rename is atomic, through pyarrow's Hadoop file system.
HADOOP = frozenset({"hdfs", "viewfs"})
SUCCESS = "_SUCCESS"


def output(location, fmt, mode, run_id):
    kind = _storage.scheme(location)
    if kind in (None, "file"):
        return LocalFileOutput(location, fmt, mode, run_id)
    if kind in HADOOP:
        return HadoopFileOutput(location, fmt, mode, run_id)
    if kind in _storage.OBJECT_STORAGE:
        return ObjectStorageFileOutput(location, fmt, mode, run_id)
    raise CheckNotRun(f"Invaract cannot write files to {location}: it writes to local and mounted file systems, HDFS "
                      "(hdfs://, viewfs://) and object storage (s3://, gs://, abfss://, ...)")


def writer(path, fmt, schema, filesystem=None):
    """A writer of ``fmt`` (``parquet`` or ``csv``) at ``path``, on ``filesystem`` (pyarrow's) or the local one."""
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


# ---- local and mounted file systems ------------------------------------------------------------------------------------

class LocalFileOutput:
    """Its staging folder is beside its location, so that publishing is a rename on the same file system."""

    def __init__(self, location, fmt, mode, run_id):
        self.target = _storage.uri(location)
        self.fmt = fmt
        parent, base = os.path.split(self.target)
        self.staging = os.path.join(parent, f".{base}.invaract-staging-{run_id}")
        self.previous = os.path.join(parent, f".{base}.invaract-previous-{run_id}")
        self.mode = mode
        self.run_id = run_id
        self.files = 0

    def has_data(self):
        return _has_data(self.target)

    def stage(self, schema, batches, required):
        os.makedirs(self.staging, exist_ok=True)
        self.files += 1
        path = os.path.join(self.staging, f"part-{self.run_id}-{self.files:05d}.{self.fmt}")
        return _stage_into(path, self.fmt, schema, batches, required, None, _remove)

    def publish(self):
        if not os.path.isdir(self.staging):
            return
        if self.mode == "append":
            os.makedirs(self.target, exist_ok=True)
            for f in sorted(os.listdir(self.staging)):
                destination = os.path.join(self.target, f)
                if os.path.exists(destination):
                    raise FileExistsError(f"Invaract will not replace {destination} when appending")
                os.rename(os.path.join(self.staging, f), destination)
            self.discard()
        elif self.mode == "ignore" and _has_data(self.target):
            self.discard()
        else:
            if os.path.lexists(self.target):
                os.rename(self.target, self.previous)
            try:
                os.rename(self.staging, self.target)
            except BaseException:
                if os.path.lexists(self.previous):
                    os.rename(self.previous, self.target)
                raise

    def forget_previous(self):
        _remove(self.previous)

    def discard(self):
        _remove(self.staging)


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


class HadoopFileOutput:
    """``LocalFileOutput`` on HDFS: staged beside the location and renamed into place, through pyarrow's Hadoop file system
    (which needs Hadoop's client libraries where the job runs, as any pyarrow HDFS access does)."""

    def __init__(self, location, fmt, mode, run_id, filesystem=None):
        self.target = _storage.uri(location)
        self.fmt = fmt
        self.mode = mode
        self.run_id = run_id
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
        path = posixpath.join(self.staging, f"part-{self.run_id}-{self.files:05d}.{self.fmt}")
        return _stage_into(path, self.fmt, schema, batches, required, self.fs, self._remove_file)

    def publish(self):
        if not _exists(self.fs, self.staging):
            return
        if self.mode == "append":
            self.fs.create_dir(self.path, recursive=True)
            for info in sorted(_listing(self.fs, self.staging), key=lambda i: i.path):
                destination = posixpath.join(self.path, posixpath.basename(info.path))
                if _exists(self.fs, destination):
                    raise FileExistsError(f"Invaract will not replace {destination} when appending")
                self.fs.move(info.path, destination)
            self.discard()
        elif self.mode == "ignore" and self.has_data():
            self.discard()
        else:
            if _exists(self.fs, self.path):
                self.fs.move(self.path, self.previous)
            try:
                self.fs.move(self.staging, self.path)
            except BaseException:
                if _exists(self.fs, self.previous):
                    self.fs.move(self.previous, self.path)
                raise

    def forget_previous(self):
        if _exists(self.fs, self.previous):
            self.fs.delete_dir(self.previous)

    def discard(self):
        if _exists(self.fs, self.staging):
            self.fs.delete_dir(self.staging)

    def _remove_file(self, path):
        if _exists(self.fs, path):
            self.fs.delete_file(path)


class ObjectStorageFileOutput:
    """Staged on local disk, copied into place Spark-style at publish, ``_SUCCESS`` written last (module docstring)."""

    def __init__(self, location, fmt, mode, run_id):
        self.target = _storage.uri(location)
        self.fmt = fmt
        self.mode = mode
        self.run_id = run_id
        self.fs, self.path = _storage.filesystem(location)
        self.path = self.path.rstrip("/")
        self.staging = tempfile.mkdtemp(prefix=f"invaract-{run_id}-")
        self.files = 0

    def has_data(self):
        return _fs_has_data(self.fs, self.path)

    def stage(self, schema, batches, required):
        os.makedirs(self.staging, exist_ok=True)
        self.files += 1
        path = os.path.join(self.staging, f"part-{self.run_id}-{self.files:05d}.{self.fmt}")
        return _stage_into(path, self.fmt, schema, batches, required, None, _remove)

    def publish(self):
        import pyarrow.fs as pafs

        staged = sorted(os.listdir(self.staging)) if os.path.isdir(self.staging) else []
        if not staged or (self.mode == "ignore" and self.has_data()):
            self.discard()
            return
        marker = f"{self.path}/{SUCCESS}"
        uploaded = {f"{self.path}/{name}" for name in staged}
        if self.mode == "append":
            clashes = sorted(p for p in uploaded if _exists(self.fs, p))
            if clashes:
                raise FileExistsError(f"Invaract will not replace {clashes[0]} when appending")
        old = [i.path for i in _listing(self.fs, self.path, recursive=True)] if self.mode != "append" else []
        # 1. no reader that waits for _SUCCESS takes the output for complete while it changes
        if _exists(self.fs, marker):
            self.fs.delete_file(marker)
        # 2. the new files, beside the old ones (nothing is deleted before they are all there)
        for name in staged:
            pafs.copy_files(os.path.join(self.staging, name), f"{self.path}/{name}",
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
