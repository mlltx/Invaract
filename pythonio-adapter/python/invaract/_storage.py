# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Where a location lives: a local or mounted file system, or object storage (S3, GCS, Azure) named by its URI.

Reads work from either. A plain Parquet or CSV output is published by renaming its staging folder into place, which object
storage cannot do (it has no rename; a copy then a delete can be seen half done), so such an output is refused there: an
output on object storage is a Delta or Iceberg table, whose commit is atomic (docs/PYTHONIO_ADAPTER.md, milestone 6).
Credentials and endpoints come from each library's own standard sources (``AWS_*``, ``GOOGLE_APPLICATION_CREDENTIALS``,
``AZURE_STORAGE_*``), never from Invaract.
"""
import os
import re

#: URI schemes of object storage, as pyarrow, delta-rs and pyiceberg spell them.
OBJECT_STORAGE = frozenset({"s3", "s3a", "gs", "gcs", "az", "abfs", "abfss"})

_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://")


def scheme(location):
    """The URI scheme of ``location``, lower case, or ``None`` for a plain path."""
    found = _SCHEME.match(os.fspath(location))
    return found.group(1).lower() if found else None


def on_object_storage(location):
    return scheme(location) in OBJECT_STORAGE


def uri(location):
    """``location`` as the libraries take it: a URI as given, a path made absolute (``file://`` stripped)."""
    location = os.fspath(location)
    kind = scheme(location)
    if kind is None:
        return os.path.abspath(location)
    if kind == "file":
        return os.path.abspath(location[len("file://"):])
    return location


def filesystem(location):
    """The pyarrow file system ``location`` is on, and the path within it."""
    import pyarrow.fs as fs

    return fs.FileSystem.from_uri(uri(location))
