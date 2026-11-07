# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Where a location lives: a local or mounted file system, or object storage (S3, GCS, Azure) named by its URI.

Reads work from either. Object storage has no rename, so a plain Parquet or CSV output there is published by copying, as
Spark does (``_files``), and a Delta or Iceberg table there by its own atomic commit (``_tables``).
Credentials and endpoints come from each library's own standard sources (``AWS_*``, ``GOOGLE_APPLICATION_CREDENTIALS``,
``AZURE_STORAGE_*``), never from Invaract.
"""
import os
import re

from .errors import CheckNotRun

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


#: Azure's URI schemes: ``az://<container>/<path>`` (delta-rs's and fsspec's), and
#: ``abfs[s]://<container>@<account>.dfs.core.windows.net/<path>``.
AZURE = frozenset({"az", "abfs", "abfss"})
#: Google Cloud Storage's URI schemes.
GCS = frozenset({"gs", "gcs"})


def filesystem(location):
    """The pyarrow file system ``location`` is on, and the path within it."""
    import pyarrow.fs as fs

    target = uri(location)
    kind = scheme(target)
    if kind in AZURE:
        return azure(target)
    if kind in GCS and os.environ.get("STORAGE_EMULATOR_HOST"):
        return gcs_emulator(target)
    return fs.FileSystem.from_uri(target)


def azure(location):
    """``(file system, path)`` for an Azure location, configured as delta-rs configures itself, from the environment:
    ``AZURE_STORAGE_ACCOUNT_NAME`` (or the account an ``abfs[s]://`` location names), ``AZURE_STORAGE_ACCOUNT_KEY`` (or,
    without one, Azure's default credential: a managed identity, ``AZURE_CLIENT_ID``/``AZURE_CLIENT_SECRET``/
    ``AZURE_TENANT_ID``, the Azure CLI's login, ...), and ``AZURE_STORAGE_USE_EMULATOR=true`` for Azurite (at
    ``AZURITE_BLOB_STORAGE_URL``, by default ``http://127.0.0.1:10000``). pyarrow reads none of these itself, and knows no
    ``az://``."""
    import urllib.parse

    import pyarrow.fs as fs

    if not hasattr(fs, "AzureFileSystem"):
        raise CheckNotRun(f"reading or writing Azure storage needs pyarrow 16 or later (this is {_pyarrow_version()}): {location}")
    parts = urllib.parse.urlsplit(location)
    container, _, host = parts.netloc.partition("@")
    account = host.split(".", 1)[0] if host else os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    if not account or not container:
        raise CheckNotRun(f"an Azure location is az://<container>/<path> with AZURE_STORAGE_ACCOUNT_NAME set, or "
                          f"abfss://<container>@<account>.dfs.core.windows.net/<path>, not {location!r}")
    options = {}
    if os.environ.get("AZURE_STORAGE_USE_EMULATOR", "").strip().lower() == "true":
        emulator = urllib.parse.urlsplit(os.environ.get("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000"))
        options = {"blob_storage_authority": emulator.netloc, "blob_storage_scheme": emulator.scheme,
                   "dfs_storage_authority": emulator.netloc, "dfs_storage_scheme": emulator.scheme}
    key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY") or None
    azure_fs = fs.AzureFileSystem(account_name=account, account_key=key, **options)
    return azure_fs, f"{container}/{parts.path.strip('/')}".rstrip("/")


def gcs_emulator(location):
    """``(file system, path)`` on a Google Cloud Storage emulator, named as every Google client library names one:
    ``STORAGE_EMULATOR_HOST`` (``http://127.0.0.1:4443``), with no credentials, as an emulator takes none."""
    import urllib.parse

    import pyarrow.fs as fs

    host = os.environ["STORAGE_EMULATOR_HOST"]
    emulator = urllib.parse.urlsplit(host if "://" in host else f"http://{host}")
    parts = urllib.parse.urlsplit(location)
    gcs = fs.GcsFileSystem(anonymous=True, scheme=emulator.scheme, endpoint_override=emulator.netloc)
    return gcs, f"{parts.netloc}/{parts.path.strip('/')}".rstrip("/")


def _pyarrow_version():
    import pyarrow

    return pyarrow.__version__
