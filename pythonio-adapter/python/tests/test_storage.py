# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Plain files, partitioned outputs, the other file formats, Delta Lake and Iceberg tables on real storage services: Google
Cloud Storage (an emulator, ``INVARACT_TEST_GCS=http://host:port``), Azure Blob Storage (Azurite,
``INVARACT_TEST_AZURE=http://host:port``) and HDFS (a running cluster, ``INVARACT_TEST_HDFS=hdfs://host:port``). Each backend's
tests run only when its variable is set (CI's storage job sets all three, so there they can never pass by being skipped).
Every library is reached through its standard settings alone, as a job's would be; the S3 tests are in test_tables.py."""
import json
import os
import urllib.request
import uuid

import pandas as pd
import pyarrow.fs as pafs
import pytest

import invaract
from invaract import _storage
from conftest import output, write_contract

#: Azurite's well-known development account and key (Microsoft's documentation; not a secret).
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})
MORE = pd.DataFrame({"region": ["east"], "total": [40]})

VARIABLES = {"gcs": "INVARACT_TEST_GCS", "azure": "INVARACT_TEST_AZURE", "hdfs": "INVARACT_TEST_HDFS"}
#: Read when the module loads: conftest's per-test environment clears every INVARACT_* variable but the checker's.
ENDPOINTS = {name: os.environ.get(variable) for name, variable in VARIABLES.items()}
#: Set by CI's storage job, where every backend must be there: a missing one fails rather than skips.
REQUIRED = os.environ.get("INVARACT_TEST_STORAGE_REQUIRED") == "true"


class Store:
    """One backend, reached as a job reaches it: ``root`` is the location prefix of a fresh, empty place on it, and
    ``fs``/``base`` the pyarrow file system and path the package itself resolves ``root`` to."""

    def __init__(self, name, root, object_storage):
        self.name, self.root, self.object_storage = name, root, object_storage
        self.fs, self.base = _storage.filesystem(root)

    def files(self, under=""):
        """Every file under ``root/under``, relative to ``root``, leaving out empty "folder" markers."""
        start = f"{self.base}/{under}".rstrip("/")
        found = self.fs.get_file_info(pafs.FileSelector(start, allow_not_found=True, recursive=True))
        return sorted(i.path[len(self.base) + 1:] for i in found if i.type == pafs.FileType.File and not i.path.endswith("/"))

    def put(self, relative, body=b"old"):
        path = f"{self.base}/{relative}"
        self.fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
        with self.fs.open_output_stream(path) as out:
            out.write(body)


def _no_proxy(monkeypatch, endpoint):
    host = endpoint.split("://")[-1].split(":")[0]
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(name, ",".join(filter(None, [os.environ.get(name), host, "127.0.0.1", "localhost"])))


def _gcs(monkeypatch, tmp_path, endpoint):
    """A fresh bucket on the emulator. pyarrow and pyiceberg find it through ``STORAGE_EMULATOR_HOST``, delta-rs through a
    service account file naming it (``gcs_base_url``, object_store's emulator setting)."""
    _no_proxy(monkeypatch, endpoint)
    bucket = f"lake-{uuid.uuid4().hex[:8]}"
    request = urllib.request.Request(f"{endpoint}/storage/v1/b?project=test", data=json.dumps({"name": bucket}).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request).read()
    account = tmp_path / "service-account.json"
    account.write_text(json.dumps({"gcs_base_url": endpoint, "disable_oauth": True, "client_email": "", "private_key": "",
                                   "private_key_id": ""}))
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", endpoint)
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT", str(account))
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test")
    return Store("gcs", f"gs://{bucket}", object_storage=True)


def _azure(monkeypatch, endpoint):
    """A fresh container on Azurite, reached through the variables delta-rs reads, which the package reads too."""
    _no_proxy(monkeypatch, endpoint)
    for name, value in {"AZURE_STORAGE_ACCOUNT_NAME": AZURITE_ACCOUNT, "AZURE_STORAGE_ACCOUNT_KEY": AZURITE_KEY,
                        "AZURE_STORAGE_USE_EMULATOR": "true", "AZURITE_BLOB_STORAGE_URL": endpoint}.items():
        monkeypatch.setenv(name, value)
    container = f"lake-{uuid.uuid4().hex[:8]}"
    store = Store("azure", f"az://{container}", object_storage=True)
    store.fs.create_dir(container)
    return store


def _hdfs(namenode):
    store = Store("hdfs", f"{namenode.rstrip('/')}/invaract-test-{uuid.uuid4().hex[:8]}", object_storage=False)
    store.fs.create_dir(store.base, recursive=True)
    return store


@pytest.fixture(params=sorted(VARIABLES))
def store(request, monkeypatch, tmp_path):
    endpoint = ENDPOINTS[request.param]
    if not endpoint:
        (pytest.fail if REQUIRED else pytest.skip)(f"set {VARIABLES[request.param]} to test {request.param}")
    if request.param == "gcs":
        return _gcs(monkeypatch, tmp_path, endpoint)
    if request.param == "azure":
        return _azure(monkeypatch, endpoint)
    return _hdfs(endpoint)


def run_writes(*frames, location, fmt="parquet", mode="overwrite", fields=None, **write_args):
    write_contract(outputs=output(location=location, fmt=fmt, save_mode=mode, fields=fields))
    with invaract.run("contract.yaml") as run:
        for frame in frames:
            invaract.write(frame, "report", **write_args)
    return run.run_id


def read_back(location, fmt="parquet"):
    """The rows of the dataset at ``location``, read through the package by a run that declares it as its input."""
    fields = "        - { name: region, type: string }\n        - { name: total, type: long }\n"
    write_contract(inputs=f"  - name: back\n    location: {location}\n    format: {fmt}\n    schema:\n      fields:\n{fields}",
                   outputs=output(location="local/unused"))
    with invaract.run("contract.yaml"):
        table = invaract.read("back", as_="arrow")
        invaract.write(REPORT, "report")
    return sorted(zip(table.column("region").to_pylist(), table.column("total").to_pylist()))


# ---- plain files ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_a_plain_output_is_published_and_reads_back(store, fmt):
    location = f"{store.root}/out/report"
    run_id = run_writes(REPORT, MORE, location=location, fmt=fmt)
    written = [f"out/report/part-{run_id}-00001.{fmt}", f"out/report/part-{run_id}-00002.{fmt}"]
    assert store.files() == sorted(written + (["out/report/_SUCCESS"] if store.object_storage else []))
    assert read_back(location, fmt) == [("east", 40), ("north", 10), ("south", 20)]


def test_an_overwrite_replaces_every_old_file(store):
    store.put("out/report/part-old.parquet")
    store.put("out/report/year=2025/part-older.parquet")
    run_id = run_writes(REPORT, location=f"{store.root}/out/report")
    assert [f for f in store.files() if not f.endswith("_SUCCESS")] == [f"out/report/part-{run_id}-00001.parquet"]


def test_an_append_keeps_the_old_files(store):
    first = run_writes(REPORT, location=f"{store.root}/out/report")
    second = run_writes(MORE, location=f"{store.root}/out/report", mode="append")
    assert {f"out/report/part-{first}-00001.parquet", f"out/report/part-{second}-00001.parquet"} <= set(store.files())
    assert read_back(f"{store.root}/out/report") == [("east", 40), ("north", 10), ("south", 20)]


def test_a_refused_run_writes_nothing(store):
    store.put("out/report/part-old.parquet")
    with pytest.raises(invaract.ContractViolation):
        run_writes(REPORT, pd.DataFrame({"region": [None], "total": [1]}), location=f"{store.root}/out/report")
    assert store.files() == ["out/report/part-old.parquet"]


def test_error_refuses_an_output_that_holds_data(store):
    store.put("out/report/part-old.parquet")
    with pytest.raises(FileExistsError, match="already holds data"):
        run_writes(REPORT, location=f"{store.root}/out/report", mode="error")
    assert store.files() == ["out/report/part-old.parquet"]


def test_a_partitioned_output_is_published_in_hive_folders_and_reads_back_with_its_key(store):
    location = f"{store.root}/out/report"
    run_writes(REPORT, MORE, location=location, fields=[("region", "string", True), ("total", "long", True)], partition_by=["region"])
    folders = sorted({f.split("/")[2] for f in store.files() if f.count("/") > 2})
    assert folders == ["region=east", "region=north", "region=south"]
    assert read_back(location) == [("east", 40), ("north", 10), ("south", 20)]


@pytest.mark.parametrize("fmt", ["json", "feather"])
def test_the_other_file_formats_are_written_and_read_back(store, fmt):
    location = f"{store.root}/out/report"
    run_writes(REPORT, location=location, fmt=fmt)
    assert read_back(location, fmt) == [("north", 10), ("south", 20)]


# ---- tables --------------------------------------------------------------------------------------------------------------

def test_a_delta_output_is_committed_only_when_the_run_passes_and_reads_back(store):
    pytest.importorskip("deltalake")
    location = f"{store.root}/delta/report"
    write_contract(outputs=output(location=location, fmt="delta"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(pd.DataFrame({"region": [None], "total": [1]}), "report")
    assert store.files("delta") == [], "the refused run's files are deleted"
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert any(f.startswith("delta/report/_delta_log/") for f in store.files())
    assert read_back(location, "delta") == [("north", 10), ("south", 20)]


def test_an_iceberg_output_with_its_warehouse_on_the_store(store, monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("sqlalchemy")
    import pyiceberg.catalog
    from pyiceberg.utils.config import Config

    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__URI", f"sqlite:///{tmp_path}/catalog.db")
    warehouse = f"{store.root}/warehouse"
    if store.name == "gcs":
        # pyiceberg's GCS file IO has no anonymous mode: an emulator takes any token
        monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__GCS__SERVICE__HOST", os.environ["STORAGE_EMULATOR_HOST"])
        monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__GCS__OAUTH2__TOKEN", "emulator")
        monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__GCS__OAUTH2__TOKEN_EXPIRES_AT", str(4_102_444_800_000))
    if store.name == "azure":
        # pyiceberg knows Azure as abfs[s]://<container>@<account>.dfs.core.windows.net, not az://
        warehouse = f"abfs://{store.base.split('/')[0]}@{AZURITE_ACCOUNT}.dfs.core.windows.net/warehouse"
        authority = os.environ["AZURITE_BLOB_STORAGE_URL"].split("://")[1]
        for key, value in {"ACCOUNT_NAME": AZURITE_ACCOUNT, "ACCOUNT_KEY": AZURITE_KEY, "BLOB_STORAGE_AUTHORITY": authority,
                           "BLOB_STORAGE_SCHEME": "http", "DFS_STORAGE_AUTHORITY": authority, "DFS_STORAGE_SCHEME": "http"}.items():
            monkeypatch.setenv(f"PYICEBERG_CATALOG__LAKE__ADLS__{key}", value)
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__WAREHOUSE", warehouse)
    monkeypatch.setattr(pyiceberg.catalog, "_ENV_CONFIG", Config())
    run_writes(REPORT, location="lake.sales.report", fmt="iceberg")
    assert any(f.startswith("warehouse/") and f.endswith(".parquet") for f in store.files())
    assert read_back("lake.sales.report", "iceberg") == [("north", 10), ("south", 20)]


# ---- the storage layer itself --------------------------------------------------------------------------------------------

def test_an_azure_location_is_configured_from_the_environment(monkeypatch):
    for name in ("AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY", "AZURE_STORAGE_USE_EMULATOR", "AZURITE_BLOB_STORAGE_URL"):
        monkeypatch.delenv(name, raising=False)
    if not hasattr(pafs, "AzureFileSystem"):
        with pytest.raises(invaract.CheckNotRun, match="needs pyarrow 16 or later"):
            _storage.filesystem("az://lake/out")
        return
    with pytest.raises(invaract.CheckNotRun, match="az://<container>/<path> with AZURE_STORAGE_ACCOUNT_NAME set"):
        _storage.filesystem("az://lake/out")
    azure_fs, path = _storage.filesystem("abfss://lake@acct.dfs.core.windows.net/out/report/")
    assert isinstance(azure_fs, pafs.AzureFileSystem) and path == "lake/out/report"
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "acct")
    assert _storage.filesystem("az://lake/out/report")[1] == "lake/out/report"
    assert _storage.filesystem("az://lake")[1] == "lake"


def test_a_gcs_location_goes_to_the_emulator_the_environment_names(monkeypatch):
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "http://127.0.0.1:4443")
    gcs, path = _storage.filesystem("gs://bucket/out/report")
    assert isinstance(gcs, pafs.GcsFileSystem) and path == "bucket/out/report"
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:4443")
    assert _storage.filesystem("gcs://bucket")[1] == "bucket"
