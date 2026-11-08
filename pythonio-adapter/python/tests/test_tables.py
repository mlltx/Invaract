# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Delta Lake and Iceberg tables and object storage (milestone 6): a table output is staged during the run and committed only
when the run passed, in one commit; a refused or failed run leaves the table as it was, and nothing of its own behind. Object
storage is S3, served in-process by moto; plain Parquet or CSV outputs there are refused."""
import json
import os
import subprocess
import sys
import textwrap

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from invaract import _storage
from conftest import PYTHON, output, write_contract

if os.environ.get("INVARACT_TEST_TABLES") == "true":
    # CI's table rows: the libraries must be there, so these tests can never pass by being skipped
    import deltalake  # noqa: F401
    import moto  # noqa: F401
    import pyiceberg  # noqa: F401
    import sqlalchemy  # noqa: F401
else:
    deltalake = pytest.importorskip("deltalake")
    pytest.importorskip("pyiceberg")
    pytest.importorskip("sqlalchemy")

import pyiceberg.catalog  # noqa: E402
from pyiceberg.catalog import load_catalog  # noqa: E402
from pyiceberg.utils.config import Config  # noqa: E402

REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})
MORE = pd.DataFrame({"region": ["east"], "total": [40]})
ICEBERG = "lake.sales.report"


# ---- where data lives --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("location,scheme,remote", [
    ("data/out/report", None, False), ("/abs/report", None, False), ("file:///abs/report", "file", False),
    ("s3://bucket/report", "s3", True), ("S3A://bucket/report", "s3a", True), ("gs://bucket/report", "gs", True),
    ("gcs://bucket/report", "gcs", True), ("az://container/report", "az", True), ("abfss://c@a.dfs.core.windows.net/r", "abfss", True),
    ("hdfs://namenode/report", "hdfs", False), ("lake.sales.report", None, False),
])
def test_where_a_location_lives(location, scheme, remote):
    assert (_storage.scheme(location), _storage.scheme(location) in _storage.OBJECT_STORAGE) == (scheme, remote)


def test_a_location_as_the_libraries_take_it(tmp_path):
    assert _storage.uri("data/out") == os.path.join(str(tmp_path), "data", "out")
    assert _storage.uri("file:///abs/report") == "/abs/report"
    assert _storage.uri("s3://bucket/report") == "s3://bucket/report"


# ---- Delta -------------------------------------------------------------------------------------------------------------

def delta_rows(location="data/out/report"):
    table = deltalake.DeltaTable(_storage.uri(location))
    return sorted(table.to_pyarrow_dataset().to_table().to_pylist(), key=lambda r: r["region"]), table


def leftovers(location="data/out/report"):
    """Files in a Delta table's folder that no commit ever named (an overwritten file stays, named by an older commit, until
    the table is vacuumed)."""
    folder = _storage.uri(location)
    if not os.path.isdir(folder):
        return []
    log, named = os.path.join(folder, "_delta_log"), set()
    for commit in sorted(os.listdir(log)) if os.path.isdir(log) else []:
        if commit.endswith(".json"):
            with open(os.path.join(log, commit), encoding="utf-8") as f:
                named |= {json.loads(line)["add"]["path"] for line in f if '"add"' in line}
    return sorted(f for f in os.listdir(folder) if f != "_delta_log" and f not in named)


def existing_delta(rows=(("west", 5),), location="data/out/report"):
    deltalake.write_deltalake(_storage.uri(location), pa.table({"region": [r for r, _ in rows], "total": [t for _, t in rows]}))


def test_a_delta_output_is_created_in_one_commit_with_every_write_and_its_required_columns_not_null():
    write_contract(outputs=output(fmt="delta"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
        invaract.write(MORE, "report")
        assert not os.path.isdir("data/out/report/_delta_log"), "no table before the run ends"
    rows, table = delta_rows()
    assert rows == [{"region": "east", "total": 40}, {"region": "north", "total": 10}, {"region": "south", "total": 20}]
    assert table.version() == 0 and [h["operation"] for h in table.history()] == ["CREATE TABLE"]
    assert [(f.name, f.nullable) for f in table.schema().fields] == [("region", False), ("total", False)]
    assert leftovers() == []


def test_a_refused_run_creates_no_delta_table_and_leaves_no_files():
    write_contract(outputs=output(fmt="delta") + output(name="audit", location="data/out/audit", fmt="delta"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT.astype({"total": str}), "audit")
    assert leftovers() == [] and not os.path.isdir("data/out/report/_delta_log")


def test_a_failed_job_leaves_an_existing_delta_table_as_it_was():
    existing_delta()
    write_contract(outputs=output(fmt="delta"))
    with pytest.raises(ZeroDivisionError):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            1 / 0
    rows, table = delta_rows()
    assert rows == [{"region": "west", "total": 5}] and table.version() == 0 and leftovers() == []


@pytest.mark.parametrize("mode,expected,version", [
    ("overwrite", ["north", "south"], 1),
    ("append", ["north", "south", "west"], 1),
    ("ignore", ["west"], 0),
])
def test_delta_save_modes_on_a_table_with_rows(mode, expected, version):
    existing_delta()
    write_contract(outputs=output(fmt="delta", save_mode=mode))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    rows, table = delta_rows()
    assert [r["region"] for r in rows] == expected and table.version() == version and leftovers() == []


def test_delta_error_refuses_a_table_with_rows_and_commits_nothing():
    existing_delta()
    write_contract(outputs=output(fmt="delta", save_mode="error"))
    with pytest.raises(FileExistsError, match="already holds data"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    rows, table = delta_rows()
    assert rows == [{"region": "west", "total": 5}] and table.version() == 0 and leftovers() == []


@pytest.mark.parametrize("mode", ["error", "ignore", "append", "overwrite"])
def test_every_save_mode_creates_a_delta_table_that_does_not_exist(mode):
    write_contract(outputs=output(fmt="delta", save_mode=mode))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert [r["region"] for r in delta_rows()[0]] == ["north", "south"]


def test_an_empty_value_in_a_required_column_of_a_delta_output_is_refused():
    existing_delta()
    write_contract(outputs=output(fmt="delta"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(pd.DataFrame({"region": ["north", None], "total": [1, 2]}), "report")
    assert delta_rows()[1].version() == 0 and leftovers() == []


def test_a_delta_input_is_read_with_only_its_declared_columns():
    deltalake.write_deltalake(_storage.uri("data/in/orders"), pa.table({"order_id": [1, 2], "region": ["n", None], "extra": [0, 0]}))
    write_contract(inputs="  - name: orders\n    location: data/in/orders\n    format: delta\n    schema:\n      fields:\n"
                          "        - { name: order_id, type: long, required: true }\n        - { name: region, type: string }\n",
                   outputs=output())
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_="arrow")
        invaract.write(REPORT, "report")
    assert orders.column_names == ["order_id", "region"] and orders.num_rows == 2


def test_a_delta_input_is_read_in_chunks_with_only_its_declared_columns():
    deltalake.write_deltalake(_storage.uri("data/in/orders"), pa.table({"order_id": [1, 2, 3], "region": ["n", None, "s"], "extra": [0, 0, 0]}))
    write_contract(inputs="  - name: orders\n    location: data/in/orders\n    format: delta\n    schema:\n      fields:\n"
                          "        - { name: order_id, type: long, required: true }\n        - { name: region, type: string }\n",
                   outputs=output())
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_="arrow", chunksize=2))
        invaract.write(REPORT, "report")
    assert [c.num_rows for c in chunks] == [2, 1] and chunks[0].column_names == ["order_id", "region"]
    assert sorted(v for c in chunks for v in c.column("order_id").to_pylist()) == [1, 2, 3]


def test_a_delta_input_that_does_not_exist_is_an_error():
    write_contract(inputs="  - name: orders\n    location: data/in/orders\n    format: delta\n", outputs=output())
    with pytest.raises(FileNotFoundError, match="no Delta table at data/in/orders"):
        with invaract.run("contract.yaml"):
            invaract.read("orders")


# ---- Iceberg -----------------------------------------------------------------------------------------------------------

@pytest.fixture
def catalog(monkeypatch, tmp_path):
    """The catalog ``lake``: SQLite, with its warehouse in the test's folder, configured as a platform would, by environment."""
    os.makedirs(tmp_path / "warehouse")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__URI", f"sqlite:///{tmp_path}/catalog.db")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__WAREHOUSE", f"file://{tmp_path}/warehouse")
    return reconfigure(monkeypatch)


def reconfigure(monkeypatch):
    """pyiceberg reads its PYICEBERG_ variables once, when imported; a job's are set before it starts, a test's after."""
    monkeypatch.setattr(pyiceberg.catalog, "_ENV_CONFIG", Config())
    return load_catalog("lake")


def iceberg_rows(catalog, identifier="sales.report"):
    table = catalog.load_table(identifier)
    return sorted(r["region"] for r in table.scan().to_arrow().to_pylist()), table


def existing_iceberg(catalog, regions=("west",)):
    catalog.create_namespace_if_not_exists("sales")
    schema = pa.schema([pa.field("region", pa.string(), nullable=False), pa.field("total", pa.int64(), nullable=False)])
    table = catalog.create_table("sales.report", schema=schema)
    table.append(pa.table({"region": list(regions), "total": [5] * len(regions)}, schema=schema))


def test_an_iceberg_output_and_its_namespace_are_created_in_one_commit(catalog):
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
        invaract.write(MORE, "report")
        assert not catalog.table_exists("sales.report"), "no table before the run ends"
    regions, table = iceberg_rows(catalog)
    assert regions == ["east", "north", "south"] and len(table.snapshots()) == 1
    assert [(f.name, f.required) for f in table.schema().fields] == [("region", True), ("total", True)]


def test_a_refused_run_creates_no_iceberg_table(catalog):
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg") + output(name="audit", location="lake.sales.audit", fmt="iceberg"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT.astype({"total": str}), "audit")
    assert not catalog.table_exists("sales.report") and not catalog.table_exists("sales.audit")


@pytest.mark.parametrize("mode,expected,snapshots", [
    ("overwrite", ["north", "south"], 3),  # the overwrite commits a delete and an append, as one transaction
    ("append", ["north", "south", "west"], 2),
    ("ignore", ["west"], 1),
])
def test_iceberg_save_modes_on_a_table_with_rows(catalog, mode, expected, snapshots):
    existing_iceberg(catalog)
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg", save_mode=mode))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    regions, table = iceberg_rows(catalog)
    assert regions == expected and len(table.snapshots()) == snapshots


@pytest.mark.parametrize("mode", ["append", "ignore"])
def test_an_iceberg_outputs_write_event_names_the_snapshot_its_commit_made(catalog, monkeypatch, mode):
    from conftest import events, notify_to_file

    existing_iceberg(catalog)
    notify_to_file(monkeypatch)
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg", save_mode=mode))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    written = [e for e in events() if e["eventType"] == "WRITE"]
    if mode == "ignore":
        # a table that holds rows is left as it is: nothing was written, so there is no write event
        assert written == []
        return
    (event,) = written
    table = catalog.load_table("sales.report")
    assert event["icebergSnapshotId"] == table.current_snapshot().snapshot_id
    assert (event["location"], event["rowCount"], event["fileCount"], event["deltaVersion"]) == (ICEBERG, 2, 1, None)
    assert event["catalog"] == {"technology": "iceberg", "catalogName": "lake", "namespace": ["sales"], "table": "report"}


def test_iceberg_error_refuses_a_table_with_rows_and_commits_nothing(catalog):
    existing_iceberg(catalog)
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg", save_mode="error"))
    with pytest.raises(FileExistsError, match="already holds data"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    regions, table = iceberg_rows(catalog)
    assert regions == ["west"] and len(table.snapshots()) == 1


def test_iceberg_error_and_ignore_write_a_table_with_no_rows(catalog):
    existing_iceberg(catalog, regions=())
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg", save_mode="error"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert iceberg_rows(catalog)[0] == ["north", "south"]


def test_a_failed_job_leaves_an_existing_iceberg_table_as_it_was(catalog):
    existing_iceberg(catalog)
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg"))
    with pytest.raises(ZeroDivisionError):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            1 / 0
    regions, table = iceberg_rows(catalog)
    assert regions == ["west"] and len(table.snapshots()) == 1


def test_an_iceberg_input_is_read_with_only_its_declared_columns(catalog):
    existing_iceberg(catalog, regions=("west", "east"))
    write_contract(inputs=f"  - name: orders\n    location: {ICEBERG}\n    format: iceberg\n    schema:\n      fields:\n"
                          "        - { name: total, type: long, required: true }\n", outputs=output())
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_="arrow")
        invaract.write(REPORT, "report")
    assert orders.column_names == ["total"] and orders.num_rows == 2


def test_an_iceberg_input_is_read_in_chunks_with_only_its_declared_columns(catalog):
    existing_iceberg(catalog, regions=("west", "east", "north"))
    write_contract(inputs=f"  - name: orders\n    location: {ICEBERG}\n    format: iceberg\n    schema:\n      fields:\n"
                          "        - { name: total, type: long, required: true }\n", outputs=output())
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_="arrow", chunksize=2))
        invaract.write(REPORT, "report")
    assert [c.num_rows for c in chunks] == [2, 1] and chunks[0].column_names == ["total"]


@pytest.mark.parametrize("location", ["lake", "lake.report", ".sales.report"])
def test_an_iceberg_location_names_its_catalog_namespace_and_table(catalog, location):
    write_contract(outputs=output(location=location, fmt="iceberg"))
    with pytest.raises(invaract.CheckNotRun, match=r"an Iceberg location is <catalog>\.<namespace>\.<table>"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")


# ---- a library that is not installed -----------------------------------------------------------------------------------

@pytest.mark.parametrize("fmt,module,location", [("delta", "deltalake", "data/out/report"), ("iceberg", "pyiceberg", ICEBERG)])
def test_a_table_format_whose_library_is_missing_is_not_checked(monkeypatch, fmt, module, location):
    monkeypatch.setitem(sys.modules, module, None)
    write_contract(outputs=output(location=location, fmt=fmt))
    with pytest.raises(invaract.CheckNotRun, match=rf'needs the {module} package .*pip install "invaract\[{fmt}\]"'):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")


# ---- object storage (S3, served by moto) -------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def s3_server():
    moto_server = pytest.importorskip("moto.server")
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def s3(s3_server, monkeypatch):
    """A fresh bucket on the moto server, reached through the standard AWS variables alone, as a job's would be."""
    import uuid

    import boto3

    for name, value in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1",
                        "AWS_ENDPOINT_URL": s3_server, "AWS_ALLOW_HTTP": "true", "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}.items():
        monkeypatch.setenv(name, value)
    bucket = f"lake-{uuid.uuid4().hex[:8]}"
    boto3.client("s3").create_bucket(Bucket=bucket)
    return bucket


def keys(bucket, prefix=""):
    """The bucket's objects, leaving out empty "folder" markers ("out/report/"): pyarrow leaves one wherever it deletes a
    file, as Hadoop's S3 client does, and readers ignore them."""
    import boto3

    listed = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    return sorted(o["Key"] for o in listed if not o["Key"].endswith("/"))


def put(bucket, key, body=b"old"):
    import boto3

    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)


def run_writes(*frames, fmt="parquet", mode="overwrite", location):
    write_contract(outputs=output(location=location, fmt=fmt, save_mode=mode))
    with invaract.run("contract.yaml") as run:
        for frame in frames:
            invaract.write(frame, "report")
    return run.run_id


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_a_plain_output_on_object_storage_is_published_with_success_written_last(s3, fmt):
    run_id = run_writes(REPORT, MORE, fmt=fmt, location=f"s3://{s3}/out/report")
    assert keys(s3) == [f"out/report/_SUCCESS", f"out/report/part-{run_id}-00001.{fmt}", f"out/report/part-{run_id}-00002.{fmt}"]
    import pyarrow.dataset as ds

    table = ds.dataset(f"s3://{s3}/out/report", format=fmt).to_table()
    assert sorted(table.column("region").to_pylist()) == ["east", "north", "south"]


def test_an_object_storage_overwrite_replaces_every_old_file_and_marker(s3):
    put(s3, "out/report/part-old.parquet")
    put(s3, "out/report/year=2025/part-older.parquet")
    put(s3, "out/report/_SUCCESS", b"")
    run_id = run_writes(REPORT, location=f"s3://{s3}/out/report")
    assert keys(s3) == ["out/report/_SUCCESS", f"out/report/part-{run_id}-00001.parquet"]


def test_an_object_storage_append_keeps_the_old_files_and_rewrites_success(s3):
    put(s3, "out/report/part-old.parquet")
    put(s3, "out/report/_SUCCESS", b"")
    run_id = run_writes(REPORT, mode="append", location=f"s3://{s3}/out/report")
    assert keys(s3) == sorted(["out/report/_SUCCESS", "out/report/part-old.parquet", f"out/report/part-{run_id}-00001.parquet"])


@pytest.mark.parametrize("mode,old,written", [("ignore", True, False), ("ignore", False, True), ("error", False, True)])
def test_object_storage_ignore_and_error(s3, mode, old, written):
    if old:
        put(s3, "out/report/part-old.parquet")
    run_writes(REPORT, mode=mode, location=f"s3://{s3}/out/report")
    assert ("out/report/_SUCCESS" in keys(s3)) == written


def test_object_storage_error_refuses_an_output_with_data_and_writes_nothing(s3):
    put(s3, "out/report/part-old.parquet")
    with pytest.raises(FileExistsError, match="already holds data"):
        run_writes(REPORT, mode="error", location=f"s3://{s3}/out/report")
    assert keys(s3) == ["out/report/part-old.parquet"]


def test_a_refused_run_never_writes_a_plain_output_to_object_storage(s3):
    put(s3, "out/report/part-old.parquet")
    with pytest.raises(invaract.ContractViolation):
        run_writes(REPORT, pd.DataFrame({"region": [None], "total": [1]}), location=f"s3://{s3}/out/report")
    assert keys(s3) == ["out/report/part-old.parquet"]


def test_a_location_on_a_file_system_invaract_does_not_know_is_refused_not_written_locally():
    write_contract(outputs=output(location="ftp://host/out/report"))
    with pytest.raises(invaract.CheckNotRun, match="cannot write files to ftp://host/out/report: it writes to local and mounted "
                                                   r"file systems, HDFS \(hdfs://, viewfs://\) and object storage"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert not os.path.exists("ftp:")


def test_plain_parquet_and_csv_inputs_are_read_from_object_storage(s3):
    import pyarrow.fs as fs

    s3fs, _ = fs.FileSystem.from_uri(f"s3://{s3}/in")
    pq.write_table(pa.table({"order_id": [1, 2]}), f"{s3}/in/orders/part-0.parquet", filesystem=s3fs)
    with s3fs.open_output_stream(f"{s3}/in/customers.csv") as out:
        out.write(b"customer_id,region\n1,north\n")
    write_contract(inputs=f"  - name: orders\n    location: s3://{s3}/in/orders\n    format: parquet\n"
                          f"  - name: customers\n    location: s3://{s3}/in/customers.csv\n    format: csv\n    schema:\n"
                          "      fields:\n        - { name: customer_id, type: long, required: true }\n", outputs=output())
    with invaract.run("contract.yaml"):
        assert invaract.read("orders", as_="arrow").column("order_id").to_pylist() == [1, 2]
        assert invaract.read("customers", as_="arrow").schema.field("customer_id").type == pa.int64()
        invaract.write(REPORT, "report")


def test_a_delta_output_on_object_storage_is_committed_only_when_the_run_passes(s3):
    import boto3

    location = f"s3://{s3}/report"
    write_contract(outputs=output(location=location, fmt="delta"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(pd.DataFrame({"region": [None], "total": [1]}), "report")
    assert [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket=s3).get("Contents", [])] == [], "the refused run's files are deleted"
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    table = deltalake.DeltaTable(location)
    assert table.to_pyarrow_dataset().to_table().num_rows == 2 and table.version() == 0


def test_an_iceberg_output_with_its_warehouse_on_object_storage(s3, monkeypatch, tmp_path):
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__URI", f"sqlite:///{tmp_path}/catalog.db")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__WAREHOUSE", f"s3://{s3}/warehouse")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__S3__ENDPOINT", os.environ["AWS_ENDPOINT_URL"])
    catalog = reconfigure(monkeypatch)
    write_contract(outputs=output(location=ICEBERG, fmt="iceberg"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert iceberg_rows(catalog)[0] == ["north", "south"]


def test_a_job_writing_delta_to_object_storage_exits_when_it_ends(s3, tmp_path):
    """deltalake 1.6's to_pyarrow_table leaves a process that used S3 unable to exit; the package must not."""
    write_contract(inputs=f"  - name: orders\n    location: s3://{s3}/in\n    format: delta\n",
                   outputs=output(location=f"s3://{s3}/report", fmt="delta"))
    deltalake.write_deltalake(f"s3://{s3}/in", pa.table({"region": ["n"], "total": [1]}))
    script = textwrap.dedent(f"""\
        import sys
        sys.path.insert(0, {PYTHON!r})
        import invaract
        with invaract.run("contract.yaml"):
            invaract.write(invaract.read("orders"), "report")
        """)
    done = subprocess.run([sys.executable, "-c", script], env=os.environ.copy(), timeout=60, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert deltalake.DeltaTable(f"s3://{s3}/report").to_pyarrow_dataset().to_table().num_rows == 1


def test_a_dry_run_writes_a_delta_table_where_the_job_says_and_drafts_its_contract(monkeypatch):
    from conftest import event_of, notify_to_file

    notify_to_file(monkeypatch)
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    with invaract.run():
        invaract.write(REPORT, "report", location="data/out/report", format="delta")
    assert [r["region"] for r in delta_rows()[0]] == ["north", "south"]
    draft = event_of("DRY_RUN_SUMMARY")["mergedContractYaml"]
    assert "location: data/out/report" in draft and "format: delta" in draft
    # the table's first commit, as its write event says
    written = event_of("WRITE")
    assert (written["format"], written["deltaVersion"], written["fileCount"], written["rowCount"]) == ("delta", 0, 1, 2)
    assert written["bytesWritten"] > 0 and written["icebergSnapshotId"] is None


# ---- HDFS: the same staging and rename, through a pyarrow file system --------------------------------------------------

@pytest.fixture
def hdfs(monkeypatch, tmp_path):
    """hdfs://namenode/... served by a local pyarrow file system rooted in the test's folder: the package's HDFS output goes
    through pyarrow's file-system interface only, so this runs exactly its code (a real cluster needs Hadoop's libhdfs)."""
    import pyarrow.fs as fs

    root = tmp_path / "hdfs"
    root.mkdir()
    real = _storage.filesystem

    def filesystem(location):
        if str(location).startswith("hdfs://namenode/"):
            return fs.SubTreeFileSystem(str(root), fs.LocalFileSystem()), str(location)[len("hdfs://namenode/"):]
        return real(location)

    monkeypatch.setattr(_storage, "filesystem", filesystem)
    return root


def hdfs_files(root, path="warehouse"):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs_ in os.walk(root / path) for f in fs_)


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_an_hdfs_output_is_staged_beside_its_location_and_renamed_into_place(hdfs, fmt):
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report", fmt=fmt))
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
        assert os.listdir(hdfs / "warehouse") == [f".report.invaract-staging-{run.run_id}"], "staged beside it, on HDFS"
    assert hdfs_files(hdfs) == [f"warehouse/report/part-{run.run_id}-00001.{fmt}"]


def test_an_hdfs_overwrite_replaces_the_folder_and_leaves_nothing_beside_it(hdfs):
    os.makedirs(hdfs / "warehouse" / "report")
    pq.write_table(pa.table({"region": ["old"], "total": [1]}), hdfs / "warehouse" / "report" / "old.parquet")
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report"))
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
    assert hdfs_files(hdfs) == [f"warehouse/report/part-{run.run_id}-00001.parquet"]


def test_an_hdfs_append_adds_and_ignore_and_error_respect_what_is_there(hdfs):
    os.makedirs(hdfs / "warehouse" / "report")
    pq.write_table(pa.table({"region": ["old"], "total": [1]}), hdfs / "warehouse" / "report" / "old.parquet")
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report", save_mode="append"))
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
    assert hdfs_files(hdfs) == ["warehouse/report/old.parquet", f"warehouse/report/part-{run.run_id}-00001.parquet"]
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report", save_mode="ignore"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert len(hdfs_files(hdfs)) == 2 and os.listdir(hdfs / "warehouse") == ["report"]
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report", save_mode="error"))
    with pytest.raises(FileExistsError, match="already holds data"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert len(hdfs_files(hdfs)) == 2 and os.listdir(hdfs / "warehouse") == ["report"]


def test_a_refused_run_leaves_hdfs_as_it_was(hdfs):
    write_contract(outputs=output(location="hdfs://namenode/warehouse/report"))
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(pd.DataFrame({"region": [None], "total": [1]}), "report")
    assert os.listdir(hdfs / "warehouse") == []


# ---- catalog registration: an Iceberg table names its catalog, namespace and table -------------------------------------

def registered(text, catalog):
    """A contract dataset (``output()``'s text, or an input's) with a ``catalog:`` requirement."""
    return text + f"    catalog: {catalog}\n"


@pytest.mark.parametrize("location,fmt,expected", [
    ("lake.sales.report", "iceberg", {"technology": "iceberg", "catalogName": "lake", "namespace": ["sales"], "table": "report"}),
    ("lake.a.b.report", "iceberg", {"technology": "iceberg", "catalogName": "lake", "namespace": ["a", "b"], "table": "report"}),
    ("lake.report", "iceberg", None), ("lake.sales.report", "parquet", None), ("data/out/report", "delta", None),
    (None, "iceberg", None),
])
def test_the_catalog_a_dataset_is_registered_in(location, fmt, expected):
    from invaract import _datasets

    assert _datasets.catalog_of(location, fmt) == expected


def test_an_iceberg_output_in_the_catalog_the_contract_requires_is_published(catalog):
    write_contract(outputs=registered(output(location=ICEBERG, fmt="iceberg"),
                                      "{ required: true, technology: iceberg, catalogName: lake, namespace: [sales], table: report }"))
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert iceberg_rows(catalog)[0] == ["north", "south"]


def test_an_iceberg_output_in_another_catalog_than_the_contract_requires_is_refused(catalog):
    write_contract(outputs=registered(output(location=ICEBERG, fmt="iceberg"), "{ required: true, catalogName: warehouse }"))
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert refused.value.violations == ["OUTPUT_CATALOG_MISMATCH"]
    assert "catalogName (expected 'warehouse', actual 'lake')" in str(refused.value)
    assert not catalog.table_exists("sales.report")


@pytest.mark.parametrize("fmt", ["parquet", "delta"])
def test_an_output_named_by_its_path_is_not_registered_in_any_catalog(fmt):
    write_contract(outputs=registered(output(fmt=fmt), "{ required: true }"))
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert refused.value.violations == ["MISSING_OUTPUT_CATALOG_REGISTRATION"]
    assert not os.path.exists("data/out/report") or os.listdir("data/out/report") == []


def test_an_iceberg_input_in_another_namespace_than_the_contract_requires_is_refused_at_its_read(catalog):
    existing_iceberg(catalog)
    write_contract(inputs=registered(f"  - name: orders\n    location: {ICEBERG}\n    format: iceberg\n", "{ required: true, namespace: [staging] }"),
                   outputs=output())
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.read("orders")
    assert refused.value.violations == ["INPUT_CATALOG_MISMATCH"]
    assert "namespace (expected 'staging', actual 'sales')" in str(refused.value)


# ---- partitioned outputs on object storage, HDFS, Delta and Iceberg ----------------------------------------------------

NORTH = pd.DataFrame({"region": ["north"], "total": [70]})


def partitioned(location, fmt="parquet", mode=None, save_mode="overwrite"):
    overwrite = f"    partitionOverwriteMode: {mode}\n" if mode else ""
    write_contract(outputs=f"  - name: report\n    location: {location}\n    format: {fmt}\n    saveMode: {save_mode}\n{overwrite}"
                           "    schema:\n      fields:\n        - { name: region, type: string, required: true, partitioned: true }\n"
                           "        - { name: total, type: long, required: true }\n")


def write_report(frame):
    with invaract.run("contract.yaml") as run:
        invaract.write(frame, "report")
    return run.run_id


@pytest.mark.parametrize("mode, kept", [("dynamic", True), ("static", False)])
def test_a_partitioned_object_storage_overwrite_replaces_the_written_partitions_or_all(s3, mode, kept):
    put(s3, "out/report/region=north/old.parquet")
    put(s3, "out/report/region=south/old.parquet")
    partitioned(f"s3://{s3}/out/report", mode=mode)
    run_id = write_report(NORTH)
    expected = [f"out/report/region=north/part-{run_id}-00001-00001-0.parquet", "out/report/_SUCCESS"]
    if kept:
        expected.append("out/report/region=south/old.parquet")
    assert keys(s3) == sorted(expected)


def test_a_partitioned_object_storage_append_adds_to_the_folders(s3):
    put(s3, "out/report/region=north/old.parquet")
    partitioned(f"s3://{s3}/out/report", save_mode="append")
    run_id = write_report(REPORT)
    assert keys(s3) == sorted(["out/report/_SUCCESS", "out/report/region=north/old.parquet",
                               f"out/report/region=north/part-{run_id}-00001-00001-0.parquet",
                               f"out/report/region=south/part-{run_id}-00001-00001-0.parquet"])


def test_a_partitioned_hdfs_dynamic_overwrite_renames_only_the_written_partitions_into_place(hdfs):
    for region in ("north", "south"):
        os.makedirs(hdfs / "warehouse" / "report" / f"region={region}")
        pq.write_table(pa.table({"total": [1]}), hdfs / "warehouse" / "report" / f"region={region}" / "old.parquet")
    partitioned("hdfs://namenode/warehouse/report", mode="dynamic")
    run_id = write_report(NORTH)
    assert hdfs_files(hdfs, "warehouse") == [f"warehouse/report/region=north/part-{run_id}-00001-00001-0.parquet",
                                             "warehouse/report/region=south/old.parquet"]


def test_a_partitioned_hdfs_append_and_static_overwrite(hdfs):
    partitioned("hdfs://namenode/warehouse/report", save_mode="append")
    first = write_report(REPORT)
    second = write_report(NORTH)
    assert hdfs_files(hdfs, "warehouse") == sorted([f"warehouse/report/region=north/part-{first}-00001-00001-0.parquet",
                                                    f"warehouse/report/region=north/part-{second}-00001-00001-0.parquet",
                                                    f"warehouse/report/region=south/part-{first}-00001-00001-0.parquet"])
    partitioned("hdfs://namenode/warehouse/report")
    third = write_report(NORTH)
    assert hdfs_files(hdfs, "warehouse") == [f"warehouse/report/region=north/part-{third}-00001-00001-0.parquet"]


def delta_partitions(location="data/out/report"):
    return sorted((r["region"], r["total"]) for r in delta_rows(location)[0])


def test_a_new_delta_table_is_created_partitioned_with_the_keys_out_of_its_files():
    partitioned("data/out/report", fmt="delta")
    write_report(REPORT)
    table = deltalake.DeltaTable("data/out/report")
    assert table.metadata().partition_columns == ["region"]
    assert delta_partitions() == [("north", 10), ("south", 20)]
    folders = sorted(f for f in os.listdir("data/out/report") if f.startswith("region="))
    assert folders == ["region=north", "region=south"]
    data_file = next(f for f in os.listdir("data/out/report/region=north") if f.endswith(".parquet"))
    assert pq.ParquetFile(f"data/out/report/region=north/{data_file}").schema_arrow.names == ["total"]


def test_a_partitioned_delta_static_overwrite_replaces_every_partition():
    partitioned("data/out/report", fmt="delta")
    write_report(REPORT)
    write_report(NORTH)
    assert delta_partitions() == [("north", 70)]


def test_a_partitioned_delta_dynamic_overwrite_replaces_only_the_partitions_written_in_one_commit():
    partitioned("data/out/report", fmt="delta", mode="dynamic")
    write_report(REPORT)
    version = deltalake.DeltaTable("data/out/report").version()
    from deltalake import transaction

    if not hasattr(transaction, "RemoveAction"):
        with pytest.raises(invaract.CheckNotRun, match="RemoveAction"):
            write_report(NORTH)
        assert delta_partitions() == [("north", 10), ("south", 20)]
        return
    write_report(NORTH)
    assert delta_partitions() == [("north", 70), ("south", 20)]
    assert deltalake.DeltaTable("data/out/report").version() == version + 1


def test_a_delta_table_partitioned_otherwise_than_the_write_is_not_checked_and_left_as_it_was():
    existing_delta()
    partitioned("data/out/report", fmt="delta")
    with pytest.raises(invaract.CheckNotRun, match="is not partitioned, but the write is partitioned by region"):
        write_report(NORTH)
    assert delta_partitions() == [("west", 5)]


def test_a_new_iceberg_table_is_partitioned_by_identity_on_the_keys(catalog):
    partitioned(ICEBERG, fmt="iceberg")
    write_report(REPORT)
    table = catalog.load_table("sales.report")
    assert [(table.schema().find_column_name(f.source_id), str(f.transform)) for f in table.spec().fields] == [("region", "identity")]
    assert iceberg_rows(catalog)[0] == ["north", "south"]


@pytest.mark.parametrize("mode, expected", [("dynamic", [("north", 70), ("south", 20)]), ("static", [("north", 70)])])
def test_a_partitioned_iceberg_overwrite_replaces_the_written_partitions_or_all(catalog, mode, expected):
    partitioned(ICEBERG, fmt="iceberg", mode=mode)
    write_report(REPORT)
    write_report(NORTH)
    table = catalog.load_table("sales.report")
    assert sorted((r["region"], r["total"]) for r in table.scan().to_arrow().to_pylist()) == expected


def test_an_iceberg_table_partitioned_otherwise_than_the_write_is_not_checked(catalog):
    existing_iceberg(catalog)
    partitioned(ICEBERG, fmt="iceberg")
    with pytest.raises(invaract.CheckNotRun, match="is not partitioned, but the write is partitioned by region"):
        write_report(NORTH)
    assert iceberg_rows(catalog)[0] == ["west"]
