# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""SQLite outputs and inputs through ADBC: staged in a file of their own, moved into place in one transaction at publish, and
read back as the contract types each column."""
import datetime
import decimal
import os
import sqlite3

import pandas as pd
import pyarrow as pa
import pytest

import invaract
from invaract import _sqlite
from conftest import write_contract

pytest.importorskip("adbc_driver_sqlite", reason='the SQLite tests need adbc-driver-sqlite (pip install "invaract[sqlite]")')

DB = "data/shop.db"
WHERE = f"sqlite:///{DB}/report"
REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})
MORE = pd.DataFrame({"region": ["east"], "total": [40]})


def sql(statement, *args):
    conn = sqlite3.connect(DB)
    try:
        rows = conn.execute(statement, args).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


def contract(where=WHERE, save_mode="overwrite", fmt="jdbc", extra_output="", inputs="", fields=None):
    mode = f"    saveMode: {save_mode}\n" if save_mode else ""
    form = f"    format: {fmt}\n" if fmt else ""
    fields = fields or "        - { name: region, type: string, required: true }\n        - { name: total, type: long }\n"
    write_contract(inputs=inputs, outputs=f"  - name: report\n    location: {where}\n{form}{mode}{extra_output}    schema:\n      fields:\n{fields}")


def rows(table="report"):
    return sorted(sql(f"SELECT region, total FROM {table}"))


def write(*frames):
    with invaract.run("contract.yaml"):
        for frame in frames:
            invaract.write(frame, "report")


def test_a_new_table_is_created_in_one_transaction_with_its_required_columns_not_null():
    contract()
    write(REPORT, MORE)
    assert rows() == [("east", 40), ("north", 10), ("south", 20)]
    assert sql("SELECT name, type, \"notnull\" FROM pragma_table_info('report')") == [("region", "TEXT", 1), ("total", "INTEGER", 0)]
    assert sql("SELECT name FROM sqlite_master") == [("report",)], "nothing of the staging is left in the database"


def test_with_no_format_declared_a_sqlite_location_is_written_as_jdbc():
    contract(fmt=None)
    write(REPORT)
    assert rows() == [("north", 10), ("south", 20)]


@pytest.mark.parametrize("mode, expected", [
    ("overwrite", [("east", 40)]),
    ("append", [("east", 40), ("west", 5)]),
    ("ignore", [("west", 5)]),
])
def test_save_modes_on_a_table_with_rows(mode, expected):
    os.makedirs("data")
    sql("CREATE TABLE report (region TEXT NOT NULL, total INTEGER, note TEXT DEFAULT 'kept')")
    sql("INSERT INTO report (region, total) VALUES ('west', 5)")
    contract(save_mode=mode)
    write(MORE)
    assert rows() == expected
    # the table itself is kept: its own columns and defaults are not replaced
    assert sql("SELECT DISTINCT note FROM report") == [("kept",)]


def test_error_refuses_a_table_with_rows_and_writes_nothing():
    os.makedirs("data")
    sql("CREATE TABLE report (region TEXT, total INTEGER)")
    sql("INSERT INTO report VALUES ('west', 5)")
    contract(save_mode="error")
    with pytest.raises(FileExistsError):
        write(MORE)
    assert rows() == [("west", 5)]


def test_error_and_ignore_write_to_an_empty_table_or_a_database_without_it():
    os.makedirs("data")
    sql("CREATE TABLE report (region TEXT, total INTEGER)")
    contract(save_mode="error")
    write(MORE)
    assert rows() == [("east", 40)]
    sql("DROP TABLE report")
    contract(save_mode="ignore")
    write(MORE)
    assert rows() == [("east", 40)]


def test_a_refused_run_leaves_the_table_as_it_was_and_creates_no_database():
    contract()
    with pytest.raises(invaract.ContractViolation):
        write(pd.DataFrame({"region": [None], "total": [1]}))
    assert not os.path.exists(DB)
    os.makedirs("data")
    sql("CREATE TABLE report (region TEXT, total INTEGER)")
    sql("INSERT INTO report VALUES ('west', 5)")
    with pytest.raises(invaract.ContractViolation):
        write(pd.DataFrame({"region": [None], "total": [1]}))
    assert rows() == [("west", 5)]


def test_a_job_that_fails_after_writing_publishes_nothing():
    contract()
    with pytest.raises(RuntimeError, match="the job broke"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            raise RuntimeError("the job broke")
    assert not os.path.exists(DB)


def test_a_write_of_no_rows_publishes_the_tables_columns():
    contract()
    write(REPORT.iloc[0:0])
    assert sql("SELECT name FROM pragma_table_info('report')") == [("region",), ("total",)] and rows() == []


TYPED = ("        - { name: id, type: long, required: true }\n        - { name: price, type: \"decimal(10,2)\" }\n"
         "        - { name: day, type: date }\n        - { name: at, type: timestamp }\n        - { name: local, type: timestamp_ntz }\n"
         "        - { name: active, type: boolean }\n        - { name: score, type: double }\n        - { name: blob, type: binary }\n")


def test_every_type_reads_back_as_declared():
    table = pa.table({
        "id": pa.array([1, 2], pa.int64()),
        "price": pa.array([decimal.Decimal("12.30"), None], pa.decimal128(10, 2)),
        "day": pa.array([datetime.date(2026, 11, 6), None]),
        "at": pa.array([datetime.datetime(2026, 11, 6, 10, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=1))), None],
                       pa.timestamp("us", tz="Europe/Berlin")).cast(pa.timestamp("us", tz="UTC")),
        "local": pa.array([datetime.datetime(2026, 11, 6, 9, 0), None], pa.timestamp("us")),
        "active": [True, None],
        "score": [0.5, None],
        "blob": pa.array([b"\x00\x01", None], pa.binary()),
    })
    contract(fields=TYPED)
    with invaract.run("contract.yaml"):
        invaract.write(table, "report")
    assert sql("SELECT price, day, at, local, active FROM report WHERE id = 1") == [
        ("12.30", "2026-11-06", "2026-11-06T09:30:00.000000Z", "2026-11-06T09:00:00.000000", 1)]
    contract(where="data/out/unused", fmt="parquet", inputs=f"  - name: back\n    location: {WHERE}\n    format: jdbc\n    schema:\n      fields:\n{TYPED}")
    with invaract.run("contract.yaml"):
        back = invaract.read("back", as_="arrow")
        invaract.write(REPORT, "report")
    assert back.to_pylist() == table.to_pylist()


def test_an_input_written_by_another_program_is_read_with_its_declared_columns_typed_as_declared():
    os.makedirs("data")
    sql("CREATE TABLE orders (id INTEGER, amount REAL, at TEXT, paid INTEGER, secret TEXT)")
    sql("INSERT INTO orders VALUES (1, 1.1, '2026-11-06 09:30:00', 1, 'x'), (2, NULL, NULL, 0, 'y')")
    inputs = (f"  - name: orders\n    location: sqlite:///{DB}/orders\n    format: jdbc\n    schema:\n      fields:\n"
              "        - { name: id, type: long }\n        - { name: amount, type: \"decimal(10,2)\" }\n"
              "        - { name: at, type: timestamp }\n        - { name: paid, type: boolean }\n")
    contract(inputs=inputs)
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_="arrow")
        invaract.write(REPORT, "report")
    assert orders.column_names == ["id", "amount", "at", "paid"]
    assert orders.to_pylist() == [
        {"id": 1, "amount": decimal.Decimal("1.10"), "at": datetime.datetime(2026, 11, 6, 9, 30, tzinfo=datetime.timezone.utc), "paid": True},
        {"id": 2, "amount": None, "at": None, "paid": False}]


def test_an_input_is_read_in_chunks_typed_as_declared():
    os.makedirs("data")
    sql("CREATE TABLE orders (id INTEGER, amount TEXT)")
    for i in range(1, 6):
        sql("INSERT INTO orders VALUES (?, ?)", i, f"{i}.50")
    inputs = (f"  - name: orders\n    location: sqlite:///{DB}/orders\n    format: jdbc\n    schema:\n      fields:\n"
              "        - { name: id, type: long, required: true }\n        - { name: amount, type: \"decimal(10,2)\" }\n")
    contract(inputs=inputs)
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_="arrow", chunksize=2))
        invaract.write(REPORT, "report")
    assert [c.num_rows for c in chunks] == [2, 2, 1]
    assert chunks[0].schema.field("amount").type == pa.decimal128(10, 2)
    assert [v for c in chunks for v in c.column("amount").to_pylist()][:2] == [decimal.Decimal("1.50"), decimal.Decimal("2.50")]


def test_a_missing_database_or_table_is_not_read_and_not_created():
    inputs = f"  - name: orders\n    location: sqlite:///{DB}/orders\n    format: jdbc\n"
    contract(inputs=inputs)
    with pytest.raises(FileNotFoundError, match="no SQLite database"):
        with invaract.run("contract.yaml"):
            invaract.read("orders")
    assert not os.path.exists(DB)
    os.makedirs("data")
    sql("CREATE TABLE other (x INTEGER)")
    with pytest.raises(FileNotFoundError, match="no SQLite table orders"):
        with invaract.run("contract.yaml"):
            invaract.read("orders")


def test_a_table_is_registered_in_the_jdbc_catalog_under_its_file_main_and_table():
    contract(extra_output="    catalog: { required: true, technology: jdbc, catalogName: shop, namespace: [main], table: report }\n")
    write(REPORT)
    assert rows() == [("north", 10), ("south", 20)]
    contract(extra_output="    catalog: { required: true, technology: jdbc, catalogName: elsewhere }\n")
    with pytest.raises(invaract.ContractViolation) as refused:
        write(MORE)
    assert refused.value.violations == ["OUTPUT_CATALOG_MISMATCH"]


def test_a_partitioned_sqlite_output_is_not_written():
    contract()
    with pytest.raises(invaract.CheckNotRun, match="partitioned"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report", partition_by=["region"])
    assert not os.path.exists(DB)


def test_one_output_written_twice_with_other_columns_is_not_written():
    contract()
    with pytest.raises(invaract.CheckNotRun, match="has the same columns"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT[["total", "region"]], "report")
    assert not os.path.exists(DB)


@pytest.mark.parametrize("where, message", [
    ("sqlite://data/shop.db/report", "is sqlite:///<path"),
    ("sqlite:///report", "is sqlite:///<path"),
    ("sqlite:///data/shop.db/", "is sqlite:///<path"),
    ("sqlite:///data/shop.db/my table", "plain identifiers"),
    ("data/shop.db/report", "is sqlite:///<path"),
])
def test_a_location_that_does_not_name_a_file_and_a_plain_table_is_refused(where, message):
    with pytest.raises(invaract.CheckNotRun, match=message):
        _sqlite.parse(where)


def test_how_a_location_names_its_file_and_table():
    assert _sqlite.parse("sqlite:///data/shop.db/report") == ("data/shop.db", "report")
    assert _sqlite.parse("sqlite:////var/lib/shop.sqlite/orders") == ("/var/lib/shop.sqlite", "orders")
    assert _sqlite.catalog_of("SQLITE:///data/shop.db/report") == {"technology": "jdbc", "catalogName": "shop", "namespace": ["main"], "table": "report"}
    assert _sqlite.is_sqlite("sqlite:///a/b") and not _sqlite.is_sqlite("data/a.db") and not _sqlite.is_sqlite(None)
    assert [_sqlite._affinity(t) for t in (pa.int8(), pa.bool_(), pa.float32(), pa.binary(), pa.string(), pa.date32())] == \
        ["INTEGER", "INTEGER", "REAL", "BLOB", "TEXT", "TEXT"]
