# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""PostgreSQL outputs and inputs, against a real server: ``INVARACT_TEST_POSTGRES`` names it (CI runs one as a service; with
the variable set, these tests can never pass by being skipped). Each test works in a schema of its own."""
import decimal
import os
import uuid

import pandas as pd
import pyarrow as pa
import pytest

import invaract
from invaract import _postgres
from conftest import write_contract

SERVER = os.environ.get("INVARACT_TEST_POSTGRES")
if SERVER:
    import adbc_driver_postgresql.dbapi as dbapi
else:
    pytest.skip("set INVARACT_TEST_POSTGRES to a PostgreSQL server (postgresql://user@host:port/database)", allow_module_level=True)

REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})
MORE = pd.DataFrame({"region": ["east"], "total": [40]})


def sql(statement):
    """Runs ``statement`` on the test server, committed; returns its rows, if it has any."""
    with dbapi.connect(SERVER) as conn, conn.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.fetchall() if cursor.description else None
        conn.commit()
        return rows


@pytest.fixture
def schema():
    name = f"t_{uuid.uuid4().hex[:10]}"
    sql(f"CREATE SCHEMA {name}")
    yield name
    sql(f"DROP SCHEMA {name} CASCADE")


def location(schema, table="report"):
    return f"{SERVER.rstrip('/')}/{schema}.{table}" if SERVER.count("/") > 2 else f"{SERVER}/postgres/{schema}.{table}"


def contract(where, save_mode="overwrite", fmt="jdbc", extra_output="", inputs=""):
    mode = f"    saveMode: {save_mode}\n" if save_mode else ""
    form = f"    format: {fmt}\n" if fmt else ""
    write_contract(inputs=inputs, outputs=f"  - name: report\n    location: {where}\n{form}{mode}{extra_output}    schema:\n      fields:\n"
                                          "        - { name: region, type: string, required: true }\n        - { name: total, type: long }\n")


def rows(schema, table="report"):
    return sorted(sql(f"SELECT region, total FROM {schema}.{table}"))


def write(*frames):
    with invaract.run("contract.yaml"):
        for frame in frames:
            invaract.write(frame, "report")


def test_a_new_table_is_created_in_one_transaction_with_its_required_columns_not_null(schema):
    contract(location(schema))
    write(REPORT, MORE)
    assert rows(schema) == [("east", 40), ("north", 10), ("south", 20)]
    nullable = dict(sql(f"SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = 'report'"))
    assert nullable == {"region": "NO", "total": "YES"}
    assert sql("SELECT count(*) FROM pg_tables WHERE tablename LIKE 'invaract_%'") == [(0,)], "no staging table is left"


def test_with_no_format_declared_a_postgresql_location_is_written_as_jdbc(schema):
    contract(location(schema), fmt=None)
    write(REPORT)
    assert rows(schema) == [("north", 10), ("south", 20)]


@pytest.mark.parametrize("mode, expected", [
    ("overwrite", [("east", 40)]),
    ("append", [("east", 40), ("west", 5)]),
    ("ignore", [("west", 5)]),
])
def test_save_modes_on_a_table_with_rows(schema, mode, expected):
    sql(f"CREATE TABLE {schema}.report (region text NOT NULL, total bigint, note text DEFAULT 'kept')")
    sql(f"INSERT INTO {schema}.report (region, total) VALUES ('west', 5)")
    contract(location(schema), save_mode=mode)
    write(MORE)
    assert rows(schema) == expected
    # the table itself is kept: its own columns and defaults are not replaced
    assert sql(f"SELECT DISTINCT note FROM {schema}.report") == [("kept",)]


def test_error_refuses_a_table_with_rows_and_writes_nothing(schema):
    sql(f"CREATE TABLE {schema}.report (region text, total bigint)")
    sql(f"INSERT INTO {schema}.report VALUES ('west', 5)")
    contract(location(schema), save_mode="error")
    with pytest.raises(FileExistsError):
        write(MORE)
    assert rows(schema) == [("west", 5)]


def test_a_refused_run_leaves_the_table_as_it_was_and_creates_none(schema):
    sql(f"CREATE TABLE {schema}.report (region text, total bigint)")
    sql(f"INSERT INTO {schema}.report VALUES ('west', 5)")
    contract(location(schema))
    with pytest.raises(invaract.ContractViolation):
        write(pd.DataFrame({"region": [None], "total": [1]}))
    assert rows(schema) == [("west", 5)]
    contract(location(schema, "fresh"))
    with pytest.raises(invaract.ContractViolation):
        write(pd.DataFrame({"region": ["x"], "total": ["not a number"]}))
    assert sql(f"SELECT to_regclass('{schema}.fresh') IS NULL") == [(True,)]


def test_a_job_that_fails_after_writing_rolls_back(schema):
    contract(location(schema))
    with pytest.raises(RuntimeError, match="the job broke"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            raise RuntimeError("the job broke")
    assert sql(f"SELECT to_regclass('{schema}.report') IS NULL") == [(True,)]


def test_an_input_table_is_read_with_its_declared_columns_typed_as_declared(schema):
    sql(f"CREATE TABLE {schema}.orders (id bigint, amount numeric(10,2), secret text)")
    sql(f"INSERT INTO {schema}.orders VALUES (1, 1.50, 'x'), (2, NULL, 'y')")
    inputs = (f"  - name: orders\n    location: {location(schema, 'orders')}\n    format: jdbc\n    schema:\n      fields:\n"
              "        - { name: id, type: long }\n        - { name: amount, type: \"decimal(10,2)\" }\n")
    contract(location(schema), inputs=inputs)
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_="arrow")
        invaract.write(REPORT, "report")
    assert orders.column_names == ["id", "amount"]
    assert orders.schema.field("amount").type == pa.decimal128(10, 2)
    assert orders.to_pylist() == [{"id": 1, "amount": decimal.Decimal("1.50")}, {"id": 2, "amount": None}]


def test_an_input_table_is_read_in_chunks_typed_as_declared(schema):
    sql(f"CREATE TABLE {schema}.orders (id bigint, amount numeric(10,2), secret text)")
    sql(f"INSERT INTO {schema}.orders SELECT g, g * 1.5, 'x' FROM generate_series(1, 5) g")
    inputs = (f"  - name: orders\n    location: {location(schema, 'orders')}\n    format: jdbc\n    schema:\n      fields:\n"
              "        - { name: id, type: long, required: true }\n        - { name: amount, type: \"decimal(10,2)\" }\n")
    contract(location(schema), inputs=inputs)
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_="arrow", chunksize=2))
        invaract.write(REPORT, "report")
    assert [c.num_rows for c in chunks] == [2, 2, 1]
    assert chunks[0].column_names == ["id", "amount"] and chunks[0].schema.field("amount").type == pa.decimal128(10, 2)
    assert sorted(v for c in chunks for v in c.column("id").to_pylist()) == [1, 2, 3, 4, 5]


def test_a_table_is_registered_in_the_jdbc_catalog_under_its_database_schema_and_table(schema):
    database = _postgres.parse(location(schema))[1]
    contract(location(schema), extra_output=f"    catalog: {{ required: true, technology: jdbc, catalogName: {database}, namespace: [{schema}], table: report }}\n")
    write(REPORT)
    assert rows(schema) == [("north", 10), ("south", 20)]
    contract(location(schema), extra_output=f"    catalog: {{ required: true, technology: jdbc, namespace: [elsewhere] }}\n")
    with pytest.raises(invaract.ContractViolation) as refused:
        write(MORE)
    assert refused.value.violations == ["OUTPUT_CATALOG_MISMATCH"]


def test_a_partitioned_postgres_output_is_not_written(schema):
    contract(location(schema))
    with pytest.raises(invaract.CheckNotRun, match="partitioned"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report", partition_by=["region"])
    assert sql(f"SELECT to_regclass('{schema}.report') IS NULL") == [(True,)]


@pytest.mark.parametrize("where, message", [
    ("postgresql://user:hunter2@localhost/db/s.t", "must not hold a password"),
    ("postgresql://localhost/db", "is postgresql://host"),
    ("postgresql://localhost/db/s.t/extra", "is postgresql://host"),
    ('postgresql://localhost/db/s."T x"', "plain identifiers"),
])
def test_a_location_that_is_not_a_plain_table_or_holds_a_password_is_refused(where, message):
    with pytest.raises(invaract.CheckNotRun, match=message) as refused:
        _postgres.parse(where)
    assert "hunter2" not in str(refused.value)


def test_how_a_location_names_its_server_database_schema_and_table():
    assert _postgres.parse("postgresql://app@db.internal:6432/sales/finance.report?sslmode=require") == (
        "postgresql://app@db.internal:6432/sales?sslmode=require", "sales", "finance", "report")
    assert _postgres.parse("postgres://localhost/sales/report") == ("postgresql://localhost/sales", "sales", "public", "report")
    assert _postgres.catalog_of("postgresql://localhost/sales/finance.report") == {
        "technology": "jdbc", "catalogName": "sales", "namespace": ["finance"], "table": "report"}
    assert _postgres.is_postgres("postgresql://h/d/t") and _postgres.is_postgres("POSTGRES://h/d/t")
    assert not _postgres.is_postgres("data/out/report") and not _postgres.is_postgres(None)
