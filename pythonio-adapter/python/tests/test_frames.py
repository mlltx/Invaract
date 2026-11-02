# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Every kind of table a job can hand ``invaract.write``, and every frame ``invaract.read`` can return, through a real run."""
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from invaract import _frames
from conftest import output, write_contract

ROWS = {"region": ["north", "south", "east"], "total": [10, 20, 30]}


def report_contract():
    write_contract(
        inputs="  - name: orders\n    location: data/in/orders\n    format: parquet\n",
        outputs=output(),
    )
    os.makedirs("data/in/orders")
    pq.write_table(pa.table({"region": ["north"], "total": pa.array([1], pa.int64())}), "data/in/orders/part-0.parquet")


def write_and_read_back(frame):
    write_contract(outputs=output())
    with invaract.run("contract.yaml"):
        invaract.write(frame, "report")
    table = pq.read_table("data/out/report")
    assert [(f.name, f.nullable) for f in table.schema] == [("region", False), ("total", False)]
    return table.to_pydict()


def tables():
    """Each kind of table a job may write, by the library that makes it; a library that is not installed is skipped."""
    yield "pandas", lambda: pd.DataFrame(ROWS)
    yield "pyarrow Table", lambda: pa.table({"region": ROWS["region"], "total": pa.array(ROWS["total"], pa.int64())})
    yield "pyarrow RecordBatchReader", lambda: pa.table({"region": ROWS["region"], "total": pa.array(ROWS["total"], pa.int64())}).to_reader(max_chunksize=1)
    yield "polars", lambda: pytest.importorskip("polars").DataFrame(ROWS)
    yield "polars LazyFrame", lambda: pytest.importorskip("polars").LazyFrame(ROWS).filter(pytest.importorskip("polars").col("total") > 0)
    yield "duckdb", lambda: pytest.importorskip("duckdb").sql("select * from (values ('north', 10::bigint), ('south', 20), ('east', 30)) t(region, total)")


@pytest.mark.parametrize("kind,make", list(tables()), ids=[k for k, _ in tables()])
def test_every_kind_of_table_is_written(kind, make):
    assert write_and_read_back(make()) == ROWS


def test_a_polars_lazyframe_is_described_before_it_runs_and_runs_batch_by_batch():
    pl = pytest.importorskip("polars")
    frame = pl.LazyFrame(ROWS)
    schema, batches = _frames.stream(frame)
    assert schema.names == ["region", "total"]
    assert sum(b.num_rows for b in batches) == 3


@pytest.mark.parametrize("as_", ["pandas", "polars", "polars-lazy", "arrow"])
def test_a_read_returns_the_frame_asked_for(as_):
    if as_.startswith("polars"):
        pytest.importorskip("polars")
    report_contract()
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_=as_)
        invaract.write(pd.DataFrame(ROWS), "report")
    kind = {"pandas": "DataFrame", "polars": "DataFrame", "polars-lazy": "LazyFrame", "arrow": "Table"}[as_]
    assert type(orders).__name__ == kind
    assert type(orders).__module__.split(".")[0] == {"pandas": "pandas", "arrow": "pyarrow"}.get(as_, "polars")


def test_a_read_is_pandas_by_default():
    report_contract()
    with invaract.run("contract.yaml"):
        assert isinstance(invaract.read("orders"), pd.DataFrame)
        invaract.write(pd.DataFrame(ROWS), "report")


def test_an_object_with_no_arrow_stream_has_no_description():
    assert _frames.stream(object()) is None
    assert _frames.stream({"region": ["a"]}) is None


def test_the_checker_is_told_each_columns_pyarrow_type_name_flag_and_empty_values():
    schema = pa.schema([pa.field("a", pa.list_(pa.struct([("sku", pa.string())]))), pa.field("b", pa.int64(), nullable=False)])
    assert _frames.describe(schema) == [
        {"name": "a", "type": "list<item: struct<sku: string>>", "nullable": True},
        {"name": "b", "type": "int64", "nullable": False},
    ]
    assert _frames.describe(schema, {"a": 2, "b": 0})[0]["nulls"] == 2


@pytest.mark.parametrize("contract_type,arrow", [
    ("string", pa.string()), ("long", pa.int64()), ("BIGINT", pa.int64()), ("int", pa.int32()), ("short", pa.int16()),
    ("byte", pa.int8()), ("boolean", pa.bool_()), ("float", pa.float32()), ("double", pa.float64()), ("date", pa.date32()),
    ("timestamp", pa.timestamp("us", tz="UTC")), ("timestamp_ntz", pa.timestamp("us")), ("binary", pa.binary()),
    ("decimal(10,2)", pa.decimal128(10, 2)), ("numeric( 38 , 0 )", pa.decimal128(38, 0)),
    ("array<string>", None), ("struct<a:int>", None),
])
def test_a_csv_column_is_read_as_the_contracts_type(contract_type, arrow):
    assert _frames.arrow_type(contract_type) == arrow


def test_a_format_that_cannot_be_read_is_an_error():
    with pytest.raises(ValueError, match="parquet and csv"):
        _frames.read_table("anywhere", "delta", [])


def test_a_pandas_frames_row_index_is_not_written():
    frame = pd.DataFrame(ROWS)
    filtered = frame[frame["total"] > 10]
    assert write_and_read_back(filtered) == {"region": ["south", "east"], "total": [20, 30]}
