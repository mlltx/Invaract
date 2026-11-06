# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""pandas frames: written as the contract declares where no value changes (``_pandas``), named index levels the contract
declares written as columns, notes in pandas' terms on a refusal, and reads in the dtypes the job asks for. Other frame
libraries are written as they were."""
import datetime
import decimal
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from invaract import _pandas
from conftest import output, write_contract


def chunked(values, type=None):
    return pa.chunked_array([pa.array(values, type=type, from_pandas=True)])


def converted(values, target, type=None):
    """The column ``values`` as ``target``, asserting it was converted."""
    column, note = _pandas.conform(chunked(values, type), target)
    assert note is None, note
    assert column.type == target
    return column.to_pylist()


def refused(values, target, type=None):
    """Why ``values`` is not converted to ``target``, asserting it is left as it was."""
    original = chunked(values, type)
    column, note = _pandas.conform(original, target)
    assert column is original
    assert note
    return note


# numbers


def test_a_whole_number_column_with_a_missing_value_becomes_an_integer():
    assert converted([1.0, np.nan, 3.0], pa.int64()) == [1, None, 3]


def test_a_fraction_or_a_number_out_of_range_is_not_an_integer():
    assert "1.5 is not a whole number" in refused([1.0, 1.5], pa.int64())
    assert "1e+20" in refused([1e20], pa.int64())


def test_an_integer_becomes_a_narrower_integer_only_when_every_value_fits():
    assert converted([1, -32768, 32767], pa.int16(), pa.int64()) == [1, -32768, 32767]
    assert "32768 does not fit int16" in refused([1, 32768], pa.int16(), pa.int64())
    assert "-32769 does not fit" in refused([-32769], pa.int16(), pa.int64())


def test_an_integer_becomes_a_double_only_when_it_is_exact():
    assert converted([5, 2**53], pa.float64(), pa.int64()) == [5.0, float(2**53)]
    assert str(2**53 + 1) in refused([2**53 + 1], pa.float64(), pa.int64())


def test_a_double_becomes_a_float_only_when_it_is_exact():
    assert converted([0.5, 0.25], pa.float32()) == [0.5, 0.25]
    assert "1.1 does not fit float" in refused([1.1], pa.float32())
    assert converted([1.5], pa.float64(), pa.float32()) == [1.5]


def test_decimals_take_the_declared_precision_and_scale_when_every_value_fits():
    assert converted([decimal.Decimal("1.50"), None], pa.decimal128(10, 2)) == [decimal.Decimal("1.50"), None]
    assert "round(2)" in refused([decimal.Decimal("1.2345")], pa.decimal128(10, 2))
    assert "123456789012.5 does not fit decimal128(10, 2)" in refused([decimal.Decimal("123456789012.5")], pa.decimal128(10, 2))


def test_a_float_becomes_a_decimal_when_it_is_the_nearest_float_to_one():
    assert converted([19.99, 2.67, -0.05], pa.decimal128(10, 2)) == [decimal.Decimal(v) for v in ("19.99", "2.67", "-0.05")]
    assert "0.30000000000000004" in refused([0.1 + 0.2], pa.decimal128(10, 2))


def test_an_integer_becomes_a_decimal_when_it_fits():
    assert converted([5, None], pa.decimal128(10, 2), pa.int64()) == [decimal.Decimal("5.00"), None]
    assert "1000000000 does not fit" in refused([10**9], pa.decimal128(10, 2), pa.int64())


# times


def test_a_datetime_at_midnight_becomes_a_date_and_one_with_a_time_of_day_does_not():
    midnight = pd.Series(pd.to_datetime(["2026-01-01", None]))
    assert converted(midnight, pa.date32()) == [datetime.date(2026, 1, 1), None]
    note = refused(pd.Series([pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02 10:30")]), pa.date32())
    assert "10:30" in note and "dt.normalize()" in note


def test_a_time_zone_is_never_added_or_dropped():
    naive = pd.Series(pd.to_datetime(["2026-01-01 10:30"]))
    assert "tz_localize('UTC')" in refused(naive, pa.timestamp("us", tz="UTC"))
    aware = naive.dt.tz_localize("Europe/London")
    assert "tz_convert(None)" in refused(aware, pa.timestamp("us"))


def test_a_datetime_in_another_zone_is_the_same_moment_in_utc():
    london = pd.Series(pd.to_datetime(["2026-06-01 12:00"]).tz_localize("Europe/London"))
    [moment] = converted(london, pa.timestamp("us", tz="UTC"))
    assert moment == datetime.datetime(2026, 6, 1, 11, 0, tzinfo=datetime.timezone.utc)


def test_a_timestamp_of_another_unit_in_the_same_zone_already_matches():
    utc = chunked(pd.Series(pd.to_datetime(["2026-01-01 00:00:00.000000001"], utc=True)))
    column, note = _pandas.conform(utc, pa.timestamp("us", tz="UTC"))
    assert column is utc and note is None


# text, kinds, empty columns


def test_text_of_either_width_and_a_category_of_text_already_match():
    for column in (chunked(["a"], pa.large_string()), chunked(pd.Series(pd.Categorical(["a", "b"])))):
        assert _pandas.conform(column, pa.string()) == (column, None)


def test_a_category_of_numbers_is_written_as_its_values():
    assert converted(pd.Series(pd.Categorical([1, 2, 1])), pa.int32()) == [1, 2, 1]


def test_values_of_another_kind_are_never_converted():
    assert "astype('string')" in refused([1, 2], pa.string(), pa.int64())
    assert "pd.to_numeric" in refused(["1"], pa.int64())
    assert "pd.to_numeric" in refused([True], pa.int64())
    assert "pd.to_datetime" in refused(["2026-01-01"], pa.date32())
    assert "astype('boolean')" in refused([1], pa.bool_(), pa.int64())


def test_a_column_with_no_values_takes_the_declared_type():
    assert converted([None, None], pa.string()) == [None, None]
    assert converted([np.nan], pa.int64()) == [None]
    assert converted([], pa.decimal128(10, 2), pa.float64()) == []


# frames


FIELDS = [{"name": "region", "type": "string"}, {"name": "total", "type": "long"}]


def test_a_frame_is_converted_only_where_the_contract_declares_a_type():
    frame = pd.DataFrame({"region": ["a", "b"], "total": [1.0, np.nan], "extra": [1.5, np.nan]})
    table, notes = _pandas.to_arrow(frame, FIELDS + [{"name": "nested", "type": "array<int>"}])
    assert notes == []
    assert table.schema.field("total").type == pa.int64() and table.column("total").to_pylist() == [1, None]
    assert table.schema.field("extra").type == pa.float64()
    assert table.column_names == ["region", "total", "extra"]


def test_with_no_declared_fields_a_frame_is_written_as_pandas_holds_it():
    table, notes = _pandas.to_arrow(pd.DataFrame({"total": [1.0, np.nan]}))
    assert notes == [] and table.schema.field("total").type == pa.float64()


def test_a_note_names_the_column_its_pandas_dtype_and_the_fix():
    _, notes = _pandas.to_arrow(pd.DataFrame({"region": ["a"], "total": [1.5]}), FIELDS)
    assert notes == ["'total' (float64 in pandas) is not written as long: 1.5 is not a whole number, or is out of range (a "
                     "whole-number column with missing values is float64 in pandas, and is written as an integer as it is); "
                     "round it in the job if that is intended"]
    _, notes = _pandas.to_arrow(pd.DataFrame({"region": [1]}), FIELDS)
    assert notes == ["'region' (int64 in pandas) is not written as string: it does not hold text; convert it in the job, "
                     "for example df['region'].astype('string')"]


def test_named_index_levels_the_contract_declares_are_written_as_columns():
    grouped = pd.DataFrame({"region": ["a", "a", "b"], "total": [1, 2, 3]}).groupby("region").sum()
    table, _ = _pandas.to_arrow(grouped, FIELDS)
    assert table.column_names == ["region", "total"] and table.column("region").to_pylist() == ["a", "b"]
    two = pd.DataFrame({"region": ["a", "b"], "day": [1, 2], "total": [1, 2]}).set_index(["region", "day"])
    table, _ = _pandas.to_arrow(two, FIELDS)
    assert table.column_names == ["region", "total"]


def test_an_unnamed_or_undeclared_index_or_one_the_frame_also_holds_as_a_column_is_not_written():
    frame = pd.DataFrame({"region": ["a", "b"], "total": [1, 2]})
    assert _pandas.to_arrow(frame[frame.total > 1], FIELDS)[0].column_names == ["region", "total"]
    assert _pandas.to_arrow(frame.set_index("total"), [FIELDS[0]])[0].column_names == ["region"]
    both = frame.set_index(pd.Index(["x", "y"], name="region"))
    assert _pandas.to_arrow(both, FIELDS)[0].column("region").to_pylist() == ["a", "b"]


def test_a_column_of_mixed_values_is_named_rather_than_left_to_arrow():
    with pytest.raises(TypeError, match=r"column 'region' holds values of more than one kind \(int, str\)"):
        _pandas.to_arrow(pd.DataFrame({"total": [1, 2], "region": ["a", 1]}), FIELDS)


# runs


def contract(fields=(("region", "string", True), ("total", "long", False)), inputs=""):
    write_contract(inputs=inputs, outputs=output(fields=list(fields)))


def written(location="data/out/report"):
    return pq.read_table(location)


def test_an_integer_column_with_a_missing_value_is_written_as_the_declared_integer():
    contract()
    with invaract.run("contract.yaml"):
        invaract.write(pd.DataFrame({"region": ["a", "b"], "total": [1, None]}), "report")
    table = written()
    assert table.schema.field("total").type == pa.int64()
    assert table.column("total").to_pylist() == [1, None]


def test_a_groupby_result_is_written_with_its_keys():
    contract()
    sales = pd.DataFrame({"region": ["north", "north", "south"], "total": [1, 2, 3]})
    with invaract.run("contract.yaml"):
        invaract.write(sales.groupby("region").sum(), "report")
    assert written().to_pydict() == {"region": ["north", "south"], "total": [3, 3]}


def test_a_refusal_says_in_pandas_terms_why_a_column_was_not_written_as_declared():
    contract()
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.write(pd.DataFrame({"region": ["a"], "total": [1.5]}), "report")
    assert refusal.value.violations == ["OUTPUT_FIELD_TYPE_MISMATCH"]
    assert "\n\nIn pandas:\n  - 'total' (float64 in pandas) is not written as long: 1.5 is not a whole number" in str(refusal.value)
    assert str(refusal.value) == refusal.value.explanation


def test_a_refusal_with_nothing_to_say_about_pandas_has_no_pandas_notes():
    contract()
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.write(pd.DataFrame({"total": [1]}), "report")
    assert refusal.value.violations == ["MISSING_OUTPUT_FIELD"]
    assert "In pandas" not in refusal.value.explanation


def test_an_arrow_table_is_written_as_it_is_and_refused_as_before():
    contract()
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"region": ["a"], "total": [1.0]}), "report")
    assert refusal.value.violations == ["OUTPUT_FIELD_TYPE_MISMATCH"]
    assert "In pandas" not in refusal.value.explanation


def test_a_polars_frame_is_written_as_it_is_and_refused_as_before():
    pl = pytest.importorskip("polars")
    contract()
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.write(pl.DataFrame({"region": ["a"], "total": [1.0]}), "report")
    assert refusal.value.violations == ["OUTPUT_FIELD_TYPE_MISMATCH"]


ORDERS = ("  - name: orders\n    type: DATA_ASSET\n    location: data/in/orders.parquet\n    format: parquet\n    schema:\n"
          "      fields:\n        - { name: region, type: string }\n        - { name: total, type: long }\n")


@pytest.fixture
def orders(environment):
    (environment / "data" / "in").mkdir(parents=True)
    pq.write_table(pa.table({"region": ["a", "b"], "total": pa.array([1, None], pa.int64())}), "data/in/orders.parquet")
    contract(inputs=ORDERS)


def test_a_frame_read_with_pandas_default_dtypes_is_written_back_as_declared(orders):
    with invaract.run("contract.yaml"):
        frame = invaract.read("orders")
        assert str(frame["total"].dtype) == "float64"
        invaract.write(frame, "report")
    assert written().column("total").to_pylist() == [1, None]
    assert written().schema.field("total").type == pa.int64()


@pytest.mark.parametrize("backend, dtype", [("numpy_nullable", "Int64"), ("pyarrow", "int64[pyarrow]")])
def test_a_read_takes_pandas_dtype_backend(orders, backend, dtype):
    with invaract.run("contract.yaml"):
        frame = invaract.read("orders", dtype_backend=backend)
        invaract.write(frame, "report")
    assert str(frame["total"].dtype) == dtype
    assert frame["total"].isna().tolist() == [False, True]
    assert written().column("total").to_pylist() == [1, None]


def test_reads_passes_dtype_backend_on(orders):
    @invaract.reads("orders", dtype_backend="numpy_nullable")
    def job(orders):
        return str(orders["total"].dtype)

    with invaract.run("contract.yaml"):
        assert job() == "Int64"
        invaract.write(pd.DataFrame({"region": ["a"], "total": [1]}), "report")


@pytest.mark.parametrize("backend, as_, message", [
    ("numpy", "pandas", "cannot take dtype_backend='numpy'"),
    ("pyarrow", "arrow", "dtype_backend is for a pandas read"),
])
def test_an_unknown_dtype_backend_or_one_for_another_library_is_refused_before_reading(orders, backend, as_, message):
    with pytest.raises(ValueError, match=message):
        with invaract.run("contract.yaml"):
            invaract.read("orders", as_=as_, dtype_backend=backend)


# nested columns, sparse columns, column names, and what pandas cannot say


def test_a_list_of_whole_numbers_held_as_floats_beside_a_missing_one_is_written_as_the_declared_list():
    table, notes = _pandas.to_arrow(pd.DataFrame({"x": [[1.0, np.nan], [3.0]]}), [{"name": "x", "type": "array<long>"}])
    assert notes == [] and table.schema.field("x").type == pa.list_(pa.int64())
    assert table.column("x").to_pylist() == [[1, None], [3]]


def test_a_list_with_a_fraction_is_not_written_as_a_list_of_integers():
    table, notes = _pandas.to_arrow(pd.DataFrame({"x": [[1.5]]}), [{"name": "x", "type": "array<long>"}])
    assert table.schema.field("x").type == pa.list_(pa.float64())
    assert notes == ["'x' (object in pandas) is not written as array<long>: its values are not all list<item: int64>; convert them in the job"]


def test_an_empty_list_column_takes_the_declared_type():
    table, _ = _pandas.to_arrow(pd.DataFrame({"x": pd.Series([], dtype=object)}), [{"name": "x", "type": "array<long>"}])
    assert table.schema.field("x").type == pa.list_(pa.int64())


@pytest.mark.parametrize("declared, arrow", [
    ("long", pa.int64()),
    ("decimal(10, 2)", pa.decimal128(10, 2)),
    ("array<string>", pa.list_(pa.string())),
    (" ARRAY< array<int> > ", pa.list_(pa.list_(pa.int32()))),
    ("map<string,double>", pa.map_(pa.string(), pa.float64())),
    ("struct<a:long, `b c`:string>", pa.struct([("a", pa.int64()), ("b c", pa.string())])),
    ("struct<a long>", pa.struct([("a", pa.int64())])),
    ("interval", None), ("array<interval>", None), ("array<long", None), ("array<long>>", None), ("struct<>", None),
    ("map<string>", None), ("", None),
])
def test_which_contract_types_a_pandas_column_can_be_written_as(declared, arrow):
    assert _pandas.arrow_target(declared) == arrow


def test_a_sparse_column_is_written_with_its_values():
    table, _ = _pandas.to_arrow(pd.DataFrame({"x": pd.arrays.SparseArray([0.0, 1.5, 0.0])}), [{"name": "x", "type": "double"}])
    assert table.schema.field("x").type == pa.float64() and table.column("x").to_pylist() == [0.0, 1.5, 0.0]


def test_columns_that_cannot_be_the_columns_of_one_table_are_named():
    with pytest.raises(TypeError, match=r"more than one column named 'x'"):
        _pandas.to_arrow(pd.DataFrame([[1, 2, 3]], columns=["x", "x", "y"]))
    with pytest.raises(TypeError, match=r"more than one level .*'_'\.join"):
        _pandas.to_arrow(pd.DataFrame([[1]], columns=pd.MultiIndex.from_tuples([("x", "a")])))


def test_a_required_column_with_missing_values_says_how_pandas_holds_them():
    _, notes = _pandas.to_arrow(pd.DataFrame({"region": ["a", None], "total": [1.0, np.nan]}),
                                [{"name": "region", "type": "string", "required": True}, {"name": "total", "type": "long"}])
    assert notes == ["'region' holds 1 missing value(s) (NaN, None, NaT or pd.NA in pandas), and the contract requires a value in "
                     "every row: fill them (df['region'].fillna(...)) or drop those rows (df.dropna(subset=['region']))"]


def test_periods_and_timedeltas_are_explained_rather_than_converted():
    frame = pd.DataFrame({"month": pd.period_range("2026-01", periods=2, freq="M"), "took": pd.to_timedelta(["1h", "2h"])})
    _, notes = _pandas.to_arrow(frame, [{"name": "month", "type": "date"}, {"name": "took", "type": "interval"}])
    assert len(notes) == 2
    assert "a pandas period is a span of time" in notes[0] and "df['month'].dt.to_timestamp()" in notes[0]
    assert "a pandas timedelta is a fixed length of time" in notes[1] and "df['took'].dt.total_seconds()" in notes[1]


# dry runs: written as pandas' own to_parquet writes them


def dry_run(monkeypatch):
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")


def test_a_dry_run_writes_every_named_index_level_as_pandas_to_parquet_does(monkeypatch):
    dry_run(monkeypatch)
    sales = pd.DataFrame({"region": ["n", "n", "s"], "day": [1, 1, 2], "total": [1, 2, 3]})
    with invaract.run():
        invaract.write(sales.groupby(["region", "day"]).sum(), "by_region", location="data/out/by_region")
        invaract.write(sales[sales.total > 1], "filtered", location="data/out/filtered")
    assert written("data/out/by_region").column_names == ["region", "day", "total"]
    assert written("data/out/filtered").column_names == ["region", "day", "total"]


def test_a_dry_run_gives_a_column_of_decimals_the_widest_precision_at_its_scale(monkeypatch):
    dry_run(monkeypatch)
    with invaract.run():
        invaract.write(pd.DataFrame({"amount": [decimal.Decimal("1.50"), None]}), "out", location="data/out/amounts")
    assert written("data/out/amounts").schema.field("amount").type == pa.decimal128(38, 2)


def test_under_a_contract_decimals_are_the_declared_precision_and_an_undeclared_named_index_is_not_written():
    write_contract(outputs=output(fields=[("region", "string", True), ("amount", '"decimal(10,2)"', True)]))
    frame = pd.DataFrame({"region": ["a"], "amount": [decimal.Decimal("1.50")]}).set_index(pd.Index(["x"], name="key"))
    with invaract.run("contract.yaml"):
        invaract.write(frame, "report")
    assert written().schema.field("amount").type == pa.decimal128(10, 2)
    assert written().column_names == ["region", "amount"]


# invaract.check


def test_a_check_that_passes_writes_nothing():
    contract()
    assert invaract.check(pd.DataFrame({"region": ["a"], "total": [1.0]}), "report", contract="contract.yaml") is None
    assert not os.path.exists("data/out")


def test_a_check_that_fails_raises_what_the_write_would_with_its_pandas_notes():
    contract()
    with pytest.raises(invaract.ContractViolation) as refusal:
        invaract.check(pd.DataFrame({"region": ["a", None], "total": [1, 2]}), "report", contract="contract.yaml")
    assert refusal.value.violations == ["OUTPUT_FIELD_NULLABILITY_MISMATCH"]
    assert "In pandas:\n  - 'region' holds 1 missing value(s)" in refusal.value.explanation
    assert not os.path.exists("data/out")


def test_a_check_judges_the_frame_alone_whatever_the_job_has_read(orders):
    # the contract declares the input orders: a run that never read it would be refused, a check of the output is not
    invaract.check(pd.DataFrame({"region": ["a"], "total": [1]}), "report", contract="contract.yaml")


def test_a_check_inside_a_run_uses_the_runs_contract_and_leaves_the_run_alone(orders):
    with invaract.run("contract.yaml"):
        invaract.read("orders")
        with pytest.raises(invaract.ContractViolation):
            invaract.check(pd.DataFrame({"region": [None], "total": [1]}), "report")
        invaract.check(pd.DataFrame({"region": ["a"], "total": [1]}), "report")
        invaract.write(pd.DataFrame({"region": ["b"], "total": [2]}), "report")
    assert written().to_pydict() == {"region": ["b"], "total": [2]}


def test_a_check_needs_a_contract_and_ignores_a_dry_run_setting(monkeypatch):
    with pytest.raises(invaract.CheckNotRun, match="contract"):
        invaract.check(pd.DataFrame({"x": [1]}), "report")
    contract()
    dry_run(monkeypatch)
    with pytest.raises(invaract.ContractViolation):
        invaract.check(pd.DataFrame({"region": ["a"], "total": ["text"]}), "report", contract="contract.yaml")


def test_a_check_publishes_no_events(monkeypatch):
    from conftest import notify_to_file

    contract()
    notify_to_file(monkeypatch)
    with pytest.raises(invaract.ContractViolation):
        invaract.check(pd.DataFrame({"region": [None], "total": [1]}), "report", contract="contract.yaml")
    assert not os.path.exists("events.jsonl") or open("events.jsonl").read().strip() == ""


# reading in chunks


def test_an_input_is_read_in_chunks_of_the_given_size(orders):
    pq.write_table(pa.table({"region": [f"r{i}" for i in range(10)], "total": pa.array(range(10), pa.int64())}),
                   "data/in/orders.parquet")
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", chunksize=4))
        invaract.write(pd.concat(chunks), "report")
    assert [len(c) for c in chunks] == [4, 4, 2]
    assert all(isinstance(c, pd.DataFrame) for c in chunks)
    assert written().column("total").to_pylist() == list(range(10))


@pytest.mark.parametrize("as_, kind", [("arrow", pa.Table), ("pandas", pd.DataFrame)])
def test_chunks_come_as_the_library_asked_for(orders, as_, kind):
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_=as_, chunksize=1, **({"dtype_backend": "numpy_nullable"} if as_ == "pandas" else {})))
        invaract.write(pd.DataFrame({"region": ["a"], "total": [1]}), "report")
    assert [type(c) for c in chunks] == [kind, kind]
    if as_ == "pandas":
        assert [str(c["total"].dtype) for c in chunks] == ["Int64", "Int64"]


@pytest.mark.parametrize("size", [0, -1, True, 2.5, "10"])
def test_a_chunksize_that_is_not_a_number_of_rows_is_refused_before_reading(orders, size):
    with pytest.raises(ValueError, match="chunksize"):
        with invaract.run("contract.yaml"):
            invaract.read("orders", chunksize=size)


def test_a_required_input_column_with_an_empty_value_in_a_later_chunk_is_refused_before_any_chunk(environment):
    (environment / "data" / "in").mkdir(parents=True)
    pq.write_table(pa.table({"region": ["a", "b", None]}), "data/in/orders.parquet")
    inputs = ("  - name: orders\n    location: data/in/orders.parquet\n    format: parquet\n    schema:\n      fields:\n"
              "        - { name: region, type: string, required: true }\n")
    contract(inputs=inputs)
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.read("orders", chunksize=1)
    assert "INPUT_FIELD_NULLABILITY_MISMATCH" in refusal.value.violations


def test_an_empty_input_read_in_chunks_gives_no_chunks(environment):
    (environment / "data" / "in").mkdir(parents=True)
    pq.write_table(pa.table({"region": pa.array([], pa.string()), "total": pa.array([], pa.int64())}), "data/in/orders.parquet")
    contract(inputs=ORDERS)
    with invaract.run("contract.yaml"):
        assert list(invaract.read("orders", chunksize=5)) == []
        invaract.write(pd.DataFrame({"region": ["a"], "total": [1]}), "report")


def test_a_csv_input_read_in_chunks_is_typed_as_declared(environment):
    (environment / "data" / "in").mkdir(parents=True)
    with open("data/in/orders.csv", "w") as f:
        f.write("region,total\n" + "".join(f"r{i},{i}\n" for i in range(5)))
    contract(inputs=ORDERS.replace("orders.parquet", "orders.csv").replace("format: parquet", "format: csv"))
    with invaract.run("contract.yaml"):
        chunks = list(invaract.read("orders", as_="arrow", chunksize=2))
        invaract.write(pd.DataFrame({"region": ["a"], "total": [1]}), "report")
    assert [c.num_rows for c in chunks] == [2, 2, 1]
    assert chunks[0].schema.field("total").type == pa.int64()
