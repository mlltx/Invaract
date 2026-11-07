# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""pandas frames: written as the contract declares where no value changes (``_pandas``), named index levels the contract
declares written as columns, notes in pandas' terms on a refusal, and reads in the dtypes the job asks for. Other frame
libraries are written as they were."""
import datetime
import decimal

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
