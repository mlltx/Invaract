# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""runtimeDataQuality: each constraint's meaning on real Arrow data, and a run that holds the rows it writes to them."""
import decimal
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from invaract import _checks
from conftest import write_contract


def broken(check, **columns):
    """Which rows of the batch made of ``columns`` break ``check`` (a null in the mask, as for a null value, is not broken)."""
    batch = pa.RecordBatch.from_pydict(columns)
    mask = _checks.failing(check, batch.column(batch.schema.get_field_index(check["field"])), batch)
    return [bool(b) for b in pa.array(mask).to_pylist()]


def rng(**bounds):
    return {"field": "v", "type": "range", **bounds}


# what each check means


def test_equals_and_one_of_compare_numbers_by_value_and_anything_else_by_equality():
    assert broken({"field": "v", "type": "equals", "value": 1}, v=pa.array([1, 2, None], pa.int64())) == [False, True, False]
    assert broken({"field": "v", "type": "equals", "value": 1}, v=pa.array([1.0, 1.5], pa.float64())) == [False, True]
    assert broken({"field": "v", "type": "equals", "value": 1},
                  v=pa.array([decimal.Decimal("1.00"), decimal.Decimal("2.00")], pa.decimal128(5, 2))) == [False, True]
    assert broken({"field": "v", "type": "oneOf", "values": [1.5, 1.234, 123456]},
                  v=pa.array([decimal.Decimal("1.50"), decimal.Decimal("1.23")], pa.decimal128(5, 2))) == [False, True]
    assert broken({"field": "v", "type": "oneOf", "values": [1, 3]}, v=pa.array([1, 2, 3], pa.int64())) == [False, True, False]
    assert broken({"field": "v", "type": "equals", "value": "EU"}, v=pa.array(["EU", "eu", None])) == [False, True, False]
    assert broken({"field": "v", "type": "equals", "value": True}, v=pa.array([True, False])) == [False, True]


def test_a_value_of_another_kind_never_equals_one_of_the_columns():
    # the text "1" is not the number 1, True is not 1, and 1.5 is no integer
    assert broken({"field": "v", "type": "equals", "value": "1"}, v=pa.array([1], pa.int64())) == [True]
    assert broken({"field": "v", "type": "equals", "value": 1}, v=pa.array(["1"])) == [True]
    assert broken({"field": "v", "type": "equals", "value": True}, v=pa.array([1], pa.int64())) == [True]
    assert broken({"field": "v", "type": "equals", "value": 1}, v=pa.array([True])) == [True]
    assert broken({"field": "v", "type": "oneOf", "values": [1.5, 2]}, v=pa.array([1, 2], pa.int64())) == [True, False]


def test_a_column_of_another_type_is_compared_row_by_row():
    import datetime
    day = datetime.date(2026, 1, 1)
    assert broken({"field": "v", "type": "equals", "value": 1}, v=pa.array([day, None], pa.date32())) == [True, False]


@pytest.mark.parametrize("bounds, at, inside, outside", [
    ({"gte": 0}, 0, 1, -1),
    ({"gt": 0}, None, 1, 0),
    ({"lte": 10}, 10, 9, 11),
    ({"lt": 10}, None, 9, 10),
])
def test_range_holds_at_and_inside_each_bound_and_only_there(bounds, at, inside, outside):
    values = [v for v in (at, inside, outside) if v is not None]
    expected = ([False] if at is not None else []) + [False, True]
    assert broken(rng(**bounds), v=pa.array(values, pa.int64())) == expected


def test_range_compares_integers_floats_and_decimals_with_each_other():
    both = rng(gte=0, lt=0.5)
    assert broken(both, v=pa.array([0, -1, 1], pa.int64())) == [False, True, True]
    assert broken(both, v=pa.array([0.25, 0.5, -0.0], pa.float64())) == [False, True, False]
    assert broken(both, v=pa.array([decimal.Decimal("0.49"), decimal.Decimal("0.50")], pa.decimal128(5, 2))) == [False, True]


def test_range_is_broken_by_anything_that_is_not_a_number():
    assert broken(rng(gte=0), v=pa.array(["5", None])) == [True, False]
    assert broken(rng(gte=0), v=pa.array([True])) == [True]
    assert broken(rng(gte=0), v=pa.array([float("nan"), 1.0])) == [True, False]


def test_length_counts_characters_and_holds_at_each_bound():
    assert broken({"field": "v", "type": "length", "exact": 3}, v=pa.array(["abc", "ab", "abcd", "éü😀", None])) == [False, True, True, False, False]
    assert broken({"field": "v", "type": "length", "min": 2}, v=pa.array(["ab", "abc", "a"])) == [False, False, True]
    assert broken({"field": "v", "type": "length", "max": 2}, v=pa.array(["ab", "a", "abc"])) == [False, False, True]
    assert broken({"field": "v", "type": "length", "min": 1, "max": 2}, v=pa.array(["", "a", "abc"])) == [True, False, True]
    # a value that is not text is measured as its text
    assert broken({"field": "v", "type": "length", "exact": 3}, v=pa.array([12, 123, None], pa.int64())) == [True, False, False]


def test_field_range_compares_with_the_other_column_of_the_same_row():
    at_least = {"field": "end", "type": "fieldRange", "gte": "start"}
    assert broken(at_least, end=pa.array([5, 6, 4, 4], pa.int64()), start=pa.array([5, 5, 5, None], pa.int64())) == [False, False, True, False]
    after = {"field": "end", "type": "fieldRange", "gt": "start"}
    assert broken(after, end=pa.array([5, 6], pa.int64()), start=pa.array([5, 5], pa.int64())) == [True, False]
    at_most = {"field": "v", "type": "fieldRange", "lte": "hi"}
    assert broken(at_most, v=pa.array([3, 4], pa.int64()), hi=pa.array([3, 3], pa.int64())) == [False, True]
    below = {"field": "v", "type": "fieldRange", "lt": "hi"}
    assert broken(below, v=pa.array([3, 2], pa.int64()), hi=pa.array([3, 3], pa.float64())) == [True, False]


def test_a_field_range_bound_that_is_missing_or_not_a_number_is_no_bound():
    check = {"field": "v", "type": "fieldRange", "gte": "lo", "lte": "hi"}
    assert broken(check, v=pa.array([9], pa.int64()), lo=pa.array([1], pa.int64())) == [False]
    assert broken(check, v=pa.array([9], pa.int64()), lo=pa.array([1], pa.int64()), hi=pa.array(["3"])) == [False]
    assert broken(check, v=pa.array([9], pa.int64()), lo=pa.array([1], pa.int64()), hi=pa.array([3], pa.int64())) == [True]
    assert broken(check, v=pa.array(["x"]), lo=pa.array([1], pa.int64())) == [True]


def test_a_constraint_of_another_type_is_not_evaluated_silently():
    with pytest.raises(ValueError, match="myOwnCheck"):
        broken({"field": "v", "type": "myOwnCheck"}, v=pa.array([1]))


# counting over a whole write


def test_an_evaluation_counts_failing_rows_across_batches_and_keeps_five_distinct_examples_in_order():
    checks = [rng(gte=0), {"field": "v", "type": "range", "lt": 100}, {"field": "absent", "type": "range", "gte": 0}]
    evaluation = _checks.Evaluation(checks)
    batches = [pa.RecordBatch.from_pydict({"v": pa.array(values, pa.int64())}) for values in ([-1, -2, -1, 5], [-3, -4, None], [-5, -6, 7])]
    assert list(evaluation.observe(batches)) == batches, "every batch goes by unchanged"
    assert evaluation.results() == [
        {"failed": 7, "examples": ["-1", "-2", "-3", "-4", "-5"]},
        {"failed": 0, "examples": []},
        {"failed": 0, "examples": []},
    ]
    assert _checks.Evaluation([]).results() == []


# a run


def constrained_contract(total_constraint="{ type: range, gte: 0 }", extra_fields=""):
    return write_contract(outputs=(
        "  - name: report\n    location: data/out/report\n    format: parquet\n    saveMode: overwrite\n    schema:\n      fields:\n"
        "        - { name: region, type: string, required: true }\n"
        f"        - name: total\n          type: long\n          constraints:\n            - {total_constraint}\n{extra_fields}"))


def test_rows_that_all_meet_their_constraints_are_published(monkeypatch):
    monkeypatch.setenv("INVARACT_RUNTIME_DATA_QUALITY", "true")
    constrained_contract()
    with invaract.run("contract.yaml"):
        invaract.write(pa.table({"region": ["n", "s", "e"], "total": pa.array([0, 7, None], pa.int64())}), "report")
    assert pq.read_table("data/out/report").column("total").to_pylist() == [0, 7, None]


def test_a_row_that_breaks_a_constraint_refuses_the_run_and_nothing_is_published(monkeypatch):
    monkeypatch.setenv("INVARACT_RUNTIME_DATA_QUALITY", "true")
    constrained_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"region": ["n", "s", "e"], "total": pa.array([3, -2, -9], pa.int64())}), "report")
    assert refused.value.violations == ["RUNTIME_DATA_QUALITY_VIOLATION"]
    assert "2 row(s), for example -2, -9" in str(refused.value)
    assert "range gte 0" in str(refused.value)
    assert not os.path.exists("data/out/report")


def test_a_comparison_with_another_column_is_checked_row_by_row(monkeypatch):
    monkeypatch.setenv("INVARACT_RUNTIME_DATA_QUALITY", "true")
    constrained_contract("{ type: fieldRange, lte: budget }", "        - { name: budget, type: long }\n")
    table = pa.table({"region": ["n", "s"], "total": pa.array([5, 9], pa.int64()), "budget": pa.array([5, 8], pa.int64())})
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(table, "report")
    assert "1 row(s), for example 9" in str(refused.value)


def test_without_the_setting_the_constraints_are_not_checked_at_run_time():
    constrained_contract()
    with invaract.run("contract.yaml"):
        invaract.write(pa.table({"region": ["n"], "total": pa.array([-1], pa.int64())}), "report")
    assert pq.read_table("data/out/report").column("total").to_pylist() == [-1]
