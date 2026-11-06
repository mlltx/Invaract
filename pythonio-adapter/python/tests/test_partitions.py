# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Partitioned outputs: written Hive-style as the contract (or the job) says, checked against the contract's partition keys,
and published by the save mode and the overwrite mode, whole output or partition by partition."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from conftest import write_contract

DAY1 = pa.table({"region": ["north", "south", "north"], "total": pa.array([1, 2, 3], pa.int64())})
DAY2 = pa.table({"region": ["north", "east"], "total": pa.array([10, 20], pa.int64())})


def contract(keys=("region",), mode=None, fmt="parquet", save_mode="overwrite"):
    region = "        - { name: region, type: string" + (", partitioned: true, partitionKeyPosition: 1" if "region" in keys else "") + " }\n"
    overwrite = f"    partitionOverwriteMode: {mode}\n" if mode else ""
    return write_contract(outputs=f"  - name: report\n    location: data/out/report\n    format: {fmt}\n    saveMode: {save_mode}\n{overwrite}"
                                  f"    schema:\n      fields:\n{region}        - {{ name: total, type: long }}\n")


def tree(root="data/out/report"):
    """The output's partition folders, each with how many files it holds, hidden and metadata files left out."""
    found = {}
    for folder, _, names in os.walk(root):
        data = [n for n in names if not n.startswith((".", "_"))]
        if data:
            found[os.path.relpath(folder, root).replace(os.sep, "/")] = len(data)
    return found


def rows(root="data/out/report"):
    return sorted(((r["region"], r["total"]) for r in invaract_read(root)), key=lambda r: (r[0] is None, r))


def invaract_read(root):
    import pyarrow.dataset as ds

    return ds.dataset(root, format="parquet", partitioning="hive").to_table().to_pylist()


def run(*tables, **write_args):
    with invaract.run("contract.yaml"):
        for table in tables:
            invaract.write(table, "report", **write_args)


# laid out as the contract says


def test_an_output_the_contract_partitions_is_written_in_one_folder_per_value_with_the_key_out_of_the_files():
    contract()
    run(DAY1)
    assert tree() == {"region=north": 1, "region=south": 1}
    data_file = next(f for f in os.listdir("data/out/report/region=north") if f.endswith(".parquet"))
    assert pq.ParquetFile(f"data/out/report/region=north/{data_file}").schema_arrow.names == ["total"]
    assert rows() == [("north", 1), ("north", 3), ("south", 2)]


def test_a_partitioned_input_is_read_with_its_keys_typed_as_the_contract_declares():
    os.makedirs("data/in/orders/day=7", exist_ok=True)
    pq.write_table(pa.table({"v": pa.array([1], pa.int64())}), "data/in/orders/day=7/part-0.parquet")
    write_contract(inputs="  - name: orders\n    location: data/in/orders\n    format: parquet\n    schema:\n      fields:\n"
                          "        - { name: day, type: long, partitioned: true }\n        - { name: v, type: long }\n",
                   outputs="  - name: o\n    location: data/out/o\n    schema:\n      fields:\n        - { name: v, type: long }\n")
    with invaract.run("contract.yaml"):
        table = invaract.read("orders", as_="arrow")
        invaract.write(table.select(["v"]), "o")
    assert table.schema.field("day").type == pa.int64()
    assert table.to_pylist() == [{"v": 1, "day": 7}]


def test_an_empty_key_value_goes_to_the_hive_default_partition_and_reads_back_empty():
    contract()
    run(pa.table({"region": pa.array(["north", None]), "total": pa.array([1, 2], pa.int64())}))
    assert tree() == {"region=north": 1, "region=__HIVE_DEFAULT_PARTITION__": 1}
    assert rows() == [("north", 1), (None, 2)]


def test_a_csv_output_is_partitioned_too():
    contract(fmt="csv")
    run(DAY1)
    assert tree() == {"region=north": 1, "region=south": 1}
    assert all(f.endswith(".csv") for f in os.listdir("data/out/report/region=north"))


def test_a_partitioned_csv_output_is_written_with_its_declared_csv_options():
    path = contract(fmt="csv")
    with open(path, encoding="utf-8") as f:
        text = f.read().replace("    format: csv\n", '    format: csv\n    csv: { delimiter: ";", header: false }\n')
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    run(DAY1)
    assert tree() == {"region=north": 1, "region=south": 1}
    [north] = os.listdir("data/out/report/region=north")
    assert north.endswith(".csv")
    with open(f"data/out/report/region=north/{north}", encoding="utf-8") as f:
        assert f.read() == "1\n3\n"


# the job's own layout, checked


def test_the_job_may_say_the_contracts_layout_itself():
    contract()
    run(DAY1, partition_by=["region"])
    assert set(tree()) == {"region=north", "region=south"}


def test_a_write_partitioned_otherwise_than_the_contract_declares_is_refused_and_nothing_is_published():
    contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        run(DAY1, partition_by=[])
    assert refused.value.violations == ["OUTPUT_PARTITIONING_MISMATCH"]
    assert "partitioned by region but the write partitions by none" in str(refused.value)
    assert not os.path.exists("data/out/report")


def test_with_no_keys_in_the_contract_the_job_partitions_as_it_likes():
    contract(keys=())
    run(DAY1, partition_by=["region"])
    assert set(tree()) == {"region=north", "region=south"}


def test_partitioning_by_a_column_the_table_lacks_is_not_checked():
    contract(keys=())
    with pytest.raises(invaract.CheckNotRun, match="no such column"):
        run(DAY1, partition_by=["country"])
    assert not os.path.exists("data/out/report")


def test_one_output_written_twice_in_a_run_has_one_layout():
    contract(keys=())
    with pytest.raises(invaract.CheckNotRun, match="one output has one layout"):
        with invaract.run("contract.yaml"):
            invaract.write(DAY1, "report", partition_by=["region"])
            invaract.write(DAY2, "report")
    assert not os.path.exists("data/out/report")


# publishing: the save mode, and an overwrite's mode


def test_an_append_adds_each_runs_files_to_their_folders():
    contract(save_mode="append")
    run(DAY1)
    run(DAY2)
    assert tree() == {"region=north": 2, "region=south": 1, "region=east": 1}
    assert rows() == [("east", 20), ("north", 1), ("north", 3), ("north", 10), ("south", 2)]


def test_a_static_overwrite_replaces_every_partition():
    contract()
    run(DAY1)
    run(DAY2)
    assert tree() == {"region=north": 1, "region=east": 1}
    assert rows() == [("east", 20), ("north", 10)]
    assert [f for f in os.listdir("data/out") if f.startswith(".")] == []


def test_a_dynamic_overwrite_replaces_only_the_partitions_written():
    contract(mode="dynamic")
    run(DAY1)
    run(DAY2)
    assert tree() == {"region=north": 1, "region=south": 1, "region=east": 1}
    assert rows() == [("east", 20), ("north", 10), ("south", 2)]
    assert [f for f in os.listdir("data/out") if f.startswith(".")] == [], "nothing is left beside the output"


def test_an_overwrite_in_another_mode_than_the_contract_declares_is_refused():
    contract(mode="dynamic")
    with pytest.raises(invaract.ContractViolation) as refused:
        run(DAY1, partition_overwrite="static")
    assert refused.value.violations == ["OUTPUT_PARTITIONING_MISMATCH"]
    assert "partitionOverwriteMode 'dynamic' but the write overwrites with 'static'" in str(refused.value)


def test_a_dynamic_overwrite_that_fails_part_way_puts_back_the_partitions_it_had_replaced(monkeypatch):
    contract(mode="dynamic")
    run(DAY1)
    before = rows()
    real = os.rename
    moved = []

    def failing(src, dst):
        # the second partition folder moved into place fails
        if ".invaract-staging-" in str(src) and "region=" in str(src):
            moved.append(src)
            if len(moved) == 2:
                raise OSError("disk gone")
        return real(src, dst)

    monkeypatch.setattr(os, "rename", failing)
    with pytest.raises(OSError, match="disk gone"):
        run(pa.table({"region": ["north", "south"], "total": pa.array([7, 8], pa.int64())}))
    monkeypatch.setattr(os, "rename", real)
    assert rows() == before
