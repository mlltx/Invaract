# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The catalogue's tables that strict mode (the invaract package's ``_strict``) reads: one list of writers for the scan and
for the watch."""
import pytest

from invaract_lint import catalogue
from invaract_lint.catalogue import CONNECT, READ, WRITE


def test_writer_names_are_a_modules_attributes_the_catalogue_says_write():
    names = ["write_table", "read_table", "ParquetWriter", "ParquetFile", "write_to_dataset", "helper", "__name__"]
    found = catalogue.writer_names("pyarrow.parquet", names)
    assert sorted(found) == ["ParquetWriter", "write_table", "write_to_dataset"]
    assert {(rule.name, rule.code) for rule in found.values()} == {("pyarrow-write", WRITE)}


@pytest.mark.parametrize("module,names,expected", [
    ("pyarrow.parquet", ["read_table", "ParquetFile"], []),                    # reads are the scan's
    ("duckdb", ["connect", "read_parquet", "sql"], []),                        # so are connections
    ("deltalake", ["write_deltalake", "DeltaTable"], ["write_deltalake"]),
    ("helpers", ["write_table", "to_csv"], []),                                # no library the catalogue knows
    ("pyarrow.csv", [], []),
    ("pyarrow", ["output_stream", "input_stream", "create_memory_map", "table"], ["create_memory_map", "output_stream"]),
])
def test_writer_names(module, names, expected):
    assert sorted(catalogue.writer_names(module, names)) == expected


def test_frame_writer_names_are_the_methods_that_write_once_given_somewhere_to():
    names = ["write_parquet", "write_csv", "sink_parquet", "sink_csv", "to_pandas", "to_arrow", "collect", "write", "sinks", "to_csv",
             "write_clipboard", "tofile"]
    assert catalogue.frame_writer_names(names) == ["sink_csv", "sink_parquet", "to_csv", "tofile", "write_csv", "write_parquet"]
    assert catalogue.frame_writer_names([]) == []
    assert catalogue.frame_writer_names(["sink_"]) == ["sink_"]


def test_what_strict_mode_wraps_is_named_in_one_place():
    assert catalogue.COMPILED == {"pyarrow", "polars", "duckdb", "deltalake"}
    assert catalogue.COMPILED <= catalogue.DATA_LIBRARIES
    assert set(catalogue.FRAME_CLASSES) | set(catalogue.SQL_RUNNERS) <= catalogue.COMPILED
    assert catalogue.FRAME_CLASSES == {"polars": ("DataFrame", "LazyFrame"), "duckdb": ("DuckDBPyRelation",)}
    assert catalogue.SQL_RUNNERS == {"duckdb": ("DuckDBPyConnection",)}
    assert {"where", "sink", "path", "file", "dest", "base_dir", "root_path", "output_file", "destination", "file_name"} <= catalogue.DESTINATIONS


@pytest.mark.parametrize("name,code,rule", [
    ("pandas.read_csv", READ, "pandas-read"), ("pyarrow.parquet.write_table", WRITE, "pyarrow-write"),
    ("sqlite3.connect", CONNECT, "database-connect"), ("shutil.copy", WRITE, "file-operation"),
])
def test_module_rule(name, code, rule):
    found = catalogue.module_rule(name)
    assert (found.code, found.name) == (code, rule)


@pytest.mark.parametrize("name", ["pandas", "pandas.concat", "read_csv", "json.dumps", "xpandas.read_csv", "pandas.read_csv.extra", ""])
def test_module_rule_matches_the_whole_name_or_nothing(name):
    assert catalogue.module_rule(name) is None


def test_every_code_has_a_category_and_words_for_what_it_found():
    codes = [catalogue.READ, catalogue.WRITE, catalogue.CONNECT, catalogue.SQL, catalogue.PROCESS, catalogue.DYNAMIC]
    assert codes == ["IVR001", "IVR002", "IVR003", "IVR004", "IVR005", "IVR006"]
    assert [catalogue.CATEGORIES[c] for c in codes] == ["read", "write", "connect", "sql", "process", "dynamic"]
    assert set(catalogue.WHAT) == set(codes) and all(catalogue.WHAT.values())
    assert {rule.code for rule in catalogue.MODULE_RULES} == set(codes) - {catalogue.SQL}
    assert len({rule.name for rule in catalogue.MODULE_RULES}) == len(catalogue.MODULE_RULES)
