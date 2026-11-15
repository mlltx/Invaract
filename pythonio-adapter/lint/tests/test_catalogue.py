# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The catalogue's tables that strict mode (the invaract package's ``_strict``) reads: one list of writers for the scan and
for the watch."""
import pytest

from invaract_lint import catalogue
from invaract_lint.catalogue import CONNECT, READ, WRITE


def test_watched_names_are_the_readers_everywhere_and_the_writers_of_compiled_libraries():
    names = ["write_table", "read_table", "ParquetWriter", "ParquetFile", "write_to_dataset", "helper", "__name__"]
    found = catalogue.watched_names("pyarrow.parquet", names)
    assert sorted(found) == ["ParquetFile", "ParquetWriter", "read_table", "write_table", "write_to_dataset"]
    assert {name: (rule.name, rule.code) for name, rule in found.items()} == {
        "ParquetFile": ("pyarrow-read", READ), "read_table": ("pyarrow-read", READ), "ParquetWriter": ("pyarrow-write", WRITE),
        "write_table": ("pyarrow-write", WRITE), "write_to_dataset": ("pyarrow-write", WRITE)}


@pytest.mark.parametrize("module,names,expected", [
    ("pandas", ["read_csv", "ExcelWriter", "HDFStore", "concat"], ["read_csv"]),       # pandas writes through Python's open
    ("numpy", ["load", "save", "savetxt", "loadtxt", "array"], ["load", "loadtxt"]),
    ("duckdb", ["connect", "read_parquet", "sql"], ["connect", "read_parquet"]),
    ("deltalake", ["write_deltalake", "DeltaTable"], ["DeltaTable", "write_deltalake"]),
    ("pyreadstat", ["write_sav", "read_sav", "set_value_labels"], ["read_sav", "write_sav"]),
    ("psycopg", ["connect", "Connection", "sql"], ["connect"]),
    ("adbc_driver_postgresql.dbapi", ["connect", "Cursor"], ["connect"]),
    ("sqlalchemy", ["create_engine"], []),                                      # the driver's connect is where it connects
    ("requests", ["get", "post"], []),                                          # Python's sockets are seen by themselves
    ("shutil", ["copy", "move"], []),                                           # so are its files
    ("helpers", ["write_table", "to_csv", "read_csv"], []),                     # no library the catalogue knows
    ("pyarrow.csv", [], []),
    ("pyarrow", ["output_stream", "input_stream", "create_memory_map", "table"], ["create_memory_map", "input_stream", "output_stream"]),
])
def test_watched_names(module, names, expected):
    assert sorted(catalogue.watched_names(module, names)) == expected


@pytest.mark.parametrize("module,watched", [
    ("pyarrow", True), ("pyarrow.parquet", True), ("polars", True), ("duckdb", True), ("deltalake", True), ("h5py", True),
    ("tables", True), ("netCDF4", True), ("pyreadstat", True), ("pandas", True), ("pandas.io.parsers", True), ("numpy", True),
    ("psycopg", True), ("psycopg2", True), ("sqlite3", True), ("mysql.connector", True), ("mysql", True),
    ("adbc_driver_postgresql.dbapi", True), ("adbc_driver_postgresql", False), ("requests", False), ("json", False), ("helpers", False),
    ("pandasql", False), ("", False),
])
def test_the_libraries_strict_mode_looks_into(module, watched):
    assert catalogue.watched_library(module) is watched


def test_opener_names_are_a_compiled_librarys_functions_that_open_a_file_by_name_and_mode():
    assert catalogue.opener_names("pyarrow", ["OSFile", "memory_map", "table"]) == {"OSFile": (1, "mode", "wax+"), "memory_map": (1, "mode", "wax+")}
    assert catalogue.opener_names("h5py", ["File", "Group"]) == {"File": (1, "mode", "wax+")}
    assert catalogue.opener_names("gzip", ["open"]) == {}, "Python's own openers are seen by themselves"
    assert catalogue.opener_names("dbm.dumb", ["open", "error"]) == {"open": (1, "flag", "wcn")}, "but for the ones that write from compiled code"
    assert catalogue.watched_library("dbm") and catalogue.watched_library("dbm.gnu") and not catalogue.watched_library("gzip")
    assert catalogue.DATA_OPENERS <= set(catalogue.OPENERS)


def test_frame_writer_names_are_the_methods_that_write_once_given_somewhere_to():
    names = ["write_parquet", "write_csv", "sink_parquet", "sink_csv", "to_pandas", "to_arrow", "collect", "write", "sinks", "to_csv",
             "write_clipboard", "tofile"]
    assert catalogue.frame_writer_names(names) == ["sink_csv", "sink_parquet", "to_csv", "tofile", "write_csv", "write_parquet"]
    assert catalogue.frame_writer_names([]) == []
    assert catalogue.frame_writer_names(["sink_"]) == ["sink_"]


def test_what_strict_mode_wraps_is_named_in_one_place():
    assert catalogue.COMPILED == {"pyarrow", "polars", "duckdb", "deltalake", "h5py", "tables", "netCDF4", "pyreadstat"}
    assert catalogue.COMPILED_STANDARD == {"dbm"} and not catalogue.COMPILED_STANDARD & catalogue.DATA_LIBRARIES
    assert catalogue.COMPILED | catalogue.READERS <= catalogue.DATA_LIBRARIES
    assert catalogue.DATA_OPENERS == {"tables.open_file", "h5py.File", "netCDF4.Dataset"}, "pyarrow's own files are plain files, like open()"
    assert not catalogue.COMPILED & catalogue.READERS
    assert set(catalogue.FRAME_CLASSES) | set(catalogue.SQL_RUNNERS) | set(catalogue.READER_CLASSES) <= catalogue.COMPILED
    assert catalogue.FRAME_CLASSES == {"polars": ("DataFrame", "LazyFrame"), "duckdb": ("DuckDBPyRelation",)}
    assert catalogue.SQL_RUNNERS == {"duckdb": ("DuckDBPyConnection", "DuckDBPyRelation"), "polars": ("SQLContext",)}
    assert catalogue.FILESYSTEM_WRITES >= catalogue.STREAM_WRITES and not catalogue.FILESYSTEM_WRITES & catalogue.FILESYSTEM_READS
    assert {"source", "path", "filepath_or_buffer", "io", "con"} <= catalogue.SOURCES
    assert {"where", "sink", "path", "file", "dest", "base_dir", "root_path", "output_file", "destination", "file_name", "dst_path"} <= catalogue.DESTINATIONS


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
