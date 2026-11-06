# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The formats besides Parquet and CSV: JSON lines, ORC, Avro, Feather and Excel, each written and read back as the
contract types it, laid out as Spark lays it out, partitioned or not."""
import builtins
import contextlib
import datetime
import decimal
import glob
import json
import os

import pyarrow as pa
import pytest

import invaract
from invaract import _formats
from invaract.errors import CheckNotRun
from conftest import write_contract

FORMATS = ("json", "orc", "avro", "feather", "excel")
UTC = datetime.timezone.utc

FIELDS = (("id", "long", True), ("city", "string", False), ("price", "decimal(10,2)", False), ("score", "double", False),
          ("day", "date", False), ("at", "timestamp", False), ("active", "boolean", False))

TABLE = pa.table({
    "id": pa.array([1, 2], pa.int64()),
    "city": ["Köln", None],
    "price": pa.array([decimal.Decimal("12.34"), None], pa.decimal128(10, 2)),
    "score": [0.5, None],
    "day": pa.array([datetime.date(2026, 11, 6), None], pa.date32()),
    "at": pa.array([datetime.datetime(2026, 11, 6, 9, 30, 0, 123000, tzinfo=UTC), None], pa.timestamp("us", tz="UTC")),
    "active": [True, None],
})


def dataset(name, location, fmt, fields=FIELDS, extra=""):
    columns = "".join(f"        - {{ name: {n}, type: \"{t}\", required: {str(r).lower()} }}\n" for n, t, r in fields)
    return f"  - name: {name}\n    location: {location}\n    format: {fmt}\n{extra}    schema:\n      fields:\n{columns}"


SINK = "  - name: unused\n    location: data/out/unused\n    schema:\n      fields:\n        - { name: n, type: long }\n"


def reading(inputs):
    """A contract with ``inputs``, and an output the reads in these tests never write."""
    return write_contract(inputs=inputs, outputs=SINK)


@contextlib.contextmanager
def reading_run():
    """A run of a contract from ``reading``: the reads inside, then the output written."""
    with invaract.run("contract.yaml"):
        yield
        invaract.write(pa.table({"n": pa.array([0], pa.int64())}), "unused")


def round_trip(fmt, table=TABLE, fields=FIELDS, extra=""):
    """``table`` written as ``fmt`` by one run, then read back by the next, as the contract types it."""
    write_contract(outputs=dataset("report", "data/out/report", fmt, fields, extra))
    with invaract.run("contract.yaml"):
        invaract.write(table, "report")
    reading(dataset("report", "data/out/report", fmt, fields, extra))
    with reading_run():
        return invaract.read("report", as_="arrow")


def files(fmt, root="data/out/report"):
    return sorted(glob.glob(os.path.join(root, "**", f"*.{_formats.extension(fmt)}"), recursive=True))


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_table_written_in_the_format_reads_back_the_same(fmt):
    read = round_trip(fmt)
    assert read.select(TABLE.column_names).to_pylist() == TABLE.to_pylist()
    assert len(files(fmt)) == 1


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_partitioned_output_is_written_in_hive_style_folders_and_read_back_with_its_key(fmt):
    fields = (("region", "string", False), ("total", "long", True))
    table = pa.table({"region": ["north", "south", "north"], "total": pa.array([1, 2, 3], pa.int64())})
    region = '        - { name: region, type: string, partitioned: true, partitionKeyPosition: 1 }\n'
    write_contract(outputs=dataset("report", "data/out/report", fmt, ()).replace("      fields:\n", "      fields:\n" + region)
                   + "        - { name: total, type: long, required: true }\n")
    with invaract.run("contract.yaml"):
        invaract.write(table, "report")
    assert [os.path.relpath(f, "data/out/report").split(os.sep)[0] for f in files(fmt)] == ["region=north", "region=south"]
    reading(dataset("report", "data/out/report", fmt, fields))
    with reading_run():
        read = invaract.read("report", as_="arrow")
    assert sorted((r["region"], r["total"]) for r in read.to_pylist()) == [("north", 1), ("north", 3), ("south", 2)]


def test_json_lines_are_written_as_spark_writes_them():
    round_trip("json")
    [path] = files("json")
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    assert lines == ['{"id":1,"city":"K\\u00f6ln","price":12.34,"score":0.5,"day":"2026-11-06","at":"2026-11-06T09:30:00.123Z","active":true}',
                     '{"id":2}']


def test_json_values_json_has_no_literal_for_are_text_and_nested_values_are_objects_and_arrays():
    assert _formats._json_line({"a": float("nan"), "b": float("inf"), "c": float("-inf")}) == '{"a":"NaN","b":"Infinity","c":"-Infinity"}\n'
    assert _formats._json_value({"x": 1, "y": None}) == '{"x":1}'
    assert _formats._json_value([1, None]) == "[1,null]"
    assert _formats._json_value([("k", 1)]) == '{"k":1}'
    assert _formats._json_value(b"\x00\x01") == '"AAE="'
    assert _formats._json_value(datetime.datetime(2026, 1, 2, 3, 4, 5, 6)) == '"2026-01-02T03:04:05.000006"'
    assert _formats._json_value(decimal.Decimal("1E+2")) == "100"


def test_a_json_file_written_elsewhere_is_read_with_the_contracts_types_and_extra_fields_kept():
    os.makedirs("data/in", exist_ok=True)
    with open("data/in/orders.json", "w", encoding="utf-8") as f:
        f.write('{"id": 1, "day": "2026-11-06", "at": "2026-11-06T09:30:00Z", "note": "x"}\n{"id": 2}\n')
    reading(dataset("orders", "data/in/orders.json", "json", (("id", "long", True), ("day", "date", False), ("at", "timestamp", False))))
    with reading_run():
        read = invaract.read("orders", as_="arrow")
    assert read.schema.field("day").type == pa.date32() and read.schema.field("at").type == pa.timestamp("us", tz="UTC")
    assert read.to_pylist() == [{"id": 1, "day": datetime.date(2026, 11, 6), "at": datetime.datetime(2026, 11, 6, 9, 30, tzinfo=UTC)},
                                {"id": 2, "day": None, "at": None}]


def test_avro_is_laid_out_as_spark_lays_it_out():
    import fastavro

    round_trip("avro")
    [path] = files("avro")
    with open(path, "rb") as f:
        schema = fastavro.reader(f).writer_schema
    assert schema["name"] == "topLevelRecord"
    types = {f["name"]: f["type"] for f in schema["fields"]}
    assert types["id"] == "long"
    assert types["city"] == ["null", "string"]
    assert types["price"] == ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}]
    assert types["day"] == ["null", {"type": "int", "logicalType": "date"}]
    assert types["at"] == ["null", {"type": "long", "logicalType": "timestamp-micros"}]


def test_avro_nested_columns_and_types_without_a_time_zone_round_trip():
    table = pa.table({
        "id": pa.array([1], pa.int64()),
        "point": pa.array([{"x": 1, "y": 2.5}], pa.struct([("x", pa.int32()), ("y", pa.float64())])),
        "tags": pa.array([["a", None]], pa.list_(pa.string())),
        "attrs": pa.array([[("k", 1)]], pa.map_(pa.string(), pa.int64())),
        "local": pa.array([datetime.datetime(2026, 1, 2, 3, 4)], pa.timestamp("us")),
        "blob": pa.array([b"\x00"], pa.binary()),
        "ratio": pa.array([0.25], pa.float32()),
    })
    write_contract(outputs="  - name: report\n    location: data/out/report\n    format: avro\n")
    with invaract.run("contract.yaml"):
        invaract.write(table, "report")
    reading("  - name: report\n    location: data/out/report\n    format: avro\n")
    with reading_run():
        read = invaract.read("report", as_="arrow")
    assert read.to_pylist() == table.to_pylist()
    assert read.schema.field("local").type == pa.timestamp("us") and read.schema.field("ratio").type == pa.float32()


def test_avro_refuses_a_column_name_it_cannot_hold():
    write_contract(outputs="  - name: report\n    location: data/out/report\n    format: avro\n")
    with pytest.raises(CheckNotRun, match="Avro names a column with letters, digits and _, not 'total sales'"):
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"total sales": [1]}), "report")
    assert not os.path.exists("data/out/report")


def test_an_avro_schema_maps_onto_arrow_types():
    t = _formats._avro_type
    assert t(["null", "long"]) == (pa.int64(), True) and t("string") == (pa.string(), False)
    assert t({"type": "long", "logicalType": "timestamp-millis"}) == (pa.timestamp("us", tz="UTC"), False)
    assert t({"type": "long", "logicalType": "local-timestamp-millis"}) == (pa.timestamp("us"), False)
    assert t({"type": "fixed", "name": "f", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 3}) == (pa.decimal128(18, 3), False)
    assert t({"type": "enum", "name": "e", "symbols": ["A"]}) == (pa.string(), False)
    assert t({"type": "fixed", "name": "g", "size": 4}) == (pa.binary(), False)
    assert t({"type": "map", "values": "int"}) == (pa.map_(pa.string(), pa.int32()), False)
    assert t({"type": "string"}) == (pa.string(), False)
    with pytest.raises(CheckNotRun, match="only of one type and null"):
        t(["null", "long", "string"])


def test_an_excel_sheet_has_a_header_row_and_times_in_utc():
    import openpyxl

    table = TABLE.set_column(5, "at", pa.array([datetime.datetime(2026, 11, 6, 10, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=1))), None],
                                               pa.timestamp("us", tz="Europe/Berlin")))
    write_contract(outputs=dataset("report", "data/out/report", "excel"))
    with invaract.run("contract.yaml"):
        invaract.write(table.cast(TABLE.schema), "report")
    [path] = files("excel")
    assert path.endswith(".xlsx")
    rows = list(openpyxl.load_workbook(path).worksheets[0].iter_rows(values_only=True))
    assert rows[0] == ("id", "city", "price", "score", "day", "at", "active")
    assert rows[1][5] == datetime.datetime(2026, 11, 6, 9, 30)


def test_an_excel_sheet_written_by_hand_is_read_as_the_contract_types_it():
    import openpyxl

    book = openpyxl.Workbook()
    sheet = book.active
    sheet.append(["id", "price", "day", "score", "code", "free"])
    sheet.append([1.0, 12.34, datetime.datetime(2026, 11, 6), 2, 7, "x"])
    sheet.append([None, None, None, None, None, None])
    sheet.append([2, None, None, None, None, 3])
    os.makedirs("data/in", exist_ok=True)
    book.save("data/in/orders.xlsx")
    fields = (("id", "long", True), ("price", "decimal(10,2)", False), ("day", "date", False), ("score", "double", False), ("code", "string", False))
    reading(dataset("orders", "data/in/orders.xlsx", "excel", fields))
    with reading_run():
        read = invaract.read("orders", as_="arrow")
    assert read.to_pylist() == [{"id": 1, "price": decimal.Decimal("12.34"), "day": datetime.date(2026, 11, 6), "score": 2.0, "code": "7"},
                                {"id": 2, "price": None, "day": None, "score": None, "code": None}]


def test_a_sheet_column_that_is_not_the_declared_type_is_not_read():
    import openpyxl

    book = openpyxl.Workbook()
    book.active.append(["id"])
    book.active.append(["one"])
    os.makedirs("data/in", exist_ok=True)
    book.save("data/in/orders.xlsx")
    reading(dataset("orders", "data/in/orders.xlsx", "excel", (("id", "long", True),)))
    with pytest.raises(CheckNotRun, match="cannot read the sheet's column 'id' as int64"):
        with reading_run():
            invaract.read("orders")


def test_an_undeclared_sheet_column_is_typed_by_its_cells_or_read_as_text():
    assert _formats._guess([1, 2, None]).type == pa.int64()
    assert _formats._guess([1, "a"]).to_pylist() == ["1", "a"]


def test_excel_refuses_a_column_a_cell_cannot_hold_and_too_many_rows(monkeypatch):
    write_contract(outputs="  - name: report\n    location: data/out/report\n    format: excel\n")
    with pytest.raises(CheckNotRun, match="cannot write the column 'blob'"):
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"blob": [b"x"]}), "report")
    monkeypatch.setattr(_formats, "EXCEL_ROWS", 2)
    with pytest.raises(CheckNotRun, match="holds at most 2 rows"):
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"n": [1, 2, 3]}), "report")
    assert not os.path.exists("data/out/report")


@pytest.mark.parametrize("fmt, module, extra", [("avro", "fastavro", "avro"), ("excel", "openpyxl", "excel")])
def test_a_format_whose_library_is_missing_says_which_extra_to_install(monkeypatch, fmt, module, extra):
    real = builtins.__import__

    def without(name, *args, **kwargs):
        if name == module or name.startswith(module + "."):
            raise ImportError(name)
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without)
    write_contract(outputs=f"  - name: report\n    location: data/out/report\n    format: {fmt}\n")
    with pytest.raises(CheckNotRun, match=rf'needs the {module} package to read or write {fmt}: pip install "invaract\[{extra}\]"'):
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"n": [1]}), "report")


def test_a_pandas_frame_is_written_in_each_format():
    import pandas as pd

    frame = pd.DataFrame({"id": [1, 2], "city": ["a", None]})
    for fmt in FORMATS:
        write_contract(outputs=dataset("report", f"data/out/{fmt}", fmt, (("id", "long", True), ("city", "string", False))))
        with invaract.run("contract.yaml"):
            invaract.write(frame, "report")
        reading(dataset("report", f"data/out/{fmt}", fmt, (("id", "long", True), ("city", "string", False))))
        with reading_run():
            assert invaract.read("report").equals(frame.astype({"id": "int64"})), fmt


def test_an_empty_folder_reads_as_the_contracts_columns():
    os.makedirs("data/in/orders", exist_ok=True)
    reading(dataset("orders", "data/in/orders", "json", (("id", "long", True), ("day", "date", False))))
    with reading_run():
        read = invaract.read("orders", as_="arrow")
    assert read.num_rows == 0 and read.schema.names == ["id", "day"] and read.schema.field("day").type == pa.date32()


def test_chunked_reads_go_file_by_file():
    for fmt in ("json", "avro"):
        write_contract(outputs=dataset("report", f"data/out/{fmt}", fmt, (("n", "long", True),)))
        with invaract.run("contract.yaml"):
            invaract.write(pa.table({"n": pa.array([1, 2, 3], pa.int64())}), "report")
            invaract.write(pa.table({"n": pa.array([4], pa.int64())}), "report", save_mode="append")
        reading(dataset("report", f"data/out/{fmt}", fmt, (("n", "long", True),)))
        with reading_run():
            chunks = [c["n"].tolist() for c in invaract.read("report", chunksize=2)]
        assert sorted(sum(chunks, [])) == [1, 2, 3, 4] and [len(c) for c in chunks] == [2, 2], fmt


def test_a_location_s_extension_names_its_format_when_nothing_else_does(monkeypatch):
    assert [_formats.by_extension(p) for p in ("a.JSONL", "b.ndjson", "c.json", "d.orc", "e.avro", "f.feather", "g.arrow", "h.xlsx", "i.csv")] == \
        ["json", "json", "json", "orc", "avro", "feather", "feather", "excel", "csv"]
    assert _formats.by_extension("data/report") is None and _formats.by_extension("x.parquet") is None
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    with invaract.run():
        invaract.write(pa.table({"n": [1]}), "report", location="data/out/report.jsonl")
    with open(files("json", "data/out/report.jsonl")[0], encoding="utf-8") as f:
        assert json.loads(f.read()) == {"n": 1}
