# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""CSV files laid out as their contract's ``csv:`` options say: read and written with them, the options a job gives (``csv=``)
reported and refused when they disagree, and a dry run's draft declaring the ones it saw."""
import datetime
import glob
import json
import os

import pandas as pd
import pyarrow as pa
import pytest

import invaract
from invaract import _csv
from conftest import notify_to_file, write_contract


def dataset(name, location, csv="", fields=(("id", "long", True), ("city", "string", False), ("day", "date", False))):
    columns = "".join(f"        - {{ name: {n}, type: {t}, required: {str(r).lower()} }}\n" for n, t, r in fields)
    options = f"    csv: {csv}\n" if csv else ""
    return f"  - name: {name}\n    location: {location}\n    format: csv\n{options}    schema:\n      fields:\n{columns}"


def text(location="data/out/report", encoding="utf-8"):
    [path] = glob.glob(os.path.join(location, "*.csv"))
    with open(path, "rb") as f:
        return f.read().decode(encoding)


LATIN = '{ delimiter: ";", header: false, encoding: ISO-8859-1, nullValue: NA, dateFormat: dd/MM/yyyy }'


@pytest.fixture
def orders(environment):
    (environment / "data" / "in").mkdir(parents=True)
    with open("data/in/orders.csv", "wb") as f:
        f.write("1;K\xf6ln;03/11/2026\n2;NA;04/11/2026\n".encode("latin-1"))


def test_an_input_is_read_with_its_declared_delimiter_header_encoding_empty_marker_and_date_format(orders):
    write_contract(inputs=dataset("orders", "data/in/orders.csv", LATIN), outputs=dataset("report", "data/out/report"))
    with invaract.run("contract.yaml"):
        table = invaract.read("orders", as_="arrow")
        invaract.write(table, "report")
    assert table.to_pylist() == [{"id": 1, "city": "Köln", "day": datetime.date(2026, 11, 3)}, {"id": 2, "city": None, "day": datetime.date(2026, 11, 4)}]
    assert table.schema.field("day").type == pa.date32()


def test_an_output_is_written_with_its_declared_options(orders):
    out = '{ delimiter: "|", header: true, encoding: ISO-8859-1, quote: "\'", nullValue: "-", dateFormat: dd.MM.yy }'
    write_contract(inputs=dataset("orders", "data/in/orders.csv", LATIN), outputs=dataset("report", "data/out/report", out))
    with invaract.run("contract.yaml"):
        invaract.write(invaract.read("orders").assign(city=lambda d: d.city.replace("Köln", "Köln|Cologne")), "report")
    assert text(encoding="latin-1") == "id|city|day\n1|'Köln|Cologne'|03.11.26\n2|-|04.11.26\n"


def test_a_tab_delimiter_an_escape_and_timestamps_in_a_pattern(environment):
    out = '{ delimiter: "\\t", escape: "\\\\", timestampFormat: "yyyy-MM-dd\'T\'HH:mm" }'
    write_contract(outputs=dataset("report", "data/out/report", out, fields=(("id", "long", True), ("note", "string", False), ("at", "timestamp_ntz", False))))
    frame = pd.DataFrame({"id": [1], "note": ['say "hi"'], "at": pd.to_datetime(["2026-11-03 09:15"])})
    with invaract.run("contract.yaml"):
        invaract.write(frame, "report")
    assert text() == 'id\tnote\tat\n1\tsay \\"hi\\"\t2026-11-03T09:15\n'
    write_contract(inputs=dataset("again", "data/out/report", out, fields=(("id", "long", True), ("note", "string", False), ("at", "timestamp_ntz", False))),
                   outputs=dataset("report", "data/out/copy"))
    with invaract.run("contract.yaml"):
        again = invaract.read("again", as_="arrow")
        invaract.write(pd.DataFrame({"id": [1], "city": ["x"], "day": [None]}), "report")
    assert again.to_pylist() == [{"id": 1, "note": 'say "hi"', "at": datetime.datetime(2026, 11, 3, 9, 15)}]


def test_a_written_file_reads_back_with_the_same_options_as_the_same_values(orders):
    out = '{ delimiter: ";", header: false, nullValue: "", dateFormat: "MM/dd/yyyy" }'
    write_contract(inputs=dataset("orders", "data/in/orders.csv", LATIN), outputs=dataset("report", "data/out/report", out))
    with invaract.run("contract.yaml"):
        first = invaract.read("orders", as_="arrow")
        invaract.write(first, "report")
    write_contract(inputs=dataset("again", "data/out/report", out), outputs=dataset("report", "data/out/copy"))
    with invaract.run("contract.yaml"):
        again = invaract.read("again", as_="arrow")
        invaract.write(again, "report")
    assert again.to_pylist() == first.to_pylist()


def test_with_no_options_declared_a_csv_file_is_read_and_written_as_before(environment):
    write_contract(outputs=dataset("report", "data/out/report"))
    with invaract.run("contract.yaml"):
        invaract.write(pd.DataFrame({"id": [1], "city": [None], "day": [datetime.date(2026, 1, 2)]}), "report")
    assert text() == '"id","city","day"\n1,,2026-01-02\n'


def test_options_a_job_gives_that_disagree_with_the_contract_are_refused(orders):
    write_contract(inputs=dataset("orders", "data/in/orders.csv", LATIN), outputs=dataset("report", "data/out/report", '{ delimiter: "|" }'))
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.read("orders", csv={"header": True})
    # read as if it had a header, its first line names the columns: the declared ones are missing, and the options differ
    assert "INPUT_CSV_OPTIONS_MISMATCH" in refusal.value.violations
    with pytest.raises(invaract.ContractViolation) as refusal:
        with invaract.run("contract.yaml"):
            invaract.write(invaract.read("orders"), "report", csv={"sep": ","})
    assert refusal.value.violations == ["OUTPUT_CSV_OPTIONS_MISMATCH"]
    assert not os.path.exists("data/out/report")


def test_options_a_job_gives_that_agree_are_used_as_given(orders):
    write_contract(inputs=dataset("orders", "data/in/orders.csv", '{ delimiter: ";" }'), outputs=dataset("report", "data/out/report"))
    with invaract.run("contract.yaml"):
        table = invaract.read("orders", as_="arrow", csv={"delimiter": ";", "header": False, "encoding": "latin1", "nullValue": "NA",
                                                          "dateFormat": "dd/MM/yyyy"})
        invaract.write(table, "report")
    assert table.column("city").to_pylist() == ["Köln", None]


def test_a_dry_run_reads_and_writes_with_the_options_the_job_gives_and_drafts_them(orders, monkeypatch):
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    notify_to_file(monkeypatch)
    with invaract.run():
        table = invaract.read("orders", location="data/in/orders.csv", format="csv", as_="arrow",
                              csv={"delimiter": ";", "header": False, "encoding": "ISO-8859-1"})
        invaract.write(table, "report", location="data/out/report", format="csv", csv={"delimiter": "|"})
    assert text().startswith("f0|f1|f2\n1|Köln|03/11/2026\n")
    with open("events.jsonl") as f:
        summary = [json.loads(line) for line in f if "mergedContractYaml" in line][-1]["mergedContractYaml"]
    assert "csv:\n    delimiter: ;\n    header: false\n    encoding: ISO-8859-1" in summary
    assert "csv:\n    delimiter: '|'" in summary


def test_csv_options_on_another_format_are_a_mistake(environment):
    write_contract(outputs=dataset("report", "data/out/report").replace("format: csv", "format: parquet"))
    with pytest.raises(ValueError, match="csv= is for a CSV dataset, and 'report' is parquet"):
        with invaract.run("contract.yaml"):
            invaract.write(pd.DataFrame({"id": [1], "city": ["a"], "day": [datetime.date(2026, 1, 2)]}), "report", csv={"delimiter": ";"})


def test_an_option_the_package_does_not_know_is_refused_before_reading(orders):
    write_contract(inputs=dataset("orders", "data/in/orders.csv", LATIN), outputs=dataset("report", "data/out/report"))
    with pytest.raises(ValueError, match="does not know the CSV option 'separator'"):
        with invaract.run("contract.yaml"):
            invaract.read("orders", csv={"separator": ";"})


@pytest.mark.parametrize("java, strptime", [
    ("dd/MM/yyyy", "%d/%m/%Y"), ("yyyy-MM-dd'T'HH:mm:ss", "%Y-%m-%dT%H:%M:%S"), ("d MMM yy", "%d %b %y"),
    ("EEE, dd MMMM uuuu hh:mm a", "%a, %d %B %Y %I:%M %p"), ("yyyy-MM-dd HH:mm:ssXXX", "%Y-%m-%d %H:%M:%S%z"),
    ("'week' ''yy", "week '%y"), ("100% yyyy", "100%% %Y"),
])
def test_how_a_java_date_pattern_is_read(java, strptime):
    assert _csv.strptime_pattern(java) == strptime


@pytest.mark.parametrize("java", ["yyyy-MM-dd HH:mm:ss.SSS", "w", "VV"])
def test_a_java_pattern_arrow_cannot_read_is_said(java):
    with pytest.raises(invaract.CheckNotRun, match="does not take"):
        _csv.strptime_pattern(java)


def test_how_options_are_merged_and_spelt():
    assert _csv.options({"delimiter": ";", "header": "false"}, {"header": True, "sep": "|"}) == {"delimiter": "|", "header": "true"}
    assert _csv.options(None, None) == {}
    assert _csv.character({"delimiter": "\\t"}, "delimiter") == "\t" and _csv.character({}, "quote", '"') == '"'
    with pytest.raises(invaract.CheckNotRun, match="one-character delimiter"):
        _csv.character({"delimiter": "||"}, "delimiter")
    assert _csv.header({}) and not _csv.header({"header": "False"})
    assert _csv.Format({"delimiter": ";"}) == "csv" and _csv.Format({"delimiter": ";"}).options == {"delimiter": ";"}
