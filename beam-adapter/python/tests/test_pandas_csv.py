# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What the wrapper runner states of a pipeline's pandas CSV reads and writes (``invaract_beam.pandas_csv``)."""
import csv
import json

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2

from invaract_beam import pandas_csv


def stated(proto):
    """``{step: {key: (namespace, value)}}`` for every item ``annotate`` added."""
    found = {}
    for t in proto.components.transforms.values():
        for d in t.display_data:
            item = beam_runner_api_pb2.LabelledPayload.FromString(d.payload)
            if item.namespace.startswith("invaract_beam."):
                found.setdefault(t.unique_name, {})[item.key] = (item.namespace, item.string_value)
    return found


def test_each_pandas_csv_read_and_write_states_its_path_and_options_on_its_own_step(tmp_path):
    (tmp_path / "orders.csv").write_text("id;label\n1;a\n")
    p = beam.Pipeline()
    rows = p | "ReadOrders" >> beam.io.ReadFromCsv(str(tmp_path / "*.csv"), sep=";")
    _ = rows | "WriteReport" >> beam.io.WriteToCsv("out/report", sep="|", header=False, na_rep="NA")
    _ = rows | "Text" >> beam.Map(str) | "WriteText" >> beam.io.WriteToText("out/text")
    found = stated(pandas_csv.annotate(p, p.to_runner_api()))
    assert set(found) == {"ReadOrders", "WriteReport"}, "nothing is said of any other step"
    assert found["ReadOrders"]["file_pattern"] == (pandas_csv.READ, str(tmp_path / "*.csv"))
    assert json.loads(found["ReadOrders"]["csv_options"][1]) == {
        "delimiter": ";", "header": "true", "encoding": "utf-8", "quote": '"', "escape": ""}
    assert found["WriteReport"]["file_pattern"] == (pandas_csv.WRITE, "out/report")
    assert json.loads(found["WriteReport"]["csv_options"][1]) == {
        "delimiter": "|", "header": "false", "encoding": "utf-8", "quote": '"', "escape": "", "nullValue": "NA"}


def test_a_read_has_a_header_unless_the_job_names_the_columns_or_says_none():
    assert pandas_csv.read_options({})["header"] == "true", "pandas' default 'infer', with no names: the first line names them"
    assert pandas_csv.read_options({"names": ["id"]})["header"] == "false", "'infer' with names: the first line is data"
    assert pandas_csv.read_options({"header": None})["header"] == "false"
    assert pandas_csv.read_options({"header": 0, "names": ["id"]})["header"] == "true", "an explicit header line wins"


def test_a_write_has_a_header_unless_it_turns_it_off():
    assert pandas_csv.write_options({})["header"] == "true"
    assert pandas_csv.write_options({"header": False})["header"] == "false"
    assert pandas_csv.write_options({"header": ["a", "b"]})["header"] == "true", "column aliases are still a header line"


def test_the_delimiter_is_the_job_s_or_pandas_default_and_is_not_stated_when_pandas_would_guess_it():
    assert pandas_csv.read_options({})["delimiter"] == ","
    assert pandas_csv.read_options({"sep": "\t"})["delimiter"] == "\t"
    assert pandas_csv.read_options({"delimiter": ";", "sep": ","})["delimiter"] == ";", "pandas' delimiter wins over sep"
    assert pandas_csv.read_options({"delimiter": None, "sep": "|"})["delimiter"] == "|"
    assert "delimiter" not in pandas_csv.read_options({"sep": None}), "sniffed"
    assert "delimiter" not in pandas_csv.read_options({"sep": r"\s+"}), "a regular expression"


def test_quote_escape_and_encoding_are_pandas_defaults_unless_the_job_sets_them():
    assert pandas_csv.write_options({}) == {
        "delimiter": ",", "header": "true", "encoding": "utf-8", "quote": '"', "escape": "", "nullValue": ""}
    options = pandas_csv.write_options({"quotechar": "'", "escapechar": "\\", "encoding": "latin1"})
    assert (options["quote"], options["escape"], options["encoding"]) == ("'", "\\", "latin1")
    assert pandas_csv.write_options({"quoting": csv.QUOTE_NONE, "quotechar": "'"})["quote"] == "", "never quoted"
    assert pandas_csv.write_options({"quoting": csv.QUOTE_ALL})["quote"] == '"'


def test_a_read_states_its_missing_value_text_only_when_the_job_replaced_pandas_list_with_one_of_its_own():
    assert "nullValue" not in pandas_csv.read_options({})
    assert "nullValue" not in pandas_csv.read_options({"na_values": "NA"}), "added to pandas' own list, which still counts"
    assert pandas_csv.read_options({"na_values": "NA", "keep_default_na": False})["nullValue"] == "NA"
    assert pandas_csv.read_options({"na_values": ["-"], "keep_default_na": False})["nullValue"] == "-"
    assert "nullValue" not in pandas_csv.read_options({"na_values": ["-", "NA"], "keep_default_na": False}), "several"
    assert "nullValue" not in pandas_csv.read_options({"keep_default_na": False}), "none at all"


def test_other_pandas_reads_and_writes_are_not_csv():
    from apache_beam.dataframe import io as dataframe_io
    assert pandas_csv.describe(dataframe_io.ReadViaPandas("json", "in/orders.json")) is None
    assert pandas_csv.describe(dataframe_io.WriteViaPandas("json", "out/report")) is None
    assert pandas_csv.describe(beam.Map(str)) is None
