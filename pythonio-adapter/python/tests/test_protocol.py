# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The protocol's requests as the package builds them (``_protocol``), and its published JSON Schema held to the checker's
recorded cases (``src/test/resources/protocol``): what the checker accepts, the schema allows, and what the schema rejects,
the checker does not check. Every request and reply of every other test is validated too (``conftest.protocol_checked``)."""
import json
import os

import pyarrow as pa
import pytest

from invaract import _csv, _outputs, _protocol, _settings
from conftest import PROTOCOL_SCHEMA, protocol_errors, reply_errors

CASES = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, "src", "test", "resources", "protocol")

#: Requests the checker accepts that the schema does not allow, each recorded to show how the checker reads it, and why.
TOLERATED = {
    ("protocol-version", 3): "an open without protocol, from a package older than the field, is taken as version 1",
    ("finish-defaults-to-completed", 6): "a finish without completed is taken as completed",
    ("check-needs-a-contract", 2): "a check flag that is not true is no check",
}


def _schema():
    with open(PROTOCOL_SCHEMA, encoding="utf-8") as f:
        return json.load(f)


def _json_lines(path):
    """Each line of ``path``, parsed, or ``None`` for one that is not JSON (a case that sends the checker nonsense)."""
    with open(path, encoding="utf-8") as f:
        lines = [line for line in f.read().splitlines() if line.strip()]
    parsed = []
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except ValueError:
            parsed.append(None)
    return parsed


def _cases():
    return sorted(c for c in os.listdir(CASES) if os.path.isdir(os.path.join(CASES, c)))


# what the package sends


def test_a_column_is_described_by_pyarrows_name_for_its_type_its_flag_and_its_empty_values():
    schema = pa.schema([pa.field("a", pa.list_(pa.struct([("sku", pa.string())]))), pa.field("b", pa.int64(), nullable=False)])
    assert _protocol.columns(schema) == [
        {"name": "a", "type": "list<item: struct<sku: string>>", "nullable": True},
        {"name": "b", "type": "int64", "nullable": False},
    ]
    assert _protocol.columns(schema, {"a": 2, "b": 0}) == [
        {"name": "a", "type": "list<item: struct<sku: string>>", "nullable": True, "nulls": 2},
        {"name": "b", "type": "int64", "nullable": False, "nulls": 0},
    ]


def test_open_says_the_protocol_version_and_asks_for_a_check_only_when_it_is_one():
    job = {"name": "orders.main", "runId": "r1", "pythonVersion": "3.12.0", "libraries": {"pyarrow": "17.0.0"}}
    assert _protocol.open_run({"contract": "c.yaml"}, job) == {"op": "open", "protocol": 1, "settings": {"contract": "c.yaml"}, "job": job}
    assert _protocol.open_run({}, job, check=True)["check"] is True
    assert _protocol.VERSION == 1


def test_a_request_leaves_out_what_it_does_not_have():
    assert _protocol.read("orders") == {"op": "read", "name": "orders"}
    assert _protocol.read("orders", "in/orders.csv", "csv", schema=[], csv={}) == {
        "op": "read", "name": "orders", "location": "in/orders.csv", "format": "csv", "schema": []}
    assert _protocol.read("orders", fmt="csv", csv={"delimiter": ";"})["csv"] == {"delimiter": ";"}
    assert _protocol.written("report", {"total": 0}) == {"op": "written", "name": "report", "nulls": {"total": 0}}
    assert _protocol.written("report", {}, checks=[]) == {"op": "written", "name": "report", "nulls": {}, "checks": []}
    assert _protocol.finish(False) == {"op": "finish", "completed": False}


def test_a_write_always_says_its_layout_and_a_path_location_is_sent_as_text(tmp_path):
    flat = _protocol.write("report", None, "parquet", "overwrite", [], _outputs.FLAT)
    assert flat == {"op": "write", "name": "report", "format": "parquet", "saveMode": "overwrite", "schema": [],
                    "partitionBy": [], "partitionOverwriteMode": "static"}
    by_region = _protocol.write("report", tmp_path / "out", "parquet", "append", [], _outputs.Layout(("region",), "dynamic"),
                                catalog={"technology": "iceberg"})
    assert (by_region["location"], by_region["partitionBy"], by_region["partitionOverwriteMode"], by_region["catalog"]) == (
        str(tmp_path / "out"), ["region"], "dynamic", {"technology": "iceberg"})
    assert _protocol.unverifiable_write("report", "no stream") == {"op": "write", "name": "report", "unverifiable": "no stream"}


@pytest.mark.parametrize("request_", [
    _protocol.open_run({"contract": "c.yaml", "runtimeDataQuality": "true"}, {"name": "j", "libraries": {}}, check=True),
    _protocol.read("orders", "in", "csv", [{"name": "id", "type": "int64", "nullable": True, "nulls": 0}],
                   {"technology": "jdbc", "catalogName": "db", "namespace": ["public"], "table": "orders"}, {"header": "false"}),
    _protocol.write("report", None, "parquet", "overwrite", [{"name": "id", "type": "int64", "nullable": False}],
                    _outputs.Layout(("id",), "dynamic")),
    _protocol.unverifiable_write("report", "no stream", "out"),
    _protocol.written("report", {"id": 0}, [{"failed": 1, "examples": ["-3"]}]),
    _protocol.finish(True),
    _protocol.scan("start", True, "invaract-lint 0.1.0", ["job.py"], [], [("broken.py", "invalid syntax")], 3),
])
def test_every_request_the_package_builds_is_one_the_schema_allows(request_):
    assert protocol_errors(request_, "Request") == []


# the schema, against the checker


def test_the_schema_is_the_one_the_package_names_and_lists_every_setting_and_csv_option():
    schema = _schema()
    assert schema["$id"] == _protocol.SCHEMA
    assert schema["$defs"]["OpenRequest"]["properties"]["protocol"]["const"] == _protocol.VERSION
    assert set(schema["$defs"]["Settings"]["properties"]) == set(_settings.SETTINGS)
    assert set(schema["$defs"]["CsvOptions"]["properties"]) == set(_csv.NAMES)


def test_every_recorded_case_has_a_reply_for_each_request():
    assert len(_cases()) >= 60
    for case in _cases():
        requests = _json_lines(os.path.join(CASES, case, "requests.jsonl"))
        replies = _json_lines(os.path.join(CASES, case, "replies.jsonl"))
        assert len(requests) == len(replies), case


@pytest.mark.parametrize("case", _cases())
def test_every_recorded_reply_is_one_the_schema_allows(case):
    requests = _json_lines(os.path.join(CASES, case, "requests.jsonl"))
    replies = _json_lines(os.path.join(CASES, case, "replies.jsonl"))
    for i, (request, reply) in enumerate(zip(requests, replies), 1):
        op = request.get("op") if isinstance(request, dict) else None
        if op in ("open", "read", "write", "written", "scan", "finish", "published"):
            assert reply_errors(op, reply) == [], f"{case}: reply {i}"
        else:
            # a request with no op the checker knows can only be answered not-checked
            assert protocol_errors(reply, "NotCheckedReply") == [], f"{case}: reply {i}"


@pytest.mark.parametrize("case", _cases())
def test_a_request_the_schema_rejects_is_one_the_checker_does_not_check(case):
    """The other way round (what the checker cannot read, the schema rejects) is ``test_the_schema_rejects_what_the_checker_
    cannot_read``: a not-checked reply can also be about order (a write before open) or a dry run's rules, which no schema
    of a single message can say."""
    requests = _json_lines(os.path.join(CASES, case, "requests.jsonl"))
    replies = _json_lines(os.path.join(CASES, case, "replies.jsonl"))
    for i, (request, reply) in enumerate(zip(requests, replies), 1):
        allowed = request is not None and protocol_errors(request, "Request") == []
        if (case, i) in TOLERATED:
            assert not allowed and reply.get("status") != "not-checked", f"{case}: request {i} is no longer tolerated: {TOLERATED[case, i]}"
        elif not allowed:
            # malformed, or out of the protocol: the checker must not have checked it either
            assert reply.get("status") == "not-checked", f"{case}: request {i} breaks the schema, yet the checker answered {reply}"


def test_the_schema_rejects_malformed_requests_and_replies():
    malformed = [
        ({"op": "launch"}, "an unknown op"),
        ({"op": "read", "schema": []}, "a read with no name"),
        ({"op": "write", "name": "r", "schema": [{"name": "total"}]}, "a column with no type"),
        ({"op": "write", "name": "r", "schema": ["total"]}, "a column that is not an object"),
        ({"op": "written", "name": "r", "nulls": {"total": -1}}, "a negative count"),
        ({"op": "written", "name": "r", "nulls": {"total": 1.5}}, "a count that is not whole"),
        ({"op": "write", "name": "r", "schema": [], "partitionOverwriteMode": "sometimes"}, "an unknown overwrite mode"),
        ({"op": "open", "protocol": 2, "settings": {}, "job": {}}, "another protocol version"),
        ({"op": "open", "protocol": 1, "settings": {"contrat": "c.yaml"}, "job": {}}, "an unknown setting"),
        ({"op": "read", "name": "o", "csv": {"sep": ";"}}, "a CSV option by another name than the contract's"),
        ({"op": "read", "name": "o", "nulls": 0}, "a field no read has"),
    ]
    for message, what in malformed:
        assert protocol_errors(message, "Request") != [], what
    assert protocol_errors({"ok": True, "publish": "yes"}, "FinishReply") != []
    assert protocol_errors({"ok": False, "status": "refused", "violations": []}, "RefusedReply") != []

