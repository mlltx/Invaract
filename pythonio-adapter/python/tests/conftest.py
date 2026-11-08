# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Every test runs a real checker: ``INVARACT_TEST_CHECKER`` (a command: the native program, or ``java -jar <jar>``), else
the jar ``sbt assembly`` builds in this module. With neither, the tests fail rather than pass without checking anything.

Every request the package sends and every reply it gets back, in every test, is validated against the protocol's JSON Schema
(``protocol_schema``), and every event a test reads (``events``) against the notification events' schema: both are the
published files, so the package, the checker and what the docs publish cannot drift apart."""
import json
import os
import shlex
import shutil
import sys

import jsonschema
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.dirname(HERE)
JAR = os.path.join(PYTHON, os.pardir, "target", "scala-2.12", "invaract-pythonio-checker-0.1.0.jar")
#: The JSON Schemas the docs publish.
SCHEMAS = os.path.join(PYTHON, os.pardir, os.pardir, "docs-site", "public", "schemas")
PROTOCOL_SCHEMA = os.path.join(SCHEMAS, "pythonio", "v1", "protocol.schema.json")
EVENT_SCHEMA = os.path.join(SCHEMAS, "notification", "v1", "event.schema.json")

sys.path.insert(0, PYTHON)

from invaract import _checker  # noqa: E402  (after the path is set)

#: ``Checker.ask`` itself, for a test of a checker that breaks the protocol on purpose (``protocol_checked`` fails any other).
UNCHECKED_ASK = _checker.Checker.ask

#: The replies each request may get, by its ``op``: its own, or a refusal, or not checked.
REPLIES = {
    "open": "OpenReply", "read": "OkReply", "write": "OkReply", "written": "OkReply", "finish": "FinishReply", "published": "OkReply",
}


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def protocol_schema(definition=None):
    """A validator for the protocol schema, or for one of its definitions (``"OpenReply"``)."""
    schema = _load(PROTOCOL_SCHEMA)
    if definition is not None:
        schema = {"$schema": schema["$schema"], "$defs": schema["$defs"], "$ref": f"#/$defs/{definition}"}
    return jsonschema.Draft202012Validator(schema)


def protocol_errors(message, definition):
    """What is wrong with ``message`` as the protocol's ``definition``: nothing when it is valid."""
    return [e.message for e in protocol_schema(definition).iter_errors(message)]


def reply_errors(op, reply):
    """What is wrong with ``reply`` as the checker's answer to an ``op`` request."""
    expected = reply_definition(op, reply)
    return protocol_errors(reply, expected) if expected else [f"no reply is defined for op {op!r}"]


def reply_definition(op, reply):
    if reply.get("ok") is False:
        return "RefusedReply" if reply.get("status") == "refused" else "NotCheckedReply"
    return REPLIES.get(op)


@pytest.fixture(autouse=True)
def protocol_checked(monkeypatch):
    """Every request the package sends, and the reply it gets, held to the protocol's schema."""
    ask = UNCHECKED_ASK

    def checked(self, request):
        problems = protocol_errors(request, "Request")
        assert not problems, f"the package sent a request the protocol schema does not allow: {request}\n{problems}"
        reply = ask(self, request)
        problems = reply_errors(request["op"], reply)
        assert not problems, f"the checker's reply to {request['op']} is not one the protocol schema allows: {reply}\n{problems}"
        return reply

    monkeypatch.setattr(_checker.Checker, "ask", checked)


def checker_command():
    if os.environ.get("INVARACT_TEST_CHECKER"):
        return os.environ["INVARACT_TEST_CHECKER"]
    java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else shutil.which("java")
    if os.path.isfile(JAR) and java:
        return shlex.join([java, "-jar", os.path.abspath(JAR)])
    pytest.fail(f"no checker to test against: set INVARACT_TEST_CHECKER, or build {os.path.abspath(JAR)} (cd pythonio-adapter && sbt assembly)")


@pytest.fixture(autouse=True)
def environment(monkeypatch, tmp_path):
    """Each test in its own folder, with the checker and no other Invaract setting from the outside."""
    for name in list(os.environ):
        if name.startswith("INVARACT_") and name != "INVARACT_TEST_CHECKER":
            monkeypatch.delenv(name)
    monkeypatch.setenv("INVARACT_CHECKER", checker_command())
    monkeypatch.chdir(tmp_path)
    return tmp_path


def write_contract(path="contract.yaml", inputs="", outputs=""):
    """A contract with the given YAML list items under ``inputs:`` and ``outputs:``."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(f'id: test_contract\nversion: "1.0.0"\ninputs:\n{inputs or "  []"}\noutputs:\n{outputs or "  []"}\n')
    return path


def output(name="report", location="data/out/report", fmt="parquet", save_mode="overwrite", fields=None):
    fields = fields if fields is not None else [("region", "string", True), ("total", "long", True)]
    columns = "".join(f"        - {{ name: {n}, type: {t}, required: {str(r).lower()} }}\n" for n, t, r in fields)
    mode = f"    saveMode: {save_mode}\n" if save_mode else ""
    return (f"  - name: {name}\n    type: DATA_ASSET\n    location: {location}\n    format: {fmt}\n{mode}"
            f"    schema:\n      fields:\n{columns}")


def events(path="events.jsonl"):
    """The events the file sink wrote, each validated against the published notification events' schema."""
    with open(path, encoding="utf-8") as f:
        written = [json.loads(line) for line in f if line.strip()]
    validator = jsonschema.Draft202012Validator(_load(EVENT_SCHEMA))
    for event in written:
        problems = [e.message for e in validator.iter_errors(event)]
        assert not problems, f"an event the checker published breaks {EVENT_SCHEMA}: {problems}\n{event}"
    return written


def event_of(kind, path="events.jsonl"):
    """The one event of type ``kind`` the file sink wrote (``DRY_RUN_SUMMARY``, ``JOB_SUMMARY``)."""
    (found,) = [e for e in events(path) if e["eventType"] == kind]
    return found


def notify_to_file(monkeypatch, path="events.jsonl"):
    with open("notify.properties", "w", encoding="utf-8") as f:
        f.write(f"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path={os.path.abspath(path)}\n")
    monkeypatch.setenv("INVARACT_NOTIFY_CONFIG", os.path.abspath("notify.properties"))
