# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Every test runs a real checker: ``INVARACT_TEST_CHECKER`` (a command: the native program, or ``java -jar <jar>``), else
the jar ``sbt assembly`` builds in this module. With neither, the tests fail rather than pass without checking anything."""
import json
import os
import shlex
import shutil
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.dirname(HERE)
JAR = os.path.join(PYTHON, os.pardir, "target", "scala-2.12", "invaract-pythonio-checker-0.1.0.jar")

sys.path.insert(0, PYTHON)


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
    """The events the file sink wrote, as ``(type, status)`` pairs, and the events themselves."""
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def notify_to_file(monkeypatch, path="events.jsonl"):
    with open("notify.properties", "w", encoding="utf-8") as f:
        f.write(f"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path={os.path.abspath(path)}\n")
    monkeypatch.setenv("INVARACT_NOTIFY_CONFIG", os.path.abspath("notify.properties"))
