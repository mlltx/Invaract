# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The example job of examples/orders_job (the sink records survey's probe 14, made permanent): a job over three files, and
what happens to it when each of its pieces has a bug."""
import importlib
import os
import shutil
import subprocess
import sys

import pyarrow.parquet as pq
import pytest

import invaract
from conftest import PYTHON

EXAMPLE = os.path.join(PYTHON, "examples", "orders_job")
MODULES = ("load", "build", "main")


@pytest.fixture
def job(tmp_path, monkeypatch):
    """The example copied to its own folder and imported from there, as ``python main.py`` would import it."""
    folder = tmp_path / "orders_job"
    shutil.copytree(EXAMPLE, folder)
    monkeypatch.chdir(folder)
    monkeypatch.syspath_prepend(str(folder))
    for name in MODULES:
        sys.modules.pop(name, None)
    yield type("Job", (), {name: importlib.import_module(name) for name in MODULES})
    for name in MODULES:
        sys.modules.pop(name, None)


def schema(location):
    return [(f.name, str(f.type), f.nullable) for f in pq.read_schema(os.path.join(location, os.listdir(location)[0]))]


def test_the_correct_job_publishes_both_outputs_exactly_as_the_contract_declares_them(job):
    done = subprocess.run([sys.executable, "main.py"], env={**os.environ, "PYTHONPATH": PYTHON}, stderr=subprocess.PIPE, text=True)
    assert done.returncode == 0, done.stderr
    # pandas 3 holds text as large_string, pandas 2 as string: either is the contract's string
    assert schema("data/out/report") in ([("region", t, False), ("total", "int64", False)] for t in ("string", "large_string"))
    assert [(n, nullable) for n, _, nullable in schema("data/out/audit")] == [("order_id", False), ("status", True)]
    report = pq.read_table("data/out/report").to_pydict()
    assert sorted(zip(report["region"], report["total"])) == [("east", 40), ("north", 10), ("south", 20)]
    assert sorted(pq.read_table("data/out/audit").column("order_id").to_pylist()) == [1, 2, 4]


def refused(job):
    with pytest.raises(invaract.ContractViolation) as e:
        job.main.main()
    assert not os.path.exists("data/out") or os.listdir("data/out") == [], "nothing published, nothing left staged"
    return e.value


def test_a_bug_in_the_report_is_refused_at_its_write(job, monkeypatch):
    build_report = job.build.report
    monkeypatch.setattr(job.build, "report", lambda *a: build_report(*a).astype({"total": str}))
    assert refused(job).violations == ["OUTPUT_FIELD_TYPE_MISMATCH"]


def test_a_bug_in_the_audit_refuses_the_run_though_the_report_passed(job, monkeypatch):
    @invaract.writes("audit")
    def audit_without_order_id(paid):
        return paid[["status"]]

    monkeypatch.setattr(job.build, "audit", audit_without_order_id)
    violation = refused(job)
    assert violation.violations == ["MISSING_OUTPUT_FIELD"] and "order_id" in str(violation)


def test_a_main_that_forgets_the_audit_is_refused_at_the_runs_end(job, monkeypatch):
    monkeypatch.setattr(job.build, "audit", lambda paid: None)
    violation = refused(job)
    assert violation.violations == ["MISSING_OUTPUT"] and "audit" in str(violation)


def test_a_main_that_writes_somewhere_undeclared_is_refused(job, monkeypatch):
    real_report = job.build.report

    def report_and_dump(paid, customers):
        invaract.write(paid, "debug_dump")
        return real_report(paid, customers)

    monkeypatch.setattr(job.build, "report", report_and_dump)
    assert refused(job).violations == ["OUTPUT_LOCATION_MISMATCH"]
