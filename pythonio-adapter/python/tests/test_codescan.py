# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Scanning the job's own code when a run starts (INVARACT_LINT): what is scanned, what the CODE_SCAN event says, and when
the run is refused (UNGOVERNED_DATA_ACCESS) or not checked. In-process tests name the code to scan (INVARACT_LINT_PATHS),
since a test's own process has loaded the test suite; the jobs run as their own processes show which code a run finds by
itself, and code a job imports while it runs."""
import json
import os
import subprocess
import sys
import textwrap

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
import invaract_lint
from invaract import _codescan
from conftest import LINT_SOURCE, PYTHON, event_of, events, notify_to_file, output, write_contract

REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})
SCANNER = f"invaract-lint {invaract_lint.__version__}"

#: A job's code: one write around the package, and one read a marker keeps.
JOB = {
    "jobs/helpers.py": """\
        import pandas as pd


        def lookup():
            return pd.read_csv("lookups/regions.csv")  # invaract: allow - a lookup table shipped with the job


        def backup(df):
            df.to_parquet("backup/report.parquet")
        """,
    "jobs/clean.py": """\
        import invaract


        def run():
            invaract.write(invaract.read("orders"), "report")
        """,
}


def files(tree):
    for name, text in tree.items():
        os.makedirs(os.path.dirname(name) or ".", exist_ok=True)
        with open(name, "w", encoding="utf-8") as f:
            f.write(textwrap.dedent(text))


@pytest.fixture
def scanned(monkeypatch):
    """A contract with one output, events to a file, and the scan set to ``mode`` over ``paths``; returns a function that
    runs a job writing the report, and says whether its body ran."""
    write_contract(outputs=output())
    notify_to_file(monkeypatch)
    files(JOB)

    def run(mode, paths="jobs", exclude=None):
        monkeypatch.setenv("INVARACT_LINT", mode)
        monkeypatch.setenv("INVARACT_LINT_PATHS", paths)
        if exclude:
            monkeypatch.setenv("INVARACT_LINT_EXCLUDE", exclude)
        ran = []
        with invaract.run("contract.yaml"):
            ran.append(True)
            invaract.write(REPORT, "report")
        return ran

    return run


def test_off_by_default_nothing_is_scanned(scanned):
    scanned("off")
    assert [e["eventType"] for e in events()] == ["CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY"]
    assert os.path.isdir("data/out/report")


def test_report_publishes_every_finding_and_the_run_goes_on(scanned):
    scanned("report")
    assert os.path.isdir("data/out/report")
    scan = event_of("CODE_SCAN")
    assert (scan["status"], scan["phase"], scan["mode"], scan["refused"]) == ("FINDINGS", "START", "REPORT", False)
    assert scan["scanner"] == SCANNER and scan["contract"] == "test_contract@1.0.0"
    assert scan["files"] == [os.path.join("jobs", "clean.py"), os.path.join("jobs", "helpers.py")]
    assert scan["counts"] == {"read": {"findings": 0, "exceptions": 1}, "write": {"findings": 1, "exceptions": 0},
                              "connect": {"findings": 0, "exceptions": 0}, "sql": {"findings": 0, "exceptions": 0}}
    assert (scan["findingsOmitted"], scan["errors"]) == (0, [])
    found, kept = scan["findings"]  # the call not explained first
    assert (found["path"], found["line"], found["column"], found["scope"], found["code"], found["rule"], found["target"]) == (
        os.path.join("jobs", "helpers.py"), 9, 5, "backup", "IVR002", "frame-write", "backup/report.parquet")
    assert (found["allowed"], found["reason"], found["snippet"]) == (False, None, "df.to_parquet('backup/report.parquet')")
    assert (kept["line"], kept["scope"], kept["resolved"], kept["allowed"], kept["reason"]) == (
        5, "lookup", "pandas.read_csv", True, "a lookup table shipped with the job")
    assert found["id"] == invaract_lint.scan_file(os.path.join("jobs", "helpers.py"))[1].id
    assert scan["job"]["engine"] == "pythonio"
    assert [e["eventType"] for e in events()] == ["CODE_SCAN", "CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY"]


def test_enforce_refuses_the_run_before_the_job_runs_for_each_call_not_explained(scanned):
    with pytest.raises(invaract.ContractViolation) as refused:
        ran = scanned("enforce")
    assert refused.value.violations == ["UNGOVERNED_DATA_ACCESS"]
    explanation = str(refused.value)
    assert f"`df.to_parquet` in backup ({os.path.join('jobs', 'helpers.py')}:9:5) writes data outside Invaract" in explanation
    assert "# invaract: allow - <why>" in explanation
    assert "pd.read_csv" not in explanation.split("Why it violates")[1], "a call a marker keeps is never a violation"
    assert not os.path.exists("data/out/report")
    scan = event_of("CODE_SCAN")
    assert (scan["status"], scan["mode"], scan["refused"]) == ("FINDINGS", "ENFORCE", True)
    validation = event_of("CONTRACT_VALIDATION")
    assert validation["status"] == "FAILED" and validation["violations"][0]["rule"] == "IVR002"
    assert validation["violations"][0]["actual"] == f"{os.path.join('jobs', 'helpers.py')}:9:5 df.to_parquet('backup/report.parquet')"


def test_the_job_body_does_not_run_when_the_scan_refuses_it(scanned, monkeypatch):
    write_contract(outputs=output())
    monkeypatch.setenv("INVARACT_LINT", "enforce")
    monkeypatch.setenv("INVARACT_LINT_PATHS", "jobs")
    ran = []
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            ran.append(True)
    assert ran == []


def test_enforce_lets_a_job_whose_only_findings_are_explained_run(scanned):
    files({"jobs/helpers.py": "import pandas as pd\npd.read_csv('a.csv')  # invaract: allow - a lookup\n"})
    assert scanned("enforce") == [True]
    assert os.path.isdir("data/out/report")
    scan = event_of("CODE_SCAN")
    assert (scan["status"], scan["refused"]) == ("EXCEPTIONS", False)


def test_enforce_lets_a_clean_job_run(scanned):
    assert scanned("enforce", paths=os.path.join("jobs", "clean.py")) == [True]
    assert event_of("CODE_SCAN")["status"] == "CLEAN" and event_of("CODE_SCAN")["files"] == [os.path.join("jobs", "clean.py")]


def test_excluded_files_are_not_scanned(scanned):
    scanned("enforce", exclude="helpers.py")
    assert event_of("CODE_SCAN")["files"] == [os.path.join("jobs", "clean.py")]


def test_several_paths_are_comma_separated(scanned):
    files({"other/x.py": "df.to_csv('x')\n"})
    with pytest.raises(invaract.ContractViolation):
        scanned("enforce", paths=f"{os.path.join('jobs', 'clean.py')}, other")
    assert event_of("CODE_SCAN")["files"] == [os.path.join("jobs", "clean.py"), os.path.join("other", "x.py")]


def test_a_file_that_cannot_be_parsed_leaves_an_enforced_run_not_checked(scanned):
    files({"jobs/broken.py": "def (:\n"})
    with pytest.raises(invaract.CheckNotRun) as not_checked:
        scanned("enforce", paths=os.path.join("jobs", "broken.py"))
    assert "could not all be scanned" in str(not_checked.value) and "broken.py" in str(not_checked.value)
    scan = event_of("CODE_SCAN")
    assert (scan["status"], scan["refused"], scan["files"]) == ("NOT_SCANNED", True, [])
    assert [e["path"] for e in scan["errors"]] == [os.path.join("jobs", "broken.py")]


def test_a_file_that_cannot_be_parsed_is_reported_and_the_run_goes_on(scanned):
    files({"jobs/broken.py": "def (:\n"})
    scanned("report")
    scan = event_of("CODE_SCAN")
    assert scan["status"] == "FINDINGS" and [e["path"] for e in scan["errors"]] == [os.path.join("jobs", "broken.py")]
    assert os.path.isdir("data/out/report")


def test_nothing_to_scan_leaves_an_enforced_run_not_checked(scanned):
    os.makedirs("empty")
    with pytest.raises(invaract.CheckNotRun) as not_checked:
        scanned("enforce", paths="empty")
    assert "no file of the job's own code was found to scan" in str(not_checked.value)
    assert event_of("CODE_SCAN")["status"] == "NOT_SCANNED"


def test_more_than_fifty_calls_are_refused_fifty_by_name_and_the_rest_counted(scanned):
    files({"jobs/many.py": "".join(f"df.to_csv('out{i}.csv')\n" for i in range(53))})
    with pytest.raises(invaract.ContractViolation) as refused:
        scanned("enforce", paths=os.path.join("jobs", "many.py"))
    assert refused.value.violations == ["UNGOVERNED_DATA_ACCESS"] * 51
    assert "and 3 more call(s) in the job's code that read or write data outside Invaract" in str(refused.value)
    assert len(event_of("CODE_SCAN")["findings"]) == 53


def test_a_dry_run_reports_even_when_enforcing(monkeypatch):
    notify_to_file(monkeypatch)
    files(JOB)
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    monkeypatch.setenv("INVARACT_LINT", "enforce")
    monkeypatch.setenv("INVARACT_LINT_PATHS", "jobs")
    with invaract.run():
        invaract.write(REPORT, "report", location="data/out/report")
    scan = event_of("CODE_SCAN")
    assert (scan["status"], scan["mode"], scan["refused"], scan["contract"]) == ("FINDINGS", "ENFORCE", False, None)


@pytest.mark.parametrize("value", ["Enforce", " REPORT ", "off", ""])
def test_the_mode_is_read_without_regard_to_case_or_spaces(value, monkeypatch):
    monkeypatch.setenv("INVARACT_LINT", value)
    scanner = _codescan.from_environment()
    assert (scanner.enforce if scanner else None) == {"Enforce": True, " REPORT ": False}.get(value)


def test_any_other_mode_is_not_checked(monkeypatch):
    report_contract = write_contract(outputs=output())
    monkeypatch.setenv("INVARACT_LINT", "strict")
    with pytest.raises(invaract.CheckNotRun) as not_checked:
        with invaract.run(report_contract):
            pass
    assert str(not_checked.value) == "INVARACT_LINT must be off, report or enforce, not 'strict'"


def test_a_scan_asked_for_without_invaract_lint_installed_is_not_checked(monkeypatch):
    monkeypatch.setitem(sys.modules, "invaract_lint", None)
    monkeypatch.setenv("INVARACT_LINT", "report")
    with pytest.raises(invaract.CheckNotRun) as not_checked:
        _codescan.from_environment()
    assert "invaract-lint is not installed" in str(not_checked.value) and "invaract[lint]" in str(not_checked.value)


def test_a_check_never_scans(monkeypatch):
    write_contract(outputs=output())
    files(JOB)
    monkeypatch.setenv("INVARACT_LINT", "enforce")
    monkeypatch.setenv("INVARACT_LINT_PATHS", "jobs")
    invaract.check(REPORT, "report", contract="contract.yaml")


def test_job_files_are_the_jobs_own_modules_not_pythons_nor_installed_packages():
    found = _codescan.job_files()
    assert os.path.realpath(__file__) in found, "this test module is the code of the process it runs in"
    assert not any(f.endswith(os.path.join("json", "__init__.py")) for f in found), "Python's own library"
    assert not any(os.path.basename(os.path.dirname(f)) == "pyarrow" for f in found), "an installed package"
    assert not any(f.startswith(os.path.realpath(os.path.dirname(invaract.__file__)) + os.sep) for f in found), "this package"
    assert not any(f.startswith(os.path.realpath(LINT_SOURCE) + os.sep) for f in found), "invaract-lint"
    assert found == sorted(found)


def test_a_file_inside_the_working_folder_is_named_relative_to_it_and_one_outside_as_it_is(tmp_path):
    assert _codescan._shown(os.path.join(os.getcwd(), "jobs", "a.py")) == os.path.join("jobs", "a.py")
    outside = os.path.realpath(os.path.join(PYTHON, "invaract", "_run.py"))
    assert _codescan._shown(outside) == outside


# whole jobs, each in a process of its own


def _job_process(tmp_path, mode, main_body, helpers=JOB["jobs/helpers.py"]):
    """Runs ``python -m jobs.main`` in ``tmp_path`` with the scan in ``mode``: the job imports ``jobs.helpers`` (``helpers``),
    and reads ``orders`` and writes the report after ``main_body``. Returns the process and the events it published."""
    pq.write_table(pa.table({"region": ["north"], "total": [1]}), "orders.parquet")
    write_contract(inputs="  - { name: orders, location: orders.parquet, format: parquet }\n", outputs=output())
    files({**JOB, "jobs/helpers.py": helpers, "jobs/__init__.py": "", "jobs/late.py": "def save(df):\n    df.to_json('late.json')\n",
           "jobs/main.py": f"""\
            import invaract
            from jobs import helpers


            @invaract.job("contract.yaml")
            def main():
            {textwrap.indent(textwrap.dedent(main_body), '    ')}
                invaract.write(invaract.read("orders"), "report")


            if __name__ == "__main__":
                main()
            """})
    with open("notify.properties", "w", encoding="utf-8") as f:
        f.write(f"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\n"
                f"sink.property.path={tmp_path / 'events.jsonl'}\n")
    env = {**os.environ, "INVARACT_LINT": mode, "INVARACT_NOTIFY_CONFIG": str(tmp_path / "notify.properties"),
           "PYTHONPATH": os.pathsep.join([PYTHON, LINT_SOURCE])}
    env.pop("INVARACT_LINT_PATHS", None)
    done = subprocess.run([sys.executable, "-m", "jobs.main"], env=env, cwd=tmp_path, capture_output=True, text=True)
    return done, [json.loads(line) for line in open(tmp_path / "events.jsonl", encoding="utf-8")]


def test_a_job_run_on_its_own_scans_the_modules_it_loaded(tmp_path):
    done, published = _job_process(tmp_path, "enforce", "pass")
    assert done.returncode == 1 and "UNGOVERNED_DATA_ACCESS" in done.stderr
    [scan] = [e for e in published if e["eventType"] == "CODE_SCAN"]
    # its own modules, named from where it runs; not Python's start-up hook, nor the package or the lint
    assert scan["files"] == [os.path.join("jobs", "__init__.py"), os.path.join("jobs", "helpers.py"), os.path.join("jobs", "main.py")]
    assert not (tmp_path / "data" / "out" / "report").exists()


def test_code_a_job_imports_while_it_runs_is_scanned_before_anything_is_published(tmp_path):
    late = """\
        from jobs import late
        """
    done, published = _job_process(tmp_path, "report", late)
    assert done.returncode == 0, done.stderr
    start, finish = [e for e in published if e["eventType"] == "CODE_SCAN"]
    assert start["phase"] == "START" and os.path.join("jobs", "late.py") not in start["files"]
    assert (finish["phase"], finish["files"]) == ("FINISH", [os.path.join("jobs", "late.py")])
    assert [(f["scope"], f["target"]) for f in finish["findings"]] == [("save", "late.json")]


def test_enforced_code_a_job_imports_while_it_runs_refuses_the_run_and_nothing_is_published(tmp_path):
    done, published = _job_process(tmp_path, "enforce", "from jobs import late", helpers="def lookup():\n    return None\n")
    assert done.returncode == 1 and "`df.to_json` in save" in done.stderr
    assert [(e["phase"], e["status"], e["refused"]) for e in published if e["eventType"] == "CODE_SCAN"] == [
        ("START", "CLEAN", False), ("FINISH", "FINDINGS", True)]
    assert not (tmp_path / "data" / "out" / "report").exists()
    assert [e["eventType"] for e in published if e["eventType"] in ("WRITE", "CONTRACT_VALIDATION")] == [
        "CONTRACT_VALIDATION", "CONTRACT_VALIDATION"], "the write passed its own check; the late scan refused the run"
