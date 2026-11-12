# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Strict mode (INVARACT_STRICT): what a job does around the package is seen as it runs, placed in the job's code, reported
as a CODE_SCAN event of phase RUNTIME and, enforced, stopped before it happens.

Every job here is a real one, run as its own process against a real checker, in a folder that is not the temporary folder
(what is written there is scratch, and no finding). ``strict_matrix.TECHNIQUES`` are the ways of writing around the
package: one job attempts them all with strict mode and the scan reporting, and one with strict mode enforcing; each
technique is then held, by name, to what each layer says of it and to whether its write happened. ``LEGITIMATE`` is what
a job does that must raise nothing."""
import json
import os
import subprocess
import sys
import textwrap

import pytest

from conftest import LINT_SOURCE, PYTHON, checker_command, events
from strict_matrix import BY_NAME, HELPER, LEGIT_HELPER, LEGITIMATE, LEGITIMATE_WITH, TECHNIQUES, Technique

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the jobs use POSIX programs and fork")

CONTRACT = """\
id: orders_report
version: "1.0.0"
inputs:
  - { name: orders, location: data/in/orders, format: parquet }
outputs:
  - name: report
    location: data/out/report
    format: parquet
    saveMode: overwrite
    schema:
      fields:
        - { name: region, type: string, required: true }
        - { name: total,  type: long,   required: true }
"""

JOB = '''\
import json, os, sys
{before}
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import invaract

df = pd.DataFrame({{"region": ["north", "south"], "total": [10, 20]}})
table = pa.table({{"region": ["north", "south"], "total": [10, 20]}})
scratch = os.environ["TMPDIR"]
OUTCOME = {{}}

{functions}

def attempt(name, function):
    try:
        function("evade/" + name + ".out", "seed/" + name + ".txt")
        OUTCOME[name] = "ran"
    except BaseException as e:
        OUTCOME[name] = type(e).__name__ + ": " + str(e)[:300]


if __name__ == "__main__":
    try:
        with invaract.run("contract.yaml"):
{governed}
{calls}
{after}
    finally:
        with open(os.path.join(scratch, "outcome.json"), "w") as f:  # invaract: allow - the test's own record of what ran
            json.dump(OUTCOME, f)
'''


class Ran:
    """What a job did: how it ended, what it printed, its events, and what each of its functions came to."""

    def __init__(self, folder, process, scratch):
        self.folder, self.status, self.stderr = folder, process.returncode, process.stderr
        path = os.path.join(folder, "events.jsonl")
        self.events = events(path) if os.path.exists(path) else []
        outcome = os.path.join(scratch, "outcome.json")
        self.outcome = json.load(open(outcome)) if os.path.exists(outcome) else {}

    def scans(self, *phases):
        return [e for e in self.events if e["eventType"] == "CODE_SCAN" and (not phases or e["phase"] in phases)]

    def findings(self, *phases):
        return [f for e in self.scans(*phases) for f in e["findings"]]

    def exists(self, path):
        return os.path.lexists(os.path.join(self.folder, path))

    def published(self):
        return self.exists("data/out/report")


GOVERNED = 'invaract.write(invaract.read("orders"), "report")'


def run_job(root, functions, before=(), after="", strict=None, lint=None, allow=None, extra_env=None, files=None, calls=None,
            dry_run=False, governed=GOVERNED):
    """Runs a job whose functions are ``functions`` (name -> body), each called once inside the run with its own ``p`` and
    ``seed``. ``root`` is a fresh folder: the job runs in ``root/job``, and its temporary folder is ``root/scratch``."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    folder, scratch = os.path.join(root, "job"), os.path.join(root, "scratch")
    for made in (folder, scratch, os.path.join(folder, "evade"), os.path.join(folder, "seed"), os.path.join(folder, "data/in/orders")):
        os.makedirs(made)
    pq.write_table(pa.table({"region": ["north", "south"], "total": [10, 20]}), os.path.join(folder, "data/in/orders/part-0.parquet"))
    for name in functions:
        with open(os.path.join(folder, f"seed/{name}.txt"), "w") as f:
            f.write("seed\n")
        os.makedirs(os.path.join(folder, f"seed/{name}.txt.d"))
        open(os.path.join(folder, f"seed/{name}.txt.d/kept.txt"), "w").close()
    written = {"contract.yaml": CONTRACT, "evade_helper.py": HELPER, "legit_helper.py": LEGIT_HELPER,
               "notify.properties": "sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\n"
                                    f"sink.property.path={os.path.join(folder, 'events.jsonl')}\n", **(files or {})}
    source = JOB.format(
        before="\n".join(before),
        functions="\n\n".join(f"def {function_name(name)}(p, seed):\n{textwrap.indent(body, '    ')}" for name, body in functions.items()),
        governed=textwrap.indent(governed, "            "),
        calls=calls if calls is not None else "\n".join(f"            attempt({name!r}, {function_name(name)})" for name in functions),
        after=textwrap.indent(after, "        "))
    written["job.py"] = source
    for name, text in written.items():
        with open(os.path.join(folder, name), "w", encoding="utf-8") as f:
            f.write(text)
    env = {k: v for k, v in os.environ.items() if not k.startswith("INVARACT_")}
    env.update({"PYTHONPATH": os.pathsep.join([PYTHON, LINT_SOURCE]), "INVARACT_CHECKER": checker_command(),
                "INVARACT_NOTIFY_CONFIG": os.path.join(folder, "notify.properties"), "TMPDIR": scratch, "PYTHONDONTWRITEBYTECODE": ""})
    if dry_run:
        env["INVARACT_DRY_RUN"] = "true"
    for variable, value in (("INVARACT_STRICT", strict), ("INVARACT_LINT", lint), ("INVARACT_STRICT_ALLOW", allow)):
        if value is not None:
            env[variable] = value
    env.update(extra_env or {})
    process = subprocess.run([sys.executable, "job.py"], cwd=folder, env=env, capture_output=True, text=True, timeout=600)
    return Ran(folder, process, scratch)


def function_name(name):
    return "t_" + "".join(c if c.isalnum() else "_" for c in name)


# --- every technique, as one job per mode ------------------------------------------------------------------------------------

AVAILABLE = [t for t in TECHNIQUES if t.available]


def matrix(root, **modes):
    before = [t.before for t in AVAILABLE if t.before]
    return run_job(root, {t.name: t.body for t in AVAILABLE}, before=before, **modes)


@pytest.fixture(scope="module")
def reported(tmp_path_factory):
    """Every technique attempted in one job, with strict mode and the scan both reporting."""
    return matrix(str(tmp_path_factory.mktemp("reported")), strict="report", lint="report")


@pytest.fixture(scope="module")
def enforced(tmp_path_factory):
    """Every technique attempted in one job, with strict mode enforcing."""
    return matrix(str(tmp_path_factory.mktemp("enforced")), strict="enforce")


def of(technique, findings):
    """The findings that are ``technique``'s: in its function (or one inside it), in its module-level lines, naming its own
    path, or, for one that works through the helper module, in that module."""
    function, path = function_name(technique.name), f"evade/{technique.name}.out"

    def its(f):
        text = " ".join(str(f.get(k) or "") for k in ("target", "detail", "snippet"))
        if f["scope"] == function or f["scope"].startswith(function + ".") or path in text:
            return True
        if f["path"] == "evade_helper.py" and "evade_helper" in technique.body:
            return True
        return bool(technique.before) and f["path"] == "job.py" and f["scope"] == "<module>" and any(
            line.strip() and line.strip() in f["snippet"] or f["snippet"] in line for line in technique.before.splitlines())

    return [f for f in findings if its(f)]


def happened(technique, ran):
    """Whether what the technique does shows in the job's folder: a file under its own path (a folder with nothing in it is
    no data), or its seed gone or changed."""
    name, evade = technique.name, os.path.join(ran.folder, "evade")
    made = []
    for entry in os.listdir(evade):
        if entry.startswith(name + ".out"):
            full = os.path.join(evade, entry)
            made += [full] if not os.path.isdir(full) or os.path.islink(full) else [f for _, _, files in os.walk(full) for f in files]
    seed = os.path.join(ran.folder, f"seed/{name}.txt")
    unchanged = os.path.isfile(seed) and open(seed, "rb").read() == b"seed\n" and os.path.isfile(os.path.join(seed + ".d", "kept.txt"))
    return bool(made) or not unchanged


def test_the_job_that_reports_runs_to_its_end_and_publishes(reported):
    assert reported.status == 0, reported.stderr[-3000:]
    assert reported.published()
    [watched] = reported.scans("RUNTIME")
    assert (watched["status"], watched["mode"], watched["refused"], watched["files"], watched["errors"]) == ("FINDINGS", "REPORT", False, [], [])
    assert watched["scanner"].startswith("invaract strict ")


def test_the_job_that_enforces_is_refused_and_publishes_nothing(enforced):
    assert enforced.status == 1, enforced.stderr[-3000:]
    assert "UNGOVERNED_DATA_ACCESS" in enforced.stderr and "invaract.errors.ContractViolation" in enforced.stderr
    assert not enforced.published()
    refusals = [e for e in enforced.scans("RUNTIME") if e["refused"]]
    assert refusals and all((e["mode"], e["status"]) == ("ENFORCE", "FINDINGS") for e in refusals)


@pytest.mark.parametrize("technique", TECHNIQUES, ids=lambda t: t.name)
def test_no_technique_gets_past_both_the_scan_and_strict_mode(technique):
    assert technique.rules or technique.static, f"{technique.name} is seen by nothing"
    assert technique.rules or technique.why, f"{technique.name}: say why strict mode cannot see it"


@pytest.mark.parametrize("technique", TECHNIQUES, ids=lambda t: t.name)
def test_strict_mode_sees_the_write_as_it_happens(technique, reported):
    if not technique.available:
        pytest.skip(f"{technique.needs} is not installed")
    found = of(technique, reported.findings("RUNTIME"))
    rules = sorted({f["rule"] for f in found})
    if technique.runtime is None:
        assert rules == [], f"strict mode now sees {technique.name}: say so in strict_matrix ({rules})"
        return
    assert set(technique.rules) & set(rules), (rules, reported.outcome.get(technique.name), reported.stderr[-1500:])
    for f in found:
        assert f["detail"].startswith("seen as the job ran: ") and not f["allowed"]
        assert f["code"].startswith("IVR00") and f["category"] in ("write", "connect", "sql", "process")


@pytest.mark.parametrize("technique", TECHNIQUES, ids=lambda t: t.name)
def test_the_scan_sees_it_in_the_source(technique, reported):
    if not technique.available:
        pytest.skip(f"{technique.needs} is not installed")
    rules = sorted({f["rule"] for f in of(technique, reported.findings("START", "FINISH"))})
    if technique.static is None:
        assert rules == [], f"the scan now sees {technique.name}: say so in strict_matrix ({rules})"
    else:
        assert technique.static in rules, rules


@pytest.mark.parametrize("technique", [t for t in TECHNIQUES if t.effect], ids=lambda t: t.name)
def test_reported_the_write_happens_and_enforced_it_does_not(technique, reported, enforced):
    if not technique.available:
        pytest.skip(f"{technique.needs} is not installed")
    assert happened(technique, reported), f"{technique.name} did not do what it is here to do: {reported.outcome.get(technique.name)}"
    if technique.stopped:
        assert not happened(technique, enforced), f"enforcing did not stop {technique.name}: {enforced.outcome.get(technique.name)}"
    else:
        assert happened(technique, enforced), f"enforcing now stops {technique.name}: say so in strict_matrix"


def test_every_finding_is_placed_in_the_jobs_code_or_said_to_be_outside_it(reported):
    for f in reported.findings("RUNTIME"):
        if f["path"] == "<not in the job's code>":
            assert f["line"] == 0 and f["scope"].startswith("<thread ") and "with none of the job's code calling it" in f["detail"]
        else:
            assert f["path"] in ("job.py", "evade_helper.py") and f["line"] > 0 and f["snippet"]


def test_the_matrix_is_broad():
    assert len(TECHNIQUES) > 140
    unseen_at_run_time = sorted(t.name for t in TECHNIQUES if t.runtime is None)
    assert unseen_at_run_time == ["compiled-writer-kept-in-a-table-before-the-run", "filesystem-stream-written-by-hand", "native-code",
                                  "written-after-the-run", "written-as-the-job-is-imported"]


# --- what must raise nothing ------------------------------------------------------------------------------------------------

def legitimate():
    import importlib.util
    bodies = dict(LEGITIMATE)
    for library, more in LEGITIMATE_WITH.items():
        if importlib.util.find_spec(library) is not None:
            bodies.update(more)
    return bodies


@pytest.fixture(scope="module")
def clean(tmp_path_factory):
    after = "OUTCOME['unwatched'] = sorted(invaract._strict.UNWATCHED.items())\n"
    return run_job(str(tmp_path_factory.mktemp("clean")), legitimate(), strict="enforce", after=after)


def test_an_ordinary_job_is_clean_with_strict_mode_enforcing(clean):
    """Reads are the scan's to judge, not strict mode's: this job reads files, and strict mode has nothing to say."""
    assert clean.status == 0, clean.stderr[-3000:]
    assert clean.published()
    assert [(e["phase"], e["status"], e["mode"], e["refused"], e["findings"], e["errors"]) for e in clean.scans()] == [
        ("RUNTIME", "CLEAN", "ENFORCE", False, [], [])]


@pytest.mark.parametrize("name", sorted(legitimate()))
def test_ordinary_code_runs_as_it_would_without_strict_mode(name, clean):
    assert clean.outcome.get(name) == "ran", clean.outcome.get(name)


def test_every_writer_the_catalogue_names_in_a_loaded_library_is_wrapped(clean):
    assert clean.outcome["unwatched"] == []


# --- one job at a time: what is kept, what is said, and what happens when it cannot watch -------------------------------------

@pytest.fixture
def job(tmp_path):
    """Runs a job with one function, ``body``, in its own folder."""
    def run(body, name="backup", **modes):
        return run_job(str(tmp_path / f"run-{len(os.listdir(tmp_path))}"), {name: body}, **modes)
    return run


WRITE = "pq.write_table(table, p)"


def test_a_marker_on_the_call_keeps_it_reported_and_enforced(job):
    marked = "pq.write_table(table, p)  # invaract: allow - the finance team's copy"
    for mode in ("report", "enforce"):
        ran = job(marked, strict=mode)
        assert ran.status == 0, ran.stderr[-2000:]
        assert ran.published() and ran.exists("evade/backup.out")
        [watched] = ran.scans("RUNTIME")
        assert (watched["status"], watched["refused"], watched["counts"]["write"]) == ("EXCEPTIONS", False, {"findings": 0, "exceptions": 1})
        [kept] = watched["findings"]
        assert (kept["allowed"], kept["reason"], kept["rule"], kept["call"]) == (True, "the finance team's copy", "pyarrow-write", "pyarrow.parquet.write_table")


def test_a_marker_on_any_line_of_the_call_keeps_it(job):
    ran = job("pq.write_table(\n    table,  # invaract: allow - a copy\n    p,\n)", strict="enforce")
    assert ran.status == 0, ran.stderr[-2000:]
    assert [(f["allowed"], f["reason"]) for f in ran.findings("RUNTIME")] == [(True, "a copy")]


def test_a_marker_elsewhere_does_not_keep_it(job):
    ran = job("x = 1  # invaract: allow - not this\npq.write_table(table, p)\ny = 2  # invaract: allow", strict="enforce")
    assert ran.status == 1 and not ran.exists("evade/backup.out")


@pytest.mark.parametrize("body,allow,pattern", [
    (WRITE, "evade/*", "evade/*"),
    (WRITE, "*.out", "*.out"),
    (WRITE, "nothing/*, evade/backup.out", "evade/backup.out"),
    ("open(p, 'w').write('x')", "evade/*", "evade/*"),
    ("import subprocess\nsubprocess.run(['cp', seed, p])", "cp", "cp"),
    ("import socket\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\nserver.listen(1)\nclient = socket.socket()\n"
     "client.connect(server.getsockname())\nclient.close()\nserver.close()", "127.0.0.1:*", "127.0.0.1:*"),
])
def test_what_the_platform_lets_through_is_kept_with_its_pattern_as_the_reason(job, body, allow, pattern):
    ran = job(body, strict="enforce", allow=allow)
    assert ran.status == 0, ran.stderr[-2000:]
    assert [(f["allowed"], f["reason"]) for f in ran.findings("RUNTIME")] == [(True, f"INVARACT_STRICT_ALLOW lets through {pattern}")]


def test_what_the_platform_does_not_name_is_still_refused(job):
    ran = job(WRITE, strict="enforce", allow="logs/*,*.log")
    assert ran.status == 1 and not ran.exists("evade/backup.out")


def test_a_call_made_many_times_is_one_finding_that_says_how_many(job):
    ran = job("for n in range(5):\n    pq.write_table(table, p + str(n))", strict="report")
    [found] = ran.findings("RUNTIME")
    assert found["detail"] == "seen as the job ran: wrote evade/backup.out0; 5 times in all"
    assert found["target"] == "evade/backup.out0"


def test_the_finding_says_where_the_call_is_and_what_it_reached(job):
    ran = job("def save(path):\n    pq.write_table(\n        table, path)\nsave(p)", strict="report")
    [found] = ran.findings("RUNTIME")
    source = open(os.path.join(ran.folder, "job.py")).read().splitlines()
    line = next(n for n, text in enumerate(source, 1) if text.strip() == "pq.write_table(")
    assert (found["path"], found["line"], found["endLine"], found["scope"]) == ("job.py", line, line + 1, "t_backup.save")
    assert (found["code"], found["category"], found["rule"], found["target"]) == ("IVR002", "write", "pyarrow-write", "evade/backup.out")
    assert found["call"] == found["resolved"] == "pyarrow.parquet.write_table"
    if sys.version_info >= (3, 11):
        assert (found["column"], found["snippet"]) == (9, "pq.write_table( table, path)")
    else:
        assert (found["column"], found["snippet"]) == (0, "pq.write_table( table, path)")
    assert found["message"].startswith("`pyarrow.parquet.write_table` writes data outside invaract")
    assert len(found["id"]) == 16


def test_enforced_the_refusal_is_raised_where_the_write_was_about_to_be_made(job):
    ran = job(WRITE, strict="enforce")
    assert ran.status == 1
    assert ran.outcome["backup"].startswith("ContractViolation: Contract violation: 'orders_report@1.0.0' rejected")
    assert not ran.exists("evade/backup.out") and not ran.published()
    assert "(job.py:" in ran.stderr and "writes data outside Invaract, so no check sees it (IVR002 pyarrow-write, seen as the job ran: wrote evade/backup.out)" in ran.stderr
    assert "`# invaract: allow - <why>`" in ran.stderr
    [refused] = ran.scans("RUNTIME")
    assert (refused["status"], refused["mode"], refused["refused"]) == ("FINDINGS", "ENFORCE", True)
    [verdict] = [e for e in ran.events if e["eventType"] == "CONTRACT_VALIDATION" and e["status"] == "FAILED"]
    assert [v["type"] for v in verdict["violations"]] == ["UNGOVERNED_DATA_ACCESS"]


def test_a_job_that_swallows_the_refusal_is_refused_all_the_same(job):
    ran = job("try:\n    pq.write_table(table, p)\nexcept Exception:\n    pass\ntry:\n    open(p, 'w')\nexcept OSError:\n    pass", strict="enforce")
    assert ran.status == 1 and ran.outcome["backup"] == "ran"
    assert "UNGOVERNED_DATA_ACCESS" in ran.stderr
    assert not ran.exists("evade/backup.out") and not ran.published()
    assert [(e["refused"], len(e["findings"])) for e in ran.scans("RUNTIME")] == [(True, 1), (True, 1)]


def test_the_same_call_tried_again_is_refused_again(job):
    ran = job("for attempt in range(3):\n    try:\n        pq.write_table(table, p)\n    except Exception:\n        pass", strict="enforce")
    assert ran.status == 1 and not ran.exists("evade/backup.out")
    assert len(ran.scans("RUNTIME")) == 1, "asked of the checker once, refused each time"


def test_a_dry_run_reports_even_when_asked_to_enforce(job):
    ran = job(WRITE, strict="enforce", dry_run=True, governed='invaract.write(df, "report", location="data/out/report")')
    assert ran.status == 0, ran.stderr[-2000:]
    assert ran.exists("evade/backup.out")
    scans = ran.scans("RUNTIME")
    assert [(e["status"], e["mode"], e["refused"]) for e in scans] == [("FINDINGS", "ENFORCE", False)]


def test_a_job_that_fails_still_says_what_it_did(job):
    ran = job(WRITE + "\nraise RuntimeError('the job broke')", strict="report", calls="            t_backup('evade/backup.out', 'seed/backup.txt')")
    assert ran.status == 1 and "RuntimeError: the job broke" in ran.stderr
    assert not ran.published()
    assert [(e["status"], [f["rule"] for f in e["findings"]]) for e in ran.scans("RUNTIME")] == [("FINDINGS", ["pyarrow-write"])]


def test_the_scan_and_strict_mode_each_report_the_same_call(job):
    ran = job("pq.write_table(table, p)  # invaract: allow - a copy", strict="enforce", lint="enforce")
    assert ran.status == 0, ran.stderr[-2000:]
    static = [f for f in ran.findings("START") if f["scope"] == "t_backup"]
    [seen] = ran.findings("RUNTIME")
    assert [(f["rule"], f["allowed"], f["reason"], f["line"]) for f in static] == [("pyarrow-write", True, "a copy", seen["line"])]
    assert (seen["rule"], seen["allowed"], seen["reason"]) == ("pyarrow-write", True, "a copy")


def test_without_invaract_lint_the_run_is_not_checked(job):
    # as Python finds it when the package is not installed: the import fails
    ran = job(WRITE, strict="report", files={"invaract_lint.py": "raise ImportError('No module named invaract_lint')\n"})
    assert ran.status == 2
    assert "INVARACT_STRICT=report asks for the job to be watched as it runs, and invaract-lint, which names what to watch, is not installed" in ran.stderr
    assert ran.outcome == {} and not ran.exists("evade/backup.out")


def test_with_an_invaract_lint_too_old_to_name_what_to_watch_the_run_is_not_checked(job):
    ran = job(WRITE, strict="enforce", files={"invaract_lint.py": "__version__ = '0.1.0'\n"})
    assert ran.status == 2
    assert "needs the invaract-lint released with this package" in ran.stderr and "the one installed is older (0.1.0)" in ran.stderr
    assert ran.outcome == {} and not ran.exists("evade/backup.out")


def test_a_mode_that_is_none_of_the_three_is_not_checked(job):
    ran = job(WRITE, strict="strict")
    assert ran.status == 2 and "INVARACT_STRICT must be off, report or enforce, not 'strict'" in ran.stderr


@pytest.mark.parametrize("value", ["off", "OFF", ""])
def test_off_watches_nothing(job, value):
    ran = job(WRITE, strict=value)
    assert ran.status == 0 and ran.exists("evade/backup.out") and ran.scans() == []


BREAK_THE_WATCH = "def broken(frame):\n    raise KeyError('f_code')\ninvaract._strict._position = broken\n"


def test_when_the_watch_itself_fails_a_reporting_run_goes_on_and_says_it_was_not_all_watched(job):
    ran = job(BREAK_THE_WATCH + WRITE, strict="report")
    assert ran.status == 0, ran.stderr[-2000:]
    assert ran.exists("evade/backup.out")
    [watched] = ran.scans("RUNTIME")
    assert (watched["status"], watched["findings"]) == ("NOT_SCANNED", [])
    assert watched["errors"][0] == {"path": "<strict mode>", "message": "KeyError: 'f_code' while watching pyarrow.parquet.write_table"}
    assert all(error["path"] == "<strict mode>" for error in watched["errors"])


def test_when_the_watch_itself_fails_an_enforcing_run_is_not_checked_and_the_write_is_not_made(job):
    ran = job(BREAK_THE_WATCH + WRITE, strict="enforce")
    assert ran.status == 2
    assert "Invaract's strict mode could not watch pyarrow.parquet.write_table (KeyError: 'f_code'), so the run is not checked" in ran.stderr
    assert not ran.exists("evade/backup.out") and not ran.published()


def test_a_password_in_a_connection_string_is_never_published(job):
    pytest.importorskip("polars")
    body = ("import polars as pl\ntry:\n    pl.from_arrow(table).write_database('copy', 'postgresql://loader:hunter2@db.internal/warehouse')\n"
            "except Exception:\n    pass")
    ran = job(body, strict="report")
    [found] = ran.findings("RUNTIME")
    assert "hunter2" not in json.dumps(ran.events)
    assert found["target"] == "postgresql://loader:***@db.internal/warehouse"
    assert found["detail"] == "seen as the job ran: wrote postgresql://loader:***@db.internal/warehouse"


def test_a_program_is_named_and_its_arguments_are_not(job):
    ran = job("import subprocess\nsubprocess.run(['cp', '--', seed, p + '-secret-name'])", strict="report")
    [found] = ran.findings("RUNTIME")
    assert (found["category"], found["target"], found["detail"]) == ("process", "cp", "seen as the job ran: started cp")
    assert "secret-name" not in json.dumps({k: v for k, v in found.items() if k != "snippet"})
