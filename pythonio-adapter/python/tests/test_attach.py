# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract attached to a job without the job's annotation (docs/PYTHONIO_ATTACH_DESIGN.md): real jobs, run as their own
processes against a real checker, attached each way a platform can attach them.

``hook`` is the start-up hook: the wheel's ``.pth`` calls ``_invaract_attach.at_startup()`` as Python starts; here a
``sitecustomize`` on the job's path makes the same call at the same point of start-up (a ``.pth`` is read only from an
installed package's folder; the wheel's own is tested by ``.github/scripts/python_wheel.py``). ``launcher`` is
``python -m invaract job.py``. ``import`` is neither: Invaract attaches when the job imports it."""
import os
import subprocess
import sys
import textwrap

import pytest

from conftest import LINT_SOURCE, PYTHON, checker_command, events

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the jobs use POSIX paths and programs")

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

HOOK = "import _invaract_attach\n_invaract_attach.at_startup()\n"
#: The job's run, as an annotated job writes it.
RUN = "import invaract\nwith invaract.run('contract.yaml'):\n    invaract.write(invaract.read('orders'), 'report')\n"


class Ran:
    def __init__(self, folder, scratch, process):
        self.folder, self.scratch = folder, scratch
        self.status, self.stdout, self.stderr = process.returncode, process.stdout, process.stderr
        path = os.path.join(folder, "events.jsonl")
        self.events = events(path) if os.path.exists(path) else []

    @property
    def said(self):
        """What Invaract wrote to standard error, without what Java and the checker's log add."""
        return "".join(line for line in self.stderr.splitlines(keepends=True)
                       if not line.startswith(("Picked up JAVA_TOOL_OPTIONS", "[main] WARN")))

    def scans(self, phase="RUNTIME"):
        return [e for e in self.events if e["eventType"] == "CODE_SCAN" and e["phase"] == phase]

    def findings(self, phase="RUNTIME"):
        return [f for e in self.scans(phase) for f in e["findings"]]

    def exists(self, path):
        return os.path.exists(os.path.join(self.folder, path))

    def published(self):
        return self.exists("data/out/report")

    def left_behind(self):
        """Spill files left in the temporary folder: a job's are removed when it ends."""
        return [name for name in os.listdir(self.scratch) if name.startswith("invaract-strict-")]


@pytest.fixture
def job(tmp_path):
    """Runs ``source`` as ``job.py``, attached at ``point``, with ``env`` set (and every other INVARACT_ variable unset)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    def run(source, point="hook", env=None, files=None, args=(), command=None, hook=HOOK):
        root = tmp_path / f"run-{len(os.listdir(tmp_path))}"
        folder, scratch, hook_dir = root / "job", root / "scratch", root / "hook"
        for made in (folder / "data" / "in" / "orders", scratch, hook_dir):
            made.mkdir(parents=True)
        pq.write_table(pa.table({"region": ["north", "south"], "total": [10, 20]}), folder / "data/in/orders/part-0.parquet")
        (hook_dir / "sitecustomize.py").write_text(hook)
        written = {"contract.yaml": CONTRACT, "job.py": textwrap.dedent(source),
                   "notify.properties": "sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\n"
                                        f"sink.property.path={folder / 'events.jsonl'}\n", **(files or {})}
        for name, text in written.items():
            (folder / name).parent.mkdir(parents=True, exist_ok=True)
            (folder / name).write_text(text)
        environ = {k: v for k, v in os.environ.items() if not k.startswith("INVARACT_") and k != "PYTHONPATH"}
        path = ([str(hook_dir)] if point == "hook" else []) + [PYTHON, LINT_SOURCE]
        environ.update({"PYTHONPATH": os.pathsep.join(path), "TMPDIR": str(scratch), "INVARACT_CHECKER": checker_command(),
                        "INVARACT_NOTIFY_CONFIG": str(folder / "notify.properties"), "PYTHONDONTWRITEBYTECODE": "1"})
        environ.update(env or {})
        if command is None:
            command = (["-m", "invaract"] if point == "launcher" else []) + ["job.py", *args]
        process = subprocess.run([sys.executable, *command], cwd=folder, env=environ, capture_output=True, text=True, timeout=300)
        return Ran(str(folder), str(scratch), process)
    return run


EVERY_POINT = ["hook", "launcher"]
#: What each point says, as ``invaract.attach``, on the events of a run Invaract opened.
ATTRIBUTE = {"hook": "startup", "launcher": "launcher", "import": "import"}


# --- the hook, idle and attached ------------------------------------------------------------------------------------------

def test_with_nothing_asked_the_hook_does_nothing(job):
    ran = job("import sys\nprint('invaract' in sys.modules, sys.exit.__module__)\n")
    assert (ran.status, ran.stdout.strip(), ran.stderr) == (0, "False sys", "")


@pytest.mark.parametrize("point", EVERY_POINT)
def test_attached_a_job_is_watched_from_its_first_line(job, point):
    source = "open('before-import.txt', 'w').write('x')\n" + RUN
    ran = job(source, point, env={"INVARACT_STRICT": "report"})
    assert ran.status == 0, ran.stderr[-2000:]
    assert ran.published()
    [found] = ran.findings()
    assert (found["path"], found["line"], found["target"], found["scope"]) == ("job.py", 1, "before-import.txt", "<module>")
    assert found["detail"] == "seen before the run opened: opened before-import.txt to write (mode 'w')"
    assert ran.left_behind() == []


@pytest.mark.parametrize("point", EVERY_POINT)
def test_enforced_what_a_job_did_before_importing_the_package_refuses_its_run(job, point):
    ran = job("open('before-import.txt', 'w').write('x')\n" + RUN, point, env={"INVARACT_STRICT": "enforce"})
    assert ran.status == 1 and "UNGOVERNED_DATA_ACCESS" in ran.stderr
    assert not ran.published()
    assert [(e["refused"], [f["target"] for f in e["findings"]]) for e in ran.scans()] == [(True, ["before-import.txt"])]


@pytest.mark.parametrize("asked", [{}, {"INVARACT_ATTACH": "import"}], ids=["no hook", "hook turned off"])
def test_attached_at_import_what_the_job_did_before_is_not_seen(job, asked):
    point = "import" if not asked else "hook"
    ran = job("open('before-import.txt', 'w').write('x')\n" + RUN, point, env={"INVARACT_STRICT": "enforce", **asked})
    assert ran.status == 0, ran.stderr[-2000:]
    assert ran.published() and ran.findings() == []


# --- after the run ends ------------------------------------------------------------------------------------------------------

TAIL = RUN + "open('after-run.txt', 'w').write('x')\nprint('the job ended')\n"


@pytest.mark.parametrize("point", EVERY_POINT + ["import"])
def test_what_a_job_does_after_its_run_ends_is_reported_when_it_ends(job, point):
    ran = job(TAIL, point, env={"INVARACT_STRICT": "report"})
    assert ran.status == 0 and ran.published() and ran.stdout == "the job ended\n"
    assert ran.said.startswith("Invaract: this job read or wrote data after its run ended, so nothing checked it "
                                 "(INVARACT_STRICT=report).\n  job.py:4 `open` writes data outside invaract")
    assert ran.left_behind() == []


@pytest.mark.parametrize("point", EVERY_POINT + ["import"])
def test_enforced_a_job_that_wrote_after_its_run_ended_ends_refused(job, point):
    ran = job(TAIL, point, env={"INVARACT_STRICT": "enforce"})
    assert ran.status == 1, ran.stderr[-2000:]
    assert "after its run ended" in ran.stderr and ran.stdout == "the job ended\n"
    assert ran.published(), "the run itself was clean and published before the write"


def test_what_a_job_does_in_its_own_exit_handlers_is_watched_too(job):
    source = RUN + "import atexit\natexit.register(lambda: open('at-exit.txt', 'w').write('x'))\n"
    ran = job(source, env={"INVARACT_STRICT": "enforce"})
    assert ran.status == 1 and "job.py:5 `open` writes data" in ran.said and ran.exists("at-exit.txt")


@pytest.mark.parametrize("ending,status", [("raise ValueError('the job broke')", 1), ("import sys\nsys.exit(3)", 3)])
def test_a_job_that_failed_by_itself_keeps_its_own_exit_status(job, ending, status):
    ran = job(TAIL + ending + "\n", env={"INVARACT_STRICT": "enforce"})
    assert ran.status == status and "after its run ended" in ran.stderr


def test_a_marker_or_the_platform_keeps_what_a_job_does_after_its_run(job):
    marked = RUN + "open('after-run.txt', 'w').write('x')  # invaract: allow - the job's own marker file\n"
    assert job(marked, env={"INVARACT_STRICT": "enforce"}).status == 0
    allowed = job(TAIL, env={"INVARACT_STRICT": "enforce", "INVARACT_STRICT_ALLOW": "after-*.txt"})
    assert (allowed.status, allowed.said) == (0, "")


def test_a_child_process_that_writes_after_the_run_is_reported_by_the_job(job):
    child = "import sys\nopen(sys.argv[1], 'w').write('x')\n"
    source = RUN + "import subprocess, sys\nsubprocess.run([sys.executable, 'child.py', 'from-child.txt'], check=True)\n"
    ran = job(source, env={"INVARACT_STRICT": "report"}, files={"child.py": child})
    assert ran.status == 0 and ran.exists("from-child.txt")
    assert "child.py:2 `open` writes data outside invaract" in ran.stderr, ran.stderr
    assert ran.left_behind() == []


# --- when attaching fails -------------------------------------------------------------------------------------------------

#: A first stage that fails as it attaches (the package's own attach, which the job's import makes, still works).
BROKEN = "import _invaract_attach\ndef broken(program):\n    raise KeyError('broken')\n" \
         "_invaract_attach._attach = broken\n_invaract_attach.at_startup()\n"


def test_when_attaching_fails_python_still_starts_and_says_so(job):
    ran = job("print('ran')\n", env={"INVARACT_STRICT": "report"}, hook=BROKEN)
    assert (ran.status, ran.stdout) == (0, "ran\n")
    assert ran.said == "Invaract could not attach to this process (KeyError: 'broken'): it is not watched\n"


def test_when_attaching_failed_an_enforced_run_is_not_checked(job):
    ran = job(RUN, env={"INVARACT_STRICT": "enforce"}, hook=BROKEN)
    assert ran.status == 2 and not ran.published()
    assert "Invaract could not attach to this process as it started (KeyError: 'broken'), so what the job did before this run " \
           "was not seen, and the run is not checked (INVARACT_STRICT=enforce)" in ran.stderr


@pytest.mark.parametrize("variable,value,said", [
    ("INVARACT_ATTACH", "early", "INVARACT_ATTACH must be startup or import, not 'early'"),
    ("INVARACT_REQUIRE_RUN", "always", "INVARACT_REQUIRE_RUN must be off, report or enforce, not 'always'"),
    ("INVARACT_AUTO_RUN", "yes", "INVARACT_AUTO_RUN must be true or false, not 'yes'"),
])
@pytest.mark.parametrize("source", [RUN, "import invaract\ninvaract.read('orders')\n"], ids=["a run", "a read with no run"])
def test_a_value_the_package_does_not_know_leaves_the_run_not_checked(job, variable, value, said, source):
    ran = job(source, env={variable: value})
    assert ran.status == 2 and said in ran.stderr and not ran.published()


# --- the launcher -------------------------------------------------------------------------------------------------------------

def test_the_launcher_runs_a_script_as_python_would(job):
    source = "import sys\nprint(__name__, sys.argv, sys.path[0] == __import__('os').getcwd())\n"
    ran = job(source, "launcher", args=("a", "--b"))
    assert (ran.status, ran.stdout) == (0, "__main__ ['job.py', 'a', '--b'] True\n")


def test_the_launcher_runs_a_module_and_code(job):
    files = {"jobs/__init__.py": "", "jobs/daily.py": "import sys\nprint(__name__, sys.argv[1:])\n"}
    assert job("", "launcher", files=files, command=["-m", "invaract", "-m", "jobs.daily", "x"]).stdout == "__main__ ['x']\n"
    assert job("", "launcher", command=["-m", "invaract", "-c", "import sys; print(sys.argv)", "y"]).stdout == "['-c', 'y']\n"


@pytest.mark.parametrize("ending,status", [("pass", 0), ("import sys\nsys.exit(3)", 3), ("raise SystemExit(4)", 4),
                                            ("raise ValueError('boom')", 1), ("import sys\nsys.exit('a message')", 1)])
def test_the_launcher_ends_with_the_jobs_own_status(job, ending, status):
    ran = job(ending + "\n", "launcher")
    assert ran.status == status
    if "boom" in ending:
        assert ran.stderr.rstrip().endswith("ValueError: boom")


@pytest.mark.parametrize("command", [["-m", "invaract"], ["-m", "invaract", "-m"]])
def test_the_launcher_says_how_to_use_it(job, command):
    ran = job("", "launcher", command=command)
    assert ran.status == 2 and ran.stderr.startswith("usage: python -m invaract")


def test_with_the_hook_installed_too_the_launcher_attaches_once(job):
    source = "open('before-import.txt', 'w').write('x')\n" + RUN
    ran = job(source, "hook", env={"INVARACT_STRICT": "report"}, command=["-m", "invaract", "job.py"])
    assert ran.status == 0, ran.stderr[-2000:]
    assert [f["target"] for f in ran.findings()] == ["before-import.txt"]


# --- a run required (INVARACT_REQUIRE_RUN) ----------------------------------------------------------------------------------------

#: A job that never imports the package, and writes a file.
FORGOT = "import atexit\natexit.register(lambda: print('the job exited'))\nopen('report.csv', 'w').write('region,total\\n')\n"
NO_RUN = "Invaract: this job read or wrote data and opened no run, so nothing checked it (INVARACT_REQUIRE_RUN={mode}).\n" \
         "  job.py:3 `open` writes data outside invaract"
HOW = "Decorate the job's entry point with @invaract.job(), or use 'with invaract.run():'.\n"


@pytest.mark.parametrize("point", EVERY_POINT)
def test_reported_a_job_that_touched_data_and_opened_no_run_is_said_and_goes_on(job, point):
    ran = job(FORGOT, point, env={"INVARACT_REQUIRE_RUN": "report"})
    assert ran.status == 0 and ran.stdout == "the job exited\n" and ran.exists("report.csv")
    assert ran.said.startswith(NO_RUN.format(mode="report")) and ran.said.endswith(HOW)
    assert ran.left_behind() == []
    # where the platform's notifications go, what it did is published, and nothing in it judged
    assert [e["eventType"] for e in ran.events] == ["CODE_SCAN", "JOB_SUMMARY"]
    scanned = ran.events[0]
    assert (scanned["phase"], scanned["status"], scanned["refused"]) == ("RUNTIME", "FINDINGS", False)
    assert [f["target"] for f in scanned["findings"]] == ["report.csv"]
    assert scanned["job"]["attributes"] == {"invaract.run": "none", "invaract.attach": ATTRIBUTE[point]}


def test_a_job_that_opened_no_run_names_its_contract_and_no_run_reports_it_without_reading_the_contract(job):
    ran = job(FORGOT, env={"INVARACT_REQUIRE_RUN": "enforce", "INVARACT_CONTRACT": "no-such-contract.yaml"})
    assert ran.status == 2 and [e["eventType"] for e in ran.events] == ["CODE_SCAN", "JOB_SUMMARY"]


def test_a_failure_to_report_a_job_that_opened_no_run_is_said_and_changes_nothing(job):
    ran = job(FORGOT, env={"INVARACT_REQUIRE_RUN": "report", "INVARACT_CHECKER": "no-such-checker"})
    assert (ran.status, ran.events) == (0, [])
    assert "Invaract could not report this job (CheckNotRun: Invaract could not start its checker" in ran.said


@pytest.mark.parametrize("point", EVERY_POINT)
def test_enforced_a_job_that_touched_data_and_opened_no_run_ends_not_checked(job, point):
    ran = job(FORGOT, point, env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert ran.status == 2 and ran.stdout == "the job exited\n", "its own exit handlers ran first"
    assert ran.said.startswith(NO_RUN.format(mode="enforce"))


def test_without_the_hook_or_the_launcher_a_job_that_never_imports_the_package_is_not_seen(job):
    ran = job(FORGOT, "import", env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert (ran.status, ran.said) == (0, "")


def test_attached_at_import_a_job_that_opened_no_run_is_caught(job):
    ran = job("import invaract\n" + FORGOT.replace("open(", "\nopen(", 1), "import", env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert ran.status == 2 and "opened no run" in ran.said


@pytest.mark.parametrize("source", [
    "import tempfile, os\nprint('computed', sum(range(10)))\nwith tempfile.NamedTemporaryFile('w') as f:\n    f.write('scratch')\n",
    "open('report.csv', 'w').write('x')  # invaract: allow - the job's own marker\n",
])
def test_a_job_that_touched_no_data_or_only_what_is_explained_is_left_alone(job, source):
    ran = job(source, env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert (ran.status, ran.said, ran.events) == (0, "", [])


def test_what_the_platform_lets_through_does_not_count_as_touching_data(job):
    ran = job(FORGOT, env={"INVARACT_REQUIRE_RUN": "enforce", "INVARACT_STRICT_ALLOW": "*.csv"})
    assert (ran.status, ran.said) == (0, "")


@pytest.mark.parametrize("point", EVERY_POINT)
def test_an_annotated_job_runs_as_it_always_has(job, point):
    ran = job(RUN, point, env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert ran.status == 0 and ran.published() and ran.said == ""
    assert [(e["status"], e["mode"]) for e in ran.scans()] == [("CLEAN", "REPORT")], "the watch a required run implies"


def test_with_the_watch_switched_off_every_job_that_opened_no_run_counts(job):
    ran = job("print('nothing touched')\n", env={"INVARACT_REQUIRE_RUN": "enforce", "INVARACT_STRICT": "off"})
    assert ran.status == 2
    assert ran.said.startswith("Invaract: this job opened no run, so nothing checked it (INVARACT_REQUIRE_RUN=enforce; with "
                               "INVARACT_STRICT=off what it did is not known).")


def test_a_job_that_cannot_be_watched_and_opened_no_run_is_not_checked(job):
    missing = {"invaract_lint.py": "raise ImportError('No module named invaract_lint')\n"}
    ran = job("print('x')\n", env={"INVARACT_REQUIRE_RUN": "enforce"}, files=missing)
    assert ran.status == 2 and "could not be watched (is invaract-lint installed?)" in ran.said


def test_a_job_that_failed_by_itself_keeps_its_status_when_no_run_is_required_of_it(job):
    ran = job(FORGOT + "import sys\nsys.exit(3)\n", env={"INVARACT_REQUIRE_RUN": "enforce"})
    assert ran.status == 3 and "opened no run" in ran.said


def test_what_a_child_process_touched_counts_for_the_job(job):
    child = "import sys\nopen(sys.argv[1], 'w').write('x')\n"
    source = "import subprocess, sys\nsubprocess.run([sys.executable, 'child.py', 'from-child.csv'], check=True)\n"
    ran = job(source, env={"INVARACT_REQUIRE_RUN": "enforce"}, files={"child.py": child})
    assert ran.status == 2
    assert "child.py:2 `open` writes data outside invaract" in ran.said and "job.py:2 `subprocess.Popen` runs another program" in ran.said
    assert ran.left_behind() == []


def test_a_librarys_own_command_is_no_job(job):
    """``python -m zipfile`` writes a file: it is Python's own program, not a job, so nothing is required of it."""
    command = ["-m", "zipfile", "-c", "out.zip", "contract.yaml"]
    ran = job("", env={"INVARACT_REQUIRE_RUN": "enforce"}, command=command)
    assert (ran.status, ran.said) == (0, "") and ran.exists("out.zip")
    named = job("", env={"INVARACT_REQUIRE_RUN": "enforce", "INVARACT_ATTACH_INCLUDE": "zipfile"}, command=command)
    assert named.status == 2 and "opened no run" in named.said


def test_a_script_the_platform_excludes_is_no_job(job):
    ran = job(FORGOT, env={"INVARACT_REQUIRE_RUN": "enforce", "INVARACT_ATTACH_EXCLUDE": "job.py"})
    assert (ran.status, ran.said) == (0, "")


# --- a run Invaract opens for the job (INVARACT_AUTO_RUN) --------------------------------------------------------------------------

AMBIENT = "import invaract\ninvaract.write(invaract.read('orders'), 'report')\nprint('the job ended')\n"
RAW = "import pandas as pd\npd.read_parquet('data/in/orders').to_csv('copy.csv')\nprint('the job ended')\n"
AUTO = {"INVARACT_AUTO_RUN": "true", "INVARACT_CONTRACT": "contract.yaml"}


def attributes(ran):
    return {tuple(sorted(e["job"]["attributes"].items())) for e in ran.events}


@pytest.mark.parametrize("point", EVERY_POINT + ["import"])
def test_a_job_that_uses_the_package_needs_no_annotation(job, point):
    ran = job(AMBIENT, point, env=AUTO)
    assert ran.status == 0 and ran.published() and ran.stdout == "the job ended\n", ran.said
    assert [e["eventType"] for e in ran.events] == ["CONTRACT_VALIDATION", "CODE_SCAN", "WRITE", "JOB_SUMMARY"]
    assert attributes(ran) == {(("invaract.attach", ATTRIBUTE[point]), ("invaract.run", "auto"))}


def test_attributes_the_platform_set_are_kept_and_win(job):
    ran = job(AMBIENT, env={**AUTO, "INVARACT_JOB_ATTRIBUTES": "team=orders,invaract.run=platform"})
    assert ran.status == 0, ran.said
    assert attributes(ran) == {(("invaract.attach", "startup"), ("invaract.run", "platform"), ("team", "orders"))}


@pytest.mark.parametrize("point", EVERY_POINT)
@pytest.mark.parametrize("ending,published", [("pass", True), ("import sys\nsys.exit(0)", True),
                                              ("raise ValueError('the job broke')", False), ("import sys\nsys.exit(3)", False)])
def test_what_a_job_with_no_annotation_wrote_is_published_only_if_it_completed(job, point, ending, published):
    ran = job(AMBIENT + ending + "\n", point, env=AUTO)
    assert ran.published() is published
    assert ran.status == (0 if published else 1 if "ValueError" in ending else 3)


def test_how_a_job_ended_by_raising_systemexit_is_told(job):
    """The launcher knows exactly; the hook knows on Python 3.12 and later; before that a raised SystemExit is taken to
    be a completed job (the user guide says so)."""
    source = AMBIENT + "raise SystemExit(4)\n"
    launched = job(source, "launcher", env=AUTO)
    assert (launched.status, launched.published()) == (4, False)
    hooked = job(source, "hook", env=AUTO)
    if sys.version_info >= (3, 12):
        # not told: nothing is published, and the job ends not checked, whatever status it raised
        assert (hooked.status, hooked.published()) == (2, False)
        assert "could not tell whether this job completed" in hooked.said
    else:
        assert hooked.published()


@pytest.mark.parametrize("point", EVERY_POINT)
def test_enforced_a_raw_read_is_refused_where_it_happens(job, point):
    ran = job(RAW, point, env={**AUTO, "INVARACT_STRICT": "enforce"})
    assert ran.status == 1 and ran.stdout == "" and not ran.exists("copy.csv")
    assert "UNGOVERNED_DATA_ACCESS" in ran.stderr
    assert ("CONTRACT_VALIDATION", "FAILED") in [(e["eventType"], e.get("status")) for e in ran.events]


def test_a_refusal_the_job_swallows_still_ends_it_refused(job):
    source = "try:\n" + "".join("    " + line + "\n" for line in RAW.splitlines()) + "except Exception:\n    print('swallowed')\n" \
             "import invaract\ntry:\n    invaract.write(invaract.read('orders'), 'report')\nexcept Exception as e:\n    print(type(e).__name__)\n"
    ran = job(source, env={**AUTO, "INVARACT_STRICT": "enforce"})
    assert ran.status == 1 and ran.stdout == "swallowed\nContractViolation\n" and not ran.published()


def test_reported_what_a_job_did_around_the_package_is_judged_with_its_run_when_it_ends(job):
    """Its raw read is reported; its contract is enforced, so a declared output it never wrote through Invaract refuses it."""
    ran = job(RAW, env=AUTO)
    assert ran.stdout == "the job ended\n" and ran.exists("copy.csv")
    assert [f["target"] for f in ran.findings()] == ["data/in/orders", "copy.csv"]
    assert ran.status == 1 and "Add a write to 'data/out/report' to the job" in ran.stderr


def test_with_a_dry_run_every_job_is_observed_and_none_fails(job):
    ran = job(RAW, env={"INVARACT_AUTO_RUN": "true", "INVARACT_DRY_RUN": "true"})
    assert (ran.status, ran.stdout) == (0, "the job ended\n")
    assert [e["eventType"] for e in ran.events] == ["CODE_SCAN", "DRY_RUN_SUMMARY", "JOB_SUMMARY"]
    assert sorted(f["target"] for f in ran.findings()) == ["copy.csv", "data/in/orders"]


def test_a_job_that_touched_no_data_gets_no_run(job):
    ran = job("print(sum(range(4)))\n", env=AUTO)
    assert (ran.status, ran.stdout, ran.events, ran.said) == (0, "6\n", [], "")


@pytest.mark.parametrize("source", [AMBIENT, RAW])
def test_without_a_contract_or_a_dry_run_the_job_is_not_checked(job, source):
    ran = job(source, env={"INVARACT_AUTO_RUN": "true"})
    assert ran.status == 2 and not ran.published()
    assert "no contract says what the job may do: set INVARACT_CONTRACT, or INVARACT_DRY_RUN=true to observe it" in ran.stderr


def test_an_annotated_job_runs_as_written_and_what_it_did_before_is_in_its_run(job):
    ran = job("open('before.txt', 'w').write('x')\n" + RUN, env={**AUTO, "INVARACT_STRICT": "report"})
    assert ran.status == 0 and ran.published(), ran.said
    assert attributes(ran) == {()}, "an annotated run is the job's own"
    assert [f["target"] for f in ran.findings()] == ["before.txt"]


def test_using_the_package_outside_its_own_run_and_then_opening_one_is_not_checked(job):
    source = "import invaract\ninvaract.read('orders')\nwith invaract.run('contract.yaml'):\n    pass\n"
    ran = job(source, env=AUTO)
    assert ran.status == 2 and "reads or writes through Invaract outside its own run and then opens one" in ran.stderr


def test_after_its_own_run_a_job_gets_one_from_the_platform(job):
    ran = job(RUN + "invaract.write(invaract.read('orders'), 'report')\n", env=AUTO)
    assert ran.status == 0, ran.said
    validations = [e for e in ran.events if e["eventType"] == "CONTRACT_VALIDATION"]
    assert [tuple(sorted(e["job"]["attributes"])) for e in validations] == [(), ("invaract.attach", "invaract.run")]


def test_enforced_a_child_process_refuses_by_itself_and_the_job_has_one_run(job):
    child = "import sys\nopen(sys.argv[1], 'w').write('x')\n"
    source = AMBIENT + "import subprocess, sys\nprint(subprocess.run([sys.executable, 'child.py', 'from-child.csv']).returncode)\n"
    # the platform lets the job start Python; what that Python does is the job's
    ran = job(source, env={**AUTO, "INVARACT_STRICT": "enforce", "INVARACT_STRICT_ALLOW": sys.executable}, files={"child.py": child})
    assert ran.stdout == "the job ended\n1\n" and not ran.exists("from-child.csv")
    assert ran.status == 1 and not ran.published(), "the child's write refuses the job when its run ends"
    assert len({e["job"]["runId"] for e in ran.events}) == 1


def test_a_child_process_has_no_run_of_its_own(job):
    child = "import invaract\ninvaract.read('orders')\n"
    source = AMBIENT + "import subprocess, sys\nprint(subprocess.run([sys.executable, 'child.py'], capture_output=True, text=True).stderr.splitlines()[-1])\n"
    ran = job(source, env=AUTO, files={"child.py": child})
    assert ran.stdout.endswith("(this process was started by a job: its run is the parent process's)\n")


def test_a_value_the_package_does_not_know_never_breaks_the_import(job):
    ran = job("import invaract\nprint('imported')\n", env={"INVARACT_STRICT": "enforce", "INVARACT_AUTO_RUN": "yes"})
    assert ran.stdout == "imported\n"
    assert ran.status == 2 and "INVARACT_AUTO_RUN must be true or false, not 'yes', so this job was not checked" in ran.said
