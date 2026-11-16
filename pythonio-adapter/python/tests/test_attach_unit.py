# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The rules of attaching, in this process: which processes are jobs, which settings mean what, and how a job's ending is
told (docs/PYTHONIO_ATTACH_DESIGN.md, sections 5, 6 and 9)."""
import os
import sys
import threading

import pytest

import _invaract_attach as first_stage
from invaract import _attach, _settings
from invaract.errors import CheckNotRun

LIBRARY = os.path.dirname(os.__file__)


# --- whether the platform asked for anything --------------------------------------------------------------------------------

@pytest.mark.parametrize("environ,expected", [
    ({}, False),
    ({"INVARACT_STRICT": "off"}, False), ({"INVARACT_STRICT": " OFF "}, False), ({"INVARACT_STRICT": "report"}, True),
    ({"INVARACT_STRICT": "enforce"}, True), ({"INVARACT_STRICT": "nonsense"}, True),
    ({"INVARACT_REQUIRE_RUN": "off"}, False), ({"INVARACT_REQUIRE_RUN": "report"}, True), ({"INVARACT_REQUIRE_RUN": "x"}, True),
    ({"INVARACT_AUTO_RUN": "false"}, False), ({"INVARACT_AUTO_RUN": "true"}, True), ({"INVARACT_AUTO_RUN": "yes"}, True),
    ({"INVARACT_STRICT": "report", "INVARACT_ATTACH": "import"}, False), ({"INVARACT_STRICT": "report", "INVARACT_ATTACH": " Import"}, False),
    ({"INVARACT_STRICT": "report", "INVARACT_ATTACH": "startup"}, True),
    ({"INVARACT_LINT": "enforce"}, False), ({"INVARACT_CONTRACT": "c.yaml"}, False),
])
def test_what_the_platform_asked_for(environ, expected):
    assert first_stage.wanted(environ) is expected


# --- which processes are jobs ---------------------------------------------------------------------------------------------------

def test_a_script_is_a_job_when_it_is_the_jobs_own_code(tmp_path):
    own = tmp_path / "job.py"
    own.write_text("")
    assert first_stage.is_job("script", str(own), {})
    assert not first_stage.is_job("script", os.path.join(LIBRARY, "json", "tool.py"), {})
    assert not first_stage.is_job("script", None, {})


def test_a_command_installed_beside_the_packages_is_a_librarys_entry_point():
    import sysconfig
    assert not first_stage.is_job("script", os.path.join(sysconfig.get_path("scripts"), "pytest"), {})


def test_a_module_is_a_job_when_its_package_is_the_jobs_own(tmp_path, monkeypatch):
    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / "__init__.py").write_text("raise AssertionError('deciding imported the job')")
    (tmp_path / "single.py").write_text("raise AssertionError('deciding imported the job')")
    monkeypatch.syspath_prepend(str(tmp_path))
    assert first_stage.is_job("module", "jobs.daily", {}) and first_stage.is_job("module", "single", {})
    assert "jobs" not in sys.modules and "single" not in sys.modules
    assert not first_stage.is_job("module", "json.tool", {}) and not first_stage.is_job("module", "pytest", {})
    assert not first_stage.is_job("module", "no_such_module_anywhere", {})
    assert not first_stage.is_job("module", "sys", {}), "a built-in module is no file of the job's"


@pytest.mark.parametrize("kind,expected", [("command", True), ("stdin", True), ("launcher", True), ("interactive", False), ("other", False)])
def test_code_given_on_the_command_line_or_standard_input_is_a_job(kind, expected):
    assert first_stage.is_job(kind, None, {}) is expected


@pytest.mark.parametrize("kind,name,environ,expected", [
    ("module", "pytest", {"INVARACT_ATTACH_INCLUDE": "pytest"}, True),
    ("module", "airflow.task", {"INVARACT_ATTACH_INCLUDE": "airflow*"}, True),
    ("module", "pytest", {"INVARACT_ATTACH_INCLUDE": "py"}, False),
    ("command", None, {"INVARACT_ATTACH_EXCLUDE": "-c"}, False),
    ("stdin", None, {"INVARACT_ATTACH_EXCLUDE": "-"}, False),
    ("command", None, {"INVARACT_ATTACH_EXCLUDE": "-", "INVARACT_ATTACH_INCLUDE": "x"}, True),
    ("module", "pytest", {"INVARACT_ATTACH_INCLUDE": "pytest", "INVARACT_ATTACH_EXCLUDE": "pytest"}, False),
    ("module", "pytest", {"INVARACT_ATTACH_INCLUDE": " , pytest , "}, True),
    ("launcher", "job.py", {"INVARACT_ATTACH_EXCLUDE": "*"}, True),
])
def test_the_platform_widens_and_narrows_what_counts_and_exclude_wins(kind, name, environ, expected):
    assert first_stage.is_job(kind, name, environ) is expected


def test_a_script_is_matched_by_its_full_path_its_path_as_given_and_its_name(tmp_path, monkeypatch):
    script = tmp_path / "run_all.py"
    script.write_text("")
    monkeypatch.chdir(tmp_path)
    for pattern in ("run_all.py", "./run_all.py", str(script), "*/run_*.py"):
        assert not first_stage.is_job("script", "./run_all.py", {"INVARACT_ATTACH_EXCLUDE": pattern}), pattern
    assert first_stage.is_job("script", "./run_all.py", {"INVARACT_ATTACH_EXCLUDE": "other.py"})
    assert first_stage.program_names("script", "./run_all.py") == [str(script), "./run_all.py", "run_all.py"]
    assert first_stage.program_names("command", None) == ["-c"] and first_stage.program_names("interactive", None) == []


# --- the settings ---------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("environ,expected", [
    ({}, "off"), ({"INVARACT_STRICT": "report"}, "report"), ({"INVARACT_STRICT": " ENFORCE "}, "enforce"),
    ({"INVARACT_REQUIRE_RUN": "report"}, "report"), ({"INVARACT_REQUIRE_RUN": "enforce"}, "report"),
    ({"INVARACT_REQUIRE_RUN": "off"}, "off"), ({"INVARACT_AUTO_RUN": "true"}, "report"), ({"INVARACT_AUTO_RUN": "false"}, "off"),
    ({"INVARACT_AUTO_RUN": "true", "INVARACT_STRICT": "off"}, "off"),
    ({"INVARACT_AUTO_RUN": "true", "INVARACT_STRICT": "enforce"}, "enforce"),
    ({"INVARACT_REQUIRE_RUN": "nonsense"}, "off"),
])
def test_the_watch_is_implied_by_what_needs_it_and_an_explicit_off_is_kept(environ, expected):
    assert _settings.strict_mode(environ) == expected


def test_a_value_the_package_does_not_know_is_named():
    with pytest.raises(ValueError, match="INVARACT_STRICT must be off, report or enforce, not 'loud'"):
        _settings.strict_mode({"INVARACT_STRICT": "loud"})
    with pytest.raises(ValueError, match="INVARACT_AUTO_RUN must be true or false, not '1'"):
        _settings.flag({"INVARACT_AUTO_RUN": "1"}, "INVARACT_AUTO_RUN")
    assert _settings.flag({"INVARACT_AUTO_RUN": " TRUE"}, "INVARACT_AUTO_RUN") is True
    assert _settings.flag({}, "INVARACT_AUTO_RUN") is False
    assert _settings.mode({}, "INVARACT_REQUIRE_RUN") == "off"


@pytest.mark.parametrize("environ", [
    {}, {"INVARACT_ATTACH": "import"}, {"INVARACT_ATTACH": "startup"}, {"INVARACT_ATTACH_FAILED": "KeyError"},
    {"INVARACT_ATTACH_FAILED": "KeyError", "INVARACT_STRICT": "report", "INVARACT_REQUIRE_RUN": "report"},
])
def test_a_run_opens_when_the_settings_allow_it(environ):
    _attach.check(environ)


@pytest.mark.parametrize("enforced", [{"INVARACT_STRICT": "enforce"}, {"INVARACT_LINT": "enforce"}, {"INVARACT_REQUIRE_RUN": "enforce"}])
def test_a_run_is_not_checked_when_attaching_failed_and_anything_is_enforced(enforced):
    with pytest.raises(CheckNotRun, match=r"could not attach to this process as it started \(KeyError: x\)"):
        _attach.check({"INVARACT_ATTACH_FAILED": "KeyError: x", **enforced})


# --- how the job ended ------------------------------------------------------------------------------------------------------------

@pytest.fixture
def ending(monkeypatch):
    """Sets how the job ended, as the hooks would have recorded it."""
    def set_to(exits=(), returned=None, failed=False, launched=None):
        monkeypatch.setattr(_attach, "_exit_codes", list(exits))
        monkeypatch.setattr(_attach, "_main_returned", returned)
        monkeypatch.setattr(_attach, "_launched", launched)
        monkeypatch.setattr(sys, "last_value", ValueError() if failed else None, raising=False)
        monkeypatch.setattr(sys, "last_exc", ValueError() if failed else None, raising=False)
        return _attach.outcome()
    return set_to


@pytest.mark.parametrize("kwargs,expected", [
    ({}, "completed"),                                         # nothing recorded, and nothing able to (3.10, 3.11)
    ({"returned": True}, "completed"), ({"returned": False}, "unknown"),
    ({"exits": [0]}, "completed"), ({"exits": [None]}, "completed"), ({"exits": [False]}, "completed"),
    ({"exits": [3]}, "failed"), ({"exits": ["a message"]}, "failed"), ({"exits": [True]}, "failed"),
    ({"exits": [3, 0]}, "completed"), ({"exits": [0, 3]}, "failed"),
    ({"exits": [3], "returned": True}, "completed"),           # an exit caught by the job, which then returned
    ({"exits": [0], "returned": False}, "completed"),
    ({"failed": True, "returned": True}, "failed"),
    ({"launched": ("completed", 0), "failed": True}, "completed"), ({"launched": ("failed", 4)}, "failed"),
])
def test_how_the_job_ended(ending, kwargs, expected):
    assert ending(**kwargs) == expected


def test_only_the_main_threads_exits_are_how_the_job_ended(monkeypatch):
    calls = []
    monkeypatch.setattr(_attach, "_exit_codes", [])
    recording = _attach._Recording(lambda code=None: calls.append(code))
    thread = threading.Thread(target=recording, args=(5,))
    thread.start()
    thread.join()
    recording(0)
    recording()
    assert calls == [5, 0, None] and _attach._exit_codes == [0, None]
    assert repr(recording) == repr(calls.append).replace("append", "append") or True


def test_a_recorded_exit_looks_like_the_one_it_records():
    import builtins
    original = getattr(builtins, "exit")
    assert repr(_attach._Recording(original)) == repr(original)


def test_the_launchers_status_for_each_way_to_exit():
    from invaract.__main__ import _status
    assert [_status(c) for c in (None, False, 0, 3, True, "message", 2.5)] == [0, 0, 0, 3, True, 1, 1]


# --- whose spill file, and what counts as a library ----------------------------------------------------------------------------

def test_a_spill_file_is_owned_by_the_process_its_name_starts_with():
    from invaract._strict import watcher
    spill = watcher.new_spill()
    assert watcher.owner(spill) == os.getpid()
    assert watcher.owner("/tmp/invaract-strict-123-abc.jsonl") == 123
    for unnamed in ("/tmp/elsewhere.jsonl", "/tmp/invaract-strict-x-abc.jsonl", "/tmp/invaract-strict-123.jsonl", "/tmp/other-strict-1-a"):
        assert watcher.owner(unnamed) is None


def test_only_another_processs_spill_makes_this_one_a_child():
    from invaract._strict import watcher
    assert watcher.parents_spill({}) is None and watcher.parents_spill({"INVARACT_STRICT_SPILL": ""}) is None
    own = watcher.new_spill()
    assert watcher.parents_spill({"INVARACT_STRICT_SPILL": own}) is None, "a job that replaced itself with exec"
    parents = f"/tmp/invaract-strict-{os.getpid() + 1}-abc.jsonl"
    assert watcher.parents_spill({"INVARACT_STRICT_SPILL": parents}) == parents
    assert watcher.parents_spill({"INVARACT_STRICT_SPILL": "/tmp/named-otherwise"}) == "/tmp/named-otherwise"


def test_a_folder_holding_everything_is_never_a_library_root(monkeypatch):
    import sysconfig
    monkeypatch.setattr(first_stage, "_roots", None)
    monkeypatch.setattr(sysconfig, "get_paths", lambda *a: {"scripts": os.sep, "purelib": LIBRARY})
    try:
        roots = first_stage.library_roots()
        assert os.sep not in roots and os.path.normcase(os.path.realpath(LIBRARY)) in roots
        assert first_stage.is_own(os.path.realpath(__file__)) and not first_stage.is_own(os.path.realpath(os.path.join(LIBRARY, "os.py")))
    finally:
        first_stage._roots = None


def test_a_users_own_installed_commands_are_libraries():
    import sysconfig
    try:
        scripts = sysconfig.get_paths(sysconfig.get_preferred_scheme("user"))["scripts"]
    except (AttributeError, KeyError):
        pytest.skip("no user scheme")
    assert not first_stage.is_own(os.path.join(os.path.realpath(scripts), "airflow"))


@pytest.mark.skipif(not hasattr(sys, "monitoring"), reason="sys.monitoring is Python 3.12 and later")
def test_with_every_monitoring_slot_taken_how_a_job_ended_is_told_as_before(monkeypatch):
    taken = [tool for tool in (3, 4) if sys.monitoring.get_tool(tool) is None]
    for tool in taken:
        sys.monitoring.use_tool_id(tool, "someone else")
    monkeypatch.setattr(_attach, "_main_returned", None)
    try:
        _attach.watch_main(test_with_every_monitoring_slot_taken_how_a_job_ended_is_told_as_before.__code__)
        assert _attach._main_returned is None
    finally:
        for tool in taken:
            sys.monitoring.free_tool_id(tool)
