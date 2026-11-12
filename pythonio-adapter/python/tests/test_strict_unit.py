# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Strict mode's parts, in this process: what each thing Python reports is taken to be, where a writer's call writes, what
the checker is sent (every request held to the protocol's schema, as in every test here), and that the writers the
installed libraries offer are the ones the lint's catalogue names (so a library's new writer is noticed)."""
import io
import os
import socket
import sqlite3
import sys
import textwrap

import pyarrow as pa
import pyarrow.csv
import pyarrow.dataset
import pyarrow.feather
import pyarrow.fs
import pyarrow.ipc
import pyarrow.orc
import pyarrow.parquet as pq
import pytest

import invaract
import invaract_lint
from invaract import _strict
from invaract_lint import catalogue
from conftest import events, notify_to_file, output, write_contract

_strict.install(invaract_lint)

TABLE = pa.table({"region": ["north"], "total": [1]})


def seen(event, *args):
    found = _strict._EVENTS[event](event, args)
    return None if found is None else (found.code, found.rule, found.did, found.target, list(found.paths))


# --- what Python reports -----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("args,expected", [
    (("a.txt", "w", 0), "opened a.txt to write (mode 'w')"),
    (("a.txt", "a", 0), "opened a.txt to write (mode 'a')"),
    (("a.txt", "x", 0), "opened a.txt to write (mode 'x')"),
    (("a.txt", "rb+", 0), "opened a.txt to write (mode 'rb+')"),
    ((b"a.txt", "wb", 0), "opened a.txt to write (mode 'wb')"),
    ((os.path.join("d", "a.txt"), None, os.O_WRONLY | os.O_CREAT), f"opened {os.path.join('d', 'a.txt')} to write (flags {os.O_WRONLY | os.O_CREAT})"),
    (("a.txt", None, os.O_RDWR), f"opened a.txt to write (flags {os.O_RDWR})"),
    (("a.txt", None, os.O_APPEND), f"opened a.txt to write (flags {os.O_APPEND})"),
    (("a.txt", "r", os.O_RDONLY), None),
    (("a.txt", "rb", 0), None),
    (("a.txt", None, os.O_RDONLY), None),
    (("a.txt", None, None), None),
    ((7, "w", 0), None),                      # a descriptor: the file was seen when it was opened
    (("a.txt",), None),
])
def test_a_file_opened(args, expected):
    found = seen("open", *args)
    assert (found and found[2]) == expected
    if expected:
        assert found[:2] == ("IVR002", "open-write-mode")
        assert found[3] == os.fsdecode(args[0]) and found[4] == [found[3]]


@pytest.mark.parametrize("event,args,did,target,paths", [
    ("os.rename", ("a", "b", -1, -1), "moved a to b", "b", ["a", "b"]),
    ("os.link", ("a", "b", -1, -1), "linked b to a", "b", ["a", "b"]),
    ("os.symlink", ("a", "b", -1), "linked b to a", "b", ["a", "b"]),
    ("os.remove", ("a", -1), "deleted a", "a", ["a"]),
    ("os.rmdir", ("d", -1), "deleted the folder d", "d", ["d"]),
    ("os.truncate", ("a", 0), "truncated a", "a", ["a"]),
    ("os.remove", (b"a", -1), "deleted a", "a", ["a"]),
])
def test_a_file_moved_linked_deleted_or_truncated(event, args, did, target, paths):
    assert seen(event, *args) == ("IVR002", "file-operation", did, target, paths)


@pytest.mark.parametrize("event,args", [("os.truncate", (7, 0)), ("os.remove", (7, -1)), ("os.rename", ("a",)), ("os.rename", (3, "b", -1, -1))])
def test_a_change_to_a_descriptor_or_one_python_does_not_describe_is_no_finding(event, args):
    assert seen(event, *args) is None


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs the system to say which folder a descriptor is of")
def test_a_path_given_from_an_open_folder_is_placed_in_that_folder(tmp_path):
    folder = os.open(tmp_path, os.O_RDONLY)
    try:
        real = os.path.realpath(tmp_path)
        assert seen("os.remove", "part.parquet", folder)[3] == os.path.join(real, "part.parquet")
        assert seen("os.rmdir", "sub", folder)[3] == os.path.join(real, "sub")
        assert seen("os.rename", "a", "b", folder, -1)[4] == [os.path.join(real, "a"), "b"]
        assert seen("os.rename", "a", "b", -1, folder)[4] == ["a", os.path.join(real, "b")]
        assert seen("os.symlink", "a", "b", folder)[4] == ["a", os.path.join(real, "b")]
        assert seen("os.remove", "/elsewhere/a", folder)[3] == "/elsewhere/a"
    finally:
        os.close(folder)
    assert _strict._folder_of(999_999) is None
    assert seen("os.remove", "a", 999_999)[3] == "a"


@pytest.mark.parametrize("database,expected", [
    ("warehouse.db", "opened the SQLite database warehouse.db"),
    (b"warehouse.db", "opened the SQLite database warehouse.db"),
    ("file:warehouse.db?mode=ro", "opened the SQLite database file:warehouse.db?mode=ro"),
    (":memory:", None), ("", None), ("file::memory:?cache=shared", None), ("file:shared?mode=memory&cache=shared", None), (None, None),
])
def test_a_sqlite_database_opened(database, expected):
    found = seen("sqlite3.connect", database)
    assert (found and found[2]) == expected
    if expected:
        assert found[:2] == ("IVR003", "database-connect")


@pytest.mark.parametrize("address,target", [
    (("db.internal", 5432), "db.internal:5432"), (("::1", 443, 0, 0), "::1:443"), ("/var/run/postgres.sock", "/var/run/postgres.sock"),
    (b"/var/run/x.sock", "/var/run/x.sock"), (None, None), ((), None), ("", None),
])
def test_a_network_connection(address, target):
    found = seen("socket.connect", object(), address)
    assert (found and found[:4]) == (target and ("IVR003", "network-connect", f"connected to {target}", target))
    assert seen("socket.connect", object()) is None


@pytest.mark.parametrize("event,args,program", [
    ("subprocess.Popen", ("/bin/cp", ["cp", "secret-a", "secret-b"], None, None), "/bin/cp"),
    ("subprocess.Popen", (None, ["cp", "secret-a", "secret-b"], None, None), "cp"),
    ("subprocess.Popen", ("aws s3 cp secret s3://bucket", ["/bin/sh", "-c", "aws s3 cp secret s3://bucket"], None, None), "aws"),
    ("os.system", ("gzip secret.csv",), "gzip"),
    ("os.system", (b"gzip secret.csv",), "gzip"),
    ("os.exec", ("/bin/cp", ["cp", "secret"], None), "/bin/cp"),
    ("os.posix_spawn", ("/bin/cp", ["cp", "secret"], {}), "/bin/cp"),
    ("os.spawn", (0, "/bin/cp", ["cp", "secret"], None), "/bin/cp"),
    ("os.startfile", ("report.xlsx", "open"), "report.xlsx"),
    ("_winapi.CreateProcess", (None, "cmd /c copy secret", None), "cmd"),
    ("os.system", ("",), "a program"),
    ("subprocess.Popen", (None, [], None, None), "a program"),
    ("os.system", (), "a program"),
])
def test_another_program_started_is_named_and_its_arguments_never_are(event, args, program):
    found = seen(event, *args)
    assert found == ("IVR005", "subprocess", f"started {program}", program, [])
    assert "secret" not in str(found)


def test_every_event_watched_is_one_python_raises_for_a_write_a_connection_or_a_program():
    assert sorted(_strict._EVENTS) == [
        "_winapi.CreateProcess", "open", "os.exec", "os.link", "os.posix_spawn", "os.remove", "os.rename", "os.rmdir", "os.spawn",
        "os.startfile", "os.symlink", "os.system", "os.truncate", "socket.connect", "sqlite3.connect", "subprocess.Popen"]
    assert set(_strict._CHANGES) == {e for e, describe in _strict._EVENTS.items() if describe is _strict._changed}


# --- where a writer's call writes ----------------------------------------------------------------------------------------------

class Stream:
    def write(self, data):
        pass


@pytest.mark.parametrize("args,kwargs,parameters,expected", [
    ((TABLE, "out.parquet"), {}, ["table", "where"], ("wrote out.parquet", "out.parquet", ["out.parquet"])),
    ((TABLE,), {"where": "out.parquet"}, ["table", "where"], ("wrote out.parquet", "out.parquet", ["out.parquet"])),
    ((TABLE, os.path.join("d", "x")), {}, None, (f"wrote {os.path.join('d', 'x')}", os.path.join("d", "x"), [os.path.join("d", "x")])),
    (("s3://bucket/key", TABLE), {}, ["table_or_uri", "data"], ("wrote s3://bucket/key", "s3://bucket/key", [])),
    (("out.arrow",), {}, ["source", "compression"], ("wrote out.arrow", "out.arrow", ["out.arrow"])),           # no parameter named: the first
    (("in.parquet", "out.parquet"), {}, ["source", "destination"], ("wrote out.parquet", "out.parquet", ["out.parquet"])),
    (("in.parquet", "out.parquet"), {}, None, ("wrote out.parquet", "out.parquet", ["out.parquet"])),          # of two paths, the second
    ((io.BytesIO(), "Sheet1"), {}, ["workbook", "worksheet"], None),                                           # a sheet's name is no path
    ((TABLE, io.BytesIO()), {}, ["table", "where"], None),
    ((TABLE, sys.stdout), {}, ["table", "where"], None),
    ((TABLE, pa.BufferOutputStream()), {}, ["table", "where"], None),
    ((TABLE, None), {}, ["table", "where"], None),
    ((TABLE,), {}, ["table", "where"], None),
    ((), {}, ["file"], None),
    ((TABLE, b"bytes are data"), {}, ["table", "where"], None),
    ((TABLE,), {"compression": "snappy"}, ["table", "where"], None),
    ((TABLE, Stream()), {}, ["table", "where"],
     ("wrote to an open stream (test_strict_unit.Stream)", "<a stream: test_strict_unit.Stream>", [])),
    ((TABLE,), {"sink": Stream()}, ["table", "where"],
     ("wrote to an open stream (test_strict_unit.Stream)", "<a stream: test_strict_unit.Stream>", [])),
])
def test_where_a_writer_writes(args, kwargs, parameters, expected):
    assert _strict._destination(args, kwargs, parameters) == expected


def test_a_file_python_opened_is_not_reported_again_when_a_writer_is_handed_it(tmp_path):
    with open(tmp_path / "a.parquet", "wb") as opened:
        assert _strict._destination((TABLE, opened), {}, ["table", "where"]) is None
    with open(tmp_path / "a.csv", "w") as text:
        assert _strict._destination((TABLE, text), {}, ["table", "where"]) is None


@pytest.mark.parametrize("args,kwargs,expected", [
    (("a.bin", "wb"), {}, "opened a.bin to write (mode 'wb')"),
    (("a.bin",), {"mode": "r+"}, "opened a.bin to write (mode 'r+')"),
    (("a.bin", "rb"), {}, None), (("a.bin",), {}, None), ((), {"mode": "w"}, None), ((7, "w"), {}, None), (("a.bin", 3), {}, None),
])
def test_an_opener_writes_when_its_mode_does(args, kwargs, expected):
    told = _strict._opener(1, "mode", "wax+")(args, kwargs)
    assert (told and told[0]) == expected


@pytest.mark.parametrize("args,kwargs,reached", [
    (("COPY t TO 'x.parquet'",), {}, "COPY"), ((), {"query": "ATTACH 'x.db' AS x"}, "ATTACH"),
    (("view", "SELECT * FROM read_parquet('x')"), {}, "read_parquet"),
    (("SELECT 1",), {}, None), ((7,), {}, None), ((), {}, None), (("SELECT 'COPY t TO x'",), {}, None),
])
def test_sql_about_to_run(args, kwargs, reached):
    told = _strict._sql(args, kwargs)
    assert told == (reached and (f"ran SQL that reads or writes files ({reached})", None, []))


@pytest.mark.parametrize("args,kwargs,expected", [
    (("warehouse.db",), {}, ("opened the database warehouse.db", "warehouse.db", ["warehouse.db"])),
    ((), {"database": "warehouse.db"}, ("opened the database warehouse.db", "warehouse.db", ["warehouse.db"])),
    ((), {}, None), ((":memory:",), {}, None), ((), {"database": ":memory:"}, None), ((":default:",), {}, None), ((":memory:shared",), {}, None),
])
def test_a_database_file_opened_by_a_wrapped_connect(args, kwargs, expected):
    assert _strict._file_database(args, kwargs) == expected


# --- a run, in this process ---------------------------------------------------------------------------------------------------------

@pytest.fixture
def watched(monkeypatch):
    """A contract with one output, events to a file, strict mode on, and this test's folder (which is under the temporary
    folder) not counted as scratch."""
    write_contract(outputs=output())
    notify_to_file(monkeypatch)
    monkeypatch.setattr(_strict, "_scratch_roots", lambda: [])

    def mode(value, allow=None):
        monkeypatch.setenv("INVARACT_STRICT", value)
        if allow:
            monkeypatch.setenv("INVARACT_STRICT_ALLOW", allow)
    return mode


def report():
    import pandas as pd
    return pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})


def runtime_scans():
    return [e for e in events() if e["eventType"] == "CODE_SCAN" and e["phase"] == "RUNTIME"]


def test_a_run_reports_what_it_saw_once_when_it_ends(watched):
    watched("report")
    with invaract.run("contract.yaml"):
        invaract.write(report(), "report")
        pq.write_table(TABLE, "copy.parquet")
        open("note.txt", "w").close()
    [scan] = runtime_scans()
    assert (scan["status"], scan["mode"], scan["refused"], scan["scanner"]) == ("FINDINGS", "REPORT", False, f"invaract strict {invaract.__version__}")
    assert [(f["rule"], f["target"], f["scope"], f["path"].endswith("test_strict_unit.py")) for f in scan["findings"]] == [
        ("pyarrow-write", "copy.parquet", "test_a_run_reports_what_it_saw_once_when_it_ends", True),
        ("open-write-mode", "note.txt", "test_a_run_reports_what_it_saw_once_when_it_ends", True)]
    assert os.path.exists("data/out/report") and os.path.exists("copy.parquet")


def test_an_enforcing_run_refuses_at_the_write_and_nothing_is_published(watched):
    watched("enforce")
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(report(), "report")
            pq.write_table(TABLE, "copy.parquet")
            raise AssertionError("the write went through")
    assert refused.value.violations == ["UNGOVERNED_DATA_ACCESS"]
    assert "(IVR002 pyarrow-write, seen as the job ran: wrote copy.parquet)" in str(refused.value)
    assert not os.path.exists("copy.parquet") and not os.path.exists("data/out/report")
    assert [(e["status"], e["refused"]) for e in runtime_scans()] == [("FINDINGS", True)]


def test_each_run_reports_its_own_and_nothing_is_watched_between_runs(watched):
    watched("report")
    with invaract.run("contract.yaml"):
        invaract.write(report(), "report")
        open("first.txt", "w").close()
    open("between.txt", "w").close()
    pq.write_table(TABLE, "between.parquet")
    with invaract.run("contract.yaml"):
        invaract.write(report(), "report")
    assert [[f["target"] for f in e["findings"]] for e in runtime_scans()] == [["first.txt"], []]
    assert [e["status"] for e in runtime_scans()] == ["FINDINGS", "CLEAN"]
    assert _strict._watcher is None


def test_what_the_platform_lets_through_is_matched_by_full_path_by_path_from_the_working_folder_and_as_reached(watched, tmp_path):
    watched("enforce", allow=f"logs/*, {os.path.realpath(tmp_path)}/audit/*.txt,127.0.0.*:*")
    os.makedirs("logs")
    os.makedirs("audit")
    with invaract.run("contract.yaml"):
        invaract.write(report(), "report")
        open("logs/job.log", "a").close()
        open(os.path.join("audit", "trail.txt"), "w").close()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        client = socket.socket()
        client.connect(listener.getsockname())
        client.close()
        listener.close()
    [scan] = runtime_scans()
    assert scan["status"] == "EXCEPTIONS" and [f["reason"].split(" lets through ")[1] for f in scan["findings"]] == [
        "logs/*", f"{os.path.realpath(tmp_path)}/audit/*.txt", "127.0.0.*:*"]


def test_a_check_is_not_watched(watched):
    """``invaract.check`` judges one frame and is no run: nothing is watched for it, and it publishes nothing."""
    watched("enforce")
    invaract.check(report(), "report", contract="contract.yaml")
    assert _strict._watcher is None
    open("after-a-check.txt", "w").close()
    assert os.path.exists("after-a-check.txt") and not os.path.exists("events.jsonl")


def test_sqlite_sql_that_attaches_a_file_is_stopped_before_it_runs(watched):
    watched("enforce")
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            con = sqlite3.connect(":memory:")
            try:
                with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                    con.execute("ATTACH 'side.db' AS side")
                con.execute("ATTACH ':memory:' AS scratch")   # a second database in memory is no file
            finally:
                con.close()
    assert not os.path.exists("side.db")
    [scan] = runtime_scans()
    assert [(f["category"], f["rule"], f["call"], f["target"]) for f in scan["findings"]] == [("sql", "file-sql", "sqlite3 ATTACH", "side.db")]


# --- the libraries as installed --------------------------------------------------------------------------------------------------

def writers_offered(module):
    """The names in a module that look like something that writes: what a new release might add."""
    return sorted(name for name in dir(module) if not name.startswith("_") and callable(getattr(module, name)) and (
        name.startswith(("write_", "sink_", "new_")) or name.endswith("Writer") or name in ("output_stream", "copy_files", "create_memory_map")))


def is_wrapped(thing):
    return hasattr(thing, "__invaract_original__") or getattr(thing, "__invaract_watched__", False)


@pytest.mark.parametrize("module", [pa, pq, pyarrow.csv, pyarrow.feather, pyarrow.orc, pyarrow.dataset, pyarrow.fs, pyarrow.ipc],
                         ids=lambda m: m.__name__)
def test_every_writer_pyarrow_offers_is_wrapped(module):
    unwrapped = [name for name in writers_offered(module) if not is_wrapped(getattr(module, name))]
    # a writer into a buffer the caller holds in memory writes nothing at rest
    known = {"pyarrow": ["FixedSizeBufferWriter"]}.get(module.__name__, [])
    assert [n for n in unwrapped if n not in known] == [], (
        f"{module.__name__} offers writers strict mode does not wrap: add them to invaract-lint's catalogue")


def test_the_wrapped_writers_are_the_catalogues():
    named = catalogue.writer_names("pyarrow.parquet", dir(pq))
    assert {"write_table", "ParquetWriter", "write_to_dataset", "write_metadata"} <= set(named)
    assert all(is_wrapped(getattr(pq, name)) for name in named)
    assert _strict.UNWATCHED == {}


def test_every_frame_writer_polars_offers_is_wrapped():
    pl = pytest.importorskip("polars")
    for cls in (pl.DataFrame, pl.LazyFrame):
        offered = [n for n in dir(cls) if n.startswith(("write_", "sink_"))]
        assert offered and [n for n in offered if not is_wrapped(getattr(cls, n)) and n != "write_clipboard"] == []
    assert catalogue.frame_writer_names(dir(pl.LazyFrame)) == sorted(n for n in dir(pl.LazyFrame) if n.startswith("sink_"))


def test_everything_that_runs_sql_or_writes_in_duckdb_is_wrapped():
    duckdb = pytest.importorskip("duckdb")
    for name in ("sql", "execute", "executemany", "query", "from_query"):
        assert is_wrapped(getattr(duckdb, name)), name
        assert is_wrapped(getattr(duckdb.DuckDBPyConnection, name)), name
    assert is_wrapped(duckdb.connect)
    offered = [n for n in dir(duckdb.DuckDBPyRelation) if n.startswith("write_") or n in ("to_csv", "to_parquet")]
    assert sorted(offered) == ["to_csv", "to_parquet", "write_csv", "write_parquet"]
    assert all(is_wrapped(getattr(duckdb.DuckDBPyRelation, n)) for n in offered)


def test_wrapped_libraries_work_as_before_when_no_run_is_open(tmp_path):
    """The wrappers stay in place for the life of the process: without a run they are the originals."""
    path = str(tmp_path / "a.parquet")
    pq.write_table(TABLE, path)
    assert pq.read_table(path).equals(TABLE)
    with pq.ParquetWriter(str(tmp_path / "b.parquet"), TABLE.schema) as writer:
        writer.write_table(TABLE)
    writer = pyarrow.csv.CSVWriter(str(tmp_path / "c.csv"), TABLE.schema)
    assert isinstance(writer, pyarrow.csv.CSVWriter) and issubclass(type(writer), pyarrow.csv.CSVWriter)
    writer.write_table(TABLE)
    writer.close()
    opened = pa.OSFile(str(tmp_path / "d.bin"), "wb")
    assert isinstance(opened, pa.OSFile) and isinstance(opened, pa.NativeFile)
    opened.write(b"x")
    opened.close()
    assert isinstance(pa.OSFile(str(tmp_path / "d.bin")), pa.OSFile)
    assert pq.write_table.__name__ == "write_table" and pq.write_table.__doc__
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS other")
    con.close()


def test_a_library_wrapped_as_it_is_imported_is_left_as_any_import_leaves_it():
    for module in (pq, pyarrow.csv, pyarrow.dataset):
        assert not isinstance(module.__spec__.loader, _strict._WrapAfterLoad)
        assert not isinstance(module.__loader__, _strict._WrapAfterLoad)
    assert any(isinstance(finder, _strict._WrapOnImport) for finder in sys.meta_path)
    assert sum(isinstance(finder, _strict._WrapOnImport) for finder in sys.meta_path) == 1, "installed once, however often asked"


def test_a_module_imported_after_the_hooks_are_installed_is_wrapped_as_it_loads(tmp_path):
    """A job that imports a library only inside the function that uses it: in its own process, where it is not loaded yet
    when the hooks go in (the package itself loads pyarrow's core, so the modules here are ones it does not)."""
    import subprocess
    from conftest import LINT_SOURCE, PYTHON
    script = textwrap.dedent("""
        import importlib, sys, invaract_lint
        from invaract import _strict
        _strict.install(invaract_lint)
        writers = {"pyarrow.orc": "write_table", "pyarrow.feather": "write_feather", "dbm": "open", "multiprocessing.util": "spawnv_passfds"}
        late = sorted(name for name in writers if name not in sys.modules)
        loaded = {name: importlib.import_module(name) for name in late}
        wrapped = all(hasattr(getattr(module, writers[name]), "__invaract_original__") for name, module in loaded.items())
        own = all(type(module.__spec__.loader).__name__ != "_WrapAfterLoad" for module in loaded.values())
        print("dbm" in late, len(late) >= 2, wrapped, own, _strict.UNWATCHED)
        """)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([PYTHON, LINT_SOURCE]))
    ran = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env, cwd=tmp_path)
    assert ran.stdout.strip() == "True True True True {}", ran.stderr[-2000:]


def test_duckdb_still_finds_the_frames_its_sql_names_among_the_callers_variables():
    duckdb = pytest.importorskip("duckdb")
    orders = report()  # noqa: F841 - named by the SQL below
    assert duckdb.sql("select sum(total) from orders").fetchall() == [(30,)]
    con = duckdb.connect()
    assert con.execute("select count(*) from orders").fetchall() == [(2,)]
    assert con.sql("select region from orders order by region").fetchall() == [("north",), ("south",)]
    assert duckdb.query("select max(total) from orders").fetchall() == [(20,)]
    con.close()
