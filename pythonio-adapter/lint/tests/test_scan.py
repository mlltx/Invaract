# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A scan of files: its status, its counts, and the files it could not read."""
import os

from invaract_lint import CLEAN, EXCEPTIONS, FINDINGS, NOT_SCANNED, Scan, scan, scan_files, scan_source


def finding(source):
    [f] = scan_source(source)
    return f


KEPT = finding("df.to_csv('a')  # invaract: allow - kept")
FOUND = finding("import pandas as pd\npd.read_csv('b')")


def test_the_status_sums_the_scan_up():
    assert Scan().status == NOT_SCANNED == "not-scanned"
    assert Scan(files=["a.py"]).status == CLEAN == "clean"
    assert Scan(files=["a.py"], findings=[KEPT]).status == EXCEPTIONS == "exceptions"
    assert Scan(files=["a.py"], findings=[KEPT, FOUND]).status == FINDINGS == "findings"
    assert Scan(files=["a.py"], findings=[FOUND]).status == FINDINGS
    # a file that could not be read leaves nothing more to claim than a call it did find
    assert Scan(errors=[("a.py", "invalid syntax")]).status == NOT_SCANNED
    assert Scan(files=["b.py"], errors=[("a.py", "invalid syntax")]).status == NOT_SCANNED
    assert Scan(files=["b.py"], findings=[KEPT], errors=[("a.py", "invalid syntax")]).status == NOT_SCANNED
    assert Scan(files=["b.py"], findings=[FOUND], errors=[("a.py", "invalid syntax")]).status == FINDINGS


def test_unexplained_and_exceptions_split_the_findings():
    result = Scan(files=["a.py"], findings=[KEPT, FOUND])
    assert result.unexplained == [FOUND] and result.exceptions == [KEPT]


def test_counts_by_category_split_findings_from_exceptions():
    result = Scan(files=["a.py"], findings=[KEPT, FOUND, FOUND])
    assert result.counts() == {
        "read": {"findings": 2, "exceptions": 0},
        "write": {"findings": 0, "exceptions": 1},
        "connect": {"findings": 0, "exceptions": 0},
        "sql": {"findings": 0, "exceptions": 0},
    }


def test_as_data():
    result = Scan(files=["a.py"], findings=[KEPT], errors=[("b.py", "invalid syntax")])
    assert result.to_dict() == {
        "status": "not-scanned", "files": ["a.py"], "counts": result.counts(), "findings": [KEPT.to_dict()],
        "errors": [{"path": "b.py", "message": "invalid syntax"}],
    }


def test_files_it_cannot_read_or_parse_are_listed_and_the_rest_scanned(tmp_path):
    (tmp_path / "a_broken.py").write_text("def (:\n")
    (tmp_path / "b_job.py").write_text("df.to_csv('b')\n")
    missing = str(tmp_path / "missing.py")
    result = scan_files([str(tmp_path / "a_broken.py"), missing, str(tmp_path / "b_job.py")])
    assert result.files == [str(tmp_path / "b_job.py")]
    assert [path for path, _ in result.errors] == [str(tmp_path / "a_broken.py"), missing]
    assert "invalid syntax" in result.errors[0][1] and "No such file" in result.errors[1][1]
    assert [f.target for f in result.findings] == ["b"]


def test_scan_walks_folders_and_honours_excludes(tmp_path):
    os.makedirs(tmp_path / "tests")
    (tmp_path / "job.py").write_text("df.to_csv('a')\n")
    (tmp_path / "tests" / "test_job.py").write_text("df.to_csv('b')\n")
    assert [f.target for f in scan([str(tmp_path)]).findings] == ["a", "b"]
    assert [f.target for f in scan([str(tmp_path)], exclude=["tests"]).findings] == ["a"]
