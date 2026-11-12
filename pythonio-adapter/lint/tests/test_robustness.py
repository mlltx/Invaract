# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The scan never stops a run by breaking: whatever Python a job is made of, it is scanned or listed as not scanned."""
import os
import sysconfig
import time

import pytest

from invaract_lint import scan_source
from invaract_lint.scan import NOT_SCANNED, scan_files


@pytest.mark.skipif("MUTANT_UNDER_TEST" in os.environ, reason="reaches every function: under mutation testing it would run once per mutant")
def test_it_scans_the_whole_standard_library():
    """Some eight hundred files of every style Python has been written in: each is scanned, none breaks the scan, and none
    takes long."""
    stdlib = sysconfig.get_paths()["stdlib"]
    files = sorted(os.path.join(root, name) for root, folders, names in os.walk(stdlib)
                   if "site-packages" not in root and "dist-packages" not in root and "/test" not in root.replace(os.sep, "/")
                   for name in names if name.endswith(".py"))
    assert len(files) > 400
    slowest = 0.0
    for path in files:
        started = time.perf_counter()
        result = scan_files([path])
        slowest = max(slowest, time.perf_counter() - started)
        # lib2to3's and others' sample files are deliberately not Python: those are listed, with why
        assert result.files == [path] or (result.errors and result.errors[0][0] == path and result.errors[0][1])
    assert slowest < 5


@pytest.mark.parametrize("name,content", [
    ("empty.py", b""),
    ("only-a-comment.py", b"# nothing here\n"),
    ("a-byte-order-mark.py", b"\xef\xbb\xbfimport pandas as pd\n"),
    ("latin-1.py", b"# -*- coding: latin-1 -*-\nname = '\xe9'\n"),
    ("windows-line-ends.py", b"import os\r\nx = 1\r\n"),
    ("no-final-newline.py", b"x = 1"),
    ("form-feed.py", b"x = 1\n\x0c\ny = 2\n"),
])
def test_files_python_reads_are_scanned(tmp_path, name, content):
    path = tmp_path / name
    path.write_bytes(content)
    result = scan_files([str(path)])
    assert (result.files, result.errors, result.findings) == ([str(path)], [], [])


@pytest.mark.parametrize("name,content", [
    ("not-python.py", b"def (:\n"),
    ("not-text.py", b"\xff\xfe\x00\x01 = 1\n"),
    ("a-nul.py", b"x = 1\x00\n"),
    ("bad-declared-encoding.py", b"# -*- coding: no-such-codec -*-\nx = 1\n"),
    ("wrong-declared-encoding.py", b"# -*- coding: ascii -*-\nname = '\xe9'\n"),
    ("unfinished.py", b"x = (1,\n"),
    ("bad-indent.py", b"if x:\ny = 1\n"),
    ("nested-too-deep.py", b"x = " + b"(" * 5000 + b"1" + b")" * 5000 + b"\n"),
])
def test_files_python_cannot_read_are_listed_and_the_scan_goes_on(tmp_path, name, content):
    bad, good = tmp_path / name, tmp_path / "good.py"
    bad.write_bytes(content)
    good.write_text("open('a', 'w')\n")
    result = scan_files([str(bad), str(tmp_path / "missing.py"), str(tmp_path), str(good)])
    assert result.files == [str(good)]
    assert [path for path, _ in result.errors] == [str(bad), str(tmp_path / "missing.py"), str(tmp_path)]
    assert all(message for _, message in result.errors)
    assert result.status == "findings" and len(result.findings) == 1


def test_a_file_some_pythons_cannot_parse_is_scanned_or_listed_never_a_failure(tmp_path):
    """Python runs a concatenation five thousand strings long; reading it as a syntax tree, some releases give up (3.11 and
    3.12 do). Either way the scan goes on: the file is scanned, or it is listed with why."""
    long, good = tmp_path / "long.py", tmp_path / "good.py"
    long.write_bytes(b"x = " + b" + ".join([b"'a'"] * 5000) + b"\ncon.execute(x)\n")
    good.write_text("open('a', 'w')\n")
    result = scan_files([str(long), str(good)])
    assert result.files in ([str(long), str(good)], [str(good)])
    assert [path for path, _ in result.errors] == [p for p in [str(long)] if p not in result.files]
    assert [f.path for f in result.findings] == [str(good)]


def test_a_scan_of_nothing_but_unreadable_files_says_not_scanned(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_bytes(b"def (:\n")
    assert scan_files([str(bad)]).status == NOT_SCANNED


@pytest.mark.parametrize("source", [
    "x = " + " + ".join(["'a'"] * 700) + "\ncon.execute(x)",               # a long concatenation
    "q = 'select 1'\n" + "q = q + ' and x'\n" * 2000 + "con.execute(q)",   # SQL built up line by line
    "import pandas as pd\nframe = pd.DataFrame()\n" + "frame = frame.assign(a=1)\n" * 2000 + "frame.head()",
    "a = b\nb = c\nc = a\na.to_csv('x')\na('x')",                          # names in a circle
    "x = [" + ", ".join(["1"] * 50000) + "]",                              # a long literal
    "def f():\n" + "".join("    " * (i + 1) + "if x:\n" for i in range(90)) + "    " * 91 + "pass",
    "x = " + "f(" * 150 + "1" + ")" * 150,                                   # calls nested deep
    "x = a" + ".b" * 400,                                                    # a long dotted name
    "class A:\n" + "    def m(self): pass\n" * 3000,                         # a very long class
    "lambda: (yield)",
    "async def f():\n    async with a as b, c as d:\n        async for x in y:\n            await x",
    "match command:\n    case [str() as name, *rest] if rest:\n        pass\n    case {'k': v, **others}:\n        pass\n    case _:\n        pass",
    "def f(a, /, b, *, c):\n    global g\n    nonlocal_ = 1\n    del a",
    "x: int\ny: 'str' = 'a'\n(z := 1)\n*a, b = c\nfor i, (j, k) in d: pass",
    "from __future__ import annotations\nimport a.b.c as d, e\nfrom . import f\nfrom .. import g as h\nfrom x import *",
    "try:\n    pass\nexcept* ValueError as e:\n    pass" if __import__("sys").version_info >= (3, 11) else "pass",
])
def test_code_of_unusual_shape_is_scanned_quickly(source):
    started = time.perf_counter()
    scan_source(source)
    assert time.perf_counter() - started < 5
