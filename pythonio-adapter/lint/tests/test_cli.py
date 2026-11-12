# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The command line: which files it checks, what it prints, and the exit status a CI step or pre-commit hook acts on."""
import json
import os
import subprocess
import sys
import textwrap

import pytest

from invaract_lint import __version__
from invaract_lint.cli import main, python_files

HERE = os.path.dirname(os.path.abspath(__file__))
LINT = os.path.dirname(HERE)
PACKAGE = os.path.join(os.path.dirname(LINT), "python")

#: The example job as it was before it adopted invaract (docs/SINK_RECORDS_SURVEY.md, probe 14): every read and write by path.
BEFORE = {
    "load.py": """\
        import pandas as pd

        def paid_orders():
            orders = pd.read_parquet("data/in/orders")
            return orders[orders["status"] == "paid"]

        def customers():
            return pd.read_csv("data/in/customers.csv")
        """,
    "build.py": """\
        def audit(paid):
            paid[["order_id", "status"]].to_parquet("data/out/audit/part-0.parquet")
        """,
    "main.py": """\
        import os
        import build, load

        def main():
            os.makedirs("data/out/report", exist_ok=True)
            paid = load.paid_orders()
            build.audit(paid)
            paid.merge(load.customers(), on="customer_id").to_parquet("data/out/report/part-0.parquet")
        """,
}


def write(folder, files):
    for name, text in files.items():
        path = os.path.join(folder, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(textwrap.dedent(text))


def run(capsys, *args):
    status = main(list(args))
    out, err = capsys.readouterr()
    return status, out.splitlines(), err


def test_the_example_job_before_it_adopted_invaract_has_every_read_and_write_flagged(tmp_path, capsys):
    write(tmp_path, BEFORE)
    status, lines, err = run(capsys, str(tmp_path))
    assert status == 1
    assert [line.split(": ", 1)[1].split(" ")[0:2] for line in lines] == [
        ["IVR002", "`paid[['order_id',"],
        ["IVR001", "`pd.read_parquet`"],
        ["IVR001", "`pd.read_csv`"],
        ["IVR002", "`paid.merge(load.customers(),"],
    ]
    assert [os.path.basename(line.split(":")[0]) for line in lines] == ["build.py", "load.py", "load.py", "main.py"]
    assert err.strip() == "4 read(s) or write(s) outside invaract"


def test_the_example_job_as_it_is_and_the_conformance_job_are_clean(capsys):
    # the conformance job makes its inputs with pyarrow before the run, each such line marked `# invaract: allow`
    status, lines, err = run(capsys, os.path.join(PACKAGE, "examples"), os.path.join(PACKAGE, "tests", "conformance_job.py"))
    assert (status, lines, err) == (0, [], "")


def test_github_format_annotates_each_finding(tmp_path, capsys):
    write(tmp_path, {"job.py": "import pandas as pd\npd.read_csv('a')\n"})
    status, lines, _ = run(capsys, "--format", "github", str(tmp_path))
    assert status == 1
    assert lines == [f"::error file={tmp_path}/job.py,line=2,col=1,endLine=2,endColumn=17,title=IVR001::`pd.read_csv` reads data "
                     "outside invaract: read it with invaract.read(<name>), or mark the line `# invaract: allow`"]


def test_a_file_that_cannot_be_parsed_is_reported_and_the_rest_still_checked(tmp_path, capsys):
    write(tmp_path, {"a_broken.py": "def (:\n", "b_job.py": "import pandas as pd\npd.read_csv('a')\n"})
    status, lines, err = run(capsys, str(tmp_path))
    assert status == 2
    assert len(lines) == 1 and "b_job.py:2:1: IVR001" in lines[0]
    assert "a_broken.py: cannot be checked" in err


def test_a_file_in_the_encoding_it_declares_is_read_as_python_reads_it(tmp_path, capsys):
    (tmp_path / "job.py").write_bytes("# -*- coding: latin-1 -*-\nimport pandas as pd\npd.read_csv('données.csv')\n".encode("latin-1"))
    status, lines, err = run(capsys, str(tmp_path))
    assert (status, err.splitlines()[0]) == (1, "1 read(s) or write(s) outside invaract")
    assert len(lines) == 1 and "job.py:3:1: IVR001" in lines[0]


def test_a_clean_folder_exits_0_and_says_nothing(tmp_path, capsys):
    write(tmp_path, {"job.py": "import invaract\ninvaract.read('orders')\n"})
    assert run(capsys, str(tmp_path)) == (0, [], "")


def test_which_files_are_checked(tmp_path):
    write(tmp_path, {
        "job.py": "", "pkg/step.py": "", "notes.txt": "",
        ".venv/lib.py": "", "venv/lib.py": "", "pkg/__pycache__/x.py": "", "build/gen.py": "", "node_modules/a.py": "",
        "tests/test_job.py": "", "tests/fixtures/data.py": "",
    })
    files = [os.path.relpath(f, tmp_path) for f in python_files([str(tmp_path)])]
    assert files == ["job.py", os.path.join("pkg", "step.py"), os.path.join("tests", "test_job.py"), os.path.join("tests", "fixtures", "data.py")]
    excluded = [os.path.relpath(f, tmp_path) for f in python_files([str(tmp_path)], exclude=["tests", "step.py"])]
    assert excluded == ["job.py"]
    # a file named on the command line is checked whatever its name, unless excluded
    assert list(python_files([str(tmp_path / "notes.txt")])) == [str(tmp_path / "notes.txt")]
    assert list(python_files([str(tmp_path / "job.py")], exclude=["job.py"])) == []


def test_the_current_folder_is_the_default(tmp_path, capsys, monkeypatch):
    write(tmp_path, {"job.py": "df.to_csv('a')\n"})
    monkeypatch.chdir(tmp_path)
    status, lines, _ = run(capsys)
    assert status == 1 and lines[0].startswith(os.path.join(".", "job.py") + ":1:1: IVR002")


@pytest.mark.parametrize("module", ["invaract_lint", "invaract_lint.cli"])
def test_it_runs_as_a_module(tmp_path, module):
    write(tmp_path, {"job.py": "df.to_csv('a')\n"})
    done = subprocess.run([sys.executable, "-m", module, str(tmp_path)], capture_output=True, text=True, cwd=LINT)
    assert done.returncode == 1 and "IVR002" in done.stdout


def test_every_path_given_is_checked_files_and_folders_alike(tmp_path, capsys):
    write(tmp_path, {"a.py": "df.to_csv('a')\n", "b.py": "df.to_csv('b')\n", "more/c.py": "df.to_csv('c')\n"})
    status, lines, _ = run(capsys, str(tmp_path / "a.py"), str(tmp_path / "b.py"), str(tmp_path / "more"))
    assert [os.path.basename(line.split(":")[0]) for line in lines] == ["a.py", "b.py", "c.py"]


def test_excludes_from_the_command_line_by_name_or_by_path(tmp_path, capsys):
    write(tmp_path, {"job.py": "df.to_csv('a')\n", "tests/test_job.py": "df.to_csv('b')\n", "pkg/sub/gen.py": "df.to_csv('c')\n",
                     "other/sub/keep.py": "df.to_csv('d')\n"})
    status, lines, _ = run(capsys, "--exclude", "tests", "--exclude", str(tmp_path / "pkg" / "sub"), str(tmp_path))
    assert [os.path.relpath(line.split(":")[0], tmp_path) for line in lines] == ["job.py", os.path.join("other", "sub", "keep.py")]


def test_the_format_is_text_github_or_json_and_nothing_else(tmp_path, capsys):
    write(tmp_path, {"job.py": "df.to_csv('a')\n"})
    status, lines, _ = run(capsys, "--format", "text", str(tmp_path))
    assert status == 1 and ": IVR002 " in lines[0]
    with pytest.raises(SystemExit) as usage:
        main(["--format", "sarif", str(tmp_path)])
    assert usage.value.code == 2


def test_json_format_prints_the_whole_scan_with_the_calls_a_marker_keeps(tmp_path, capsys):
    write(tmp_path, {"a.py": "import pandas as pd\npd.read_csv('in.csv')  # invaract: allow - a lookup\n",
                     "b.py": "def save(df):\n    df.to_csv('out.csv')\n"})
    status, lines, err = run(capsys, "--format", "json", str(tmp_path))
    report = json.loads("\n".join(lines))
    assert status == 1 and err.strip() == "1 read(s) or write(s) outside invaract"
    assert report["scanner"] == {"name": "invaract-lint", "version": __version__}
    assert report["status"] == "findings"
    assert report["files"] == [str(tmp_path / "a.py"), str(tmp_path / "b.py")]
    assert report["counts"]["read"] == {"findings": 0, "exceptions": 1}
    assert report["counts"]["write"] == {"findings": 1, "exceptions": 0}
    assert [(f["path"], f["line"], f["code"], f["allowed"], f["reason"], f["scope"], f["target"]) for f in report["findings"]] == [
        (str(tmp_path / "a.py"), 2, "IVR001", True, "a lookup", "<module>", "in.csv"),
        (str(tmp_path / "b.py"), 2, "IVR002", False, None, "save", "out.csv"),
    ]
    assert report["errors"] == []


def test_json_format_lists_a_file_it_could_not_parse_and_exits_2(tmp_path, capsys):
    write(tmp_path, {"broken.py": "def (:\n"})
    status, lines, err = run(capsys, "--format", "json", str(tmp_path))
    report = json.loads("\n".join(lines))
    assert status == 2 and "broken.py: cannot be checked" in err
    assert report["status"] == "not-scanned" and report["files"] == []
    assert [e["path"] for e in report["errors"]] == [str(tmp_path / "broken.py")] and report["errors"][0]["message"]


def test_a_clean_folder_in_json_exits_0(tmp_path, capsys):
    write(tmp_path, {"job.py": "import invaract\ninvaract.read('orders')\n"})
    status, lines, err = run(capsys, "--format", "json", str(tmp_path))
    assert (status, err, json.loads("\n".join(lines))["status"]) == (0, "", "clean")
