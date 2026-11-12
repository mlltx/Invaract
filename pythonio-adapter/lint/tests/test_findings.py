# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What a finding says beyond where it is: where it ends, the name it resolves to, the function it is in, the rule that
matched and why, the place it names, whether a marker keeps it and why, and an identifier that survives edits around it."""
import textwrap

import pytest

from invaract_lint import CATEGORIES, CONNECT, DYNAMIC, PROCESS, READ, SQL, WRITE, check_file, check_source, scan_file, scan_source


def one(source, path="<source>"):
    [f] = scan_source(textwrap.dedent(source), path)
    return f


def test_a_finding_as_data_names_every_field():
    f = one("import pandas as pd\n\ndef load():\n    return pd.read_csv('data/in/a.csv')  # invaract: allow - a lookup\n", "job/load.py")
    assert f.to_dict() == {
        "id": f.id, "code": READ, "category": "read", "rule": "pandas-read", "path": "job/load.py",
        "line": 4, "column": 12, "endLine": 4, "endColumn": 40, "call": "pd.read_csv", "resolved": "pandas.read_csv",
        "scope": "load", "snippet": "pd.read_csv('data/in/a.csv')", "target": "data/in/a.csv", "detail": None,
        "allowed": True, "reason": "a lookup",
        "message": "`pd.read_csv` reads data outside invaract: read it with invaract.read(<name>), or mark the line `# invaract: allow`",
    }


def test_each_code_has_its_category():
    assert CATEGORIES == {READ: "read", WRITE: "write", CONNECT: "connect", SQL: "sql", PROCESS: "process", DYNAMIC: "dynamic"}
    assert (READ, WRITE, CONNECT, SQL, PROCESS, DYNAMIC) == ("IVR001", "IVR002", "IVR003", "IVR004", "IVR005", "IVR006")
    assert [f.category for f in scan_source(
        "import pandas as pd, sqlite3, subprocess\npd.read_csv('a')\ndf.to_csv('b')\nsqlite3.connect('c')\n"
        "con.sql(\"COPY t TO 'd'\")\nsubprocess.run(['cp', 'a', 'b'])\nexec(code)\n"
    )] == ["read", "write", "connect", "sql", "process", "dynamic"]


def test_a_call_over_several_lines_ends_where_it_ends():
    f = one("""\
        df.to_parquet(
            'out.parquet',
              compression='snappy')
        """)
    assert (f.line, f.col, f.end_line, f.end_col) == (1, 1, 3, 28)
    assert f.snippet == "df.to_parquet('out.parquet', compression='snappy')"


@pytest.mark.parametrize("source,resolved", [
    ("import pandas as pd\npd.read_csv('a')", "pandas.read_csv"),
    ("from pyarrow import parquet as pq\npq.write_table(t, 'a')", "pyarrow.parquet.write_table"),
    ("from pandas import read_parquet as rp\nrp('a')", "pandas.read_parquet"),
    ("open('a', 'w')", "open"),
    ("df.to_csv('a')", None),
    ("Path('a').write_text('x')", None),
])
def test_resolved_is_the_name_the_call_stands_for_through_the_imports(source, resolved):
    assert one(source).resolved == resolved


def test_scope_is_the_function_or_class_a_call_is_in():
    source = """\
    df.to_csv('top')

    class Loader:
        df.to_csv('class body')

        def load(self):
            df.to_csv('method')

            def inner():
                df.to_csv('nested')

    async def save():
        df.to_csv('async')

    df.to_csv('top again')
    """
    assert [(f.target, f.scope) for f in scan_source(textwrap.dedent(source))] == [
        ("top", "<module>"), ("class body", "Loader"), ("method", "Loader.load"), ("nested", "Loader.load.inner"),
        ("async", "save"), ("top again", "<module>"),
    ]


@pytest.mark.parametrize("source,rule,detail", [
    ("import pandas as pd\npd.read_csv('a')", "pandas-read", None),
    ("import polars as pl\npl.scan_parquet('a')", "polars-read", None),
    ("import pyarrow.parquet as pq\npq.read_table('a')", "pyarrow-read", None),
    ("import duckdb\nduckdb.read_csv('a')", "duckdb-read", None),
    ("import pyarrow.csv as pc\npc.write_csv(t, 'a')", "pyarrow-write", None),
    ("import psycopg\npsycopg.connect()", "database-connect", None),
    ("import sqlalchemy\nsqlalchemy.create_engine(u)", "sqlalchemy-engine", None),
    ("from google.cloud import bigquery\nbigquery.Client()", "cloud-client", None),
    ("from google.cloud import storage\nstorage.Client()", "cloud-client", None),
    ("import boto3\nboto3.resource('s3')", "storage-client", None),
    ("import gcsfs\ngcsfs.GCSFileSystem()", "storage-client", None),
    ("import adlfs\nadlfs.AzureBlobFileSystem()", "storage-client", None),
    ("import fsspec\nfsspec.filesystem('s3')", "fsspec", None),
    ("open('a', 'w')", "open-write-mode", "mode 'w'"),
    ("open('a', mode='a+')", "open-write-mode", "mode 'a+'"),
    ("open('a', m)", "open-write-mode", "mode not spelled out"),
    ("lazy.sink_csv('a')", "always-writes", None),
    ("p.write_text('x')", "always-writes", None),
    ("df.to_excel('a.xlsx')", "frame-write", None),
    ("Path('a').open('wb')", "path-open-write-mode", "mode 'wb'"),
    ("Path('a').open(mode=m)", "path-open-write-mode", "mode not spelled out"),
    ("con.execute(\"COPY t TO 'a'\")", "file-sql", "COPY"),
    ("con.execute(\"export   database 'a'\")", "file-sql", "EXPORT DATABASE"),
    ("con.sql(\"select * from read_parquet ('a')\")", "file-sql", "read_parquet"),
    ("con.sql(\"SELECT * FROM 'a.csv'\")", "file-sql", "FROM 'a.csv'"),
])
def test_each_finding_names_its_rule_and_what_made_it_count(source, rule, detail):
    f = one(source)
    assert (f.rule, f.detail) == (rule, detail)


@pytest.mark.parametrize("source,target", [
    ("open('a.txt', 'w')", "a.txt"),
    ("open(path, 'w')", None),  # the mode is never taken for the place
    ("import io\nio.open('b.txt', 'w')", "b.txt"),
    ("import pyarrow.parquet as pq\npq.write_table(t, 'out.parquet', compression='zstd')", "out.parquet"),
    ("df.to_csv(path_or_buf='c.csv', sep=';')", "c.csv"),
    ("df.to_csv(index=False, path_or_buf=where)", None),
    ("df.to_parquet(compression='snappy', path='d.parquet')", "d.parquet"),
    ("Path('e.txt').write_text('the text, not the place')", "e.txt"),
    ("Path('f.txt').open('w')", "f.txt"),
    ("p.write_bytes(b'x')", None),
    ("Path(p).write_text('x')", None),
    ("lazy.sink_parquet('g.parquet')", "g.parquet"),
    ("import pandas as pd\npd.read_csv('in.csv', sep=';')", "in.csv"),
    ("import pandas as pd\npd.read_csv(where)", None),
    # a connection's argument may be a credential, and SQL's is the query: neither is a place
    ("import sqlite3\nsqlite3.connect('local.db')", None),
    ("con.execute(\"COPY t TO 'h'\")", None),
])
def test_target_is_the_place_a_read_or_write_spells_out(source, target):
    assert one(source).target == target


def test_a_password_in_a_url_is_never_copied_into_a_finding():
    f = one("df.to_sql('t', 'postgresql://reporter:s3cret@db.internal:5432/sales')")
    assert f.target == "t"
    assert f.snippet == "df.to_sql('t', 'postgresql://reporter:***@db.internal:5432/sales')"
    assert "s3cret" not in str(f.to_dict())
    g = one("import sqlalchemy\nsqlalchemy.create_engine('mysql+pymysql://u:pw@h/db')")
    assert g.snippet == "sqlalchemy.create_engine('mysql+pymysql://u:***@h/db')" and g.target is None
    # a URL with no password is left as it is
    assert one("open('s3://bucket/key', 'w')").target == "s3://bucket/key"


def test_long_text_is_cut_at_200_characters():
    exactly = "a" * 200
    assert one(f"open('{exactly}', 'w')").target == exactly
    longer = one(f"open('{'b' * 201}', 'w')").target
    assert longer == "b" * 199 + "…" and len(longer) == 200
    assert len(one(f"open('{'c' * 300}', 'w')").snippet) == 200


def test_a_marker_keeps_a_call_and_says_why():
    source = """\
    import pandas as pd
    a = pd.read_csv('a')  # invaract: allow - a lookup table shipped with the job
    b = pd.read_csv('b')  # invaract: allow
    c = pd.read_csv(
        'c',  # invaract: allow
    )  # invaract: allow: the reason, on the call's last line
    d = pd.read_csv('d')
    e = pd.read_csv('e')  # invaract:allow;  spaced out  
    """
    found = scan_source(textwrap.dedent(source))
    assert [(f.target, f.allowed, f.reason) for f in found] == [
        ("a", True, "a lookup table shipped with the job"),
        ("b", True, None),
        ("c", True, "the reason, on the call's last line"),
        ("d", False, None),
        ("e", True, "spaced out"),
    ]
    assert [f.target for f in check_source(textwrap.dedent(source))] == ["d"]


def test_scan_and_check_of_a_file_differ_only_by_the_calls_a_marker_keeps(tmp_path):
    path = tmp_path / "job.py"
    path.write_text("df.to_csv('a')  # invaract: allow - kept\ndf.to_csv('b')\n")
    assert [(f.target, f.allowed) for f in scan_file(str(path))] == [("a", True), ("b", False)]
    assert [f.target for f in check_file(str(path))] == ["b"]
    assert scan_file(str(path))[0].path == str(path)


def test_the_id_stays_the_same_when_lines_around_the_call_move():
    before = one("def save(df):\n    df.to_csv('a')\n", "job.py")
    after = scan_source("import os\n\n\ndef save(df):\n    df.to_csv('a')  # invaract: allow - it moved and got a marker\n",
                        "job.py")[0]
    assert after.line == 5 and before.line == 2
    assert after.id == before.id and len(before.id) == 16 and int(before.id, 16) >= 0


@pytest.mark.parametrize("source,path", [
    ("def save(df):\n    df.to_csv('a')\n", "other.py"),  # another file
    ("def keep(df):\n    df.to_csv('a')\n", "job.py"),  # another function
    ("def save(df):\n    df.to_csv('b')\n", "job.py"),  # another call
    ("def save(df):\n    df.to_parquet('a')\n", "job.py"),  # the same place, another call
    ("import pandas as pd\ndef save(df):\n    pd.read_csv('a')\n", "job.py"),  # another rule and code
])
def test_the_id_changes_with_the_file_the_function_or_the_call(source, path):
    reference = one("def save(df):\n    df.to_csv('a')\n", "job.py")
    assert one(source, path).id != reference.id


def test_a_call_inside_a_call_that_is_not_flagged_is_still_found():
    assert [f.target for f in scan_source("wrap(df.to_csv('a'))\nprint(len(x), df.to_csv('b'))\n")] == ["a", "b"]


def test_an_sql_method_called_with_nothing_is_not_flagged():
    assert scan_source("con.execute()\ncon.sql()\n") == []


def test_a_path_built_from_parts_names_no_single_place():
    assert one("Path(base, 'x.txt').write_text('t')").target is None


def test_source_given_without_a_path_is_named_source():
    assert scan_source("df.to_csv('a')")[0].path == "<source>"
    assert check_source("df.to_csv('a')")[0].path == "<source>"
