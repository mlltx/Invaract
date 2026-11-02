# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What the lint flags, and what it leaves alone: each rule from both sides."""
import textwrap

import pytest

from invaract_lint import CONNECT, READ, SQL, WRITE, check_source


def found(source):
    return [(f.code, f.call) for f in check_source(textwrap.dedent(source))]


@pytest.mark.parametrize("source,expected", [
    # reads, through every way of importing
    ("import pandas as pd\npd.read_csv('a.csv')", [(READ, "pd.read_csv")]),
    ("import pandas\npandas.read_parquet('a')", [(READ, "pandas.read_parquet")]),
    ("from pandas import read_sql as rs\nrs('q', con)", [(READ, "rs")]),
    ("import polars as pl\npl.read_parquet('a')\npl.scan_csv('b')", [(READ, "pl.read_parquet"), (READ, "pl.scan_csv")]),
    ("import pyarrow.parquet as pq\npq.read_table('a')", [(READ, "pq.read_table")]),
    ("from pyarrow import parquet as pq\npq.ParquetFile('a')", [(READ, "pq.ParquetFile")]),
    ("import pyarrow.parquet\npyarrow.parquet.ParquetDataset('a')", [(READ, "pyarrow.parquet.ParquetDataset")]),
    ("import pyarrow.dataset as ds\nds.dataset('a')", [(READ, "ds.dataset")]),
    ("import pyarrow.csv\npyarrow.csv.open_csv('a')", [(READ, "pyarrow.csv.open_csv")]),
    ("import duckdb\nduckdb.read_parquet('a')", [(READ, "duckdb.read_parquet")]),
    # writes
    ("import pyarrow.parquet as pq\npq.write_table(t, 'a')", [(WRITE, "pq.write_table")]),
    ("import pyarrow.parquet as pq\npq.ParquetWriter('a', s)", [(WRITE, "pq.ParquetWriter")]),
    ("import pyarrow.ipc as ipc\nipc.new_file('a', s)", [(WRITE, "ipc.new_file")]),
    ("import pyarrow.dataset as ds\nds.write_dataset(t, 'a')", [(WRITE, "ds.write_dataset")]),
    ("df.to_parquet('a')", [(WRITE, "df.to_parquet")]),
    ("df.to_csv(path_or_buf='a')", [(WRITE, "df.to_csv")]),
    ("df.to_sql('t', con)", [(WRITE, "df.to_sql")]),
    ("frame.write_parquet('a')", [(WRITE, "frame.write_parquet")]),
    ("frame.write_database(table_name='t', connection=c)", [(WRITE, "frame.write_database")]),
    ("lazy.sink_parquet('a')", [(WRITE, "lazy.sink_parquet")]),
    ("Path('a').write_text('x')", [(WRITE, "Path('a').write_text")]),
    ("p.write_bytes(b'x')", [(WRITE, "p.write_bytes")]),
    ("open('a', 'w')", [(WRITE, "open")]),
    ("open('a', mode='ab')", [(WRITE, "open")]),
    ("open('a', 'x')", [(WRITE, "open")]),
    ("open('a', 'r+')", [(WRITE, "open")]),
    ("open('a', how)", [(WRITE, "open")]),
    ("import io\nio.open('a', 'w')", [(WRITE, "io.open")]),
    ("Path('a').open('w')", [(WRITE, "Path('a').open")]),
    ("Path('a').open(mode=m)", [(WRITE, "Path('a').open")]),
    # connections
    ("import sqlite3\nsqlite3.connect('db')", [(CONNECT, "sqlite3.connect")]),
    ("import duckdb\nduckdb.connect('f.db')", [(CONNECT, "duckdb.connect")]),
    ("import mysql.connector\nmysql.connector.connect()", [(CONNECT, "mysql.connector.connect")]),
    ("from sqlalchemy import create_engine\ncreate_engine('postgresql://')", [(CONNECT, "create_engine")]),
    ("from google.cloud import bigquery\nbigquery.Client()", [(CONNECT, "bigquery.Client")]),
    ("import boto3\nboto3.client('s3')", [(CONNECT, "boto3.client")]),
    ("import s3fs\ns3fs.S3FileSystem()", [(CONNECT, "s3fs.S3FileSystem")]),
    ("import fsspec\nfsspec.open('s3://b/k')", [(CONNECT, "fsspec.open")]),
    # SQL that reads or writes files
    ("con.execute(\"COPY t TO 'out.parquet'\")", [(SQL, "con.execute")]),
    ("duckdb.sql(\"select * from read_parquet('a/*.parquet')\")", [(SQL, "duckdb.sql")]),
    ("con.sql(\"SELECT * FROM 'a.csv'\")", [(SQL, "con.sql")]),
    ("con.query(f\"attach '{path}' as db\")", [(SQL, "con.query")]),
    ("con.execute('EXPORT  DATABASE \\'out\\'')", [(SQL, "con.execute")]),
])
def test_flagged(source, expected):
    assert found(source) == expected


@pytest.mark.parametrize("source", [
    # the package itself
    "import invaract\ninvaract.read('orders')\ninvaract.write(df, 'report')",
    "from invaract import read, write\nread('orders')\nwrite(df, 'report')",
    # frames turned into text, not written
    "df.to_csv()\ndf.to_json(orient='records')\nframe.write_json()",
    # other methods with the same names on things that are not frames, without a destination
    "df.to_dict()\nf.write('x')\nwriter.write_table(t)",
    # reading a file the job ships with
    "open('config.yaml')\nopen('config.yaml', 'r')\nopen('a', mode='rb')\nPath('a').open()",
    # pyarrow and pandas used for data in memory
    "import pyarrow as pa\npa.table({'a': [1]})\nimport pandas as pd\npd.DataFrame({'a': [1]})\npd.concat([a, b])",
    "import pyarrow.parquet as pq\npq.ParquetSchema",
    # SQL over frames in memory
    "import duckdb\nduckdb.sql('select region, sum(total) from df group by 1')\ncon.execute(query)",
    # a local function that happens to share a reader's name
    "def read_csv(x): return x\nread_csv('a')",
    # an import from a relative module is the job's own
    "from .pandas import read_csv\nread_csv('a')",
    # a name that is not imported at all
    "pd.read_csv('a')",
])
def test_not_flagged(source):
    assert found(source) == []


def test_a_rebound_open_is_not_the_builtin():
    assert found("from gzip import open\nopen('a', 'w')") == []


def test_an_allow_comment_on_any_line_of_the_call_keeps_it():
    source = """\
    import pandas as pd
    a = pd.read_csv('a.csv')  # invaract: allow - a lookup table shipped with the job
    b = pd.read_csv(
        'b.csv',  #invaract:allow
    )
    c = pd.read_csv('c.csv')
    d = pd.read_csv('d.csv')  # invaract: allowed? no, only the marker counts when spelled out
    # invaract: allow
    e = pd.read_csv('e.csv')
    """
    # a: marked on its line; b: marked on a line of the call; c, d (not the marker), e (a comment above is not on the call): not
    assert [(f.line, f.call) for f in check_source(textwrap.dedent(source))] == [(6, "pd.read_csv"), (7, "pd.read_csv"), (9, "pd.read_csv")]


def test_a_finding_says_where_it_is_and_what_to_use_instead():
    [f] = check_source("import pandas as pd\n\nx = 1;  pd.read_csv('a')\n", path="job/load.py")
    assert (f.path, f.line, f.col) == ("job/load.py", 3, 9)
    assert str(f) == ("job/load.py:3:9: IVR001 `pd.read_csv` reads data outside invaract: read it with invaract.read(<name>), "
                      "or mark the line `# invaract: allow`")
    assert "invaract.write(<frame>, <name>)" in check_source("df.to_csv('a')")[0].message
    assert "connection" in check_source("import sqlite3\nsqlite3.connect('a')")[0].message
    assert "SQL" in check_source("c.sql(\"COPY t TO 'a'\")")[0].message


def test_findings_come_in_source_order_and_calls_inside_functions_and_imports_inside_functions_count():
    source = """\
    def load():
        import pandas as pd
        return pd.read_csv('b'), pd.read_csv('a')

    def save(df):
        df.to_parquet('c')
    """
    assert [(f.line, f.col) for f in check_source(textwrap.dedent(source))] == [(3, 12), (3, 30), (6, 5)]


def test_source_that_is_not_python_is_an_error_naming_the_file():
    with pytest.raises(SyntaxError) as e:
        check_source("def (:\n", path="job/broken.py")
    assert e.value.filename == "job/broken.py"
