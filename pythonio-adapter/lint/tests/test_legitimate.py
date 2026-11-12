# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Ordinary code the scan must leave alone: every case here once looked, to a scan that matched on names, like a read or a
write of data. A false alarm costs a marker and a reviewer's attention each time, and a team that sees many stops reading
them, so a case that starts being flagged is a regression as much as a missed write is."""
import textwrap

import pytest

from invaract_lint import scan_source


def case(name, source):
    return pytest.param(source, id=name)


CASES = [
    # --- held in memory, never at rest ------------------------------------------------------------------------------------
    case("read from a text buffer", "import io, pandas as pd\npd.read_csv(io.StringIO(text))"),
    case("read from a named buffer", "import io, pandas as pd\nbuf = io.BytesIO(payload)\npd.read_parquet(buf)"),
    case("read from bytes", "import pandas as pd\npd.read_json(b'{}')"),
    case("a frame written to a buffer",
         "import io\ndef encode(df):\n    buf = io.BytesIO()\n    df.to_parquet(buf)\n    return buf.getvalue()"),
    case("a frame written to a buffer by keyword", "import io\nbuf = io.StringIO()\ndf.to_csv(path_or_buf=buf, index=False)"),
    case("a table written to a buffer", "import io\nimport pyarrow.parquet as pq\nbuf = io.BytesIO()\npq.write_table(table, buf)"),
    case("an arrow stream to a buffer", "import pyarrow as pa\nsink = pa.BufferOutputStream()\nw = pa.ipc.new_stream(sink, schema)"),
    case("an arrow stream from bytes", "import pyarrow as pa\npa.ipc.open_stream(pa.BufferReader(data))"),
    case("numpy saved to a buffer", "import io, numpy as np\nbuf = io.BytesIO()\nnp.save(buf, array)"),
    case("a frame's text", "payload = model.to_json()\ntext = df.to_csv(index=False)\nrows = df.to_csv(None)"),
    case("a frame as markdown, a dict, records", "df.to_markdown()\ndf.to_dict('records')\ndf.to_string()\ndf.to_numpy()\ndf.to_list()"),
    case("a frame to another library's frame", "df.to_pandas()\ndf.to_arrow()\ntable.to_pylist()\ndf.to_frame()\ns.to_datetime()"),
    case("pickle and json in memory", "import json, pickle\njson.dumps(x)\njson.loads(s)\npickle.dumps(x)\npickle.loads(b)"),
    case("a csv writer over a buffer", "import csv, io\nout = io.StringIO()\ncsv.writer(out).writerow(row)"),

    # --- the console ----------------------------------------------------------------------------------------------------------
    case("a frame to the console", "import sys\ndf.to_csv(sys.stdout)"),
    case("a frame to stderr's bytes", "import sys\ndf.to_csv(sys.stderr.buffer)"),
    case("print", "import sys\nprint(df.head(), file=sys.stderr)"),
    case("logging", "import logging\nlogging.basicConfig(level=logging.INFO)\nlogging.getLogger(__name__).info('x')"),

    # --- scratch files, in the temporary folder -------------------------------------------------------------------------------
    case("a temporary folder", "import tempfile\nwith tempfile.TemporaryDirectory() as d:\n    pass"),
    case("a frame written to a scratch file",
         "import tempfile\nwith tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:\n    df.to_parquet(tmp.name)"),
    case("a scratch file opened to write", "import tempfile\ntmp = tempfile.mkdtemp()\nopen(f'{tmp}/part.txt', 'w')"),
    case("a scratch path opened to write",
         "import tempfile\nfrom pathlib import Path\nwork = Path(tempfile.mkdtemp())\n(work / 'a.txt').write_text('x')\n(work / 'b').open('w')"),
    case("a scratch file joined and written",
         "import os, tempfile\nwork = tempfile.mkdtemp()\nopen(os.path.join(work, 'a'), 'wb')"),
    case("a scratch folder removed", "import shutil, tempfile\nwork = tempfile.mkdtemp()\nshutil.rmtree(work)"),
    case("a scratch file from mkstemp", "import os, tempfile\nfd, path = tempfile.mkstemp()\nos.remove(path)"),
    case("os.open on a scratch file",
         "import os, tempfile\nwork = tempfile.mkdtemp()\nfd = os.open(os.path.join(work, 'a'), os.O_WRONLY | os.O_CREAT)"),
    case("os.open to read, flags by keyword", "import os\nos.open('a', flags=os.O_RDONLY)"),
    case("a scratch file copied within the scratch folder",
         "import os, shutil, tempfile\nwork = tempfile.mkdtemp()\nshutil.copy(os.path.join(work, 'a'), os.path.join(work, 'b'))"),
    case("a scratch file moved within the scratch folder",
         "import tempfile\nfrom pathlib import Path\nwork = Path(tempfile.mkdtemp())\n(work / 'a').rename(work / 'b')"),
    case("a spooled file", "import tempfile\nwith tempfile.SpooledTemporaryFile() as f:\n    df.to_csv(f)"),

    # --- a database in memory -------------------------------------------------------------------------------------------------
    case("sqlite in memory", "import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.execute('select 1')"),
    case("sqlite in memory, by URI", "import sqlite3\nsqlite3.connect('file::memory:?cache=shared', uri=True)"),
    case("duckdb in memory", "import duckdb\ncon = duckdb.connect()\ncon.execute('select 1')"),
    case("duckdb's default connection", "import duckdb\ncon = duckdb.connect(':default:')\ncon.sql('select 1')"),
    case("duckdb in memory, spelled out", "import duckdb\ncon = duckdb.connect(':memory:')\ncon.sql('select 42').fetchall()"),
    case("duckdb over a frame", "import duckdb\nresult = duckdb.sql('select a, sum(b) from frame group by a').df()"),
    case("duckdb, a value put in", "import duckdb\nduckdb.sql(f'select * from frame where amount > {limit}')"),
    case("duckdb, a column put in", "import duckdb\ncon = duckdb.connect()\ncon.sql(f'select {column} from frame')"),
    case("duckdb, SQL built up line by line",
         "import duckdb\ncon = duckdb.connect()\nq = 'select * from frame'\nq = q + ' where amount > 1'\nq += ' order by amount'\ncon.execute(q)"),
    case("duckdb, parameters bound", "import duckdb\ncon = duckdb.connect()\ncon.execute('select * from frame where id = ?', [key])"),
    case("duckdb, a frame registered", "import duckdb\ncon = duckdb.connect()\ncon.register('orders', frame)\ncon.table('orders').df()"),

    case("a frame written into a database in memory", "import sqlite3\ncon = sqlite3.connect(':memory:')\ndf.to_sql('frame', con, index=False)"),
    case("a frame read from a database in memory",
         "import sqlite3, pandas as pd\ncon = sqlite3.connect(':memory:')\npd.read_sql('select * from frame', con)\n"
         "pd.read_sql_query('select 1', con=con)"),
    case("a frame written into DuckDB in memory", "import duckdb\ncon = duckdb.connect()\nframe.write_database('t', connection=con)"),

    # --- SQL that only looks like it reaches a file --------------------------------------------------------------------------
    case("the word in a value", "cur.execute(\"INSERT INTO log VALUES ('copy failed')\")"),
    case("columns called copy and attach", "cur.execute('SELECT copy, attach FROM t')"),
    case("the word in a comment", "cur.execute('SELECT 1 -- copy this later')"),
    case("the word in a block comment", "cur.execute('/* COPY t TO x */ SELECT 1')"),
    case("a quoted column called copy", "cur.execute('SELECT \"copy to\" FROM t')"),
    case("a date after EXTRACT ... FROM", "cur.execute(\"SELECT EXTRACT(year FROM '2026-01-01'::date)\")"),
    case("TRIM ... FROM a value", "cur.execute(\"SELECT TRIM(BOTH 'x' FROM 'xabcx')\")"),
    case("a table whose name has a dot", "cur.execute('SELECT * FROM analytics.orders')"),
    case("a value compared", "cur.execute(\"SELECT * FROM t WHERE region = 'data/emea'\")"),
    case("an insert from a select", "cur.execute('INSERT INTO t SELECT * FROM staging')"),
    case("a pandas query on a column called copy", "df.query('copy > 3')"),
    case("a pandas query on a column called attach", "df.query('attach == 1 and copy in @wanted')"),
    case("a table formatted in, on a connection that is not DuckDB", "cur.execute(f'SELECT * FROM {table} WHERE id = %s', (key,))"),
    case("SQL not spelled out, on a connection of no known kind", "session.execute(statement)\nengine_connection.execute(query, params)"),
    case("a mock's query", "client.query(sql).result()"),
    case("an executor", "pool.execute(task)\nloop.run_in_executor(None, work)"),

    # --- names that only look like the library's -------------------------------------------------------------------------------
    case("the job's own helper module", "import myutils\nmyutils.to_csv(rows)\nmyutils.write_json(x, y)\nmyutils.read_csv(p)"),
    case("the job's own helper, from-imported", "from helpers import open, read_csv, write_table\nopen('a', 'w')\nread_csv('a')\nwrite_table(t, 'a')"),
    case("a function of the job's called open", "def open(path, mode):\n    return (path, mode)\nopen('a', 'w')"),
    case("a parameter that hides the import", "import pandas as pd\ndef f(pd):\n    return pd.read_csv('a')"),
    case("a local that hides the import", "import pandas as pd\ndef f(make):\n    pd = make()\n    return pd.read_csv('a')"),
    case("a loop variable that hides the import", "import shutil\ndef f(tools):\n    for shutil in tools:\n        shutil.copy('a', 'b')"),
    case("a comprehension variable that hides open", "[open(1, 'w') for open in fakes]"),
    case("a relative import", "from . import pandas\nfrom .io import read_csv\npandas.read_csv('a')\nread_csv('a')"),
    case("a method called open on the job's own class", "class Door:\n    def open(self):\n        pass\nDoor().open()"),
    case("webbrowser", "import webbrowser\nwebbrowser.open('http://example.com', 2)"),
    case("an image opened to read", "from PIL import Image\nImage.open('a.png')"),
    case("str.replace, a frame's rename, a list's remove, a dict's copy",
         "x = s.replace('a', 'b')\ny = df.rename(columns={'a': 'b'})\nz = items.remove(1)\nd.copy()\ndf.replace(0, 1)"),
    case("a string's rename-like methods on a call", "name.strip().replace(' ', '_')\nframe.rename(str.lower, axis=1)"),
    case("os.path and listing", "import os\nos.path.join('a', 'b')\nos.path.exists('a')\nos.listdir('.')\nos.getcwd()\nos.environ.get('X')"),
    case("making a folder", "import os\nfrom pathlib import Path\nos.makedirs('out', exist_ok=True)\nPath('out').mkdir(parents=True, exist_ok=True)"),
    case("shutil that reads", "import shutil\nshutil.which('java')\nshutil.disk_usage('.')\nshutil.get_terminal_size()"),
    case("a path read and inspected",
         "from pathlib import Path\np = Path('config.yaml')\np.read_text()\np.exists()\np.stat()\nlist(p.parent.glob('*.yaml'))\np.with_suffix('.bak')"),
    case("a filesystem object opened by name only", "with fs.open(path) as f:\n    f.read()"),
    case("a filesystem object opened to read", "fs.open(path, 'rb')\narchive.open('member.csv')\narchive.open('member.csv', 'r')"),
    case("a client that talks to no storage", "import requests_mock\nimport responses\nresponses.add(responses.GET, url)"),

    # --- opened to read -----------------------------------------------------------------------------------------------------------
    case("open to read", "open('a')\nopen('a', 'r')\nopen('a', 'rb')\nopen('a', mode='rt', encoding='utf-8')"),
    case("open, mode a constant that reads", "MODE = 'r'\nopen('a', MODE)"),
    case("open, mode one of two that read", "mode = 'r'\nif binary:\n    mode = 'rb'\nopen(path, mode)"),
    case("gzip to read", "import gzip\ngzip.open('a.gz')\ngzip.open('a.gz', 'rt')"),
    case("an archive to read", "import zipfile, tarfile\nzipfile.ZipFile('a.zip')\ntarfile.open('a.tar.gz')\ntarfile.open('a.tar.xz', 'r:xz')"),
    case("os.open to read", "import os\nos.open('a', os.O_RDONLY)\nos.open('a', os.O_RDONLY | os.O_CLOEXEC)\nos.open('a', 0)"),
    case("a path opened to read", "from pathlib import Path\nPath('a').open()\nPath('a').open('rb')\nPath('a').open(encoding='utf-8')"),
    case("the null device", "import os\nopen(os.devnull, 'w').write('x')\ndf.to_csv(os.devnull)"),
    case("an archive built in a buffer",
         "import io, zipfile, tarfile, gzip\nbuf = io.BytesIO()\nwith zipfile.ZipFile(buf, 'w') as z:\n    z.writestr('a', 'x')\n"
         "tarfile.open(fileobj=io.BytesIO(), mode='w')\ngzip.GzipFile(fileobj=buf, mode='wb')"),
    case("an argument that is a file to read", "import argparse\nparser.add_argument('--config', type=argparse.FileType('r'))"),
    case("a memory map to read", "import pyarrow as pa\npa.memory_map('a.arrow')\npa.memory_map('a.arrow', 'r')"),

    # --- a name used as a type, never called ---------------------------------------------------------------------------------------
    case("an annotation", "import requests\ndef fetch(session: requests.Session) -> requests.Response:\n    return session"),
    case("a variable's annotation", "import pyarrow.parquet as pq\nwriter: pq.ParquetWriter"),
    case("isinstance", "import pyarrow.fs as fs\nisinstance(x, fs.S3FileSystem)\nissubclass(t, (fs.LocalFileSystem, fs.S3FileSystem))"),
    case("an exception caught", "import requests\ntry:\n    pass\nexcept requests.Session:\n    pass"),
    case("a cast", "import typing, sqlalchemy\nengine = typing.cast(sqlalchemy.create_engine, value)"),
    case("an annotation in quotes", "import pandas as pd\ndef f(writer: 'pd.ExcelWriter'):\n    pass"),

    # --- imports by name that can be followed -----------------------------------------------------------------------------------------
    case("a module of the job's own, by a name built", "import importlib\nstep = importlib.import_module('jobs.steps.' + name)"),
    case("a module spelled out that is no data library", "import importlib\njson = importlib.import_module('json')\njson.dumps(x)"),
    case("an attribute by name on the job's own object", "getattr(config, name)\ngetattr(self, 'handle_' + kind)(event)"),
    case("an attribute by name on numpy", "import numpy as np\ndtype = getattr(np, kind)"),
    case("a module's doc", "import pandas as pd\npd.__version__\npd.__name__"),

    case("vars of the job's own scope", "names = vars()\nvalues = vars(config)"),
    case("operator's getters on ordinary names",
         "import operator\noperator.attrgetter('total')(row)\noperator.methodcaller('strip')(text)\noperator.attrgetter(name)(row)"),
    case("an attribute by a spelled-out name on the job's own module", "import helpers\nexporters = [getattr(helpers, 'to_csv')]"),

    # --- invaract itself ------------------------------------------------------------------------------------------------------------------
    case("a governed job",
         "import invaract\nwith invaract.run('contract.yaml'):\n    orders = invaract.read('orders')\n"
         "    invaract.write(orders.assign(total=orders.amount * 2), 'totals')"),
    case("a governed job, names imported",
         "from invaract import read, run, write\nwith run('contract.yaml'):\n    write(read('orders'), 'copy')"),
]


@pytest.mark.parametrize("source", CASES)
def test_left_alone(source):
    assert [(f.code, f.rule, f.snippet, f.detail) for f in scan_source(textwrap.dedent(source))] == []


def test_the_corpus_is_broad():
    assert len(CASES) > 85


@pytest.mark.parametrize("marked", [
    "open('audit.log', 'a')  # invaract: allow - the audit trail, not a dataset",
    "import pandas as pd\nlookup = pd.read_csv(  # invaract: allow reference data\n    'regions.csv')",
    "import pandas as pd\nlookup = pd.read_csv(\n    'regions.csv',\n)  # invaract: allow",
])
def test_a_marked_line_is_found_and_kept(marked):
    """What a team marks is no false alarm either: it is found, and reported as kept."""
    [finding] = scan_source(marked)
    assert finding.allowed
