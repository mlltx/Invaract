# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Every way of reading or writing data outside invaract that the scan must find, however the code is written: each case is
a way a read or write could slip past a scan that looked only for the plain call. A case that stops being found is a hole.

Each case gives the source and the ``(code, rule)`` of every finding it must produce, in order. What the scan cannot see
by reading source alone is listed at the end (``NOT_SEEN_BY_THE_SCAN``), each with what does see it."""
import textwrap

import pytest

from invaract_lint import CONNECT, DYNAMIC, PROCESS, READ, SQL, WRITE, scan_source
from invaract_lint.checker import REFERENCED


def found(source):
    return [(f.code, f.rule) for f in scan_source(textwrap.dedent(source))]


def case(name, source, *expected):
    return pytest.param(source, list(expected), id=name)


PANDAS = "import pandas as pd\n"
PQ = "import pyarrow.parquet as pq\n"

CASES = [
    # --- the function under another name --------------------------------------------------------------------------
    case("alias by assignment", PANDAS + "r = pd.read_csv\nr('a.csv')", (READ, "pandas-read")),
    case("alias of an alias", PANDAS + "r = pd.read_csv\ns = r\ns('a.csv')", (READ, "pandas-read")),
    case("alias of a writer", PQ + "w = pq.write_table\nw(t, 'a')", (WRITE, "pyarrow-write")),
    case("alias by walrus", PQ + "(w := pq.write_table)(t, 'a')", (WRITE, "pyarrow-write")),
    case("alias of the module", PANDAS + "frames = pd\nframes.read_csv('a')", (READ, "pandas-read")),
    case("alias in an outer scope, called in a function", PANDAS + "r = pd.read_csv\ndef load():\n    return r('a')",
         (READ, "pandas-read")),
    case("alias of the builtin open", "o = open\no('a', 'w')", (WRITE, "open-write-mode")),
    case("open under an imported name", "from builtins import open as o\no('a', 'w')", (WRITE, "open-write-mode")),
    case("the builtins module", "import builtins\nbuiltins.open('a', 'w')", (WRITE, "open-write-mode")),
    case("__builtins__", "__builtins__.open('a', 'w')", (WRITE, "open-write-mode")),
    case("from-import under another name", "from pandas import read_csv as rc\nrc('a')", (READ, "pandas-read")),
    case("star import", "from pandas import *\nread_csv('a')", (READ, "pandas-read")),
    case("star import of an opener", "from gzip import *\nopen('a.gz', 'wb')", (WRITE, "open-write-mode")),
    case("a submodule by its full path", "import pandas\npandas.io.parsers.read_csv('a')", (READ, "pandas-read")),
    case("a submodule's writer", "import pandas.io.parquet as pio\npio.to_parquet(df, 'a')", (WRITE, "frame-write")),
    case("__call__", PANDAS + "pd.read_csv.__call__('a')", (READ, "pandas-read")),
    case("imported inside the function", "def load():\n    import pandas as pd\n    return pd.read_csv('a')", (READ, "pandas-read")),
    case("imported in a try, set to None otherwise",
         "try:\n    import pandas as pd\nexcept ImportError:\n    pd = None\npd.read_csv('a')", (READ, "pandas-read")),
    case("a global assigned in a function",
         "def setup():\n    global reader\n    import pandas\n    reader = pandas.read_csv\ndef load():\n    return reader('a')",
         (READ, "pandas-read")),
    case("modin's pandas", "import modin.pandas as pd\npd.read_csv('a')", (READ, "pandas-read")),

    # --- the function passed on or stored, never called where the scan can see ------------------------------------------
    case("map", PANDAS + "frames = list(map(pd.read_csv, paths))", (READ, "pandas-read")),
    case("functools.partial", PQ + "import functools\nfunctools.partial(pq.write_table, t)('a')", (WRITE, "pyarrow-write")),
    case("a thread pool", PQ + "pool.submit(pq.write_table, t, 'a')", (WRITE, "pyarrow-write")),
    case("a thread's target", "import threading\nthreading.Thread(target=df.to_parquet, args=('a',)).start()", (WRITE, "frame-write")),
    case("a table of readers", PANDAS + "READERS = {'csv': pd.read_csv, 'json': pd.read_json}\nREADERS[kind](path)",
         (READ, "pandas-read"), (READ, "pandas-read")),
    case("either of two", PANDAS + "(pd.read_csv if fast else pd.read_table)('a')", (READ, "pandas-read"), (READ, "pandas-read")),
    case("a default argument", PANDAS + "def load(path, reader=pd.read_csv):\n    return reader(path)", (READ, "pandas-read")),
    case("returned", PQ + "def writer():\n    return pq.write_table", (WRITE, "pyarrow-write")),
    case("stored on an object", PQ + "self.write = pq.write_table", (WRITE, "pyarrow-write")),
    case("a frame's method stored", "write = df.to_parquet\nwrite('a')", (WRITE, "frame-write")),
    case("a path's method stored", "from pathlib import Path\nw = Path('a').write_text", (WRITE, "always-writes")),
    case("operator.methodcaller", "import operator\noperator.methodcaller('to_parquet', 'a')(df)", (WRITE, "frame-write")),
    case("operator.attrgetter", "import operator\noperator.attrgetter('to_csv')(df)('a')", (WRITE, "frame-write")),
    case("open passed on", "writers = list(map(open, paths, modes))", (WRITE, "open-write-mode")),
    case("a subclass of a client", "import requests\nclass Api(requests.Session):\n    pass", (CONNECT, "http-client")),
    case("os.remove as a callback", "import atexit, os\natexit.register(os.remove, path)", (WRITE, "file-operation")),

    # --- names built or looked up at run time -----------------------------------------------------------------------
    case("getattr, name spelled out, called", PANDAS + "getattr(pd, 'read_csv')('a')", (READ, "pandas-read")),
    case("getattr, name spelled out, stored", PANDAS + "readers = [getattr(pd, 'read_csv')]", (READ, "pandas-read")),
    case("getattr, name spelled out, named", PANDAS + "r = getattr(pd, 'read_csv')\nr('a')", (READ, "pandas-read")),
    case("getattr of a frame's writer, spelled out", "getattr(df, 'to_parquet')('a')", (WRITE, "frame-write")),
    case("getattr, name built", PANDAS + "getattr(pd, 'read_' + kind)('a')", (DYNAMIC, "dynamic-attribute")),
    case("getattr, name a variable", PANDAS + "getattr(pd, name)('a')", (DYNAMIC, "dynamic-attribute")),
    case("getattr on a frame, f-string", "getattr(df, f'to_{fmt}')(path)", (DYNAMIC, "dynamic-attribute")),
    case("getattr on a frame, concatenated", "getattr(df, 'write_' + fmt)(path)", (DYNAMIC, "dynamic-attribute")),
    case("getattr on a frame, formatted", "getattr(df, 'to_%s' % fmt)(path)", (DYNAMIC, "dynamic-attribute")),
    case("getattr on a frame, str.format", "getattr(df, 'to_{}'.format(fmt))(path)", (DYNAMIC, "dynamic-attribute")),
    case("getattr on os", "import os\ngetattr(os, verb)(path)", (DYNAMIC, "dynamic-attribute")),
    case("vars of a module", PANDAS + "vars(pd)[name]('a')", (DYNAMIC, "dynamic-attribute")),
    case("a module's __dict__", PANDAS + "pd.__dict__[name]('a')", (DYNAMIC, "dynamic-attribute")),
    case("import_module, spelled out", "import importlib\npq = importlib.import_module('pyarrow.parquet')\npq.write_table(t, 'a')",
         (WRITE, "pyarrow-write")),
    case("import_module, spelled out, used on the spot",
         "import importlib\nimportlib.import_module('pandas').read_csv('a')", (READ, "pandas-read")),
    case("import_module, the name put together from spelled-out parts",
         "import importlib\nimportlib.import_module('pyarrow.' + 'parquet').write_table(t, 'a')", (WRITE, "pyarrow-write")),
    case("import_module, the name in a variable that is spelled out",
         "import importlib\nname = 'pandas'\nimportlib.import_module(name).read_csv('a')", (READ, "pandas-read")),
    case("__import__, the name in a variable that is spelled out", "name = 'shutil'\n__import__(name).copy('a', 'b')", (WRITE, "file-operation")),
    case("import_module, a relative name built", "import importlib\nimportlib.import_module('.' + name, __package__)", (DYNAMIC, "dynamic-import")),
    case("import_module, no name at all", "import importlib\nimportlib.import_module()", (DYNAMIC, "dynamic-import")),
    case("__import__, no name at all", "__import__()", (DYNAMIC, "dynamic-import")),
    case("getattr on a submodule", "import pyarrow.parquet as pq\ngetattr(pq, name)(t, 'a')", (DYNAMIC, "dynamic-attribute")),
    case("import_module, name a variable", "import importlib\nimportlib.import_module(name).read_csv('a')", (DYNAMIC, "dynamic-import")),
    case("import_module, into a data library", "import importlib\nimportlib.import_module('pandas.' + sub)", (DYNAMIC, "dynamic-import")),
    case("__import__, spelled out", "pd = __import__('pandas')\npd.read_csv('a')", (READ, "pandas-read")),
    case("__import__, name a variable", "__import__(name)", (DYNAMIC, "dynamic-import")),
    case("exec", "exec(\"import pandas as pd; pd.read_csv('a')\")", (DYNAMIC, "dynamic-code")),
    case("exec of a file", "exec(open('step.py').read())", (DYNAMIC, "dynamic-code")),
    case("eval", "eval(code)", (DYNAMIC, "dynamic-code")),
    case("runpy", "import runpy\nrunpy.run_path('step.py')", (DYNAMIC, "dynamic-code")),
    case("a module loaded from a file", "import importlib.util\nimportlib.util.spec_from_file_location('m', path)", (DYNAMIC, "dynamic-code")),
    case("ctypes", "import ctypes\nlibc = ctypes.CDLL('libc.so.6')", (DYNAMIC, "native-code")),
    case("ctypes.cdll", "import ctypes\nctypes.cdll.LoadLibrary('libc.so.6')", (DYNAMIC, "native-code")),
    case("cffi", "from cffi import FFI\nffi = FFI()", (DYNAMIC, "native-code")),

    # --- arguments the source does not spell out -------------------------------------------------------------------------
    case("open, mode a variable", "open(path, mode)", (WRITE, "open-write-mode")),
    case("open, arguments unpacked", "open(*args)", (WRITE, "open-write-mode")),
    case("open, options unpacked", "open(path, **options)", (WRITE, "open-write-mode")),
    case("open, mode by keyword", "open(file=path, mode='w')", (WRITE, "open-write-mode")),
    case("open, mode a constant that writes", "MODE = 'wb'\nopen(path, MODE)", (WRITE, "open-write-mode")),
    case("open, mode one of two", "mode = 'r'\nif update:\n    mode = 'a'\nopen(path, mode)", (WRITE, "open-write-mode")),
    case("a frame's writer, options unpacked", "df.to_parquet(**options)", (WRITE, "frame-write")),
    case("a frame's writer, arguments unpacked", "df.to_csv(*args)", (WRITE, "frame-write")),
    case("a frame's writer, destination by keyword", "df.to_csv(path_or_buf=path, index=False)", (WRITE, "frame-write")),
    case("a buffer handed over with a real path", "import io, numpy as np\nbuf = io.BytesIO()\nnp.save('a.npy', buf)", (WRITE, "numpy-write")),
    case("a buffer elsewhere is no excuse",
         "import io\ndef a():\n    buf = io.BytesIO()\n    return buf\ndef b(df, buf):\n    df.to_parquet(buf)", (WRITE, "frame-write")),
    case("a name now a buffer, now a path",
         "import io\nout = io.BytesIO()\nif to_disk:\n    out = 'data/out.parquet'\ndf.to_parquet(out)", (WRITE, "frame-write")),

    # --- files, by every opener ---------------------------------------------------------------------------------------------
    case("open w", "open('a', 'w')", (WRITE, "open-write-mode")),
    case("open a", "open('a', 'a')", (WRITE, "open-write-mode")),
    case("open x", "open('a', 'xb')", (WRITE, "open-write-mode")),
    case("open r+", "open('a', 'r+')", (WRITE, "open-write-mode")),
    case("io.open", "import io\nio.open('a', 'w')", (WRITE, "open-write-mode")),
    case("codecs.open", "import codecs\ncodecs.open('a', 'w', 'utf-8')", (WRITE, "open-write-mode")),
    case("io.FileIO", "import io\nio.FileIO('a', 'w')", (WRITE, "open-write-mode")),
    case("os.open to write", "import os\nfd = os.open('a', os.O_WRONLY | os.O_CREAT)", (WRITE, "open-write-mode")),
    case("os.open, flags a variable", "import os\nos.open('a', flags)", (WRITE, "open-write-mode")),
    case("os.open, flags a number", "import os\nos.open('a', 577)", (WRITE, "open-write-mode")),
    case("os.fdopen", "import os\nos.fdopen(fd, 'w')", (WRITE, "open-write-mode")),
    case("gzip.open", "import gzip\ngzip.open('a.gz', 'wt')", (WRITE, "open-write-mode")),
    case("gzip.GzipFile", "import gzip\ngzip.GzipFile('a.gz', 'wb')", (WRITE, "open-write-mode")),
    case("bz2.open", "import bz2\nbz2.open('a.bz2', 'wb')", (WRITE, "open-write-mode")),
    case("lzma.open", "import lzma\nlzma.open('a.xz', 'w')", (WRITE, "open-write-mode")),
    case("zipfile to write", "import zipfile\nzipfile.ZipFile('a.zip', 'w')", (WRITE, "open-write-mode")),
    case("zipfile to append", "import zipfile\nzipfile.ZipFile('a.zip', mode='a')", (WRITE, "open-write-mode")),
    case("tarfile to write", "import tarfile\ntarfile.open('a.tar.gz', 'w:gz')", (WRITE, "open-write-mode")),
    case("aiofiles", "import aiofiles\nasync def save():\n    async with aiofiles.open('a', 'w') as f:\n        await f.write('x')",
         (WRITE, "open-write-mode")),
    case("argparse.FileType", "import argparse\nparser.add_argument('--out', type=argparse.FileType('w'))", (WRITE, "open-write-mode")),
    case("pyarrow.OSFile", "import pyarrow as pa\npa.OSFile('a', 'wb')", (WRITE, "open-write-mode")),
    case("a file opened to print to", "print('x', file=open('a.log', 'a'))", (WRITE, "open-write-mode")),
    case("stdout redirected to a file", "import sys\nsys.stdout = open('out.txt', 'w')", (WRITE, "open-write-mode")),
    case("a writer over an opened file", "import csv\ncsv.writer(open('a.csv', 'w', newline=''))", (WRITE, "open-write-mode")),
    case("pickle to an opened file", "import pickle\npickle.dump(obj, open('a.pkl', 'wb'))", (WRITE, "open-write-mode")),
    case("a filesystem object's open", "fs.open('bucket/key', 'wb')", (WRITE, "path-open-write-mode")),
    case("an archive member", "archive.open('member.csv', mode='w')", (WRITE, "path-open-write-mode")),
    case("a scratch file made in the output folder",
         "import tempfile\ntempfile.NamedTemporaryFile('w', dir='data/out', delete=False)", (WRITE, "file-operation")),
    case("an archive, its file named and a buffer beside it",
         "import io, gzip\ngzip.GzipFile('a.gz', 'wb', fileobj=io.BytesIO())", (WRITE, "open-write-mode")),
    case("a frame written into a database in a file",
         "import sqlite3\ncon = sqlite3.connect('warehouse.db')\ndf.to_sql('frame', con)", (CONNECT, "database-connect"), (WRITE, "frame-write")),
    case("a frame written into a database now in memory, now in a file",
         "import sqlite3\ncon = sqlite3.connect(':memory:')\nif keep:\n    con = sqlite3.connect(path)\ndf.to_sql('frame', con)",
         (CONNECT, "database-connect"), (WRITE, "frame-write")),
    case("a frame written into a database handed in", "def save(df, con):\n    df.to_sql('frame', con)", (WRITE, "frame-write")),
    case("a frame read from a database in a file",
         "import sqlite3, pandas as pd\ncon = sqlite3.connect('warehouse.db')\npd.read_sql('select 1', con)", (CONNECT, "database-connect"), (READ, "pandas-read")),
    case("a filesystem's stream", "stream = filesystem.open_output_stream('bucket/key')", (WRITE, "always-writes")),
    case("a filesystem's stream to append", "filesystem.open_append_stream(path)", (WRITE, "always-writes")),
    case("a filesystem's copy and deletes", "filesystem.copy_file(a, b)\nfilesystem.delete_file(a)\nfilesystem.delete_dir_contents(d)",
         (WRITE, "always-writes"), (WRITE, "always-writes"), (WRITE, "always-writes")),
    case("os.open, flags by keyword", "import os\nos.open('a', flags=os.O_WRONLY)", (WRITE, "open-write-mode")),
    case("os.open passed on", "import os\ndescriptors = list(map(os.open, paths, flags))", (WRITE, "open-write-mode")),
    case("a scratch file moved to a path in a variable", "import shutil, tempfile\ntmp = tempfile.mkdtemp()\nshutil.move(tmp, final)",
         (WRITE, "file-operation")),
    case("a path handed over with a buffer",
         "import io, numpy as np\nfrom pathlib import Path\nbuf = io.BytesIO()\nnp.save(Path('a.npy'), buf)", (WRITE, "numpy-write")),
    case("shelve", "import shelve\nshelve.open('state')", (WRITE, "library-write")),
    case("dbm to create", "import dbm\ndbm.open('state', 'c')", (WRITE, "open-write-mode")),

    # --- paths --------------------------------------------------------------------------------------------------------------
    case("Path.write_text", "from pathlib import Path\nPath('a').write_text('x')", (WRITE, "always-writes")),
    case("Path.write_bytes on a joined path", "from pathlib import Path\n(Path('out') / 'a').write_bytes(b'x')", (WRITE, "always-writes")),
    case("Path.open", "from pathlib import Path\nPath('a').open('w')", (WRITE, "path-open-write-mode")),
    case("Path.open, mode a variable", "from pathlib import Path\nPath('a').open(mode)", (WRITE, "path-open-write-mode")),
    case("Path.touch (a marker file)", "from pathlib import Path\nPath('data/out/_SUCCESS').touch()", (WRITE, "always-writes")),
    case("Path.unlink", "from pathlib import Path\nPath('a').unlink()", (WRITE, "always-writes")),
    case("Path.rename", "from pathlib import Path\nPath('a').rename('b')", (WRITE, "path-move")),
    case("Path.replace through a name", "from pathlib import Path\nstaged = Path('tmp/a')\nstaged.replace('data/out/a')", (WRITE, "path-move")),
    case("the unbound method", "import pathlib\npathlib.Path.write_text(p, 'x')", (WRITE, "always-writes")),
    case("a scratch file moved into place",
         "import tempfile\nfrom pathlib import Path\ntmp = Path(tempfile.mkdtemp()) / 'a'\ntmp.rename('data/out/a')", (WRITE, "path-move")),

    # --- copies, moves and deletes --------------------------------------------------------------------------------------------
    case("shutil.copy", "import shutil\nshutil.copy('a', 'b')", (WRITE, "file-operation")),
    case("shutil.copyfile", "import shutil\nshutil.copyfile('a', 'b')", (WRITE, "file-operation")),
    case("shutil.copytree", "import shutil\nshutil.copytree('a', 'b')", (WRITE, "file-operation")),
    case("shutil.move", "import shutil\nshutil.move(tmp, 'data/out/a')", (WRITE, "file-operation")),
    case("shutil.rmtree of an output", "import shutil\nshutil.rmtree('data/out/report')", (WRITE, "file-operation")),
    case("shutil.make_archive", "import shutil\nshutil.make_archive('out', 'zip', 'data')", (WRITE, "file-operation")),
    case("shutil.copy under another name", "from shutil import copy as cp\ncp('a', 'b')", (WRITE, "file-operation")),
    case("os.rename", "import os\nos.rename('a', 'b')", (WRITE, "file-operation")),
    case("os.replace", "import os\nos.replace(staged, final)", (WRITE, "file-operation")),
    case("os.remove", "import os\nos.remove('data/out/old.parquet')", (WRITE, "file-operation")),
    case("os.unlink", "from os import unlink\nunlink(path)", (WRITE, "file-operation")),
    case("os.symlink", "import os\nos.symlink('a', 'b')", (WRITE, "file-operation")),
    case("os.truncate", "import os\nos.truncate('a', 0)", (WRITE, "file-operation")),
    case("a scratch file copied out", "import shutil, tempfile\ntmp = tempfile.mkdtemp()\nshutil.copy(tmp, 'data/out')", (WRITE, "file-operation")),

    # --- pandas ---------------------------------------------------------------------------------------------------------------
    case("pandas readers", PANDAS + "pd.read_parquet('a')\npd.read_json('b')\npd.read_excel('c')\npd.read_sql(q, con)\npd.read_table('d')",
         *[(READ, "pandas-read")] * 5),
    case("pd.ExcelFile", PANDAS + "pd.ExcelFile('a.xlsx')", (READ, "pandas-read")),
    case("pandas writers", "df.to_csv('a')\ndf.to_parquet('b')\ndf.to_json('c')\ndf.to_excel('d')\ndf.to_feather('e')\ndf.to_orc('f')\n"
         "df.to_pickle('g')\ndf.to_hdf('h', key='k')\ndf.to_stata('i')\ndf.to_xml('j')\ndf.to_sql('t', con)\ndf.to_gbq('ds.t')",
         *[(WRITE, "frame-write")] * 12),
    case("pd.to_pickle", PANDAS + "pd.to_pickle(obj, 'a.pkl')", (WRITE, "frame-write")),
    case("pd.ExcelWriter", PANDAS + "with pd.ExcelWriter('a.xlsx') as w:\n    df.to_excel(w)", (WRITE, "pandas-write"), (WRITE, "frame-write")),
    case("pd.HDFStore", PANDAS + "pd.HDFStore('a.h5')", (WRITE, "pandas-write")),
    case("the unbound method", PANDAS + "pd.DataFrame.to_csv(df, 'a')", (WRITE, "frame-write")),
    case("by the type", "type(df).to_parquet(df, 'a')", (WRITE, "frame-write")),
    case("at the end of a chain", "df.groupby('k').sum().reset_index().to_csv('a')", (WRITE, "frame-write")),
    case("on a frame just built", PANDAS + "pd.DataFrame(rows).to_parquet('a')", (WRITE, "frame-write")),
    case("in a subclass", "class Report(Base):\n    def save(self, path):\n        super().to_csv(path)", (WRITE, "frame-write")),

    # --- pyarrow, Polars, DuckDB, numpy and the rest ----------------------------------------------------------------------------
    case("pyarrow readers", PQ + "import pyarrow.csv as pc, pyarrow.dataset as ds, pyarrow.feather as pf, pyarrow.orc as orc\n"
         "pq.read_table('a')\npq.ParquetFile('b')\npq.read_metadata('c')\npc.read_csv('d')\nds.dataset('e')\npf.read_feather('f')\n"
         "orc.ORCFile('g')", *[(READ, "pyarrow-read")] * 7),
    case("pyarrow writers", PQ + "import pyarrow as pa, pyarrow.csv as pc, pyarrow.dataset as ds, pyarrow.feather as pf, pyarrow.orc as orc\n"
         "pq.write_table(t, 'a')\npq.ParquetWriter('b', s)\npq.write_to_dataset(t, 'c')\npc.write_csv(t, 'd')\nds.write_dataset(t, 'e')\n"
         "pf.write_feather(t, 'f')\norc.write_table(t, 'g')\norc.ORCWriter('h')\npa.ipc.new_file('i', s)\npa.output_stream('j')",
         *[(WRITE, "pyarrow-write")] * 10),
    case("pyarrow filesystems", "from pyarrow import fs\nfs.S3FileSystem()\nfs.LocalFileSystem()\nfs.FileSystem.from_uri(uri)\nfs.copy_files('a', 'b')",
         (CONNECT, "storage-client"), (CONNECT, "storage-client"), (CONNECT, "storage-client"), (WRITE, "pyarrow-write")),
    case("polars", "import polars as pl\npl.read_csv('a')\npl.scan_parquet('b').sink_parquet('c')\ndf.write_parquet('d')\ndf.write_database('t', uri)\n"
         "lf.sink_csv('e')", (READ, "polars-read"), (READ, "polars-read"), (WRITE, "always-writes"), (WRITE, "frame-write"),
         (WRITE, "frame-write"), (WRITE, "always-writes")),
    case("duckdb to a file", "import duckdb\ncon = duckdb.connect('warehouse.db')", (CONNECT, "database-connect")),
    case("duckdb, database a variable", "import duckdb\ncon = duckdb.connect(database=path)", (CONNECT, "database-connect")),
    case("duckdb readers", "import duckdb\nduckdb.read_parquet('a')\nduckdb.connect().read_csv('b')", (READ, "duckdb-read"), (READ, "duckdb-read")),
    case("a duckdb result written", "import duckdb\nduckdb.sql('select 1').write_parquet('a')\nduckdb.sql('select 1').to_csv('b')",
         (WRITE, "frame-write"), (WRITE, "frame-write")),
    case("numpy", "import numpy as np\nnp.save('a.npy', x)\nnp.savetxt('b.txt', x)\nnp.savez_compressed('c', x=x)\nnp.load('d.npy')\nnp.loadtxt('e')\n"
         "np.memmap('f', mode='w+')\nx.tofile('g.bin')", (WRITE, "numpy-write"), (WRITE, "numpy-write"), (WRITE, "numpy-write"),
         (READ, "numpy-read"), (READ, "numpy-read"), (WRITE, "numpy-write"), (WRITE, "frame-write")),
    case("deltalake", "from deltalake import DeltaTable, write_deltalake\nwrite_deltalake('t', df)\nDeltaTable('t')",
         (WRITE, "library-write"), (READ, "library-read")),
    case("other libraries", "import joblib, h5py, xlsxwriter, openpyxl, xarray as xr, dask.dataframe as dd\njoblib.dump(m, 'a')\njoblib.load('a')\n"
         "h5py.File('b', 'w')\nxlsxwriter.Workbook('c.xlsx')\nopenpyxl.load_workbook('d.xlsx')\nxr.open_dataset('e.nc')\ndd.read_parquet('f')\n"
         "ds.to_netcdf('g.nc')\ngdf.to_file('h.gpkg')", (WRITE, "library-write"), (READ, "library-read"), (WRITE, "library-write"),
         (WRITE, "library-write"), (READ, "library-read"), (READ, "library-read"), (READ, "library-read"), (WRITE, "frame-write"),
         (WRITE, "frame-write")),

    # --- connections ----------------------------------------------------------------------------------------------------------------
    case("database drivers", "import sqlite3, psycopg, pymysql, pyodbc, asyncpg, snowflake.connector, mysql.connector\n"
         "sqlite3.connect('a.db')\npsycopg.connect(dsn)\npymysql.connect()\npyodbc.connect(dsn)\nasyncpg.connect(dsn)\nasyncpg.create_pool(dsn)\n"
         "snowflake.connector.connect()\nmysql.connector.connect()", *[(CONNECT, "database-connect")] * 8),
    case("an ADBC driver", "import adbc_driver_postgresql.dbapi as pg\npg.connect(uri)", (CONNECT, "database-connect")),
    case("sqlite, database a variable", "import sqlite3\nsqlite3.connect(path)", (CONNECT, "database-connect")),
    case("sqlalchemy", "from sqlalchemy import create_engine\ncreate_engine(url)", (CONNECT, "sqlalchemy-engine")),
    case("sqlalchemy async", "from sqlalchemy.ext.asyncio import create_async_engine\ncreate_async_engine(url)", (CONNECT, "sqlalchemy-engine")),
    case("cloud clients", "from google.cloud import bigquery, storage\nfrom azure.storage.blob import BlobServiceClient\nimport pymongo, redis\n"
         "bigquery.Client()\nstorage.Client()\nBlobServiceClient.from_connection_string(cs)\npymongo.MongoClient(uri)\nredis.Redis.from_url(url)",
         *[(CONNECT, "cloud-client")] * 5),
    case("storage clients", "import boto3, s3fs, gcsfs, adlfs, paramiko, ftplib\nboto3.client('s3')\nboto3.resource('s3')\nboto3.Session()\n"
         "s3fs.S3FileSystem()\ngcsfs.GCSFileSystem()\nadlfs.AzureBlobFileSystem()\nparamiko.SSHClient()\nftplib.FTP(host)",
         *[(CONNECT, "storage-client")] * 8),
    case("boto3.session", "import boto3.session\nboto3.session.Session()", (CONNECT, "storage-client")),
    case("fsspec", "import fsspec\nfsspec.open('s3://b/k', 'wb')\nfsspec.filesystem('gcs')", (CONNECT, "fsspec"), (CONNECT, "fsspec")),
    case("HTTP", "import requests, httpx, urllib.request\nrequests.post(url, data=body)\nrequests.get(url)\nrequests.Session()\nhttpx.Client()\n"
         "urllib.request.urlopen(url)", *[(CONNECT, "http-client")] * 5),
    case("HTTP under another name", "from urllib.request import urlopen\nfrom requests import put as upload\nurlopen(url)\nupload(url, data=b)",
         (CONNECT, "http-client"), (CONNECT, "http-client")),

    # --- SQL that reaches files -------------------------------------------------------------------------------------------------------
    case("COPY", "con.execute(\"COPY orders TO 'out.parquet' (FORMAT PARQUET)\")", (SQL, "file-sql")),
    case("COPY in an f-string", "con.execute(f\"COPY ({query}) TO '{path}'\")", (SQL, "file-sql")),
    case("COPY, formatted", "con.execute(\"COPY {} TO '{}'\".format(table, path))", (SQL, "file-sql")),
    case("COPY, with %", "con.execute(\"COPY %s TO '%s'\" % (table, path))", (SQL, "file-sql")),
    case("COPY, concatenated", "con.execute('COPY ' + table + \" TO '\" + path + \"'\")", (SQL, "file-sql")),
    case("SQL in a variable", "query = \"COPY t TO 'x.parquet'\"\ncon.execute(query)", (SQL, "file-sql")),
    case("SQL in a constant, run in a function", "EXPORT = \"COPY t TO 'x.parquet'\"\ndef run(con):\n    con.execute(EXPORT)", (SQL, "file-sql")),
    case("the second of two statements", "con.execute(\"SET threads = 2; COPY t TO 'x'\")", (SQL, "file-sql")),
    case("over several lines", "con.execute('''\n    -- export\n    COPY (\n        SELECT * FROM t\n    ) TO 'x.csv'\n''')", (SQL, "file-sql")),
    case("ATTACH", "con.execute(\"ATTACH 'other.db' AS other\")", (SQL, "file-sql")),
    case("ATTACH, path joined in", "con.execute(\"ATTACH '\" + path + \"' AS other\")", (SQL, "file-sql")),
    case("VACUUM INTO", "con.execute(\"VACUUM INTO 'backup.db'\")", (SQL, "file-sql")),
    case("INTO OUTFILE", "cur.execute(\"SELECT * FROM t INTO OUTFILE '/tmp/x.csv'\")", (SQL, "file-sql")),
    case("LOAD DATA INFILE", "cur.execute(\"LOAD DATA INFILE 'x.csv' INTO TABLE t\")", (SQL, "file-sql")),
    case("a table function", "con.sql(\"SELECT * FROM read_parquet('data/*.parquet')\")", (SQL, "file-sql")),
    case("a file as a table", "con.sql(\"SELECT * FROM 'data/in/orders.csv'\")", (SQL, "file-sql")),
    case("a file joined", "con.sql(\"SELECT * FROM t JOIN 's3://bucket/k.parquet' USING (id)\")", (SQL, "file-sql")),
    case("through sqlalchemy.text", "from sqlalchemy import text\nconn.execute(text(\"COPY t TO 'x'\"))", (SQL, "file-sql")),
    case("executescript", "con.executescript(\"ATTACH DATABASE 'x.db' AS x;\")", (SQL, "file-sql")),
    case("duckdb's own functions", "import duckdb\nduckdb.sql(\"COPY t TO 'x'\")\nduckdb.execute(\"select * from 'a.csv'\")",
         (SQL, "file-sql"), (SQL, "file-sql")),
    case("SQL not spelled out, on duckdb", "import duckdb\nduckdb.sql(query)", (SQL, "dynamic-sql")),
    case("SQL not spelled out, on a duckdb connection", "import duckdb\ncon = duckdb.connect()\ncon.execute(query)", (SQL, "dynamic-sql")),
    case("SQL not spelled out, on a with-connection", "import duckdb\nwith duckdb.connect() as con:\n    con.sql(build())", (SQL, "dynamic-sql")),
    case("SQL whose table is not spelled out, on duckdb", "import duckdb\nduckdb.sql(f'select * from {source}')", (SQL, "dynamic-sql")),
    case("SQL whose table is formatted in, on a duckdb connection",
         "import duckdb\ncon = duckdb.connect()\ncon.sql('select a from t join {} using (id)'.format(other))", (SQL, "dynamic-sql")),
    case("SQL whose first words are not spelled out, on sqlite",
         "import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.execute(f'{verb} INTO t VALUES (1)')", (SQL, "dynamic-sql")),
    case("a file as a table, its name built", "con.sql(f\"SELECT * FROM '{folder}/orders.parquet'\")", (SQL, "file-sql")),
    case("SQL not spelled out, on a sqlite cursor",
         "import sqlite3\ncon = sqlite3.connect(':memory:')\ncur = con.cursor()\ncur.execute(statement)", (SQL, "dynamic-sql")),

    # --- other programs ---------------------------------------------------------------------------------------------------------------
    case("subprocess", "import subprocess\nsubprocess.run(['cp', 'a', 'b'])\nsubprocess.Popen(cmd)\nsubprocess.check_output(cmd)",
         *[(PROCESS, "subprocess")] * 3),
    case("subprocess under another name", "from subprocess import run as sh\nsh('aws s3 cp a s3://b/a', shell=True)", (PROCESS, "subprocess")),
    case("os.system", "import os\nos.system('echo x > out.txt')", (PROCESS, "subprocess")),
    case("os.popen", "import os\nos.popen('cat a > b')", (PROCESS, "subprocess")),
    case("os.execv", "import os\nos.execv('/bin/cp', ['cp', 'a', 'b'])", (PROCESS, "subprocess")),
    case("asyncio", "import asyncio\nasync def go():\n    await asyncio.create_subprocess_shell('cp a b')", (PROCESS, "subprocess")),

    # --- wherever the code is --------------------------------------------------------------------------------------------------------
    case("in a lambda", "save = lambda df, path: df.to_parquet(path)", (WRITE, "frame-write")),
    case("in a comprehension", PANDAS + "frames = [pd.read_csv(p) for p in paths]", (READ, "pandas-read")),
    case("in a class body", PANDAS + "class Lookup:\n    regions = pd.read_csv('regions.csv')", (READ, "pandas-read")),
    case("in a nested function", PQ + "def outer():\n    def inner(t):\n        pq.write_table(t, 'a')\n    return inner", (WRITE, "pyarrow-write")),
    case("in a decorator's argument", PANDAS + "@cached(pd.read_csv('a'))\ndef f():\n    pass", (READ, "pandas-read")),
    case("in a default argument", PANDAS + "def f(frame=pd.read_csv('a')):\n    pass", (READ, "pandas-read")),
    case("in an f-string", PANDAS + "print(f\"{pd.read_csv('a').shape}\")", (READ, "pandas-read")),
    case("in a match", "match kind:\n    case 'csv':\n        df.to_csv('a')", (WRITE, "frame-write")),
    case("in an except", "try:\n    pass\nexcept Exception:\n    open('errors.log', 'a')", (WRITE, "open-write-mode")),
    case("in an atexit hook", "import atexit\n@atexit.register\ndef flush():\n    open('late.txt', 'w')", (WRITE, "open-write-mode")),
    case("in an async function", "async def save(df):\n    df.to_parquet('a')", (WRITE, "frame-write")),
    case("in a generator", PANDAS + "def frames(paths):\n    for p in paths:\n        yield pd.read_parquet(p)", (READ, "pandas-read")),
    case("in a method of a nested class", "class A:\n    class B:\n        def save(self, df):\n            df.to_csv('a')", (WRITE, "frame-write")),
    case("under if __name__", "if __name__ == '__main__':\n    open('a', 'w')", (WRITE, "open-write-mode")),
    case("a name the class body also binds", PANDAS + "class A:\n    pd = None\n    def load(self):\n        return pd.read_csv('a')", (READ, "pandas-read")),
]


@pytest.mark.parametrize("source,expected", CASES)
def test_found(source, expected):
    assert found(source) == expected


def test_there_are_cases_for_every_code_and_every_module_rule():
    """A rule no case reaches could stop matching without a test failing."""
    from invaract_lint import catalogue
    reached = {rule for _, expected, _ in ((c.values[0], c.values[1], c.id) for c in CASES) for _, rule in expected}
    assert {rule.name for rule in catalogue.MODULE_RULES} <= reached
    assert {code for c in CASES for code, _ in c.values[1]} == {READ, WRITE, CONNECT, SQL, PROCESS, DYNAMIC}
    assert len(CASES) > 200


def test_a_function_referred_to_says_so():
    [f] = scan_source("import pandas as pd\nframes = map(pd.read_csv, paths)")
    assert (f.detail, f.call, f.resolved, f.target) == (REFERENCED, "pd.read_csv", "pandas.read_csv", None)
    [g] = scan_source("import pandas as pd\nreaders = [getattr(pd, 'read_csv')]")
    assert (g.detail, g.resolved, g.target, g.snippet) == (REFERENCED, "pandas.read_csv", None, "getattr(pd, 'read_csv')")


def test_a_finding_of_code_the_scan_cannot_follow_says_what_it_could_not():
    [f] = scan_source("getattr(df, f'to_{fmt}')(path)")
    assert f.detail == "a method whose name starts 'to_', the rest built at run time"
    assert f.message == ("`getattr` is code the scan cannot follow (built or loaded at run time), so what it reads or writes is not "
                         "known, or mark the line `# invaract: allow`")
    [g] = scan_source("import importlib\nimportlib.import_module(name)")
    assert (g.rule, g.detail, g.category) == ("dynamic-import", "the module's name is not spelled out", "dynamic")
    [h] = scan_source("import subprocess\nsubprocess.run(cmd)")
    assert h.category == "process" and "runs another program" in h.message


@pytest.mark.parametrize("source,detail", [
    ("open('a', 'w')", "mode 'w'"),
    ("open('a', m)", "mode not spelled out"),
    ("import tarfile\ntarfile.open('a', 'x:gz')", "mode 'x:gz'"),
    ("import os\nos.open('a', os.O_WRONLY | os.O_CREAT | os.O_TRUNC)", "flags O_CREAT | O_TRUNC | O_WRONLY"),
    ("import os\nos.open('a', os.O_RDWR)", "flags O_RDWR"),
    ("import os\nos.open('a', flags)", "flags not spelled out"),
    ("import os\nos.open('a', 577)", "flags 577"),
    ("import os\nos.open('a')", "flags not spelled out"),
    ("import os\nos.open('a', os.O_RDONLY | mine)", "flags not spelled out"),
    ("import duckdb\nduckdb.sql(q)", "the SQL is not spelled out, and this connection's SQL can read and write files"),
    ("import duckdb\nduckdb.sql(f'select * from {source} where x > 1')",
     "what the SQL reads from is not spelled out, and in DuckDB that can be a file"),
    ("con.sql(f\"select * from '{path}'\")", "FROM '...'"),
    ("con.sql(\"select * from '{}'\".format(path))", "FROM '{}'"),
    ("con.execute(f'COPY t {direction} {path}')", "COPY"),
    ("con.execute('ATTACH ' + path)", "ATTACH"),
])
def test_the_detail_says_what_made_it_a_write(source, detail):
    [f] = scan_source(source)
    assert f.detail == detail


# What reading the source cannot show, each with what does see it. Kept here so the list is one place, and so that a change
# that makes the scan see one of them is noticed (the case then fails, and moves up).
NOT_SEEN_BY_THE_SCAN = {
    "a connection handed to another function, SQL not spelled out there":
        ("def run(con, query):\n    con.execute(query)", "the connection is found where it is made, unless it is in memory"),
    "a frame's writer, on a name the file does not show to be a frame, called with nothing that could be a destination":
        ("df.to_parquet()", "nothing is written: with no destination the frame's writer returns the data"),
    "a write by a path that is a parameter": ("def move(staged, final):\n    staged.rename(final)", "strict mode sees the rename as it happens"),
    "a method reached through globals()": ("globals()['pd'].read_csv('a')", "a read: not policed at run time; pd's own import is in the file"),
    "a log file": ("import logging\nlogging.FileHandler('job.log')", "strict mode sees the file opened; a log is not data"),
}


@pytest.mark.parametrize("source", [pytest.param(s, id=name) for name, (s, _) in NOT_SEEN_BY_THE_SCAN.items()])
def test_what_the_scan_does_not_see_is_known(source):
    assert found(source) == []
