# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The ways a job can write around the invaract package, each as code that really does it, and the ordinary things a job
does that are no such write. ``test_strict.py`` runs them all as real jobs.

Each ``Technique`` is the body of a function in the job. It writes to ``p`` (a path under ``evade/`` that is the
technique's own; ``p + ".csv"`` and the like), and may use ``seed`` (a file under ``seed/`` that exists when the job starts)
and the names the job defines: ``df`` (a pandas frame), ``table`` (an Arrow table), ``pd``, ``np``, ``pa``, ``pq``.

``runtime`` is the rule strict mode must report it by as the job runs (``None``: it cannot see it), and ``static`` the rule
the scan of the source must report it by (``None``: it cannot). Every technique is caught by at least one, and the tests
hold each to exactly what is written here, so a technique that stops being caught fails by name. ``blocked`` says whether
enforcing stops the write from happening at all (it does whenever strict mode sees it before it happens).

To add a case: add a ``T(...)`` line to the group it belongs to (or a line to ``LEGITIMATE`` for something that must raise
nothing), run ``pytest tests/test_strict.py -k <its name>``, and write down what each layer says of it. A case neither layer
catches is a gap to close in invaract-lint's catalogue first (docs/PYTHONIO_LINT_AND_STRICT.md)."""
import importlib.util
from dataclasses import dataclass
from typing import Optional, Tuple, Union


@dataclass(frozen=True)
class Technique:
    name: str
    body: str
    #: the rule strict mode reports it by; several where it depends on how the installed library writes (pandas hands
    #: pyarrow a path in one release and a file it opened in another: each is seen, by a different rule)
    runtime: Union[None, str, Tuple[str, ...]]
    static: Optional[str]
    #: module-level code, before ``import invaract``: what a job does as it is imported
    before: str = ""
    #: whether enforcing prevents the write (None: as ``runtime`` says)
    blocked: Optional[bool] = None
    #: the libraries it needs, when they are not ones the package itself needs
    needs: Union[None, str, Tuple[str, ...]] = None
    #: why strict mode cannot see it, for a technique only the scan catches
    why: str = ""
    #: whether it leaves a file behind (or takes one away) that shows it happened; a network connection does not
    effect: bool = True

    @property
    def stopped(self):
        return self.runtime is not None if self.blocked is None else self.blocked

    @property
    def function(self):
        return "t_" + self.name.replace("-", "_").replace(".", "_")

    @property
    def rules(self):
        """The rules strict mode may report it by: none when it cannot see it."""
        return () if self.runtime is None else (self.runtime,) if isinstance(self.runtime, str) else self.runtime

    @property
    def available(self):
        needs = () if self.needs is None else (self.needs,) if isinstance(self.needs, str) else self.needs
        return all(importlib.util.find_spec(library) is not None for library in needs)


OPEN, FILE, PYARROW, FRAME, SQL, CONNECT, NETWORK, PROCESS = (
    "open-write-mode", "file-operation", "pyarrow-write", "frame-write", "file-sql", "database-connect", "network-connect", "subprocess")
ALWAYS, DYNAMIC_ATTRIBUTE, DYNAMIC_CODE, DYNAMIC_IMPORT, DYNAMIC_SQL, NATIVE = (
    "always-writes", "dynamic-attribute", "dynamic-code", "dynamic-import", "dynamic-sql", "native-code")
PANDAS_READ, PYARROW_READ, POLARS_READ, DUCKDB_READ, NUMPY_READ, LIBRARY_READ, LIBRARY_WRITE, STORAGE = (
    "pandas-read", "pyarrow-read", "polars-read", "duckdb-read", "numpy-read", "library-read", "library-write", "storage-client")
#: A dataset that exists when the job starts, for the techniques that read around the package.
ORDERS = "data/in/orders/part-0.parquet"

T = Technique

TECHNIQUES = [
    # --- Python's own files -------------------------------------------------------------------------------------------------
    T("open-w", "open(p, 'w').write('x')", OPEN, OPEN),
    T("open-a", "open(p, 'a').write('x')", OPEN, OPEN),
    T("open-x", "open(p, 'x').write('x')", OPEN, OPEN),
    T("open-wb", "open(p, 'wb').write(b'x')", OPEN, OPEN),
    T("open-r-plus", "f = open(seed, 'r+')\nf.write('changed')\nf.close()", OPEN, OPEN),
    T("open-mode-keyword", "open(file=p, mode='w').write('x')", OPEN, OPEN),
    T("open-mode-variable", "mode = 'w' if df is not None else 'r'\nopen(p, mode).write('x')", OPEN, OPEN),
    T("open-with", "with open(p, 'w', encoding='utf-8') as f:\n    f.write('x')", OPEN, OPEN),
    T("io.open", "import io\nio.open(p, 'w').write('x')", OPEN, OPEN),
    T("codecs.open", "import codecs\ncodecs.open(p, 'w', 'utf-8').write('x')", OPEN, OPEN),
    T("builtins.open", "import builtins\nbuiltins.open(p, 'w').write('x')", OPEN, OPEN),
    T("os.open", "fd = os.open(p, os.O_WRONLY | os.O_CREAT)\nos.write(fd, b'x')\nos.close(fd)", OPEN, OPEN),
    T("os.fdopen", "f = os.fdopen(os.open(p, os.O_WRONLY | os.O_CREAT), 'w')\nf.write('x')\nf.close()", OPEN, OPEN),
    T("io.FileIO", "import io\nio.FileIO(p, 'w').write(b'x')", OPEN, OPEN),
    T("path.write_text", "from pathlib import Path\nPath(p).write_text('x')", OPEN, ALWAYS),
    T("path.write_bytes", "from pathlib import Path\nPath(p).write_bytes(b'x')", OPEN, ALWAYS),
    T("path.open", "from pathlib import Path\nwith Path(p).open('w') as f:\n    f.write('x')", OPEN, "path-open-write-mode"),
    T("path.touch", "from pathlib import Path\nPath(p).touch()", OPEN, ALWAYS),
    T("gzip", "import gzip\nwith gzip.open(p, 'wt') as f:\n    f.write('x')", OPEN, OPEN),
    T("bz2", "import bz2\nwith bz2.open(p, 'wb') as f:\n    f.write(b'x')", OPEN, OPEN),
    T("lzma", "import lzma\nwith lzma.open(p, 'wb') as f:\n    f.write(b'x')", OPEN, OPEN),
    T("zipfile", "import zipfile\nwith zipfile.ZipFile(p, 'w') as z:\n    z.writestr('a.txt', 'x')", OPEN, OPEN),
    T("tarfile", "import tarfile\ntarfile.open(p, 'w:gz').close()", OPEN, OPEN),
    T("csv.writer", "import csv\nwith open(p, 'w', newline='') as f:\n    csv.writer(f).writerow(['a', 1])", OPEN, OPEN),
    T("json.dump", "import json\nwith open(p, 'w') as f:\n    json.dump({'a': 1}, f)", OPEN, OPEN),
    T("pickle.dump", "import pickle\nwith open(p, 'wb') as f:\n    pickle.dump(df, f)", OPEN, OPEN),
    T("print-to-file", "print('x', file=open(p, 'w'))", OPEN, OPEN),
    T("shelve", "import shelve\nwith shelve.open(p) as db:\n    db['a'] = 1", OPEN, "library-write"),
    T("dbm", "import dbm\nwith dbm.open(p, 'c') as db:\n    db['a'] = '1'", OPEN, OPEN),
    T("logging-file", "import logging\nhandler = logging.FileHandler(p)\nhandler.close()", OPEN, None,
      why="the scan does not treat a log file as data; strict mode sees the file opened"),
    T("mmap", "import mmap\nwith open(seed, 'r+b') as f:\n    m = mmap.mmap(f.fileno(), 0)\n    m[0:1] = b'X'\n    m.close()", OPEN, OPEN),

    # --- copies, moves and deletes -------------------------------------------------------------------------------------------
    T("shutil.copy", "import shutil\nshutil.copy(seed, p)", OPEN, FILE),
    T("shutil.copyfile", "import shutil\nshutil.copyfile(seed, p)", OPEN, FILE),
    T("shutil.copytree", "import shutil\nshutil.copytree('seed', p)", OPEN, FILE),
    T("shutil.move", "import shutil\nshutil.move(seed, p)", FILE, FILE),
    T("shutil.rmtree", "import shutil\nshutil.rmtree(seed + '.d')", FILE, FILE),
    T("shutil.make_archive", "import shutil\nshutil.make_archive(p, 'zip', 'seed')", OPEN, FILE),
    T("os.rename", "os.rename(seed, p)", FILE, FILE),
    T("os.replace", "os.replace(seed, p)", FILE, FILE),
    T("os.remove", "os.remove(seed)", FILE, FILE),
    T("os.unlink", "os.unlink(seed)", FILE, FILE),
    T("os.truncate", "os.truncate(seed, 0)", FILE, FILE),
    T("os.symlink", "os.symlink(os.path.abspath(seed), p)", FILE, FILE),
    T("os.link", "os.link(seed, p)", FILE, FILE),
    T("path.rename", "from pathlib import Path\nPath(seed).rename(p)", FILE, "path-move"),
    T("path.replace", "from pathlib import Path\nPath(seed).replace(p)", FILE, "path-move"),
    T("path.unlink", "from pathlib import Path\nPath(seed).unlink()", FILE, ALWAYS),
    T("scratch-file-moved-out", "import tempfile\nfd, scratch = tempfile.mkstemp()\nos.write(fd, b'x')\nos.close(fd)\nos.replace(scratch, p)",
      FILE, FILE),
    T("scratch-file-copied-out", "import shutil, tempfile\nwith tempfile.NamedTemporaryFile(delete=False) as f:\n    f.write(b'x')\nshutil.copy(f.name, p)",
      OPEN, FILE),

    # --- pandas and numpy ---------------------------------------------------------------------------------------------------
    T("pandas.to_csv", "df.to_csv(p)", OPEN, FRAME),
    T("pandas.to_json", "df.to_json(p)", OPEN, FRAME),
    T("pandas.to_pickle", "df.to_pickle(p)", OPEN, FRAME),
    T("pandas.to_parquet", "df.to_parquet(p)", (OPEN, PYARROW), FRAME),
    T("pandas.to_feather", "df.to_feather(p)", (OPEN, PYARROW), FRAME),
    T("pandas.to_orc", "df.to_orc(p)", (OPEN, PYARROW), FRAME),
    T("pandas.to_excel", "df.to_excel(p + '.xlsx')", OPEN, FRAME, needs="openpyxl"),
    T("pandas.to_sql", "import sqlite3\ncon = sqlite3.connect(p)\ndf.to_sql('t', con)\ncon.close()", CONNECT, CONNECT),
    T("pandas.to_csv-keyword", "df.to_csv(path_or_buf=p, index=False)", OPEN, FRAME),
    T("pandas.to_csv-chained", "df.groupby('region').sum().reset_index().to_csv(p)", OPEN, FRAME),
    T("numpy.save", "np.save(p, np.arange(3))", OPEN, "numpy-write"),
    T("numpy.savetxt", "np.savetxt(p, np.arange(3))", OPEN, "numpy-write"),
    T("numpy.savez", "np.savez(p, a=np.arange(3))", OPEN, "numpy-write"),
    T("numpy.tofile", "np.arange(3).tofile(p)", OPEN, FRAME),
    T("numpy.memmap", "m = np.memmap(p, dtype='uint8', mode='w+', shape=(4,))\nm[:] = 1\nm.flush()", OPEN, "numpy-write"),

    # --- pyarrow: compiled, so only a wrapper sees it ------------------------------------------------------------------------
    T("pq.write_table", "pq.write_table(table, p)", PYARROW, PYARROW),
    T("pq.write_table-keyword", "pq.write_table(table, where=p)", PYARROW, PYARROW),
    T("pq.ParquetWriter", "with pq.ParquetWriter(p, table.schema) as w:\n    w.write_table(table)", PYARROW, PYARROW),
    T("pq.write_to_dataset", "pq.write_to_dataset(table, p)", PYARROW, PYARROW),
    T("pq.write_metadata", "pq.write_metadata(table.schema, p)", PYARROW, PYARROW),
    T("ds.write_dataset", "import pyarrow.dataset as ds\nds.write_dataset(table, p, format='parquet')", PYARROW, PYARROW),
    T("csv.write_csv", "import pyarrow.csv as pc\npc.write_csv(table, p)", PYARROW, PYARROW),
    T("csv.CSVWriter", "import pyarrow.csv as pc\nw = pc.CSVWriter(p, table.schema)\nw.write_table(table)\nw.close()", PYARROW, PYARROW),
    T("feather.write_feather", "import pyarrow.feather as pf\npf.write_feather(table, p)", PYARROW, PYARROW),
    T("orc.write_table", "import pyarrow.orc as orc\norc.write_table(table, p)", PYARROW, PYARROW),
    T("ipc.new_file", "w = pa.ipc.new_file(p, table.schema)\nw.write_table(table)\nw.close()", PYARROW, PYARROW),
    T("ipc.RecordBatchFileWriter", "w = pa.ipc.RecordBatchFileWriter(p, table.schema)\nw.write_table(table)\nw.close()", PYARROW, PYARROW),
    T("pa.OSFile", "f = pa.OSFile(p, 'wb')\nf.write(b'x')\nf.close()", OPEN, OPEN),
    T("pa.output_stream", "f = pa.output_stream(p)\nf.write(b'x')\nf.close()", PYARROW, PYARROW),
    T("pa.create_memory_map", "m = pa.create_memory_map(p, 8)\nm.close()", PYARROW, PYARROW),
    T("fs.copy_files", "import pyarrow.fs as pafs\npafs.copy_files(seed, p)", PYARROW, PYARROW),
    T("from-imported-before-the-run", "write_table_imported_early(table, p)", PYARROW, PYARROW,
      before="from pyarrow.parquet import write_table as write_table_imported_early"),
    T("alias-made-before-the-run", "writer_named_early(table, p)", PYARROW, PYARROW,
      before="import pyarrow.parquet\nwriter_named_early = pyarrow.parquet.write_table"),
    T("module-alias", "import pyarrow.parquet as parquet_files\nparquet_files.write_table(table, p)", PYARROW, PYARROW),
    T("stream-from-a-filesystem-handed-to-a-writer",
      "import pyarrow.fs as pafs\nwith pafs.LocalFileSystem().open_output_stream(p) as stream:\n    pq.write_table(table, stream)",
      ALWAYS, ALWAYS),
    T("filesystem-stream-written-by-hand",
      "import pyarrow.fs as pafs\nwith pafs.LocalFileSystem().open_output_stream(p) as stream:\n    stream.write(b'x')", ALWAYS, ALWAYS),
    T("filesystem-append-stream", "import pyarrow.fs as pafs\nwith pafs.LocalFileSystem().open_append_stream(p) as stream:\n    stream.write(b'x')",
      ALWAYS, ALWAYS),
    T("filesystem-from-a-uri", "import pyarrow.fs as pafs\nfilesystem, where = pafs.FileSystem.from_uri('file://' + os.path.abspath(p))\n"
      "with filesystem.open_output_stream(where) as stream:\n    stream.write(b'x')", ALWAYS, ALWAYS),
    T("filesystem-subtree", "import pyarrow.fs as pafs\ntree = pafs.SubTreeFileSystem(os.path.abspath('evade'), pafs.LocalFileSystem())\n"
      "with tree.open_output_stream(os.path.basename(p)) as stream:\n    stream.write(b'x')", ALWAYS, ALWAYS),
    T("filesystem-copies-a-file", "import pyarrow.fs as pafs\npafs.LocalFileSystem().copy_file(seed, p)", ALWAYS, ALWAYS),
    T("filesystem-moves-a-file", "import pyarrow.fs as pafs\npafs.LocalFileSystem().move(seed, p)", ALWAYS, STORAGE),
    T("filesystem-deletes-a-file", "import pyarrow.fs as pafs\npafs.LocalFileSystem().delete_file(seed)", ALWAYS, ALWAYS),
    T("filesystem-handed-to-a-writer", "import pyarrow.fs as pafs\npq.write_table(table, os.path.abspath(p), filesystem=pafs.LocalFileSystem())",
      PYARROW, PYARROW),
    T("filesystem-remote-from-a-uri", "import pyarrow.fs as pafs\ntry:\n    pafs.FileSystem.from_uri('hdfs://127.0.0.1:9/evade')\n"
      "except OSError:\n    pass", STORAGE, STORAGE, effect=False),
    T("filesystem-made-before-the-run", "with FILESYSTEM_MADE_EARLY.open_output_stream(p) as stream:\n    stream.write(b'x')", None, ALWAYS,
      before="import pyarrow.fs\nFILESYSTEM_MADE_EARLY = pyarrow.fs.LocalFileSystem()",
      why="a filesystem object made before the package was imported is pyarrow's own compiled one, whose methods cannot be watched"),
    T("writer-kept-in-a-table-before-the-run", "WRITERS_KEPT_EARLY['parquet'](table, p)", PYARROW, PYARROW,
      before="import pyarrow.parquet\nWRITERS_KEPT_EARLY = {'parquet': pyarrow.parquet.write_table}"),
    T("compiled-writer-kept-in-a-table-before-the-run", "COMPILED_WRITERS_KEPT_EARLY['csv'](table, p)", PYARROW, PYARROW,
      before="import pyarrow.csv\nCOMPILED_WRITERS_KEPT_EARLY = {'csv': pyarrow.csv.write_csv}"),
    T("compiled-writer-kept-in-a-list-before-the-run", "COMPILED_WRITERS_LISTED_EARLY[0](table, p)", PYARROW, PYARROW,
      before="import pyarrow.csv\nCOMPILED_WRITERS_LISTED_EARLY = [pyarrow.csv.write_csv]"),
    T("compiled-writer-a-default-argument-before-the-run", "write_with_an_early_default(table, p)", PYARROW, PYARROW,
      before="import pyarrow.csv\ndef write_with_an_early_default(t, where, write=pyarrow.csv.write_csv):\n    write(t, where)"),
    T("compiled-writer-closed-over-before-the-run", "CLOSED_OVER_EARLY(table, p)", PYARROW, PYARROW,
      before="import pyarrow.csv\ndef close_over(write):\n    return lambda t, where: write(t, where)\nCLOSED_OVER_EARLY = close_over(pyarrow.csv.write_csv)"),
    T("compiled-writer-an-attribute-before-the-run", "HOLDER_MADE_EARLY.write(table, p)", PYARROW, PYARROW,
      before="import pyarrow.csv, types\nHOLDER_MADE_EARLY = types.SimpleNamespace(write=pyarrow.csv.write_csv)"),
    T("compiled-writer-kept-in-a-tuple-before-the-run", "COMPILED_WRITERS_IN_A_TUPLE_EARLY[0](table, p)", None, PYARROW,
      before="import pyarrow.csv\nCOMPILED_WRITERS_IN_A_TUPLE_EARLY = (pyarrow.csv.write_csv,)",
      why="a compiled function put in a tuple before the package was imported is the original, and a tuple cannot be changed"),

    # --- Polars, DuckDB, deltalake ------------------------------------------------------------------------------------------
    T("polars.write_parquet", "import polars as pl\npl.from_arrow(table).write_parquet(p)", FRAME, FRAME, needs="polars"),
    T("polars.write_csv", "import polars as pl\npl.from_arrow(table).write_csv(p)", FRAME, FRAME, needs="polars"),
    T("polars.write_ipc", "import polars as pl\npl.from_arrow(table).write_ipc(p)", FRAME, FRAME, needs="polars"),
    T("polars.write_ndjson", "import polars as pl\npl.from_arrow(table).write_ndjson(p)", FRAME, FRAME, needs="polars"),
    T("polars.write_json", "import polars as pl\npl.from_arrow(table).write_json(p)", FRAME, FRAME, needs="polars"),
    T("polars.sink_parquet", "import polars as pl\npl.from_arrow(table).lazy().sink_parquet(p)", ALWAYS, ALWAYS, needs="polars"),
    T("polars.sink_csv", "import polars as pl\npl.from_arrow(table).lazy().sink_csv(p)", ALWAYS, ALWAYS, needs="polars"),
    T("polars.write_delta", "import polars as pl\npl.from_arrow(table).write_delta(p)", FRAME, FRAME, needs=("polars", "deltalake")),
    T("duckdb.sql-copy", "import duckdb\nduckdb.sql(f\"COPY (SELECT 1 AS a) TO '{p}.csv'\")", SQL, SQL, needs="duckdb"),
    T("duckdb.execute-copy", "import duckdb\nduckdb.connect().execute(\"COPY (SELECT 1 AS a) TO '\" + p + \".csv'\")", SQL, SQL, needs="duckdb"),
    T("duckdb.copy-built-elsewhere", "import duckdb\ndef build(path):\n    return 'CO' + 'PY (SELECT 1 AS a) TO ' + repr(path + '.csv')\n"
      "def run(con, statement):\n    con.execute(statement)\nrun(duckdb.connect(), build(p))", SQL, None, needs="duckdb",
      why="the scan cannot tell what SQL a connection handed to a function is given"),
    T("duckdb.export-database", "import duckdb\ncon = duckdb.connect()\ncon.execute('CREATE TABLE t AS SELECT 1 AS a')\n"
      "con.execute(f\"EXPORT DATABASE '{p}'\")", SQL, SQL, needs="duckdb"),
    T("duckdb.attach", "import duckdb\ncon = duckdb.connect()\ncon.execute(f\"ATTACH '{p}.db' AS other\")\n"
      "con.execute('CREATE TABLE other.t AS SELECT 1 AS a')\ncon.close()", SQL, SQL, needs="duckdb"),
    T("duckdb.connect-file", "import duckdb\ncon = duckdb.connect(p + '.db')\ncon.execute('CREATE TABLE t AS SELECT 1 AS a')\ncon.close()",
      CONNECT, CONNECT, needs="duckdb"),
    T("duckdb.relation.write_parquet", "import duckdb\nduckdb.sql('SELECT 1 AS a').write_parquet(p)", FRAME, FRAME, needs="duckdb"),
    T("duckdb.relation.to_csv", "import duckdb\nduckdb.sql('SELECT 1 AS a').to_csv(p)", FRAME, FRAME, needs="duckdb"),
    T("deltalake.write_deltalake", "from deltalake import write_deltalake\nwrite_deltalake(p, table)", "library-write", "library-write",
      needs="deltalake"),

    # --- SQLite ------------------------------------------------------------------------------------------------------------------
    T("sqlite.connect-file", "import sqlite3\ncon = sqlite3.connect(p)\ncon.execute('create table t(a)')\ncon.commit()\ncon.close()", CONNECT, CONNECT),
    T("sqlite.attach", "import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.execute('ATTACH ' + repr(p) + ' AS other')\n"
      "con.execute('create table other.t(a)')\ncon.commit()\ncon.close()", SQL, SQL),
    T("sqlite.vacuum-into", "import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.execute('create table t(a)')\n"
      "con.execute(f\"VACUUM INTO '{p}'\")\ncon.close()", SQL, SQL),
    T("sqlite.attach-built-in-place", "import sqlite3\ncon = sqlite3.connect(':memory:')\nstatement = 'ATT' + 'ACH ' + repr(p) + ' AS other'\n"
      "con.execute(statement)\ncon.execute('create table other.t(a)')\ncon.commit()\ncon.close()", SQL, SQL),
    T("sqlite.attach-built-elsewhere", "import sqlite3\ndef build(path):\n    return 'ATT' + 'ACH ' + repr(path) + ' AS other'\n"
      "def attach(con, statement):\n    con.execute(statement)\n    con.execute('create table other.t(a)')\n    con.commit()\n    con.close()\n"
      "attach(sqlite3.connect(':memory:'), build(p))", SQL, None,
      why="the scan cannot tell what SQL a connection handed to a function is given"),

    # --- the call hidden from the scan: strict mode sees the write all the same -------------------------------------------------
    T("getattr-built", "getattr(df, 'to_' + 'csv')(p)", OPEN, DYNAMIC_ATTRIBUTE),
    T("getattr-built-compiled", "getattr(pq, 'write_' + 'table')(table, p)", PYARROW, DYNAMIC_ATTRIBUTE),
    T("getattr-on-a-variable", "kind = 'parquet'\ngetattr(df, f'to_{kind}')(p)", (OPEN, PYARROW), DYNAMIC_ATTRIBUTE),
    T("vars-lookup", "vars(pq)['write_table'](table, p)", PYARROW, DYNAMIC_ATTRIBUTE),
    T("exec", "exec('open(p, \"w\").write(\"x\")', {'p': p})", OPEN, DYNAMIC_CODE),
    T("eval", "eval('open(p, \"w\")', {'p': p, 'open': open}).close()", OPEN, DYNAMIC_CODE),
    T("compile-and-exec", "exec(compile('import pyarrow.parquet as q\\nq.write_table(table, p)', 'built', 'exec'), {'table': table, 'p': p})",
      PYARROW, DYNAMIC_CODE),
    T("import-by-built-name", "import importlib\nimportlib.import_module('pyarrow.' + 'parquet').write_table(table, p)", PYARROW, PYARROW),
    T("import-by-a-name-from-elsewhere", "import importlib\nimportlib.import_module(os.environ.get('WRITER', 'pyarrow.parquet')).write_table(table, p)",
      PYARROW, DYNAMIC_IMPORT),
    T("dunder-import", "name = 'shutil'\n__import__(name).copy(seed, p)", OPEN, FILE),
    T("dunder-import-of-a-name-from-elsewhere", "__import__(os.environ.get('COPIER', 'shutil')).copy(seed, p)", OPEN, DYNAMIC_IMPORT),
    T("functools.partial", "import functools\nfunctools.partial(pq.write_table, table)(p)", PYARROW, PYARROW),
    T("map", "list(map(lambda path: pq.write_table(table, path), [p]))", PYARROW, PYARROW),
    T("lambda", "save = lambda path: df.to_csv(path)\nsave(p)", OPEN, FRAME),
    T("operator.methodcaller", "import operator\noperator.methodcaller('to_csv', p)(df)", OPEN, FRAME),
    T("unbound-method", "pd.DataFrame.to_csv(df, p)", OPEN, FRAME),
    T("nested-function", "def inner(path):\n    def innermost():\n        pq.write_table(table, path)\n    innermost()\ninner(p)", PYARROW, PYARROW),
    T("class-method", "class Saver:\n    def __call__(self, path):\n        df.to_csv(path)\nSaver()(p)", OPEN, FRAME),
    T("generator", "def parts():\n    yield pq.write_table(table, p)\nlist(parts())", PYARROW, PYARROW),
    T("comprehension", "[pq.write_table(table, path) for path in [p]]", PYARROW, PYARROW),
    T("decorator", "def saved(function):\n    def wrapper(path):\n        function(path)\n        df.to_csv(path)\n    return wrapper\n"
      "@saved\ndef work(path):\n    pass\nwork(p)", OPEN, FRAME),
    T("helper-in-another-module", "import evade_helper\nevade_helper.save(table, p)", PYARROW, PYARROW),
    T("except-handler", "try:\n    raise ValueError('x')\nexcept ValueError:\n    open(p, 'w').write('x')", OPEN, OPEN),
    T("finally-block", "try:\n    pass\nfinally:\n    open(p, 'w').write('x')", OPEN, OPEN),

    # --- threads --------------------------------------------------------------------------------------------------------------
    T("thread-running-the-jobs-function", "import threading\ndef work():\n    pq.write_table(table, p)\nt = threading.Thread(target=work)\nt.start()\nt.join()",
      PYARROW, PYARROW),
    T("thread-pool", "from concurrent.futures import ThreadPoolExecutor\nwith ThreadPoolExecutor(1) as pool:\n"
      "    try:\n        pool.submit(lambda: open(p, 'w').write('x')).result()\n    except Exception:\n        pass", OPEN, OPEN),
    T("thread-given-the-writer-itself", "import threading\nt = threading.Thread(target=df.to_csv, args=(p,))\nt.start()\nt.join()", OPEN, FRAME),
    T("thread-given-a-compiled-writer", "import threading\nt = threading.Thread(target=pq.write_table, args=(table, p))\nt.start()\nt.join()",
      PYARROW, PYARROW),

    # --- other processes ---------------------------------------------------------------------------------------------------------
    T("subprocess.run", "import subprocess\nsubprocess.run(['cp', seed, p])", PROCESS, PROCESS),
    T("subprocess.Popen", "import subprocess\nsubprocess.Popen(['cp', seed, p]).wait()", PROCESS, PROCESS),
    T("subprocess-shell", "import subprocess\nsubprocess.run(f'cp {seed} {p}', shell=True)", PROCESS, PROCESS),
    T("os.system", "os.system(f'cp {seed} {p}')", PROCESS, PROCESS),
    T("os.popen", "os.popen(f'cp {seed} {p}').close()", PROCESS, PROCESS),
    T("os.posix_spawn", "pid = os.posix_spawn('/bin/cp', ['cp', seed, p], os.environ)\nos.waitpid(pid, 0)", PROCESS, PROCESS),
    T("forked-child", "pid = os.fork()\nif pid == 0:\n    status = 0\n    try:\n        open(p, 'w').write('x')\n    except BaseException:\n        status = 1\n"
      "    os._exit(status)\nos.waitpid(pid, 0)", OPEN, OPEN),
    T("multiprocessing-fork", "import multiprocessing, evade_helper\nprocess = multiprocessing.get_context('fork').Process(target=evade_helper.save, args=(table, p))\n"
      "process.start()\nprocess.join()", PYARROW, PYARROW),
    T("program-started-through-a-module-that-asks-the-system", "import uuid\nuuid._get_command_stdout('cp', seed, p)", PROCESS, None),
    T("multiprocessing-spawn", "import multiprocessing, evade_helper\nprocess = multiprocessing.get_context('spawn').Process(target=evade_helper.save_plain, args=(p,))\n"
      "process.start()\nprocess.join()", PROCESS, OPEN),

    # --- the network -------------------------------------------------------------------------------------------------------------
    T("socket", "import socket\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\nserver.listen(1)\nclient = socket.socket()\n"
      "try:\n    client.connect(server.getsockname())\n    client.sendall(b'data')\nexcept OSError:\n    pass\n"
      "finally:\n    client.close()\n    server.close()", NETWORK, None, effect=False,
      why="the scan names the clients it knows (requests, boto3, ...), not a bare socket"),
    T("urllib", "import socket, urllib.request\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\nserver.listen(1)\n"
      "try:\n    urllib.request.urlopen('http://127.0.0.1:%d/upload' % server.getsockname()[1], data=b'x', timeout=0.2)\nexcept OSError:\n    pass\n"
      "finally:\n    server.close()", NETWORK, "http-client", effect=False),
    T("http.client", "import http.client, socket\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\nserver.listen(1)\n"
      "connection = http.client.HTTPConnection('127.0.0.1', server.getsockname()[1], timeout=0.2)\n"
      "try:\n    connection.request('PUT', '/upload', body=b'x')\nexcept OSError:\n    pass\nfinally:\n    connection.close()\n    server.close()",
      NETWORK, "http-client", effect=False),

    T("udp", "import socket\nsender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)\ntry:\n    sender.sendto(b'data', ('127.0.0.1', 9))\n"
      "except OSError:\n    pass\nfinally:\n    sender.close()", NETWORK, None, effect=False,
      why="the scan names the clients it knows (requests, boto3, ...), not a bare socket"),

    # --- database drivers that connect from compiled code -------------------------------------------------------------------
    T("psycopg2", "import psycopg2\ntry:\n    psycopg2.connect(host='127.0.0.1', port=9, dbname='sales', connect_timeout=1)\nexcept psycopg2.Error:\n    pass",
      CONNECT, CONNECT, needs="psycopg2", effect=False),
    T("psycopg2-connection-string", "import psycopg2\ntry:\n    psycopg2.connect('host=127.0.0.1 port=9 dbname=sales connect_timeout=1')\n"
      "except psycopg2.Error:\n    pass", CONNECT, CONNECT, needs="psycopg2", effect=False),
    T("psycopg", "import psycopg\ntry:\n    psycopg.connect('host=127.0.0.1 port=9 dbname=sales connect_timeout=1')\nexcept psycopg.Error:\n    pass",
      CONNECT, CONNECT, needs="psycopg", effect=False),
    T("psycopg-from-imported", "from psycopg import connect\ntry:\n    connect('host=127.0.0.1 port=9 dbname=sales connect_timeout=1')\n"
      "except Exception as e:\n    if type(e).__module__.startswith('invaract'):\n        raise", CONNECT, CONNECT, needs="psycopg", effect=False),

    # --- scientific and statistical files, written and read by compiled code ------------------------------------------------
    T("h5py", "import h5py\nwith h5py.File(p, 'w') as f:\n    f['total'] = [1, 2]", OPEN, OPEN, needs="h5py"),
    T("h5py-append", "import h5py\nwith h5py.File(p, 'a') as f:\n    f['total'] = [1, 2]", OPEN, OPEN, needs="h5py"),
    T("h5py-mode-keyword", "import h5py\nwith h5py.File(name=p, mode='w') as f:\n    f['total'] = [1, 2]", OPEN, OPEN, needs="h5py"),
    T("netCDF4", "import netCDF4\nnetCDF4.Dataset(p, 'w').close()", OPEN, OPEN, needs="netCDF4"),
    T("pytables", "import tables\ntables.open_file(p, 'w').close()", OPEN, OPEN, needs="tables"),
    T("pandas.to_hdf", "df.to_hdf(p, key='report')", OPEN, FRAME, needs="tables"),
    T("pandas.HDFStore", "with pd.HDFStore(p, 'w') as store:\n    store['report'] = df", OPEN, "pandas-write", needs="tables"),
    T("pyreadstat.write_sav", "import pyreadstat\npyreadstat.write_sav(df, p)", LIBRARY_WRITE, LIBRARY_WRITE, needs="pyreadstat"),
    T("pyreadstat.write_dta", "import pyreadstat\npyreadstat.write_dta(df, p)", LIBRARY_WRITE, LIBRARY_WRITE, needs="pyreadstat"),
    T("pyreadstat.write_xport", "import pyreadstat\npyreadstat.write_xport(df, p)", LIBRARY_WRITE, LIBRARY_WRITE, needs="pyreadstat"),
    T("dbm.dumb", "import dbm.dumb\nwith dbm.dumb.open(p, 'c') as db:\n    db['a'] = '1'", OPEN, OPEN),

    # --- reads around the package ---------------------------------------------------------------------------------------------
    T("read-pandas-csv", "pd.read_csv(seed, header=None)", PANDAS_READ, PANDAS_READ, effect=False),
    T("read-pandas-parquet", f"pd.read_parquet('{ORDERS}')", PANDAS_READ, PANDAS_READ, effect=False),
    T("read-pandas-an-open-file", "with open(seed) as f:\n    pd.read_csv(f, header=None)", PANDAS_READ, PANDAS_READ, effect=False),
    T("read-pandas-getattr", "getattr(pd, 'read_' + 'csv')(seed, header=None)", PANDAS_READ, DYNAMIC_ATTRIBUTE, effect=False),
    T("read-pandas-from-imported", "from pandas import read_csv\nread_csv(seed, header=None)", PANDAS_READ, PANDAS_READ, effect=False),
    T("read-pandas-kept-before-the-run", "READERS_KEPT_EARLY['csv'](seed, header=None)", PANDAS_READ, PANDAS_READ, effect=False,
      before="import pandas\nREADERS_KEPT_EARLY = {'csv': pandas.read_csv}"),
    T("read-pandas-a-url", "import socket\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\ntry:\n"
      "    pd.read_csv('http://127.0.0.1:%d/orders.csv' % server.getsockname()[1])\nexcept OSError:\n    pass\nfinally:\n    server.close()",
      PANDAS_READ, PANDAS_READ, effect=False),
    T("read-pyarrow-table", f"pq.read_table('{ORDERS}')", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-a-folder", "pq.read_table('data/in/orders')", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-file", f"pq.ParquetFile('{ORDERS}').read()", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-metadata", f"pq.read_metadata('{ORDERS}')", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-dataset", "import pyarrow.dataset as ds\nds.dataset('data/in/orders').to_table()", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-parquet-dataset", "pq.ParquetDataset('data/in/orders').read()", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-csv", "import pyarrow.csv\npyarrow.csv.read_csv(seed)", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-a-list-of-files", f"pq.read_table(['{ORDERS}'])", PYARROW_READ, PYARROW_READ, effect=False),
    T("read-pyarrow-filesystem-stream", f"import pyarrow.fs as pafs\nwith pafs.LocalFileSystem().open_input_file('{ORDERS}') as f:\n    f.read()",
      PYARROW_READ, STORAGE, effect=False),
    T("read-pyarrow-a-file-it-mapped-itself", f"pq.read_table(pa.memory_map('{ORDERS}'))", None, PYARROW_READ, effect=False,
      why="a file pyarrow opens or maps to read is a plain file, as one from open() is: a reader is seen by the path it is handed"),
    T("read-numpy-loadtxt", "np.loadtxt(seed, dtype=str)", NUMPY_READ, NUMPY_READ, effect=False),
    T("read-numpy-genfromtxt", "np.genfromtxt(seed, dtype=str)", NUMPY_READ, NUMPY_READ, effect=False),
    T("read-numpy-fromfile", "np.fromfile(seed, dtype='uint8')", NUMPY_READ, NUMPY_READ, effect=False),
    T("read-polars-parquet", f"import polars as pl\npl.read_parquet('{ORDERS}')", POLARS_READ, POLARS_READ, needs="polars", effect=False),
    T("read-polars-scan", f"import polars as pl\npl.scan_parquet('{ORDERS}').collect()", POLARS_READ, POLARS_READ, needs="polars", effect=False),
    T("read-polars-csv", "import polars as pl\npl.read_csv(seed, has_header=False)", POLARS_READ, POLARS_READ, needs="polars", effect=False),
    T("read-polars-sql", f"import polars as pl\npl.sql(\"select * from read_parquet('{ORDERS}')\").collect()", SQL, SQL, needs="polars", effect=False),
    T("read-polars-sql-context", f"import polars as pl\npl.SQLContext().execute(\"select * from read_parquet('{ORDERS}')\").collect()",
      SQL, SQL, needs="polars", effect=False),
    T("read-duckdb-parquet", f"import duckdb\nduckdb.read_parquet('{ORDERS}').fetchall()", DUCKDB_READ, DUCKDB_READ, needs="duckdb", effect=False),
    T("read-duckdb-connection", f"import duckdb\ncon = duckdb.connect()\ncon.read_parquet('{ORDERS}').fetchall()\ncon.close()",
      DUCKDB_READ, DUCKDB_READ, needs="duckdb", effect=False),
    T("read-duckdb-sql", f"import duckdb\nduckdb.sql(\"select * from read_parquet('{ORDERS}')\").fetchall()", SQL, SQL, needs="duckdb", effect=False),
    T("read-duckdb-relation-query", f"import duckdb\nduckdb.sql('select 1 as a').query('one', \"select * from one, read_parquet('{ORDERS}')\").fetchall()",
      SQL, None, needs="duckdb", effect=False),
    T("read-h5py", "import h5py\ntry:\n    h5py.File(seed, 'r')\nexcept OSError:\n    pass", LIBRARY_READ, LIBRARY_READ, needs="h5py", effect=False),
    T("read-h5py-default-mode", "import h5py\ntry:\n    h5py.File(seed)\nexcept OSError:\n    pass", LIBRARY_READ, LIBRARY_READ, needs="h5py", effect=False),
    T("read-netCDF4", "import netCDF4\ntry:\n    netCDF4.Dataset(seed)\nexcept OSError:\n    pass", LIBRARY_READ, LIBRARY_READ, needs="netCDF4", effect=False),
    T("read-pytables", "import tables\ntry:\n    tables.open_file(seed)\nexcept (OSError, tables.HDF5ExtError):\n    pass", LIBRARY_READ, LIBRARY_READ,
      needs="tables", effect=False),
    T("read-pyreadstat", "import pyreadstat\ntry:\n    pyreadstat.read_sav(seed)\nexcept pyreadstat.ReadstatError:\n    pass", LIBRARY_READ, LIBRARY_READ,
      needs="pyreadstat", effect=False),

    # --- native code ----------------------------------------------------------------------------------------------------------
    T("native-code", "import ctypes\nlibc = ctypes.CDLL(None)\nlibc.fopen.restype = ctypes.c_void_p\n"
      "handle = libc.fopen(p.encode(), b'w')\nlibc.fclose(ctypes.c_void_p(handle))", NATIVE, NATIVE),
    T("native-code-library-loaded-before-the-run", "LIBC_LOADED_EARLY.fopen.restype = ctypes.c_void_p\n"
      "handle = LIBC_LOADED_EARLY.fopen(p.encode(), b'w')\nLIBC_LOADED_EARLY.fclose(ctypes.c_void_p(handle))", NATIVE, NATIVE,
      before="import ctypes\nLIBC_LOADED_EARLY = ctypes.CDLL(None)"),
    T("native-code-python-itself", "import ctypes\nctypes.pythonapi.Py_GetVersion.restype = ctypes.c_char_p\nctypes.pythonapi.Py_GetVersion()",
      NATIVE, NATIVE, effect=False),
    T("native-code-cffi", "import cffi\nffi = cffi.FFI()\nffi.cdef('void *fopen(const char *, const char *); int fclose(void *);')\n"
      "libc = ffi.dlopen(None)\nlibc.fclose(libc.fopen(p.encode(), b'w'))", NATIVE, NATIVE, needs="cffi"),
    T("native-code-the-jobs-own-compiled-module", "import importlib.util, shutil\norigin = importlib.util.find_spec('_lsprof').origin\n"
      "os.makedirs(scratch + '/native', exist_ok=True)\nshutil.copy(origin, scratch + '/native')\nsys.path.insert(0, scratch + '/native')\n"
      "sys.modules.pop('_lsprof', None)\ntry:\n    import _lsprof\nfinally:\n    sys.path.remove(scratch + '/native')", NATIVE, FILE, effect=False),

    # --- what strict mode cannot see: the scan must ----------------------------------------------------------------------------
    T("written-as-the-job-is-imported", "pass", None, OPEN, before="open('evade/written-as-the-job-is-imported.out', 'w').write('x')",
      why="the run has not opened yet: strict mode watches a run"),
    T("written-after-the-run", "import atexit\natexit.register(lambda: open(p, 'w').write('x'))", None, OPEN,
      why="the run has ended: strict mode watches a run"),
]

#: The helper module the job imports while it runs.
HELPER = """\
import pyarrow.parquet as pq


def save(table, path):
    pq.write_table(table, path)


def save_plain(path):
    with open(path, "w") as f:
        f.write("x")
"""

BY_NAME = {t.name: t for t in TECHNIQUES}
assert len(BY_NAME) == len(TECHNIQUES), "two techniques share a name"


# What a job does that is no write around the package: each must leave strict mode with nothing to say.
LEGITIMATE = {
    "reads a file": "open(seed).read()\nopen(seed, 'rb').read()\nwith open(seed, encoding='utf-8') as f:\n    f.readline()",
    "reads scratch files with the libraries": "pq.write_table(table, scratch + '/in.parquet')\npq.read_table(scratch + '/in.parquet')\n"
                                              "pd.read_parquet(scratch + '/in.parquet')\npq.ParquetFile(scratch + '/in.parquet').read()\n"
                                              "import pyarrow.dataset as ds\nds.dataset(scratch + '/in.parquet').to_table()\n"
                                              "np.save(scratch + '/a.npy', np.arange(3))\nnp.load(scratch + '/a.npy')",
    "reads data it holds in memory": "import io\npd.read_csv(io.StringIO('a,b\\n1,2'))\npd.read_json(io.StringIO('[{\"a\": 1}]'))\n"
                                     "buf = io.BytesIO()\npq.write_table(table, buf)\npq.read_table(io.BytesIO(buf.getvalue()))\n"
                                     "pq.read_table(pa.BufferReader(buf.getvalue()))\npd.read_parquet(io.BytesIO(buf.getvalue()))\n"
                                     "saved = io.BytesIO()\nnp.save(saved, np.arange(3))\nnp.load(io.BytesIO(saved.getvalue()))",
    "reads the files a library ships": "np.genfromtxt(os.path.join(os.path.dirname(os.__file__), 'this.py'), dtype=str, delimiter='\\x00', comments=None)\n"
                                       "pd.read_csv(os.path.join(os.path.dirname(os.__file__), 'this.py'), sep='\\x00', header=None, engine='python')",
    "reads through the package": "invaract.read('orders')",
    "uses a pyarrow filesystem on scratch files": "import pyarrow.fs as pafs\nlocal = pafs.LocalFileSystem()\n"
                                                  "assert isinstance(local, pafs.LocalFileSystem) and isinstance(local, pafs.FileSystem)\n"
                                                  "with local.open_output_stream(scratch + '/fs.bin') as out:\n    out.write(b'x')\n"
                                                  "with local.open_input_file(scratch + '/fs.bin') as opened:\n    opened.read()\n"
                                                  "pq.write_table(table, scratch + '/fs.parquet', filesystem=local)\n"
                                                  "pq.read_table(scratch + '/fs.parquet', filesystem=local)\n"
                                                  "local.get_file_info('seed')\nlocal.get_file_info(pafs.FileSelector('seed'))\n"
                                                  "found, where = pafs.FileSystem.from_uri('file://' + scratch)\n"
                                                  "assert isinstance(found, pafs.LocalFileSystem) and where == scratch\n"
                                                  "local.copy_file(scratch + '/fs.bin', scratch + '/fs2.bin')\nlocal.delete_file(scratch + '/fs2.bin')\n"
                                                  "assert local == pafs.LocalFileSystem() and local.type_name == 'local'",
    "computes in forked workers": "import multiprocessing\nwith multiprocessing.get_context('fork').Pool(2) as pool:\n"
                                  "    assert pool.map(abs, [-1, -2]) == [1, 2]",
    "listens on a socket": "import socket\nserver = socket.socket()\nserver.bind(('127.0.0.1', 0))\nserver.listen(1)\nserver.close()",
    "imports libraries that use native code themselves": "import ctypes, ctypes.util, uuid, platform, locale\nuuid.uuid1()\nuuid.getnode()\nplatform.platform()\n"
                                                         "platform.uname().processor\nplatform.architecture()\nctypes.util.find_library('c')\n"
                                                         "ctypes.c_int(3).value\nctypes.sizeof(ctypes.c_double)\nnp.arange(3).ctypes.data",
    "lists and inspects files": "os.listdir('seed')\nos.path.exists(seed)\nos.stat(seed)\nos.makedirs('made/folder', exist_ok=True)\n"
                                "from pathlib import Path\nlist(Path('seed').glob('*'))\nPath(seed).read_text()",
    "writes to a buffer": "import io\nbuf = io.BytesIO()\npq.write_table(table, buf)\ndf.to_parquet(io.BytesIO())\ndf.to_csv(io.StringIO())\n"
                          "sink = pa.BufferOutputStream()\npq.write_table(table, sink)\nwriter = pa.ipc.new_stream(pa.BufferOutputStream(), table.schema)\n"
                          "writer.write_table(table)\nwriter.close()\nnp.save(io.BytesIO(), np.arange(3))",
    "writes text it keeps": "text = df.to_csv()\njson_text = df.to_json()\ndf.to_dict('records')\ndf.to_markdown() if False else None",
    "writes scratch files": "import tempfile\nwith tempfile.NamedTemporaryFile('w', suffix='.csv') as f:\n    df.to_csv(f.name)\n"
                            "work = tempfile.mkdtemp()\npq.write_table(table, os.path.join(work, 'a.parquet'))\ndf.to_parquet(os.path.join(work, 'b.parquet'))\n"
                            "open(os.path.join(work, 'c.txt'), 'w').write('x')\nos.rename(os.path.join(work, 'c.txt'), os.path.join(work, 'd.txt'))\n"
                            "os.remove(os.path.join(work, 'd.txt'))\nimport shutil\nshutil.rmtree(work)\n"
                            "with tempfile.TemporaryDirectory() as d:\n    np.save(os.path.join(d, 'a.npy'), np.arange(3))\n"
                            "with tempfile.SpooledTemporaryFile() as f:\n    f.write(b'x')\nfd, path = tempfile.mkstemp()\nos.close(fd)\nos.remove(path)",
    "writes to the console and the null device": "print('progress')\nsys.stderr.write('note\\n')\ndf.to_csv(sys.stdout)\nopen(os.devnull, 'w').write('x')",
    "logs to the console": "import logging\nlogging.basicConfig(level=logging.INFO)\nlogging.getLogger('job').info('started')",
    "imports its own module for the first time": "import legit_helper\nlegit_helper.double(2)",
    "uses a database in memory": "import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.execute('create table t(a)')\ncon.execute('insert into t values (1)')\n"
                                 "con.execute('select * from t').fetchall()\ndf.to_sql('frame', con, index=False)\npd.read_sql('select * from frame', con)\ncon.close()",
    "computes in threads": "from concurrent.futures import ThreadPoolExecutor\nwith ThreadPoolExecutor(2) as pool:\n    list(pool.map(lambda n: n * 2, range(4)))\n"
                           "import threading\nt = threading.Thread(target=lambda: df.describe())\nt.start()\nt.join()",
    "ordinary Python": "import json, hashlib, uuid, random, datetime, re, collections, decimal\njson.dumps({'a': 1})\nhashlib.sha256(b'x').hexdigest()\n"
                       "uuid.uuid4()\nrandom.random()\ndatetime.datetime.now()\nre.compile('a+').match('aa')\nos.environ.get('HOME')\nos.getcwd()",
    "reads an archive": "import gzip, zipfile, io\nbuf = io.BytesIO()\nwith zipfile.ZipFile(buf, 'w') as z:\n    z.writestr('a', 'x')\n"
                        "zipfile.ZipFile(io.BytesIO(buf.getvalue())).read('a')\ngzip.decompress(gzip.compress(b'x'))",
    "raises and handles an error": "try:\n    open('no/such/file')\nexcept OSError:\n    pass\ntry:\n    pq.read_table(scratch + '/no/such.parquet')\n"
                                   "except Exception:\n    pass",
    "works on frames": "df.assign(double=df.total * 2).groupby('region').sum()\ndf.rename(columns={'total': 't'})\ndf.replace(1, 2)\n"
                       "table.to_pandas()\ntable.to_pylist()\npa.Table.from_pandas(df)",
}

#: The same, for the libraries the package does not itself need.
LEGITIMATE_WITH = {
    "h5py": {
        "keeps a scientific file in scratch": "import h5py\nwith h5py.File(scratch + '/a.h5', 'w') as f:\n    f['total'] = [1, 2]\n"
                                              "with h5py.File(scratch + '/a.h5', 'r') as f:\n    list(f['total'])\n"
                                              "with h5py.File(scratch + '/core.h5', 'w', driver='core', backing_store=False) as f:\n    f['a'] = [1]\n"
                                              "assert isinstance(h5py.File(scratch + '/a.h5'), h5py.File)",
    },
    "psycopg2": {
        "imports a database driver and connects to nothing": "import psycopg2, psycopg2.extras, psycopg2.sql\n"
                                                             "psycopg2.sql.SQL('select {}').format(psycopg2.sql.Literal(1))\npsycopg2.Binary(b'x')",
    },
    "polars": {
        "works in Polars": "import polars as pl, io\nframe = pl.from_arrow(table)\nframe.write_csv()\nframe.write_json()\nframe.write_parquet(io.BytesIO())\n"
                           "frame.lazy().filter(pl.col('total') > 1).collect()\nframe.to_pandas()\nframe.to_arrow()\n"
                           "pl.sql('select region from frame where total > 1').collect()\n"
                           "pl.SQLContext(orders=frame).execute('select sum(total) from orders').collect()\n"
                           "frame.write_parquet(scratch + '/pl.parquet')\npl.read_parquet(scratch + '/pl.parquet')\n"
                           "pl.scan_parquet(scratch + '/pl.parquet').collect()\npl.read_csv(io.BytesIO(b'a,b\\n1,2'))",
    },
    "duckdb": {
        "queries frames in DuckDB": "import duckdb\nduckdb.sql('select region, sum(total) from df group by region').df()\ncon = duckdb.connect()\n"
                                    "con.register('orders', df)\ncon.execute('select * from orders where total > ?', [1]).fetchall()\n"
                                    "con.execute('create table t as select * from orders')\ncon.sql('select count(*) from t').fetchall()\n"
                                    "con.execute(\"select 'copy this to that' as note\").fetchall()\nduckdb.sql('select 1 as a').to_df()\n"
                                    "duckdb.sql('select 1 as a').query('one', 'select a + 1 from one').fetchall()\n"
                                                                        "duckdb.sql('select 1 as a').write_parquet(scratch + '/d.parquet')\nduckdb.read_parquet(scratch + '/d.parquet').fetchall()\n"
                                    "con.close()",
    },
}

LEGIT_HELPER = "def double(n):\n    return n * 2\n"
