# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What counts as reading or writing data outside invaract: every rule, as data, in one place.

Two things read this catalogue, so they cannot disagree about what a write is: the lint (``checker``), which matches the
names in a job's source against it, and the invaract package's strict mode, which wraps the same names at run time
(``writer_names``). A new library or function is added here once.

A rule is a code (``READ`` ... ``DYNAMIC``), a name for reports (``pandas-read``), and a pattern the fully qualified name
of what the code refers to must match in full (``pandas.read_csv`` for ``pd.read_csv``). Methods of a value the source
does not say the type of (``df.to_parquet``) are matched by the method's name (``FRAME_WRITES``, ``ALWAYS_WRITE``).
"""
import re
from dataclasses import dataclass

READ = "IVR001"
WRITE = "IVR002"
CONNECT = "IVR003"
SQL = "IVR004"
PROCESS = "IVR005"
DYNAMIC = "IVR006"

#: What each code is about, as a word a report can group by.
CATEGORIES = {READ: "read", WRITE: "write", CONNECT: "connect", SQL: "sql", PROCESS: "process", DYNAMIC: "dynamic"}

#: What a finding of each code says the code does.
WHAT = {
    READ: "reads data outside invaract: read it with invaract.read(<name>)",
    WRITE: "writes data outside invaract: write it with invaract.write(<frame>, <name>)",
    CONNECT: "opens a database, storage or network connection outside invaract",
    SQL: "runs SQL that reads or writes files outside invaract",
    PROCESS: "runs another program, whose reads and writes invaract does not see",
    DYNAMIC: "is code the scan cannot follow (built or loaded at run time), so what it reads or writes is not known",
}


@dataclass(frozen=True)
class Rule:
    name: str
    code: str
    pattern: "re.Pattern[str]"


def _rule(name, code, pattern):
    return Rule(name, code, re.compile(pattern))


_PANDAS = r"(?:modin\.)?pandas(?:\.\w+)*"
_PYARROW_IO = r"pyarrow\.(?:parquet|csv|feather|json|orc|ipc|dataset)"
#: Database drivers, by the module their ``connect`` is in.
_DRIVERS = (r"sqlite3|aiosqlite|psycopg2|psycopg|asyncpg|pymysql|aiomysql|pymssql|pyodbc|cx_Oracle|oracledb|duckdb"
            r"|mysql\.connector|snowflake\.connector|teradatasql|redshift_connector|trino\.dbapi|pyhive\.hive"
            r"|databricks\.sql|adbc_driver_\w+\.dbapi")

#: Functions and classes by the qualified name they resolve to. The first rule that matches a name is its rule.
MODULE_RULES = [
    # reads
    _rule("pandas-read", READ, _PANDAS + r"\.(?:read_\w+|ExcelFile)"),
    _rule("polars-read", READ, r"polars(?:\.\w+)*\.(?:read|scan)_\w+"),
    _rule("pyarrow-read", READ, _PYARROW_IO + r"\.(?:read_\w+|open_\w+|dataset|parquet_dataset|ParquetFile|ParquetDataset|ORCFile"
                                              r"|RecordBatchFileReader|RecordBatchStreamReader)|pyarrow\.input_stream"),
    _rule("duckdb-read", READ, r"duckdb\.(?:read_\w+|from_csv_auto|from_parquet)"),
    _rule("numpy-read", READ, r"numpy\.(?:load|loadtxt|genfromtxt|fromfile)"),
    _rule("library-read", READ, r"(?:dask\.dataframe|cudf|geopandas|vaex|pandas_gbq)\.read_\w+|xarray\.open_\w+|joblib\.load"
                                r"|openpyxl\.load_workbook|deltalake\.DeltaTable"),
    # writes
    _rule("pyarrow-write", WRITE, _PYARROW_IO + r"\.(?:write_\w+|new_\w+|ParquetWriter|CSVWriter|ORCWriter|RecordBatchFileWriter"
                                                r"|RecordBatchStreamWriter)"
                                                r"|pyarrow\.(?:create_memory_map|output_stream)|pyarrow\.fs\.copy_files"),
    _rule("pandas-write", WRITE, _PANDAS + r"\.(?:ExcelWriter|HDFStore)"),
    _rule("numpy-write", WRITE, r"numpy\.(?:save|savez|savez_compressed|savetxt|memmap)"),
    _rule("library-write", WRITE, r"deltalake\.(?:write_deltalake|convert_to_deltalake)|joblib\.dump|xlsxwriter\.Workbook|h5py\.File"
                                  r"|pandas_gbq\.to_gbq|shelve\.open"),
    _rule("file-operation", WRITE, r"shutil\.(?:copy|copy2|copyfile|copytree|move|rmtree|make_archive|unpack_archive)"
                                   r"|os\.(?:rename|renames|replace|remove|unlink|rmdir|removedirs|truncate|link|symlink|mkfifo|mknod)"),
    # connections
    _rule("database-connect", CONNECT, r"(?:" + _DRIVERS + r")\.(?:connect|create_pool)"),
    _rule("sqlalchemy-engine", CONNECT, r"(?:sqlalchemy|sqlmodel)(?:\.\w+)*\.create_(?:async_)?engine"),
    _rule("cloud-client", CONNECT, r"google\.cloud\.(?:bigquery|storage)\.Client(?:\.from_\w+)?"
                                   r"|azure\.storage\.(?:blob|filedatalake)(?:\.aio)?\.\w+Client(?:\.from_\w+)?"
                                   r"|clickhouse_connect\.get_client|clickhouse_driver\.Client|pymongo\.MongoClient"
                                   r"|redis\.(?:Redis|StrictRedis)(?:\.from_\w+)?|redis\.from_url|elasticsearch\.Elasticsearch|cassandra\.cluster\.Cluster"
                                   r"|kafka\.Kafka(?:Producer|Consumer)|confluent_kafka\.(?:Producer|Consumer)"
                                   r"|pyiceberg\.catalog\.load_catalog"),
    _rule("storage-client", CONNECT, r"boto3(?:\.session)?\.(?:client|resource|Session)|botocore\.session\.(?:get_session|Session)"
                                     r"|aioboto3\.Session|minio\.Minio|s3fs\.S3FileSystem"
                                     r"|gcsfs\.GCSFileSystem|adlfs\.AzureBlobFileSystem|pyarrow\.fs\.\w*FileSystem(?:\.from_uri)?"
                                     r"|hdfs\.(?:Insecure)?Client|paramiko\.(?:SSHClient|Transport)|pysftp\.Connection"
                                     r"|ftplib\.FTP(?:_TLS)?|smart_open\.open"),
    _rule("fsspec", CONNECT, r"fsspec\.(?:open|open_files|open_local|filesystem|get_mapper)|fsspec(?:\.core)?\.url_to_fs"),
    _rule("http-client", CONNECT, r"requests\.(?:get|post|put|patch|delete|head|request|Session)"
                                  r"|httpx\.(?:get|post|put|patch|delete|head|request|stream|Client|AsyncClient)"
                                  r"|urllib\.request\.(?:urlopen|urlretrieve)|urllib3\.(?:PoolManager|request)|aiohttp\.ClientSession"
                                  r"|http\.client\.HTTPS?Connection"),
    # other programs
    _rule("subprocess", PROCESS, r"subprocess\.(?:run|call|check_call|check_output|Popen|getoutput|getstatusoutput)"
                                 r"|os\.(?:system|popen|startfile|exec[lv]p?e?|spawn[lv]p?e?|posix_spawnp?)"
                                 r"|asyncio\.create_subprocess_(?:exec|shell)|pty\.spawn"),
    # code the scan cannot follow
    _rule("dynamic-code", DYNAMIC, r"exec|eval|runpy\.run_(?:path|module)|importlib\.util\.spec_from_file_location|imp\.load_source"),
    _rule("native-code", DYNAMIC, r"ctypes\.(?:CDLL|PyDLL|WinDLL|OleDLL|(?:cdll|pydll|windll|oledll)(?:\.\w+)*)|cffi\.FFI"),
]

#: Functions that open a file, each with where its mode is among its arguments (position, keyword) and the mode letters
#: that write. A call that gives no mode reads. A mode that writes, or one the source does not spell out, is a write.
_MODE = (1, "mode", "wax+")
OPENERS = {
    "open": _MODE, "io.open": _MODE, "builtins.open": _MODE, "codecs.open": _MODE, "aiofiles.open": _MODE,
    "anyio.open_file": _MODE, "trio.open_file": _MODE, "gzip.open": _MODE, "bz2.open": _MODE, "lzma.open": _MODE,
    "tarfile.open": _MODE, "tarfile.TarFile": _MODE, "tarfile.TarFile.open": _MODE, "zipfile.ZipFile": _MODE,
    "gzip.GzipFile": _MODE, "bz2.BZ2File": _MODE, "lzma.LZMAFile": _MODE, "io.FileIO": _MODE, "os.fdopen": _MODE,
    "tables.open_file": _MODE, "pyarrow.OSFile": _MODE, "pyarrow.memory_map": _MODE,
    "dbm.open": (1, "flag", "wcn"), "argparse.FileType": (0, "mode", "wax+"),
}
#: Mode letters that write, for a method called ``open`` on a value (``Path(...).open("w")``).
WRITE_MODE = "wax+"

#: ``os.open`` flags that leave a file as it is.
OS_OPEN_READ_FLAGS = {"os.O_RDONLY", "os.O_CLOEXEC", "os.O_BINARY", "os.O_NONBLOCK", "os.O_NOFOLLOW", "os.O_DIRECTORY", "os.O_NOCTTY"}

#: Methods of a frame that write once they are given somewhere to write (pandas' ``to_*``, Polars' ``write_*``, numpy's
#: ``tofile``); without one, most of them return the text instead.
FRAME_WRITES = frozenset({
    "to_csv", "to_parquet", "to_json", "to_excel", "to_sql", "to_feather", "to_orc", "to_hdf", "to_pickle", "to_stata", "to_xml",
    "to_gbq", "to_netcdf", "to_zarr", "to_file", "to_postgis", "to_iceberg", "to_delta", "tofile",
    "write_csv", "write_parquet", "write_json", "write_ndjson", "write_ipc", "write_ipc_stream", "write_avro", "write_excel",
    "write_database", "write_delta", "write_iceberg", "write_ods",
})
#: Keyword arguments that name a destination.
DESTINATIONS = frozenset({
    "path", "path_or_buf", "excel_writer", "buf", "con", "name", "file", "target", "table_name", "connection", "uri",
    "workbook", "fname", "fid", "where", "sink", "dest", "base_dir", "filename", "table_or_uri", "destination_table",
    "root_path", "output_file", "destination", "file_name",
})
#: A ``Path``'s writers, whose place is the ``Path`` itself.
PATH_WRITES = frozenset({"write_text", "write_bytes", "touch", "unlink", "rmdir", "symlink_to", "hardlink_to"})
#: A filesystem object's methods that write, whose names say so (pyarrow's ``FileSystem``).
STREAM_WRITES = frozenset({"open_output_stream", "open_append_stream", "copy_file", "delete_file", "delete_dir", "delete_dir_contents"})
#: Methods that always write: a ``Path``'s writers, a filesystem object's, and (by prefix, ``SINK_PREFIX``) Polars'
#: streaming sinks.
ALWAYS_WRITE = PATH_WRITES | STREAM_WRITES
SINK_PREFIX = "sink_"
#: A ``Path``'s methods that move it, flagged only on what the source shows to be a ``Path`` (``str.replace`` and a
#: frame's ``rename`` share the names).
PATH_MOVES = frozenset({"rename", "replace"})

#: Methods that run SQL given as their first argument.
SQL_METHODS = frozenset({"sql", "execute", "executemany", "executescript", "query", "from_query"})
#: A DuckDB connection's methods that read files.
DUCKDB_READS = frozenset({"read_parquet", "read_csv", "read_json", "from_parquet", "from_csv_auto"})

#: The libraries the rules are about, by the first part of their name. A module that is certainly none of them
#: (``import helpers``) is the job's own: its ``helpers.to_csv(rows)`` is a function of the job's, not a frame's method.
DATA_LIBRARIES = frozenset({
    "pandas", "modin", "polars", "pyarrow", "duckdb", "numpy", "dask", "cudf", "geopandas", "vaex", "xarray", "pandas_gbq",
    "deltalake", "joblib", "openpyxl", "xlsxwriter", "h5py", "shelve", "shutil", "os", "io", "builtins", "pathlib",
})

#: Modules whose attributes, looked up by a name built at run time (``getattr(pd, "read_" + kind)``), may be a reader or
#: a writer: such a lookup is a finding of its own. numpy is left out: ``getattr(np, dtype)`` is ordinary numeric code.
DYNAMIC_ROOTS = frozenset({"pandas", "modin", "polars", "pyarrow", "duckdb", "deltalake", "shutil", "os", "builtins", "io",
                           "subprocess", "sqlite3", "fsspec"})
#: The start of a method name built at run time that makes ``getattr(frame, name)`` a reader or writer lookup.
DYNAMIC_METHOD_PREFIXES = ("to_", "write_", "sink_", "read_", "scan_")

#: Calls that make a value held in memory, and names for the console and the null device, never a dataset: handing one to a reader or writer is no
#: read or write of data at rest.
IN_MEMORY = re.compile(r"io\.(?:BytesIO|StringIO)|pyarrow\.(?:BufferOutputStream|BufferReader|py_buffer)")
CONSOLE = re.compile(r"sys\.(?:stdout|stderr|__stdout__|__stderr__)(?:\.buffer)?|os\.devnull")
#: Calls that make a scratch file or folder in the system's temporary folder.
TEMPORARY = re.compile(r"tempfile\.(?:TemporaryDirectory|mkdtemp|mkstemp|gettempdir|NamedTemporaryFile|TemporaryFile"
                       r"|SpooledTemporaryFile)")
#: Calls that make a path.
PATHS = re.compile(r"pathlib\.(?:Path|PurePath|PosixPath|WindowsPath|PurePosixPath|PureWindowsPath)(?:\.(?:cwd|home))?")
#: Methods of a path that return a path.
PATH_METHODS = frozenset({"resolve", "absolute", "expanduser", "with_name", "with_suffix", "with_stem", "joinpath", "relative_to"})
#: Connections whose SQL can read and write files, by the call that makes them.
SQL_CONNECTIONS = {"duckdb.connect": "duckdb", "sqlite3.connect": "sqlite"}

#: The builtins the scan gives a meaning to; a name the file does not bind resolves to itself when it is one of these.
BUILTINS = frozenset({"open", "exec", "eval", "getattr", "__import__", "vars", "str"})


def module_rule(qualified):
    """The rule the qualified name matches, or ``None``."""
    for rule in MODULE_RULES:
        if rule.pattern.fullmatch(qualified):
            return rule
    return None


#: The libraries whose writers are compiled: they write without Python's own ``open``, so nothing but a wrapper around each
#: writer sees one happen. Strict mode wraps these (``writer_names``, ``frame_writer_names``, ``SQL_RUNNERS``).
COMPILED = frozenset({"pyarrow", "polars", "duckdb", "deltalake"})
#: The classes of those libraries whose methods write a frame (``frame_writer_names``), by module.
FRAME_CLASSES = {"polars": ("DataFrame", "LazyFrame"), "duckdb": ("DuckDBPyRelation",)}
#: The classes whose methods run SQL that can reach files (``SQL_METHODS``), by module; the module's own functions of the
#: same names run it on a default connection.
SQL_RUNNERS = {"duckdb": ("DuckDBPyConnection",)}


def writer_names(module, names):
    """Of ``names`` (a module's attributes), the ones the catalogue says write, each with its rule: what strict mode wraps
    in ``module`` so that a write reached by any route is seen. Only writes: reads, connections and the rest are the
    static scan's."""
    found = {}
    for name in names:
        rule = module_rule(f"{module}.{name}")
        if rule is not None and rule.code == WRITE:
            found[name] = rule
    return found


def frame_writer_names(names):
    """Of ``names`` (a frame class's attributes), the methods that write once given somewhere to: ``FRAME_WRITES``, and
    the sinks."""
    return sorted(name for name in names if name in FRAME_WRITES or name.startswith(SINK_PREFIX))
