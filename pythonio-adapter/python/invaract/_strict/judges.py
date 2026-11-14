# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What a call of a wrapped function is about to do, judged from what it is handed.

A library that reads or writes from compiled code raises no event Python can report, so each of its readers and writers is
wrapped (``wrapping``), and a judge looks at the call's arguments before it is made. A judge takes ``(args, kwargs,
parameters)``, ``parameters`` being the wrapped function's own parameter names where Python can say them, and returns a
``Reached`` (what it is about to do, what it reaches, and the local paths that touches) or ``None`` for a call that is no
read or write of data at rest: a buffer, the console, a database in memory, no destination at all.

One judge per shape of call: ``destination`` (a writer), ``source`` (a reader), ``opener`` (a function that takes a name
and a mode), ``sql`` (a function that runs SQL), ``connection`` (a database driver's ``connect``), ``always`` (a call that
is a finding whatever it is handed).
"""
import io
import os
import re
from dataclasses import dataclass
from typing import Optional, Tuple

from . import _state
from .events import in_memory, local_path, program


@dataclass(frozen=True)
class Reached:
    """What a wrapped call is about to do. ``code`` and ``rule`` are given only where they differ from the wrapped
    function's own (an opener reads or writes by its mode)."""

    did: str
    target: Optional[str] = None
    paths: Tuple[str, ...] = ()
    code: Optional[str] = None
    rule: Optional[str] = None


def _named(args, kwargs, parameters, names):
    """The arguments that may say where a call reads or writes: the ones whose parameter is among ``names``, or the first
    argument when the function has no such parameter (or Python cannot say its parameters: then the first two)."""
    if parameters is None:
        chosen = list(args[:2])
    elif any(name in names for name in parameters):
        chosen = [value for name, value in zip(parameters, args) if name in names]
    else:
        chosen = list(args[:1])
    return chosen + [value for key, value in kwargs.items() if key in names]


def _places(path):
    """The local paths a place touches: itself, unless it is an address (``s3://...``), which is never scratch."""
    return () if "://" in path else (path,)


def destination(args, kwargs, parameters=None):
    """Where a writer's call writes. A path among the arguments that may say where is where (of two, the second: a copy's
    destination); a stream that is neither held in memory nor one that was seen when it was opened is reported as it is;
    anything else (a buffer, the console, no destination) is no write at rest."""
    candidates = _named(args, kwargs, parameters, _state.lint.catalogue.DESTINATIONS)
    paths = [local_path(c) for c in candidates if isinstance(c, (str, os.PathLike))]
    if paths:
        path = paths[1] if len(paths) > 1 and len(candidates) == len(paths) == 2 else paths[0]
        return Reached(f"wrote {path}", path, _places(path))
    for candidate in candidates:
        if callable(getattr(candidate, "write", None)) and not held(candidate):
            kind = f"{type(candidate).__module__}.{type(candidate).__qualname__}"
            return Reached(f"wrote to an open stream ({kind})", f"<a stream: {kind}>")
    return None


def held(stream):
    """Whether a stream is certainly no dataset to report now: a buffer, the console, or a file that was seen when it was
    opened (by Python's own ``open``, or by a call this package wraps)."""
    if isinstance(stream, (io.BytesIO, io.StringIO, io.FileIO, io.BufferedWriter, io.BufferedRandom, io.TextIOWrapper)):
        return True
    if getattr(type(stream), "__invaract_watched__", False) or _seen_opening(stream):
        return True
    return type(stream).__name__ in ("BufferOutputStream", "MockOutputStream", "FixedSizeBufferWriter", "PythonFile",
                                     "_TemporaryFileWrapper", "SpooledTemporaryFile")


def _seen_opening(stream):
    try:
        return stream in _state.streams
    except TypeError:  # a stream that cannot be kept track of was not kept track of
        return False


def source(args, kwargs, parameters=None):
    """What a reader's call reads from. A path or address among the arguments that may say (the first of a list of them)
    is it, and so is a file Python opened, by its name; a buffer, bytes, text that is the data itself, or a connection
    (reported where it was made) is no read of data at rest."""
    for candidate in _named(args, kwargs, parameters, _state.lint.catalogue.SOURCES):
        for one in (candidate[:1] if isinstance(candidate, (list, tuple)) else [candidate]):
            where = _read_from(one)
            if where is not None:
                return Reached(f"read {where}", where, _places(where))
    return None


def _read_from(value):
    if isinstance(value, (str, os.PathLike)):
        text = local_path(value)
        # text that is the data itself (JSON, XML, CSV handed over as a string) names no place
        return None if not text.strip() or "\n" in text or text.lstrip()[:1] in "{[<" else text
    name = getattr(value, "name", None)
    return name if isinstance(value, io.IOBase) and isinstance(name, str) else None


#: What an opener's first parameter may be called, for one whose parameters Python cannot say.
_FIRST = ("name", "file", "filename", "path", "filepath")


def opener(position, keyword, letters, reads=False):
    """The judge of a function that opens a file by name and mode (the catalogue's ``OPENERS``): a write when its mode
    writes; with ``reads`` (it opens a dataset), a read otherwise."""
    def judge(args, kwargs, parameters=None):
        mode = args[position] if len(args) > position else kwargs.get(keyword)
        # what it opens is its first parameter, however it is passed
        named = [parameters[0]] if parameters else _FIRST
        path = local_path(args[0] if args else next((kwargs[name] for name in named if name in kwargs), None))
        if path is None:
            return None
        if isinstance(mode, str) and set(mode) & set(letters):
            return Reached(f"opened {path} to write (mode {mode!r})", path, _places(path))
        if reads:
            return Reached(f"read {path}", path, _places(path), code=_state.lint.READ, rule="library-read")
        return None
    return judge


def sql(args, kwargs, parameters=None):
    """SQL about to be run that reaches files, read as the scan reads it: any of the call's texts may be the statement."""
    for statement in list(args) + list(kwargs.values()):
        reached = _state.lint.sqltext.file_access(statement) if isinstance(statement, str) else None
        if reached:
            return Reached(f"ran SQL that reads or writes files ({reached})")
    return None


#: Where a connection string or its keywords name the host: never the whole string, which may hold a password.
_HOST = re.compile(r"(?:\b(?:host|hostaddr|server)\s*=\s*([^\s;,]+))|(?:^\w[\w+.-]*://(?:[^@/]*@)?([^:/?#\s]+))", re.IGNORECASE)


def connection(driver):
    """The judge of a database driver's ``connect``. A database that is a file (SQLite, DuckDB) is reached by its path, and
    one in memory is no finding; any other is reached through its driver, at its host when the call names one."""
    is_file = driver.split(".")[0] in ("sqlite3", "aiosqlite", "duckdb")

    def judge(args, kwargs, parameters=None):
        if is_file:
            database = local_path(args[0] if args else kwargs.get("database"))
            if database is None or in_memory(database):
                return None
            return Reached(f"opened the database {database}", database, (database,))
        host = _host(args, kwargs)
        return Reached(f"connected to a database through {driver}" + (f" at {host}" if host else ""), f"{driver}:{host}" if host else driver)
    return judge


def _host(args, kwargs):
    named = next((kwargs[k] for k in ("host", "hostaddr", "server", "hostname") if isinstance(kwargs.get(k), str)), None)
    if named:
        return named
    for text in list(args[:1]) + [kwargs.get(k) for k in ("dsn", "conninfo", "connection_string", "uri", "url")]:
        found = _HOST.search(text) if isinstance(text, str) else None
        if found:
            return found.group(1) or found.group(2)
    return None


def always(describe):
    """The judge of a call that is a finding whatever it is handed: ``describe(args)`` gives what it did and reached."""
    def judge(args, kwargs, parameters=None):
        did, target = describe(args)
        return Reached(did, target)
    return judge


def child_process(args):
    """``_posixsubprocess.fork_exec``: how Python starts a process when nothing else reports it (a ``multiprocessing``
    worker started with ``spawn``)."""
    name = program(args[0] if args else None)
    return f"started {name}", name


def native_library(args):
    name = local_path(args[0]) if args else None
    return f"loaded the native library {name or 'of this process'}", name
