# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Putting strict mode's hooks in place: Python's audit hook, and a wrapper around every reader, writer and ``connect``
that Python's own events do not cover.

``install`` is done once for the process. It adds the audit hook (``events``), wraps what the catalogue names in every
library already loaded, and adds an import hook that wraps a library as it loads. Nothing installed does anything while no
watcher is set (``_state.watcher``).

What is wrapped comes from invaract-lint's catalogue, so the scan and the watch share one list: ``plan`` turns a loaded
module into ``(owner, attribute, Watch)`` for each thing to wrap, and ``_apply`` wraps it. A function is replaced where it
lives and in every module that holds it under a name of its own; a class has its construction wrapped, in place where
Python lets a class be changed, else in a subclass that takes its place; a filesystem class is replaced by a subclass
whose reading and writing methods are watched.

To watch something the catalogue cannot describe (a function of the standard library, a hook after the call): add it to
``OTHERS``.
"""
import functools
import gc
import importlib.abc
import importlib.util
import inspect
import os
import sys
import threading
import types
from dataclasses import dataclass
from typing import Callable, Optional

from ..errors import CheckNotRun, ContractViolation
from . import _state, events, judges
from ._state import Seen

#: What the catalogue names that could not be wrapped, each with why.
UNWATCHED = {}

_installed = False
_installing = threading.Lock()
#: Each wrapped function that other modules may hold under a name of their own: ``id(original) -> (original, wrapper)``.
_replaced = {}
#: The wrappers' own references to their originals (a closure cell, ``__wrapped__``): never rebound.
_own = set()

WATCHED = "__invaract_watched__"
ORIGINAL = "__invaract_original__"


@dataclass(frozen=True)
class Watch:
    """How one function or class is watched: what a finding calls it, the catalogue's code and rule for it, and the judge
    of its calls (``judges``).

    ``method``: it is a method, so its first argument is the object and no part of what the judge reads. ``as_caller``: it
    looks at the code that calls it (DuckDB finds the frames its SQL names among its caller's variables), so it is called
    from a frame that has the caller's variables. ``after``: called with what the call returned, when a run is watching.
    ``direct``: it counts only when the job's own code called it (``_state.Seen.direct``). ``everywhere``: other modules
    that hold it under their own name are given the wrapper too."""

    call: str
    code: str
    rule: str
    judge: Callable
    method: bool = False
    as_caller: bool = False
    after: Optional[Callable] = None
    direct: Optional[str] = None
    everywhere: bool = True


def install(lint):
    """Installs, once for the process, the audit hook, the hook that wraps a library as it is imported, and the hook that
    marks a forked child; then wraps what is already loaded."""
    global _installed
    with _installing:
        _state.lint = lint
        if not _installed:
            sys.addaudithook(_audit)
            sys.meta_path.insert(0, WrapOnImport())
            if hasattr(os, "register_at_fork"):
                os.register_at_fork(after_in_child=_in_child)
            _installed = True
        wrap_loaded()


def _audit(event, args):
    watcher = _state.watcher
    if watcher is None or event not in events.EVENTS or _state.quiet():
        return
    seen = events.describe(event, args)
    if seen is not None:
        watcher.saw(seen)


def _in_child():
    """After a fork, in the child: it keeps watching, and leaves what it sees where its parent picks it up."""
    _state.reset_thread()
    if _state.watcher is not None:
        _state.watcher.in_forked_child()


# what to wrap

def wrap_loaded():
    """Wraps what is loaded now."""
    if _state.lint is None:
        return
    for name, module in list(sys.modules.items()):
        if module is not None and (_state.lint.catalogue.watched_library(name) or name in OTHERS or name in _state.lint.catalogue.FILESYSTEMS):
            wrap_module(module)
    _rebind_modules()


def wrap_module(module):
    for owner, attribute, watch in plan(module):
        _apply(owner, attribute, watch)
    if module.__name__ in _state.lint.catalogue.FILESYSTEMS:
        _watch_filesystems(module)


def plan(module):
    """``(owner, attribute, Watch)`` for everything to wrap in a loaded module, by the catalogue: its readers, its writers
    if it is compiled, its ``connect`` if it is a database driver, its openers, and the methods of its frame, reader and
    SQL classes."""
    catalogue = _state.lint.catalogue
    name, namespace = getattr(module, "__name__", ""), getattr(module, "__dict__", None)
    if not isinstance(namespace, dict):
        return
    names = list(namespace)
    yield from OTHERS.get(name, lambda module: ())(module)
    if not catalogue.watched_library(name):
        return
    for attribute, rule in catalogue.watched_names(name, names).items():
        yield module, attribute, _by_rule(name, attribute, rule)
    for attribute, (position, keyword, letters) in catalogue.opener_names(name, names).items():
        reads = f"{name}.{attribute}" in catalogue.DATA_OPENERS
        yield module, attribute, Watch(f"{name}.{attribute}", catalogue.WRITE, "open-write-mode", judges.opener(position, keyword, letters, reads))
    for cls in _classes(namespace, catalogue.FRAME_CLASSES.get(name, ())):
        for attribute in catalogue.frame_writer_names(dir(cls)):
            rule = "frame-write" if attribute in catalogue.FRAME_WRITES else "always-writes"
            yield cls, attribute, Watch(f"{name}.{cls.__name__}.{attribute}", catalogue.WRITE, rule, judges.destination, method=True)
    for cls in _classes(namespace, catalogue.READER_CLASSES.get(name, ())):
        for attribute in sorted(catalogue.READ_METHODS & set(dir(cls))):
            yield cls, attribute, Watch(f"{name}.{cls.__name__}.{attribute}", catalogue.READ, f"{name}-read", judges.source, method=True)
    if name in catalogue.SQL_RUNNERS:
        for attribute in sorted(catalogue.SQL_METHODS & set(names)):
            yield module, attribute, Watch(f"{name}.{attribute}", catalogue.SQL, "file-sql", judges.sql, as_caller=True)
        for cls in _classes(namespace, catalogue.SQL_RUNNERS[name]):
            for attribute in sorted(catalogue.SQL_METHODS & set(dir(cls))):
                yield cls, attribute, Watch(f"{name}.{cls.__name__}.{attribute}", catalogue.SQL, "file-sql", judges.sql, method=True, as_caller=True)


def _by_rule(module, attribute, rule):
    """How a function or class the catalogue's rule names is watched: a reader by what it reads from, a writer by where it
    writes, a driver's ``connect`` by what it connects to."""
    catalogue, call = _state.lint.catalogue, f"{module}.{attribute}"
    if rule.code == catalogue.READ:
        return Watch(call, rule.code, rule.name, judges.source)
    if rule.code == catalogue.WRITE:
        return Watch(call, rule.code, rule.name, judges.destination)
    # SQLite says which files its SQL attaches only if asked, connection by connection
    return Watch(call, rule.code, rule.name, judges.connection(module), after=_authorize if module == "sqlite3" else None)


def _classes(namespace, names):
    return [namespace[name] for name in names if isinstance(namespace.get(name), type)]


def _standard_library(module):
    """What is wrapped in Python's own modules: how a process is started when nothing else reports it, and ``cffi``'s way
    of loading a native library (which raises no event, where ``ctypes`` does)."""
    lint, name = _state.lint, module.__name__
    if name == "_posixsubprocess":
        # subprocess keeps its own name for it, and raises an event of its own: only what calls it here is new
        yield module, "fork_exec", Watch("a child process", lint.PROCESS, "subprocess", judges.always(judges.child_process), everywhere=False)
    if name == "cffi.api" and isinstance(getattr(module, "FFI", None), type):
        yield module.FFI, "dlopen", Watch("cffi.FFI.dlopen", lint.DYNAMIC, "native-code", judges.always(judges.native_library),
                                          method=True, direct="cffi")


#: Modules outside the catalogue's libraries that hold something to wrap: each gives its ``(owner, attribute, Watch)``.
OTHERS = {"_posixsubprocess": _standard_library, "cffi.api": _standard_library}


# how it is wrapped

def _apply(owner, attribute, watch):
    """Wraps ``owner.attribute``, or notes why it could not be (``UNWATCHED``)."""
    original = vars(owner).get(attribute) if isinstance(owner, type) else None
    original = getattr(owner, attribute, None) if original is None else original
    if original is None or isinstance(original, types.ModuleType) or hasattr(original, ORIGINAL) or getattr(original, WATCHED, False):
        return  # nothing there, a module that happens to be named like a reader, or wrapped already
    try:
        if isinstance(original, type):
            _watch_class(owner, attribute, original, watch)
        elif isinstance(original, (staticmethod, classmethod)) or not callable(original):
            UNWATCHED[watch.call] = "not a plain function"
        else:
            known = _replaced.get(id(original))
            wrapper = known[1] if known is not None else watched(original, watch)
            setattr(owner, attribute, wrapper)
            if watch.everywhere and not watch.method:
                _replaced[id(original)] = (original, wrapper)
    except (TypeError, AttributeError) as e:
        UNWATCHED[watch.call] = f"{type(e).__name__}: {e}"


def watched(original, watch):
    """``original`` wrapped: a call of it is first judged, and what the judge says it is about to do is accounted for by
    the watcher, which may refuse it. Without a watcher it is the original."""
    parameters = _parameters(original, watch.method)

    def call(args, kwargs):
        if not watch.as_caller:
            return original(*args, **kwargs)
        caller = sys._getframe(2)
        scope = dict(caller.f_locals)
        scope[_CALL] = (original, args, kwargs)
        return eval(_CALL_IT, caller.f_globals, scope)  # noqa: S307 - the text is this module's own, never the job's

    @functools.wraps(original)
    def wrapper(*args, **kwargs):
        watcher = _state.watcher
        if watcher is None or _state.quiet():
            return call(args, kwargs)
        reached = watch.judge(args[1:] if watch.method else args, kwargs, parameters)
        accounted = reached is not None and watcher.saw(Seen(reached.code or watch.code, reached.rule or watch.rule, watch.call,
                                                            reached.did, reached.target, reached.paths, watch.direct))
        if accounted:
            # what it opens on the way is the same read or write
            with _state.inside():
                result = call(args, kwargs)
        else:
            result = call(args, kwargs)
        return watch.after(result) if watch.after is not None else result

    setattr(wrapper, ORIGINAL, original)
    _own.add(id(wrapper.__dict__))
    _own.update(id(cell) for cell in wrapper.__closure__ or ())
    _own.update(id(cell) for cell in call.__closure__ or ())
    return wrapper


_CALL = "__invaract_call__"
_CALL_IT = compile(f"{_CALL}[0](*{_CALL}[1], **{_CALL}[2])", "<invaract strict mode>", "eval")


def _parameters(function, method):
    """The names of a function's positional parameters, in order (without ``self``); ``None`` when Python cannot say."""
    try:
        names = [p.name for p in inspect.signature(function).parameters.values()
                 if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    except (TypeError, ValueError):
        return None
    return names[1:] if method else names


def _watch_class(module, attribute, cls, watch):
    """A class whose construction reads or writes: its ``__init__`` is wrapped where Python lets the class be changed;
    else a subclass that watches its construction takes its place in the module."""
    if "__init__" in vars(cls):
        try:
            cls.__init__ = watched(cls.__init__, _as_method(watch))
            setattr(cls, WATCHED, True)
            return
        except TypeError:
            pass  # a compiled class: Python does not let it be changed
    parameters = _parameters(cls, method=False)
    plain = cls.__new__ is object.__new__

    def __new__(new_cls, *args, **kwargs):
        watcher = _state.watcher
        if watcher is not None and not _state.quiet():
            reached = watch.judge(args, kwargs, parameters)
            if reached is not None:
                watcher.saw(Seen(reached.code or watch.code, reached.rule or watch.rule, watch.call, reached.did, reached.target, reached.paths))
        return cls.__new__(new_cls) if plain else cls.__new__(new_cls, *args, **kwargs)

    setattr(module, attribute, _stand_in(cls, {"__new__": __new__}))


def _as_method(watch):
    return Watch(watch.call, watch.code, watch.rule, watch.judge, True, watch.as_caller, watch.after, watch.direct, watch.everywhere)


def _stand_in(cls, members):
    """A subclass of a compiled class that takes its place, with ``members`` added: to ``isinstance`` and ``issubclass``
    it is the class it stands in for."""
    class Same(type(cls)):
        def __instancecheck__(self, instance):
            return isinstance(instance, cls)

        def __subclasscheck__(self, subclass):
            return issubclass(subclass, cls)

    return Same(cls.__name__, (cls,), {"__module__": cls.__module__, "__qualname__": cls.__qualname__, "__doc__": cls.__doc__,
                                        WATCHED: True, ORIGINAL: cls, **members})


def _watch_filesystems(module):
    """Every filesystem class of a module (pyarrow's) replaced by a subclass whose reading and writing methods are watched.
    A file on the local filesystem is judged like any other; one on a remote filesystem is never scratch."""
    catalogue, name = _state.lint.catalogue, module.__name__
    for attribute, cls in list(vars(module).items()):
        if not isinstance(cls, type) or not attribute.endswith("FileSystem") or attribute.startswith("_") or getattr(cls, WATCHED, False):
            continue
        local = attribute == "LocalFileSystem"
        members = {}
        for method in sorted((catalogue.FILESYSTEM_READS | catalogue.FILESYSTEM_WRITES) & set(dir(cls))):
            reads = method in catalogue.FILESYSTEM_READS
            watch = Watch(f"{name}.{attribute}.{method}", catalogue.READ if reads else catalogue.WRITE,
                          "pyarrow-read" if reads else "always-writes", _filesystem_judge(method, reads, local), method=True,
                          after=_remember_stream if method.startswith("open_") else None)
            members[method] = watched(getattr(cls, method), watch)
        if "from_uri" in dir(cls):
            members["from_uri"] = staticmethod(_from_uri(module, cls, f"{name}.{attribute}.from_uri"))
        try:
            setattr(module, attribute, _stand_in(cls, members))
        except TypeError as e:
            UNWATCHED[f"{name}.{attribute}"] = f"{type(e).__name__}: {e}"


def _filesystem_judge(method, reads, local):
    verb = {"open_input_stream": "read", "open_input_file": "read", "open_output_stream": "wrote", "open_append_stream": "appended to",
            "copy_file": "copied a file to", "move": "moved a file to"}.get(method, "deleted")

    def judge(args, kwargs, parameters=None):
        paths = [p for p in list(args[:2]) + [kwargs.get(k) for k in ("path", "src", "dest")] if isinstance(p, str)]
        if not paths:
            return None
        return judges.Reached(f"{verb} {paths[-1]}", paths[-1], tuple(paths) if local else ())
    return judge


def _remember_stream(stream):
    try:
        _state.streams.add(stream)
    except TypeError:
        pass
    return stream


def _from_uri(module, cls, call):
    """``FileSystem.from_uri``: a local address gives the watched local filesystem; any other gives a filesystem made by
    compiled code, whose reads and writes cannot be watched, so reaching it is itself what is reported."""
    lint = _state.lint

    def judge(args, kwargs, parameters=None):
        uri = args[0] if args else kwargs.get("uri")
        if not isinstance(uri, str) or "://" not in uri or uri.startswith("file:"):
            return None
        return judges.Reached(f"opened the filesystem at {uri}, whose reads and writes are not watched from here", uri)

    def local_is_watched(result):
        filesystem, path = result
        stand_in = getattr(module, "LocalFileSystem", None)
        if type(filesystem).__name__ == "LocalFileSystem" and getattr(stand_in, WATCHED, False):
            return stand_in(), path
        return result

    return watched(cls.from_uri, Watch(call, lint.CONNECT, "storage-client", judge, after=local_is_watched))


def _authorize(connection):
    """A SQLite connection made while a run is watched reports each database file its SQL attaches (``ATTACH``, ``VACUUM
    INTO``), through SQLite's own authorizer, which is asked before the statement runs."""
    if _state.watcher is not None:
        try:
            connection.set_authorizer(_attached)
        except Exception:  # noqa: BLE001 - a connection that takes no authorizer is watched by its file alone
            pass
    return connection


def _attached(action, first, second, database, source):
    import sqlite3
    watcher = _state.watcher
    if action == sqlite3.SQLITE_ATTACH and watcher is not None and isinstance(first, str) and not events.in_memory(first):
        try:
            watcher.saw(Seen(_state.lint.SQL, "file-sql", "sqlite3 ATTACH", f"ran SQL that attached the database file {first}", first, (first,)))
        except (ContractViolation, CheckNotRun):
            return sqlite3.SQLITE_DENY  # the refusal is kept by the run; here the statement is stopped
    return sqlite3.SQLITE_OK


# giving the wrapper to whoever holds the original

def _rebind_modules():
    """Gives every module that holds a wrapped function under a name of its own (``from pyarrow.parquet import
    write_table``, a library's own re-export) the wrapper in its place."""
    if not _replaced:
        return
    for module in list(sys.modules.values()):
        namespace = getattr(module, "__dict__", None)
        if isinstance(namespace, dict) and namespace is not globals():
            _swap_in(namespace)


def _swap_in(holder):
    """Replaces, in a dict or a list, every wrapped original by its wrapper."""
    for key, value in list(holder.items()) if isinstance(holder, dict) else list(enumerate(holder)):
        replaced = _replaced.get(id(value))
        if replaced is not None and replaced[0] is value:
            holder[key] = replaced[1]


def rebind_everywhere():
    """Gives the wrapper to whatever else holds a wrapped original from before it was wrapped: a table of writers, a
    default argument, a closure. Done when a run opens, so a reference a job took before it imported the package does not
    lead around the watch."""
    originals = [original for original, _ in _replaced.values()]
    if not originals:
        return
    for holder in gc.get_referrers(*originals):
        if holder is _replaced or id(holder) in _own:
            continue
        if isinstance(holder, (dict, list)):
            _swap_in(holder)
        elif isinstance(holder, types.CellType):
            replaced = _replaced.get(id(holder.cell_contents))
            if replaced is not None:
                holder.cell_contents = replaced[1]
        elif isinstance(holder, tuple):
            _swap_in_defaults(holder)
    # a class may have kept an attribute it looked up before
    getattr(sys, "_clear_internal_caches", getattr(sys, "_clear_type_cache", lambda: None))()


def _swap_in_defaults(defaults):
    """A function whose default arguments hold a wrapped original is given defaults that hold its wrapper."""
    swapped = tuple(_replaced[id(v)][1] if id(v) in _replaced and _replaced[id(v)][0] is v else v for v in defaults)
    for function in gc.get_referrers(defaults):
        if isinstance(function, types.FunctionType) and function.__defaults__ is defaults:
            function.__defaults__ = swapped


# wrapping a library as it is imported

class WrapOnImport(importlib.abc.MetaPathFinder):
    """Wraps a watched library's module when it has been imported: it finds the module as Python would have, and has its
    loader wrap it once it has run."""

    _finding = threading.local()

    def find_spec(self, fullname, path, target=None):
        lint = _state.lint
        wanted = lint is not None and (lint.catalogue.watched_library(fullname) or fullname in OTHERS or fullname in lint.catalogue.FILESYSTEMS)
        if not wanted or getattr(self._finding, "busy", False):
            return None
        self._finding.busy = True
        try:
            spec = importlib.util.find_spec(fullname)
        except (ImportError, ValueError, AttributeError):
            return None
        finally:
            self._finding.busy = False
        if spec is None or spec.loader is None or not hasattr(spec.loader, "exec_module"):
            return None
        spec.loader = WrapAfterLoad(spec.loader)
        return spec


class WrapAfterLoad(importlib.abc.Loader):
    def __init__(self, loader):
        self._loader = loader

    def create_module(self, spec):
        return self._loader.create_module(spec)

    def exec_module(self, module):
        self._loader.exec_module(module)
        # the module is left as any other import leaves it: found and loaded by its own loader
        spec = getattr(module, "__spec__", None)
        if spec is not None and spec.loader is self:
            spec.loader = self._loader
        if getattr(module, "__loader__", None) is self:
            module.__loader__ = self._loader
        try:
            wrap_loaded()
        except Exception as e:  # noqa: BLE001 - an import must not fail on the watch: the gap is listed instead
            UNWATCHED[getattr(module, "__name__", "?")] = f"{type(e).__name__}: {e}"

    def __getattr__(self, name):
        return getattr(self._loader, name)
