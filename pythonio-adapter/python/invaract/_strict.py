# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Watching what the job does as it runs (``INVARACT_STRICT``): a write made around the package is seen when it happens,
whatever the code that made it looks like, and, enforced, is stopped before it happens.

``INVARACT_STRICT`` is ``off`` (the default), ``report`` or ``enforce``. The scan of the job's source (``_codescan``) reads
code; this watches the run. It sees:

- every file Python itself opens to write, and every rename, delete, link and truncation (Python's audit events), which is
  how pandas, numpy, ``json``, ``csv``, ``pickle``, ``zipfile``, ``shutil`` and ``open`` itself write;
- every write by a library whose writers are compiled and so raise no such event (pyarrow, Polars, DuckDB, deltalake):
  each of their writers the lint's catalogue names is wrapped, so the list is the lint's and is kept in one place;
- SQL that reaches files, on a DuckDB connection (the statement run is read as the lint reads it) and on SQLite
  (``ATTACH``, ``VACUUM INTO``);
- a SQLite database file opened, a network connection made from Python, and another program started.

What it sees it places in the job's code, by the call that led to it: the nearest frame on the stack that is the job's own.
A write the invaract package makes on the job's behalf is its own, and is not a finding. A ``# invaract: allow`` marker on
the call keeps it, as it does for the scan, and ``INVARACT_STRICT_ALLOW`` lets through what it names (a log folder, a
metrics host). Scratch files in the temporary folder, Python's own bytecode files and the user's cache folder are never
findings.

What is found goes to the checker as a ``scan`` of phase ``runtime`` (``_protocol.scan``), published as a ``CODE_SCAN``
event. With ``report`` it is sent once, when the run ends. With ``enforce`` a finding no marker explains is sent at once,
the checker refuses the run, and the refusal is raised where the write was about to be made: it does not happen.

What it cannot see is in docs/PYTHONIO_ADAPTER.md ("Strict mode"): reads; a compiled library it does not wrap; what another
program does once started; what the job does before its run opens or after it ends.

invaract-lint holds the catalogue, so it must be installed (``pip install invaract[lint]``): a run asked to watch without it
is not checked.
"""
import fnmatch
import functools
import importlib.abc
import importlib.util
import itertools
import json
import linecache
import os
import sys
import tempfile
import threading
import time

from . import _codescan, _protocol
from ._settings import STRICT, STRICT_ALLOW
from .errors import CheckNotRun, ContractViolation

MODES = ("off", "report", "enforce")

#: Where a finding with none of the job's code on the stack is said to be.
NOWHERE = "<not in the job's code>"
#: The most internal failures a run lists; one is enough to say the run was not all watched.
MAX_ERRORS = 20

#: ``os.open`` flags that write.
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
#: Mode letters that write.
_WRITE_MODE = "wax+"

#: The watcher of the run in progress, or None: everything installed below does nothing without one.
_watcher = None
_installed = False
_installing = threading.Lock()
#: Per thread: ``busy`` while a finding is being worked out (what that itself opens is not the job's), and ``inside`` while a
#: wrapped writer that has been accounted for is running (what it opens on the way is the same write).
_state = threading.local()
#: invaract-lint, once a run has asked for it.
_lint = None
#: Each wrapped function, by the identity of the original: ``id -> (original, wrapper)``.
_replaced = {}
#: The writers the catalogue names that could not be wrapped, each with why: an ``open`` they make is still seen if it is
#: Python's own.
UNWATCHED = {}

_INVARACT, _JOB, _LIBRARY, _UNPLACED = "invaract", "job", "library", "unplaced"


def from_environment(environ=None):
    """The watcher the environment asks for, or ``None`` when ``INVARACT_STRICT`` is off or not set."""
    environ = os.environ if environ is None else environ
    mode = environ.get(STRICT, "").strip().lower() or "off"
    if mode not in MODES:
        raise CheckNotRun(f"{STRICT} must be off, report or enforce, not {environ[STRICT]!r}")
    if mode == "off":
        return None
    try:
        import invaract_lint
    except ImportError:
        raise CheckNotRun(f"{STRICT}={mode} asks for the job to be watched as it runs, and invaract-lint, which names what to "
                          "watch, is not installed: install it (pip install invaract[lint]), or set it to off") from None
    if not _names_what_to_watch(invaract_lint):
        raise CheckNotRun(f"{STRICT}={mode} needs the invaract-lint released with this package (its catalogue names what to watch), "
                          f"and the one installed is older ({getattr(invaract_lint, '__version__', 'unknown')}): upgrade it "
                          "(pip install --upgrade invaract[lint])")
    install(invaract_lint)
    return Watcher(invaract_lint, mode == "enforce", _codescan._split(environ.get(STRICT_ALLOW)))


def _names_what_to_watch(lint):
    """Whether an installed invaract-lint is one strict mode can read: it has the catalogue and the reader of SQL."""
    catalogue = getattr(lint, "catalogue", None)
    return all(hasattr(catalogue, name) for name in ("COMPILED", "writer_names", "frame_writer_names", "SQL_RUNNERS")) and hasattr(lint, "sqltext")


def install_early(environ=None):
    """Installs the hooks as the package is imported, when the environment asks for strict mode, so that a library the job
    imports afterwards is wrapped as it loads. Never raises: a run that cannot watch says so when it opens."""
    environ = os.environ if environ is None else environ
    if environ.get(STRICT, "").strip().lower() in ("report", "enforce"):
        try:
            import invaract_lint
            if _names_what_to_watch(invaract_lint):
                install(invaract_lint)
        except Exception:  # noqa: BLE001 - said when the run opens (from_environment), where it can be refused
            pass


def install(lint):
    """Installs, once for the process, the audit hook, the hook that wraps a compiled library as it is imported, and the
    hook that marks a forked child; then wraps what is already loaded. None of them does anything without a run."""
    global _installed, _lint
    with _installing:
        _lint = lint
        if not _installed:
            sys.addaudithook(_audit)
            sys.meta_path.insert(0, _WrapOnImport())
            if hasattr(os, "register_at_fork"):
                os.register_at_fork(after_in_child=_in_child)
            _installed = True
        wrap_loaded()


class Seen:
    """Something the job did, as it was seen: the catalogue's code and rule for it, the function that did it, what it did in
    words, what it reached (a path, ``host:port``, a program), and the local paths it touched (all scratch: no finding)."""

    def __init__(self, code, rule, call, did, target=None, paths=()):
        self.code, self.rule, self.call, self.did, self.target, self.paths = code, rule, call, did, target, tuple(paths)


class Watcher:
    """One run's watch: what was seen, placed in the job's code and counted; refused at once when enforced."""

    def __init__(self, lint, enforce, allow=()):
        self._lint = lint
        self.enforce = enforce
        self.allow = list(allow)
        self._run = None
        self._lock = threading.RLock()
        #: findings by where they are and what they are: ``(path, line, column, rule) -> [finding, times seen, sent, refusal]``
        self._found = {}
        self._errors = []
        self._sent_any = False
        self._started = None
        #: where a forked child leaves what it saw, for this process to pick up when the run ends
        self._spill = None
        self._child = False
        self._markers = {}
        self._scratch = _scratch_roots()

    @property
    def scanner(self):
        from . import __version__
        return f"invaract strict {__version__}"

    def start(self, run):
        """Starts watching, for ``run``: from here to ``stop``, what the job does is seen."""
        global _watcher
        wrap_loaded()
        self._run = run
        self._started = time.monotonic()
        self._spill = os.path.join(tempfile.gettempdir(), f"invaract-strict-{os.getpid()}-{run.run_id}.jsonl")
        _watcher = self

    def stop(self):
        """Stops watching and returns the ``scan`` request for what was seen and not yet sent, or ``None`` when everything
        was (an enforced run refused on its one finding has nothing more to say)."""
        global _watcher
        if _watcher is self:
            _watcher = None
        if self._started is None:
            return None
        with self._lock:
            self._gather()
            unsent = [entry for entry in self._found.values() if not entry[2]]
            if not unsent and self._sent_any and not self._errors:
                return None
            for entry in unsent:
                entry[2] = True
            elapsed, self._started = round((time.monotonic() - self._started) * 1000), None
            return _protocol.scan("runtime", self.enforce, self.scanner, [], [_counted(entry) for entry in unsent], self._errors, elapsed)

    # what was seen

    def saw(self, seen, event):
        """Accounts for ``seen``. Returns whether it was accounted for (a finding, or the package's own doing); false for
        what is no finding (a scratch file). Enforced, a finding no marker explains raises the checker's refusal. A failure
        of the watching itself is listed, and, enforced, leaves the run not checked: it is never let through."""
        _state.busy = True
        try:
            return self._account(seen)
        except (ContractViolation, CheckNotRun):
            raise
        except Exception as e:  # noqa: BLE001 - the job's open() must not fail on a fault of the watch's own, unless enforced
            with self._lock:
                if len(self._errors) < MAX_ERRORS:
                    self._errors.append(("<strict mode>", f"{type(e).__name__}: {e} while watching {event}"))
            if self.enforce:
                failure = CheckNotRun(f"Invaract's strict mode could not watch {event} ({type(e).__name__}: {e}), so the run is "
                                      f"not checked ({STRICT}=enforce)")
                self._fail(failure)
                raise failure from e
            return False
        finally:
            _state.busy = False

    def _account(self, seen):
        if seen.paths and all(self._is_scratch(p) for p in seen.paths):
            return False
        whose, frame = _place(sys._getframe(2))
        if whose == _INVARACT:
            return True
        where = _position(frame) if frame is not None else (NOWHERE, 0, 0, 0, 0, f"<thread {threading.current_thread().name}>", "")
        path, line, column, end_line, end_column, scope, snippet = where
        key = (path, line, column, seen.rule)
        with self._lock:
            entry = self._found.get(key)
            if entry is not None:
                entry[1] += 1
                if entry[3] is not None:
                    # refused once, refused every time: a library that tries again (zipfile, shutil.copytree) gets no further
                    raise ContractViolation(entry[3].violations, entry[3].explanation)
                return True
            allowed, reason = self._explained(seen, frame, line, end_line)
            # what it reached may be a connection string: its password is never kept
            did = f"seen as the job ran: {self._lint.short(seen.did)}"
            if frame is None:
                did += f", in thread {threading.current_thread().name}, with none of the job's code calling it"
            finding = self._lint.Finding(
                path, line, column, seen.code, seen.call, end_line=end_line, end_col=end_column, resolved=seen.call, scope=scope,
                rule=seen.rule, snippet=self._lint.short(snippet or seen.call), target=self._lint.short(seen.target) if seen.target else None,
                detail=did, allowed=allowed, reason=reason)
            entry = self._found[key] = [finding, 1, False, None]
            if self._child:
                self._leave(finding)
        if self.enforce and not allowed:
            self._refuse(entry)
        return True

    def _explained(self, seen, frame, line, end_line):
        """Whether a marker on one of the call's lines keeps it, or ``INVARACT_STRICT_ALLOW`` names what it reached; and the
        reason given."""
        for pattern in self.allow:
            if any(fnmatch.fnmatch(candidate, pattern) for candidate in _named(seen)):
                return True, f"{STRICT_ALLOW} lets through {pattern}"
        if frame is None or line == 0:
            return False, None
        markers = self._markers_of(frame.f_code.co_filename)
        reasons = [markers[n] for n in range(line, max(line, end_line) + 1) if n in markers]
        return bool(reasons), next((r for r in reasons if r), None)

    def _markers_of(self, filename):
        if filename not in self._markers:
            try:
                self._markers[filename] = self._lint.allowed_lines("".join(linecache.getlines(filename)))
            except Exception:  # noqa: BLE001 - source that cannot be read holds no marker the watch can honour
                self._markers[filename] = {}
        return self._markers[filename]

    def _is_scratch(self, path):
        real = os.path.realpath(path)
        return ("__pycache__" in real.split(os.sep) or real.endswith((".pyc", ".pyo"))
                or any(_codescan._inside(real, root) for root in self._scratch))

    # refusing, and what a forked child saw

    def _refuse(self, entry):
        """Sends one finding at once, enforced: the checker refuses the run (unless it is a dry run, which reports), and the
        refusal is raised here, in the thread about to make the write, and again each time the same call is tried. A forked
        child cannot ask the checker (it shares its parent's pipe to it): it raises a refusal of its own, and its parent is
        refused when it picks up what the child saw."""
        finding = entry[0]
        try:
            if self._child:
                raise ContractViolation(["UNGOVERNED_DATA_ACCESS"], f"Invaract refused this run ({STRICT}=enforce): {finding.message}. "
                                                                  f"{finding.detail} ({finding.path}:{finding.line})")
            entry[2] = self._sent_any = True
            self._run._ask(_protocol.scan("runtime", True, self.scanner, [], [_counted(entry)], [], 0))
        except ContractViolation as refusal:
            entry[3] = refusal
            raise

    def _fail(self, failure):
        if self._run is not None and self._run._failure is None:
            self._run._failure = failure

    def _leave(self, finding):
        with open(self._spill, "a", encoding="utf-8") as spill:
            spill.write(json.dumps([finding.path, finding.line, finding.col, finding.code, finding.call, finding.end_line, finding.end_col,
                                    finding.resolved, finding.scope, finding.rule, finding.snippet, finding.target, finding.detail,
                                    finding.allowed, finding.reason]) + "\n")

    def _gather(self):
        """Adds what forked children saw to what this process saw."""
        if self._child or not os.path.exists(self._spill):
            return
        _state.busy = True
        try:
            with open(self._spill, encoding="utf-8") as spill:
                rows = [json.loads(line) for line in spill if line.strip()]
            os.remove(self._spill)
        finally:
            _state.busy = False
        for row in rows:
            finding = self._lint.Finding(*row[:5], end_line=row[5], end_col=row[6], resolved=row[7], scope=row[8], rule=row[9],
                                         snippet=row[10], target=row[11], detail=row[12] + ", in a child process", allowed=row[13],
                                         reason=row[14])
            entry = self._found.setdefault((finding.path, finding.line, finding.col, finding.rule, "child"), [finding, 0, False, None])
            entry[1] += 1


def _counted(entry):
    """A finding as it is sent: with how many times it was seen, when more than once."""
    finding, times = entry[0], entry[1]
    sent = finding.to_dict()
    if times > 1:
        sent["detail"] = f"{finding.detail}; {times} times in all"
    return sent


def _named(seen):
    """The names ``INVARACT_STRICT_ALLOW`` may match what was reached by: as it was reached, and for a local path its full
    path and its path from the working folder."""
    names = [seen.target] if seen.target else []
    for path in seen.paths:
        real = os.path.realpath(path)
        names += [real, os.path.relpath(real, os.path.realpath(os.getcwd()))]
    return names


def _scratch_roots():
    """Where what is written is never data at rest: the temporary folder, the devices, and the user's cache folder, where
    libraries keep what they can rebuild."""
    home = os.path.expanduser("~")
    roots = [tempfile.gettempdir(), "/dev", "/proc", os.environ.get("XDG_CACHE_HOME") or os.path.join(home, ".cache"),
             os.path.join(home, "Library", "Caches"), os.environ.get("LOCALAPPDATA"), sys.pycache_prefix]
    return [os.path.realpath(root) for root in roots if root]


# placing what was seen in the job's code

_whose_file = {}


def _whose(filename):
    """Whose code a frame's file is: the package's own (and invaract-lint's), the job's, a library's (Python's and every
    installed package's), or not placed (code compiled from text, which has no file)."""
    whose = _whose_file.get(filename)
    if whose is None:
        if filename.startswith("<"):
            whose = _LIBRARY if filename.startswith(("<frozen", "<built-in", "<invaract")) else _UNPLACED
        elif not os.path.isfile(filename):
            whose = _UNPLACED  # code compiled under a name that is no file
        else:
            real = os.path.realpath(filename)
            ours = [os.path.dirname(os.path.abspath(__file__))]
            if _lint is not None and getattr(_lint, "__file__", None):
                ours.append(os.path.dirname(os.path.abspath(_lint.__file__)))
            if any(_codescan._inside(real, os.path.realpath(root)) for root in ours):
                whose = _INVARACT
            else:
                whose = _LIBRARY if any(_codescan._inside(real, root) for root in _codescan._not_the_jobs()) else _JOB
        _whose_file[filename] = whose
    return whose


def _place(frame):
    """Whose doing something is, and the frame to place it at: walking out from where it happened, the first frame that is
    the package's own makes it the package's (it wrote on the job's behalf), and the first that is the job's places it
    there. Code with no file (``exec``) is placed at the job's frame that ran it, or, with none, where it is."""
    unplaced = None
    while frame is not None:
        whose = _whose(frame.f_code.co_filename)
        if whose == _INVARACT and frame.f_code.co_filename != _THIS_FILE:
            return _INVARACT, None
        if whose == _JOB:
            return _JOB, frame
        if whose == _UNPLACED and unplaced is None:
            unplaced = frame
        frame = frame.f_back
    return (_JOB, unplaced) if unplaced is not None else (_LIBRARY, None)


def _position(frame):
    """``(path, line, column, end line, end column, scope, snippet)`` of the call a frame is making: the call's own extent
    where Python records it (3.11 and later); else the lines of the statement it is in, with no columns."""
    code = frame.f_code
    line, end_line, column, end_column = frame.f_lineno or 0, frame.f_lineno or 0, 0, 0
    at = None
    if hasattr(code, "co_positions") and frame.f_lasti >= 0:
        at = next(itertools.islice(code.co_positions(), frame.f_lasti // 2, None), None)
    if at is not None and None not in at:
        line, end_line, column, end_column = at[0], at[1], at[2] + 1, at[3] + 1
    elif line:
        line, end_line = _statement(code.co_filename, line)
    scope = _scope(code, frame.f_lineno or 0)
    lines = [linecache.getline(code.co_filename, n) for n in range(line, end_line + 1)] if line else []
    if lines and column:
        lines[-1] = lines[-1][:end_column - 1]
        lines[0] = lines[0][column - 1:]
    path = _codescan._shown(code.co_filename) if not code.co_filename.startswith("<") else code.co_filename
    return path, line, column, end_line, end_column, scope, " ".join(" ".join(lines).split())


#: Per file: its statements' first and last lines, and its functions' and classes' ``(first line, last line, dotted name)``.
_parsed = {}


def _parse(filename):
    import ast
    if filename not in _parsed:
        statements, scopes = [], []
        try:
            tree = ast.parse("".join(linecache.getlines(filename)))
        except (SyntaxError, ValueError, RecursionError, MemoryError):
            tree = None
        pending = [(tree, ())] if tree is not None else []
        while pending:
            node, inside = pending.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inside = inside + (node.name,)
                scopes.append((node.lineno, node.end_lineno, ".".join(inside)))
            if isinstance(node, ast.stmt):
                statements.append((node.lineno, node.end_lineno))
            pending.extend((child, inside) for child in ast.iter_child_nodes(node))
        _parsed[filename] = statements, scopes
    return _parsed[filename]


def _statement(filename, line):
    """The first and last line of the innermost statement of a file that ``line`` is in: ``line`` alone when the file
    cannot be read as Python."""
    around = [(last - first, first, last) for first, last in _parse(filename)[0] if first <= line <= last]
    # a compound statement (a def, a with) holds others: the innermost is the shortest
    return min(around)[1:] if around else (line, line)


def _scope(code, line):
    """The function or class a frame's code is in, as the scan names it (``Loader.load``; ``<module>``): Python's own
    qualified name for it where it keeps one (3.11 and later), else worked out from the source."""
    if hasattr(code, "co_qualname"):
        name = code.co_qualname.replace(".<locals>", "")
    else:
        around = [(last - first, name) for first, last, name in _parse(code.co_filename)[1] if first <= line <= last]
        name = min(around)[1] if around else code.co_name
        if code.co_name.startswith("<") and code.co_name != "<module>" and around:
            name += "." + code.co_name
    # a comprehension is no scope of its own to a reader
    parts = [part for part in name.split(".") if part not in ("<listcomp>", "<setcomp>", "<dictcomp>", "<genexpr>")]
    return ".".join(parts) or "<module>"


# Python's audit events

def _audit(event, args):
    watcher = _watcher
    if watcher is None:
        return
    describe = _EVENTS.get(event)
    if describe is None or getattr(_state, "busy", False) or getattr(_state, "inside", 0):
        return
    seen = describe(event, args)
    if seen is not None:
        watcher.saw(seen, event)


def _local(path, folder=None):
    """``path`` as text, or ``None`` for what is no path (a file descriptor). A path given relative to an open folder
    (``dir_fd``, which is how ``shutil.rmtree`` deletes) is placed in that folder."""
    if not isinstance(path, (str, bytes, os.PathLike)):
        return None
    path = os.fsdecode(path)
    if isinstance(folder, int) and folder >= 0 and not os.path.isabs(path):
        inside = _folder_of(folder)
        if inside is not None:
            return os.path.join(inside, path)
    return path


def _folder_of(descriptor):
    """The folder an open descriptor is of, where the system can say (Linux, macOS); ``None`` where it cannot."""
    try:
        return os.readlink(f"/proc/self/fd/{descriptor}")
    except OSError:
        pass
    try:
        import fcntl
        return fcntl.fcntl(descriptor, fcntl.F_GETPATH, b"\0" * 1024).rstrip(b"\0").decode()
    except (ImportError, AttributeError, OSError, ValueError):
        return None


def _opened(event, args):
    path, mode, flags = (tuple(args) + (None, None, None))[:3]
    path = _local(path)
    if path is None:
        return None
    writes = any(letter in mode for letter in _WRITE_MODE) if isinstance(mode, str) else isinstance(flags, int) and bool(flags & _WRITE_FLAGS)
    if not writes:
        return None
    how = f"mode {mode!r}" if isinstance(mode, str) else f"flags {flags}"
    return Seen(_lint.WRITE, "open-write-mode", "open", f"opened {path} to write ({how})", path, [path])


#: Each rename, link, delete and truncation Python reports: what it did in words, and where among the event's arguments
#: each path's open folder (``dir_fd``) is, if it may have one.
_CHANGES = {
    "os.rename": ("moved {0} to {1}", (2, 3)), "os.link": ("linked {1} to {0}", (2, 3)), "os.symlink": ("linked {1} to {0}", (None, 2)),
    "os.remove": ("deleted {0}", (1,)), "os.rmdir": ("deleted the folder {0}", (1,)), "os.truncate": ("truncated {0}", (None,)),
}


def _changed(event, args):
    """A rename, link, delete or truncation: what it reaches is its destination, or, with one path, that path."""
    did, folders = _CHANGES[event]
    paths = [_local(args[i], args[at] if at is not None and len(args) > at else None) for i, at in enumerate(folders) if len(args) > i]
    if None in paths or len(paths) < len(folders):
        return None
    return Seen(_lint.WRITE, "file-operation", event, did.format(*paths), paths[-1], paths)


def _database(event, args):
    database = _local(args[0]) if args else None
    if database is None or _in_memory(database):
        return None
    return Seen(_lint.CONNECT, "database-connect", "sqlite3.connect", f"opened the SQLite database {database}", database, [database])


def _in_memory(database):
    """Whether a SQLite or DuckDB database is held in memory, by its name: unnamed, ``:memory:``, a ``file:`` URI that says
    so, or DuckDB's default connection (``:default:``, which is in memory)."""
    return (database == "" or database.startswith((":memory:", "file::memory:")) or "mode=memory" in database
            or database == ":default:")


def _connected(event, args):
    address = args[1] if len(args) > 1 else None
    if isinstance(address, tuple) and len(address) >= 2:
        target = f"{address[0]}:{address[1]}"
    elif isinstance(address, (str, bytes)) and address:
        target = os.fsdecode(address)
    else:
        return None
    return Seen(_lint.CONNECT, "network-connect", "socket.connect", f"connected to {target}", target)


def _started(event, args):
    """Another program started. Only its name is kept: its arguments may hold anything."""
    first = args[1] if event == "os.spawn" else args[0] if args else None
    if first is None and len(args) > 1:
        first = args[1]  # no program named apart: its command line names it
    if isinstance(first, (list, tuple)):
        first = first[0] if first else None
    program = os.fsdecode(first).strip().split()[0] if isinstance(first, (str, bytes)) and os.fsdecode(first).strip() else "a program"
    return Seen(_lint.PROCESS, "subprocess", event, f"started {program}", program)


_EVENTS = {
    "open": _opened,
    "os.rename": _changed, "os.link": _changed, "os.symlink": _changed, "os.remove": _changed, "os.rmdir": _changed,
    "os.truncate": _changed,
    "sqlite3.connect": _database,
    "socket.connect": _connected,
    "subprocess.Popen": _started, "os.system": _started, "os.exec": _started, "os.posix_spawn": _started, "os.spawn": _started,
    "os.startfile": _started, "_winapi.CreateProcess": _started,
}


def _in_child():
    """After a fork, in the child: it keeps watching, and leaves what it sees where its parent picks it up."""
    watcher = _watcher
    if watcher is not None:
        watcher._child = True
        watcher._found = {}
    _state.busy = False
    _state.inside = 0


# wrapping the compiled libraries' writers

def _watched(original, call, code, rule, judge, method=False, as_caller=False):
    """``original`` wrapped: a call of it is first judged (``judge`` says what it is about to do, or ``None`` for nothing to
    report), and accounted for by the run's watcher. Without a run it is the original. ``as_caller`` is for a function that
    looks at the code calling it (DuckDB finds the frames its SQL names among its caller's variables): it is then called
    from a frame that has the caller's variables, so it finds what it would have."""
    parameters = _parameters(original, method)

    def call_it(args, kwargs):
        if not as_caller:
            return original(*args, **kwargs)
        caller = sys._getframe(2)
        scope = dict(caller.f_locals)
        scope[_CALL] = (original, args, kwargs)
        return eval(_CALL_IT, caller.f_globals, scope)  # noqa: S307 - the text is this module's own, never the job's

    @functools.wraps(original)
    def watched(*args, **kwargs):
        watcher = _watcher
        if watcher is None or getattr(_state, "busy", False) or getattr(_state, "inside", 0):
            return call_it(args, kwargs)
        told = judge(args[1:] if method else args, kwargs, parameters)
        if told is None:
            return call_it(args, kwargs)
        did, target, paths = told
        if not watcher.saw(Seen(code, rule, call, did, target, paths), call):
            return call_it(args, kwargs)
        # accounted for: what it opens on the way is the same write
        _state.inside = 1
        try:
            return call_it(args, kwargs)
        finally:
            _state.inside = 0
    watched.__invaract_original__ = original
    return watched


_CALL = "__invaract_call__"
_CALL_IT = compile(f"{_CALL}[0](*{_CALL}[1], **{_CALL}[2])", "<invaract strict mode>", "eval")


def _destination(args, kwargs, parameters=None):
    """Where a writer's call writes, by what it is handed. The arguments that may say where are the ones the catalogue's
    destination names fit (``where``, ``sink``, ``path``, ...), by the function's own parameter names, or its first argument
    when none of its parameters is so named. A path among them is where (of two, the second: a copy's destination); a
    stream that is neither held in memory nor a file Python itself opened (which was seen then) is reported as it is;
    anything else (a buffer, the console, no destination) is no write at rest."""
    named = _lint.catalogue.DESTINATIONS
    if parameters is None:
        candidates = list(args[:2])
    elif any(name in named for name in parameters):
        candidates = [value for name, value in zip(parameters, args) if name in named]
    else:
        candidates = list(args[:1])
    candidates += [value for key, value in kwargs.items() if key in named]
    paths = [_local(c) for c in candidates if isinstance(c, (str, os.PathLike))]
    if paths:
        path = paths[1] if len(paths) > 1 and len(candidates) == len(paths) == 2 else paths[0]
        return f"wrote {path}", path, ([path] if "://" not in path else [])
    for candidate in candidates:
        if callable(getattr(candidate, "write", None)) and not _held_in_memory(candidate):
            kind = f"{type(candidate).__module__}.{type(candidate).__qualname__}"
            return f"wrote to an open stream ({kind})", f"<a stream: {kind}>", []
    return None


def _parameters(function, method):
    """The names of a function's positional parameters, in order (without ``self``); ``None`` when Python cannot say."""
    import inspect
    try:
        names = [p.name for p in inspect.signature(function).parameters.values()
                 if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    except (TypeError, ValueError):
        return None
    return names[1:] if method else names


def _opener(position, keyword, letters):
    """The judge of a function that opens a file (the catalogue's ``OPENERS``): a write when its mode writes."""
    def judge(args, kwargs, parameters=None):
        mode = args[position] if len(args) > position else kwargs.get(keyword)
        path = _local(args[0]) if args else None
        if path is None or not isinstance(mode, str) or not set(mode) & set(letters):
            return None
        return f"opened {path} to write (mode {mode!r})", path, [path]
    return judge


def _held_in_memory(stream):
    """Whether a stream is certainly no dataset to report now: a buffer, the console, or a file that was seen when it was
    opened (by Python's own ``open``, or by a class this module wraps)."""
    import io
    if isinstance(stream, (io.BytesIO, io.StringIO, io.FileIO, io.BufferedWriter, io.BufferedRandom, io.TextIOWrapper)):
        return True
    if getattr(type(stream), "__invaract_watched__", False):
        return True  # a stream this module saw being opened
    return type(stream).__name__ in ("BufferOutputStream", "MockOutputStream", "FixedSizeBufferWriter", "PythonFile",
                                     "_TemporaryFileWrapper", "SpooledTemporaryFile")


def _sql(args, kwargs, parameters=None):
    """SQL about to be run that reaches files, read as the scan reads it: any of the call's texts may be the statement."""
    for statement in list(args) + list(kwargs.values()):
        reached = _lint.sqltext.file_access(statement) if isinstance(statement, str) else None
        if reached:
            return f"ran SQL that reads or writes files ({reached})", None, []
    return None


def _file_database(args, kwargs, parameters=None):
    database = _local(args[0] if args else kwargs.get("database"))
    if database is None or _in_memory(database):
        return None
    return f"opened the database {database}", database, [database]


def _spawned(args, kwargs, parameters=None):
    return "started a Python worker process (multiprocessing), which is not watched", "a Python worker process", []


def _watch_sqlite(original):
    """``sqlite3.connect`` wrapped: a connection made during a run reports each database file its SQL attaches (``ATTACH``,
    ``VACUUM INTO``), through SQLite's own authorizer, which is asked before the statement runs."""
    @functools.wraps(original)
    def connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        if _watcher is not None and not getattr(_state, "busy", False):
            try:
                connection.set_authorizer(_attached)
            except Exception:  # noqa: BLE001 - a connection that takes no authorizer is watched by its file alone
                pass
        return connection
    connect.__invaract_original__ = original
    return connect


def _attached(action, first, second, database, source):
    import sqlite3
    watcher = _watcher
    if action == sqlite3.SQLITE_ATTACH and watcher is not None and isinstance(first, str) and not _in_memory(first):
        try:
            watcher.saw(Seen(_lint.SQL, "file-sql", "sqlite3 ATTACH", f"ran SQL that attached the database file {first}", first, [first]),
                        "sqlite3 ATTACH")
        except (ContractViolation, CheckNotRun):
            return sqlite3.SQLITE_DENY  # the refusal is kept by the run; here the statement is stopped
    return sqlite3.SQLITE_OK


#: Functions outside the compiled libraries that are wrapped too, by module: ``(code, rule, judge)`` for each.
_OTHERS = {
    "multiprocessing.util": {"spawnv_passfds": (lambda: _lint.PROCESS, "subprocess", _spawned)},
    # the standard library's key-value files (what shelve keeps its data in) are written by compiled code
    **{module: {"open": (lambda: _lint.WRITE, "open-write-mode", _opener(1, "flag", "wcn"))} for module in ("dbm", "dbm.gnu", "dbm.ndbm", "dbm.dumb")},
}
_THIS_FILE = _audit.__code__.co_filename


def wrap_loaded():
    """Wraps what is loaded now: every compiled library's module the catalogue names writers in."""
    if _lint is None:
        return
    for name, module in list(sys.modules.items()):
        if module is not None and _wrapped_module(name):
            _wrap_module(module)
    sqlite = sys.modules.get("sqlite3")
    connect = getattr(sqlite, "connect", None)
    if connect is not None and not hasattr(connect, "__invaract_original__"):
        wrapper = _watch_sqlite(connect)
        _replaced[id(connect)] = (connect, wrapper)
        sqlite.connect = wrapper
    _rebind()


def _wrapped_module(name):
    return name.split(".")[0] in _lint.catalogue.COMPILED or name in _OTHERS


def _wrap_module(module):
    catalogue = _lint.catalogue
    name = getattr(module, "__name__", None)
    namespace = getattr(module, "__dict__", None)
    if not name or not isinstance(namespace, dict):
        return
    for attribute, (code, rule, judge) in _OTHERS.get(name, {}).items():
        if attribute in namespace:
            _wrap(module, attribute, f"{name}.{attribute}", code(), rule, judge)
    if name.split(".")[0] not in catalogue.COMPILED:
        return
    for attribute, rule in catalogue.writer_names(name, list(namespace)).items():
        _wrap(module, attribute, f"{name}.{attribute}", rule.code, rule.name, _destination)
    for attribute in list(namespace):
        opener = catalogue.OPENERS.get(f"{name}.{attribute}")
        if opener is not None:
            _wrap(module, attribute, f"{name}.{attribute}", catalogue.WRITE, "open-write-mode", _opener(*opener))
    if name in catalogue.SQL_RUNNERS:
        for attribute in sorted(catalogue.SQL_METHODS & set(namespace)):
            _wrap(module, attribute, f"{name}.{attribute}", catalogue.SQL, "file-sql", _sql, as_caller=True)
        if "connect" in namespace:
            _wrap(module, "connect", f"{name}.connect", catalogue.CONNECT, "database-connect", _file_database)
        for owner in catalogue.SQL_RUNNERS[name]:
            cls = namespace.get(owner)
            for attribute in sorted(catalogue.SQL_METHODS & set(dir(cls))) if isinstance(cls, type) else ():
                _wrap(cls, attribute, f"{name}.{owner}.{attribute}", catalogue.SQL, "file-sql", _sql, method=True, as_caller=True)
    for owner in catalogue.FRAME_CLASSES.get(name, ()):
        cls = namespace.get(owner)
        for attribute in catalogue.frame_writer_names(dir(cls)) if isinstance(cls, type) else ():
            rule = "frame-write" if attribute in catalogue.FRAME_WRITES else "always-writes"
            _wrap(cls, attribute, f"{name}.{owner}.{attribute}", catalogue.WRITE, rule, _destination, method=True)


def _wrap(owner, attribute, call, code, rule, judge, method=False, as_caller=False):
    """Wraps ``owner.attribute``: a function is replaced where it is (and wherever else a module has it, ``_rebind``); a
    class has its construction wrapped, in place where Python lets a class be changed, else in a subclass that takes its
    place in the module."""
    original = vars(owner).get(attribute) if isinstance(owner, type) else getattr(owner, attribute, None)
    if original is None:
        original = getattr(owner, attribute, None)
    if original is None or hasattr(original, "__invaract_original__") or getattr(original, "__invaract_watched__", False):
        return
    try:
        if isinstance(original, type):
            _wrap_class(owner, attribute, original, call, code, rule, judge)
        elif isinstance(original, (staticmethod, classmethod)) or not callable(original):
            UNWATCHED[call] = "not a plain function"
        else:
            wrapper = _watched(original, call, code, rule, judge, method, as_caller)
            setattr(owner, attribute, wrapper)
            if not method:
                _replaced[id(original)] = (original, wrapper)
    except (TypeError, AttributeError) as e:
        UNWATCHED[call] = f"{type(e).__name__}: {e}"


def _wrap_class(module, attribute, cls, call, code, rule, judge):
    if "__init__" in vars(cls):
        try:
            cls.__init__ = _watched(cls.__init__, call, code, rule, judge, method=True)
            cls.__invaract_watched__ = True
            return
        except TypeError:
            pass  # a compiled class: Python does not let it be changed
    parameters = _parameters(cls, method=False)
    plain = cls.__new__ is object.__new__

    def __new__(new_cls, *args, **kwargs):
        watcher = _watcher
        if watcher is not None and not getattr(_state, "busy", False) and not getattr(_state, "inside", 0):
            told = judge(args, kwargs, parameters)
            if told is not None:
                watcher.saw(Seen(code, rule, call, *told), call)
        return cls.__new__(new_cls) if plain else cls.__new__(new_cls, *args, **kwargs)

    class Same(type(cls)):
        """The class that takes a compiled class's place is, to ``isinstance`` and ``issubclass``, that class."""

        def __instancecheck__(self, instance):
            return isinstance(instance, cls)

        def __subclasscheck__(self, subclass):
            return issubclass(subclass, cls)

    watched = Same(cls.__name__, (cls,), {"__new__": __new__, "__module__": cls.__module__, "__qualname__": cls.__qualname__,
                                         "__doc__": cls.__doc__, "__invaract_watched__": True, "__invaract_original__": cls})
    setattr(module, attribute, watched)


def _rebind():
    """Gives every module that holds a wrapped function under a name of its own (``from pyarrow.parquet import
    write_table``, a library's own re-export) the wrapper in its place."""
    if not _replaced:
        return
    for module in list(sys.modules.values()):
        namespace = getattr(module, "__dict__", None)
        if not isinstance(namespace, dict) or namespace is globals():
            continue
        for key, value in list(namespace.items()):
            replaced = _replaced.get(id(value))
            if replaced is not None and replaced[0] is value:
                namespace[key] = replaced[1]


class _WrapOnImport(importlib.abc.MetaPathFinder):
    """Wraps a compiled library's module when it has been imported: it finds the module as Python would have, and has its
    loader wrap it once it has run."""

    def find_spec(self, fullname, path, target=None):
        if _lint is None or not (_wrapped_module(fullname) or fullname == "sqlite3") or getattr(_state, "finding", False):
            return None
        _state.finding = True
        try:
            spec = importlib.util.find_spec(fullname)
        except (ImportError, ValueError, AttributeError):
            return None
        finally:
            _state.finding = False
        if spec is None or spec.loader is None or not hasattr(spec.loader, "exec_module"):
            return None
        spec.loader = _WrapAfterLoad(spec.loader)
        return spec


class _WrapAfterLoad(importlib.abc.Loader):
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
