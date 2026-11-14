# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""One watch: what was seen, placed in the job's code, counted, and, enforced, refused.

A ``Watcher`` is given everything the hooks see (``saw``). It leaves out what is no finding (a scratch file, the package's
own reads and writes, a library's use of ``ctypes`` for its own ends), places the rest in the job's code (``placing``), and
keeps one finding per call site with how many times it was seen. Enforced, a finding no marker explains is sent to the
checker at once, and the refusal is raised where the job was about to act, and again every time the same call is tried.

There are three kinds, all this one class:

- a run's (``start`` to ``stop``): its findings go to the checker as a ``scan`` of phase ``runtime``;
- the one that keeps what the job does between importing the package and opening its run (``collecting``): the run takes
  over what it saw when it opens;
- a child process's (``in_child``; a forked child becomes one by itself): it cannot ask the checker, so it leaves what it
  sees in a file its parent's run picks up when it ends, and, enforced, refuses on its own.
"""
import dataclasses
import fnmatch
import importlib.machinery
import json
import linecache
import os
import sys
import tempfile
import threading
import time

from .. import _codescan, _protocol
from .._settings import STRICT, STRICT_ALLOW
from ..errors import CheckNotRun, ContractViolation
from . import _state, placing, wrapping
from ._state import Seen

#: Where a finding with none of the job's code on the stack is said to be.
NOWHERE = "<not in the job's code>"
#: The most internal failures a run lists; one is enough to say the run was not all watched.
MAX_ERRORS = 20
#: Set, while a run is watched, for the Python processes the job starts: the file they leave what they see in.
SPILL = "INVARACT_STRICT_SPILL"


@dataclasses.dataclass
class Entry:
    """One call site's finding: how many times it was seen, whether it has been sent, and the refusal it met."""

    finding: object
    times: int = 1
    sent: bool = False
    refusal: object = None

    def to_send(self):
        """The finding as it is sent: with how many times it was seen, when more than once."""
        sent = self.finding.to_dict()
        if self.times > 1:
            sent["detail"] = f"{self.finding.detail}; {self.times} times in all"
        return sent


class Watcher:
    def __init__(self, lint, enforce, allow=(), when="as the job ran", spill=None):
        self._lint = lint
        self.enforce = enforce
        self.allow = list(allow)
        #: when what it sees is seen, as a finding says it
        self._when = when
        self._run = None
        self._lock = threading.RLock()
        #: findings by where they are and what they are (``_key``)
        self._found = {}
        self._errors = []
        self._sent_any = False
        self._started = None
        #: where a child process leaves what it saw, and whether this is one
        self._spill = spill
        self._child = spill is not None
        self._outer = None
        self._markers = {}
        self._scratch = scratch_roots()

    @classmethod
    def collecting(cls, lint, allow=()):
        """The watcher of what a job does before its run opens: it refuses nothing, and the run that opens next takes over
        what it saw."""
        return cls(lint, False, allow, when="before the run opened")

    @classmethod
    def in_child(cls, lint, enforce, allow, spill):
        """The watcher of a Python process a watched job started: it leaves what it sees in ``spill``."""
        return cls(lint, enforce, allow, spill=spill)

    @property
    def scanner(self):
        from .. import __version__
        return f"invaract strict {__version__}"

    # a run's watch

    def start(self, run):
        """Starts watching, for ``run``: from here to ``stop``, what the job does is seen. What was seen since the package
        was imported, and the job's own compiled modules already loaded, are this run's findings too; enforced, one of
        them no marker explains refuses the run here."""
        wrapping.wrap_loaded()
        wrapping.rebind_everywhere()
        self._run = run
        self._started = time.monotonic()
        self._spill = os.path.join(tempfile.gettempdir(), f"invaract-strict-{os.getpid()}-{run.run_id}.jsonl")
        self._outer, _state.watcher = _state.watcher, self
        self._outer_spill, os.environ[SPILL] = os.environ.get(SPILL), self._spill
        with _state.working():
            self._take_over(self._outer)
            self._note_compiled_modules()

    def stop(self):
        """Stops watching and returns the ``scan`` request for what was seen and not yet sent, or ``None`` when everything
        was (an enforced run refused on its one finding has nothing more to say)."""
        if _state.watcher is self:
            _state.watcher = self._outer
        if self._started is None:
            return None
        if self._outer_spill is None:
            os.environ.pop(SPILL, None)
        else:
            os.environ[SPILL] = self._outer_spill
        with self._lock:
            self._gather()
            unsent = [entry for entry in self._found.values() if not entry.sent]
            elapsed, self._started = round((time.monotonic() - self._started) * 1000), None
            if not unsent and self._sent_any and not self._errors:
                return None
            for entry in unsent:
                entry.sent = True
            return _protocol.scan("runtime", self.enforce, self.scanner, [], [entry.to_send() for entry in unsent], self._errors, elapsed)

    def in_forked_child(self):
        """This process is a fork of the watched one: what it sees from here is left for its parent."""
        self._child = True
        self._found = {}
        self._lock = threading.RLock()

    def _take_over(self, earlier):
        """Makes what an earlier watcher of this process saw before the run opened this run's."""
        if earlier is None or earlier._child:
            return  # a child's findings are with its parent already
        with earlier._lock:
            entries, earlier._found = list(earlier._found.items()), {}
        for key, entry in entries:
            self._found[key] = entry
            if self.enforce and not entry.finding.allowed:
                self._refuse(entry)

    def _note_compiled_modules(self):
        """The job's own compiled modules that were loaded before anything watched: what they read or write, nothing sees."""
        known = {entry.finding.target for entry in self._found.values()}
        for name, module in list(sys.modules.items()):
            filename = getattr(module, "__file__", None)
            if isinstance(filename, str) and filename.endswith(tuple(importlib.machinery.EXTENSION_SUFFIXES)):
                seen = placing.compiled_module(name, filename)
                if seen is not None and self._lint.short(seen.target) not in known:
                    self._record(seen, None, "loaded before the run opened")

    # what was seen

    def saw(self, seen):
        """Accounts for ``seen``. Returns whether it was accounted for (a finding, or the package's own doing); false for
        what is no finding (a scratch file). Enforced, a finding no marker explains raises the checker's refusal. A failure
        of the watching itself is listed, and, enforced, leaves the run not checked: it is never let through."""
        with _state.working():
            try:
                return self._account(seen)
            except (ContractViolation, CheckNotRun):
                raise
            except Exception as e:  # noqa: BLE001 - the job's open() must not fail on a fault of the watch's own, unless enforced
                with self._lock:
                    if len(self._errors) < MAX_ERRORS:
                        self._errors.append(("<strict mode>", f"{type(e).__name__}: {e} while watching {seen.call}"))
                if self.enforce:
                    failure = CheckNotRun(f"Invaract's strict mode could not watch {seen.call} ({type(e).__name__}: {e}), so the "
                                          f"run is not checked ({STRICT}=enforce)")
                    if self._run is not None and self._run._failure is None:
                        self._run._failure = failure
                    raise failure from e
                return False

    def _account(self, seen):
        if seen.paths and all(self._is_scratch(path) for path in seen.paths):
            return False
        if seen.code == self._lint.READ and seen.paths and all(placing.is_a_librarys(os.path.realpath(p)) for p in seen.paths):
            return False  # a library's own data files
        if seen.code == self._lint.PROCESS and placing.is_a_probe(sys._getframe(1), seen.target):
            return False  # Python asking the system about itself
        whose, frame = placing.place(sys._getframe(1), seen.direct)
        if whose == placing.INVARACT:
            return True
        if whose == placing.LIBRARY and seen.direct:
            return False  # a library's own use of it
        note = None if frame is not None else f"in thread {threading.current_thread().name}, with none of the job's code calling it"
        self._record(seen, frame, note)
        return True

    def _record(self, seen, frame, note=None):
        """Keeps ``seen`` as a finding at ``frame`` (or nowhere in the job's code), or counts it once more."""
        if frame is not None:
            path, line, column, end_line, end_column, scope, snippet = placing.position(frame)
        else:
            path, line, column, end_line, end_column, scope, snippet = NOWHERE, 0, 0, 0, 0, f"<thread {threading.current_thread().name}>", ""
        key = (path, line, column, seen.rule, None if frame is not None else seen.target)
        with self._lock:
            entry = self._found.get(key)
            if entry is not None:
                entry.times += 1
                if entry.refusal is not None:
                    # refused once, refused every time: a library that tries again (zipfile, shutil.copytree) gets no further
                    raise ContractViolation(entry.refusal.violations, entry.refusal.explanation)
                return
            allowed, reason = self._explained(seen, frame, line, end_line)
            # what it reached may be a connection string: its password is never kept
            did = f"seen {self._when}: {self._lint.short(seen.did)}" + (f", {note}" if note else "")
            finding = self._lint.Finding(
                path, line, column, seen.code, seen.call, end_line=end_line, end_col=end_column, resolved=seen.call, scope=scope,
                rule=seen.rule, snippet=self._lint.short(snippet or seen.call, code=True), target=self._lint.short(seen.target) if seen.target else None,
                detail=did, allowed=allowed, reason=reason)
            entry = self._found[key] = Entry(finding)
            if self._child:
                self._leave(finding)
        if self.enforce and not allowed:
            self._refuse(entry)

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

    # refusing, and what a child process saw

    def _refuse(self, entry):
        """Sends one finding at once, enforced: the checker refuses the run (unless it is a dry run, which reports), and the
        refusal is raised here, in the thread about to act, and again each time the same call is tried. A child process
        cannot ask the checker: it raises a refusal of its own, and its parent is refused when it picks up what the child
        saw."""
        finding = entry.finding
        try:
            if self._child:
                raise ContractViolation(["UNGOVERNED_DATA_ACCESS"], f"Invaract refused this run ({STRICT}=enforce): {finding.message}. "
                                                                  f"{finding.detail} ({finding.path}:{finding.line})")
            entry.sent = self._sent_any = True
            self._run._ask(_protocol.scan("runtime", True, self.scanner, [], [entry.to_send()], [], 0))
        except ContractViolation as refusal:
            entry.refusal = refusal
            raise

    def _leave(self, finding):
        with open(self._spill, "a", encoding="utf-8") as spill:
            spill.write(json.dumps(dataclasses.asdict(finding)) + "\n")

    def _gather(self):
        """Adds what child processes saw to what this process saw."""
        if self._child or not os.path.exists(self._spill):
            return
        with _state.working():
            with open(self._spill, encoding="utf-8") as spill:
                rows = [json.loads(line) for line in spill if line.strip()]
            os.remove(self._spill)
        for row in rows:
            finding = self._lint.Finding(**{**row, "detail": row["detail"] + ", in a child process"})
            key = (finding.path, finding.line, finding.col, finding.rule, finding.target, "child")
            if key in self._found:
                self._found[key].times += 1
            else:
                self._found[key] = Entry(finding)


def _named(seen):
    """The names ``INVARACT_STRICT_ALLOW`` may match what was reached by: as it was reached, and for a local path its full
    path and its path from the working folder."""
    names = [seen.target] if seen.target else []
    for path in seen.paths:
        real = os.path.realpath(path)
        names += [real, os.path.relpath(real, os.path.realpath(os.getcwd()))]
    return names


def scratch_roots():
    """Where what is written is never data at rest: the temporary folder, the devices, and the user's cache folder, where
    libraries keep what they can rebuild."""
    home = os.path.expanduser("~")
    roots = [tempfile.gettempdir(), "/dev", "/proc", os.environ.get("XDG_CACHE_HOME") or os.path.join(home, ".cache"),
             os.path.join(home, "Library", "Caches"), os.environ.get("LOCALAPPDATA"), sys.pycache_prefix]
    return [os.path.realpath(root) for root in roots if root]
