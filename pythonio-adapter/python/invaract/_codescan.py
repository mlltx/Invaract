# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Scanning the job's own code with invaract-lint when a run starts, so a read or write made around the package is found
before the job reads anything (``INVARACT_LINT``).

``INVARACT_LINT`` is ``off`` (the default), ``report`` or ``enforce``. The scan covers the job's own code: every module
loaded when the run starts that is not part of Python or an installed package (``job_files``), or the files and folders
``INVARACT_LINT_PATHS`` names instead (comma-separated); ``INVARACT_LINT_EXCLUDE`` leaves files out (names or globs). What it
finds goes to the checker (``_protocol.scan``), which publishes it as a ``CODE_SCAN`` event and, with ``enforce``, refuses
the run for a call no ``# invaract: allow`` marker explains. A job that imports more of its own code while it runs is
scanned again, for those files, before the run finishes, so a module imported late is not missed.

invaract-lint is an optional dependency (``pip install invaract[lint]``): a run asked to scan without it is not checked,
never let through unscanned.
"""
import os
import site
import sys
import sysconfig
import time

from . import _protocol
from ._settings import LINT, LINT_EXCLUDE, LINT_PATHS
from .errors import CheckNotRun

MODES = ("off", "report", "enforce")

#: Modules Python loads at start-up from wherever the environment keeps them (Debian's is in /etc): the environment's, not the
#: job's.
STARTUP_HOOKS = ("sitecustomize", "usercustomize")


def from_environment(environ=None):
    """The scanner the environment asks for, or ``None`` when ``INVARACT_LINT`` is off or not set."""
    environ = os.environ if environ is None else environ
    mode = environ.get(LINT, "").strip().lower() or "off"
    if mode not in MODES:
        raise CheckNotRun(f"{LINT} must be off, report or enforce, not {environ[LINT]!r}")
    if mode == "off":
        return None
    try:
        import invaract_lint
    except ImportError:
        raise CheckNotRun(f"{LINT}={mode} asks for the job's code to be scanned, and invaract-lint is not installed: install it "
                          "(pip install invaract[lint]), or set it to off") from None
    return Scanner(invaract_lint, mode == "enforce", _split(environ.get(LINT_PATHS)), _split(environ.get(LINT_EXCLUDE)))


class Scanner:
    """Scans the job's code at the run's start and, for files loaded since, at its finish; each time, the request to send."""

    def __init__(self, lint, enforce, paths=(), exclude=()):
        self._lint = lint
        self.enforce = enforce
        self.paths = list(paths)
        self.exclude = list(exclude)
        self._scanned = set()

    @property
    def scanner(self):
        return f"invaract-lint {self._lint.__version__}"

    def start(self):
        """The ``scan`` request for the code there is when the run starts."""
        files = self.paths if self.paths else job_files()
        return self._scan("start", list(self._lint.python_files(files, self.exclude)))

    def finish(self):
        """The ``scan`` request for the job's own modules loaded since the run started, or ``None`` when there are none (or
        the platform named the paths to scan, which do not change)."""
        if self.paths:
            return None
        files = [f for f in self._lint.python_files(job_files(), self.exclude) if os.path.realpath(f) not in self._scanned]
        return self._scan("finish", files) if files else None

    def _scan(self, phase, files):
        started = time.monotonic()
        self._scanned.update(os.path.realpath(f) for f in files)
        result = self._lint.scan_files([_shown(f) for f in files])
        return _protocol.scan(phase, self.enforce, self.scanner, result.files, [f.to_dict() for f in result.findings], result.errors,
                              round((time.monotonic() - started) * 1000))


def job_files():
    """The job's own code: the file of every module loaded now that is not Python's own (its standard library and its
    start-up hooks), an installed package's, this package's or invaract-lint's, in a stable order."""
    excluded = _not_the_jobs()
    files = set()
    for name, module in list(sys.modules.items()):
        path = getattr(module, "__file__", None)
        if not path or not path.endswith(".py") or name in STARTUP_HOOKS:
            continue
        real = os.path.realpath(path)
        if not any(_inside(real, root) for root in excluded):
            files.add(real)
    return sorted(files)


def _not_the_jobs():
    """Where code that is not the job's lives: Python's standard library, every installed package's folder, and this
    package and invaract-lint wherever they are installed from."""
    roots = {sysconfig.get_paths().get(key) for key in ("stdlib", "platstdlib", "purelib", "platlib")}
    roots.update(site.getsitepackages() if hasattr(site, "getsitepackages") else [])
    roots.add(site.getusersitepackages() if hasattr(site, "getusersitepackages") else None)
    roots.add(os.path.dirname(os.path.abspath(__file__)))
    lint = sys.modules.get("invaract_lint")
    if lint is not None and getattr(lint, "__file__", None):
        roots.add(os.path.dirname(os.path.abspath(lint.__file__)))
    return [os.path.realpath(r) for r in roots if r]


def _inside(path, root):
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def _shown(path):
    """``path`` as findings name it: relative to the working folder when it is inside it, else as it is."""
    real = os.path.realpath(path)
    here = os.path.realpath(os.getcwd())
    return os.path.relpath(real, here) if _inside(real, here) else path


def _split(value):
    return [part.strip() for part in (value or "").split(",") if part.strip()]
