# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Watching what the job does as it runs (``INVARACT_STRICT``): a read or write made around the package is seen when it
happens, whatever the code that made it looks like, and, enforced, is stopped before it happens.

``INVARACT_STRICT`` is ``off`` (the default), ``report`` or ``enforce``. The scan of the job's source (``_codescan``) reads
code; this watches the run. What it finds goes to the checker as a ``scan`` of phase ``runtime`` (``_protocol.scan``),
published as a ``CODE_SCAN`` event: with ``report`` once, when the run ends; with ``enforce`` at once, and the checker's
refusal is raised where the job was about to act, so it does not happen.

The parts, each a module to read on its own:

- ``events``: what Python itself reports (its audit events), read as what the job did. Every file Python opens to write,
  every rename and delete, every SQLite database, socket, program started and native library loaded.
- ``judges``: what a call of a wrapped function is about to do, from its arguments. Libraries that read or write from
  compiled code raise no event, so their readers and writers are wrapped.
- ``wrapping``: installs the hooks and wraps what invaract-lint's catalogue names, in every library loaded and every
  library imported later. The catalogue is the one list of what counts, shared with the scan.
- ``placing``: whose doing something is (the package's own, the job's, a library's) and where in the job's code.
- ``watcher``: one watch. It leaves out what is no finding, keeps one finding per call site, and refuses when enforced.

What it lets through: the package's own reads and writes; scratch files (the temporary folder, bytecode, the user's cache
folder); a call a ``# invaract: allow`` marker keeps, as for the scan; what ``INVARACT_STRICT_ALLOW`` names.

What it cannot see is in docs/PYTHONIO_ADAPTER.md ("Strict mode"): what another program does once started; what compiled
code of the job's own does (loading it is reported); what the job does after its run ends.

To watch something new, see docs/PYTHONIO_LINT_AND_STRICT.md: nearly always it is a line in invaract-lint's catalogue.

invaract-lint holds the catalogue, so it must be installed (``pip install invaract[lint]``): a run asked to watch without it
is not checked.
"""
import os

from .. import _codescan
from .._settings import STRICT, STRICT_ALLOW
from ..errors import CheckNotRun
from . import _state, wrapping
from .watcher import SPILL, Watcher

MODES = ("off", "report", "enforce")

#: What invaract-lint's catalogue must have for strict mode to read it: an older one names less than is watched.
_CATALOGUE = ("COMPILED", "READERS", "SOURCES", "FILESYSTEMS", "watched_library", "watched_names", "opener_names", "frame_writer_names")


def from_environment(environ=None):
    """The watcher of a run that the environment asks for, or ``None`` when ``INVARACT_STRICT`` is off or not set."""
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
    wrapping.install(invaract_lint)
    return Watcher(invaract_lint, mode == "enforce", _codescan._split(environ.get(STRICT_ALLOW)))


def _names_what_to_watch(lint):
    """Whether an installed invaract-lint is one strict mode can read: it has the catalogue and the reader of SQL."""
    catalogue = getattr(lint, "catalogue", None)
    return all(hasattr(catalogue, name) for name in _CATALOGUE) and hasattr(lint, "sqltext")


def install_early(environ=None):
    """Starts watching as the package is imported, when the environment asks for strict mode: a library the job imports
    afterwards is wrapped as it loads, and what the job does before its run opens is kept for that run. In a Python process
    a watched job started, what is seen is left for that job's run. Never raises: a run that cannot watch says so when it
    opens."""
    environ = os.environ if environ is None else environ
    mode = environ.get(STRICT, "").strip().lower()
    if mode not in ("report", "enforce"):
        return
    try:
        import invaract_lint
        if not _names_what_to_watch(invaract_lint):
            return
        wrapping.install(invaract_lint)
        if _state.watcher is None:
            allow, spill = _codescan._split(environ.get(STRICT_ALLOW)), environ.get(SPILL)
            _state.watcher = (Watcher.in_child(invaract_lint, mode == "enforce", allow, spill) if spill
                              else Watcher.collecting(invaract_lint, allow))
    except Exception:  # noqa: BLE001 - said when the run opens (from_environment), where it can be refused
        pass
