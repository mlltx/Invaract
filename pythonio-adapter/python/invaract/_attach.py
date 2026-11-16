# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract attached to a job process, whether or not the job opens a run (docs/PYTHONIO_ATTACH_DESIGN.md).

A process attaches once: as Python starts its main program (``_invaract_attach``, through the wheel's ``.pth``), as the
launcher starts it (``python -m invaract``), or, failing both, as the job imports this package. Attaching starts the watch
(``_strict``), when the platform asked for anything a watch serves, and arranges for the process to be settled when it
ends (``at_exit``): what the job did outside any run is reported, and, enforced, the job's exit status says so.

A **job process** is one whose main program is the job's own code (``_invaract_attach.is_job``). Only a job process is
settled. A job process started by another one (its ``INVARACT_STRICT_SPILL`` set) is part of that job: it leaves what it
sees for the **root**, the process the job started as, which settles the whole job.
"""
import atexit
import logging
import os
import sys
import threading

import _invaract_attach as first_stage

from . import _codescan, _settings, _strict
from .errors import CheckNotRun
from ._strict import _state
from ._strict.watcher import SPILL, new_spill

#: Where Invaract attached: ``startup`` (the ``.pth``), ``launcher``, or ``import``; ``None`` before it has.
point = None
#: The main program, ``(kind, name)``, once known.
program = None
#: Whether this process is the root of a job (it settles the job when it ends).
root = False
#: How many runs this process opened.
runs_opened = 0

_exit_codes = []
_main_returned = None
_launched = None
_settled = False
_lock = threading.RLock()


def attach():
    """Attaches this process, as the package is imported. A process the platform asked nothing of gets the watch alone,
    as before (``INVARACT_STRICT``); a job process is also settled when it ends."""
    global point, program
    watcher = _strict.install_early()
    if point is not None or not first_stage.wanted():
        return
    if first_stage.program is not None:
        point, program = "startup", first_stage.program
    else:
        point, program = "import", _main_program()
    if program is not None and not first_stage.is_job(*program):
        return
    _begin(watcher)
    if point == "import":
        _watch_main_from_stack()


def launched(kind, name):
    """The launcher's start: the program it is about to run is the job."""
    global point, program
    point, program = "launcher", (kind, name)
    _begin(_state.watcher)


def _begin(watcher):
    """What a job process needs: its spill file (if it is a root), the record of how it ends, and the handler that settles
    it. A process started by a job is a child: it only watches."""
    global root
    with _lock:
        if root or SPILL in os.environ:
            return
        root = True
        if watcher is not None and not watcher._child:
            watcher._spill = new_spill()
            os.environ[SPILL] = watcher._spill
        _record_exits()
        if not first_stage.handles_exit:
            atexit.register(at_exit)


def _main_program():
    """The main program, from ``__main__``: ``None`` while ``-m`` is still finding the module (its packages are imported
    before it runs)."""
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    if spec is not None and spec.name:
        return "module", spec.name
    if getattr(main, "__file__", None):
        return "script", main.__file__
    argv0 = sys.argv[0] if sys.argv else ""
    if argv0 == "-m":
        return None
    return {"-c": ("command", None), "-": ("stdin", None)}.get(argv0, ("interactive", None))


def is_job_process():
    """Whether this process is the root of a job: attached, and its main program a job's (worked out now if it was not
    known when it attached)."""
    global program
    if not root:
        return False
    if program is None:
        program = _main_program()
    return program is not None and first_stage.is_job(*program)


def check(environ=None):
    """Refuses to open a run the attach settings leave unchecked: a value the package does not know, or, with any of its
    checks enforced, a process start-up attach failed in (so what the job did before this run was not seen)."""
    environ = os.environ if environ is None else environ
    try:
        if environ.get(_settings.ATTACH, "").strip().lower() not in ("", "startup", "import"):
            raise ValueError(f"{_settings.ATTACH} must be startup or import, not {environ[_settings.ATTACH]!r}")
        enforced = [name for name in (_settings.REQUIRE_RUN, _codescan.LINT) if _settings.mode(environ, name) == "enforce"]
        if _settings.strict_mode(environ) == "enforce":
            enforced.append(_settings.STRICT)
        _settings.flag(environ, _settings.AUTO_RUN)
    except ValueError as e:
        raise CheckNotRun(str(e)) from None
    failed = environ.get(_settings.FAILED)
    if failed and enforced:
        raise CheckNotRun(f"Invaract could not attach to this process as it started ({failed}), so what the job did before this "
                          f"run was not seen, and the run is not checked ({', '.join(enforced)}=enforce)")


def opened():
    """A run opened in this process."""
    global runs_opened
    runs_opened += 1


# how the job ended

def _record_exits():
    """Keeps the code of every ``sys.exit`` and ``exit()`` the main thread makes: how a job ended, where Python keeps no
    record of it."""
    import builtins
    sys.exit = _Recording(sys.exit)
    for name in ("exit", "quit"):
        if hasattr(builtins, name):
            setattr(builtins, name, _Recording(getattr(builtins, name)))


class _Recording:
    def __init__(self, original):
        self._original = original

    def __call__(self, code=None):
        if threading.current_thread() is threading.main_thread():
            _exit_codes.append(code)
        return self._original(code)

    def __repr__(self):
        return repr(self._original)


def watch_main(code):
    """Notes when the main program's code returns (Python 3.12 and later): a job that returned completed. Earlier
    versions say nothing, and a job that neither failed nor called ``sys.exit`` is taken to have completed."""
    global _main_returned
    monitoring = getattr(sys, "monitoring", None)
    if monitoring is None or code is None or _main_returned is not None:
        return
    for tool in (4, 3):
        if monitoring.get_tool(tool) is None:
            break
    else:
        return
    monitoring.use_tool_id(tool, "invaract")

    def returned(returning, offset, value):
        global _main_returned
        if returning is code:
            _main_returned = True
        return monitoring.DISABLE

    monitoring.register_callback(tool, monitoring.events.PY_RETURN, returned)
    monitoring.set_local_events(tool, code, monitoring.events.PY_RETURN)
    _main_returned = False


def _watch_main_from_stack():
    main = getattr(sys.modules.get("__main__"), "__dict__", None)
    frame = sys._getframe()
    while frame is not None:
        if frame.f_globals is main and frame.f_code.co_name == "<module>":
            watch_main(frame.f_code)
            return
        frame = frame.f_back


def launched_ended(completed, status):
    """The launcher's account of how the program it ran ended: exact."""
    global _launched
    _launched = ("completed" if completed else "failed", status)


def outcome():
    """How the job ended: ``completed``, ``failed`` (it raised, or exited with a status other than 0) or ``unknown``."""
    if _launched is not None:
        return _launched[0]
    if getattr(sys, "last_exc", None) is not None or getattr(sys, "last_value", None) is not None:
        return "failed"
    if _main_returned:
        return "completed"
    if _exit_codes:
        return "completed" if _exit_codes[-1] in (None, 0, False) else "failed"
    return "unknown" if _main_returned is False else "completed"


# settling the job

def at_exit():
    """Settles the job as its process ends, after the job's own ``atexit`` handlers: says what it did outside any run, and,
    when that refuses the job, sets its exit status."""
    global _settled
    with _lock:
        if _settled:
            return
        _settled = True
    if not is_job_process():
        return
    watcher = _state.watcher if _state.watcher is not None and not _state.watcher._child else None
    status = None
    try:
        status = _settle(watcher)
    finally:
        _remove_spill(watcher)
    _set_status(status)


def _settle(watcher):
    """The exit status the job ends with because of what it did outside any run, or ``None`` to leave it."""
    try:
        require = _settings.mode(os.environ, _settings.REQUIRE_RUN)
        strict = _settings.strict_mode(os.environ)
    except ValueError as e:
        _say(f"Invaract: {e}, so this job was not checked.", [])
        return 2
    if runs_opened == 0:
        return _no_run(watcher, require, strict)
    return _after_runs(watcher, strict)


def _no_run(watcher, require, strict):
    """A job that opened no run (``INVARACT_REQUIRE_RUN``): one that touched data is reported, and, enforced, ends not
    checked. Without the watch, what it did is not known, so every job that opened no run counts."""
    if require == "off":
        return None
    if strict == "off":
        _say(f"Invaract: this job opened no run, so nothing checked it ({_settings.REQUIRE_RUN}={require}; with "
             f"{_settings.STRICT}=off what it did is not known).", [], _HOW_TO_OPEN_A_RUN)
    elif watcher is None:
        _say(f"Invaract: this job opened no run and could not be watched (is invaract-lint installed?), so nothing checked "
             f"it ({_settings.REQUIRE_RUN}={require}).", [], _HOW_TO_OPEN_A_RUN)
    else:
        unexplained = watcher.unexplained()
        if not unexplained:
            return None
        _say(f"Invaract: this job read or wrote data and opened no run, so nothing checked it ({_settings.REQUIRE_RUN}="
             f"{require}).", unexplained, _HOW_TO_OPEN_A_RUN)
    return 2 if require == "enforce" else None


def _after_runs(watcher, strict):
    """What the job did after its last run ended: reported, and, enforced, the job ends refused."""
    if watcher is None:
        return None
    unexplained = watcher.unexplained()
    if not unexplained:
        return None
    _say(f"Invaract: this job read or wrote data after its run ended, so nothing checked it ({_settings.STRICT}={strict}).",
         unexplained)
    return 1 if strict == "enforce" else None


_HOW_TO_OPEN_A_RUN = "Decorate the job's entry point with @invaract.job(), or use 'with invaract.run():'."


def _say(headline, findings, footer=None):
    lines = [headline] + [f"  {f.path}:{f.line} {f.message}" for f in findings[:5]]
    if len(findings) > 5:
        lines.append(f"  and {len(findings) - 5} more")
    if footer:
        lines.append(footer)
    try:
        sys.stderr.write("\n".join(lines) + "\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001 - nowhere left to say it
        pass


def _remove_spill(watcher):
    spill = getattr(watcher, "_spill", None)
    if spill and os.path.exists(spill):
        try:
            os.remove(spill)
        except OSError:
            pass


def _set_status(status):
    """Ends the process with ``status`` (1, refused, or 2, not checked), unless the job failed by itself and keeps its
    own. An ``atexit`` handler cannot change the status by raising; this one runs last, so it ends the process itself,
    once what is buffered is written."""
    if status is None or outcome() == "failed":
        return
    if _launched is not None and _launched[1] not in (0, None):
        return
    try:
        logging.shutdown()
        for stream in (sys.stdout, sys.stderr):
            if stream is not None:
                stream.flush()
    finally:
        os._exit(status)
