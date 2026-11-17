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
from .errors import CheckNotRun, ContractViolation
from ._strict import _state
from ._strict.watcher import SPILL, new_spill, owner, parents_spill

#: Where Invaract attached: ``startup`` (the ``.pth``), ``launcher``, or ``import``; ``None`` before it has.
point = None
#: The main program, ``(kind, name)``, once known.
program = None
#: How many runs this process opened.
runs_opened = 0
#: The refusal a run Invaract opened for the job met (``INVARACT_AUTO_RUN``): the job is refused for good.
refusal = None

_exit_codes = []
_main_returned = None
_launched = None
_settled = False
_lock = threading.RLock()
#: The job's root process: the one it started as (``_begin``). A process forked from it is part of the job, never its root.
_root_pid = None
#: Whether the main program is a job's, once known (``is_job_process``).
_job = None
#: The exit handlers registered after ours, when ours is not the first (attached at import or by the launcher), as a set
#: that keeps their order.
_registered_after = None


def attach():
    """Attaches this process, as the package is imported. A process the platform asked nothing of gets the watch alone,
    as before (``INVARACT_STRICT``); a job process is also settled when it ends."""
    global point, program, _job
    watcher = _strict.install_early()
    if point is not None or not first_stage.wanted():
        return
    if first_stage.program is not None:
        point, program, _job = "startup", first_stage.program, True  # the first stage attaches jobs only
    else:
        point, program = "import", _main_program()
        if program is not None:
            _job = first_stage.is_job(*program)
            if not _job:
                return
    _begin(watcher)
    if point == "import":
        _watch_main_from_stack()


def launched(kind, name):
    """The launcher's start: the program it is about to run is the job, whatever the patterns say (the platform named it)."""
    global point, program, _job
    point, program, _job = "launcher", (kind, name), True
    _begin(_state.watcher)


def _begin(watcher):
    """What a job process needs: its spill file (if it is a root), the record of how it ends, and the handler that settles
    it. A process started by a job is a child: it only watches."""
    global _root_pid
    with _lock:
        if _root_pid == os.getpid() or parents_spill() is not None:
            return
        _root_pid = os.getpid()
        if watcher is not None and not watcher._child:
            # a job that replaced itself (os.exec*) keeps the file its children already report to
            own = os.environ.get(SPILL)
            watcher._spill = _state.job_spill = own if own and owner(own) == os.getpid() else new_spill()
            os.environ[SPILL] = _state.job_spill
            try:
                _, strict, auto = asked()
            except ValueError:
                strict, auto = None, False  # said when a run opens, or when the job ends
            if auto and strict == "enforce":
                # what nothing explains is refused where it happens: Invaract opens the run that refuses it
                watcher.on_unexplained = _refuse_outside_run
        _record_exits()
        if not first_stage.handles_exit:
            _register_at_exit()


def _register_at_exit():
    """Registers ``at_exit``. Python runs exit handlers last registered first, and the handler that sets the exit status
    ends the process: one registered before ours (a library the job imported first, or the job's own) would never run.
    So the handlers registered from here on are kept, and ``_set_status`` runs the earlier ones before ending."""
    global _registered_after
    _registered_after = {}
    register, unregister = atexit.register, atexit.unregister

    def registering(func, *args, **kwargs):
        _registered_after[func] = None
        return register(func, *args, **kwargs)

    def unregistering(func):
        _registered_after.pop(func, None)
        return unregister(func)

    registering.__wrapped__, unregistering.__wrapped__ = register, unregister
    register(at_exit)
    atexit.register, atexit.unregister = registering, unregistering


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
    """Whether this process is the root of a job: attached, and its main program a job's (worked out once, when it is
    known: attached at import while ``-m`` was still finding the module, it was not)."""
    global program, _job
    if os.getpid() != _root_pid:
        return False
    if _job is None:
        program = program or _main_program()
        if program is None:
            return False
        _job = first_stage.is_job(*program)
    return _job


def asked(environ=None):
    """What the platform asked of a job: ``(require, strict, auto)``, the modes of ``INVARACT_REQUIRE_RUN`` and of the watch
    (``_settings.strict_mode``), and whether Invaract opens the run (``INVARACT_AUTO_RUN``). ``ValueError`` names a value
    the package does not know, ``INVARACT_ATTACH``'s included."""
    environ = os.environ if environ is None else environ
    if _settings.value(environ, _settings.ATTACH) not in ("", "startup", "import"):
        raise ValueError(f"{_settings.ATTACH} must be startup or import, not {environ[_settings.ATTACH]!r}")
    return (_settings.mode(environ, _settings.REQUIRE_RUN), _settings.strict_mode(environ),
            _settings.flag(environ, _settings.AUTO_RUN))


def check(environ=None):
    """Refuses to open a run the attach settings leave unchecked: a value the package does not know, or, with any of its
    checks enforced, a process start-up attach failed in (so what the job did before this run was not seen). A job a run
    Invaract opened for it has refused stays refused."""
    _still_refused()
    environ = os.environ if environ is None else environ
    try:
        require, strict, _ = asked(environ)
        lint = _settings.mode(environ, _codescan.LINT)
    except ValueError as e:
        raise CheckNotRun(str(e)) from None
    enforced = [name for name, value in ((_settings.REQUIRE_RUN, require), (_codescan.LINT, lint), (_settings.STRICT, strict))
                if value == "enforce"]
    failed = environ.get(_settings.FAILED)
    if failed and enforced:
        raise CheckNotRun(f"Invaract could not attach to this process as it started ({failed}), so what the job did before this "
                          f"run was not seen, and the run is not checked ({', '.join(enforced)}=enforce)")


def opened():
    """A run opened in this process."""
    global runs_opened
    runs_opened += 1


# the run Invaract opens for the job (INVARACT_AUTO_RUN)

def started_by_a_job():
    """Whether this process was started by a job's (it reports to that job's process)."""
    return not is_job_process() and SPILL in os.environ


def implicit_run():
    """The run Invaract opened for the job, once it is open, opening it if the platform asked for it
    (``INVARACT_AUTO_RUN=true``) and this is the root of a job with no run open; ``None`` otherwise. The run lasts until
    the process ends (``at_exit``). Threads calling at once wait for the one run to open."""
    from . import _run
    _still_refused()
    try:
        auto = asked()[2]
    except ValueError as e:
        _run._exit_status_on_check_not_run()
        raise CheckNotRun(str(e)) from None
    with _lock:
        current = _run.implicit_current()  # opened by another thread while this one waited
        if current is not None:
            return current
        if not (auto and is_job_process()):
            return None
        return _open_implicit()


def _open_implicit(scanned=None):
    """Opens a run for the job, by the contract the platform names (or a dry run); ``scanned``, the scan of the job's code
    already made as the process ends."""
    from . import _run
    _run._exit_status_on_check_not_run()
    settings = _settings.from_environment()
    if "contract" not in settings and settings.get("dryRun", "").strip().lower() != "true":
        raise CheckNotRun(f"Invaract opens this job's run ({_settings.AUTO_RUN}=true), and no contract says what the job may "
                          f"do: set {_settings.variable('contract')}, or {_settings.variable('dryRun')}=true to observe it")
    run = _run.Run(implicit=True, scanned=scanned)
    run.__enter__()
    return run


def _still_refused():
    """A job a run Invaract opened for it has refused stays refused: nothing after it opens a run."""
    if refusal is not None:
        raise ContractViolation(refusal.violations, refusal.explanation)


def _refuse_outside_run():
    """The watch saw what nothing explains outside any run, enforced: the run Invaract opens refuses it there."""
    global refusal
    try:
        implicit_run()
    except ContractViolation as refused:
        refusal = refused
        raise


def attributes(given, run="auto"):
    """``jobAttributes`` for a run Invaract opened (``invaract.run=auto``) or for the report of a job that opened none
    (``none``): what the platform set, and those and where Invaract attached (``invaract.attach``), unless the platform set
    the keys itself."""
    pairs = _codescan._split(given)
    keys = {p.split("=", 1)[0].strip() for p in pairs}
    for key, value in (("invaract.run", run), ("invaract.attach", point or "import")):
        if key not in keys:
            pairs.append(f"{key}={value}")
    return ",".join(pairs)


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

    def returned(returning, offset, value):
        global _main_returned
        if returning is code:
            _main_returned = True
        return monitoring.DISABLE

    for tool in (4, 3):
        try:
            monitoring.use_tool_id(tool, "invaract")
        except ValueError:
            continue  # another tool holds it
        try:
            monitoring.register_callback(tool, monitoring.events.PY_RETURN, returned)
            monitoring.set_local_events(tool, code, monitoring.events.PY_RETURN)
        except Exception:  # noqa: BLE001 - how the job ended is then told as on Python 3.11
            monitoring.free_tool_id(tool)
            return
        _main_returned = False
        return


def _watch_main_from_stack():
    main = getattr(sys.modules.get("__main__"), "__dict__", None)
    frame = sys._getframe()
    while frame is not None:
        if frame.f_globals is main and frame.f_code.co_name == "<module>":
            watch_main(frame.f_code)
            return
        frame = frame.f_back


def launched_ended(status):
    """The launcher's account of how the program it ran ended, as its exit status: exact."""
    global _launched
    _launched = status


def outcome():
    """How the job ended: ``completed``, ``failed`` (it raised, or exited with a status other than 0) or ``unknown``."""
    if _launched is not None:
        return "completed" if _launched == 0 else "failed"
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
    except BaseException:  # noqa: BLE001 - an exit handler that raises leaves the job's status as it was: fail closed
        import traceback
        _said_only(traceback.format_exc())
        _say("Invaract could not settle this job as it ended, so it was not checked.", [])
        status = CheckNotRun.exit_status
    finally:
        _remove_spill(watcher)
    _set_status(status)


def _settle(watcher):
    """The exit status the job ends with because of what it did outside any run, or ``None`` to leave it."""
    from . import _run
    try:
        require, strict, auto = asked()
    except ValueError as e:
        _say(f"Invaract: {e}, so this job was not checked.", [])
        return 2
    if refusal is not None:
        return 1
    run = _run.implicit_current()
    if run is not None:
        return _close(run)
    if auto:
        return _run_at_the_end(watcher)
    if runs_opened == 0:
        return _no_run(watcher, require, strict)
    return _after_runs(watcher, strict)


def _close(run):
    """Ends the run Invaract opened for the job: published if the job completed; not, if it failed, or if how it ended
    is not known and it has outputs to publish."""
    ended = outcome()
    if ended == "unknown" and run._outputs:
        run.__exit__(RuntimeError, RuntimeError("how the job ended is not known"), None)
        _say("Invaract could not tell whether this job completed, so its outputs are not published: end it with "
             "sys.exit(<status>), or start it with python -m invaract.", [])
        return 2
    if ended == "failed":
        run.__exit__(RuntimeError, RuntimeError("the job failed"), None)
        return None
    return _ending(lambda: run.__exit__(None, None, None))


def _run_at_the_end(watcher):
    """No run is open as the job ends, and Invaract opens runs for it: one that has something to say gets a run that says
    it (what it did outside any run, or what the scan of its code found), and is judged by it. The scan made to find out
    is the run's own: the job's code is scanned once."""
    scanned, unscannable = None, False
    try:
        scanner = _codescan.from_environment()
        if scanner is not None:
            scanned = (scanner, scanner.start())
    except CheckNotRun:
        unscannable = True  # said, and judged, by the run that opens for it
    found = watcher is not None and watcher.seen()[0]
    if not (found or unscannable or (scanned and (scanned[1]["findings"] or scanned[1]["errors"]))):
        return None
    return _ending(lambda: _open_implicit(scanned).__exit__(None, None, None))


def _ending(action):
    """Runs ``action`` as the process ends: a refusal ends the job refused (1), a check that could not be made ends it not
    checked (2), and any other failure (an output that could not be published) ends it as an uncaught error would (1),
    each said as Python would have said it."""
    try:
        action()
    except ContractViolation as e:
        _said(e)
        return ContractViolation.exit_status
    except CheckNotRun as e:
        _said(e)
        return CheckNotRun.exit_status
    except Exception:  # noqa: BLE001 - what an annotated run would have raised to the job
        import traceback
        _said_only(traceback.format_exc())
        return 1
    return None


def _said(error):
    _said_only(f"{type(error).__module__}.{type(error).__qualname__}: {error}\n")


def _said_only(text):
    try:
        sys.stderr.write(text)
        sys.stderr.flush()
    except Exception:  # noqa: BLE001 - nowhere left to say it
        pass


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
        outside = _outside_runs(watcher)
        if outside is None:
            return None
        unexplained, errors = outside
        did = "read or wrote data" if unexplained else "started processes whose reports could not be read"
        _say(f"Invaract: this job {did} and opened no run, so nothing checked it ({_settings.REQUIRE_RUN}={require}).",
             unexplained, _HOW_TO_OPEN_A_RUN, errors)
        _report_without_run(watcher)
    return 2 if require == "enforce" else None


def _after_runs(watcher, strict):
    """What the job did after its last run ended: reported, and, enforced, the job ends refused."""
    outside = _outside_runs(watcher) if watcher is not None else None
    if outside is None:
        return None
    unexplained, errors = outside
    _say(f"Invaract: this job read or wrote data after its run ended, so nothing checked it ({_settings.STRICT}={strict}).",
         unexplained, None, errors)
    return 1 if strict == "enforce" else None


def _outside_runs(watcher):
    """What the watch saw that nothing explains, and what it could not do: ``(unexplained, errors)``, or ``None`` for
    neither."""
    findings, errors = watcher.seen()
    unexplained = [finding for finding in findings if not finding.allowed]
    return (unexplained, errors) if unexplained or errors else None


def _report_without_run(watcher):
    """Publishes what the watch saw of a job that opened no run, where the platform's notifications go: a ``CODE_SCAN``
    event and the job's summary, marked ``invaract.run=none``. Nothing is judged, and a failure to report is said and
    changes nothing."""
    try:
        from . import _run
        _run.report_without_run(watcher.everything())
    except Exception as e:  # noqa: BLE001 - the job's ending is decided already
        _said_only(f"Invaract could not report this job ({type(e).__name__}: {e})\n")


_HOW_TO_OPEN_A_RUN = "Decorate the job's entry point with @invaract.job(), or use 'with invaract.run():'."


def _say(headline, findings, footer=None, errors=()):
    lines = [headline] + [f"  {f.path}:{f.line} {f.message}" for f in findings[:5]]
    if len(findings) > 5:
        lines.append(f"  and {len(findings) - 5} more")
    lines += [f"  {where}: {message}" for where, message in errors]
    if footer:
        lines.append(footer)
    _said_only("\n".join(lines) + "\n")


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
    try:
        _run_earlier_exit_handlers()
        logging.shutdown()
        for stream in (sys.stdout, sys.stderr):
            if stream is not None:
                stream.flush()
    finally:
        os._exit(status)


def _run_earlier_exit_handlers():
    """Runs the exit handlers registered before ours, which ``os._exit`` would skip; those registered after it have run."""
    if _registered_after is None or not hasattr(atexit, "_run_exitfuncs"):
        return  # registered first (start-up attach): nothing runs after it
    atexit.register, atexit.unregister = (getattr(f, "__wrapped__", f) for f in (atexit.register, atexit.unregister))
    for func in [*_registered_after, at_exit]:
        atexit.unregister(func)
    try:
        atexit._run_exitfuncs()
    except Exception:  # noqa: BLE001 - Python itself only reports a failing exit handler
        pass
