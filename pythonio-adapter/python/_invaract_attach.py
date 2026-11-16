# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Attaches Invaract to a Python job as the interpreter starts (docs/PYTHONIO_ATTACH_DESIGN.md, section 7).

The wheel ships ``00_invaract_attach.pth``, whose one line calls ``at_startup()`` in every Python process of the
environment, before any of its code runs. Unless the platform asked for Invaract (``wanted``), that is all that happens.
Otherwise an audit hook waits for Python to say what the main program is (a script, a module, ``-c``, standard input),
and, if it is a job (``is_job``), imports the ``invaract`` package, which attaches: the watch starts, and what the job
does is settled when the process ends (``invaract._attach``).

This module is the first stage and must stay cheap and safe: it imports only the standard library, runs in processes that
have nothing to do with Invaract (``pip``, a test runner), and never lets a fault of its own stop Python from starting.
Its rules for which processes are jobs are the package's too (``invaract._attach`` imports them from here).
"""
import os
import sys

ATTACH, INCLUDE, EXCLUDE = "INVARACT_ATTACH", "INVARACT_ATTACH_INCLUDE", "INVARACT_ATTACH_EXCLUDE"
STRICT, REQUIRE_RUN, AUTO_RUN = "INVARACT_STRICT", "INVARACT_REQUIRE_RUN", "INVARACT_AUTO_RUN"
#: Set when this stage could not attach, with why: a run that opens later, enforced, is then not checked.
FAILED = "INVARACT_ATTACH_FAILED"

#: Python's audit events that say what the main program is, and the kind of program each one starts.
PROGRAM_EVENTS = {"cpython.run_file": "script", "cpython.run_module": "module", "cpython.run_command": "command",
                  "cpython.run_stdin": "stdin"}
#: The events of an interactive session: no job.
INTERACTIVE_EVENTS = ("cpython.run_interactivehook", "cpython.run_startup")
#: The launcher (``python -m invaract job.py``) attaches by itself, and what it starts is always a job.
LAUNCHER = "invaract"

#: What the main program is, once Python has said and it is a job: ``(kind, name)``; ``None`` otherwise.
program = None
#: Whether ``at_startup`` registered the end-of-process handler (registered first, it runs last).
handles_exit = False

_armed = False
#: The main program's file, while its code is awaited: a path, ``<string>`` for ``-c``, or ``_ARGV`` for a module.
_main_file = None
_ARGV = object()


def wanted(environ=None):
    """Whether the platform asked for anything start-up attach serves: the watch, a run required, or a run opened for the
    job. A value that is set and is not the one meaning "off" counts, so a value the package rejects is said, not ignored.
    ``INVARACT_ATTACH=import`` turns start-up attach off."""
    environ = os.environ if environ is None else environ
    if _value(environ, ATTACH) == "import":
        return False
    return _value(environ, STRICT) not in ("", "off") or _value(environ, REQUIRE_RUN) not in ("", "off") or \
        _value(environ, AUTO_RUN) not in ("", "false")


def _value(environ, name):
    return environ.get(name, "").strip().lower()


def at_startup():
    """The ``.pth`` line's call: arms the hook that waits for the main program, when the platform asked for Invaract."""
    global _armed, handles_exit
    try:
        if not wanted() or _armed:
            return
        import atexit
        sys.addaudithook(_hook)
        atexit.register(_at_exit)
        _armed = handles_exit = True
    except Exception as e:  # noqa: BLE001 - a start-up hook must never stop Python from starting
        _failed(e)


def _hook(event, args):
    """Python's audit hook: one comparison per event once the main program is known."""
    global _armed
    if _main_file is not None:
        if event == "exec":
            _saw_main_code(args)
        return
    if not _armed:
        return
    if event in INTERACTIVE_EVENTS:
        _armed = False
        return
    kind = PROGRAM_EVENTS.get(event)
    if kind is None:
        return
    _armed = False
    try:
        name = args[0] if args and kind in ("script", "module") else None
        name = os.fsdecode(name) if isinstance(name, bytes) else name
        if kind == "module" and name == LAUNCHER:
            return
        if is_job(kind, name):
            _attach((kind, name))
    except Exception as e:  # noqa: BLE001 - raising from this event would stop Python
        _failed(e)


def _attach(found):
    global program, _main_file
    program = found
    import invaract  # noqa: F401 - the package attaches as it is imported (invaract._attach.attach)
    # awaited only now: what attaching imported ran code of its own, some of it compiled from text like ``-c``'s
    kind, name = found
    if kind == "script":
        _main_file = os.path.realpath(name)
    elif kind == "module":
        _main_file = _ARGV  # where it is, runpy says by sys.argv[0] once it has found it
    elif kind == "command":
        _main_file = "<string>"
    elif kind == "stdin":
        _main_file = "<stdin>"


def _saw_main_code(args):
    """The first code Python runs whose file is the main program's is the main program: kept, so that how it ended can be
    told (``invaract._attach``)."""
    global _main_file
    code = args[0] if args else None
    filename = getattr(code, "co_filename", None)
    if not filename or getattr(code, "co_name", None) != "<module>":
        return
    expected = _main_file
    if expected is _ARGV:
        if not sys.argv or sys.argv[0] in ("-m", ""):
            return  # runpy is still finding the module: what runs now is its packages
        expected = os.path.realpath(sys.argv[0])
    if filename != expected and (filename.startswith("<") or os.path.basename(filename) != os.path.basename(expected)
                                 or os.path.realpath(filename) != expected):
        return  # the cheap comparisons first: this runs for every module the job imports until its program starts
    _main_file = None
    try:
        sys.modules["invaract._attach"].watch_main(code)
    except Exception:  # noqa: BLE001 - how the job ended is then told as on Python 3.11: nothing else depends on it
        pass


def _at_exit():
    attached = sys.modules.get("invaract._attach")
    if attached is not None:
        attached.at_exit()


def _failed(error):
    why = f"{type(error).__name__}: {error}"
    os.environ[FAILED] = why
    try:
        sys.stderr.write(f"Invaract could not attach to this process ({why}): it is not watched\n")
    except Exception:  # noqa: BLE001 - nowhere left to say it
        pass


# which processes are jobs

def is_job(kind, name, environ=None):
    """Whether a main program counts as a job: ``INVARACT_ATTACH_EXCLUDE`` first, then ``INVARACT_ATTACH_INCLUDE``, then the
    default: a script or a module (by its top-level package) that is the job's own code, and code given on the command line
    or standard input. An interactive session never does. (What the launcher starts always counts: ``invaract._attach``.)"""
    environ = os.environ if environ is None else environ
    if kind not in ("script", "module", "command", "stdin"):
        return False
    excluded, included = _patterns(environ, EXCLUDE), _patterns(environ, INCLUDE)
    if excluded or included:
        import fnmatch  # only now: it imports re, which most processes of the environment never need
        names = program_names(kind, name)

        def matched(patterns):
            return any(fnmatch.fnmatch(n, p) for n in names for p in patterns)

        if matched(excluded):
            return False
        if matched(included):
            return True
    if kind == "script":
        return name is not None and is_own(os.path.realpath(name))
    if kind == "module":
        location = _module_file(name)
        return location is not None and is_own(location)
    return True


def program_names(kind, name):
    """The names a pattern may match a main program by: a script's full path, its path as given and its file name; a
    module's dotted name; ``-c`` for code on the command line; ``-`` for standard input."""
    if kind == "script" and name:
        return [os.path.realpath(name), name, os.path.basename(name)]
    if kind == "module" and name:
        return [name]
    return {"command": ["-c"], "stdin": ["-"]}.get(kind, [])


def _patterns(environ, variable):
    return [p.strip() for p in environ.get(variable, "").split(",") if p.strip()]


def _module_file(name):
    """Where a module's top-level package is, found without importing it (importing it would run the job's code before
    the watch is on)."""
    import importlib.util
    try:
        spec = importlib.util.find_spec(name.split(".")[0])
    except (ImportError, ValueError):
        return None
    if spec is None:
        return None
    if spec.origin and spec.origin not in ("built-in", "frozen", "namespace"):
        return os.path.realpath(spec.origin)
    locations = list(spec.submodule_search_locations or [])
    return os.path.realpath(locations[0]) if locations else None


def is_own(path):
    """Whether a file is the job's own: not Python's, not an installed package's, and not a command installed beside the
    environment's packages or the user's (``pytest``, ``pip``: a library's entry point)."""
    return not any(inside(path, root) for root in library_roots())


def inside(path, root):
    """Whether ``path`` is ``root`` or under it, compared as the file system compares names."""
    path, root = os.path.normcase(path), os.path.normcase(root)
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


_roots = None


def library_roots():
    global _roots
    if _roots is None:
        import site
        import sysconfig
        keys = ("stdlib", "platstdlib", "purelib", "platlib", "scripts")
        roots = {sysconfig.get_paths().get(key) for key in keys}
        try:
            user = sysconfig.get_preferred_scheme("user")  # pip install --user: its packages and commands
            roots.update(sysconfig.get_paths(user).get(key) for key in ("purelib", "platlib", "scripts"))
        except (AttributeError, KeyError, ValueError):
            pass
        roots.update(site.getsitepackages() if hasattr(site, "getsitepackages") else [])
        roots.add(site.getusersitepackages() if hasattr(site, "getusersitepackages") else None)
        real = {os.path.normcase(os.path.realpath(root)) for root in roots if root}
        # a folder that holds everything (a file system's root) would make no program a job's: never one of these
        _roots = sorted(root for root in real if os.path.dirname(root) != root)
    return _roots
