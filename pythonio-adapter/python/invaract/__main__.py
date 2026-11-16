# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The launcher: ``python -m invaract job.py [args]``, ``python -m invaract -m package.job [args]``, or
``python -m invaract -c "code" [args]``.

It runs the job as Python would, with Invaract attached before the job's first line and an exact account of how the job
ended, so a platform that controls the start command needs no start-up hook (docs/PYTHONIO_ATTACH_DESIGN.md, section 7).
"""
import builtins
import importlib.machinery
import io
import os
import runpy
import sys
import types

from . import _attach

USAGE = "usage: python -m invaract (script.py | -m module | -c code) [args...]"
#: This module, kept while the job's program takes its place as ``__main__``.
_launcher = sys.modules.get(__name__)


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        sys.stderr.write(USAGE + "\n")
        return 0 if argv else 2
    if argv[0] in ("-m", "-c"):
        if len(argv) < 2:
            sys.stderr.write(USAGE + "\n")
            return 2
        kind, target, rest = ("module" if argv[0] == "-m" else "command"), argv[1], argv[2:]
    else:
        kind, target, rest = "script", argv[0], argv[1:]
        if not os.path.exists(target):
            # as Python says it
            sys.stderr.write(f"{sys.executable}: can't open file {os.path.abspath(target)!r}: [Errno 2] No such file or directory\n")
            return 2
    sys.argv = [target if kind == "script" else argv[0]] + rest
    if kind != "script":
        sys.path[0] = os.getcwd()
    elif _is_source(target):
        sys.path[0] = os.path.dirname(os.path.realpath(target))  # the script's folder, its links followed, as Python does
    else:
        sys.path[0] = os.path.abspath(target)  # a folder or zip file holding __main__.py is itself on the path
    _attach.launched(kind, target if kind != "command" else None)
    try:
        _run(kind, target)
    except SystemExit as e:
        status = _status(e.code)
        _attach.launched_ended(status == 0, status)
        raise
    except BaseException:
        _attach.launched_ended(False, 1)
        raise
    _attach.launched_ended(True, 0)
    return 0


def _run(kind, target):
    """Runs the program as Python would, in a module of its own that stays ``__main__`` after it returns (its exit
    handlers, ``pickle`` and the job's name all find it there)."""
    program = types.ModuleType("__main__")
    program.__builtins__ = builtins
    sys.modules["__main__"] = program
    if kind == "module":
        runpy._run_module_as_main(target)  # what python -m runs: in __main__, and says "No module named" as it does
    elif kind == "command":
        exec(compile(target, "<string>", "exec"), program.__dict__)  # noqa: S102 - python -c
    elif _is_source(target):
        program.__file__, program.__cached__ = target, None
        program.__loader__ = importlib.machinery.SourceFileLoader("__main__", target)
        with io.open_code(target) as source:
            code = compile(source.read(), target, "exec", dont_inherit=True)
        exec(code, program.__dict__)  # noqa: S102 - python script.py
    else:
        runpy._run_module_as_main("__main__")  # a folder or a zip file holding __main__.py, found on sys.path[0]


def _is_source(path):
    """A script file, rather than a folder or a zip file holding ``__main__.py``."""
    import zipfile
    return os.path.isfile(path) and not zipfile.is_zipfile(path)


def _status(code):
    if code is None or code is False:
        return 0
    if isinstance(code, int):
        return code
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
