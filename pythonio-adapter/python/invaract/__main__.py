# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The launcher: ``python -m invaract job.py [args]``, ``python -m invaract -m package.job [args]``, or
``python -m invaract -c "code" [args]``.

It runs the job as Python would, with Invaract attached before the job's first line and an exact account of how the job
ended, so a platform that controls the start command needs no start-up hook (docs/PYTHONIO_ATTACH_DESIGN.md, section 7).
"""
import os
import runpy
import sys

from . import _attach

USAGE = "usage: python -m invaract (script.py | -m module | -c code) [args...]"


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
    sys.argv = [target if kind == "script" else argv[0]] + rest
    if kind == "script":
        sys.path[0] = os.path.dirname(os.path.abspath(target))
    else:
        sys.path[0] = os.getcwd()
    _attach.launched(kind, target if kind != "command" else None)
    try:
        if kind == "script":
            runpy.run_path(target, run_name="__main__")
        elif kind == "module":
            runpy.run_module(target, run_name="__main__", alter_sys=True)
        else:
            exec(compile(target, "<string>", "exec"), {"__name__": "__main__", "__builtins__": __builtins__})  # noqa: S102
    except SystemExit as e:
        status = _status(e.code)
        _attach.launched_ended(status == 0, status)
        raise
    except BaseException:
        _attach.launched_ended(False, 1)
        raise
    _attach.launched_ended(True, 0)
    return 0


def _status(code):
    if code is None or code is False:
        return 0
    if isinstance(code, int):
        return code
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
