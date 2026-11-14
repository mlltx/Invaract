# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Whose doing something is, and where in the job's code it was done.

Something seen as a job runs happens deep inside a library. ``place`` walks out from there: the first frame that is the
invaract package's own makes it the package's doing (it read or wrote on the job's behalf), and the first that is the
job's own places it at that line. ``position`` then says where that line's call starts and ends, the function it is in,
and its text, as the scan would say them.

The job's own code is what the scan takes it to be (``_codescan``): not Python's, not an installed package's.
"""
import itertools
import linecache
import os
import sys

from .. import _codescan
from . import _state
from ._state import Seen

INVARACT, JOB, LIBRARY, UNPLACED = "invaract", "job", "library", "unplaced"

#: This package's own folder: its frames are how a thing was seen, never who did it.
_HERE = os.path.dirname(os.path.abspath(__file__))
_INVARACT = os.path.dirname(_HERE)

_whose_file = {}
_folders = {}
_library_roots = None


def whose(filename):
    """Whose code a file is: the package's own (and invaract-lint's), the job's, a library's (Python's and every installed
    package's), or not placed (code compiled from text, which has no file)."""
    found = _whose_file.get(filename)
    if found is None:
        if filename.startswith("<"):
            found = LIBRARY if filename.startswith(("<frozen", "<built-in", "<invaract")) else UNPLACED
        elif not os.path.isfile(filename):
            found = UNPLACED  # code compiled under a name that is no file
        else:
            real = os.path.realpath(filename)
            ours = [_INVARACT] + ([os.path.dirname(os.path.abspath(_state.lint.__file__))] if getattr(_state.lint, "__file__", None) else [])
            if any(_codescan._inside(real, os.path.realpath(root)) for root in ours):
                found = INVARACT
            else:
                found = LIBRARY if is_a_librarys(real) else JOB
        _whose_file[filename] = found
    return found


def is_a_librarys(path):
    """Whether a path is inside Python's own folders or an installed package's: code, and files, that are not the job's."""
    global _library_roots
    if _library_roots is None:
        _library_roots = _codescan._not_the_jobs()
    return any(_codescan._inside(path, root) for root in _library_roots)


def compiled_module(name, filename):
    """A compiled module loaded from ``filename``, as something seen, when it is the job's own: Python's and installed
    packages' are how libraries work, and each that reads or writes is watched by name."""
    if is_a_librarys(os.path.realpath(filename)):
        return None
    return Seen(_state.lint.DYNAMIC, "native-code", "import",
                f"loaded the job's own compiled module {name} ({shown(filename)}), and what compiled code reads or writes is not watched", filename)


def place(frame, direct=None):
    """``(whose, frame)`` for something that happened under ``frame``: ``INVARACT`` when the package did it, ``JOB`` and
    the job's frame that led to it, or ``LIBRARY`` and no frame when none of the job's code is on the stack. Code with no
    file (``exec``) is placed at the job's frame that ran it, or, with none, where it is.

    ``direct`` names a package (``ctypes``): the thing then counts as the job's only when the job's own code called that
    package itself, so the frame just outside the package decides, and ``LIBRARY`` means a library did it for its own ends."""
    within = _folder_of(direct) if direct else None
    unplaced = None
    while frame is not None:
        filename = frame.f_code.co_filename
        if os.path.dirname(filename) != _HERE and not filename.startswith("<invaract"):
            found = whose(filename)
            if within is not None and not (found == LIBRARY and _codescan._inside(os.path.realpath(filename), within)):
                return (JOB, frame) if found in (JOB, UNPLACED) else (found, None)
            if found == INVARACT:
                return INVARACT, None
            if found == JOB:
                return JOB, frame
            if found == UNPLACED and unplaced is None:
                unplaced = frame
        frame = frame.f_back
    return (JOB, unplaced) if unplaced is not None else (LIBRARY, None)


#: Python's own modules that start a program only to ask the system about itself, each with the programs it asks: no data
#: moves, and a job starts them by importing a library (``platform.uname`` runs ``uname``). Any other program started
#: from one of them is reported like any other.
PROBES = {
    "platform": ("uname", "file", "ver"),
    "uuid": ("ifconfig", "ip", "arp", "netstat", "lanscan"),
    "ctypes.util": ("ldconfig", "gcc", "cc", "ld", "objdump", "ldd", "dump", "crle"),
}
#: The modules a program is started through, which are how it was started and not who started it.
_STARTERS = ("subprocess", "os")


def is_a_probe(frame, program):
    """Whether a program being started under ``frame`` is Python asking the system about itself: one of ``PROBES``' programs,
    started by the module that asks it."""
    name = os.path.basename(program or "")
    name = name[:-4] if name.lower().endswith(".exe") else name
    through = {_file_of(module) for module in _STARTERS}
    while frame is not None:
        filename = frame.f_code.co_filename
        if os.path.dirname(filename) != _HERE and filename not in through:
            return any(name in programs and filename == _file_of(module) for module, programs in PROBES.items())
        frame = frame.f_back
    return False


def _file_of(module):
    return getattr(sys.modules.get(module), "__file__", None)


def _folder_of(package):
    """The folder a loaded package's code is in; a made-up one nothing is inside while it is not loaded."""
    if package not in _folders:
        location = getattr(sys.modules.get(package), "__file__", None)
        if not location:
            return os.sep + package
        _folders[package] = os.path.realpath(os.path.dirname(location))
    return _folders[package]


def position(frame):
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
    lines = [linecache.getline(code.co_filename, n) for n in range(line, end_line + 1)] if line else []
    if lines and column:
        lines[-1] = lines[-1][:end_column - 1]
        lines[0] = lines[0][column - 1:]
    return shown(code.co_filename), line, column, end_line, end_column, _scope(code, frame.f_lineno or 0), " ".join(" ".join(lines).split())


def shown(filename):
    """A file as findings name it: relative to the working folder when it is inside it."""
    return filename if filename.startswith("<") else _codescan._shown(filename)


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
            node, within = pending.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                within = within + (node.name,)
                scopes.append((node.lineno, node.end_lineno, ".".join(within)))
            if isinstance(node, ast.stmt):
                statements.append((node.lineno, node.end_lineno))
            pending.extend((child, within) for child in ast.iter_child_nodes(node))
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
