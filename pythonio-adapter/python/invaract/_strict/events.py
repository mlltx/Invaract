# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What Python itself reports as a job runs (its audit events), read as what the job did.

Python raises an event for every file opened, moved, linked, deleted or truncated, every SQLite database opened, every
socket connected or sent to, every program started, and every native library loaded through ``ctypes``. That is everything
written through Python's own ``open``, whatever library asked for it. ``EVENTS`` names each event watched and the function
that reads it; ``describe`` gives what one event was, or ``None`` for one that is no write, connection or program (a file
opened to read, a descriptor).

To watch another event: add it to ``EVENTS``, and a case to ``tests/test_strict_unit.py``.
"""
import os

from . import _state
from ._state import Seen

#: ``os.open`` flags that write.
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
#: Mode letters that write.
_WRITE_MODE = "wax+"


def describe(event, args):
    """What the audit event ``event`` was, as a ``Seen``; ``None`` for one that is not watched or is nothing to report."""
    read = EVENTS.get(event)
    return read(event, args) if read is not None else None


def local_path(path, folder=None):
    """``path`` as text, or ``None`` for what is no path (a file descriptor). A path given relative to an open folder
    (``dir_fd``, which is how ``shutil.rmtree`` deletes) is placed in that folder."""
    if not isinstance(path, (str, bytes, os.PathLike)):
        return None
    path = os.fsdecode(path)
    if isinstance(folder, int) and folder >= 0 and not os.path.isabs(path):
        inside = folder_of(folder)
        if inside is not None:
            return os.path.join(inside, path)
    return path


def folder_of(descriptor):
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


def in_memory(database):
    """Whether a SQLite or DuckDB database is held in memory, by its name: unnamed, ``:memory:``, a ``file:`` URI that says
    so, or DuckDB's default connection (``:default:``, which is in memory)."""
    return (database == "" or database.startswith((":memory:", "file::memory:")) or "mode=memory" in database
            or database == ":default:")


def _opened(event, args):
    path, mode, flags = (tuple(args) + (None, None, None))[:3]
    path = local_path(path)
    if path is None:
        return None
    writes = any(letter in mode for letter in _WRITE_MODE) if isinstance(mode, str) else isinstance(flags, int) and bool(flags & _WRITE_FLAGS)
    if not writes:
        return None
    how = f"mode {mode!r}" if isinstance(mode, str) else f"flags {flags}"
    return Seen(_state.lint.WRITE, "open-write-mode", "open", f"opened {path} to write ({how})", path, (path,))


#: Each rename, link, delete and truncation Python reports: what it did in words, and where among the event's arguments
#: each path's open folder (``dir_fd``) is, if it may have one.
CHANGES = {
    "os.rename": ("moved {0} to {1}", (2, 3)), "os.link": ("linked {1} to {0}", (2, 3)), "os.symlink": ("linked {1} to {0}", (None, 2)),
    "os.remove": ("deleted {0}", (1,)), "os.rmdir": ("deleted the folder {0}", (1,)), "os.truncate": ("truncated {0}", (None,)),
}


def _changed(event, args):
    """A rename, link, delete or truncation: what it reaches is its destination, or, with one path, that path."""
    did, folders = CHANGES[event]
    paths = [local_path(args[i], args[at] if at is not None and len(args) > at else None) for i, at in enumerate(folders) if len(args) > i]
    if None in paths or len(paths) < len(folders):
        return None
    return Seen(_state.lint.WRITE, "file-operation", event, did.format(*paths), paths[-1], tuple(paths))


def _database(event, args):
    database = local_path(args[0]) if args else None
    if database is None or in_memory(database):
        return None
    return Seen(_state.lint.CONNECT, "database-connect", "sqlite3.connect", f"opened the SQLite database {database}", database, (database,))


def _connected(event, args):
    """A connection made, or data sent with none (``sendto``): its address is what it reached."""
    address = args[1] if len(args) > 1 else None
    if isinstance(address, tuple) and len(address) >= 2:
        target = f"{address[0]}:{address[1]}"
    elif isinstance(address, (str, bytes)) and address:
        target = os.fsdecode(address)
    else:
        return None
    did = "connected to" if event == "socket.connect" else "sent data to"
    return Seen(_state.lint.CONNECT, "network-connect", event, f"{did} {target}", target)


def _started(event, args):
    """Another program started. Only its name is kept: its arguments may hold anything."""
    first = args[1] if event == "os.spawn" else args[0] if args else None
    if first is None and len(args) > 1:
        first = args[1]  # no program named apart: its command line names it
    return Seen(_state.lint.PROCESS, "subprocess", event, f"started {program(first)}", program(first))


def program(command):
    """The name of the program a command starts (its first word, or first item), never its arguments."""
    if isinstance(command, (list, tuple)):
        command = command[0] if command else None
    text = os.fsdecode(command).strip() if isinstance(command, (str, bytes, os.PathLike)) else ""
    return text.split()[0] if text else "a program"


def _native(event, args):
    """A native library loaded, or a function looked up in one, through ``ctypes``: what such code then reads or writes,
    nothing sees. It counts when the job's own code did it; libraries use ``ctypes`` for their own ends."""
    name = args[-1] if args else None
    name = os.fsdecode(name) if isinstance(name, (str, bytes)) else "this process" if name is None else str(name)
    did = "loaded the native library" if event == "ctypes.dlopen" else "looked up the native function"
    return Seen(_state.lint.DYNAMIC, "native-code", event, f"{did} {name}", name, direct="ctypes")


def _imported(event, args):
    """A compiled module loaded: only one that is the job's own is anything to report (``placing.compiled_module``)."""
    name, filename = (tuple(args) + (None, None))[:2]
    if not isinstance(filename, str):
        return None  # a module of Python source, which is the job's code like any other
    from . import placing
    return placing.compiled_module(name, filename)


EVENTS = {
    "open": _opened,
    "os.rename": _changed, "os.link": _changed, "os.symlink": _changed, "os.remove": _changed, "os.rmdir": _changed,
    "os.truncate": _changed,
    "sqlite3.connect": _database,
    "socket.connect": _connected, "socket.sendto": _connected, "socket.sendmsg": _connected,
    "subprocess.Popen": _started, "os.system": _started, "os.exec": _started, "os.posix_spawn": _started, "os.spawn": _started,
    "os.startfile": _started, "_winapi.CreateProcess": _started,
    "ctypes.dlopen": _native, "ctypes.dlsym": _native,
    "import": _imported,
}
