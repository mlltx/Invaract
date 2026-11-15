# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What strict mode's parts share: who is watching now, and what one thing seen is."""
import contextlib
import threading
import weakref
from dataclasses import dataclass
from typing import Optional, Tuple

#: The watcher that what is seen now goes to: a run's, a child process's, or the one that keeps what a job does before its
#: run opens. ``None``: nothing is watched, and every hook and wrapper does nothing.
watcher = None
#: invaract-lint, whose catalogue names what to watch; set when the hooks are installed.
lint = None
#: Streams a watched call was seen to open: handed to a writer later, they are not reported a second time.
streams = weakref.WeakSet()

_thread = threading.local()


@dataclass(frozen=True)
class Seen:
    """Something the job did, as it was seen: the catalogue's code and rule for it, the function that did it, what it did
    in words, what it reached (a path, ``host:port``, a program), and the local paths it touched (all scratch: no finding).
    ``direct`` names a package (``ctypes``) whose use counts only when the job's own code called that package itself: a
    library that uses it for its own ends is not the job's doing."""

    code: str
    rule: str
    call: str
    did: str
    target: Optional[str] = None
    paths: Tuple[str, ...] = ()
    direct: Optional[str] = None


def quiet():
    """Whether this thread is to be left alone: it is working out a finding (what that itself opens is not the job's), or
    it is inside a watched call that has been accounted for (what that opens on the way is the same read or write)."""
    return getattr(_thread, "working", False) or getattr(_thread, "inside", False)


@contextlib.contextmanager
def working():
    """While a finding is worked out on this thread."""
    before, _thread.working = getattr(_thread, "working", False), True
    try:
        yield
    finally:
        _thread.working = before


@contextlib.contextmanager
def inside():
    """While a watched call that has been accounted for runs on this thread."""
    before, _thread.inside = getattr(_thread, "inside", False), True
    try:
        yield
    finally:
        _thread.inside = before


def reset_thread():
    """A forked child starts with a clean slate, whatever its parent's thread was doing."""
    _thread.working = _thread.inside = False
