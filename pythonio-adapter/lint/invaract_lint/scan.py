# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Scanning files and folders: which files a scan reads (``python_files``), and what it found in them (``Scan``), as the
command line prints it and as a run of the invaract package publishes it, in one shape (``Scan.to_dict``)."""
import fnmatch
import os
from dataclasses import dataclass, field
from typing import List, Tuple

from .checker import CATEGORIES, scan_file

#: Folders never walked into: tooling, caches and installed packages, not the team's job.
SKIPPED = {"__pycache__", "node_modules", "site-packages", "venv", "env", "build", "dist"}

CLEAN = "clean"
EXCEPTIONS = "exceptions"
FINDINGS = "findings"
NOT_SCANNED = "not-scanned"

#: What reading or parsing a file can raise: it is then a file the scan could not read, never a scan that stops. Source
#: that is not Python, or not text; a file that cannot be opened; code nested too deep to parse or to follow.
_UNREADABLE = (SyntaxError, ValueError, OSError, RecursionError, MemoryError)


@dataclass
class Scan:
    """What a scan found: the files it read, every finding in them (``Finding.allowed`` for those a ``# invaract: allow``
    marker keeps), and the files it could not read or parse, each with why.

    ``status`` sums it up: ``findings`` when a call is not explained by a marker; else ``not-scanned`` when a file could not
    be read, or none was, so nothing more can be said; else ``exceptions`` when a marker keeps a call, else ``clean``."""

    files: List[str] = field(default_factory=list)
    findings: list = field(default_factory=list)
    errors: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def unexplained(self):
        return [f for f in self.findings if not f.allowed]

    @property
    def exceptions(self):
        return [f for f in self.findings if f.allowed]

    @property
    def status(self):
        if self.unexplained:
            return FINDINGS
        if self.errors or not self.files:
            return NOT_SCANNED
        return EXCEPTIONS if self.exceptions else CLEAN

    def counts(self):
        """How many findings of each category, not explained and explained: ``{"read": {"findings": 2, "exceptions": 1}}``."""
        return {category: {"findings": sum(1 for f in self.unexplained if f.category == category),
                           "exceptions": sum(1 for f in self.exceptions if f.category == category)}
                for category in CATEGORIES.values()}

    def to_dict(self):
        """The scan as JSON-ready data: its status, the files read, counts by category, every finding (``Finding.to_dict``)
        and every file that could not be read."""
        return {
            "status": self.status,
            "files": list(self.files),
            "counts": self.counts(),
            "findings": [f.to_dict() for f in self.findings],
            "errors": [{"path": path, "message": message} for path, message in self.errors],
        }


def scan(paths, exclude=()):
    """Scans every ``.py`` file under ``paths`` (``python_files``)."""
    return scan_files(python_files(paths, exclude))


def scan_files(files):
    """Scans each file in ``files``; one that cannot be read or parsed is listed in ``Scan.errors``, and the rest are still
    scanned."""
    result = Scan()
    for path in files:
        try:
            result.findings.extend(scan_file(path))
        except _UNREADABLE as e:
            result.errors.append((path, str(e) or type(e).__name__))
            continue
        result.files.append(path)
    return result


def python_files(paths, exclude=()):
    """Every ``.py`` file under ``paths`` (files are taken as given), skipping hidden folders, ``SKIPPED`` and ``exclude``
    patterns, in a stable order."""
    exclude = [(p, os.path.normpath(p)) for p in exclude]
    for path in paths:
        if os.path.isfile(path):
            if not _excluded(path, exclude):
                yield path
            continue
        for root, folders, files in os.walk(path):
            folders[:] = sorted(f for f in folders if _walked(root, f, exclude))
            for name in sorted(files):
                full = os.path.join(root, name)
                if name.endswith(".py") and not _excluded(full, exclude):
                    yield full


def _walked(root, folder, exclude):
    return not folder.startswith(".") and folder not in SKIPPED and not _excluded(os.path.join(root, folder), exclude)


def _excluded(path, patterns):
    """Whether ``path`` matches one of ``patterns`` (each given as written and normalized), by its path or by its name."""
    normal = os.path.normpath(path)
    name = os.path.basename(normal)
    return any(fnmatch.fnmatch(normal, full) or fnmatch.fnmatch(name, given) for given, full in patterns)
