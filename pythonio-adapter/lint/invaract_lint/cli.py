# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""``invaract-lint [paths...]``: reports every call that reads or writes data outside invaract, one line each.

Exits 0 when there are none, 1 when there are, and 2 when a file could not be read or parsed (the others are still checked).
``--format github`` prints each finding as a GitHub Actions annotation, so it shows on the pull request's diff.
"""
import argparse
import fnmatch
import os
import sys

from .checker import check_file

#: Folders never walked into: tooling, caches and installed packages, not the team's job.
SKIPPED = {"__pycache__", "node_modules", "site-packages", "venv", "env", "build", "dist"}


def python_files(paths, exclude=()):
    """Every ``.py`` file under ``paths`` (files are taken as given), skipping hidden folders, ``SKIPPED`` and ``exclude``
    patterns, in a stable order."""
    for path in paths:
        if os.path.isfile(path):
            if not _excluded(path, exclude):
                yield path
            continue
        for root, folders, files in os.walk(path):
            folders[:] = sorted(f for f in folders if not f.startswith(".") and f not in SKIPPED and not _excluded(os.path.join(root, f), exclude))
            for name in sorted(files):
                full = os.path.join(root, name)
                if name.endswith(".py") and not _excluded(full, exclude):
                    yield full


def _excluded(path, patterns):
    normal = os.path.normpath(path)
    return any(fnmatch.fnmatch(normal, os.path.normpath(p)) or fnmatch.fnmatch(os.path.basename(normal), p) for p in patterns)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="invaract-lint", description="Reports reads and writes made outside the invaract package.")
    parser.add_argument("paths", nargs="*", default=["."], help="files or folders to check (default: the current folder)")
    parser.add_argument("--exclude", action="append", default=[], metavar="GLOB", help="skip matching files or folders (repeatable)")
    parser.add_argument("--format", choices=("text", "github"), default="text", help="text (default), or GitHub Actions annotations")
    args = parser.parse_args(argv)

    found, broken = 0, 0
    for path in python_files(args.paths, args.exclude):
        try:
            findings = check_file(path)
        except (SyntaxError, UnicodeDecodeError, OSError) as e:
            print(f"{path}: cannot be checked: {e}", file=sys.stderr)
            broken += 1
            continue
        for f in findings:
            if args.format == "github":
                print(f"::error file={f.path},line={f.line},col={f.col},title={f.code}::{f.message}")
            else:
                print(f)
        found += len(findings)
    if found:
        print(f"{found} read(s) or write(s) outside invaract", file=sys.stderr)
    return 2 if broken else (1 if found else 0)


if __name__ == "__main__":
    sys.exit(main())
