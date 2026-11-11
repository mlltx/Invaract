# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""``invaract-lint [paths...]``: reports every call that reads or writes data outside invaract, one line each.

Exits 0 when there are none, 1 when there are, and 2 when a file could not be read or parsed (the others are still checked).
``--format github`` prints each finding as a GitHub Actions annotation, so it shows on the pull request's diff; ``--format
json`` prints the whole scan as one JSON object (``Scan.to_dict``), the calls a marker keeps included, for a report to read.
"""
import argparse
import json
import sys

from . import __version__
from .scan import python_files, scan  # noqa: F401  (python_files: the command line's own, kept importable here)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="invaract-lint",
                                     description="Reports reads and writes made outside the invaract package.")
    parser.add_argument("paths", nargs="*", default=["."],
                        help="files or folders to check (default: the current folder)")
    parser.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                        help="skip matching files or folders (repeatable)")
    parser.add_argument("--format", choices=("text", "github", "json"), default="text",
                        help="text (default), GitHub Actions annotations, or one JSON object with every finding")
    args = parser.parse_args(argv)

    result = scan(args.paths, args.exclude)
    for path, message in result.errors:
        print(f"{path}: cannot be checked: {message}", file=sys.stderr)
    found = result.unexplained
    if args.format == "json":
        print(json.dumps({"scanner": {"name": "invaract-lint", "version": __version__}, **result.to_dict()}, indent=2))
    else:
        for f in found:
            if args.format == "github":
                print(f"::error file={f.path},line={f.line},col={f.col},endLine={f.end_line},endColumn={f.end_col},"
                      f"title={f.code}::{f.message}")
            else:
                print(f)
    if found:
        print(f"{len(found)} read(s) or write(s) outside invaract", file=sys.stderr)
    return 2 if result.errors else (1 if found else 0)


if __name__ == "__main__":
    sys.exit(main())
