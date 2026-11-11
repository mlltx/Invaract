# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""invaract-lint: finds the reads and writes a Python job makes outside the invaract package (docs/PYTHON_IO_ADAPTER_DESIGN.md,
section 7). ``invaract-lint [paths...]`` on the command line; from Python, ``check_source`` and ``check_file`` for the calls
not marked ``# invaract: allow``, ``scan_source`` and ``scan_file`` for every call, and ``scan`` for files and folders."""
__version__ = "0.1.0"

from .checker import CATEGORIES, CONNECT, READ, SQL, WRITE, Finding, check_file, check_source, scan_file, scan_source  # noqa: E402
from .scan import CLEAN, EXCEPTIONS, FINDINGS, NOT_SCANNED, Scan, python_files, scan, scan_files  # noqa: E402

__all__ = ["check_source", "check_file", "scan_source", "scan_file", "scan", "scan_files", "python_files", "Scan", "Finding", "READ", "WRITE",
           "CONNECT", "SQL", "CATEGORIES", "CLEAN", "EXCEPTIONS", "FINDINGS", "NOT_SCANNED"]
