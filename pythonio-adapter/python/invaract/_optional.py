# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The libraries an extra installs (``pip install "invaract[avro]"``), imported only when a job reads or writes what needs one."""
import importlib

from .errors import CheckNotRun


def require(module, extra, what, package=None):
    """``module``, imported; ``CheckNotRun`` saying which extra installs it when it is not installed. ``what`` is what the job
    reads or writes with it (``avro``, ``delta tables``); ``package`` is the name it is installed by, where that is not
    ``module``'s."""
    try:
        return importlib.import_module(module)
    except ImportError as e:
        raise CheckNotRun(f'Invaract needs the {package or module} package to read or write {what}: '
                          f'pip install "invaract[{extra}]"') from e
