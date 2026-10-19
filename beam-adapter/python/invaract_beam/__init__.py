# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract for Apache Beam pipelines written in Python: ``--runner=invaract_beam.InvaractRunner``.

See ``runner.py`` for what the runner does, and docs/BEAM_PYTHON_ADAPTER.md for the design.
"""
from .options import InvaractOptions
from .runner import CheckNotRun, ContractViolation, InvaractRunner

__version__ = "0.1.0"

__all__ = ["InvaractRunner", "InvaractOptions", "ContractViolation", "CheckNotRun"]
