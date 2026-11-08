# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The package's public interface as a type checker and an editor see it (``invaract/__init__.py``, ``py.typed``)."""
import typing

import invaract
from invaract import _frames, _pandas, _run


def test_the_public_interface_is_what_all_names():
    assert sorted(invaract.__all__) == sorted(
        ["job", "run", "read", "write", "check", "reads", "writes", "Run", "ContractViolation", "CheckNotRun"])
    for name in invaract.__all__:
        assert getattr(invaract, name).__doc__, name


def test_the_type_hints_name_exactly_the_values_the_package_takes():
    def values(fn, parameter):
        hint = typing.get_type_hints(fn)[parameter]
        literals = [a for a in typing.get_args(hint) if typing.get_origin(a) is typing.Literal] or [hint]
        return {v for literal in literals for v in typing.get_args(literal)}

    assert values(invaract.read, "as_") == set(_frames.FRAME_KINDS)
    assert values(invaract.read, "dtype_backend") == set(_pandas.DTYPE_BACKENDS)
    assert values(invaract.write, "save_mode") == set(_run.SAVE_MODES)
    assert values(invaract.write, "partition_overwrite") == {"static", "dynamic"}
    assert values(invaract.reads, "as_") == set(_frames.FRAME_KINDS)
