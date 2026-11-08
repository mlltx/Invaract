# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The values a job writes, checked against its output's declared constraints (``runtimeDataQuality``).

With the setting on, the checker sends each output's checks with its reply to ``open``: the field, the contract's constraint
type (``equals``, ``oneOf``, ``range``, ``length``, ``fieldRange``) and its properties. Every batch the run stages for that
output is evaluated here as it goes by, and the ``written`` request carries, for each check in order, how many rows broke it
and a few of the values they held. The checker judges them (verification-core's ``RuntimeDataQuality``, whose meaning this
module follows):

- a null value never breaks a check (whether a column may hold nulls is its ``required``/``nullable`` declaration);
- numbers compare by value whatever their type (``1``, ``1.0``, ``Decimal("1")``), anything else by equality: the text ``"1"``
  is not the number ``1``, and ``True`` is not ``1``;
- a ``range`` or ``fieldRange`` value that is not a number (text, a boolean, NaN) breaks it;
- a ``length`` counts characters, not bytes, of the value's text;
- a ``fieldRange`` bound whose field is null, missing or not a number is no bound at all.
"""
import decimal

import pyarrow as pa
import pyarrow.compute as pc

#: How many of the values that broke a check are reported, at most.
MAX_EXAMPLES = 5

_BOUNDS = (("gte", pc.less), ("gt", pc.less_equal), ("lte", pc.greater), ("lt", pc.greater_equal))


class Evaluation:
    """The checks of one output, evaluated over every batch handed to ``observe``."""

    def __init__(self, checks):
        self._checks = list(checks)
        self._failed = [0] * len(self._checks)
        self._examples = [[] for _ in self._checks]

    def observe(self, batches):
        """``batches``, each evaluated as it goes by."""
        for batch in batches:
            self.evaluate(batch)
            yield batch

    def evaluate(self, batch):
        for i, check in enumerate(self._checks):
            column = _column(batch, check["field"])
            if column is None:
                continue
            mask = pc.fill_null(failing(check, column, batch), False)
            n = pc.sum(mask).as_py() or 0
            if n:
                self._failed[i] += n
                examples = self._examples[i]
                for value in pc.filter(column, mask).to_pylist():
                    if len(examples) >= MAX_EXAMPLES:
                        break
                    text = _text(value)
                    if text not in examples:
                        examples.append(text)

    def results(self):
        return [{"failed": failed, "examples": examples} for failed, examples in zip(self._failed, self._examples)]


def failing(check, column, batch):
    """A boolean array: which rows of ``column`` break ``check`` (null where the value is null)."""
    kind = check["type"]
    if kind in ("equals", "oneOf"):
        return _not_one_of(column, [check["value"]] if kind == "equals" else check["values"])
    if kind == "range":
        return _outside(column, {k: check[k] for k, _ in _BOUNDS if k in check})
    if kind == "length":
        return _wrong_length(column, check.get("exact"), check.get("min"), check.get("max"))
    if kind == "fieldRange":
        bounds = {}
        for key, _ in _BOUNDS:
            other = _column(batch, check[key]) if key in check else None
            if other is not None and _numeric(other.type):
                bounds[key] = other
        return _outside(column, bounds)
    raise ValueError(f"Invaract cannot check a constraint of type '{kind}'")


def _column(batch, name):
    index = batch.schema.get_field_index(name)
    return batch.column(index) if index >= 0 else None


def _numeric(t):
    return pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t)


def _is_text(t):
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _is_number(value):
    return isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool)


def _not_one_of(column, values):
    t = column.type
    if _numeric(t):
        allowed = [v for v in values if _is_number(v)]
    elif _is_text(t):
        allowed = [v for v in values if isinstance(v, str)]
    elif pa.types.is_boolean(t):
        allowed = [v for v in values if isinstance(v, bool)]
    else:
        return _per_row(column, lambda v: not any(_same(v, a) for a in values))
    kept = []
    for v in allowed:
        try:
            # a decimal column's value is made at its scale (Arrow casts an integer to a decimal only at full precision)
            scalar = pa.scalar(decimal.Decimal(str(v)), type=t) if pa.types.is_decimal(t) else pa.scalar(v).cast(t, safe=True)
            kept.append(scalar.as_py())
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OverflowError):
            pass  # a value the column's type cannot hold (1.5 in an integer column, 1.234 at scale 2) matches no row of it
    return pc.and_(pc.is_valid(column), pc.invert(pc.is_in(column, value_set=pa.array(kept, type=t))))


def _outside(column, bounds):
    """Rows of ``column`` outside ``bounds`` (``gte``/``gt``/``lte``/``lt`` to a number or another column); a value that is not a
    number is outside any range. pyarrow compares integers, floats and decimals with each other directly (from 15 on)."""
    if not _numeric(column.type):
        return pc.is_valid(column)
    mask = pc.is_nan(column) if pa.types.is_floating(column.type) else pa.array([False] * len(column))
    for key, broken_by in _BOUNDS:
        if key in bounds:
            bound = bounds[key] if isinstance(bounds[key], (pa.Array, pa.ChunkedArray)) else pa.scalar(bounds[key])
            mask = pc.or_(mask, pc.fill_null(broken_by(column, bound), False))
    return pc.and_(pc.is_valid(column), mask)


def _wrong_length(column, exact, least, most):
    if _is_text(column.type):
        text = column
    else:
        text = pa.array([None if v is None else _text(v) for v in column.to_pylist()], pa.string())
    n = pc.utf8_length(text)
    mask = pa.array([False] * len(column))
    if exact is not None:
        mask = pc.or_(mask, pc.not_equal(n, exact))
    if least is not None:
        mask = pc.or_(mask, pc.less(n, least))
    if most is not None:
        mask = pc.or_(mask, pc.greater(n, most))
    return mask


def _per_row(column, breaks):
    return pa.array([None if v is None else breaks(v) for v in column.to_pylist()], pa.bool_())


def _same(a, b):
    if _is_number(a) and _is_number(b):
        try:
            return decimal.Decimal(str(a)) == decimal.Decimal(str(b))
        except decimal.InvalidOperation:
            return False
    return a == b


def _text(value):
    """A value as the checker shows it: ``-3``, ``1.5``, ``EU``."""
    return str(value)
