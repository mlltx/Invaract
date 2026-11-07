# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The Arrow type a column the contract declares is read or written as: ``long`` is ``int64``, ``decimal(10,2)`` is
``decimal128(10, 2)``, ``array<struct<sku:string>>`` a list of structs.

The checker maps the other way, Arrow's names to the contract's (verification-core's ``ArrowSchemas``); this module is what
the package needs where it builds data rather than describes it: a CSV, JSON, Excel or database column typed as declared,
and a pandas column converted to the declared type (``_pandas``).
"""
import re

import pyarrow as pa

_SCALARS = {
    "string": pa.string(), "boolean": pa.bool_(), "tinyint": pa.int8(), "byte": pa.int8(), "smallint": pa.int16(),
    "short": pa.int16(), "int": pa.int32(), "integer": pa.int32(), "bigint": pa.int64(), "long": pa.int64(),
    "float": pa.float32(), "real": pa.float32(), "double": pa.float64(), "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"), "timestamp_ltz": pa.timestamp("us", tz="UTC"), "timestamp_ntz": pa.timestamp("us"),
    "binary": pa.binary(),
}
_DECIMAL = re.compile(r"^(?:decimal|dec|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_KEYWORD = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*")
#: A struct member's name, `quoted` when it holds anything but letters, digits and _, then ':' or a space.
_MEMBER = re.compile(r"\s*(?:`([^`]+)`|([A-Za-z0-9_]+))\s*:?\s*")
_PARAMETERS = re.compile(r"\s*\([^)]*\)")


def scalar(contract_type):
    """The Arrow type of a scalar contract type (``long``, ``decimal(10,2)``); ``None`` for any other (a nested type, which a
    CSV file cannot hold, or a type with no one Arrow form, such as ``interval``)."""
    t = contract_type.strip().lower()
    if t in _SCALARS:
        return _SCALARS[t]
    m = _DECIMAL.match(t)
    return pa.decimal128(int(m.group(1)), int(m.group(2))) if m else None


def of(contract_type):
    """The Arrow type of any contract type: a scalar's (``scalar``), and ``array<T>``, ``map<K,V>`` and
    ``struct<name:T,...>`` built of those; ``None`` for a type with no one Arrow form."""
    try:
        arrow, rest = _parse(contract_type.strip())
    except ValueError:
        return None
    return arrow if not rest.strip() else None


def scalars(fields):
    """Each declared field's scalar Arrow type, by name (``None`` for one that is not scalar)."""
    return {f["name"]: scalar(f["type"]) for f in fields}


def _parse(text):
    """``(arrow type, the text after it)`` for the contract type at the start of ``text``; ``ValueError`` if there is none."""
    match = _KEYWORD.match(text)
    if not match:
        raise ValueError(text)
    keyword, rest = match.group(1).lower(), text[match.end():]
    if keyword == "array":
        element, rest = _parse(_expect(rest, "<"))
        return pa.list_(element), _expect(rest, ">")
    if keyword == "map":
        key, rest = _parse(_expect(rest, "<"))
        value, rest = _parse(_expect(rest, ","))
        return pa.map_(key, value), _expect(rest, ">")
    if keyword == "struct":
        rest, members = _expect(rest, "<"), []
        while True:
            name = _MEMBER.match(rest)
            if not name:
                raise ValueError(text)
            member, rest = _parse(rest[name.end():])
            members.append(pa.field(name.group(1) or name.group(2), member))
            rest = rest.lstrip()
            if not rest.startswith(","):
                return pa.struct(members), _expect(rest, ">")
            rest = rest[1:]
    params = _PARAMETERS.match(rest)
    arrow = scalar(keyword + (params.group(0).strip() if params else ""))
    if arrow is None:
        raise ValueError(text)
    return arrow, rest[params.end():] if params else rest


def _expect(text, token):
    text = text.lstrip()
    if not text.startswith(token):
        raise ValueError(text)
    return text[len(token):]
