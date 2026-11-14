# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What a finding is, what keeps one (``# invaract: allow``), and what is never copied into one (a password).

The scan of a file (``checker``) and the watch of a running job (the invaract package's strict mode) both make ``Finding``s,
so a report reads the same whichever found it.
"""
import hashlib
import io
import re
import tokenize
from dataclasses import dataclass
from typing import Optional

from . import catalogue
from .catalogue import CATEGORIES

#: The marker that keeps a finding, and the reason written after it.
ALLOW = re.compile(r"#\s*invaract:\s*allow\b(?P<reason>.*)")

#: The longest ``Finding.snippet`` and ``Finding.target``; longer text is cut, with an ellipsis.
_LONGEST = 200
#: A password in a URL (``postgresql://user:secret@host``): never copied into a finding, which may be published.
_PASSWORD = re.compile(r"(\w[\w+.-]*://[^/\s:@'\"]+):[^@\s/'\"]+@")
#: The names a secret is given by, and a piece of text as Python source writes it.
_NAMED = r"\b((?:password|passwd|pwd|secret|token|api_?key|access_?key|secret_?key|sas_?token|account_?key)\s*=\s*)"
_TEXT = r"(?:'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")"
#: A secret given by name in a connection string (``password=secret``, ``PWD=secret;``): its value is never copied either.
_SECRET = re.compile(_NAMED + r"(?:'[^']*'|\"[^\"]*\"|[^\s;,&)'\"]+)", re.IGNORECASE)
#: In Python source, a secret given as a keyword argument's text (``password='secret'``).
_SECRET_ARGUMENT = re.compile(_NAMED + _TEXT, re.IGNORECASE)
_TEXT_IN_CODE = re.compile(_TEXT)


@dataclass(frozen=True)
class Finding:
    """A call that reads or writes data outside invaract.

    ``path``, ``line`` and ``col`` (1-based) are where the call starts, ``end_line`` and ``end_col`` where it ends. ``code``
    is ``IVR001`` to ``IVR004`` (``category`` says it in a word), and ``rule`` names the rule that matched (``pandas-read``,
    ``open-write-mode``, ...). ``call`` is the called expression as written, ``resolved`` the name it stands for through the
    file's imports (``pandas.read_csv`` for ``pd.read_csv``; ``None`` for a method of a value, such as ``df.to_parquet``), and
    ``scope`` the function or class it is in (``Loader.load``; ``<module>`` at the top level). ``end_col`` is the column just
    after the call's last character. ``snippet`` is the whole call on one line, as Python reads it (so reformatting the source
    does not change it), and ``target`` the path or URL it names when the source spells it out; ``detail`` says what
    made it count when the name alone does not (the mode of an ``open``, the SQL keyword). ``allowed`` is true when a
    ``# invaract: allow`` marker on one of its lines keeps it, and ``reason`` is what the marker says after it. A password
    in a URL is never copied into ``snippet`` or ``target``.
    """

    path: str
    line: int
    col: int
    code: str
    call: str
    end_line: int = 0
    end_col: int = 0
    resolved: Optional[str] = None
    scope: str = "<module>"
    rule: str = ""
    snippet: str = ""
    target: Optional[str] = None
    detail: Optional[str] = None
    allowed: bool = False
    reason: Optional[str] = None

    @property
    def category(self):
        return CATEGORIES[self.code]

    @property
    def message(self):
        return f"`{self.call}` {catalogue.WHAT[self.code]}, or mark the line `# invaract: allow`"

    @property
    def id(self):
        """A short identifier that stays the same while the call does: from the file, the rule, the function it is in and
        the call as written, not its line, so a finding keeps its identifier when lines above it are added or removed. Two
        identical calls in the same function share it."""
        key = "\0".join((self.path, self.code, self.rule, self.scope, self.snippet))
        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

    def to_dict(self):
        """The finding as JSON-ready data, by the names reports and events use."""
        return {
            "id": self.id, "code": self.code, "category": self.category, "rule": self.rule, "path": self.path,
            "line": self.line, "column": self.col, "endLine": self.end_line, "endColumn": self.end_col, "call": self.call,
            "resolved": self.resolved, "scope": self.scope, "snippet": self.snippet, "target": self.target,
            "detail": self.detail, "allowed": self.allowed, "reason": self.reason, "message": self.message,
        }

    def __str__(self):
        return f"{self.path}:{self.line}:{self.col}: {self.code} {self.message}"


def allowed_lines(source):
    """The lines of ``source`` a ``# invaract: allow`` marker is on, each with the reason written after it (``""`` for
    none): what keeps a finding on that line."""
    lines = {}
    if "invaract" not in source:
        return lines  # no marker can be there: skip tokenizing
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            marker = ALLOW.search(token.string)
            if marker:
                lines[token.start[0]] = marker.group("reason").strip(" \t-:,;")
    return lines


def _mask_in_code(source):
    """Python source with every secret given by name masked. A secret is a value that is text (``password='secret'``) or
    is inside text (a connection string); a value that is a name or a call (``password=password``) holds none to copy,
    and says where the secret comes from."""
    source = _SECRET_ARGUMENT.sub(r"\1***", source)
    return _TEXT_IN_CODE.sub(lambda text: _SECRET.sub(r"\1***", text.group(0)), source)


def short(text, code=False):
    """``text`` with any password masked (in a URL, or given by name), cut to ``_LONGEST`` characters. ``code`` says the text
    is Python source, where what follows ``password=`` may be a name and no secret."""
    text = _PASSWORD.sub(r"\1:***@", text)
    text = _mask_in_code(text) if code else _SECRET.sub(r"\1***", text)
    return text if len(text) <= _LONGEST else text[:_LONGEST - 1] + "\u2026"
