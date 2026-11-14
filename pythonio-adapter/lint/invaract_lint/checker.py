# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Finds the code in a Python job that reads or writes data outside the invaract package.

It reads the source as Python does (``resolver``) and matches what each call, and each reference to a function, stands for
against the catalogue of readers, writers and connections (``catalogue``). It never runs the code. What it cannot follow it
does not guess at: it reports it (``IVR006``), so the scan fails closed. A line the team means to keep is marked
``# invaract: allow``, with the reason, which shows up in review and in every report.

What it finds:

- reads (``IVR001``): pandas, Polars, pyarrow, DuckDB, numpy and other libraries' readers;
- writes (``IVR002``): those libraries' writers; a frame's ``to_csv``, ``to_parquet``, ... and ``write_*`` given a
  destination, ``sink_*``; ``open`` and the other openers (``gzip.open``, ``zipfile.ZipFile``, ``os.open``, ...) in a mode
  that writes or one not spelled out; a ``Path``'s writers; copies, moves and deletes (``shutil``, ``os.rename``, ...);
- connections (``IVR003``): database drivers, ``sqlalchemy``, cloud and storage clients, filesystems, HTTP clients;
- SQL that reads or writes files (``IVR004``): ``COPY``, ``ATTACH``, ``read_parquet(...)``, a quoted file after ``FROM``
  (``sqltext``), and SQL not spelled out that is run on a DuckDB or SQLite connection;
- other programs (``IVR005``): ``subprocess``, ``os.system``;
- code it cannot follow (``IVR006``): ``exec`` and ``eval``, ``getattr`` and imports by a name built at run time, native
  code loaded with ``ctypes``, code run in another interpreter, and code that reaches under the checks themselves (into
  the invaract package's own parts, or to the function under a wrapper).

A function that is passed or stored without being called (``map(pd.read_csv, paths)``) is found where it is referred to,
and one given another name (``reader = pd.read_csv``) where it is called. What is certainly not data at rest is left alone:
a buffer in memory (``io.BytesIO()``), the console, a scratch file in the temporary folder, a database in memory.

How a call is judged is ``_Scan._named_call``, top to bottom; what counts is the catalogue's to say, so a new library is
a line there and no change here. A function whose call needs a reading of its own (``getattr``, ``vars``) is in
``_SPECIAL``.

Each ``Finding`` (``findings``) says where it is (its start and end, the function or class it is in), what it is (as written, the name it
resolves to, the rule that matched, the path or mode it names), and whether a ``# invaract: allow`` marker keeps it, with
the reason the marker gives. ``check_source`` returns the findings not marked; ``scan_source`` returns every one.
"""
import ast
import re
import tokenize

from . import catalogue, sqltext
from .catalogue import CATEGORIES, CONNECT, DYNAMIC, PROCESS, READ, SQL, WRITE  # noqa: F401  (the codes, importable here)
from .findings import Finding, allowed_lines, short
from .resolver import CONSOLE, MEMORY, PATH, TEMPORARY, Names

#: What a mode looks like (``"w"``, ``"rb+"``), as against a file's name.
_MODE = re.compile(r"[rwxabtU+]{1,4}")

#: What a finding about a function that is referred to, not called, says of it.
REFERENCED = "passed or stored, not called here"
#: What a finding about code that reaches into the invaract package's own parts says of it.
INTERNALS = "reaches into the invaract package's own parts, which is how its checks could be switched off"

def check_source(source, path="<source>"):
    """The code in ``source`` that reads or writes data outside invaract and is not marked ``# invaract: allow``, in order.
    Raises ``SyntaxError`` for source that is not Python."""
    return [f for f in scan_source(source, path) if not f.allowed]


def scan_source(source, path="<source>"):
    """Everything in ``source`` that reads or writes data outside invaract, marked ``# invaract: allow`` or not
    (``Finding.allowed``), in order. Raises ``SyntaxError`` for source that is not Python."""
    tree = ast.parse(source, filename=path)
    allowed = allowed_lines(source)
    scan = _Scan(tree)
    findings = []
    for node, (code, rule, detail) in scan.matches():
        reasons = [allowed[line] for line in range(node.lineno, node.end_lineno + 1) if line in allowed]
        written = node.func if isinstance(node, ast.Call) else node
        # a call that stands for a function itself (getattr with the name spelled out) resolves to that function
        resolved = (scan.names.qualify(node) if isinstance(node, ast.Call) else []) or scan.names.qualify(written)
        findings.append(Finding(
            path, node.lineno, node.col_offset + 1, code, ast.unparse(written),
            end_line=node.end_lineno, end_col=node.end_col_offset + 1, resolved=resolved[0] if resolved else None,
            scope=".".join(scan.names.scope_of[id(node)].path()) or "<module>", rule=rule,
            snippet=short(" ".join(ast.unparse(node).split()), code=True),
            target=_target(node, code, rule) if isinstance(node, ast.Call) and detail != REFERENCED else None,
            detail=detail, allowed=bool(reasons),
            reason=next((r for r in reasons if r), None),
        ))
    return sorted(findings, key=lambda f: (f.line, f.col, f.end_line, f.end_col))


def check_file(path):
    """``check_source`` of the file at ``path``, read as Python reads it (UTF-8, or the encoding its first lines declare)."""
    return [f for f in scan_file(path) if not f.allowed]


def scan_file(path):
    """``scan_source`` of the file at ``path``, read as ``check_file`` reads it."""
    with tokenize.open(path) as f:
        source = f.read()
    return scan_source(source, path)


#: A call that is certainly no read or write: found to be nothing, so no other reading of it is tried.
_LEFT_ALONE = (None, None, None)
#: What a value certainly is when it is no data at rest.
_NOT_AT_REST = (MEMORY, CONSOLE, TEMPORARY)
#: The functions whose calls have a reading of their own, each with the method of ``_Scan`` that reads it. To teach the
#: scan another such function, add it here; a function that simply reads, writes or connects goes in the catalogue.
_SPECIAL = {
    "getattr": "_getattr", "vars": "_vars", "importlib.import_module": "_imported", "__import__": "_imported",
    "operator.methodcaller": "_method_by_name", "operator.attrgetter": "_method_by_name", "os.open": "_os_open_call",
}


class _Scan:
    """One file's scan: every call and every reference in it, each matched against the catalogue."""

    def __init__(self, tree):
        self.tree = tree
        self.names = Names(tree)
        nodes = list(ast.walk(tree))
        #: expressions that are what a call calls: the call is judged, not the expression again
        self._called = {id(n.func) for n in nodes if isinstance(n, ast.Call)}
        #: expressions given a name of their own by a plain assignment: calls through that name are followed
        self._named = {id(n.value) for n in nodes if isinstance(n, (ast.Assign, ast.AnnAssign, ast.NamedExpr)) and n.value is not None
                       and all(isinstance(t, ast.Name) for t in (n.targets if isinstance(n, ast.Assign) else [n.target]))}
        #: the inner parts of a dotted name (``pyarrow.fs`` in ``pyarrow.fs.S3FileSystem``): judged as the whole
        self._inner = {id(n.value) for n in nodes if isinstance(n, ast.Attribute)}
        #: expressions that only name a type: annotations, and what ``isinstance`` and ``except`` test against
        self._types = {id(n) for holder in nodes for part in _type_positions(holder) for n in ast.walk(part)}
        self._nodes = nodes

    def matches(self):
        """``(node, (code, rule, detail))`` for every call and reference that reads or writes data outside invaract."""
        for node in self._nodes:
            if isinstance(node, ast.Call):
                match = self._call(node)
            elif isinstance(node, (ast.Name, ast.Attribute)):
                match = self._internals(node) or (self._reference(node) if isinstance(node.ctx, ast.Load) else None)
            elif isinstance(node, ast.Subscript):
                match = self._module_table(node.value, node.slice)
            else:
                match = None
            if match is not None:
                yield node, match

    # calls

    def _call(self, call):
        names = self.names.qualify(call.func)
        for name in names:
            match = self._named_call(call, name)
            if match is not None:
                return match if match[0] is not None else None
        if isinstance(call.func, ast.Attribute):
            if call.func.attr in ("get", "pop", "__getitem__") and call.args:
                table = self._module_table(call.func.value, call.args[0])
                if table is not None:
                    return table
            return self._method_call(call, names)
        return None

    def _named_call(self, call, name):
        """What a call of the function ``name`` is: a match, ``_LEFT_ALONE`` for one that is certainly no read or write (so
        no other reading of the call is tried), or ``None`` for a name the catalogue does not know.

        In order: a function with a reading of its own (``_SPECIAL``); a scratch file made somewhere the call names; an
        opener, judged by its mode; then the catalogue's rule for the name, unless what the call reads or writes is
        certainly not data at rest."""
        special = _SPECIAL.get(name)
        if special is not None:
            return getattr(self, special)(call) or _LEFT_ALONE
        if catalogue.TEMPORARY.fullmatch(name) and any(k.arg == "dir" for k in call.keywords):
            return WRITE, "file-operation", "a scratch file made in a folder the call names (dir=), not the temporary folder"
        if name in catalogue.OPENERS:
            return self._opener_call(call, name) or _LEFT_ALONE
        rule = catalogue.module_rule(name)
        if rule is None:
            return None
        return _LEFT_ALONE if self._not_at_rest(call, name, rule) else (rule.code, rule.name, None)

    def _opener_call(self, call, name):
        """A call of an opener (``open``, ``gzip.open``, ``h5py.File``): a write when its mode writes or is not spelled
        out. Opened to read, a dataset is a read; a plain file is not the scan's to judge."""
        position, keyword, letters = catalogue.OPENERS[name]
        opened = call.args[:1] + [k.value for k in call.keywords if k.arg == "fileobj"]
        if opened and all(self.names.kind(o) in _NOT_AT_REST for o in opened):
            return None  # a scratch file, the null device, or an archive built in a buffer
        written = self._opened(_argument(call, position, keyword), "open-write-mode", letters)
        return written or ((READ, "library-read", None) if name in catalogue.DATA_OPENERS else None)

    def _not_at_rest(self, call, name, rule):
        """Whether a call the catalogue has a rule for certainly reads or writes no data at rest: a buffer in memory, the
        console, a scratch file, a database in memory."""
        if rule.code == READ:
            return (bool(call.args) and self.names.kind(call.args[0]) in _NOT_AT_REST) or any(self._memory_database(a) for a in _destinations(call))
        if rule.code == WRITE:
            return self._scratch(call.args) if rule.name == "file-operation" else self._nowhere(call)
        return rule.name == "database-connect" and _in_memory_database(call, name)

    def _os_open_call(self, call):
        return None if self._scratch(call.args[:1]) else self._os_open(_argument(call, 1, "flags"))

    def _vars(self, call):
        return self._namespace(call.args[0]) if call.args else None

    def _method_by_name(self, call):
        """``operator.methodcaller("to_csv", path)``: a frame's writer, named in a string."""
        method = call.args[0].value if call.args and isinstance(call.args[0], ast.Constant) else None
        return (WRITE, "frame-write", REFERENCED) if _writes(method) else None

    def _imported(self, call):
        """An import by a name not spelled out cannot be followed, unless what is spelled out places it in a package of
        the job's own (``"jobs.steps." + name``), whose modules are scanned as they are loaded."""
        text, complete = self.names.text(call.args[0]) if call.args else ("", False)
        if catalogue.INTERNALS.fullmatch(text):
            return DYNAMIC, "tampering", INTERNALS
        own_package = "." in text and not text.startswith(".") and text.split(".")[0] not in catalogue.DATA_LIBRARIES
        if complete or own_package:
            return None
        return DYNAMIC, "dynamic-import", "the module's name is not spelled out"

    # reaching under the checks

    def _internals(self, node):
        """A name that stands for one of the invaract package's own parts (``invaract._strict``), read or assigned to: the
        whole dotted name is judged once."""
        if id(node) in self._inner or id(node) in self._types or (isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load)):
            return None  # part of a longer name, a type, or a name being given (what it is given is judged where it is read)
        if any(catalogue.INTERNALS.fullmatch(name) for name in self.names.qualify(node)):
            return DYNAMIC, "tampering", INTERNALS
        return None

    def _module_table(self, table, name):
        """``sys.modules[...]``, ``.get(...)`` and ``.pop(...)`` naming one of the invaract package's modules: the way to its
        own parts that no import shows."""
        if "sys.modules" in self.names.qualify(table) and self.names.text(name)[0].startswith(("invaract.", "invaract_lint.")):
            return DYNAMIC, "tampering", INTERNALS
        return None

    def _under(self, node):
        """An attribute of a reader, writer or connection that does more than describe it (``__closure__``, ``__func__``, a
        method taken from a class): a use of it, by a way no call of its name shows."""
        if node.attr in catalogue.DESCRIBES or node.attr == "__call__" or id(node) in self._types:
            return None  # ``f.__call__`` is ``f``, and is judged as it
        for name in self.names.qualify(node.value):
            rule = catalogue.module_rule(name)
            if rule is not None and catalogue.module_rule(f"{name}.{node.attr}") is None:
                return rule.code, rule.name, f"reached through its attribute `{node.attr}`"
        return None

    def _getattr(self, call):
        """``getattr(x, name)``: with the name spelled out it is the attribute itself, judged as a reference; with a name
        built at run time, on a library that reads and writes or with a reader's or writer's start, it cannot be followed."""
        if len(call.args) < 2:
            return None
        target, name = call.args[0], call.args[1]
        if isinstance(name, ast.Constant) and isinstance(name.value, str):
            if id(call) in self._called and self.names.qualify(call):
                return None  # a library's function, called on the spot: the call around it is judged, as that function
            return self._reference(call, method=name.value)
        roots = {q.split(".")[0] for q in self.names.qualify(target)}
        if roots & catalogue.DYNAMIC_ROOTS:
            return DYNAMIC, "dynamic-attribute", "the attribute's name is not spelled out"
        start = self.names.text(name)[0]
        if not roots and start.startswith(catalogue.DYNAMIC_METHOD_PREFIXES):
            return DYNAMIC, "dynamic-attribute", f"a method whose name starts {start!r}, the rest built at run time"
        return None

    def _namespace(self, module):
        roots = {q.split(".")[0] for q in self.names.qualify(module)}
        if roots & {"invaract", "invaract_lint"}:
            return DYNAMIC, "tampering", INTERNALS
        if roots & catalogue.DYNAMIC_ROOTS:
            return DYNAMIC, "dynamic-attribute", "the module's names, looked up at run time"
        return None

    def _method_call(self, call, names):
        method, receiver = call.func.attr, call.func.value
        kind = self.names.kind(receiver)
        if not self._own_module(receiver):
            if method in catalogue.ALWAYS_WRITE or method.startswith(catalogue.SINK_PREFIX):
                return None if kind == TEMPORARY else (WRITE, "always-writes", None)
            if method in catalogue.PATH_MOVES and kind in (PATH, TEMPORARY) and not self._scratch(call.args[:1]):
                return WRITE, "path-move", None
            if method in catalogue.FRAME_WRITES and _destinations(call) and not self._nowhere(call):
                return WRITE, "frame-write", None
            if method == "open" and not names and kind != TEMPORARY:
                return self._opened(self._method_mode(call, kind), "path-open-write-mode", catalogue.WRITE_MODE)
            if method in catalogue.DUCKDB_READS and kind == "duckdb":
                return READ, "duckdb-read", None
        if method in catalogue.SQL_METHODS and call.args:
            text, _ = self.names.text(call.args[0], unknown=sqltext.UNKNOWN, earlier=sqltext.EARLIER)
            reached = sqltext.file_access(text)
            if reached:
                return SQL, "file-sql", reached
            duckdb = kind == "duckdb" or any(n.startswith("duckdb.") for n in names)
            gap = sqltext.built(text)
            if gap == "statement" and (duckdb or kind in catalogue.SQL_CONNECTIONS.values()):
                return SQL, "dynamic-sql", "the SQL is not spelled out, and this connection's SQL can read and write files"
            if gap == "table" and duckdb:
                return SQL, "dynamic-sql", "what the SQL reads from is not spelled out, and in DuckDB that can be a file"
        return None

    def _method_mode(self, call, kind):
        """The mode of ``x.open(...)``: a ``Path``'s first argument (``Path(...).open("w")``); the second argument of
        anything else's, whose first is the file's name (``fs.open(path, "wb")``, ``archive.open(name, "w")``)."""
        first = call.args[0] if call.args else None
        if first is None or isinstance(first, ast.Starred) or kind == PATH:
            return _argument(call, 0, "mode")
        text, complete = self.names.text(first)
        if complete and _MODE.fullmatch(text):
            return first
        return _argument(call, 1, "mode")

    def _own_module(self, receiver):
        """Whether ``receiver`` is certainly a module (``import helpers``) that is none of the libraries that read and
        write data: its ``to_csv`` is the job's own function, not a frame's method."""
        module = self.names.plain_module(receiver)
        return module is not None and module.split(".")[0] not in catalogue.DATA_LIBRARIES

    # references

    def _reference(self, node, method=None):
        """A function referred to without being called here: found when it is a reader, writer or connection that is then
        passed on or stored, so that no call of it is seen."""
        under = self._under(node) if isinstance(node, ast.Attribute) and method is None else None
        if under is not None:
            return under
        if (id(node) in self._called and method is None) or id(node) in self._inner or id(node) in self._types:
            return None
        for name in self.names.qualify(node):
            rule = catalogue.module_rule(name)
            if rule is not None or name in catalogue.OPENERS or name == "os.open":
                if id(node) in self._named:
                    return None  # given a name: the calls through that name are judged
                if rule is None:
                    return WRITE, "open-write-mode", REFERENCED
                return rule.code, rule.name, REFERENCED
        attribute = method or (node.attr if isinstance(node, ast.Attribute) else None)
        receiver = node.args[0] if isinstance(node, ast.Call) else getattr(node, "value", None)
        if _writes(attribute) and not self.names.qualify(node) and receiver is not None and not self._own_module(receiver):
            return WRITE, "frame-write" if attribute in catalogue.FRAME_WRITES else "always-writes", REFERENCED
        if isinstance(node, ast.Attribute) and node.attr == "__dict__":
            return self._namespace(node.value)
        return None

    # what a call writes to

    def _opened(self, mode, rule, letters):
        """A write when ``mode`` writes, or when the source does not spell it out; ``None`` for no mode (read) or a read
        mode. A ``tarfile`` mode's compression (``r:xz``) is no part of what it does."""
        if mode is None:
            return None
        text, complete = self.names.text(mode)
        if not complete:
            return WRITE, rule, "mode not spelled out"
        writing = [m for m in text.split(";\n") if set(re.split(r"[:|]", m)[0]) & set(letters)]
        return (WRITE, rule, f"mode {writing[0]!r}") if writing else None

    def _os_open(self, flags):
        """``os.open``: a write unless its flags, all spelled out, only read."""
        if flags is None:
            return WRITE, "open-write-mode", "flags not spelled out"
        if isinstance(flags, ast.Constant):
            return None if flags.value == 0 else (WRITE, "open-write-mode", f"flags {flags.value!r}")
        named = [n for n in ast.walk(flags) if isinstance(n, (ast.Name, ast.Attribute)) and id(n) not in self._inner_of(flags)]
        spelled = [q for n in named for q in self.names.qualify(n)]
        if spelled and len(spelled) == len(named) and set(spelled) <= catalogue.OS_OPEN_READ_FLAGS:
            return None
        writing = sorted(q.split(".")[-1] for q in spelled if q not in catalogue.OS_OPEN_READ_FLAGS)
        return WRITE, "open-write-mode", f"flags {' | '.join(writing)}" if writing else "flags not spelled out"

    @staticmethod
    def _inner_of(node):
        return {id(n.value) for n in ast.walk(node) if isinstance(n, ast.Attribute)}

    def _memory_database(self, node, depth=0):
        """Whether ``node`` is certainly a DuckDB or SQLite database held in memory: the call that makes one, or a name
        every assignment of which is one."""
        if isinstance(node, ast.Call):
            return any(name in catalogue.SQL_CONNECTIONS and _in_memory_database(node, name) for name in self.names.qualify(node.func))
        values = self.names.values(node) if depth < 5 else None
        return bool(values) and all(self._memory_database(value, depth + 1) for value, _ in values)

    def _scratch(self, arguments):
        """Whether ``arguments`` (there is at least one) are all scratch files or folders in the temporary folder."""
        return bool(arguments) and all(self.names.kind(a) == TEMPORARY for a in arguments)

    def _nowhere(self, call):
        """Whether a writer's call writes to no dataset: it is handed a buffer in memory, the console or a scratch file,
        and nothing that names a place (a string, a path), or its destination is spelled out as nothing (``None``)."""
        candidates = _destinations(call)
        if any(self._memory_database(c) for c in candidates):
            return True  # into a database held in memory: the strings beside it name tables there
        if candidates and isinstance(candidates[0], ast.Constant) and not isinstance(candidates[0].value, (str, bytes)):
            return call.func.attr in catalogue.FRAME_WRITES if isinstance(call.func, ast.Attribute) else False
        kinds = [self.names.kind(c) for c in candidates]
        places = [c for c, k in zip(candidates, kinds) if k not in _NOT_AT_REST and (
            k == PATH or isinstance(c, ast.JoinedStr) or (isinstance(c, ast.Constant) and isinstance(c.value, str)))]
        return not places and any(k in _NOT_AT_REST for k in kinds)


def _type_positions(node):
    """The parts of ``node`` that name a type and do nothing: a parameter's or a variable's annotation, a function's return
    annotation, what ``isinstance`` and ``issubclass`` test against, and the exceptions an ``except`` names."""
    if isinstance(node, ast.arg) and node.annotation is not None:
        return [node.annotation]
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns is not None:
        return [node.returns]
    if isinstance(node, ast.AnnAssign):
        return [node.annotation]
    if isinstance(node, ast.ExceptHandler) and node.type is not None:
        return [node.type]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("isinstance", "issubclass"):
        return node.args[1:2]
    if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", None)) == "cast":
        return node.args[:1]
    return []


def _writes(method):
    return isinstance(method, str) and (
        method in catalogue.FRAME_WRITES or method in catalogue.ALWAYS_WRITE or method.startswith(catalogue.SINK_PREFIX))


def _destinations(call):
    """The arguments of a call that may say where it writes: the positional ones, the destination keywords, and whatever
    is unpacked into it (``**options`` may hold a ``path``)."""
    return list(call.args) + [k.value for k in call.keywords if k.arg is None or k.arg in catalogue.DESTINATIONS]


def _in_memory_database(call, name):
    """A DuckDB or SQLite database in memory: ``duckdb.connect()``, one spelled ``":memory:"``, or DuckDB's default
    connection (``":default:"``)."""
    driver = name.rsplit(".", 1)[0]
    if driver not in ("duckdb", "sqlite3", "aiosqlite"):
        return False
    database = _argument(call, 0, "database")
    if database is None:
        return driver == "duckdb"
    return isinstance(database, ast.Constant) and isinstance(database.value, str) and (
        database.value.startswith((":memory:", "file::memory:")) or database.value == ":default:")


def _argument(call, position, keyword):
    """The argument at ``position``, or else the one passed as ``keyword``; ``None`` when neither is given. Arguments
    unpacked into the call (``*args`` at or before the position, ``**options``) may hold it: the unpacked value is
    returned, which the source does not spell out."""
    for argument in call.args[:position + 1]:
        if isinstance(argument, ast.Starred):
            return argument
    if len(call.args) > position:
        return call.args[position]
    named = next((k.value for k in call.keywords if k.arg == keyword), None)
    return named if named is not None else next((k.value for k in call.keywords if k.arg is None), None)


def _target(call, code, rule):
    """The path or URL a read or write names: the first of its arguments written as a string (``pq.write_table(t,
    "out.parquet")``; an opener's first only), else a destination keyword's (``path=``); ``None`` when the source does not
    spell one out, and for a connection (whose argument may be credentials), SQL (whose text is the query, not a place),
    another program and code the scan cannot follow."""
    if code not in (READ, WRITE):
        return None
    # an opener's second argument is its mode, never a place
    positional = call.args[:1] if rule == "open-write-mode" else call.args
    candidates = positional + [k.value for k in call.keywords if k.arg in catalogue.DESTINATIONS]
    if not isinstance(call.func, ast.Attribute):
        pass
    elif rule in ("path-open-write-mode", "path-move") or (rule == "always-writes" and call.func.attr in catalogue.PATH_WRITES):
        # Path('a').write_text(...): the place is the Path, not the first argument (the text, or the mode)
        owner = call.func.value
        candidates = owner.args[:1] if isinstance(owner, ast.Call) else []
    literal = next((c.value for c in candidates if isinstance(c, ast.Constant) and isinstance(c.value, str)), None)
    return short(literal) if literal is not None else None
