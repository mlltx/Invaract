# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What the names in a file stand for, scope by scope, as Python itself resolves them.

A name means what the nearest scope that binds it says: a function's parameter or local variable hides an import of the
same name, and hides a builtin; a class body's names are not seen from its methods. Within the binding scope, every
binding counts: a name both imported and assigned (``try: import pandas as pd`` / ``except ImportError: pd = None``) may
be either, and the scan takes it for any of them, since a finding that needs a marker is safer than a read not seen.

``Names.qualify`` gives the dotted names an expression may stand for (``pd.read_csv`` -> ``pandas.read_csv``), following
imports, plain assignments (``reader = pd.read_csv``), and ``getattr`` and ``importlib.import_module`` with the name
spelled out. ``Names.kind`` says when a value is, by every assignment the scope makes to it, held in
memory, the console, a scratch file, a path, or a DuckDB or SQLite connection. ``Names.text`` gives the text of a string
the source spells out, through the names it is assigned to.
"""
import ast

from . import catalogue

#: How deep an expression is followed, through its parts and the names it uses, before the scan stops.
_DEPTH = 60

_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)

#: How a name was bound: by ``import m`` (certainly a module), by ``from m import n``, by an assignment of one value, by
#: unpacking a value, or some other way (a parameter, a ``def``, a loop variable).
MODULE, IMPORTED, VALUE, UNPACKED, OTHER = "module", "import", "value", "unpacked", "other"

MEMORY, CONSOLE, TEMPORARY, PATH = "memory", "console", "temporary", "path"


class Scope:
    def __init__(self, node, parent, kind):
        self.node, self.parent, self.kind = node, parent, kind
        #: each name bound here, with every way it is bound: ``(how, what, scope the value is read in)``
        self.bindings = {}
        #: modules imported with ``from m import *``
        self.stars = []
        self.globals, self.nonlocals = set(), set()

    def bind(self, name, how, what=None, scope=None):
        target = self
        if name in self.globals:
            while target.parent is not None:
                target = target.parent
        elif name in self.nonlocals and self.parent is not None:
            target = self.parent
            while target.kind == "class" and target.parent is not None:
                target = target.parent
        target.bindings.setdefault(name, []).append((how, what, scope or self))

    def path(self):
        """The names of the functions and classes this scope is inside, outermost first."""
        names, scope = [], self
        while scope is not None:
            if scope.kind in ("function", "class"):
                names.append(getattr(scope.node, "name", "<lambda>"))
            scope = scope.parent
        return tuple(reversed(names))


class Names:
    """The scopes of one file, and what its expressions stand for."""

    def __init__(self, tree):
        #: the scope each node is read in, by ``id(node)``
        self.scope_of = {}
        self.module = Scope(tree, None, "module")
        #: what each name stands for, once worked out; the names being worked out, those that came back to themselves
        #: while they were, and what a name being worked out is taken to be meanwhile
        self._known, self._working, self._returned, self._assumed = {}, set(), set(), {}
        self._visit_body(tree, self.module)

    # building the scopes

    def _visit_body(self, node, scope):
        """Gives every node under ``node`` its scope, and every scope its bindings, in the order the source has them. It
        keeps its own list of what is left to visit, so code nested however deep is walked without Python's own limit on
        nested calls coming into it."""
        pending = [(child, scope) for child in reversed(list(ast.iter_child_nodes(node)))]
        while pending:
            node, scope = pending.pop()
            pending.extend(reversed(self._visit(node, scope)))

    def _visit(self, node, scope):
        """Notes ``node``'s scope and what it binds, and returns what is under it to visit next, each with its scope."""
        self.scope_of[id(node)] = scope
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scope.bind(node.name, OTHER)
            # decorators, defaults and annotations are read where the function is defined
            outside = node.decorator_list + node.args.defaults + [d for d in node.args.kw_defaults if d is not None]
            if node.returns is not None:
                outside.append(node.returns)
            inner, annotations = self._function(node, scope)
            return [(n, scope) for n in outside + annotations] + [(statement, inner) for statement in node.body]
        if isinstance(node, ast.Lambda):
            outside = node.args.defaults + [d for d in node.args.kw_defaults if d is not None]
            inner, annotations = self._function(node, scope)
            return [(n, scope) for n in outside + annotations] + [(node.body, inner)]
        if isinstance(node, ast.ClassDef):
            scope.bind(node.name, OTHER)
            outside = node.decorator_list + node.bases + [k.value for k in node.keywords]
            inner = Scope(node, scope, "class")
            self._declare(node, inner)
            return [(n, scope) for n in outside] + [(statement, inner) for statement in node.body]
        if isinstance(node, _COMPREHENSIONS):
            inner = Scope(node, scope, "comprehension")
            for generator in node.generators:
                for name in _names_in(generator.target):
                    inner.bind(name, OTHER)
            return [(child, inner) for child in ast.iter_child_nodes(node)]
        self._bind(node, scope)
        return [(child, scope) for child in ast.iter_child_nodes(node)]

    def _function(self, node, scope):
        """The scope of a function's or lambda's body, its parameters bound, and the parameters' annotations (which are
        read outside it)."""
        inner = Scope(node, scope, "function")
        self._declare(node, inner)
        arguments = node.args
        annotations = []
        for argument in arguments.posonlyargs + arguments.args + arguments.kwonlyargs + [arguments.vararg, arguments.kwarg]:
            if argument is not None:
                self.scope_of[id(argument)] = inner
                inner.bind(argument.arg, OTHER)
                if argument.annotation is not None:
                    annotations.append(argument.annotation)
        return inner, annotations

    def _declare(self, node, scope):
        """Notes the ``global`` and ``nonlocal`` names of a function or class body, before anything in it is bound."""
        pending = list(node.body) if isinstance(node.body, list) else []
        while pending:
            statement = pending.pop()
            if isinstance(statement, ast.Global):
                scope.globals.update(statement.names)
            elif isinstance(statement, ast.Nonlocal):
                scope.nonlocals.update(statement.names)
            elif not isinstance(statement, _FUNCTIONS + (ast.ClassDef,)):
                pending.extend(c for c in ast.iter_child_nodes(statement) if isinstance(c, ast.stmt))

    def _bind(self, node, scope):
        """Binds the names a statement or expression binds in ``scope``."""
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    scope.bind(alias.asname, MODULE, alias.name)
                else:
                    top = alias.name.split(".")[0]
                    scope.bind(top, MODULE, top)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    if node.module and node.level == 0:
                        scope.stars.append(node.module)
                elif node.module and node.level == 0:
                    scope.bind(alias.asname or alias.name, IMPORTED, f"{node.module}.{alias.name}")
                else:
                    scope.bind(alias.asname or alias.name, OTHER)  # a relative import: the job's own
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                self._assign(target, node.value, scope)
        elif isinstance(node, ast.AnnAssign):
            if node.value is not None:
                self._assign(node.target, node.value, scope)
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                # ``sql += more`` is ``sql = sql + more``
                before = ast.copy_location(ast.Name(id=node.target.id, ctx=ast.Load()), node.target)
                scope.bind(node.target.id, VALUE, ast.copy_location(ast.BinOp(left=before, op=node.op, right=node.value), node), scope)
        elif isinstance(node, ast.NamedExpr):
            holder = scope
            while holder.kind == "comprehension" and holder.parent is not None:
                holder = holder.parent
            holder.bind(node.target.id, VALUE, node.value, scope)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            for name in _names_in(node.target):
                scope.bind(name, OTHER)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if item.optional_vars is not None:
                    self._assign(item.optional_vars, item.context_expr, scope)
        elif isinstance(node, ast.ExceptHandler):
            if node.name:
                scope.bind(node.name, OTHER)
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)):
            if node.name:
                scope.bind(node.name, OTHER)
        elif isinstance(node, ast.MatchMapping):
            if node.rest:
                scope.bind(node.rest, OTHER)

    def _assign(self, target, value, scope):
        if isinstance(target, ast.Name):
            scope.bind(target.id, VALUE, value, scope)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for name in _names_in(target):
                scope.bind(name, UNPACKED, value, scope)

    # reading them

    def _binder(self, name, scope):
        """The scope that binds ``name`` for code read in ``scope``, or ``None`` when no scope of the file does."""
        current, first = scope, True
        while current is not None:
            if name in current.bindings and (first or current.kind != "class"):
                return current
            current, first = current.parent, False
        return None

    def _once(self, key, working, compute):
        """What ``compute`` gives for ``key``, worked out once and remembered. A name bound in terms of itself
        (``frame = frame.merge(other)``, ``sql = sql + " AND ..."``) comes back to its own key while it is being worked
        out: it is then given ``working`` (a function of the key), and whatever was worked out from that, for any other
        key on the way, is not remembered, since it stood on an answer not yet final."""
        if key in self._known:
            return self._known[key]
        if key in self._working:
            self._returned.add(key)
            return working(key)
        self._working.add(key)
        try:
            result = compute()
        finally:
            self._working.discard(key)
            self._returned.discard(key)
        if not self._returned:
            self._known[key] = result
        return result

    def qualify(self, node, scope=None, depth=0):
        """The dotted names ``node`` may stand for, in the order the file gives them; none when it stands for a value the
        file does not say the origin of."""
        scope = scope or self.scope_of.get(id(node), self.module)
        if depth > _DEPTH:
            return []
        if isinstance(node, ast.Name):
            binder = self._binder(node.id, scope)
            if binder is None:
                return self._unbound(node.id, scope)

            def bound():
                found = []
                for how, what, where in binder.bindings[node.id]:
                    if how in (MODULE, IMPORTED):
                        found.append(what)
                    elif how == VALUE:
                        found.extend(self.qualify(what, where, depth + 1))
                return list(dict.fromkeys(found))
            return self._once(("name", id(binder), node.id), lambda key: [], bound)
        if isinstance(node, ast.Attribute):
            if node.attr == "__call__":
                return self.qualify(node.value, scope, depth + 1)
            return [f"{base}.{node.attr}" for base in self.qualify(node.value, scope, depth + 1)]
        if isinstance(node, ast.NamedExpr):
            return self.qualify(node.value, scope, depth + 1)
        if isinstance(node, ast.Call):
            called = self.qualify(node.func, scope, depth + 1)
            name = None
            if node.args and ("importlib.import_module" in called or "__import__" in called):
                # the module's name, spelled out where the call is or in what a name was assigned
                text, complete = self.text(node.args[0], scope, depth + 1)
                name = text if complete and text and ";" not in text else None
            if "importlib.import_module" in called and name:
                return [name]
            if "__import__" in called and name:
                return [name.split(".")[0]]
            if "getattr" in called and len(node.args) >= 2 and _constant_text(node.args[1]):
                return [f"{base}.{node.args[1].value}" for base in self.qualify(node.args[0], scope, depth + 1)]
        return []

    def _unbound(self, name, scope):
        """A name no scope binds: one a ``from m import *`` may have brought in (when it is a name the catalogue knows),
        else a builtin."""
        current = scope
        while current is not None:
            for module in current.stars:
                candidate = f"{module}.{name}"
                if catalogue.module_rule(candidate) or candidate in catalogue.OPENERS:
                    return [candidate]
            current = current.parent
        if name == "__builtins__":
            return ["builtins"]
        return [name] if name in catalogue.BUILTINS else []

    def plain_module(self, node, scope=None):
        """The module ``node`` certainly is (a name every binding of which is ``import m``), or ``None``."""
        scope = scope or self.scope_of.get(id(node), self.module)
        if not isinstance(node, ast.Name):
            return None
        binder = self._binder(node.id, scope)
        if binder is None:
            return None
        bindings = binder.bindings[node.id]
        modules = {what for how, what, _ in bindings if how == MODULE}
        return modules.pop() if len(modules) == 1 and all(how == MODULE for how, _, _ in bindings) else None

    def values(self, node, scope=None):
        """What a name is assigned, when every binding of it is a plain assignment of one value: each ``(value, scope)``.
        ``None`` for a name bound any other way (a parameter, an import), and for what is not a name."""
        scope = scope or self.scope_of.get(id(node), self.module)
        if not isinstance(node, ast.Name):
            return None
        binder = self._binder(node.id, scope)
        if binder is None or any(how != VALUE for how, _, _ in binder.bindings[node.id]):
            return None
        return [(what, where) for _, what, where in binder.bindings[node.id]]

    def kind(self, node, scope=None, depth=0):
        """What ``node`` certainly is, by everything the file shows: ``memory`` (a buffer), ``console``, ``temporary`` (a
        scratch file or folder), ``path``, ``duckdb`` or ``sqlite`` (a connection); ``None`` when the file does not show
        it, or shows it to be now one thing and now another."""
        scope = scope or self.scope_of.get(id(node), self.module)
        if depth > _DEPTH:
            return None
        if isinstance(node, ast.Constant):
            return MEMORY if isinstance(node.value, bytes) else None
        if isinstance(node, ast.Name):
            binder = self._binder(node.id, scope)
            if binder is None:
                return None
            return self._name_kind(("kind", id(binder), node.id), binder.bindings[node.id], depth)
        if isinstance(node, ast.Attribute):
            if any(catalogue.CONSOLE.fullmatch(name) for name in self.qualify(node, scope, depth + 1)):
                return CONSOLE
            base = self.kind(node.value, scope, depth + 1)
            if base == TEMPORARY and node.attr in ("name", "parent"):
                return TEMPORARY
            if base == PATH and node.attr == "parent":
                return PATH
            return None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            left = self.kind(node.left, scope, depth + 1)
            return left if left in (PATH, TEMPORARY) else None
        if isinstance(node, ast.JoinedStr):
            first = node.values[0] if node.values else None
            if isinstance(first, ast.FormattedValue) and self.kind(first.value, scope, depth + 1) == TEMPORARY:
                return TEMPORARY
            return None
        if isinstance(node, ast.Call):
            return self._call_kind(node, scope, depth)
        return None

    def _name_kind(self, key, bindings, depth):
        """The one kind every binding of a name gives. A binding in terms of the name itself (``work = work / "part"``)
        is whatever the name is: the others decide, and the answer stands only if, taking the name for that, every
        binding agrees."""
        if key in self._known:
            return self._known[key]
        if key in self._working:
            self._returned.add(key)
            return self._assumed.get(key)

        def of(binding):
            how, what, where = binding
            if how == VALUE:
                return self.kind(what, where, depth + 1)
            if how == UNPACKED:
                # the parts of a scratch file's (descriptor, path) are scratch; of anything else, unknown
                return TEMPORARY if self.kind(what, where, depth + 1) == TEMPORARY else None
            return None

        self._working.add(key)
        try:
            others, circular = set(), False
            for binding in bindings:
                self._returned.discard(key)
                found = of(binding)
                if key in self._returned:
                    circular = True
                else:
                    others.add(found)
            result = others.pop() if len(others) == 1 else None
            if circular and result is not None:
                self._assumed[key] = result
                if {of(binding) for binding in bindings} != {result}:
                    result = None
        finally:
            self._assumed.pop(key, None)
            self._working.discard(key)
            self._returned.discard(key)
        if not self._returned:
            self._known[key] = result
        return result

    def _call_kind(self, node, scope, depth):
        first = self.kind(node.args[0], scope, depth + 1) if node.args else None
        for name in self.qualify(node.func, scope, depth + 1):
            if catalogue.IN_MEMORY.fullmatch(name):
                return MEMORY
            if catalogue.TEMPORARY.fullmatch(name):
                # a scratch file made in a folder the call names (dir=) is wherever that folder is
                return None if any(k.arg == "dir" for k in node.keywords) else TEMPORARY
            if catalogue.PATHS.fullmatch(name):
                return TEMPORARY if first == TEMPORARY else PATH
            if name in catalogue.SQL_CONNECTIONS:
                return catalogue.SQL_CONNECTIONS[name]
            if name in ("os.path.join", "os.path.abspath", "os.path.realpath", "os.path.normpath", "os.fspath", "str"):
                return TEMPORARY if first == TEMPORARY else None
        if isinstance(node.func, ast.Attribute):
            base = self.kind(node.func.value, scope, depth + 1)
            method = node.func.attr
            if base == MEMORY and method in ("getvalue", "getbuffer", "to_pybytes", "read"):
                return MEMORY
            if base in (PATH, TEMPORARY) and method in catalogue.PATH_METHODS:
                return base
            if base in catalogue.SQL_CONNECTIONS.values() and method == "cursor":
                return base
        return None

    def text(self, node, scope=None, depth=0, unknown="", earlier=None):
        """``(text, complete)``: the text of a string the source spells out, and whether that is all of it. An f-string
        gives its literal parts and is not complete; a name gives the text of every string assigned to it, one per line
        (each a statement of its own to whoever reads it); what the source does not spell out gives ``unknown`` in its
        place (nothing, unless the caller wants to see where the gaps are) and is not complete. A name built up from
        itself (``sql = sql + " AND ..."``) gives ``earlier`` (``unknown`` when not given) where its own earlier text
        goes."""
        scope = scope or self.scope_of.get(id(node), self.module)
        earlier = unknown if earlier is None else earlier
        if depth > _DEPTH:
            return unknown, False
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value, True
        if isinstance(node, ast.JoinedStr):
            parts = [v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else unknown for v in node.values]
            return "".join(parts), False
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, left_complete = self.text(node.left, scope, depth + 1, unknown, earlier)
            right, right_complete = self.text(node.right, scope, depth + 1, unknown, earlier)
            return left + right, left_complete and right_complete
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return self.text(node.left, scope, depth + 1, unknown, earlier)[0], False
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
                return self.text(node.func.value, scope, depth + 1, unknown, earlier)[0], False
            if node.args and any(name.split(".")[-1] == "text" for name in self.qualify(node.func, scope, depth + 1)):
                return self.text(node.args[0], scope, depth + 1, unknown, earlier)  # sqlalchemy.text("...")
            return unknown, False
        if isinstance(node, ast.Name):
            binder = self._binder(node.id, scope)
            if binder is None:
                return unknown, False

            def bound():
                texts, complete = [], True
                for how, what, where in binder.bindings[node.id]:
                    if how != VALUE:
                        return unknown, False
                    text, whole = self.text(what, where, depth + 1, unknown, earlier)
                    texts.append(text)
                    complete = complete and whole
                return ";\n".join(texts), complete
            return self._once(("text", id(binder), node.id, unknown, earlier), lambda key: (earlier, False), bound)
        return unknown, False


def _names_in(target):
    """Every name a target binds (``a, (b, *c)`` binds three)."""
    return [node.id for node in ast.walk(target) if isinstance(node, ast.Name)]


def _constant_text(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value else None
