# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What the names in a file stand for: scopes as Python resolves them, what a value certainly is, and the text of a string."""
import ast
import textwrap

import pytest

from invaract_lint.resolver import CONSOLE, MEMORY, PATH, TEMPORARY, Names


def last(source):
    """The scopes of ``source`` and its last expression statement's value (wherever it is nested)."""
    tree = ast.parse(textwrap.dedent(source))
    expressions = [n for n in ast.walk(tree) if isinstance(n, ast.Expr)]
    node = max(expressions, key=lambda n: (n.lineno, n.col_offset)).value
    while isinstance(node, (ast.Lambda, ast.ListComp)):  # what a lambda returns, what a comprehension collects
        node = node.body if isinstance(node, ast.Lambda) else node.elt
    return Names(tree), node


def qualify(source):
    names, node = last(source)
    return names.qualify(node)


@pytest.mark.parametrize("source,expected", [
    ("import pandas\npandas.read_csv", ["pandas.read_csv"]),
    ("import pandas as pd\npd.read_csv", ["pandas.read_csv"]),
    ("import pyarrow.parquet\npyarrow.parquet.write_table", ["pyarrow.parquet.write_table"]),
    ("import pyarrow.parquet as pq\npq.write_table", ["pyarrow.parquet.write_table"]),
    ("from pandas import read_csv\nread_csv", ["pandas.read_csv"]),
    ("from pandas import read_csv as rc\nrc", ["pandas.read_csv"]),
    ("from os import path as p\np.join", ["os.path.join"]),
    ("from . import pandas\npandas.read_csv", []),
    ("from .pandas import read_csv\nread_csv", []),
    ("from pandas import *\nread_csv", ["pandas.read_csv"]),
    ("from pandas import *\nconcat", []),
    ("from gzip import *\nopen", ["gzip.open"]),
    ("def f():\n    from gzip import *\ndef g():\n    open", ["open"]),
    ("open", ["open"]),
    ("print", []),
    ("__builtins__.open", ["builtins.open"]),
    ("unknown.read_csv", []),
    ("import pandas as pd\nr = pd.read_csv\nr", ["pandas.read_csv"]),
    ("import pandas as pd\nr = pd.read_csv\ns = r\ns", ["pandas.read_csv"]),
    ("import pandas as pd\nr: object = pd.read_csv\nr", ["pandas.read_csv"]),
    ("import pandas as pd\nr: object\nr", []),
    ("import pandas as pd\n(r := pd.read_csv)", ["pandas.read_csv"]),
    ("import pandas as pd\n[(r := pd.read_csv) for _ in range(1)]\nr", ["pandas.read_csv"]),
    ("import pandas as pd\npd.read_csv.__call__", ["pandas.read_csv"]),
    ("import pandas as pd\ngetattr(pd, 'read_csv')", ["pandas.read_csv"]),
    ("import pandas as pd\ngetattr(pd, name)", []),
    ("import pandas as pd\ngetattr(pd, '')", []),
    ("import pandas as pd\ngetattr(pd)", []),
    ("import importlib\nimportlib.import_module('pyarrow.parquet')", ["pyarrow.parquet"]),
    ("import importlib\nimportlib.import_module('pyarrow.parquet').write_table", ["pyarrow.parquet.write_table"]),
    ("import importlib\nimportlib.import_module(name)", []),
    ("import importlib\nimportlib.import_module()", []),
    ("__import__('pyarrow.parquet')", ["pyarrow"]),
    ("__import__(name)", []),
    ("import pandas as pd\npd.read_csv('a')", []),
    # every binding in the binding scope counts
    ("try:\n    import pandas as pd\nexcept ImportError:\n    pd = None\npd.read_csv", ["pandas.read_csv"]),
    ("import pandas as pd\nimport polars as pd\npd.read_csv", ["pandas.read_csv", "polars.read_csv"]),
    ("import pandas as pd\nimport pandas as pd\npd.read_csv", ["pandas.read_csv"]),
    # the nearest scope that binds the name decides
    ("import pandas as pd\ndef f(pd):\n    pd.read_csv", []),
    ("import pandas as pd\ndef f(*pd):\n    pd.read_csv", []),
    ("import pandas as pd\ndef f(**pd):\n    pd.read_csv", []),
    ("import pandas as pd\ndef f(*, pd):\n    pd.read_csv", []),
    ("import pandas as pd\ndef f(pd, /):\n    pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    pd = make()\n    pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    for pd in x:\n        pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    with x as pd:\n        pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    a, pd = x\n    pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    pd += 1\n    pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    try:\n        pass\n    except E as pd:\n        pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    def pd():\n        pass\n    pd.read_csv", []),
    ("import pandas as pd\ndef f():\n    class pd:\n        pass\n    pd.read_csv", []),
    ("import pandas as pd\ndef f(x):\n    match x:\n        case [pd]:\n            pd.read_csv", []),
    ("import pandas as pd\ndef f(x):\n    match x:\n        case [1, *pd]:\n            pd.read_csv", []),
    ("import pandas as pd\ndef f(x):\n    match x:\n        case {'a': 1, **pd}:\n            pd.read_csv", []),
    ("import pandas as pd\nlambda pd: pd.read_csv", []),
    ("import pandas as pd\nlambda *, pd: pd.read_csv", []),
    ("import pandas as pd\n[pd.read_csv for pd in x]", []),
    ("import pandas as pd\ndef f():\n    pd.read_csv", ["pandas.read_csv"]),
    ("import pandas as pd\ndef f():\n    def g():\n        pd.read_csv", ["pandas.read_csv"]),
    ("import pandas as pd\nlambda x: pd.read_csv", ["pandas.read_csv"]),
    ("import pandas as pd\n[pd.read_csv for p in x]", ["pandas.read_csv"]),
    ("def f():\n    import pandas as pd\n    pd.read_csv", ["pandas.read_csv"]),
    ("def f():\n    import pandas as pd\ndef g():\n    pd.read_csv", []),
    ("def f():\n    import pandas as pd\n    def g():\n        pd.read_csv", ["pandas.read_csv"]),
    # a class body's names are not seen from its methods, but are in the body
    ("import pandas as pd\nclass A:\n    pd = None\n    def f(self):\n        pd.read_csv", ["pandas.read_csv"]),
    ("class A:\n    import pandas as pd\n    def f(self):\n        pd.read_csv", []),
    ("class A:\n    import pandas as pd\n    pd.read_csv", ["pandas.read_csv"]),
    # global and nonlocal bind in the scope they name
    ("def setup():\n    global pd\n    import pandas as pd\ndef f():\n    pd.read_csv", ["pandas.read_csv"]),
    ("def outer():\n    def setup():\n        global pd\n        import pandas as pd\ndef f():\n    pd.read_csv", ["pandas.read_csv"]),
    ("def outer():\n    pd = None\n    def setup():\n        nonlocal pd\n        import pandas as pd\n    pd.read_csv", ["pandas.read_csv"]),
    ("def outer():\n    pd = None\n    class A:\n        def setup(self):\n            nonlocal pd\n            import pandas as pd\n    pd.read_csv",
     ["pandas.read_csv"]),
    ("def setup():\n    if x:\n        global pd\n    import pandas as pd\ndef f():\n    pd.read_csv", ["pandas.read_csv"]),
    ("def setup():\n    def inner():\n        global pd\n    import pandas as pd\ndef f():\n    pd.read_csv", []),
    # decorators, defaults and annotations are read where the function is defined
    ("import pandas as pd\ndef f(pd=pd.read_csv):\n    pass\n", None),
])
def test_qualify(source, expected):
    if expected is None:
        tree = ast.parse(source)
        default = tree.body[1].args.defaults[0]
        assert Names(tree).qualify(default) == ["pandas.read_csv"]
    else:
        assert qualify(source) == expected


def test_a_long_chain_of_names_is_followed():
    chain = "import pandas as a0\n" + "".join(f"a{i + 1} = a{i}\n" for i in range(40))
    assert qualify(chain + "a40.read_csv") == ["pandas.read_csv"]


@pytest.mark.parametrize("source,expected", [
    ("a = a\na.read_csv", []),
    ("import pandas as pd\npd = pd\npd.read_csv", ["pandas.read_csv"]),
    ("import pandas as a\na = b\nb = a\nb.read_csv", ["pandas.read_csv"]),
    ("import pandas as a\na = b\nb = a\na.read_csv", ["pandas.read_csv"]),
    ("import pandas as a\nimport polars as b\na = b\nb = a\nb.read_csv", ["polars.read_csv", "pandas.read_csv"]),
    ("import pandas as pd\nframe = pd\nframe = frame.merge(other)\nframe.read_csv", ["pandas.read_csv"]),
])
def test_names_bound_in_terms_of_themselves_or_each_other(source, expected):
    assert qualify(source) == expected


def test_a_name_asked_for_twice_gives_the_same_answer():
    names, node = last("import pandas as a\na = b\nb = a\nc = b\nc.read_csv")
    assert names.qualify(node) == names.qualify(node) == ["pandas.read_csv"]
    # asked first for b (through c), which comes back to a: what was worked out for a on the way is not kept as final
    tree = ast.parse("import pandas as a\na = b\nb = a\nb.read_csv\na.read_csv")
    fresh = Names(tree)
    assert [fresh.qualify(statement.value) for statement in tree.body[3:]] == [["pandas.read_csv"], ["pandas.read_csv"]]


def test_a_name_rebound_many_times_is_worked_out_once():
    """``frame = frame.merge(...)`` a few hundred times over is ordinary code: following it must not take longer for it."""
    import time
    source = ("import pandas as pd\nimport io\nframe = pd.DataFrame()\nsql = 'select 1'\nbuf = io.BytesIO()\n"
              + "frame = frame.merge(other)\nsql = sql + ' and x'\nsql += ' or y'\nbuf = buf if ok else buf\n" * 400
              + "frame.to_parquet(buf)\ncon.execute(sql)")
    tree = ast.parse(source)
    started = time.perf_counter()
    names = Names(tree)
    call = tree.body[-2].value
    assert names.qualify(call.func) == [] and names.kind(call.args[0]) is None
    text, complete = names.text(tree.body[-1].value.args[0], unknown="?", earlier="+")
    assert not complete and text.startswith("select 1;\n+ and x;\n+ or y")
    assert time.perf_counter() - started < 2


def test_what_is_read_where_a_function_is_defined():
    tree = ast.parse("import pandas as pd\n@pd.read_csv\ndef f(pd: pd.DataFrame = pd.read_json, *, k=pd.read_table) -> pd.Series:\n    pass\n"
                     "g = lambda pd=pd.read_excel, *, k=pd.read_orc: pd\nclass C(pd.DataFrame, metaclass=pd.Meta):\n    pass")
    names = Names(tree)
    f, g, c = tree.body[1], tree.body[2].value, tree.body[3]
    outside = [f.decorator_list[0], f.args.args[0].annotation, f.args.defaults[0], f.args.kw_defaults[0], f.returns,
               g.args.defaults[0], g.args.kw_defaults[0], c.bases[0], c.keywords[0].value]
    assert [names.qualify(n)[0] for n in outside] == [
        "pandas.read_csv", "pandas.DataFrame", "pandas.read_json", "pandas.read_table", "pandas.Series",
        "pandas.read_excel", "pandas.read_orc", "pandas.DataFrame", "pandas.Meta"]
    assert names.qualify(g.body) == []  # the lambda's own parameter


@pytest.mark.parametrize("source,path", [
    ("x", ()),
    ("def f():\n    x", ("f",)),
    ("async def f():\n    x", ("f",)),
    ("class A:\n    def f(self):\n        x", ("A", "f")),
    ("class A:\n    class B:\n        x", ("A", "B")),
    ("def f():\n    lambda: [x for _ in y]", ("f", "<lambda>")),
    ("def f():\n    [x for _ in y]", ("f",)),
])
def test_the_scope_a_node_is_in(source, path):
    names, node = last(source)
    assert names.scope_of[id(node)].path() == path


def kind(source):
    names, node = last(source)
    return names.kind(node)


@pytest.mark.parametrize("source,expected", [
    ("import io\nio.BytesIO()", MEMORY),
    ("import io\nio.StringIO('x')", MEMORY),
    ("from io import BytesIO as B\nB()", MEMORY),
    ("import pyarrow as pa\npa.BufferOutputStream()", MEMORY),
    ("import pyarrow as pa\npa.BufferReader(b)", MEMORY),
    ("import pyarrow as pa\npa.py_buffer(b)", MEMORY),
    ("b'abc'", MEMORY),
    ("'abc'", None),
    ("1", None),
    ("import io\nbuf = io.BytesIO()\nbuf", MEMORY),
    ("import io\nbuf = io.BytesIO()\nbuf.getvalue()", MEMORY),
    ("import io\nbuf = io.BytesIO()\nbuf.getbuffer()", MEMORY),
    ("import io\nbuf = io.BytesIO()\nbuf.read()", MEMORY),
    ("import pyarrow as pa\npa.BufferOutputStream().getvalue().to_pybytes()", MEMORY),
    ("import io\nbuf = io.BytesIO()\nbuf.name", None),
    ("import io\nbuf = io.BytesIO()\nbuf.fileno()", None),
    ("import io\nbuf = io.BytesIO()\nbuf = 'out.parquet'\nbuf", None),
    ("import io\nbuf = io.BytesIO()\nbuf = io.StringIO()\nbuf", MEMORY),
    ("import io\nbuf = io.BytesIO()\ndef f(buf):\n    buf", None),
    ("import io\ndef f():\n    buf = io.BytesIO()\ndef g(buf):\n    buf", None),
    ("import io\nwith io.BytesIO() as buf:\n    buf", MEMORY),
    ("import io\nfor buf in [io.BytesIO()]:\n    buf", None),
    ("import io\na, buf = io.BytesIO(), 1\nbuf", None),
    ("buf", None),
    ("import sys\nsys.stdout", CONSOLE),
    ("import sys\nsys.stderr", CONSOLE),
    ("import sys\nsys.stdout.buffer", CONSOLE),
    ("import sys\nsys.__stderr__", CONSOLE),
    ("from sys import stdout\nstdout", None),  # a bare name is only followed through an assignment's value
    ("import sys\nout = sys.stdout\nout", CONSOLE),
    ("import sys\nsys.stdin", None),
    ("import sys\nsys.stdout.encoding", None),
    ("import tempfile\ntempfile.mkdtemp()", TEMPORARY),
    ("import tempfile\ntempfile.gettempdir()", TEMPORARY),
    ("import tempfile\ntempfile.NamedTemporaryFile()", TEMPORARY),
    ("import tempfile\ntempfile.NamedTemporaryFile(dir='out')", None),
    ("import tempfile\ntempfile.NamedTemporaryFile(suffix='.csv')", TEMPORARY),
    ("import tempfile\ntempfile.NamedTemporaryFile().name", TEMPORARY),
    ("import tempfile\ntempfile.NamedTemporaryFile().mode", None),
    ("import tempfile\nwith tempfile.TemporaryDirectory() as d:\n    d", TEMPORARY),
    ("import tempfile\nfd, path = tempfile.mkstemp()\npath", TEMPORARY),
    ("fd, path = other()\npath", None),
    ("import tempfile\nfrom pathlib import Path\nPath(tempfile.mkdtemp())", TEMPORARY),
    ("import tempfile\nfrom pathlib import Path\nPath(tempfile.mkdtemp()) / 'a'", TEMPORARY),
    ("import tempfile\nfrom pathlib import Path\n(Path(tempfile.mkdtemp()) / 'a').parent", TEMPORARY),
    ("import tempfile\nfrom pathlib import Path\n(Path(tempfile.mkdtemp()) / 'a').with_suffix('.b')", TEMPORARY),
    ("import tempfile\nfrom pathlib import Path\n(Path(tempfile.mkdtemp()) / 'a').read_text()", None),
    ("import tempfile\nfrom pathlib import Path\n'a' / Path(tempfile.mkdtemp())", None),
    ("import tempfile\nfrom pathlib import Path\nPath('out') / tempfile.mkdtemp()", PATH),
    ("import tempfile\nfrom pathlib import Path\nPath(tempfile.mkdtemp()) + 'a'", None),
    ("import os, tempfile\nos.path.join(tempfile.mkdtemp(), 'a')", TEMPORARY),
    ("import os, tempfile\nos.path.join('out', tempfile.mkdtemp())", None),
    ("import os, tempfile\nos.path.abspath(tempfile.mkdtemp())", TEMPORARY),
    ("import os, tempfile\nos.path.realpath(tempfile.mkdtemp())", TEMPORARY),
    ("import os, tempfile\nos.path.normpath(tempfile.mkdtemp())", TEMPORARY),
    ("import os, tempfile\nos.fspath(tempfile.mkdtemp())", TEMPORARY),
    ("import os, tempfile\nos.path.basename(tempfile.mkdtemp())", None),
    ("import tempfile\nstr(tempfile.mkdtemp())", TEMPORARY),
    ("str('out')", None),
    ("import tempfile\nwork = tempfile.mkdtemp()\nf'{work}/a.txt'", TEMPORARY),
    ("import tempfile\nwork = tempfile.mkdtemp()\nf'out/{work}'", None),
    ("f''", None),
    ("f'{other}/a'", None),
    ("from pathlib import Path\nPath('a')", PATH),
    ("import pathlib\npathlib.PurePosixPath('a')", PATH),
    ("from pathlib import Path\nPath.cwd()", PATH),
    ("from pathlib import Path\nPath.home() / 'a'", PATH),
    ("from pathlib import Path\nPath('a').parent", PATH),
    ("from pathlib import Path\nPath('a').name", None),
    ("from pathlib import Path\nPath('a').resolve()", PATH),
    ("from pathlib import Path\nPath('a').joinpath('b')", PATH),
    ("from pathlib import Path\nPath('a').read_text()", None),
    ("from pathlib import Path\np = Path('a')\np", PATH),
    ("import duckdb\nduckdb.connect()", "duckdb"),
    ("import sqlite3\nsqlite3.connect(':memory:')", "sqlite"),
    ("import sqlite3\ncon = sqlite3.connect(':memory:')\ncon.cursor()", "sqlite"),
    ("import duckdb\ncon = duckdb.connect()\ncon.cursor()", "duckdb"),
    ("import duckdb\ncon = duckdb.connect()\ncon.fetchall()", None),
    ("import psycopg\npsycopg.connect(dsn)", None),
    ("import psycopg\npsycopg.connect(dsn).cursor()", None),
    ("x if y else z", None),
])
def test_kind(source, expected):
    assert kind(source) == expected


def test_a_long_chain_of_values_is_followed():
    chain = "import io\na0 = io.BytesIO()\n" + "".join(f"a{i + 1} = a{i}\n" for i in range(20))
    assert kind(chain + "a20") == MEMORY


@pytest.mark.parametrize("source,expected", [
    # bound in terms of itself: what the other bindings make it, if that holds for every binding
    ("import tempfile\nfrom pathlib import Path\nwork = Path(tempfile.mkdtemp())\nwork = work / 'part'\nwork", TEMPORARY),
    ("import tempfile\nfrom pathlib import Path\nwork = Path(tempfile.mkdtemp())\nwork /= 'part'\nwork", TEMPORARY),
    ("import os, tempfile\nwork = tempfile.mkdtemp()\nwork = os.path.join(work, 'part')\nwork", TEMPORARY),
    ("from pathlib import Path\nout = Path('out')\nout = out / 'part'\nout = out.resolve()\nout", PATH),
    ("import tempfile\nwork = tempfile.mkdtemp()\nwork = work + '/part'\nwork", None),
    ("import tempfile\nwork = tempfile.mkdtemp()\nwork = work / 'part'\nwork = 'data/out'\nwork", None),
    ("import os, tempfile\nfrom pathlib import Path\nwork = Path('out')\nwork = os.path.join(work, 'part')\nwork", None),
    ("work = work / 'part'\nwork", None),
    ("work /= 'part'\nwork", None),
    # in terms of each other: not followed, so of no kind (a finding, never a write missed)
    ("import tempfile\nfrom pathlib import Path\na = Path('out')\nb = a / 'x'\nb = tempfile.mkdtemp()\na = b\nb", None),
    ("import tempfile\nfrom pathlib import Path\na = Path('out')\nb = a / 'x'\nb = tempfile.mkdtemp()\na = b\na", None),
])
def test_the_kind_of_a_name_bound_in_terms_of_itself(source, expected):
    assert kind(source) == expected


def test_the_kind_of_names_bound_in_terms_of_each_other_does_not_depend_on_which_is_asked_first():
    source = "import tempfile\nfrom pathlib import Path\na = Path('out')\nb = a / 'x'\nb = tempfile.mkdtemp()\na = b\n"
    for order in (("a", "b"), ("b", "a")):
        tree = ast.parse(source + "\n".join(order))
        names = Names(tree)
        assert [names.kind(statement.value) for statement in tree.body[-2:]] == [None, None]


def text(source, **gaps):
    names, node = last(source)
    return names.text(node, **gaps)


@pytest.mark.parametrize("source,expected", [
    ("'abc'", ("abc", True)),
    ("b'abc'", ("?", False)),
    ("1", ("?", False)),
    ("x", ("?", False)),
    ("f'a{x}b'", ("a?b", False)),
    ("f'a{1}b{y!r:>3}'", ("a?b?", False)),
    ("f'ab'", ("ab", False)),
    ("'a' + 'b'", ("ab", True)),
    ("'a' + x", ("a?", False)),
    ("x + 'b'", ("?b", False)),
    ("'a' + x + 'c'", ("a?c", False)),
    ("'a' - 'b'", ("?", False)),
    ("'a %s' % x", ("a %s", False)),
    ("'a {}'.format(x)", ("a {}", False)),
    ("'a'.upper()", ("?", False)),
    ("make('a')", ("?", False)),
    ("from sqlalchemy import text\ntext('select 1')", ("select 1", True)),
    ("import sqlalchemy as sa\nsa.text('select ' + x)", ("select ?", False)),
    ("from sqlalchemy import text\ntext()", ("?", False)),
    ("from other import context\ncontext('select 1')", ("?", False)),
    ("q = 'select 1'\nq", ("select 1", True)),
    ("q = 'select 1'\nq = 'select 2'\nq", ("select 1;\nselect 2", True)),
    ("q = 'select 1'\nq = other\nq", ("select 1;\n?", False)),
    ("q = 'select 1'\nfor q in x:\n    q", ("?", False)),
    ("q = 'select 1'\ndef f(q):\n    q", ("?", False)),
    ("q = 'select 1'\ndef f():\n    q", ("select 1", True)),
    ("q = 'a'\nr = q + 'b'\nr", ("ab", True)),
])
def test_text(source, expected):
    assert text(source, unknown="?") == expected


def test_text_leaves_out_what_is_not_spelled_out_unless_asked_where():
    assert text("f'to_{x}'") == ("to_", False)
    assert text("x") == ("", False)


def test_a_long_chain_of_strings_is_followed():
    chain = "a0 = 'x'\n" + "".join(f"a{i + 1} = a{i}\n" for i in range(20))
    assert text(chain + "a20") == ("x", True)


@pytest.mark.parametrize("source,expected", [
    ("q = 'select 1'\nq = q + ' where x'\nq", ("select 1;\n+ where x", False)),
    ("q = 'select 1'\nq += ' where x'\nq", ("select 1;\n+ where x", False)),
    ("q = 'select 1'\nq += extra\nq", ("select 1;\n+?", False)),
    ("q += ' where x'\nq", ("+ where x", False)),
    ("q = q\nq", ("+", False)),
    ("a = 'x' + b\nb = 'y' + a\na", ("xy+", False)),
])
def test_the_text_of_a_name_built_up_from_itself(source, expected):
    assert text(source, unknown="?", earlier="+") == expected


def test_where_its_earlier_text_goes_is_unknown_unless_said_otherwise():
    assert text("q = 'a'\nq += 'b'\nq", unknown="?") == ("a;\n?b", False)
    assert text("q = 'a'\nq += 'b'\nq") == ("a;\nb", False)


def test_an_expression_nested_too_deep_to_follow_is_not_spelled_out():
    deep = "x = " + " + ".join(["'a'"] * 80) + "\nx"
    assert text(deep, unknown="?") == ("??" + "a" * 59, False)
    assert qualify("import pandas as pd\n" + "pd" + ".a" * 80) == []
    assert kind("import tempfile\n" + "(" * 70 + "tempfile.mkdtemp()" + " / 'a')" * 70) is None


@pytest.mark.parametrize("source,expected", [
    ("import helpers\nhelpers", "helpers"),
    ("import a.b\na", "a"),
    ("import a.b as c\nc", "a.b"),
    ("from a import b\nb", None),
    ("import helpers\nhelpers = 1\nhelpers", None),
    ("import helpers\nimport other as helpers\nhelpers", None),
    ("import helpers\nimport helpers\nhelpers", "helpers"),
    ("helpers", None),
    ("import helpers\nhelpers.sub", None),
    ("import helpers\ndef f(helpers):\n    helpers", None),
])
def test_plain_module(source, expected):
    names, node = last(source)
    assert names.plain_module(node) == expected
