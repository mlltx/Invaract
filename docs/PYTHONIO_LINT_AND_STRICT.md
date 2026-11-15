# Working on the Lint and Strict Mode

How to add a library, a function or an edge case to the two guards of the Python adapter: `invaract-lint`, which reads a
job's source, and strict mode (`INVARACT_STRICT`), which watches the job as it runs. What each is for and how it was
built is in [PYTHONIO_ADAPTER.md](PYTHONIO_ADAPTER.md); this page is the recipe.

## The one rule

**What counts as a read, a write or a connection is written once, in the lint's catalogue**
(`pythonio-adapter/lint/invaract_lint/catalogue.py`). The scan matches the names in a job's source against it. Strict mode
asks it which functions of a loaded library to wrap. So a line added there is covered by both, and the two cannot
disagree.

Nearly every change is one line in the catalogue and one line in each of two test corpora. If you find yourself writing
logic, check the tables below first.

## Where things are

| You want to... | Change | Test it in |
|---|---|---|
| Report a library's reader, writer or `connect` | `MODULE_RULES` in the catalogue: add the name to the rule's pattern | `lint/tests/test_evasions.py`, `python/tests/strict_matrix.py` |
| Report a function that opens a file by name and mode | `OPENERS` (where its mode is, and the letters that write); `DATA_OPENERS` too if opening it to read is a read of data | the same two |
| Report a method of a frame that writes | `FRAME_WRITES` | the same two |
| Watch a library that writes from compiled code | the above, then add its top-level name to `COMPILED` | `python/tests/strict_matrix.py` |
| Watch a database driver | `DRIVERS` | `strict_matrix.py` (needs the driver installed) |
| Name the parameter a reader or writer takes its path in | `SOURCES` or `DESTINATIONS` | `test_strict_unit.py` fails and names it when a library adds one |
| Stop a false alarm | the catalogue's "not data at rest" entries (`IN_MEMORY`, `CONSOLE`, `TEMPORARY`), or the scan's reading of that call | `lint/tests/test_legitimate.py`, `LEGITIMATE` in `strict_matrix.py` |
| Watch something Python reports by an audit event | `EVENTS` in `python/invaract/_strict/events.py` | `test_strict_unit.py` |
| Watch a function no catalogue rule describes | `OTHERS` in `python/invaract/_strict/wrapping.py` | `strict_matrix.py` |
| Give a function's call a reading of its own in the scan | `_SPECIAL` in `lint/invaract_lint/checker.py` | `test_evasions.py` |

## Adding a library: the steps

Say the library is `fastframes`, with `fastframes.read_table(path)` and `fastframes.write_table(frame, path)`, written in
Rust.

1. **Install it** in the environment you test in. Strict mode's tests run real jobs, and a case whose library is missing
   is skipped, not passed.
2. **Name what it does** in the catalogue's `MODULE_RULES`: add `fastframes\.read_\w+` to the `library-read` pattern
   and `fastframes\.write_\w+` to `library-write`. A pattern must match the whole qualified name.
3. **Say it is compiled**: add `"fastframes"` to `COMPILED`, and to `DATA_LIBRARIES` and `DYNAMIC_ROOTS` (so
   `getattr(fastframes, name)` is reported as code the scan cannot follow). A library that writes through Python's own
   `open` needs none of this for strict mode: Python reports its writes itself.
4. **Check its parameter names**: if the path is not in a parameter the catalogue already names (`path`, `source`,
   `where`, ...), add the name to `SOURCES` or `DESTINATIONS`. `test_strict_unit.py` tells you which.
5. **Add the cases**, one line each:
   - `lint/tests/test_evasions.py`: `case("fastframes", "import fastframes\nfastframes.write_table(f, 'a')", (WRITE, "library-write"))`
   - `python/tests/strict_matrix.py`: `T("fastframes.write_table", "import fastframes\nfastframes.write_table(df, p)", LIBRARY_WRITE, LIBRARY_WRITE, needs="fastframes")`
     and one for the reader, with `effect=False` (a read leaves no file to check for).
   - a line in `LEGITIMATE_WITH` for what the library does that must raise nothing (a write to a buffer, a read of a
     scratch file).
6. **Run them**: `pytest lint/tests` and `pytest python/tests/test_strict.py -k fastframes`. The matrix tests say what
   each layer reported for the case, so a wrong expectation reads off the failure.

## Adding an edge case

An edge case is a way of writing a read or write that one of the layers might miss, or ordinary code one might wrongly
report.

1. **Write it as a case first**, in the corpus it belongs to, and run it. Most new cases already pass: the point of the
   corpora is that they are cheap to grow.
   - A way to write around the package: `test_evasions.py` (the scan) and a `T(...)` in `strict_matrix.py` (a real job).
   - Ordinary code: `test_legitimate.py` and `LEGITIMATE`.
2. **If the scan misses it**, the fix is almost always in `resolver.py` (the scan did not know what a name stood for) or
   the catalogue (it did not know the function). `checker.py` changes only when a call needs a reading of its own.
3. **If strict mode misses it**, ask how the write reached the disk:
   - through Python's `open`: it should have been seen. Check `placing.place` (whose doing it was taken to be) and the
     watcher's scratch folders.
   - through compiled code: the function is not wrapped. Add it to the catalogue.
   - through something neither layer can see: write it down. A `T(...)` with `runtime=None` and a `why` is how the
     matrix records what strict mode cannot see, and the test that checks the list of those names is the reviewed
     record of the gaps.
4. **Every case must be caught by at least one layer.** `test_no_technique_gets_past_both_the_scan_and_strict_mode`
   fails otherwise.

## What a matrix case says

```python
T("polars.write_parquet", "import polars as pl\npl.from_arrow(table).write_parquet(p)", FRAME, FRAME, needs="polars")
```

The name, the code (it writes to `p`, a path that is the case's own), the rule strict mode must report it by, the rule
the scan must report it by, and the library it needs. `None` for a rule means that layer cannot see it. `blocked=False`
says enforcing does not stop the write (it is seen only after it happened); `effect=False` says it leaves no file behind
(a read, a connection); `before=` is module-level code that runs before `import invaract`.

Each case is run in two real jobs, one reporting and one enforcing, and held to all of it: what each layer said, that
the write happened when reported, and that it did not when enforced.

## How strict mode is laid out

`pythonio-adapter/python/invaract/_strict/`, each module small enough to read in one sitting:

| Module | What it does |
|---|---|
| `events.py` | Python's audit events, each read as what the job did. `EVENTS` is the table. |
| `judges.py` | What a call of a wrapped function is about to do, from its arguments: one judge per shape of call (a writer, a reader, an opener, SQL, a `connect`). |
| `wrapping.py` | Installs the hooks and wraps what the catalogue names, in every library loaded and every library imported later. |
| `placing.py` | Whose doing something is (the package's, the job's, a library's) and where in the job's code. |
| `watcher.py` | One watch: leaves out what is no finding, keeps one finding per call site, refuses when enforced. |
| `_state.py` | What the parts share: who is watching now. |

A wrapper never leads to the function it wraps (it has no `__wrapped__`), so code that unwraps functions as a matter of
course does not step around the watch. `wrapping.original_of` is how the package's own tests get to it.

## What cannot be closed from inside Python

Know these before promising more than the guards give. Each is pinned by a test, and each is reported some other way:

- **Another program, once started.** Starting it is reported (`IVR005`); what it then reads or writes is its own.
- **The job's own compiled code.** Loading it is reported (`IVR006`); what it does is not seen.
- **Code that digs a library's own function out of the wrapper that watches it**, or out of the process's memory. The
  scan reports the digging (`tampering`), and a run whose watch was switched off says so and, enforced, is not checked.
- **What the job does before `import invaract` and after its run ends.** The scan reads that code like any other.

## Gates

- `pytest pythonio-adapter/lint/tests` and `pytest pythonio-adapter/python/tests`, on the oldest and newest Python the
  package supports (CI runs both).
- `mutmut run` in `pythonio-adapter/lint`: the floor is 80% killed.
- A change to what a user sees (a new category, a new setting, a library now reported) needs the user guide
  (`docs-site/src/content/docs/guides/using-invaract-with-python.mdx`) updated in the same change.
