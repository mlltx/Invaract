# Python Jobs: Attaching Invaract Without the Annotation (Design)

**Status:** design, not implemented. Decisions D-2, R-6b and L-1 confirmed by the owner (2026-11-15, section 15). **Scope:** `pythonio-adapter/python` (the `invaract` package), its wheel, its docs.
No Scala change in phases 1 to 3. **Builds on:** strict mode and the scan as they are after PR #232
([PYTHONIO_ADAPTER.md](PYTHONIO_ADAPTER.md), [PYTHONIO_LINT_AND_STRICT.md](PYTHONIO_LINT_AND_STRICT.md)).

This document is written to be handed to an implementer (a person or a model) as it stands. Sections 1 to 4 are the
human summary. Sections 5 to 12 are the specification: every rule has an identifier (`D-` decision, `R-` requirement,
`S-` scenario, `T-` test, `L-` limit), and the tables are normative. Section 13 is the evidence each mechanism rests on.

---

## 1. The problem

Everything Invaract does for a Python job starts when the job opens a run (`@invaract.job()` or `with invaract.run():`).
A job that forgets to, or never imports the package at all, is not checked, not scanned, not watched, and nothing says
so. Measured on the branch of PR #232, with `INVARACT_STRICT=enforce`, `INVARACT_LINT=enforce` and `INVARACT_CONTRACT`
all set:

| Job | Exit status | File written | Events |
|---|---|---|---|
| never imports `invaract` | 0 | yes | none |
| imports `invaract`, opens no run | 0 | yes | none |

So a forgotten annotation fails open and silent. The guards are sound once a run exists; nothing guarantees one does.

## 2. What this design adds

Three capabilities, each switched on by the platform through the environment, none needing a change to a job's source:

1. **Start-up attach.** Invaract attaches when the Python interpreter starts (a `.pth` file the wheel ships), or when a
   launcher runs the job (`python -m invaract job.py`), instead of when the job imports the package. The watch then
   covers the whole program, and Invaract knows a job process exists even if the job never mentions it.
2. **Require a run** (`INVARACT_REQUIRE_RUN=off|report|enforce`). A job that touches data and ends without ever opening a
   run is reported, or fails. The annotation stays the way a run is opened; forgetting it stops being silent.
3. **Auto-run** (`INVARACT_AUTO_RUN=true`). Invaract opens the run for the job. `invaract.read` and `invaract.write` work
   with no annotation, and a job that reads or writes around the package is reported or refused exactly as inside an
   annotated run.

They compose into a ladder an organisation can climb one step at a time (section 4).

## 3. The idea in one page

**A job process** is a Python process whose main program is the job's own code (not `pip`, not `pytest`, not a library's
entry point). The platform can widen or narrow that with patterns.

**Attaching** means: the watch is collecting from the first line of the program, and a handler runs when the process
ends. Nothing else happens yet. A process that never touches data is left completely alone: no checker starts, no event
is published, no contract is needed. This is what makes it safe to switch on for a whole environment.

**A run** then comes to exist in one of two ways:

- the job opens it (the annotation), as today; or
- with auto-run, Invaract opens an **implicit run** the first time one is needed: the job calls `invaract.read` or
  `invaract.write`; or something must be refused; or the process is ending and there is something to report. An implicit
  run lasts until the process ends.

**When the process ends**, the handler settles the account: it closes an implicit run (publishing its outputs if the job
completed), reports what happened outside any run, and, if the platform requires a run and there was none, says so and
fails the job.

Two principles decide every detail below:

- **P-1. Report modes observe.** With every mode at `report`, no job's outcome, outputs or publish timing changes.
- **P-2. Enforce modes fail closed, and loudly.** When Invaract cannot check, the job ends with a non-zero status and a
  message that says what to do.

## 4. The scenarios, from an organisation's point of view

Who decides what: the **platform team** controls the environment (the image, the variables, sometimes the start
command). The **job team** owns the source. Every switch here is the platform's; a job's code cannot lower one.

| # | Posture | What the organisation wants | Platform sets | Job team does | What happens |
|---|---|---|---|---|---|
| S-0 | Not adopted | Nothing | nothing | nothing | Nothing. The `.pth` costs one environment lookup per process. |
| S-1 | Team opt-in | Teams that want Invaract use it | nothing (or modes) | annotates, uses `invaract.read`/`write` | Today's behaviour. |
| S-2 | Team opt-in, watched from the start | The same, with no blind spot before the import or after the run | `INVARACT_STRICT=report` or `enforce` | as S-1 | The watch starts with the interpreter. What the job does before `import invaract` and after its run ends is seen. |
| S-3 | Discover | An inventory of what every job reads and writes, before asking anyone to change anything | `INVARACT_AUTO_RUN=true`, `INVARACT_DRY_RUN=true` | nothing | Each job that touches data publishes one `CODE_SCAN` event listing its reads and writes. No job fails, no contract is needed. |
| S-4 | Recommended | Using Invaract is expected; not using it is visible | `INVARACT_REQUIRE_RUN=report` | annotates (or sees a warning) | A job that touches data without a run prints a warning naming what it touched. Outcome unchanged. |
| S-5 | Mandatory annotation | Every job that touches data must be an Invaract job | `INVARACT_REQUIRE_RUN=enforce` | annotates | A job that touches data without a run ends with exit status 2. |
| S-6 | Platform-governed, observing | Every job runs under its contract with no source change; violations are visible | `INVARACT_AUTO_RUN=true`, `INVARACT_CONTRACT=<per job>`, `INVARACT_STRICT=report`, `INVARACT_LINT=report` | nothing required; may use `invaract.read`/`write` with no annotation | Every job that touches data has a run. Reads and writes around the package are reported. |
| S-7 | Platform-governed, enforcing | The same, and data moves only through Invaract | as S-6 with `INVARACT_STRICT=enforce`, `INVARACT_LINT=enforce` | uses `invaract.read`/`write` (no annotation needed) | A read or write around the package is refused where it happens. |
| S-8 | Mixed fleet | Some teams annotate, the rest are caught by the net | as S-6 or S-7 | either | An annotated run behaves exactly as written. Anything outside it is handled by an implicit run. |
| S-9 | Command-controlled | The platform wraps the start command and will not ship a start-up hook | start command `python -m invaract job.py`, plus any of the above | nothing | The same as with the hook, with exact knowledge of how the job ended (section 9). |
| S-10 | Shaping what counts as a job | Framework entry points count; some scripts do not | `INVARACT_ATTACH_INCLUDE`, `INVARACT_ATTACH_EXCLUDE` | nothing | Section 6. |

**The recommended ladder:** S-3 (see what there is) → S-4 (make the gap visible) → S-6 (govern without touching source) →
S-7 (enforce). S-5 is the alternative to S-6 for organisations that want the annotation in every job's source as an
explicit statement.

**What a job team can and cannot do.** A team can always opt *in* (annotate; S-1). A team cannot opt *out* of what the
platform switched on: the settings are environment variables the job does not own, and code that reaches into the
package to undo them is reported by the scan (`tampering`). This is governance against mistakes and omissions. A job
written by its own author to defeat it (unsetting the variables and re-executing itself, starting Python with `-S`) is
stopped only by what the process is allowed to do, as the user guide already says of strict mode.

---

## 5. Settings (normative)

All five are the package's own variables: declared in `invaract/_settings.py` beside `STRICT`, never forwarded to the
checker.

| Variable | Values (default first) | Meaning |
|---|---|---|
| `INVARACT_ATTACH` | `startup`, `import` | When Invaract attaches. `startup`: at interpreter start, through the wheel's `.pth` (active only when R-1 holds). `import`: only when the job imports the package (today's behaviour; the `.pth` does nothing). |
| `INVARACT_ATTACH_INCLUDE` | empty | Comma-separated patterns (`fnmatch`). Main programs that count as jobs **in addition to** the default (D-3). `*` means every Python process. |
| `INVARACT_ATTACH_EXCLUDE` | empty | Comma-separated patterns. Main programs that never count as jobs. Wins over include and over the default. |
| `INVARACT_REQUIRE_RUN` | `off`, `report`, `enforce` | What happens when a job process touched data and ended without ever opening a run (R-6). |
| `INVARACT_AUTO_RUN` | `false`, `true` | Whether Invaract opens an implicit run for a job process that needs one and has none (R-7). |

**R-1 (when the start-up hook is active).** The `.pth` attaches only when `INVARACT_ATTACH` is not `import` **and** at
least one of these holds: `INVARACT_STRICT` is `report` or `enforce`; `INVARACT_REQUIRE_RUN` is `report` or `enforce`;
`INVARACT_AUTO_RUN` is `true`. Otherwise it returns at once.

**R-2 (the implied watch).** With `INVARACT_AUTO_RUN=true` or `INVARACT_REQUIRE_RUN` not `off`, an **unset**
`INVARACT_STRICT` behaves as `report` everywhere (attach, annotated runs, implicit runs). An **explicit**
`INVARACT_STRICT=off` is honoured, with these consequences, which the user guide must state:

| | With the watch (`report`/`enforce`, or implied) | With `INVARACT_STRICT=off` explicitly |
|---|---|---|
| `REQUIRE_RUN` applies to | job processes that **touched data** (D-5) | **every** job process (Invaract cannot tell which touched data) |
| `AUTO_RUN` opens a run when | section 8, triggers T1 to T3 | only T1 (the job calls `invaract.read`/`write`) |

**R-3 (invalid values).** An unrecognised value of any of the five is never ignored. A run that opens raises
`CheckNotRun` naming the variable (as `INVARACT_STRICT` does today). A job process that ends without a run prints the
same message and ends with status 2 (subject to R-12).

**R-4 (dependencies).** The watch needs `invaract-lint` (its catalogue), as today. If it is missing, R-3's treatment
applies: `CheckNotRun` at a run's opening; at process end without a run, the message and status 2.

**D-1.** `INVARACT_AUTO_RUN=true` with `INVARACT_REQUIRE_RUN` set: auto-run decides. Every job process that needs a run
gets one, so require-run has nothing left to judge. Not an error.

**D-2 (default on; confirmed by the owner).** `INVARACT_ATTACH` defaults to `startup`. A platform that already sets `INVARACT_STRICT` gets the
earlier watch on upgrade: under `enforce`, a job that wrote before `import invaract` and passed before is now refused.
That is the gap closing, and it must be in the release notes. `INVARACT_ATTACH=import` restores the old behaviour.

## 6. Which processes are jobs (normative)

**D-3 (default).** A process is a **job process** when its main program is:

| Started as | Counts as a job by default |
|---|---|
| `python path/to/script.py` | yes, if the script is the job's own code: its real path is not under Python's or an installed package's folders (the existing rule, `_codescan._not_the_jobs()`). A console script in `venv/bin` therefore counts. |
| `python -m package.module` | yes, if the **top-level package's** location is the job's own by the same rule. Resolve only the top level (`importlib.util.find_spec(name.split(".")[0])`): resolving the full dotted name would import the job's packages before the watch is on. |
| `python -c "..."`, `python -` (code on standard input) | yes. Code with no file is the job's. |
| interactive `python` | no |
| a library's entry point (`python -m pip`, `pytest`, `gunicorn`, `airflow`, `jupyter`) | no |
| `python -m invaract <program>` (the launcher) | yes, always, whatever the patterns say: the platform named it. |

**R-5 (patterns).** Each pattern is matched with `fnmatch` against every name the program has: for a script, its real
path, the path as given, and its base name; for a module, its dotted name; for `-c`, the literal `-c`; for standard
input, `-`. Decision order: exclude match → not a job; include match → a job; else D-3.

**Guidance the user guide must give (S-10):**

- A job **installed as a package** and run with `python -m its_package` is not a job by default (it lives beside the
  libraries). Name it: `INVARACT_ATTACH_INCLUDE=its_package*`.
- Jobs run **by a framework's entry point** (Airflow tasks, Celery workers, Papermill): name the entry point, or
  annotate the jobs. The watch places findings by the file the code is in, so task code in the job's own files is
  still attributed correctly.
- A **launcher script that starts jobs as separate processes** should be excluded
  (`INVARACT_ATTACH_EXCLUDE=run_all.py`), so each job it starts is its own job process (see R-11).

**D-4 (root and children).** A job process with no `INVARACT_STRICT_SPILL` in its environment is a **root**. A root
creates its spill file when it attaches and exports `INVARACT_STRICT_SPILL` for the life of the process, so every Python
process it starts reports to it, as a forked or spawned child already reports to a run today. Consequences:

- One job is one process tree and at most one implicit run, in the root.
- A child never opens an implicit run and is never judged by require-run. `invaract.read`/`write` in a child with no run
  of its own raises the existing `RuntimeError` (extended to say the run is held by the parent process).
- A child may still open its **own annotated run** (a job started as a subprocess of another); that works as today.
- The spill file now belongs to the root process, not to one run: a run gathers from it when it stops, and the
  end-of-process handler gathers what is left. (Today `Watcher.start` creates it per run; move it.)

**D-5 ("touched data").** A job process touched data when its watcher, after gathering its children's spill, holds at
least one finding that no `# invaract: allow` marker and no `INVARACT_STRICT_ALLOW` pattern explains. The package's own
reads and writes, scratch files, library-owned files and system probes are not findings, as today.

## 7. Attach: the two stages (normative)

The start-up path must cost almost nothing in a process that is not a job, and must never break interpreter start-up.

**Stage 1: `_invaract_attach.py`** (new, top-level module in the wheel, standard library only) loaded by
**`invaract_attach.pth`** (new, one line: `import _invaract_attach`).

```
on import:
    if not R-1: return
    sys.addaudithook(first)

first(event, args):                      # must never raise
    if armed is False: return            # one comparison per audit event afterwards
    if event not in {cpython.run_file, cpython.run_module, cpython.run_command,
                     cpython.run_stdin, cpython.run_interactivehook, cpython.run_startup}: return
    armed = False
    program = identify(event, args)      # kind + name, per D-3
    if is_job(program):                  # D-3 and R-5; standard library only
        import invaract._attach          # stage 2: the first heavy import, in job processes only
        invaract._attach.begin(program, point="startup")
```

- **R-8.** Everything in stage 1 is wrapped: on any failure it prints one line to standard error
  (`Invaract could not attach to this process (<why>): it is not watched`), sets `INVARACT_ATTACH_FAILED=<why>` in the
  process's environment, and returns. A run that opens later with that variable set raises `CheckNotRun` when any
  enforce mode is on. Stage 1 never exits the process: a fault in it must not be able to break `pip` in the same
  environment. (This leaves L-4.)
- Stage 1 must not import `invaract`: that import costs about 150 ms (it loads pyarrow), and stage 1 runs in every
  Python process of the environment.

**Stage 2: `invaract/_attach.py`** (new). `begin(program, point)` is idempotent and is called from three places: stage 1
(`point="startup"`), the launcher (`point="launcher"`), and `invaract/__init__.py` at import when no earlier attach
happened (`point="import"`, replacing today's direct `_strict.install_early()` call). It:

1. Works out the effective settings (R-2, R-3).
2. Decides root or child (D-4); a root creates and exports the spill file.
3. Starts the collecting watcher if the watch is on (`_strict.install_early`, which already exists).
4. Registers the end-of-process handler with `atexit`. Registered at attach, it runs **after** every handler the job
   registers, so what a job does in its own `atexit` hooks is watched and settled too.
5. In hook mode, arms completion detection (section 9).

With `point="import"` and no `.pth` (a source checkout, or `INVARACT_ATTACH=import`), require-run and auto-run still
work for a job that imports the package; a job that never does is invisible, as today. The user guide must say so.

**The launcher: `invaract/__main__.py`** (new). `python -m invaract [-m module | -c code | script] [args...]`:
sets `sys.argv`, calls `begin(program, point="launcher")`, runs the program with `runpy` (`run_path`, or `run_module`
with `alter_sys=True`, both as `__main__`) inside `try/except BaseException`, hands the outcome to the same
end-of-process logic, and exits with the resulting status. It must not be named `invaract/run.py`: `invaract.run` is the
public function, and a submodule of that name would replace it.

## 8. Runs (normative)

### 8.1 Opening an implicit run

**R-7.** In a **root job process**, with `INVARACT_AUTO_RUN=true` and **no run open**, an implicit run opens when, and
only when, one of these happens:

| Trigger | When | Notes |
|---|---|---|
| **T1** | the job calls `invaract.read` or `invaract.write` | `_run.current()` opens it instead of raising. This is what makes the annotation optional. |
| **T2** | something must be refused: the watch is at `enforce` and records a finding nothing explains | The run opens inside the watcher, takes over the collected findings (the existing `_take_over`), and the checker's refusal is raised at the job's call, as in an annotated run. In `report` mode this trigger never fires (P-1). |
| **T3** | the process is ending, and there is something to report: at least one collected finding (explained or not), or, with `INVARACT_LINT` on, a scan of the job's loaded code that has findings or errors | Opened and closed by the end-of-process handler. |

Never otherwise. A job process that touches no data and whose code has nothing to report never starts a checker.

- An implicit run is `Run(implicit=True)`: opened with `__enter__` exactly as an annotated one (settings, contract
  resolution, the scan at its start, the watch), and left open.
- **Contract.** From `INVARACT_CONTRACT` (a path or `registry://`), or none with `INVARACT_DRY_RUN=true`. With neither,
  the opening fails with `CheckNotRun` ("auto-run needs a contract: set INVARACT_CONTRACT, or INVARACT_DRY_RUN=true to
  observe"). At T1 and T2 that is raised in the job; at T3 it is printed and the status is 2 (R-12).
- **How the run came to be is published.** Add two job attributes through the existing `jobAttributes` setting, so
  every event of the run carries them: `invaract.run` = `annotation` or `auto`, and `invaract.attach` = `startup`,
  `launcher` or `import`. A key the platform already set wins. (Verify the setting's syntax in `_settings.py` and
  `InvaractConf`; this needs no checker change.)

### 8.2 Annotated runs beside implicit ones

| Situation | Behaviour |
|---|---|
| The job opens an annotated run, no implicit run is open | Exactly today's behaviour, including publishing when the block ends. Findings collected before it are taken over, as today. |
| An annotated run has closed; later the job triggers T1, T2 or T3 | An implicit run opens then and lasts to the end of the process. Runs in one process are sequential, never nested. |
| The job opens an annotated run while an implicit run opened by **T1** is open | `CheckNotRun`: "this job reads or writes through Invaract outside its own run and then opens one: move those calls inside the run, or remove the annotation and let the platform's run cover the whole job". A job uses the ambient API or its own runs first, not ambient then annotated. |
| An implicit run was opened by **T2** (it refused) | The run is over, and the process is marked refused: every later attempt to open a run raises the kept refusal, and the end-of-process handler ends the process with status 1. |

This table is what guarantees P-1 for annotated jobs: in report mode T2 never fires, so an annotated job's run is never
preceded by an implicit one unless the job itself called the ambient API.

### 8.3 Closing: the end-of-process handler

Runs once, in the root, after the job's own `atexit` handlers. In order:

1. Gather the children's spill.
2. **An implicit run is open:** close it. If the job **completed** (section 9), `__exit__(None, None, None)`: the late
   scan, the watch's final report, publish. Otherwise the failure path: nothing is published. A `ContractViolation` or
   `CheckNotRun` raised while closing is printed as Python would print it, and sets the status (1 or 2).
3. **No run is open, auto-run on:** if T3's condition holds, open an implicit run and close it at once (it has nothing
   staged, so completion does not matter). Its events are the report.
4. **No run is open, auto-run off:**

   | A run was opened earlier in this process | Unexplained findings remain | `REQUIRE_RUN` | `STRICT` | Outcome |
   |---|---|---|---|---|
   | no | yes (or the watch is explicitly off, R-2) | `report` | any | **R-6a.** Warning on standard error. Status unchanged. |
   | no | yes (or the watch is explicitly off) | `enforce` | any | **R-6b (confirmed by the owner).** The same message. Status 2, "not checked". |
   | no | no | any | any | Nothing. |
   | yes | yes | any | `report` | **R-9a (the tail).** Warning: what the job did after its run ended. Status unchanged. |
   | yes | yes | any | `enforce` | **R-9b.** The same message. Status 1: it wrote around the package, after the run could refuse it. |
   | yes | no | any | any | Nothing. |

5. Remove the spill file.

**R-12 (who sets the exit status).** Invaract changes the exit status only of a process that would otherwise end with
0, and only to 1 (refused) or 2 (not checked): the statuses `ContractViolation` and `CheckNotRun` already have. A job
that failed by itself keeps its own status. With the launcher this is an ordinary `sys.exit`. With the hook, an
`atexit` handler cannot change the status by raising, so the handler flushes standard output and error and calls
`os._exit(status)`; it is the last handler to run, so nothing of the job's is skipped.

**Messages (the tests assert the phrases in bold).**

- R-6: `Invaract: this job read or wrote data and **opened no run**, so nothing checked it (INVARACT_REQUIRE_RUN=<mode>).`
  then up to five findings, one per line as `<path>:<line> <message>`, then `and <n> more` if any, then
  `Decorate the job's entry point with @invaract.job(), or use 'with invaract.run():'.`
- R-9: `Invaract: this job read or wrote data **after its run ended**, so nothing checked it (INVARACT_STRICT=<mode>).`
  then the findings in the same form.

## 9. Knowing how the job ended (normative)

An implicit run publishes only if the job completed. An annotated run knows (the `with` block sees the exception). The
launcher knows exactly (it wraps the program). The start-up hook has to work it out, and Python does not say how a
process is ending. What the probes in section 13 established:

| Signal | Available | Tells |
|---|---|---|
| `sys.last_value` is set (`sys.last_exc` from 3.12) | all versions | an uncaught exception was printed: **failed** |
| the job called `sys.exit(code)` (the hook wraps `sys.exit`; record calls made on the main thread) | all versions | the last call's code: `None` or `0` → **completed**, else **failed** |
| the main program's code object returned (`sys.monitoring`, local `PY_RETURN` on the code object seen in the first `exec` audit event that follows `cpython.run_*` and whose file is the main program's) | 3.12 and later | **completed** |
| none of the above | | 3.12 and later: the program ended by `raise SystemExit(...)` not made through `sys.exit` → **unknown**. 3.10 and 3.11: a normal end and a bare `raise SystemExit(...)` look the same → **completed**. |

**R-10 (decision, hook mode), in this order:** uncaught exception → failed. Main program returned → completed. `sys.exit`
recorded → by its code. Otherwise by the last row.

**R-11 (unknown).** If the outcome is unknown **and the implicit run has staged outputs**: nothing is published, and the
process ends with status 2 and the message "Invaract could not tell whether this job completed: end it with
sys.exit(<status>), or start it with python -m invaract". If nothing is staged, unknown is treated as completed (there is
nothing to publish; only reports are sent).

**L-1 (a limit, accepted by the owner; to be stated in the user guide).** On Python 3.10 and 3.11, with the start-up hook, a job that uses the
ambient API and ends with a bare `raise SystemExit(<non-zero>)` is taken to have completed, and its outputs are
published. `sys.exit(<non-zero>)`, any uncaught exception, Python 3.12 or later, and the launcher are all exact. Where
the platform controls the start command, the launcher is the recommended way to run auto-run jobs for this reason.

A process killed by a signal or ended with `os._exit` runs no handler: an open implicit run is never closed and nothing
is published. That is the safe direction, and the same as an annotated run killed mid-way.

## 10. What changes where (for the implementer)

| File | Change |
|---|---|
| `pythonio-adapter/python/_invaract_attach.py` (new) | Stage 1 (section 7). Standard library only. |
| `pythonio-adapter/python/invaract_attach.pth` (new) | `import _invaract_attach` |
| `pythonio-adapter/python/pyproject.toml`, wheel build (`.github/scripts/python_wheel.py`) | Ship both at the wheel's top level so they land in `site-packages`. A `build_py` step that copies the `.pth` into the build's library folder was shown to work (section 13). Add `py-modules = ["_invaract_attach"]`. |
| `invaract/_attach.py` (new) | Stage 2: effective settings, program identity and D-3/R-5, root/child and the spill file, completion detection, the end-of-process handler, opening implicit runs, R-6 and R-9. |
| `invaract/__main__.py` (new) | The launcher. |
| `invaract/__init__.py` | Call `_attach.begin(None, point="import")` instead of `_strict.install_early()`. |
| `invaract/_settings.py` | The five variables, documented as the package's own. |
| `invaract/_run.py` | `Run(implicit=...)`; `current()` opens on T1; the 8.2 rules; a count of runs opened in the process; the `RuntimeError` text for a child; reuse `_exit_status_on_check_not_run`. |
| `invaract/_strict/__init__.py`, `watcher.py` | Effective mode with the implied watch (R-2); the collecting watcher fires T2; the spill file owned by the root (D-4). |
| `tests/test_attach.py` (new), `tests/test_attach_unit.py` (new) | Section 11. |
| `tests/strict_matrix.py`, `tests/test_strict.py` | `written-as-the-job-is-imported` and `written-after-the-run` are now seen under start-up attach: add an attached run of those two cases and record what each layer says. |
| `.github/scripts/test_python_wheel.py`, `release-python.yml` | The installed wheel contains both files, and an installed-wheel job run shows the hook attaching (the only place the real `.pth` is exercised). |
| Docs | Section 12. |

**Do not:** import `invaract` from stage 1; raise from an audit hook on a `cpython.run_*` event (it aborts the
interpreter); name a module `invaract/run.py`; open a checker in a process that is not a root job process; change what
an annotated run does when no new variable is set (every existing test must pass unchanged).

**Order of work, each step shippable:**

1. **Phase 1: attach.** Stage 1, stage 2 without runs, the launcher as attach-only, R-9 (the tail). Delivers S-2 and
   S-9 for the watch.
2. **Phase 2: require-run.** R-6, D-5, R-2's table. Delivers S-4 and S-5.
3. **Phase 3: auto-run.** Section 8 and section 9. Delivers S-3, S-6, S-7, S-8.
4. **Phase 4 (optional, needs the checker): an event for a job with no run.** Today "not checked" outcomes publish no
   event, and R-6 follows that: its report is standard error and the exit status. For fleet dashboards under S-4/S-5 a
   new request (the package starts the checker only to say "this job touched data and opened no run", with the
   findings) would publish it. It needs the protocol, its schema, recorded cases and the native build. Until then, the
   documented way to get events for unannotated jobs is S-3 or S-6.

## 11. Tests (normative)

Real jobs as real processes, as `test_strict.py` does. The hook is exercised in the source tree through a folder on
`PYTHONPATH` holding a `sitecustomize.py` that imports `_invaract_attach` (it runs at the same point of start-up as a
`.pth`, which only works from an installed wheel); the wheel job exercises the real file. Run on Python 3.10 and 3.13.
Every job-level test runs **twice: through the hook and through the launcher**, unless it names one.

| Id | Test | Expect |
|---|---|---|
| T-01 | No variable set; the hook folder present | Byte-identical behaviour to no hook; `invaract` not imported (`sys.modules`). |
| T-02 | `STRICT=report`; job writes before `import invaract`, and in an `atexit` hook after its run | Both are findings ("seen before the run opened"; R-9a warning for the second). |
| T-03 | `STRICT=enforce`; job writes before `import invaract`, then opens a run | Refused as the run opens, status 1. |
| T-04 | `ATTACH=import` with T-02's job | The pre-import write is not seen (today's behaviour). |
| T-05 | Not a job: `python -m pip --version`, `pytest`-style library main, interactive | No attach: `invaract` not imported, no warning, status unchanged, even with `REQUIRE_RUN=enforce`. |
| T-06 | D-3 and R-5, unit: script in the job folder, script under site-packages, `-m` own package, `-m` installed package, `-c`, stdin; include and exclude patterns, exclude winning | The table in section 6. A dotted `-m` name imports nothing while deciding. |
| T-07 | `REQUIRE_RUN=report`; job never imports `invaract`, writes a file | R-6a message with the file; status 0. |
| T-08 | `REQUIRE_RUN=enforce`; same job | Message; status 2; the job's own `atexit` handlers ran. |
| T-09 | `REQUIRE_RUN=enforce`; job touches no data (prints, computes, writes scratch) | Nothing; status 0; no checker started. |
| T-10 | `REQUIRE_RUN=enforce`; annotated job | Exactly as without the variable. |
| T-11 | `REQUIRE_RUN=enforce`; job's write is explained (marker; `STRICT_ALLOW`) | Nothing; status 0. |
| T-12 | `REQUIRE_RUN=enforce`, `STRICT=off` explicitly | Every job process without a run ends 2, data or not (R-2). |
| T-13 | `REQUIRE_RUN=enforce`; job already failing with status 3 | Status stays 3 (R-12). |
| T-14 | `AUTO_RUN`, contract; job calls `invaract.read`/`write` with no annotation and ends normally | Published; events as an annotated run's, with `invaract.run=auto`. |
| T-15 | T-14's job ending by: uncaught exception; `sys.exit(3)`; `sys.exit(0)`; `raise SystemExit(4)` | Not published / not published / published / per section 9 for the Python version and attach point (hook on 3.12+: not published, status 2; hook on 3.10–3.11: published, L-1; launcher: not published). |
| T-16 | `AUTO_RUN`, contract, `STRICT=enforce`; job does a raw `pd.read_csv` | Refused at the call (T2); status 1; `CONTRACT_VALIDATION` `FAILED` event. |
| T-17 | `AUTO_RUN`, contract, `STRICT=report`; same job | Job completes; one run opened at the end (T3); `CODE_SCAN` `RUNTIME` lists the read. |
| T-18 | `AUTO_RUN`, `DRY_RUN`; unannotated job reading and writing raw files (S-3) | Status 0; one `CODE_SCAN` event listing them; no contract needed. |
| T-19 | `AUTO_RUN`, contract; job touches no data | No checker started, no events. |
| T-20 | `AUTO_RUN`, no contract, no dry run; job touches data | `CheckNotRun` message naming both variables; status 2. |
| T-21 | `AUTO_RUN`, `STRICT=report`; **annotated** job, including one with a raw write before its run | The annotated run publishes when its block ends, as without auto-run (P-1); the earlier write is in its report. |
| T-22 | `AUTO_RUN`; ambient `invaract.write`, then `with invaract.run()` | The `CheckNotRun` of 8.2. |
| T-23 | `AUTO_RUN`; annotated run, then ambient `invaract.write` after it | A second, implicit run; both publish. |
| T-24 | `AUTO_RUN`, `STRICT=enforce`; a child process (`subprocess`, `multiprocessing` spawn, fork) does the raw write | The child refuses locally; the root's run reports it; one run in all. |
| T-25 | `AUTO_RUN`; a child calls `invaract.read` | The `RuntimeError` naming the parent process. |
| T-26 | Stage 1 made to fail (a broken include pattern is not enough: inject a fault) | One line on standard error; the interpreter starts; `pip --version` works; a run under enforce raises `CheckNotRun`. |
| T-27 | An invalid value for each of the five variables | R-3. |
| T-28 | Overhead: start-up time of a non-job process with the hook active vs. absent | Within noise (section 13 measured an inert hook at a few percent on a hot loop). Report the numbers; do not gate on a tight threshold. |
| T-29 | Installed wheel (CI): `site-packages` holds both files; T-07 and T-14 pass with the real `.pth` | In `release-python.yml`'s install-and-run step. |

The lint's mutation floor does not apply (no lint change). The package has no mutation gate; write the unit tests for
`_attach` so that each comparison in sections 6, 8.3 and 9 is pinned on both sides and at its boundary.

## 12. Documentation (normative outline)

Per the repository's documentation policy this is user-facing and ships with the code. Every quoted output must come
from a real run.

- **User guide** (`guides/using-invaract-with-python.mdx`), new section **"Decide how Invaract attaches to your jobs"**,
  placed before "Watch what the job does as it runs":
  1. One paragraph: the annotation is one way a run comes to exist; the platform can require it, or open the run itself.
  2. The scenario table of section 4, in users' words, each row linking to a short subsection with the exact variables
     to set and what a job team will see (the real message or event).
  3. "What counts as a job" (section 6), with the three pieces of guidance.
  4. "The start-up hook and the launcher": what a `.pth` file is in two sentences, that it runs in every Python process
     of the environment and does nothing unless the variables are set, how to switch it off (`INVARACT_ATTACH=import`),
     and when to prefer the launcher.
  5. "Using `invaract.read` and `invaract.write` without the annotation" (auto-run): when outputs are published, and L-1.
  6. Update "What it cannot see": before `import invaract` and after the run are closed by start-up attach; add L-1 to L-4.
- **Configuration reference:** the five variables.
- **Troubleshooting:** "A job ends with status 2 and 'opened no run'"; "Invaract could not tell whether this job
  completed"; "auto-run needs a contract".
- **Contributor docs:** fold this design into `PYTHONIO_ADAPTER.md` as a dated section when implemented; add the attach
  path to `PYTHONIO_PACKAGE.md`'s module table.

### Limits to state plainly

- **L-1** Section 9.
- **L-2** `python -S` skips the hook; a job that removes the variables and re-executes itself escapes it. As for strict
  mode: mistakes are caught, a determined author is not.
- **L-3** A job installed as a package, or run by a framework's entry point, must be named (section 6) or annotated.
- **L-4** If stage 1 itself fails, the process is not watched and, if it opens no run, nothing fails: the line on
  standard error is the only sign. Stage 1 is kept to a few dozen lines of standard-library code for this reason.

## 13. Evidence

Probes run on this repository's test environments (Python 3.10.20, 3.11.17, 3.13.16):

| Question | Result |
|---|---|
| Does a `.pth` in a wheel run before any job code? | Yes for `python job.py`, `-m`, `-c`, `-I`; no for `-S`. Children inherit it through the environment. A `build_py` step copying the `.pth` into the wheel worked; `site-packages` held it after `pip install`. |
| Is there a hook at the moment the main program starts, with its identity? | Yes: audit events `cpython.run_file` (path), `cpython.run_module` (name), `cpython.run_command`, seen by a hook installed at start-up, on all three versions. |
| Is the main program's code object available? | Yes: the next `exec` audit event carries it (for `-m`, `runpy`'s own code comes first; match on the file). |
| Can an `atexit` handler tell an uncaught exception? | Yes: `sys.last_value` is set (all three). |
| Can it tell `sys.exit(3)` from a normal end? | Only by wrapping `sys.exit`. `sys.exc_info()` is empty; a bare `raise SystemExit(4)` leaves no trace. |
| `sys.monitoring` for the main program unwinding? | `PY_UNWIND` cannot be a local event (3.13: "invalid local event set"). `PY_RETURN` can; hence "the main program returned" as the positive signal. |
| Handler order | A handler registered at start-up runs after the job's own `atexit` handlers. |
| Cost of `import invaract` | About 150 ms over a bare start (21 ms → 176 ms): hence two stages. |
| Cost of an inert audit hook | `import pandas` +3%, `pip --version` within noise, a loop of a million calls and twenty thousand opens +7%. |
| What happens today | Section 1's table. |

## 14. Alternatives considered

| Alternative | Why not |
|---|---|
| Open the implicit run eagerly, when the program starts | Every Python process in the environment that counts as a job would start a checker and need a contract, including one-line helper commands. Lazy opening leaves processes that touch no data alone. |
| The `.pth` re-executes the process under the launcher | Exact completion everywhere, but it rebuilds the command line from `sys.orig_argv`, doubles start-up, and behaves differently on Windows. Too much magic at interpreter start. |
| Tracing (`sys.settrace`/`setprofile`) to see how the main program ends on 3.10/3.11 | A Python call per function call for the whole job. |
| Replacing `builtins.SystemExit` to see bare raises | Breaks `except SystemExit` for exits raised from C. |
| `sitecustomize.py` instead of a `.pth` | Collides with an environment's own `sitecustomize`. |
| Refuse locally (no checker) for accesses outside any run | Stronger for S-2, but a refusal with no event and no checker verdict; auto-run gives the same protection with both. |
| One enum (`manual`/`expect`/`require`/`auto`) instead of two variables | `off`/`report`/`enforce` is the vocabulary users already know from the scan and the watch. |
| Fall back to a dry run when auto-run has no contract | A forgotten contract would silently stop being enforced. Observing must be asked for (`INVARACT_DRY_RUN=true`). |

## 15. Decisions and open points

Decided by the owner on 2026-11-15:

| Point | Decision |
|---|---|
| D-2: start-up attach on by default | **On by default.** `INVARACT_ATTACH=import` restores the old behaviour; the release notes must say that `INVARACT_STRICT=enforce` now covers code before `import invaract`. |
| R-6b: exit status for a job with no run under `INVARACT_REQUIRE_RUN=enforce` | **2, "not checked"**, the status `CheckNotRun` already has. |
| L-1: a bare `raise SystemExit(<non-zero>)` under the hook on Python 3.10/3.11 | **Accepted and documented.** Ambient writes stay allowed under the hook on every supported version; the user guide recommends the launcher where the platform controls the start command. |

Still open (none blocks phases 1 to 3; an implementer takes the stated default):

1. **R-2:** the implied watch, and the blunt rule when the watch is explicitly off. Default: as written.
2. **Phase 4:** whether an event for "touched data, no run" is wanted soon enough to plan the protocol change now.
   Default: not in phases 1 to 3.
3. A per-job **contract map** (one environment, many jobs, each with its own contract) is not designed here: today the
   scheduler sets `INVARACT_CONTRACT` per job. Worth its own design if platforms cannot.

## 16. As built (2026-11-16)

All four phases are implemented (docs/PYTHONIO_ADAPTER.md, "Attaching without the annotation"). Where the build
departed from this design, the build is right and this section says so:

| Point | As designed | As built, and why |
|---|---|---|
| Open point 1 (R-2) | the implied watch, and the blunt rule with the watch off | As written. |
| Open point 2 (phase 4) | not in phases 1 to 3 | Built as phase 4: an `open` with `"observe": true` (docs/PYTHONIO_PROTOCOL.md). A dry-run open was not reused, because it publishes a `DRY_RUN_SUMMARY` that claims a draft. |
| D-3 | a console script in `venv/bin` counts as a job | It does not: the environment's `scripts` folder counts as a library's, since `pytest`, `pip` and every other installed command live there. A job installed as a command is named with `INVARACT_ATTACH_INCLUDE`. |
| S-6 | "observing" | The contract named is enforced; observing without one is S-3 (the dry run). The user guide's table says so. |
| Attributes (8.1) | `invaract.run` = `annotation` or `auto`, and `invaract.attach`, on every run's events | Only on runs Invaract opened (`auto`) and on the report of a job with no run (`none`); an annotated run's events are as before, so a platform tells them apart by the key's absence. |
| The `.pth` | `invaract_attach.pth` | `00_invaract_attach.pth`, so it sorts before most other start-up files. |
| Settings errors | at a run's opening | Also when `invaract.read` or `invaract.write` meets one outside any run: the job ends not checked (2), not with an uncaught traceback's 1. |
