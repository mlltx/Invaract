# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A run: the checker started at its beginning, every read and write described to it as it is made, each output staged
(beside its location, or uncommitted in its Delta or Iceberg table), and at the end every staged output published, or, if
anything was refused or the job failed, none."""
import collections
import contextlib
import functools
import importlib.metadata
import os
import platform
import sys
import threading
import time
import uuid

from . import _checks, _csv, _datasets, _frames, _outputs, _pandas, _protocol
from ._checker import Checker, command
from ._settings import from_environment
from .errors import CheckNotRun, ContractViolation

SAVE_MODES = ("overwrite", "append", "error", "ignore")
#: The data libraries whose installed versions every event's ``job.engineDetails`` lists, by the name each is installed as.
LIBRARIES = ("pyarrow", "pandas", "polars", "duckdb", "deltalake", "pyiceberg", "fastavro", "openpyxl",
             "adbc-driver-postgresql", "adbc-driver-sqlite")
#: The save mode of a write whose contract declares none: what the pandas or Polars call it replaces does.
DEFAULT_SAVE_MODE = "overwrite"

_current = None
_starting = threading.Lock()


class Run:
    """One run of a job: a context manager (``with invaract.run():``), or what ``@invaract.job`` wraps the entry point in."""

    def __init__(self, contract=None, name=None, checking=False):
        self._default_contract = contract
        #: a check (``invaract.check``): its own checker, no events, nothing staged or published, and no run of its own
        self.checking = checking
        self.name = name or _script_name()
        self.run_id = uuid.uuid4().hex[:16]
        self.mode = None
        self.contract = None
        self._failure = None
        self._outputs = {}
        #: each output's rows staged, and seconds spent staging and publishing it, for its write event
        self._rows = collections.Counter()
        self._seconds = collections.Counter()
        self._asking = threading.Lock()
        self._checker = None

    # the run's beginning and end

    def __enter__(self):
        global _current
        if not self.checking:
            with _starting:
                if _current is not None:
                    raise RuntimeError("an Invaract run is already in progress in this process; runs do not nest")
                _current = self
        try:
            _exit_status_on_check_not_run()
            settings = from_environment(self._default_contract)
            if self.checking:
                # a check judges against the contract, and tells no one: it is the job's author's question, not a run
                settings = {k: v for k, v in settings.items() if k not in ("dryRun", "notifyConfig")}
            self._checker = Checker(command())
            reply = self._ask(_protocol.open_run(settings, self._job(), check=self.checking))
            if reply.get("protocol") != _protocol.VERSION:
                # a checker from another release (INVARACT_CHECKER) could read this package's requests otherwise than meant
                raise CheckNotRun(f"Invaract's checker ({self._checker.describe()}) speaks protocol version "
                                  f"{reply.get('protocol', 'unknown')}, and this package version {_protocol.VERSION}: install the "
                                  f"invaract package and its checker from the same release")
            self.mode, self.contract = reply["mode"], reply.get("contract")
        except BaseException:
            self._end_unpublished()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.checking:
            self._end_unpublished()
            return False
        if exc_type is not None or self._failure is not None:
            # the job failed, or swallowed a refusal: nothing is published, and the run ends refused
            self._end_unpublished()
            if exc_type is None:
                raise self._failure
            return False
        try:
            if self._ask(_protocol.finish(completed=True)).get("publish"):
                self._publish()
        finally:
            self._end_unpublished(finished=True)
        return False

    def _end_unpublished(self, finished=False):
        """Discards what is still staged and lets the checker deliver its events and stop. Every step is taken even if an
        earlier one fails, and a failure is raised only once they all have been, so one output that cannot be discarded
        leaves neither the others' staged data nor the checker behind."""
        with contextlib.ExitStack() as steps:
            # taken last to first: each output discarded, in the order first written, then the checker, then the run ended
            steps.callback(self._release)
            if self._checker is not None:
                steps.callback(self._stop_checker, finished)
            for out in reversed(list(self._outputs.values())):
                steps.callback(out.discard)

    def _stop_checker(self, finished):
        if not finished:
            try:
                self._checker.ask(_protocol.finish(completed=False))
            except CheckNotRun:
                pass
        self._checker.close()

    def _release(self):
        global _current
        if _current is self:
            _current = None

    # reads and writes

    @property
    def dry_run(self):
        return self.mode == "dry-run"

    def read(self, name, as_, location, fmt, dtype_backend=None, chunksize=None, csv=None):
        """``invaract.read``: the input ``name`` described to the checker, then handed over as the frame ``as_`` names."""
        _check_read_arguments(as_, dtype_backend, chunksize)
        declared = self._declared("inputs", name)
        where = self._where(declared, location)
        if where is None:
            # a name the contract does not declare, or a dry run with nowhere to read: the checker refuses it
            self._ask(_protocol.read(name, location, schema=[]))
            raise CheckNotRun(f"Invaract has nowhere to read '{name}' from")
        # the job's format if it names one, else the contract's: what is reported is what is read
        fmt = (fmt or declared.get("format") or _datasets.format_of(where)).lower()
        used = _csv_options(name, fmt, declared, csv)
        describe = functools.partial(_protocol.read, name, location, fmt, catalog=_catalog(where, fmt), csv=used)
        if declared.get("format") and declared["format"].lower() != fmt:
            # refused on its description, before anything is read: the checker is never told columns the package did not read
            self._ask(describe())
            raise CheckNotRun(f"Invaract's checker did not refuse reading '{name}' as {fmt}, which its contract does not declare")
        fields, partition_by = declared.get("fields", []), declared.get("partitionBy", [])
        if chunksize is not None:
            # the checker is told the input's empty values before any of it is handed over: one pass counts them, a
            # second reads the chunks, and neither holds the whole input in memory
            schema, batches = _datasets.read_batches(where, fmt, fields, partition_by, used)
            self._ask(describe(schema=_protocol.columns(schema, _frames.count_nulls(schema, batches))))
            return (_frames.as_frame(chunk, as_, dtype_backend) for chunk in _frames.chunks(schema, batches(), chunksize))
        table = _datasets.read_table(where, fmt, fields, partition_by, used)
        nulls = {n: table.column(n).null_count for n in table.column_names}
        self._ask(describe(schema=_protocol.columns(table.schema, nulls)))
        return _frames.as_frame(table, as_, dtype_backend)

    def write(self, frame, name, location, fmt, save_mode, partition_by=None, partition_overwrite=None, csv=None):
        """``invaract.write``: ``frame`` described to the checker, staged, and its empty values (and data checks) reported."""
        declared = self._declared("outputs", name)
        # a pandas frame is written as the contract declares where pandas cannot hold it, and no value changes (_pandas)
        described = _frames.stream(frame, declared.get("fields", []), dry_run=self.dry_run)
        if described is None:
            kind = f"{type(frame).__module__}.{type(frame).__qualname__}"
            self._ask(_protocol.unverifiable_write(name, f"a {kind} offers no Arrow stream (__arrow_c_stream__)", location))
            raise CheckNotRun(f"Invaract could not describe the table handed to invaract.write('{name}')")
        schema, batches, notes = described
        target = self._target(declared, location, fmt, save_mode, partition_by, partition_overwrite)
        used = _csv_options(name, target.format.lower(), declared, csv)
        with _noted(notes):
            # the pre-check, before anything is staged: everything but the empty values, which only the data shows
            self._ask(_protocol.write(name, location, target.format, target.mode, _protocol.columns(schema), target.layout,
                                      _catalog(target.where, target.format), used))
        out = self._output(name, target, used)
        required = {f["name"] for f in declared.get("fields", []) if f.get("required")}
        # with runtimeDataQuality on, the checker sent this output's data checks: each batch is evaluated as it is staged
        checks = _checks.Evaluation(declared["checks"]) if "checks" in declared else None
        started = time.monotonic()
        nulls = out.stage(schema, self._counted(name, checks.observe(batches) if checks else batches), required)
        self._seconds[name] += time.monotonic() - started
        with _noted(notes):
            self._ask(_protocol.written(name, nulls, checks.results() if checks else None))

    def _target(self, declared, location, fmt, save_mode, partition_by, partition_overwrite):
        """Where and how a write goes: what the job says (``location=``, ``format=``, ...), else what the contract declares,
        else the defaults. It is what the checker is told, so a job that says otherwise than its contract is refused."""
        where = self._where(declared, location)
        fmt = fmt or declared.get("format") or (_datasets.format_of(where) if where is not None else _datasets.DEFAULT_FORMAT)
        mode = save_mode or declared.get("saveMode") or DEFAULT_SAVE_MODE
        keys = partition_by if partition_by is not None else declared.get("partitionBy", [])
        overwrite = partition_overwrite or declared.get("partitionOverwriteMode") or "static"
        return _Target(where, fmt, mode, _outputs.Layout(tuple(keys), overwrite.lower()))

    def _output(self, name, target, csv):
        """The output ``name`` stages into: made by its first write, and the same for every later one, which must share its
        layout. A write the package cannot make (no location, a format or save mode it does not write, data already where
        the save mode is ``error``) is not made."""
        if target.where is None:
            raise CheckNotRun(f"Invaract has nowhere to write '{name}'")
        fmt, mode = target.format.lower(), target.mode.lower()
        if fmt not in _datasets.FORMATS or mode not in SAVE_MODES:
            raise CheckNotRun(f"Invaract cannot yet write '{name}' as {fmt} with save mode {mode}: it writes {', '.join(_datasets.FORMATS)}, "
                              f"with save mode {', '.join(SAVE_MODES)}")
        out = self._outputs.get(name)
        if out is None:
            out = self._outputs[name] = (_Checked(target.where, mode, target.layout) if self.checking
                                         else _datasets.output(target.where, fmt, mode, self.run_id, target.layout, csv))
        if out.layout != target.layout:
            raise CheckNotRun(f"invaract.write('{name}') is {target.layout.describe()}, but an earlier write of it in this run is "
                              f"{out.layout.describe()}: one output has one layout")
        if out.mode == "error" and out.has_data():
            raise FileExistsError(f"invaract.write('{name}'): its save mode is error, and {out.target} already holds data")
        return out

    # talking to the checker

    def _ask(self, request):
        """Sends ``request`` (``_protocol``) and returns the checker's reply; a refusal raises ``ContractViolation``, a request
        that could not be checked ``CheckNotRun``, and either one stops the run from publishing anything."""
        with self._asking:
            reply = self._checker.ask(request)
        if reply.get("ok"):
            return reply
        if reply.get("status") == "refused":
            failure = ContractViolation(reply.get("violations", []), reply.get("explanation", ""))
        else:
            failure = CheckNotRun(f"Invaract could not check '{request['op']}': {reply.get('reason', reply)}")
        self._failure = self._failure or failure
        raise failure

    def _counted(self, name, batches):
        """``batches``, each one's rows counted toward the output ``name``."""
        for batch in batches:
            self._rows[name] += batch.num_rows
            yield batch

    def _declared(self, side, name):
        """The dataset the contract declares as ``name`` among its ``inputs`` or ``outputs``; ``{}`` when it declares none."""
        return next((d for d in (self.contract or {}).get(side, []) if d["name"] == name), {})

    def _where(self, declared, location):
        """Where a read or write goes: the job's ``location=`` in a dry run; else the declared dataset's location (the job's
        own is only reported, so a location that disagrees with the contract is refused, never followed)."""
        return location if self.dry_run else declared.get("location")

    def _job(self):
        """The job, as every event names it (``job``): its name, this run's id, the Python version, and the installed version
        of each data library it could use (``LIBRARIES``), found without importing any of them."""
        return {"name": self.name, "runId": self.run_id, "pythonVersion": platform.python_version(), "libraries": _installed()}

    # publishing

    def _publish(self):
        """Every staged output put in place, in the order first written, and the checker told what each put there. An
        ``error`` output that has gained data since it was written stops the publish before anything moves. If an output
        fails to publish, those already published are put back where their storage can undo it (``Output.restore``), the
        checker is told what stays published, and the failure is raised."""
        published = {}
        try:
            for out in self._outputs.values():
                if out.mode == "error" and out.has_data():
                    raise FileExistsError(f"Invaract published nothing: the save mode of {out.target} is error, and it already holds data")
            for name, out in self._outputs.items():
                started = time.monotonic()
                result = out.publish()
                self._seconds[name] += time.monotonic() - started
                if result is not None:
                    published[name] = result
        except BaseException:
            for name in reversed(list(published)):
                if self._outputs[name].restore():
                    del published[name]
            with contextlib.suppress(CheckNotRun, ContractViolation):
                # the publish's own failure is the one to raise
                self._tell_published(published)
            raise
        self._tell_published(published)
        for out in self._outputs.values():
            out.forget_previous()

    def _tell_published(self, published):
        self._ask(_protocol.published(
            (name, self._rows[name], round(self._seconds[name] * 1000), result) for name, result in published.items()))


#: Where and how one write goes (``Run._target``): its location, format, save mode and partition layout.
_Target = collections.namedtuple("_Target", ["where", "format", "mode", "layout"])


def _check_read_arguments(as_, dtype_backend, chunksize):
    if as_ not in _frames.FRAME_KINDS:
        raise ValueError(f"invaract.read cannot return as_={as_!r}: give one of {', '.join(_frames.FRAME_KINDS)}")
    _pandas.check_dtype_backend(dtype_backend, as_)
    if chunksize is not None and (isinstance(chunksize, bool) or not isinstance(chunksize, int) or chunksize < 1):
        raise ValueError(f"invaract.read takes chunksize as a number of rows, 1 or more, not {chunksize!r}")


def _csv_options(name, fmt, declared, given):
    """The CSV options a read or write of ``name`` uses (``_csv.options``): none for a dataset that is not CSV, where giving
    some is a mistake worth saying."""
    if fmt != "csv":
        if given:
            raise ValueError(f"csv= is for a CSV dataset, and '{name}' is {fmt}")
        return {}
    return _csv.options(declared.get("csv"), given)


class _Checked(_outputs.Output):
    """What a check "writes" to: nothing. Staging only counts the empty values, which the checker judges."""

    def has_data(self):
        return False

    def stage(self, schema, batches, required):
        return _frames.take(schema, _frames.with_required(schema, required), batches, required, lambda batch: None)

    def publish(self):
        return None

    def discard(self):
        pass


def check(frame, name, contract=None, csv=None):
    """``invaract.check``: ``frame`` judged as a write of the output ``name`` would be, then nothing written."""
    if contract is None and _current is not None:
        # inside a run: the run's own contract, as the job named it
        contract = _current._default_contract
    with Run(contract, f"invaract.check('{name}')", checking=True) as run:
        if run.mode != "enforce":
            raise CheckNotRun("invaract.check needs a contract: name one, or set INVARACT_CONTRACT")
        run.write(frame, name, None, None, None, csv=csv)


@contextlib.contextmanager
def _noted(notes):
    """Adds ``notes`` (what the package knows of how pandas held the frame) to a refusal raised inside."""
    try:
        yield
    except ContractViolation as refused:
        if notes:
            refused.add_notes("In pandas", notes)
        raise


def _catalog(location, fmt):
    """The catalog a request says a catalog table is registered in (``_datasets.catalog_of``), or ``None``."""
    return _datasets.catalog_of(location, fmt) if location is not None else None


def _installed():
    versions = {}
    for library in LIBRARIES:
        try:
            versions[library] = importlib.metadata.version(library)
        except importlib.metadata.PackageNotFoundError:
            pass
    return versions


def _script_name():
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    if spec is not None and spec.name:
        return spec.name
    return os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else "python"


def _function_name(fn):
    """``fn`` as a job is named: its module and name (``orders_job.main``); a script's own function, whose module is
    ``__main__``, by the script's name instead (``main.main`` for ``python main.py``)."""
    module = fn.__module__
    if module == "__main__":
        script = _script_name()
        module = script[:-len(".py")] if script.endswith(".py") else script
    return f"{module}.{fn.__qualname__}"


def _exit_status_on_check_not_run():
    """An uncaught ``CheckNotRun`` ends the process with exit status 2, so a scheduler can tell "refused" (1) from "not
    checked" (2). Python ends with 1 for every uncaught exception, so the status is set from ``sys.excepthook``, after the
    traceback is printed as usual."""
    previous = sys.excepthook
    if getattr(previous, "_invaract", False):
        return

    def hook(kind, value, tb):
        previous(kind, value, tb)
        if isinstance(value, CheckNotRun):
            raise SystemExit(CheckNotRun.exit_status)

    hook._invaract = True
    sys.excepthook = hook


def current(verb):
    """The run in progress, for ``invaract.read`` and ``invaract.write``."""
    if _current is None:
        raise RuntimeError(f"invaract.{verb} needs a run: decorate the job's entry point with @invaract.job(), or use 'with invaract.run():'")
    return _current


def job(contract=None, *, name=None):
    """Decorates a job's entry point: each call is one run (``@invaract.job()``, ``@invaract.job("contract.yaml")``, or bare
    ``@invaract.job``). ``contract`` is the default contract (a path, or ``registry://<id>@<version>``);
    ``INVARACT_CONTRACT`` overrides it. ``name`` names the job in its events (by default, the entry point's module and
    name). The run publishes what the job wrote when the function returns, and nothing if it raises."""
    if callable(contract) and not isinstance(contract, (str, bytes, os.PathLike)):
        return job()(contract)

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with Run(contract, name or _function_name(fn)):
                return fn(*args, **kwargs)

        return wrapper

    return decorate
