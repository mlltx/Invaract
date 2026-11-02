# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A run: the checker started at its beginning, every read and write described to it as it is made, each output staged
beside its location, and at the end every staged output published, or, if anything was refused or the job failed, none."""
import functools
import os
import platform
import shutil
import sys
import threading
import uuid

import pyarrow as pa

from . import _frames
from ._checker import Checker, command
from ._settings import from_environment
from .errors import CheckNotRun, ContractViolation

SAVE_MODES = ("overwrite", "append", "error", "ignore")
FORMATS = ("parquet", "csv")
#: The save mode of a write whose contract declares none: what the pandas or Polars call it replaces does.
DEFAULT_SAVE_MODE = "overwrite"
DEFAULT_FORMAT = "parquet"

_current = None
_starting = threading.Lock()


class Run:
    """One run of a job: a context manager (``with invaract.run():``), or what ``@invaract.job`` wraps the entry point in."""

    def __init__(self, contract=None, name=None):
        self._default_contract = contract
        self.name = name or _script_name()
        self.run_id = uuid.uuid4().hex[:16]
        self.mode = None
        self.contract = None
        self._failure = None
        self._outputs = {}
        self._asking = threading.Lock()
        self._checker = None

    # the run's beginning and end

    def __enter__(self):
        global _current
        with _starting:
            if _current is not None:
                raise RuntimeError("an Invaract run is already in progress in this process; runs do not nest")
            _current = self
        try:
            _exit_status_on_check_not_run()
            self._checker = Checker(command())
            reply = self._ask({"op": "open", "settings": from_environment(self._default_contract), "job": self._job()})
            self.mode, self.contract = reply["mode"], reply.get("contract")
        except BaseException:
            self._end_unpublished()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None or self._failure is not None:
            # the job failed, or swallowed a refusal: nothing is published, and the run ends refused
            self._end_unpublished()
            if exc_type is None:
                raise self._failure
            return False
        try:
            if self._ask({"op": "finish", "completed": True}).get("publish"):
                self._publish()
        finally:
            self._end_unpublished(finished=True)
        return False

    def _end_unpublished(self, finished=False):
        """Discards what is still staged and lets the checker deliver its events and stop."""
        global _current
        try:
            for out in self._outputs.values():
                out.discard()
            if self._checker is not None:
                if not finished:
                    try:
                        self._checker.ask({"op": "finish", "completed": False})
                    except CheckNotRun:
                        pass
                self._checker.close()
        finally:
            _current = None

    # reads and writes

    @property
    def dry_run(self):
        return self.mode == "dry-run"

    def read(self, name, as_, location, fmt):
        if as_ not in _frames.FRAME_KINDS:
            raise ValueError(f"invaract.read cannot return as_={as_!r}: give one of {', '.join(_frames.FRAME_KINDS)}")
        declared = self._declared("inputs", name)
        request = _with_location({"op": "read", "name": name}, location)
        where = location if self.dry_run else declared.get("location")
        if where is None:
            # a name the contract does not declare, or a dry run with nowhere to read: the checker refuses it
            self._ask({**request, "schema": []})
            raise CheckNotRun(f"Invaract has nowhere to read '{name}' from")
        table = _frames.read_table(where, declared.get("format") or fmt or _format_of(where), declared.get("fields", []))
        nulls = {n: table.column(n).null_count for n in table.column_names}
        self._ask({**request, "schema": _frames.describe(table.schema, nulls)})
        return _frames.as_frame(table, as_)

    def write(self, frame, name, location, fmt, save_mode):
        declared = self._declared("outputs", name)
        request = _with_location({"op": "write", "name": name}, location)
        described = _frames.stream(frame)
        if described is None:
            kind = f"{type(frame).__module__}.{type(frame).__qualname__}"
            self._ask({**request, "unverifiable": f"a {kind} offers no Arrow stream (__arrow_c_stream__)"})
            raise CheckNotRun(f"Invaract could not describe the table handed to invaract.write('{name}')")
        schema, batches = described
        fmt = fmt or declared.get("format") or DEFAULT_FORMAT
        mode = save_mode or declared.get("saveMode") or DEFAULT_SAVE_MODE
        self._ask({**request, "format": fmt, "saveMode": mode, "schema": _frames.describe(schema)})
        where = location if self.dry_run else declared.get("location")
        if where is None:
            raise CheckNotRun(f"Invaract has nowhere to write '{name}'")
        if fmt.lower() not in FORMATS or mode.lower() not in SAVE_MODES:
            raise CheckNotRun(f"Invaract cannot yet write '{name}' as {fmt} with save mode {mode}: it writes {', '.join(FORMATS)}, "
                              f"with save mode {', '.join(SAVE_MODES)}")
        out = self._outputs.get(name) or self._outputs.setdefault(name, _Output(where, mode.lower(), self.run_id))
        if out.mode == "error" and _has_data(out.target):
            raise FileExistsError(f"invaract.write('{name}'): its save mode is error, and {out.target} already holds data")
        required = {f["name"] for f in declared.get("fields", []) if f.get("required")}
        nulls = out.stage(fmt.lower(), schema, batches, required)
        self._ask({"op": "written", "name": name, "nulls": nulls})

    # talking to the checker

    def _ask(self, request):
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

    def _declared(self, side, name):
        return next((d for d in (self.contract or {}).get(side, []) if d["name"] == name), {})

    def _job(self):
        libraries = {"pyarrow": pa.__version__}
        for module in ("pandas", "polars", "duckdb"):
            if module in sys.modules:
                libraries[module] = str(getattr(sys.modules[module], "__version__", "unknown"))
        return {"name": self.name, "runId": self.run_id, "pythonVersion": platform.python_version(), "libraries": libraries}

    # publishing

    def _publish(self):
        """Every staged output moved into place, in the order first written; an ``error`` output that has gained data since it
        was written stops the publish before anything moves."""
        for out in self._outputs.values():
            if out.mode == "error" and _has_data(out.target):
                raise FileExistsError(f"Invaract published nothing: the save mode of {out.target} is error, and it already holds data")
        for out in self._outputs.values():
            out.publish()
        for out in self._outputs.values():
            out.forget_previous()


class _Output:
    """One output's staging folder, beside its location so that publishing is a rename on the same file system."""

    def __init__(self, location, mode, run_id):
        self.target = os.path.abspath(location)
        parent, base = os.path.split(self.target)
        self.staging = os.path.join(parent, f".{base}.invaract-staging-{run_id}")
        self.previous = os.path.join(parent, f".{base}.invaract-previous-{run_id}")
        self.mode = mode
        self.run_id = run_id
        self.files = 0

    def stage(self, fmt, schema, batches, required):
        """Writes the batches to a new file in staging, with ``not null`` on each required column, and returns the empty values
        each column holds. A required column's first empty value ends the write: the checker refuses it, so nothing more is
        written."""
        os.makedirs(self.staging, exist_ok=True)
        self.files += 1
        path = os.path.join(self.staging, f"part-{self.run_id}-{self.files:05d}.{fmt}")
        target = pa.schema([f.with_nullable(False) if f.name in required else f for f in schema], metadata=schema.metadata)
        nulls = dict.fromkeys(schema.names, 0)
        try:
            writer = _writer(path, fmt, target)
            try:
                for batch in batches:
                    counts = _frames.null_counts(batch)
                    for column, n in counts.items():
                        nulls[column] += n
                    if any(counts.get(column) for column in required):
                        break
                    writer.write_batch(pa.RecordBatch.from_arrays(batch.columns, schema=target))
            finally:
                writer.close()
        except BaseException:
            _remove(path)
            raise
        return nulls

    def publish(self):
        if not os.path.isdir(self.staging):
            return
        if self.mode == "append":
            os.makedirs(self.target, exist_ok=True)
            for f in sorted(os.listdir(self.staging)):
                destination = os.path.join(self.target, f)
                if os.path.exists(destination):
                    raise FileExistsError(f"Invaract will not replace {destination} when appending")
                os.rename(os.path.join(self.staging, f), destination)
            self.discard()
        elif self.mode == "ignore" and _has_data(self.target):
            self.discard()
        else:
            if os.path.lexists(self.target):
                os.rename(self.target, self.previous)
            try:
                os.rename(self.staging, self.target)
            except BaseException:
                if os.path.lexists(self.previous):
                    os.rename(self.previous, self.target)
                raise

    def forget_previous(self):
        _remove(self.previous)

    def discard(self):
        _remove(self.staging)


def _writer(path, fmt, schema):
    if fmt == "csv":
        import pyarrow.csv as csv

        return csv.CSVWriter(path, schema)
    import pyarrow.parquet as pq

    return pq.ParquetWriter(path, schema)


def _has_data(path):
    """Whether a location holds data: a file, or a folder with anything in it that is not hidden (``.``) or metadata (``_``)."""
    if os.path.isfile(path):
        return True
    return os.path.isdir(path) and any(not f.startswith((".", "_")) for f in os.listdir(path))


def _remove(path):
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.lexists(path):
        os.remove(path)


def _with_location(request, location):
    return {**request, "location": os.fspath(location)} if location is not None else request


def _format_of(location):
    return "csv" if os.fspath(location).lower().endswith(".csv") else DEFAULT_FORMAT


def _script_name():
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    if spec is not None and spec.name:
        return spec.name
    return os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else "python"


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
    """Decorates a job's entry point: each call is one run. ``contract`` is the default contract (a path, or
    ``registry://<id>@<version>``); ``INVARACT_CONTRACT`` overrides it."""
    if callable(contract) and not isinstance(contract, (str, bytes, os.PathLike)):
        return job()(contract)

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with Run(contract, name or f"{fn.__module__}.{fn.__qualname__}"):
                return fn(*args, **kwargs)

        return wrapper

    return decorate
