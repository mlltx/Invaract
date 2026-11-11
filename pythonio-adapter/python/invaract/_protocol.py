# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The protocol the package speaks to its checker: every request a run sends is built here, so the whole wire format is in
one place.

One JSON object per line goes to the checker's standard input, and one reply per line comes back on its standard output
(``_checker``). docs/PYTHONIO_PROTOCOL.md describes each request and reply, and the JSON Schema at ``SCHEMA`` holds every
one of them; the package's tests validate every request sent and every reply received against it. A request describes data
(names, formats, columns and their types, how many empty values each holds); it never carries it.

A run is ``open``, then any number of ``read`` and ``write``/``written`` pairs, then ``finish``, and, when its reply says
to publish, ``published`` once the package has put the outputs in place. With ``INVARACT_LINT`` on, a ``scan`` follows
``open``, and another may come just before ``finish``.
"""
import os

#: The protocol version this package speaks. It changes only when a request or reply changes in a way an older checker or
#: package would misread; a new optional field leaves it as it is.
VERSION = 1

#: The protocol's JSON Schema, as published.
SCHEMA = "https://mlltx.github.io/Invaract/schemas/pythonio/v1/protocol.schema.json"


def open_run(settings, job, check=False):
    """Starts a run: the Invaract settings by their neutral names (``_settings``), and the job's details. ``check`` opens a
    check (``invaract.check``) instead: one frame judged as a write would be, with nothing written and no events."""
    return {"op": "open", "protocol": VERSION, "settings": settings, "job": job, **({"check": True} if check else {})}


def read(name, location=None, fmt=None, schema=None, catalog=None, csv=None):
    """A read of the input ``name``: the format it is read in, its columns (``columns``), and where it is registered. A read
    sent without ``schema`` is one the package has not made, because its description already disagrees with the contract:
    the checker refuses it, and never takes columns on trust."""
    return _present({"op": "read", "name": name, "location": _path(location), "format": fmt, "schema": schema,
                     "catalog": catalog, "csv": csv})


def write(name, location, fmt, save_mode, schema, layout, catalog=None, csv=None):
    """A write of the output ``name``, described before any of it is staged: its format, save mode, columns (``columns``,
    with no empty values counted yet) and partition layout (``_outputs.Layout``)."""
    return _present({"op": "write", "name": name, "location": _path(location), "format": fmt, "saveMode": save_mode,
                     "schema": schema, "partitionBy": list(layout.keys), "partitionOverwriteMode": layout.overwrite,
                     "catalog": catalog, "csv": csv})


def unverifiable_write(name, why, location=None):
    """A write of a table the package cannot describe (it offers no Arrow stream): always refused."""
    return _present({"op": "write", "name": name, "location": _path(location), "unverifiable": why})


def written(name, nulls, checks=None):
    """The end of a staged write: how many empty values each column turned out to hold and, with ``runtimeDataQuality`` on,
    what each of the output's data checks found, in the order ``open``'s reply gave them (``_checks.Evaluation``)."""
    return _present({"op": "written", "name": name, "nulls": nulls, "checks": checks})


def scan(phase, enforce, scanner, files, findings, errors, duration_ms):
    """What a scan of the job's own code found (``_codescan``): at the run's ``start``, or at its ``finish`` for code the job
    imported meanwhile. ``findings`` are invaract-lint's findings as data (``Finding.to_dict``), the ones a marker keeps
    included; ``errors`` are ``(path, message)`` for each file it could not read. With ``enforce``, a call no marker explains
    refuses the run."""
    return {"op": "scan", "phase": phase, "enforce": enforce, "scanner": scanner, "files": list(files), "findings": list(findings),
            "errors": [{"path": path, "message": message} for path, message in errors], "durationMs": duration_ms}


def finish(completed):
    """The end of the run. ``completed`` is false when the job failed: nothing is published, and the checker only delivers
    its events."""
    return {"op": "finish", "completed": completed}


def published(outputs):
    """What the package put in place after a ``finish`` that said to publish: ``outputs`` is ``(name, rows, milliseconds,
    _outputs.Published)`` for each output published, the time being what staging and publishing it took. The checker
    publishes a write event for each, then ends the run."""
    return {"op": "published", "outputs": [
        _present({"name": name, "rows": rows, "files": p.files, "bytes": p.bytes, "deltaVersion": p.delta_version,
                  "icebergSnapshotId": p.iceberg_snapshot_id, "durationMs": milliseconds})
        for name, rows, milliseconds, p in outputs
    ]}


def columns(schema, nulls=None):
    """A pyarrow schema's columns as a request describes them: each column's name, pyarrow's own name for its type
    (``str(type)``: ``int64``, ``large_string``, ``list<item: int32>``), which the checker maps to the contract's types,
    Arrow's nullable flag, and, where the data has been seen, how many empty values it holds."""
    return [
        {"name": f.name, "type": str(f.type), "nullable": f.nullable, **({"nulls": nulls[f.name]} if nulls is not None else {})}
        for f in schema
    ]


def _path(location):
    return os.fspath(location) if location is not None else None


def _present(request):
    """``request`` without the fields it does not have (``None``, or no CSV options), which the checker takes as absent."""
    return {k: v for k, v in request.items() if v is not None and not (k == "csv" and not v)}
