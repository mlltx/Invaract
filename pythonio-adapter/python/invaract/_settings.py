# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract's settings, given to a run through the job's environment as ``INVARACT_<NAME>``, so a platform can configure a job
it does not own without touching its code.

The settings are Invaract's engine-neutral ones, read by the checker (docs/PYTHONIO_ADAPTER.md). This package does not
interpret them: each one set is forwarded under its neutral name, so a setting means the same here as on every other engine.
``SETTINGS`` must name every setting the checker reads; pythonio-adapter's PythonIoSettingsSpec fails, naming this file, when
the two differ.
"""
import os
import re

from _invaract_attach import ATTACH, AUTO_RUN, FAILED, REQUIRE_RUN  # noqa: F401 - the package's own variables
from _invaract_attach import _value as value

# The engine-neutral settings, by their neutral names, each with its help text.
SETTINGS = {
    "contract": "The data contract to enforce: a file path, or registry://<id>@<version>. Overrides the one @invaract.job names.",
    "dryRun": "true: no contract; reads and writes go where the job's location= arguments say, and a draft contract is inferred.",
    "notifyConfig": "Path of a notification-sink .properties file: where validation events are sent.",
    "registryUrl": "Base URL of the contract registry a registry:// contract is fetched from.",
    "registryClientClass": "Class name of the registry client the checker loads (only one built into the checker).",
    "locationMap": "Path of an id=location .properties file that ref://<id> locations in the contract resolve against.",
    "locationUrl": "Base URL of a location service that ref://<id> locations the location map does not hold are asked of.",
    "locationTokenEnv": "Name of the environment variable holding the location service's bearer token (the name, never the token).",
    "orgPolicy": "Path of an organisational policy document layered on the contract.",
    "orgPolicyOverlays": "Comma-separated paths of further organisational policy layers.",
    "rejectUndeclaredInputs": "true: a read of a dataset the contract does not declare is refused (it always is here).",
    "rejectUndeclaredFields": "true: a column the contract does not declare is refused.",
    "computeFingerprint": "true: compute a semantic fingerprint (not computed for Python jobs: a warning is logged, the run goes on).",
    "staticDataQuality": "true: prove the contract's value constraints (unsupported for Python jobs, so a run with it is refused).",
    "roleConsistency": "true: check each input's declared role (unsupported for Python jobs, so a run with it is refused).",
    "runtimeDataQuality": "true: check every row a job writes against its outputs' declared constraints before publishing.",
    "jobId": "The job's stable identity across runs, named by every event (job.jobId).",
    "jobAttributes": "Facts about this run every event carries (job.attributes): comma-separated key=value pairs, team=orders,dag=daily.",
    "contractMetadata": "Facts about the contract a dry run drafts, in every draft's extensions: comma-separated key=value pairs, owner=orders-team.",
    "openLineageUrl": "The OpenLineage endpoint the job's lineage is published to (events go to <url>/api/v1/lineage). Unset: nothing is published.",
    "openLineageConfig": "Path of a .properties file configuring OpenLineage publishing: transport, headers, compression, retries, facets, batching.",
    "openLineageNamespace": "The OpenLineage namespace the job is published under. Default: the configuration file's, else OPENLINEAGE_NAMESPACE.",
}

#: The command that runs the checker, split as a shell would: the native program, or ``java -jar <checker jar>``. The
#: package's own variable, not a setting: it is never forwarded.
CHECKER = "INVARACT_CHECKER"

#: The package's own variables for scanning the job's code when a run starts (``_codescan``), none of them forwarded: ``off``
#: (the default), ``report`` or ``enforce``; the files and folders to scan instead of the job's own loaded modules
#: (comma-separated); and the files and folders to leave out (comma-separated names or globs).
LINT = "INVARACT_LINT"
LINT_PATHS = "INVARACT_LINT_PATHS"
LINT_EXCLUDE = "INVARACT_LINT_EXCLUDE"

#: The package's own variables for watching what the job does as it runs (``_strict``), neither of them forwarded: ``off``
#: (the default), ``report`` or ``enforce``; and what is let through, as comma-separated globs matched against what a
#: finding reached (a file's path, a ``host:port``, a program's name).
STRICT = "INVARACT_STRICT"
STRICT_ALLOW = "INVARACT_STRICT_ALLOW"

#: The package's own variables for attaching without the job's annotation (docs/PYTHONIO_ATTACH_DESIGN.md), none of them
#: forwarded: ``ATTACH`` (``startup``, the default, or ``import``); ``ATTACH_INCLUDE`` and ``ATTACH_EXCLUDE`` (which main
#: programs count as jobs); ``REQUIRE_RUN`` (``off``, ``report`` or ``enforce``: a job that touched data and opened no run);
#: ``AUTO_RUN`` (``true``: Invaract opens the run for the job). Their names are kept in ``_invaract_attach``, which reads them
#: as Python starts, before this package is imported.
MODES = ("off", "report", "enforce")


def mode(environ, name, default="off"):
    """The value of one of the package's ``off``/``report``/``enforce`` variables, or ``ValueError`` naming it."""
    given = value(environ, name) or default
    if given not in MODES:
        raise ValueError(f"{name} must be off, report or enforce, not {environ[name]!r}")
    return given


def flag(environ, name):
    """The value of one of the package's ``true``/``false`` variables, or ``ValueError`` naming it."""
    given = value(environ, name) or "false"
    if given not in ("true", "false"):
        raise ValueError(f"{name} must be true or false, not {environ[name]!r}")
    return given == "true"


def strict_mode(environ):
    """How the job is watched (``_strict``): ``INVARACT_STRICT`` as set; unset, ``report`` when the platform requires a run
    or opens one for the job (both need to know what the job did), else ``off``."""
    if value(environ, STRICT):
        return mode(environ, STRICT)
    implied = value(environ, REQUIRE_RUN) in ("report", "enforce") or value(environ, AUTO_RUN) == "true"
    return "report" if implied else "off"


def variable(name):
    """How the environment spells the setting ``name``: ``orgPolicy`` is ``INVARACT_ORG_POLICY``."""
    return "INVARACT_" + re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).upper()


def from_environment(contract=None, environ=None):
    """The settings the environment gives, by neutral name; ``contract`` is used when ``INVARACT_CONTRACT`` is not set."""
    environ = os.environ if environ is None else environ
    settings = {name: environ[variable(name)] for name in SETTINGS if environ.get(variable(name))}
    if contract is not None and "contract" not in settings:
        settings["contract"] = os.fspath(contract)
    return settings
