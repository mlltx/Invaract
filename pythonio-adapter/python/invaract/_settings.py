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
}

#: The command that runs the checker, split as a shell would: the native program, or ``java -jar <checker jar>``. The
#: package's own variable, not a setting: it is never forwarded.
CHECKER = "INVARACT_CHECKER"


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
