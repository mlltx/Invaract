# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Stand-ins for the checker's JVM and for the runner a checked pipeline is handed to."""
import json
import os
import stat
import sys

from apache_beam.runners.runner import PipelineRunner


class RecordingRunner(PipelineRunner):
    """A delegate that records the pipelines it is handed and runs nothing."""

    handed = []

    def run_pipeline(self, pipeline, options):
        RecordingRunner.handed.append(pipeline)
        return "ran"


def fake_java(folder, status, verdict=""):
    """An executable standing in for ``java``: records its arguments and a copy of the pipeline it is given, prints
    ``verdict`` and exits with ``status``. Returns its path and the file its record is written to."""
    record = os.path.join(folder, "record.json")
    script = os.path.join(folder, "java")
    with open(script, "w") as f:
        f.write(f"""#!{sys.executable}
import json, shutil, sys
args = sys.argv[1:]
pipeline = next((a.split("=", 1)[1] for a in args if a.startswith("--pipeline=")), None)
if pipeline:
    shutil.copy(pipeline, {os.path.join(folder, "copy.pb")!r})
json.dump(args, open({record!r}, "w"))
print({verdict!r})
sys.exit({status})
""")
    os.chmod(script, os.stat(script).st_mode | stat.S_IEXEC)
    return script, record


def recorded(record):
    with open(record) as f:
        return json.load(f)
