# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The checker process: started once per run, one JSON request per line on its standard input and one reply per line on its
standard output (the protocol, docs/PYTHONIO_ADAPTER.md). Its log goes to the job's own standard error."""
import json
import os
import shlex
import subprocess

from ._settings import CHECKER
from .errors import CheckNotRun

#: The native checker a platform wheel carries (milestone 5 of the design); absent from a source checkout.
BUNDLED = os.path.join(os.path.dirname(__file__), "bin", "invaract-pythonio-checker")


def command(environ=None):
    """The checker's command line: ``INVARACT_CHECKER`` if set, else the native checker this package carries."""
    environ = os.environ if environ is None else environ
    if environ.get(CHECKER):
        return shlex.split(environ[CHECKER])
    if os.access(BUNDLED, os.X_OK):
        return [BUNDLED]
    raise CheckNotRun(
        f"Invaract has no checker to run on this platform, so nothing the job writes will be published: set {CHECKER} to the "
        "native checker, or to 'java -jar <path>/invaract-pythonio-checker.jar'."
    )


class Checker:
    """One running checker."""

    def __init__(self, cmd):
        try:
            self._process = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, encoding="utf-8", bufsize=1
            )
        except OSError as e:
            raise CheckNotRun(f"Invaract could not start its checker ({shlex.join(cmd)}): {e}") from e

    def ask(self, request):
        """Sends one request and returns the checker's reply."""
        try:
            self._process.stdin.write(json.dumps(request) + "\n")
            self._process.stdin.flush()
            line = self._process.stdout.readline()
        except (BrokenPipeError, OSError):
            line = ""
        if not line:
            raise CheckNotRun(f"Invaract's checker stopped (exit status {self._process.wait()}) before answering '{request['op']}'.")
        return json.loads(line)

    def close(self):
        """Lets the checker deliver its events and stop."""
        try:
            self._process.stdin.close()
        except OSError:
            pass
        self._process.wait()
        self._process.stdout.close()
