# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The checker process: started once per run, one JSON request per line on its standard input and one reply per line on its
standard output (``_protocol``; docs/PYTHONIO_PROTOCOL.md). Its log goes to the job's own standard error."""
import json
import os
import shlex
import shutil
import subprocess

from ._settings import CHECKER
from .errors import CheckNotRun

_HERE = os.path.dirname(os.path.abspath(__file__))
#: The native checker a platform wheel carries.
BUNDLED = os.path.join(_HERE, "bin", "invaract-pythonio-checker")
#: The checker jar the pure-Python wheel carries, for a platform with no native checker; it runs with Java.
BUNDLED_JAR = os.path.join(_HERE, "lib", "invaract-pythonio-checker.jar")


def command(environ=None):
    """The checker's command line: ``INVARACT_CHECKER`` if set, else the native checker this package carries, else the jar it
    carries run with Java (``JAVA_HOME``'s, else the one on ``PATH``). A source checkout carries neither."""
    environ = os.environ if environ is None else environ
    if environ.get(CHECKER):
        return shlex.split(environ[CHECKER])
    if os.access(BUNDLED, os.X_OK):
        return [BUNDLED]
    if os.path.isfile(BUNDLED_JAR):
        java = _java(environ)
        if java is None:
            raise CheckNotRun(
                "Invaract has no native checker for this platform, so it runs its checker with Java, and found none: install "
                f"Java 11 or later (or set JAVA_HOME), or set {CHECKER}. Nothing the job writes will be published."
            )
        return [java, "-jar", BUNDLED_JAR]
    raise CheckNotRun(
        f"Invaract has no checker to run on this platform, so nothing the job writes will be published: set {CHECKER} to the "
        "native checker, or to 'java -jar <path>/invaract-pythonio-checker.jar'."
    )


def _java(environ):
    """``JAVA_HOME``'s ``java`` if there is one, else the one on ``PATH``, else ``None``."""
    if environ.get("JAVA_HOME"):
        java = os.path.join(environ["JAVA_HOME"], "bin", "java")
        for candidate in (java, java + ".exe"):
            if os.access(candidate, os.X_OK):
                return candidate
    return shutil.which("java", path=environ.get("PATH"))


class Checker:
    """One running checker."""

    def __init__(self, cmd):
        self._command = list(cmd)
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
        try:
            return json.loads(line)
        except ValueError as e:
            raise CheckNotRun(f"Invaract's checker answered '{request['op']}' with {line.strip()[:200]!r}, which is not a reply") from e

    def describe(self):
        """The checker's command line, as a message names it."""
        return shlex.join(self._command)

    def close(self):
        """Lets the checker deliver its events and stop."""
        try:
            self._process.stdin.close()
        except OSError:
            pass
        self._process.wait()
        self._process.stdout.close()
