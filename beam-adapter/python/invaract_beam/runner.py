# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The wrapper runner: checks a pipeline against its data contract before the real runner gets it.

A job is switched over with ``--runner=invaract_beam.InvaractRunner`` and the ``--invaract_*`` flags (``InvaractOptions``),
with no change to its code. When the job runs its pipeline, Beam hands the constructed pipeline to this runner, which:

1. writes the pipeline's portable form (the language-neutral description every Beam SDK produces) to a temporary file;
2. runs the Java checker on it (``java -jar invaract-beam-checker.jar``), which translates and verifies it exactly as the
   Java adapter does - all the checking is there, none of it here;
3. hands the pipeline to the real runner (``--invaract_delegate``) only if the checker lets it through.

Anything other than a pass stops the job before any step runs: a refusal raises ``ContractViolation`` with the checker's
explanation, and a check that could not be made (no Java, no checker, a setting the checker does not know) raises
``CheckNotRun``. A job is never submitted unchecked.
"""
import logging
import os
import shlex
import subprocess
import tempfile

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.runner import PipelineRunner, create_runner

from .options import SETTINGS, InvaractOptions

_LOG = logging.getLogger(__name__)

#: The checker's main class, run directly when no checker jar is given (the classpath then comes from the JVM options).
CHECKER_MAIN = "com.invaract.beamadapter.PortableCheck"

#: Where the checker jar is found when ``--invaract_checker_jar`` is not given.
CHECKER_JAR_ENV = "INVARACT_BEAM_CHECKER_JAR"

# The checker's exit statuses: let the pipeline through; the contract refuses it. Anything else: it could not be checked.
PASSED = 0
REFUSED = 1


class ContractViolation(RuntimeError):
    """The data contract refuses the pipeline. Nothing was submitted; the message says what failed and how to fix it."""


class CheckNotRun(RuntimeError):
    """The pipeline could not be checked, so it was not submitted."""


class InvaractRunner(PipelineRunner):
    """Checks the pipeline against its contract, then hands it to ``--invaract_delegate``."""

    def run_pipeline(self, pipeline, options):
        delegate = options.view_as(InvaractOptions).invaract_delegate
        if delegate in ("InvaractRunner", "invaract_beam.InvaractRunner", "invaract_beam.runner.InvaractRunner"):
            raise CheckNotRun("--invaract_delegate names InvaractRunner itself; name the runner the job should run on (DataflowRunner).")
        check(pipeline, options)
        return create_runner(delegate).run_pipeline(pipeline, options)


def check(pipeline, options):
    """Runs the checker on ``pipeline``; returns when it passes, raises ``ContractViolation`` or ``CheckNotRun`` otherwise."""
    command = checker_command(options.view_as(InvaractOptions))
    with tempfile.TemporaryDirectory(prefix="invaract-") as folder:
        path = os.path.join(folder, "pipeline.pb")
        with open(path, "wb") as f:
            f.write(pipeline.to_runner_api().SerializeToString())
        try:
            # the checker's log goes to the job's own standard error; what it prints to standard output is its verdict
            done = subprocess.run(command + checker_arguments(path, options), stdout=subprocess.PIPE, text=True, check=False)
        except OSError as e:
            raise CheckNotRun(f"Invaract could not start its checker ({command[0]}: {e}), so the pipeline is not submitted. "
                              "The checker needs Java 11 or later where the job is submitted.") from e
    verdict = done.stdout.strip()
    if done.returncode == PASSED:
        _LOG.info("Invaract: the pipeline was checked against its contract and passed")
        return
    if done.returncode == REFUSED:
        raise ContractViolation(verdict)
    raise CheckNotRun(verdict or f"Invaract's checker stopped with exit status {done.returncode}, so the pipeline is not submitted.")


def checker_command(settings):
    """``java [options] -jar <checker jar>``, or the checker's main class on the classpath the JVM options give."""
    java = settings.invaract_java or (os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java")
    java_options = shlex.split(settings.invaract_java_options or "")
    jar = settings.invaract_checker_jar or os.environ.get(CHECKER_JAR_ENV)
    if jar:
        return [java, *java_options, "-jar", jar]
    if java_options:
        return [java, *java_options, CHECKER_MAIN]
    raise CheckNotRun(f"Invaract has no checker to run, so the pipeline is not submitted: give --invaract_checker_jar, or set {CHECKER_JAR_ENV}.")


def checker_arguments(path, options):
    """The pipeline file, what the checker's events say about the job, and each Invaract setting the job was given."""
    settings = options.view_as(InvaractOptions)
    arguments = [f"--pipeline={path}", f"--sdk_version={beam.__version__}", f"--runner={settings.invaract_delegate}"]
    job_name = options.view_as(GoogleCloudOptions).job_name
    if job_name:
        arguments.append(f"--job_name={job_name}")
    for name in SETTINGS:
        value = getattr(settings, "invaract_" + name)
        if value is not None:
            arguments.append(f"--invaract_{name}={value}")
    return arguments
