# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The settings a run takes from the environment, and the exit status a job ends with."""
import os
import subprocess
import sys
import textwrap

import pytest

import invaract
from invaract import _checker, _settings
from conftest import PYTHON, output, write_contract


def test_each_setting_is_spelled_invaract_and_its_name_in_snake_case():
    assert _settings.variable("contract") == "INVARACT_CONTRACT"
    assert _settings.variable("dryRun") == "INVARACT_DRY_RUN"
    assert _settings.variable("orgPolicyOverlays") == "INVARACT_ORG_POLICY_OVERLAYS"


def test_the_settings_set_are_forwarded_by_their_neutral_names_and_nothing_else():
    environ = {"INVARACT_DRY_RUN": "true", "INVARACT_ORG_POLICY": "", "INVARACT_CHECKER": "x", "INVARACT_TRUSTED_STEPS": "*", "PATH": "/bin"}
    assert _settings.from_environment(environ=environ) == {"dryRun": "true"}


def test_the_decorators_contract_is_the_default_and_the_environments_wins():
    assert _settings.from_environment("c.yaml", environ={}) == {"contract": "c.yaml"}
    assert _settings.from_environment("c.yaml", environ={"INVARACT_CONTRACT": "registry://x@1.0.0"}) == {"contract": "registry://x@1.0.0"}
    assert _settings.from_environment(None, environ={}) == {}


def test_the_checker_command_is_split_as_a_shell_would():
    assert _checker.command({"INVARACT_CHECKER": "java -jar '/a b/c.jar'"}) == ["java", "-jar", "/a b/c.jar"]


def executable(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n")
    path.chmod(0o755)
    return str(path)


@pytest.fixture
def bundled(tmp_path, monkeypatch):
    """What a wheel carries, empty: place a native checker and a jar in it as a test needs them."""
    native, jar = tmp_path / "pkg" / "bin" / "checker", tmp_path / "pkg" / "lib" / "checker.jar"
    monkeypatch.setattr(_checker, "BUNDLED", str(native))
    monkeypatch.setattr(_checker, "BUNDLED_JAR", str(jar))
    return type("Bundled", (), {"native": native, "jar": jar})


def test_invaract_checker_comes_first_then_the_native_checker_then_the_jar_with_java(bundled, tmp_path):
    java = executable(tmp_path / "jdk" / "bin" / "java")
    bundled.jar.parent.mkdir(parents=True)
    bundled.jar.write_bytes(b"jar")
    executable(bundled.native)
    assert _checker.command({"INVARACT_CHECKER": "mine", "JAVA_HOME": str(tmp_path / "jdk")}) == ["mine"]
    assert _checker.command({"JAVA_HOME": str(tmp_path / "jdk")}) == [str(bundled.native)]
    bundled.native.chmod(0o644)  # a native checker that cannot run is passed over
    assert _checker.command({"JAVA_HOME": str(tmp_path / "jdk")}) == [java, "-jar", str(bundled.jar)]


def test_the_jar_runs_with_java_home_s_java_else_the_one_on_path(bundled, tmp_path):
    bundled.jar.parent.mkdir(parents=True)
    bundled.jar.write_bytes(b"jar")
    home = executable(tmp_path / "jdk" / "bin" / "java")
    on_path = executable(tmp_path / "path" / "java")
    path = str(tmp_path / "path")
    assert _checker.command({"JAVA_HOME": str(tmp_path / "jdk"), "PATH": path})[0] == home
    assert _checker.command({"JAVA_HOME": str(tmp_path / "no-jdk"), "PATH": path})[0] == on_path
    assert _checker.command({"PATH": path})[0] == on_path
    with pytest.raises(invaract.CheckNotRun, match="no native checker for this platform.*Java 11 or later"):
        _checker.command({"PATH": str(tmp_path / "empty")})


def test_with_nothing_bundled_and_nothing_named_the_run_is_not_checked(bundled, tmp_path):
    executable(tmp_path / "path" / "java")
    with pytest.raises(invaract.CheckNotRun, match="no checker to run on this platform.*INVARACT_CHECKER"):
        _checker.command({"PATH": str(tmp_path / "path")})


def run_job(body):
    """Runs a job in a new Python process, as a scheduler would, and returns its exit status and standard error."""
    script = "import sys\nsys.path.insert(0, %r)\nimport invaract\n" % PYTHON + textwrap.dedent(body)
    done = subprocess.run([sys.executable, "-c", script], stderr=subprocess.PIPE, text=True, env=os.environ.copy())
    return done.returncode, done.stderr


@pytest.mark.parametrize("body,status,says", [
    ("""
     with invaract.run("contract.yaml"):
         invaract.write(pa.table({"region": ["a"], "total": [1]}), "report")
     """, 0, ""),
    ("""
     with invaract.run("contract.yaml"):
         invaract.write(pa.table({"region": ["a"], "total": ["1"]}), "report")
     """, 1, "invaract.errors.ContractViolation"),
    ("""
     with invaract.run("no-such-contract.yaml"):
         pass
     """, 2, "invaract.errors.CheckNotRun"),
    ("""
     raise SystemExit(3)
     """, 3, ""),
    ("""
     raise KeyError("the job's own error")
     """, 1, "KeyError"),
])
def test_a_job_ends_0_when_published_1_when_refused_and_2_when_not_checked(body, status, says):
    write_contract(outputs=output())
    returncode, stderr = run_job("import pyarrow as pa\n" + textwrap.dedent(body))
    assert returncode == status, stderr
    assert says in stderr
