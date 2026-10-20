# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The wrapper runner, with a stand-in for the checker's JVM: what it hands the checker, and what it does with each answer.

What the checker itself decides is tested on the Java side (beam-adapter's PortableCheckSpec, and the conformance kit's
scenarios run as real Python pipelines in BeamPythonConformanceSpec)."""
import os

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api import beam_runner_api_pb2

import invaract_beam
from invaract_beam import CheckedOnly, CheckNotRun, ContractViolation, InvaractOptions, InvaractRunner
from invaract_beam.options import SETTINGS
from invaract_beam.runner import CHECKER_JAR_ENV, CHECKER_MAIN, checker_command
from fakes import RecordingRunner, fake_java, recorded


@pytest.fixture(autouse=True)
def no_ambient_checker(monkeypatch):
    monkeypatch.delenv(CHECKER_JAR_ENV, raising=False)
    RecordingRunner.handed.clear()


def pipeline(*flags):
    options = PipelineOptions(["--invaract_delegate=fakes.RecordingRunner", *flags])
    p = beam.Pipeline(runner=InvaractRunner(), options=options)
    _ = p | "Make" >> beam.Create([1, 2]) | "Write" >> beam.io.WriteToText("out/numbers")
    return p


def run_with(tmp_path, status, verdict="", *flags):
    java, record = fake_java(str(tmp_path), status, verdict)
    p = pipeline(f"--invaract_java={java}", "--invaract_checker_jar=checker.jar", *flags)
    return p, record


def test_a_pipeline_that_passes_is_handed_to_the_delegate(tmp_path):
    p, record = run_with(tmp_path, 0, "", "--invaract_contract=contract.yaml")
    assert p.run() == "ran"
    assert RecordingRunner.handed == [p]
    args = recorded(record)
    assert args[:2] == ["-jar", "checker.jar"]
    assert "--invaract_contract=contract.yaml" in args
    assert f"--sdk_version={beam.__version__}" in args and "--runner=fakes.RecordingRunner" in args


def test_the_checker_is_given_the_pipeline_portable_form_and_it_is_removed_afterwards(tmp_path):
    p, record = run_with(tmp_path, 0)
    p.run()
    proto = beam_runner_api_pb2.Pipeline()
    with open(os.path.join(str(tmp_path), "copy.pb"), "rb") as f:
        proto.ParseFromString(f.read())
    names = {t.unique_name for t in proto.components.transforms.values()}
    assert {"Make", "Write"} <= names
    given = next(a for a in recorded(record) if a.startswith("--pipeline="))
    assert not os.path.exists(given.split("=", 1)[1])


def test_check_only_checks_the_pipeline_and_hands_it_to_no_runner(tmp_path):
    p, record = run_with(tmp_path, 0, "", "--invaract_contract=contract.yaml", "--invaract_check_only=true")
    result = p.run()
    assert isinstance(result, CheckedOnly)
    assert RecordingRunner.handed == []
    assert "--check_only=true" in recorded(record), "the events say the pipeline was only checked"
    # a job that waits for its pipeline and reads its metrics goes on as after a run that did nothing
    assert result.wait_until_finish() == "DONE" and result.state == "DONE" and result.cancel() == "DONE"
    assert result.metrics().query()["counters"] == []


def test_without_check_only_the_checker_is_not_told_so_and_the_pipeline_runs(tmp_path):
    for flags in ([], ["--invaract_check_only=false"], ["--invaract_check_only= FALSE "]):
        RecordingRunner.handed.clear()
        p, record = run_with(tmp_path, 0, "", *flags)
        assert p.run() == "ran" and RecordingRunner.handed == [p]
        assert not any(a.startswith("--check_only") for a in recorded(record))


def test_a_pipeline_refused_under_check_only_is_refused_as_ever(tmp_path):
    p, _ = run_with(tmp_path, 1, "Contract violation: [OUTPUT_LOCATION_MISMATCH] ...", "--invaract_check_only=TRUE")
    with pytest.raises(ContractViolation, match=r"OUTPUT_LOCATION_MISMATCH"):
        p.run()
    assert RecordingRunner.handed == []


def test_a_check_only_value_that_is_not_true_or_false_stops_the_job_before_the_checker_runs(tmp_path):
    p, record = run_with(tmp_path, 0, "", "--invaract_check_only=yes")
    with pytest.raises(CheckNotRun, match=r"--invaract_check_only=yes must be true or false"):
        p.run()
    assert RecordingRunner.handed == [] and not os.path.exists(record)


def test_a_refused_pipeline_is_not_handed_on_and_the_reason_is_the_checker_s(tmp_path):
    p, _ = run_with(tmp_path, 1, "Contract violation: [OUTPUT_LOCATION_MISMATCH] ...")
    with pytest.raises(ContractViolation, match=r"OUTPUT_LOCATION_MISMATCH"):
        p.run()
    assert RecordingRunner.handed == []


@pytest.mark.parametrize("status", [2, 3, 137])
def test_a_pipeline_that_could_not_be_checked_is_not_handed_on(tmp_path, status):
    p, _ = run_with(tmp_path, status, "Invaract could not check the pipeline")
    with pytest.raises(CheckNotRun, match=r"could not check"):
        p.run()
    assert RecordingRunner.handed == []


def test_a_checker_that_says_nothing_still_says_why_it_stopped(tmp_path):
    p, _ = run_with(tmp_path, 2, "")
    with pytest.raises(CheckNotRun, match=r"exit status 2"):
        p.run()


def test_only_the_settings_given_are_forwarded_with_the_checker_s_spelling(tmp_path):
    p, record = run_with(tmp_path, 0, "", "--invaract_dry_run=true", "--invaract_trusted_steps=Notify*,Metrics", "--job_name=nightly")
    p.run()
    args = recorded(record)
    assert [a for a in args if a.startswith("--invaract_")] == ["--invaract_dry_run=true", "--invaract_trusted_steps=Notify*,Metrics"]
    assert "--job_name=nightly" in args


def test_every_setting_is_a_flag_and_is_forwarded(tmp_path):
    flags = [f"--invaract_{name}=v{i}" for i, name in enumerate(SETTINGS)]
    p, record = run_with(tmp_path, 0, "", *flags)
    p.run()
    assert [a for a in recorded(record) if a.startswith("--invaract_")] == flags


def test_no_java_is_not_checked(tmp_path):
    p = pipeline(f"--invaract_java={tmp_path}/no-such-java", "--invaract_checker_jar=checker.jar")
    with pytest.raises(CheckNotRun, match=r"needs Java 11 or later"):
        p.run()
    assert RecordingRunner.handed == []


def test_no_checker_is_not_checked():
    with pytest.raises(CheckNotRun, match=CHECKER_JAR_ENV):
        pipeline("--invaract_java=java").run()


def test_naming_the_runner_itself_as_the_delegate_is_refused():
    p = beam.Pipeline(runner=InvaractRunner(), options=PipelineOptions(["--invaract_delegate=invaract_beam.InvaractRunner"]))
    with pytest.raises(CheckNotRun, match=r"names InvaractRunner itself"):
        p.run()


def settings(*flags):
    return PipelineOptions(list(flags)).view_as(InvaractOptions)


def test_the_checker_command(monkeypatch):
    assert checker_command(settings("--invaract_java=/j/java", "--invaract_checker_jar=c.jar")) == ["/j/java", "-jar", "c.jar"]
    assert checker_command(settings("--invaract_java=java", "--invaract_checker_jar=c.jar", "--invaract_java_options=-Xmx1g -Dx=a b")) == \
        ["java", "-Xmx1g", "-Dx=a", "b", "-jar", "c.jar"]
    # without a jar, the main class on the classpath the JVM options give (how the adapter's own tests run the checker)
    assert checker_command(settings("--invaract_java=java", "--invaract_java_options=@cp.args")) == ["java", "@cp.args", CHECKER_MAIN]
    monkeypatch.setenv(CHECKER_JAR_ENV, "/opt/invaract/checker.jar")
    assert checker_command(settings("--invaract_java=java")) == ["java", "-jar", "/opt/invaract/checker.jar"]
    assert checker_command(settings("--invaract_java=java", "--invaract_checker_jar=mine.jar"))[-1] == "mine.jar", "the flag wins"


def test_java_comes_from_java_home_else_the_path(monkeypatch):
    monkeypatch.setenv("JAVA_HOME", "/opt/jdk")
    assert checker_command(settings("--invaract_checker_jar=c.jar"))[0] == os.path.join("/opt/jdk", "bin", "java")
    monkeypatch.delenv("JAVA_HOME")
    assert checker_command(settings("--invaract_checker_jar=c.jar"))[0] == "java"


def test_the_delegate_defaults_to_the_direct_runner():
    assert settings().invaract_delegate == "DirectRunner"


def test_the_package_names_what_a_job_uses():
    assert invaract_beam.__all__ == ["InvaractRunner", "InvaractOptions", "ContractViolation", "CheckNotRun", "CheckedOnly"]
