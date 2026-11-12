# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""How each dataset is classified (``type: DATA_ASSET | SOURCE | CONTROL``), held to the organisation's policy when a run
opens: a readiness check that reads a control table and writes a readiness signal, run as a real job against a real checker.

The policy is the platform's, attached with ``INVARACT_ORG_POLICY`` (the job never names it), and its ``controlTables`` list
the approved control tables. Four of its rules decide classification: every dataset declares a type
(``require_dataset_type``), a CONTROL dataset must be in the list (``require_control_registration``), a location in the list
must be declared CONTROL (``require_registered_control_type``), and a CONTROL dataset carries no sensitive data
(``forbid_control_sensitivity_tags``). A run that breaks any of them is refused before it reads or writes anything."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract

STATUS = "data/control/load_status"
READY = "data/control/report_ready"

POLICY = f"""version: "1.0"
controlTables:
  - {{ location: {STATUS}, owner: data-platform, requiredFields: [dataset, business_date, status] }}
  - {{ location: {READY}, owner: data-platform, purpose: says the report is ready }}
policies:
  - {{ id: typed-datasets, type: require_dataset_type }}
  - {{ id: control-registered, type: require_control_registration }}
  - {{ id: registered-is-control, type: require_registered_control_type }}
  - {{ id: control-not-sensitive, type: forbid_control_sensitivity_tags }}
"""


def dataset(name, location, type_line, fields, save_mode=""):
    columns = "".join(f"        - {{ name: {n}, type: {t}{extra} }}\n" for n, t, extra in fields)
    return (f"  - name: {name}\n    location: {location}\n    format: parquet\n{save_mode}{type_line}"
            f"    schema:\n      fields:\n{columns}")


def status_input(type_line="    type: CONTROL\n", location=STATUS, status_extra=""):
    fields = [("dataset", "string", ""), ("business_date", "string", ""), ("status", "string", status_extra)]
    return dataset("load_status", location, type_line, fields)


def ready_output(type_line="    type: CONTROL\n", location=READY):
    return dataset("report_ready", location, type_line, [("dataset", "string", ""), ("ready", "boolean", "")],
                   save_mode="    saveMode: overwrite\n")


@pytest.fixture
def readiness_check(monkeypatch):
    """The platform's policy attached, the control table on disk, and a function that writes the readiness check's contract
    and runs it: it reads ``load_status`` and writes whether the report is ready to ``report_ready``."""
    with open("policy.yaml", "w", encoding="utf-8") as f:
        f.write(POLICY)
    monkeypatch.setenv("INVARACT_ORG_POLICY", os.path.abspath("policy.yaml"))
    os.makedirs(STATUS)
    pq.write_table(pa.table({"dataset": ["report"], "business_date": ["2026-11-10"], "status": ["LOADED"]}), f"{STATUS}/part-0.parquet")

    def run(inputs=None, outputs=None):
        with open("contract.yaml", "w", encoding="utf-8") as f:
            f.write(f'id: readiness_check\nversion: "1.0.0"\ninputs:\n{inputs or status_input()}outputs:\n{outputs or ready_output()}')
        with invaract.run("contract.yaml"):
            status = invaract.read("load_status")
            invaract.write(status.assign(ready=status["status"] == "LOADED")[["dataset", "ready"]], "report_ready")

    return run


def test_a_readiness_check_classified_as_the_policy_says_publishes_its_signal(readiness_check):
    readiness_check()
    assert pq.read_table(READY).to_pydict() == {"dataset": ["report"], "ready": [True]}


@pytest.mark.parametrize(
    "contract, policy, message",
    [
        pytest.param(
            # at a location the list does not hold: at a registered one, registered-is-control would refuse it too
            {"outputs": ready_output(type_line="", location="data/out/report_ready")}, "typed-datasets",
            "requires dataset 'report_ready' to declare a type, but it does not",
            id="an output with no type"),
        pytest.param(
            {"outputs": ready_output(type_line="    type: DATA_ASSET\n")}, "registered-is-control",
            f"'{READY}' is registered in the org's controlTables as a control table (owner: data-platform), so dataset "
            "'report_ready' must be declared CONTROL, but the contract declares it DATA_ASSET.",
            id="the readiness signal declared a data asset"),
        pytest.param(
            {"inputs": status_input(type_line="    type: DATA_ASSET\n")}, "registered-is-control",
            "dataset 'load_status' must be declared CONTROL, but the contract declares it DATA_ASSET.",
            id="the control table it decides from declared a data asset"),
        pytest.param(
            {"inputs": status_input(type_line="    type: SOURCE\n")}, "registered-is-control",
            "dataset 'load_status' must be declared CONTROL, but the contract declares it SOURCE.",
            id="the control table declared a source"),
        pytest.param(
            {"outputs": ready_output(location="data/control/other_ready")}, "control-registered",
            "declares dataset 'report_ready' CONTROL, but 'data/control/other_ready' has no active entry in the org's "
            "controlTables registry.",
            id="a control output the platform has not registered"),
        pytest.param(
            {"inputs": status_input(status_extra=", sensitivityTags: [pii]")}, "control-not-sensitive",
            "declares dataset 'load_status' CONTROL, but its schema carries sensitivity tag(s) pii",
            id="a control table carrying sensitive data"),
    ],
)
def test_a_misclassified_dataset_refuses_the_run_when_it_opens_and_nothing_is_published(readiness_check, contract, policy, message):
    with pytest.raises(invaract.ContractViolation) as refused:
        readiness_check(**contract)
    assert refused.value.violations == ["ORG_POLICY_VIOLATION"]
    explanation = str(refused.value)
    assert f"organizational policy '{policy}'" in explanation and message in explanation
    assert explanation.count("[ORG_POLICY_VIOLATION]") == 1, "only the one rule broken is reported"
    assert not any(os.path.exists(p) for p in (READY, "data/control/other_ready", "data/out"))


def test_a_registered_control_table_missing_a_field_the_platform_requires_is_refused(readiness_check):
    fields = [("dataset", "string", ""), ("status", "string", "")]
    with pytest.raises(invaract.ContractViolation) as refused:
        readiness_check(inputs=dataset("load_status", STATUS, "    type: CONTROL\n", fields))
    assert "is registered as a CONTROL table (owner: data-platform)" in str(refused.value)
    assert "missing required field(s): business_date" in str(refused.value)
    assert not os.path.exists(READY)
