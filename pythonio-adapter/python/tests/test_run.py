# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A run against a real checker: what is read, what is refused and when, and what is published."""
import contextlib
import http.server
import json
import os
import shlex
import threading

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invaract
from conftest import event_of, events, notify_to_file, output, write_contract

REPORT = pd.DataFrame({"region": ["north", "south"], "total": [10, 20]})


def published(location="data/out/report"):
    """The files at a published location, its staging and previous versions left beside it, and the table it holds."""
    parent, base = os.path.split(location)
    leftovers = [f for f in os.listdir(parent) if f.startswith(f".{base}.invaract-")] if os.path.isdir(parent) else []
    files = sorted(os.listdir(location)) if os.path.isdir(location) else []
    return files, leftovers


def report_contract(**kwargs):
    return write_contract(outputs=output(**kwargs))


def orders_input(fields=True):
    os.makedirs("data/in/orders", exist_ok=True)
    pq.write_table(pa.table({"order_id": pa.array([1, 2, 3], pa.int64()), "region": ["north", None, "south"], "extra": [1, 2, 3]}),
                   "data/in/orders/part-0.parquet")
    schema = "    schema:\n      fields:\n        - { name: order_id, type: long, required: true }\n        - { name: region, type: string }\n"
    return "  - name: orders\n    location: data/in/orders\n    format: parquet\n" + (schema if fields else "")


# a run that passes


def test_a_passing_run_publishes_each_output_with_its_required_columns_not_null():
    report_contract()
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
        assert not os.path.exists("data/out/report"), "nothing is published before the run ends"
    files, leftovers = published()
    assert len(files) == 1 and files[0].endswith(".parquet") and leftovers == []
    table = pq.read_table("data/out/report")
    assert [(f.name, f.nullable) for f in table.schema] == [("region", False), ("total", False)]
    assert table.to_pydict() == {"region": ["north", "south"], "total": [10, 20]}


def test_the_decorator_runs_the_job_once_per_call_and_returns_its_result():
    report_contract()

    @invaract.job("contract.yaml")
    def main(n):
        invaract.write(REPORT.head(n), "report")
        return n

    assert main(1) == 1
    assert pq.read_table("data/out/report").num_rows == 1
    assert main(2) == 2
    assert pq.read_table("data/out/report").num_rows == 2, "overwrite replaced the first run's output"
    assert main.__name__ == "main"


def test_the_decorator_also_works_bare_and_takes_its_contract_from_the_environment(monkeypatch):
    report_contract()
    monkeypatch.setenv("INVARACT_CONTRACT", "contract.yaml")

    @invaract.job
    def main():
        invaract.write(REPORT, "report")

    main()
    assert published()[0]


def test_invaract_contract_overrides_the_decorators_argument(monkeypatch):
    report_contract()
    monkeypatch.setenv("INVARACT_CONTRACT", "contract.yaml")

    @invaract.job("no-such-contract.yaml")
    def main():
        invaract.write(REPORT, "report")

    main()
    assert published()[0]


def test_a_read_returns_only_the_declared_columns_and_tells_the_checker_their_empty_values():
    write_contract(inputs=orders_input(), outputs=output())
    with invaract.run("contract.yaml"):
        orders = invaract.read("orders", as_="arrow")
        invaract.write(REPORT, "report")
    assert orders.column_names == ["order_id", "region"]
    assert orders.column("region").null_count == 1


def test_a_read_of_an_input_with_no_declared_columns_returns_every_column():
    write_contract(inputs=orders_input(fields=False), outputs=output())
    with invaract.run("contract.yaml"):
        assert invaract.read("orders", as_="arrow").column_names == ["order_id", "region", "extra"]
        invaract.write(REPORT, "report")


def test_a_csv_input_is_read_with_the_contracts_types():
    os.makedirs("data/in", exist_ok=True)
    with open("data/in/customers.csv", "w") as f:
        f.write("customer_id,code,region\n1,007,north\n2,010,south\n")
    write_contract(
        inputs="  - name: customers\n    location: data/in/customers.csv\n    format: csv\n    schema:\n      fields:\n"
               "        - { name: customer_id, type: long }\n        - { name: code, type: string }\n",
        outputs=output(),
    )
    with invaract.run("contract.yaml"):
        customers = invaract.read("customers", as_="arrow")
        invaract.write(REPORT, "report")
    assert customers.schema.field("customer_id").type == pa.int64()
    assert customers.column("code").to_pylist() == ["007", "010"], "read as the contract's string, not inferred as a number"
    assert customers.column_names == ["customer_id", "code"]


def test_the_reads_and_writes_decorators():
    write_contract(inputs=orders_input(), outputs=output())

    @invaract.reads("orders")
    def regions(orders):
        return orders

    @invaract.writes("report")
    def report():
        return REPORT

    with invaract.run("contract.yaml"):
        assert list(regions().columns) == ["order_id", "region"]
        assert list(regions(orders="given").__iter__()) == list("given"), "a caller can pass the input itself"
        assert report() is REPORT
    assert published()[0]


# refusals: each raises ContractViolation, and the run publishes nothing


def test_a_write_whose_columns_cannot_pass_is_refused_before_anything_is_staged():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            try:
                invaract.write(REPORT.astype({"total": str}), "report")
            finally:
                assert not os.path.exists("data/out"), "refused before staging"
    assert refused.value.violations == ["OUTPUT_FIELD_TYPE_MISMATCH"]
    assert "total" in str(refused.value) and "How to correct it" in str(refused.value)
    assert isinstance(refused.value, PermissionError)


def test_an_empty_value_in_a_required_column_is_refused_and_nothing_is_published():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(pd.DataFrame({"region": ["north", None], "total": [1, 2]}), "report")
    assert refused.value.violations == ["OUTPUT_FIELD_NULLABILITY_MISMATCH"]
    assert published() == ([], [])


def test_an_empty_value_in_an_optional_column_is_published():
    report_contract(fields=[("region", "string", True), ("total", "long", False)])
    with invaract.run("contract.yaml"):
        invaract.write(pa.table({"region": ["a", "b"], "total": pa.array([1, None], pa.int64())}), "report")
    table = pq.read_table("data/out/report")
    assert [(f.name, f.nullable) for f in table.schema] == [("region", False), ("total", True)]
    assert table.column("total").null_count == 1


def test_a_required_columns_first_empty_value_ends_the_write_after_counting_every_batch_before_it():
    report_contract()
    schema = pa.schema([("region", pa.string()), ("total", pa.int64())])
    batches = [pa.record_batch([pa.array(["a"]), pa.array([1], pa.int64())], schema=schema),
               pa.record_batch([pa.array([None], pa.string()), pa.array([2], pa.int64())], schema=schema)]
    taken = []

    def produce():
        for b in batches + batches:
            taken.append(b)
            yield b

    reader = pa.RecordBatchReader.from_batches(schema, produce())
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(reader, "report")
    assert len(taken) == 2, "the batches after the first empty value are never taken"


def test_a_write_to_a_name_the_contract_does_not_declare_is_refused():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT, "debug_dump")
    assert refused.value.violations == ["OUTPUT_LOCATION_MISMATCH"]
    assert published() == ([], []), "the report passed, but the run was refused, so it is not published"


def test_a_read_of_a_name_the_contract_does_not_declare_is_refused():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.read("orders")
    assert refused.value.violations == ["UNDECLARED_INPUT"]


def test_a_table_with_no_arrow_stream_is_refused_as_unverifiable():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write([{"region": "north", "total": 1}], "report")
    assert refused.value.violations == ["UNVERIFIABLE_WRITE"]
    assert "builtins.list" in str(refused.value)


def test_a_run_that_never_writes_a_declared_output_is_refused_at_its_end():
    write_contract(outputs=output() + output(name="audit", location="data/out/audit", fields=[("order_id", "long", True)]))
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert refused.value.violations == ["MISSING_OUTPUT"]
    assert published() == ([], [])


def test_a_refusal_the_job_swallows_still_refuses_the_run():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            try:
                invaract.write(REPORT, "debug_dump")
            except invaract.ContractViolation:
                pass
    assert refused.value.violations == ["OUTPUT_LOCATION_MISMATCH"]
    assert published() == ([], [])


def test_a_job_that_fails_publishes_nothing_and_its_own_error_is_raised():
    report_contract()
    with pytest.raises(ZeroDivisionError):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            1 / 0
    assert published() == ([], [])


def test_a_write_that_fails_part_way_leaves_nothing_staged():
    report_contract()

    def produce():
        yield pa.record_batch([pa.array(["a"]), pa.array([1], pa.int64())], names=["region", "total"])
        raise OSError("the source went away")

    reader = pa.RecordBatchReader.from_batches(pa.schema([("region", pa.string()), ("total", pa.int64())]), produce())
    with pytest.raises(OSError):
        with invaract.run("contract.yaml"):
            try:
                invaract.write(reader, "report")
            finally:
                staged = [f for d in os.listdir("data/out") for f in os.listdir(os.path.join("data/out", d))]
                assert staged == [], "the part-written file is removed at once"
    assert published() == ([], [])


# location=, format= and save_mode=


def test_a_location_that_agrees_with_the_contract_is_allowed():
    write_contract(inputs=orders_input(), outputs=output())
    with invaract.run("contract.yaml"):
        invaract.read("orders", location="./data/in/orders")
        invaract.write(REPORT, "report", location="data/out/report", format="parquet", save_mode="overwrite")
    assert published()[0]


def test_a_location_that_disagrees_with_the_contract_is_refused():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report", location="data/out/report_old")
    assert refused.value.violations == ["OUTPUT_LOCATION_MISMATCH"]
    assert not os.path.exists("data/out/report_old") and published() == ([], [])


def test_ref_locations_resolve_through_the_platforms_location_map(monkeypatch):
    # the contract names its datasets by id; the platform maps each id to where it is in this environment, and the job reads
    # and writes there without naming a path (the same resolution, in verification-core, as Spark's and Beam's)
    orders = orders_input().replace("location: data/in/orders", "location: ref://orders_in")
    write_contract(inputs=orders, outputs=output(location="ref://report_out"))
    with open("locations.properties", "w", encoding="utf-8") as f:
        f.write(f"orders_in={os.path.abspath('data/in/orders')}\nreport_out={os.path.abspath('data/out/report')}\n")
    monkeypatch.setenv("INVARACT_LOCATION_MAP", "locations.properties")
    with invaract.run("contract.yaml") as run:
        assert [d["location"] for d in run.contract["inputs"] + run.contract["outputs"]] == [
            os.path.abspath("data/in/orders"), os.path.abspath("data/out/report")]
        orders = invaract.read("orders")
        invaract.write(orders.groupby("region", as_index=False).size().rename(columns={"size": "total"}), "report")
    assert pq.read_table("data/out/report").to_pydict() == {"region": ["north", "south"], "total": [1, 1]}
    assert not os.path.exists("ref:"), "nothing is written under the unresolved name"


def test_a_ref_location_the_map_does_not_hold_is_not_checked_and_names_the_ids_it_does(monkeypatch):
    report_contract(location="ref://report_out")
    with open("locations.properties", "w", encoding="utf-8") as f:
        f.write("orders_in=/data/orders\n")
    monkeypatch.setenv("INVARACT_LOCATION_MAP", "locations.properties")
    with pytest.raises(invaract.CheckNotRun) as not_checked:
        with invaract.run("contract.yaml"):
            pass
    assert "ref://report_out" in str(not_checked.value) and "orders_in" in str(not_checked.value)
    assert "INVARACT_LOCATION_MAP" in str(not_checked.value), "the setting is named as the job's environment spells it"


@contextlib.contextmanager
def contract_registry(contracts):
    """A contract registry on a free local port, serving each ``(id, version) -> YAML`` in ``contracts`` as the registry's
    HTTP API does (``GET /contracts/<id>/versions/<version>``): its base URL, and the paths asked for."""
    asked = []

    class Registry(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            asked.append(self.path)
            parts = self.path.strip("/").split("/")
            served = contracts.get((parts[1], parts[3])) if len(parts) == 4 and parts[0] == "contracts" else None
            body = (served or "not found").encode("utf-8")
            self.send_response(200 if served else 404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Registry)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", asked
    finally:
        server.shutdown()
        server.server_close()


def test_a_registry_contract_is_fetched_from_the_registry_the_platform_names(monkeypatch):
    # the checker carries the default registry client: a Python job has no classpath a platform could add one to
    with open(report_contract(), encoding="utf-8") as f:
        served = f.read()
    os.remove("contract.yaml")
    with contract_registry({("test_contract", "1.0.0"): served}) as (url, asked):
        monkeypatch.setenv("INVARACT_CONTRACT", "registry://test_contract@1.0.0")
        monkeypatch.setenv("INVARACT_REGISTRY_URL", url)
        with invaract.run() as run:
            assert (run.contract["id"], run.contract["version"]) == ("test_contract", "1.0.0")
            invaract.write(REPORT, "report")
    assert asked == ["/contracts/test_contract/versions/1.0.0"]
    assert pq.read_table("data/out/report").to_pydict() == {"region": ["north", "south"], "total": [10, 20]}


def test_a_registry_contract_the_registry_does_not_hold_is_not_checked(monkeypatch):
    with contract_registry({}) as (url, _):
        monkeypatch.setenv("INVARACT_CONTRACT", "registry://test_contract@9.9.9")
        monkeypatch.setenv("INVARACT_REGISTRY_URL", url)
        with pytest.raises(invaract.CheckNotRun) as not_checked:
            with invaract.run():
                pass
    # the registry client's own message, not the reflective call's wrapper around it
    assert "No contract registered for id 'test_contract' at version '9.9.9'" in str(not_checked.value)


@contextlib.contextmanager
def location_service(locations):
    """A location service on a free local port, answering ``GET /locations/<id>`` with ``{"location": ...}`` for each id in
    ``locations`` and 404 for any other: its base URL, and each request's ``(path, Authorization header)``."""
    asked = []

    class Service(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            asked.append((self.path, self.headers.get("Authorization")))
            known = self.path.startswith("/locations/") and self.path[len("/locations/"):] in locations
            body = json.dumps({"location": locations[self.path[len("/locations/"):]]}).encode("utf-8") if known else b""
            self.send_response(200 if known else 404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Service)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", asked
    finally:
        server.shutdown()
        server.server_close()


def test_ref_locations_resolve_through_the_platforms_location_service(monkeypatch):
    # the same resolution as Spark's and Beam's (verification-core's HttpLocationResolver): GET <url>/locations/<id>, with the
    # bearer token from the variable INVARACT_LOCATION_TOKEN_ENV names; the location map, when set too, is asked first
    orders = orders_input().replace("location: data/in/orders", "location: ref://orders_in")
    write_contract(inputs=orders, outputs=output(location="ref://report_out"))
    with open("locations.properties", "w", encoding="utf-8") as f:
        f.write(f"orders_in={os.path.abspath('data/in/orders')}\n")
    monkeypatch.setenv("INVARACT_LOCATION_MAP", "locations.properties")
    monkeypatch.setenv("LOCATIONS_TOKEN", "s3cr3t")
    monkeypatch.setenv("INVARACT_LOCATION_TOKEN_ENV", "LOCATIONS_TOKEN")
    with location_service({"report_out": os.path.abspath("data/out/report")}) as (url, asked):
        monkeypatch.setenv("INVARACT_LOCATION_URL", url)
        with invaract.run("contract.yaml"):
            invaract.write(invaract.read("orders").groupby("region", as_index=False).size().rename(columns={"size": "total"}), "report")
    assert asked == [("/locations/report_out", "Bearer s3cr3t")], "orders_in came from the map; only report_out was asked"
    assert pq.read_table("data/out/report").to_pydict() == {"region": ["north", "south"], "total": [1, 1]}


def test_a_ref_location_the_service_does_not_hold_is_not_checked(monkeypatch):
    report_contract(location="ref://report_out")
    with location_service({}) as (url, _):
        monkeypatch.setenv("INVARACT_LOCATION_URL", url)
        with pytest.raises(invaract.CheckNotRun) as not_checked:
            with invaract.run("contract.yaml"):
                pass
    assert f"could not resolve 'ref://report_out' ({url}/locations/report_out): it has no location for that id" in str(not_checked.value)


def test_a_read_location_that_disagrees_with_the_contract_is_refused():
    write_contract(inputs=orders_input(), outputs=output())
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.read("orders", location="data/in/orders_backup")
    assert refused.value.violations == ["UNDECLARED_INPUT"]


def test_a_read_format_that_disagrees_with_the_contract_is_refused_before_anything_is_read(monkeypatch):
    from invaract import _datasets

    write_contract(inputs=orders_input(), outputs=output())
    reads = []
    monkeypatch.setattr(_datasets, "read_table", lambda *args, **kwargs: reads.append(args))
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.read("orders", format="csv")
    assert refused.value.violations == ["INPUT_FORMAT_MISMATCH"]
    assert "contract declares input format 'parquet' but the plan reads 'data/in/orders' in format 'csv'" in str(refused.value)
    assert reads == [], "the data is never read in a format the contract does not declare"


def test_a_read_format_that_agrees_with_the_contract_is_read_in_it():
    write_contract(inputs=orders_input(), outputs=output())
    with invaract.run("contract.yaml"):
        assert invaract.read("orders", format="PARQUET", as_="arrow").column_names == ["order_id", "region"]
        invaract.write(REPORT, "report")


def test_a_dry_runs_draft_declares_the_format_each_input_was_read_in(monkeypatch):
    orders_input()
    notify_to_file(monkeypatch)
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    with invaract.run(name="orders_job.main"):
        invaract.read("orders", location="data/in/orders")
        invaract.write(REPORT, "report", location="data/out/report")
    assert "location: data/in/orders\n  format: parquet\n" in event_of("DRY_RUN_SUMMARY")["mergedContractYaml"]


def test_a_format_or_save_mode_that_disagrees_with_the_contract_is_refused():
    report_contract()
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report", format="csv")
    assert refused.value.violations == ["OUTPUT_FORMAT_MISMATCH"]
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report", save_mode="append")
    assert refused.value.violations == ["OUTPUT_SAVE_MODE_MISMATCH"]


def test_a_csv_output():
    report_contract(fmt="csv")
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    files, _ = published()
    assert len(files) == 1 and files[0].endswith(".csv")
    with open(os.path.join("data/out/report", files[0])) as f:
        assert f.read().splitlines() == ['"region","total"', '"north",10', '"south",20']


def existing(location="data/out/report", name="old.parquet"):
    os.makedirs(location, exist_ok=True)
    pq.write_table(pa.table({"region": ["old"], "total": pa.array([0], pa.int64())}), os.path.join(location, name))


def test_overwrite_replaces_what_was_there():
    existing()
    report_contract(save_mode="overwrite")
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    files, leftovers = published()
    assert "old.parquet" not in files and len(files) == 1 and leftovers == []


def test_append_adds_files_named_after_the_run_and_keeps_what_was_there():
    existing()
    report_contract(save_mode="append")
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
    files, leftovers = published()
    assert files[0] == "old.parquet" and len(files) == 2 and run.run_id in files[1] and leftovers == []
    assert pq.read_table("data/out/report").num_rows == 3


def test_one_output_written_twice_in_a_run_publishes_both_writes():
    existing()
    report_contract(save_mode="overwrite")
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
        invaract.write(REPORT, "report")
    files, leftovers = published()
    assert len(files) == 2 and all(run.run_id in f for f in files) and leftovers == []
    assert pq.read_table("data/out/report").num_rows == 2 * len(REPORT)


def test_append_to_an_output_that_does_not_exist_yet():
    report_contract(save_mode="append")
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert pq.read_table("data/out/report").num_rows == 2


def test_error_refuses_an_output_that_already_holds_data_and_publishes_nothing():
    existing()
    write_contract(outputs=output(save_mode="error") + output(name="audit", location="data/out/audit", fields=[("region", "string", True)]))
    with pytest.raises(FileExistsError):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT[["region"]], "audit")
            invaract.write(REPORT, "report")
    assert published() == (["old.parquet"], []) and not os.path.exists("data/out/audit")


def test_error_and_ignore_write_an_output_that_holds_no_data_yet():
    os.makedirs("data/out/report/_meta")
    for mode in ("error", "ignore"):
        report_contract(save_mode=mode)
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
        assert pq.read_table("data/out/report").num_rows == 2
        _remove_tree("data/out/report")


def test_ignore_keeps_what_was_there_and_publishes_nothing_for_that_output():
    existing()
    report_contract(save_mode="ignore")
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert published() == (["old.parquet"], [])


def test_with_no_save_mode_declared_a_write_overwrites():
    existing()
    report_contract(save_mode=None)
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    assert "old.parquet" not in published()[0]


def test_a_format_the_package_cannot_write_yet_is_not_checked_rather_than_written_wrongly():
    report_contract(fmt="text")
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert "cannot yet write 'report' as text with save mode overwrite: it writes parquet, csv, json, orc, avro, feather, excel, delta, iceberg" in str(not_run.value)
    assert not os.path.exists("data/out")


# dry runs: no contract; the job's own locations


def test_a_dry_run_reads_and_writes_where_the_job_says_and_infers_a_contract(monkeypatch):
    orders_input()
    notify_to_file(monkeypatch)
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    with invaract.run(name="orders_job.main") as run:
        assert run.dry_run and run.contract is None
        orders = invaract.read("orders", location="data/in/orders", as_="arrow")
        invaract.write(REPORT, "report", location="data/out/report")
    assert orders.column_names == ["order_id", "region", "extra"], "with no contract, every column"
    assert pq.read_table("data/out/report").num_rows == 2
    kinds = [e["eventType"] for e in events()]
    assert kinds == ["CONTRACT_INFERENCE", "DRY_RUN_SUMMARY", "WRITE", "JOB_SUMMARY"]
    summary = event_of("DRY_RUN_SUMMARY")
    assert "data/out/report" in summary["mergedContractYaml"] and "data/in/orders" in summary["mergedContractYaml"]
    # what a dry run really writes is still a write: it has no contract to name
    written = event_of("WRITE")
    assert (written["contract"], written["location"], written["rowCount"], written["fileCount"]) == (None, "data/out/report", 2, 1)


def test_a_platform_puts_its_facts_about_the_contract_in_a_dry_runs_drafts_from_the_environment(monkeypatch):
    orders_input()
    notify_to_file(monkeypatch)
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    monkeypatch.setenv("INVARACT_CONTRACT_METADATA", "owner=orders-team,source=erp")
    with invaract.run(name="orders_job.main"):
        invaract.read("orders", location="data/in/orders")
        invaract.write(REPORT, "report", location="data/out/report")
    for kind, yaml in (("CONTRACT_INFERENCE", "contractYaml"), ("DRY_RUN_SUMMARY", "mergedContractYaml")):
        event = event_of(kind)
        assert event["metadata"] == {"owner": "orders-team", "source": "erp"}
        assert "owner: orders-team" in event[yaml] and "source: erp" in event[yaml]


def test_a_dry_run_read_or_write_with_no_location_is_not_checked(monkeypatch):
    monkeypatch.setenv("INVARACT_DRY_RUN", "true")
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run():
            invaract.read("orders")
    assert "location" in str(not_run.value)
    with pytest.raises(invaract.CheckNotRun):
        with invaract.run():
            invaract.write(REPORT, "report")


# events and the job's details


def test_the_events_name_the_engine_the_job_python_and_its_libraries(monkeypatch):
    report_contract()
    notify_to_file(monkeypatch)
    with invaract.run("contract.yaml", name="orders_job.main") as run:
        invaract.write(REPORT, "report")
    [event] = [e for e in events() if e["eventType"] == "CONTRACT_VALIDATION"]
    job = event["job"]
    assert (event["status"], job["engine"], job["name"], job["runId"]) == ("PASSED", "pythonio", "orders_job.main", run.run_id)
    assert job["engineVersion"].count(".") == 2
    assert job["engineDetails"]["pyarrow"] == pa.__version__ and job["engineDetails"]["pandas"] == pd.__version__


def test_a_platform_names_the_job_and_its_run_in_every_event_from_the_environment(monkeypatch):
    report_contract()
    notify_to_file(monkeypatch)
    monkeypatch.setenv("INVARACT_JOB_ID", "orders_nightly")
    monkeypatch.setenv("INVARACT_JOB_ATTRIBUTES", "team=orders,dag=orders_daily")
    with invaract.run("contract.yaml", name="orders_job.main"):
        invaract.write(REPORT, "report")
    jobs = [e["job"] for e in events()]
    assert [e["eventType"] for e in events()] == ["CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY"]
    assert all((j["jobId"], j["name"], j["attributes"]) == ("orders_nightly", "orders_job.main", {"team": "orders", "dag": "orders_daily"})
               for j in jobs)


def test_malformed_job_attributes_are_not_checked_and_name_the_variable(monkeypatch):
    report_contract()
    monkeypatch.setenv("INVARACT_JOB_ATTRIBUTES", "team=orders,dag")
    with pytest.raises(invaract.CheckNotRun, match="INVARACT_JOB_ATTRIBUTES.*'dag' is not key=value"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
    assert published() == ([], [])


def test_the_events_list_each_installed_data_library_whether_or_not_the_job_has_imported_it_yet(monkeypatch):
    import importlib.metadata
    import sys

    from invaract import _run

    report_contract()
    notify_to_file(monkeypatch)
    monkeypatch.delitem(sys.modules, "polars", raising=False)
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")
    details = events()[0]["job"]["engineDetails"]
    installed = {}
    for library in _run.LIBRARIES:
        try:
            installed[library] = importlib.metadata.version(library)
        except importlib.metadata.PackageNotFoundError:
            pass
    assert details == installed and {"pyarrow", "pandas"} <= set(details)


def test_a_jobs_name_is_its_function_s_module_and_name_or_for_a_script_the_script_s(monkeypatch):
    from invaract import _run

    def main():
        pass

    assert _run._function_name(main) == f"{__name__}.test_a_jobs_name_is_its_function_s_module_and_name_or_for_a_script_the_script_s.<locals>.main"
    main.__module__ = "__main__"
    monkeypatch.setattr(_run, "_script_name", lambda: "main.py")
    assert _run._function_name(main).startswith("main.test_a_jobs_name")
    # python -m orders_job.main: the module's own name, which a dot does not end
    monkeypatch.setattr(_run, "_script_name", lambda: "orders_job.main")
    assert _run._function_name(main).startswith("orders_job.main.test_a_jobs_name")


def test_the_checker_is_closed_and_its_events_delivered_even_when_the_job_fails(monkeypatch):
    report_contract()
    notify_to_file(monkeypatch)
    with pytest.raises(invaract.ContractViolation):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT.astype({"total": str}), "report")
    assert [(e["eventType"], e.get("status")) for e in events()] == [("CONTRACT_VALIDATION", "FAILED"), ("JOB_SUMMARY", None)]


# a run that cannot be checked raises CheckNotRun, and publishes nothing


def test_no_checker_to_run(monkeypatch):
    monkeypatch.delenv("INVARACT_CHECKER")
    report_contract()
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("contract.yaml"):
            pass
    assert "INVARACT_CHECKER" in str(not_run.value)


def test_a_checker_that_cannot_be_started(monkeypatch):
    monkeypatch.setenv("INVARACT_CHECKER", "/no/such/checker")
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("contract.yaml"):
            pass
    assert "/no/such/checker" in str(not_run.value)


def test_a_checker_that_stops_without_answering(monkeypatch):
    monkeypatch.setenv("INVARACT_CHECKER", "true")
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("contract.yaml"):
            pass
    assert "stopped" in str(not_run.value) and "'open'" in str(not_run.value)


def test_a_checker_whose_reply_is_not_json_is_not_checked(monkeypatch):
    # it reads the request before answering, so the package is never writing to a checker that has already gone
    monkeypatch.setenv("INVARACT_CHECKER", "sh -c 'read request; echo not-a-reply'")
    with pytest.raises(invaract.CheckNotRun, match="answered 'open' with 'not-a-reply', which is not a reply"):
        with invaract.run("contract.yaml"):
            pass


@pytest.mark.parametrize("reply, speaks", [('{"ok": true, "mode": "enforce"}', "unknown"),
                                           ('{"ok": true, "protocol": 2, "mode": "enforce"}', "2")])
def test_a_checker_from_another_release_is_not_checked_against(monkeypatch, reply, speaks):
    # a checker older than the protocol's version, or newer, could read the package's requests otherwise than meant
    from invaract import _checker
    from conftest import UNCHECKED_ASK

    monkeypatch.setattr(_checker.Checker, "ask", UNCHECKED_ASK)
    with open("checker.sh", "w", encoding="utf-8") as f:
        # it answers the open as a checker of another release would, then any finish as every checker does
        f.write(f"read request\necho {shlex.quote(reply)}\nwhile read request; do echo '{{\"ok\": true, \"publish\": false}}'; done\n")
    monkeypatch.setenv("INVARACT_CHECKER", "sh checker.sh")
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("contract.yaml"):
            pass
    assert f"speaks protocol version {speaks}, and this package version 1" in str(not_run.value)
    assert "(sh checker.sh)" in str(not_run.value) and "from the same release" in str(not_run.value)


def _failing_publish(monkeypatch, failing="audit"):
    """Makes publishing the output at ``data/out/<failing>`` fail, as a full disk would."""
    from invaract import _files

    publish = _files.RenamedOutput.publish

    def fails(out):
        if out.target.endswith(failing):
            raise OSError("no space left on device")
        return publish(out)

    monkeypatch.setattr(_files.RenamedOutput, "publish", fails)


def two_outputs():
    existing()
    write_contract(outputs=output() + output(name="audit", location="data/out/audit"))


def test_a_publish_that_fails_part_way_puts_back_what_it_had_published(monkeypatch):
    two_outputs()
    notify_to_file(monkeypatch)
    _failing_publish(monkeypatch)
    with pytest.raises(OSError, match="no space left on device"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT, "audit")
    # the report, published first, is as it was before the run, and nothing is left beside either output
    assert published() == (["old.parquet"], []) and published("data/out/audit") == ([], [])
    assert [e["eventType"] for e in events()] == ["CONTRACT_VALIDATION", "CONTRACT_VALIDATION", "JOB_SUMMARY"]
    assert event_of("JOB_SUMMARY")["totalWrites"] == 0


def test_an_output_whose_storage_cannot_undo_a_publish_stays_published_and_its_write_event_says_so(monkeypatch):
    from invaract import _files

    two_outputs()
    notify_to_file(monkeypatch)
    _failing_publish(monkeypatch)
    # as a Delta or Iceberg commit, a copy to object storage or a database transaction is: done once made
    monkeypatch.setattr(_files.RenamedOutput, "restore", lambda out: False)
    with pytest.raises(OSError, match="no space left on device"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "report")
            invaract.write(REPORT, "audit")
    files, _ = published()
    assert files != ["old.parquet"] and pq.read_table("data/out/report").num_rows == len(REPORT)
    written = event_of("WRITE")
    assert (written["location"], written["rowCount"], written["fileCount"]) == ("data/out/report", len(REPORT), 1)
    assert event_of("JOB_SUMMARY")["totalWrites"] == 1


def test_a_passing_run_publishes_a_write_event_for_each_output_and_a_summary(monkeypatch):
    two_outputs()
    notify_to_file(monkeypatch)
    with invaract.run("contract.yaml") as run:
        invaract.write(REPORT, "report")
        invaract.write(REPORT, "report")
        invaract.write(REPORT, "audit")
    kinds = [e["eventType"] for e in events()]
    assert kinds == ["CONTRACT_VALIDATION"] * 3 + ["WRITE", "WRITE", "JOB_SUMMARY"]
    report, audit = [e for e in events() if e["eventType"] == "WRITE"]
    # one event per output, however many writes staged it, in the order first written
    assert (report["location"], report["rowCount"], report["fileCount"], audit["location"], audit["rowCount"]) == (
        "data/out/report", 2 * len(REPORT), 2, "data/out/audit", len(REPORT))
    assert report["bytesWritten"] == sum(os.path.getsize(os.path.join("data/out/report", f)) for f in os.listdir("data/out/report"))
    assert report["runId"] == run.run_id and report["job"]["engine"] == "pythonio" and report["durationMs"] >= 0
    assert [(f["name"], f["type"], f["nullable"]) for f in report["schema"]] == [("region", "string", False), ("total", "long", False)]
    summary = event_of("JOB_SUMMARY")
    assert (summary["totalWrites"], summary["checksPassed"], summary["checksFailed"], summary["totalViolations"]) == (2, 3, 0, 0)


def test_an_output_that_cannot_be_discarded_leaves_neither_the_others_nor_the_checker_behind(monkeypatch):
    from invaract import _checker, _files, _run

    write_contract(outputs=output(name="first", location="data/out/first") + output(name="second", location="data/out/second"))
    discard, close, closed = _files.RenamedOutput.discard, _checker.Checker.close, []

    def failing(out):
        if out.target.endswith("first"):
            raise OSError("cannot discard first")
        discard(out)

    def closing(checker):
        closed.append(checker)
        close(checker)

    monkeypatch.setattr(_files.RenamedOutput, "discard", failing)
    monkeypatch.setattr(_checker.Checker, "close", closing)
    with pytest.raises(OSError, match="cannot discard first"):
        with invaract.run("contract.yaml"):
            invaract.write(REPORT, "first")
            invaract.write(REPORT, "second")
            raise RuntimeError("the job failed")
    assert published("data/out/second") == ([], []), "the second output is discarded although the first could not be"
    assert len(closed) == 1, "the checker is closed"
    assert _run._current is None, "the run is over, so the process can start another"


def test_a_missing_contract_or_no_contract_at_all_is_not_checked():
    with pytest.raises(invaract.CheckNotRun) as not_run:
        with invaract.run("no-such-contract.yaml"):
            pass
    assert "no-such-contract.yaml" in str(not_run.value)
    with pytest.raises(invaract.CheckNotRun):
        with invaract.run():
            pass


def test_a_capability_this_engine_does_not_have_is_refused_before_the_job_runs(monkeypatch):
    report_contract()
    monkeypatch.setenv("INVARACT_STATIC_DATA_QUALITY", "true")
    ran = []
    with pytest.raises(invaract.ContractViolation) as refused:
        with invaract.run("contract.yaml"):
            ran.append(True)
    assert ran == [] and refused.value.violations == ["UNSUPPORTED_CONTRACT_FEATURE"]


# using the API outside a run


def test_reads_and_writes_need_a_run():
    with pytest.raises(RuntimeError, match="@invaract.job"):
        invaract.read("orders")
    with pytest.raises(RuntimeError, match="@invaract.job"):
        invaract.write(REPORT, "report")


def test_runs_do_not_nest_and_a_finished_run_frees_the_process_for_the_next():
    report_contract()
    with invaract.run("contract.yaml"):
        with pytest.raises(RuntimeError, match="do not nest"):
            with invaract.run("contract.yaml"):
                pass
        invaract.write(REPORT, "report")
    with invaract.run("contract.yaml"):
        invaract.write(REPORT, "report")


def test_an_unknown_frame_kind_is_an_error_before_anything_is_read():
    write_contract(inputs=orders_input(), outputs=output())
    with pytest.raises(ValueError, match="as_="):
        with invaract.run("contract.yaml"):
            invaract.read("orders", as_="spark")


def _remove_tree(path):
    import shutil

    shutil.rmtree(path)
