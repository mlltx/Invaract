# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A conformance scenario's job as a real Python Beam pipeline: the Python counterpart of beam-adapter's BeamScenarioJobs.

beam-adapter's BeamPythonConformanceAdapter writes the neutral job (a ScenarioJob) as JSON and runs

    python conformance_job.py <job.json> --runner=invaract_beam.InvaractRunner --invaract_contract=... [other flags]

so the job goes through the adapter's real path: the wrapper runner, the Java checker, then the real runner. Nothing here
knows about Invaract beyond telling a refusal from a failure by its exit status: it is what a team's job looks like.

Every input is an empty Parquet dataset read with ReadFromParquet(as_rows=True); the join is a CoGroupByKey (or, with no
condition, a side input); the filter a beam.Filter; the projection a typed beam.Map; the output a WriteToParquet with the
schema it writes, rows written straight to text for a CSV output, or a WriteToBigQuery load. The data is empty: the
scenarios are about shapes, and an allowed job really runs on the DirectRunner over nothing.
"""
import json
import os
import sys
import typing
import uuid

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineRunner
from apache_beam.transforms import window
from apache_beam.typehints.row_type import RowTypeConstraint

from invaract_beam import CheckNotRun, ContractViolation

# The exit statuses the adapter reads: the contract refused the job, or it could not be checked. 0 is a job that ran.
REFUSED = 10
NOT_CHECKED = 11


class CheckOnlyRunner(PipelineRunner):
    """A delegate that runs nothing: for a job whose sink has no in-process stand-in (BigQuery) or that never ends (a stream).
    The check happens before the hand-over either way; this only skips what comes after it."""

    def run_pipeline(self, pipeline, options):
        return None


def arrow_type(name):
    """The pyarrow type of a column type as the contract spells it (catalogString)."""
    if name.startswith("array<") and name.endswith(">"):
        return pa.list_(arrow_type(name[len("array<"):-1]))
    return {"bigint": pa.int64(), "string": pa.string()}[name]


def python_type(name, nullable):
    if name.startswith("array<") and name.endswith(">"):
        t = typing.List[python_type(name[len("array<"):-1], False)]
    else:
        t = {"bigint": int, "string": str}[name]
    return typing.Optional[t] if nullable else t


def arrow_schema(fields):
    return pa.schema([pa.field(f["name"], arrow_type(f["type"]), nullable=f["nullable"]) for f in fields])


class row_type:
    """A typed row: elements are beam.Row, and the step that makes them declares this type, so Beam gives the collection a
    row schema (a RowTypeConstraint, Beam's way of typing rows a job builds at run time)."""

    def __init__(self, name, fields):
        self.names = [f["name"] for f in fields]
        self.hint = RowTypeConstraint.from_fields([(f["name"], python_type(f["type"], f["nullable"])) for f in fields])

    def __call__(self, **values):
        return beam.Row(**{n: values[n] for n in self.names})


def as_dict(row):
    return row._asdict() if hasattr(row, "_asdict") else row.as_dict()


def materialize(root, dataset):
    """An empty Parquet file of the dataset's schema in its folder, so a read of the folder finds it."""
    folder = os.path.join(root, dataset["location"])
    os.makedirs(folder, exist_ok=True)
    schema = arrow_schema(dataset["fields"])
    pq.write_table(pa.table({f.name: pa.array([], f.type) for f in schema}, schema=schema), os.path.join(folder, "part-0.parquet"))
    return os.path.join(folder, "*.parquet")


def read(p, root, dataset, label, streaming):
    pattern = materialize(root, dataset)
    if not streaming:
        return p | label >> beam.io.ReadFromParquet(pattern, as_rows=True)
    t = row_type(label + "Row", dataset["fields"])
    return (p | label + "Watch" >> fileio.MatchContinuously(pattern, interval=1.0)
              | label + "Read" >> beam.io.ReadAllFromParquet()
              | label + "Rows" >> beam.Map(lambda d: t(**d)).with_output_types(t.hint)
              | label + "Window" >> beam.WindowInto(window.FixedWindows(60)))


def merged_fields(job):
    """The fields of two joined inputs, each prefixed with its side (l_, r_), as BeamScenarioJobs names them."""
    return [dict(f, name=f"{side}_{f['name']}") for side, i in (("l", 0), ("r", 1)) for f in job["inputs"][i]["fields"]]


def merge(left, right, merged):
    values = {f"l_{k}": v for k, v in as_dict(left).items()}
    values.update({f"r_{k}": v for k, v in as_dict(right).items()})
    return merged(**values)


def combine(reads, job):
    """The rows the rest of the job works on, and the field that holds (input, column)."""
    if len(reads) == 1:
        return reads[0], lambda i, column: column
    merged = row_type("Joined", merged_fields(job))
    left, right = reads
    if job.get("join"):
        on_left, on_right = job["join"]["left"], job["join"]["right"]
        rows = ({"lhs": left | "KeyLeft" >> beam.Map(lambda r: (getattr(r, on_left), r)),
                 "rhs": right | "KeyRight" >> beam.Map(lambda r: (getattr(r, on_right), r))}
                | "Join" >> beam.CoGroupByKey()
                | "Merge" >> beam.FlatMap(lambda kv: [merge(l, r, merged) for l in kv[1]["lhs"] for r in kv[1]["rhs"]]).with_output_types(merged.hint))
    else:
        rows = left | "CrossJoin" >> beam.FlatMap(
            lambda l, rights: [merge(l, r, merged) for r in rights], beam.pvalue.AsList(right)).with_output_types(merged.hint)
    return rows, lambda i, column: f"{'l' if i == 0 else 'r'}_{column}"


def value_of(source, row, field_of, count=None):
    kind = source["kind"]
    if kind == "FromInput":
        return getattr(row, field_of(source["input"], source["column"]))
    if kind == "CastInput":
        v = getattr(row, field_of(source["input"], source["column"]))
        return None if v is None else {"string": str, "bigint": int}[source["to"]](v)
    if kind == "NonNullLong":
        return source["value"]
    if kind == "UniqueId":
        return str(uuid.uuid4())
    if kind == "RowCount":
        return count
    raise ValueError(f"no Python job for the column source {kind}")


def build(p, job, root, target):
    reads = [read(p, root, dataset, f"Read{i}", job["streaming"]) for i, dataset in enumerate(job["inputs"])]
    if job["untranslatableWrite"]:
        # an operation that changes data and that the adapter has no translation for: a function pushing every row somewhere
        _ = reads[0] | "PushToSomeService" >> beam.Map(print)
        return
    if job.get("filterColumn"):
        column = job["filterColumn"]
        reads[0] = reads[0] | "Filter" >> beam.Filter(lambda r: getattr(r, column) is not None and getattr(r, column) > 0)
    rows, field_of = combine(reads, job)
    if job["opaqueStep"]:
        rows = rows | "OpaqueStep" >> beam.Reshuffle()
    out = row_type("Report", job["outputFields"])
    columns = job["columns"]
    if job.get("groupBy"):
        keys = [field_of(0, k) for k in job["groupBy"]]
        shaped = (rows
                  | "Key" >> beam.Map(lambda r: (tuple(getattr(r, k) for k in keys), r))
                  | "Group" >> beam.GroupByKey()
                  | "Count" >> beam.Map(lambda kv: out(**{c["name"]: value_of(c["source"], list(kv[1])[0], field_of, len(list(kv[1]))) for c in columns}))
                  .with_output_types(out.hint))
    else:
        shaped = rows | "Project" >> beam.Map(lambda r: out(**{c["name"]: value_of(c["source"], r, field_of) for c in columns})).with_output_types(out.hint)
    write(shaped, job, root, target)
    # the job's further writes, after its main one: each a copy of an input, in parquet
    for i, extra in enumerate(job["extraWrites"]):
        copied = read(p, root, extra["input"], f"ReadExtra{i}", False)
        _ = (copied | f"Dicts{i}" >> beam.Map(as_dict)
                    | f"WriteExtra{i}" >> beam.io.WriteToParquet(os.path.join(root, extra["output"]["location"]), arrow_schema(extra["input"]["fields"])))


DISPOSITIONS = {"append": "WRITE_APPEND", "overwrite": "WRITE_TRUNCATE", "error": "WRITE_EMPTY"}


def write(rows, job, root, target):
    output = job["output"]
    if target == "bigquery":
        dataset, table = "_".join(output["location"].split("/")[:-1]), output["location"].split("/")[-1]
        schema = {"fields": [bigquery_field(f) for f in job["outputFields"]]}
        _ = (rows | "Dicts" >> beam.Map(as_dict)
                  | "Write" >> beam.io.WriteToBigQuery(
                      f"conformance:{dataset}.{table}", schema=schema, write_disposition=DISPOSITIONS[output["saveMode"]],
                      create_disposition="CREATE_IF_NEEDED", method="FILE_LOADS"))
        return
    destination = os.path.join(root, output["location"])
    frequency = {"triggering_frequency": 60} if job["streaming"] else {}
    if output["format"] == "parquet":
        _ = (rows | "Dicts" >> beam.Map(as_dict)
                  | "Write" >> beam.io.WriteToParquet(destination, arrow_schema(job["outputFields"]), file_name_suffix=".parquet", **frequency))
    elif output["format"] == "csv":
        # rows written straight to text: the write's columns are the rows'
        _ = rows | "Write" >> beam.io.WriteToText(destination, file_name_suffix=".csv", **frequency)
    else:
        raise ValueError(f"this adapter's jobs write parquet or csv, not {output['format']!r}")


def bigquery_field(f):
    if f["type"].startswith("array<"):
        return {"name": f["name"], "type": {"bigint": "INT64", "string": "STRING"}[f["type"][len("array<"):-1]], "mode": "REPEATED"}
    return {"name": f["name"], "type": {"bigint": "INT64", "string": "STRING"}[f["type"]], "mode": "NULLABLE" if f["nullable"] else "REQUIRED"}


def main(argv):
    with open(argv[1]) as f:
        job = json.load(f)
    p = beam.Pipeline(options=PipelineOptions(argv[2:]))
    build(p, job, job["root"], job["target"])
    try:
        result = p.run()
        if result is not None:
            result.wait_until_finish()
    except ContractViolation as e:
        print(e)
        return REFUSED
    except CheckNotRun as e:
        print(e)
        return NOT_CHECKED
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
