# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A conformance scenario's job (adapter-testkit's ScenarioJob) as a real Python job through the invaract package.

    python conformance_job.py job.json

Run by pythonio-adapter's PythonIoPackageConformanceAdapter, in a folder of its own, with the settings in INVARACT_*
variables as a platform would give them. The job first makes its inputs, as data some earlier job wrote: a few rows, one with
every column set, and for each column the input lets be empty, one where only that column is empty (the package judges a
required column by the empty values the data holds). Then, inside one run, it reads each input through invaract.read, joins,
filters, groups and projects them with pyarrow as the scenario says, and writes the result through invaract.write, with the
format and save mode the job itself uses. It exits 0 when the run published, REFUSED when the contract refused it, and 2 (the
package's own status) when the check could not be made.
"""
import datetime
import decimal
import json
import os
import sys
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

import invaract

REFUSED = 10

_SIMPLE = {
    "string": pa.string(), "boolean": pa.bool_(), "byte": pa.int8(), "short": pa.int16(), "integer": pa.int32(),
    "long": pa.int64(), "float": pa.float32(), "double": pa.float64(), "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"), "timestamp_ntz": pa.timestamp("us"), "binary": pa.binary(),
    "time": pa.time64("us"), "interval": pa.month_day_nano_interval(),
}


def arrow_type(t):
    kind = t["kind"]
    if kind in _SIMPLE:
        return _SIMPLE[kind]
    if kind == "decimal":
        return pa.decimal128(t["precision"], t["scale"])
    if kind == "array":
        return pa.list_(arrow_type(t["element"]))
    if kind == "map":
        return pa.map_(arrow_type(t["key"]), arrow_type(t["value"]))
    if kind == "struct":
        return pa.struct([pa.field(f["name"], arrow_type(f["type"]), nullable=f["nullable"]) for f in t["fields"]])
    raise ValueError(f"no pyarrow type for {t}")


def value(t, row):
    """A value of type ``t`` for row ``row``: numbers are row + 1, so a filter keeping values above zero keeps every row, and the
    same row of two inputs joins on equal keys."""
    kind = t["kind"]
    n = row + 1
    if kind in ("byte", "short", "integer", "long"):
        return n
    if kind in ("float", "double"):
        return float(n)
    if kind == "string":
        return str(n)
    if kind == "boolean":
        return True
    if kind == "binary":
        return str(n).encode()
    if kind == "date":
        return datetime.date(2024, 1, n % 28 + 1)
    if kind in ("timestamp", "timestamp_ntz"):
        return datetime.datetime(2024, 1, n % 28 + 1, tzinfo=datetime.timezone.utc if kind == "timestamp" else None)
    if kind == "time":
        return datetime.time(n % 24)
    if kind == "interval":
        return pa.MonthDayNano([n, 0, 0])
    if kind == "decimal":
        return decimal.Decimal(n)
    if kind == "array":
        return [value(t["element"], row)]
    if kind == "map":
        return [(value(t["key"], row), value(t["value"], row))]
    if kind == "struct":
        return {f["name"]: value(f["type"], row) for f in t["fields"]}
    raise ValueError(f"no value for {t}")


def make_input(dataset):
    """The input's data, at its location, in its format: the rows the scenario gives, when its check is about values; otherwise
    one row with every column set, then one per column it lets be empty, with only that column empty."""
    fields = dataset["fields"]
    nullable = [f["name"] for f in fields if f["nullable"]]
    rows = 1 + len(nullable)
    schema = pa.schema([pa.field(f["name"], arrow_type(f["type"]), nullable=f["nullable"]) for f in fields])
    if dataset.get("rows"):
        columns = [pa.array([row.get(f["name"]) for row in dataset["rows"]], arrow_type(f["type"])) for f in fields]
    else:
        columns = [
            pa.array([None if row > 0 and nullable[row - 1] == f["name"] else value(f["type"], row) for row in range(rows)], arrow_type(f["type"]))
            for f in fields
        ]
    table = pa.Table.from_arrays(columns, schema=schema)
    os.makedirs(dataset["location"], exist_ok=True)
    # the data an earlier job wrote, made before the run: deliberately not through invaract
    if dataset["format"] == "csv" and dataset.get("csv"):
        # laid out with the options the job reads it with: the file a team that reads it that way has
        from invaract import _csv

        out = _csv.Writer(open(os.path.join(dataset["location"], "part-0.csv"), "wb"), table.schema, _csv.options({}, dataset["csv"]))  # invaract: allow
        for batch in table.to_batches():
            out.write_batch(batch)
        out.close()
    elif dataset["format"] == "csv":
        csv.write_csv(table, os.path.join(dataset["location"], "part-0.csv"))  # invaract: allow
    else:
        pq.write_table(table, os.path.join(dataset["location"], "part-0.parquet"))  # invaract: allow


def qualified(table, index):
    """The table's columns named ``<input>:<column>``, so two inputs' columns never collide."""
    return table.rename_columns([f"{index}:{name}" for name in table.column_names])


def transform(job, tables):
    """The scenario's transformation: join (or cross join), filter, repeat-and-merge, group, then project."""
    data = qualified(tables[0], 0)
    if len(tables) == 2:
        right = qualified(tables[1], 1)
        if job["crossJoin"]:
            data = data.append_column("__all", pa.array([0] * data.num_rows))
            right = right.append_column("__all", pa.array([0] * right.num_rows))
            data = data.join(right, keys="__all", join_type="inner").drop_columns(["__all"])
        else:
            data = data.join(right, keys=f"0:{job['join']['left']}", right_keys=f"1:{job['join']['right']}", join_type="inner", coalesce_keys=False)
    if job["filterColumn"]:
        data = data.filter(pc.greater(data[f"0:{job['filterColumn']}"], 0))
    for _ in range(job["rejoins"]):
        data = pa.concat_tables([data, data])
    if job["groupBy"]:
        data = data.group_by([f"0:{c}" for c in job["groupBy"]]).aggregate([([], "count_all")])
    out = {}
    for column in job["columns"]:
        source = column["source"]
        kind = source["kind"]
        if kind == "FromInput":
            out[column["name"]] = data[f"{source['input']}:{source['column']}"]
        elif kind == "CastInput":
            out[column["name"]] = pc.cast(data[f"{source['input']}:{source['column']}"], arrow_type(source["to"]))
        elif kind == "NonNullLong":
            out[column["name"]] = pa.array([source["value"]] * data.num_rows, pa.int64())
        elif kind == "UniqueId":
            out[column["name"]] = pa.array([str(uuid.uuid4()) for _ in range(data.num_rows)], pa.string())
        elif kind == "RowCount":
            out[column["name"]] = pc.cast(data["count_all"], pa.int64())
        else:
            raise ValueError(f"unknown column source {source}")
    return pa.table(out)


def main(job):
    for dataset in job["inputs"] + [e["input"] for e in job["extraWrites"]]:
        make_input(dataset)
    with invaract.run(name=job["scenario"]):
        tables = [invaract.read(i["name"], as_="arrow", format=i["format"], csv=i.get("csv")) for i in job["inputs"]]
        output = job["output"]
        if job["untranslatableWrite"]:
            # a table the package cannot describe: no Arrow stream
            invaract.write({"rows": tables[0].num_rows}, output["name"])
        else:
            invaract.write(transform(job, tables), output["name"], format=output["format"], save_mode=output["saveMode"],
                           partition_by=output.get("partitionBy", []), partition_overwrite=output.get("partitionOverwriteMode", "static"),
                           csv=output.get("csv"))
        read = {i["location"]: t for i, t in zip(job["inputs"], tables)}
        for extra in job["extraWrites"]:
            source = extra["input"]
            if source["location"] not in read:
                read[source["location"]] = invaract.read(source["name"], as_="arrow", format=source["format"])
            invaract.write(read[source["location"]], extra["output"]["name"], format=extra["output"]["format"], save_mode=extra["output"]["saveMode"])


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as f:
        scenario = json.load(f)
    try:
        main(scenario)
    except invaract.ContractViolation as refused:
        print(f"REFUSED {refused.violations}", file=sys.stderr)
        sys.exit(REFUSED)
