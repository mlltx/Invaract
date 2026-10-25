"""Records the portable form of small Python pipelines as test fixtures for the Java side of the Beam Python adapter.

The Java translator is tested against these recordings (beam-adapter/src/test/resources/portable/*.pb), so its unit tests
need no Python. Re-record them after moving to a new Python SDK release, and let the Java tests say what changed:

    cd "$(mktemp -d)" && python /path/to/beam-adapter/python/tests/record_fixtures.py /path/to/beam-adapter/src/test/resources/portable

The pipelines are built, never run. Paths are relative (data/in/...) so the recordings do not depend on where they were made;
the script creates the few input files a read checks for when the pipeline is built. Each step's own payload (its pickled
Python code) is cleared, since the checker never reads it and it is most of the size; coders, which carry the row schemas,
are kept as they are.
"""
import os
import sys
import typing

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions


class Order(typing.NamedTuple):
    id: int
    amount: typing.Optional[int]


class Report(typing.NamedTuple):
    id: int
    amount: typing.Optional[int]
    tags: typing.List[str]


beam.coders.registry.register_coder(Order, beam.coders.RowCoder)
beam.coders.registry.register_coder(Report, beam.coders.RowCoder)

ORDERS = "data/in/orders/*.parquet"
REPORT_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    ("amount", pa.int64()),
    ("tags", pa.list_(pa.string())),
    ("placed", pa.timestamp("us", tz="UTC")),
    ("price", pa.decimal128(10, 2)),
    ("customer", pa.struct([("name", pa.string()), pa.field("since", pa.date32(), nullable=False)])),
    ("attrs", pa.map_(pa.string(), pa.int32())),
])
ORDER_SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False), ("amount", pa.int64())])
AVRO_SCHEMA = {"type": "record", "name": "Report", "fields": [{"name": "id", "type": "long"}, {"name": "amount", "type": ["null", "long"]}]}
BQ_SCHEMA = {"fields": [{"name": "id", "type": "INT64", "mode": "REQUIRED"}, {"name": "tags", "type": "STRING", "mode": "REPEATED"}]}


def options(*extra):
    return PipelineOptions(["--project=proj", "--temp_location=gs://bucket/tmp", *extra])


def parquet_job(p):
    """The survey's example: Parquet rows in, a filter, typed rows out, a Parquet write with the schema it writes."""
    (p | "ReadOrders" >> beam.io.ReadFromParquet(ORDERS, as_rows=True)
       | "Positive" >> beam.Filter(lambda r: r.amount is not None and r.amount > 0)
       | "Project" >> beam.Map(lambda r: Report(r.id, r.amount, [])).with_output_types(Report)
       | "WriteReport" >> beam.io.WriteToParquet("data/out/report", REPORT_SCHEMA, file_name_suffix=".parquet"))


def parquet_dicts(p):
    """A Parquet read that yields Python dictionaries (no row schema), written as text."""
    (p | "ReadOrders" >> beam.io.ReadFromParquet(ORDERS, columns=["id"])
       | "Format" >> beam.Map(str)
       | "WriteLines" >> beam.io.WriteToText("data/out/lines", file_name_suffix=".txt"))


def rows_to_text(p):
    """Rows written straight to text: the write's columns are the rows'."""
    (p | "ReadOrders" >> beam.io.ReadFromParquet(ORDERS, as_rows=True)
       | "Typed" >> beam.Map(lambda r: Order(r.id, r.amount)).with_output_types(Order)
       | "WriteCsv" >> beam.io.WriteToText("data/out/report", file_name_suffix=".csv"))


def text_and_avro(p):
    lines = p | "ReadLines" >> beam.io.ReadFromText("data/in/lines/*.txt", validate=False)
    _ = lines | "WriteLines" >> beam.io.WriteToText("data/out/lines/")
    avro = p | "ReadAvro" >> beam.io.ReadFromAvro("data/in/avro/*.avro", validate=False)
    _ = avro | "WriteAvro" >> beam.io.WriteToAvro("data/out/avro/part", AVRO_SCHEMA)
    tf = p | "ReadRecords" >> beam.io.ReadFromTFRecord("data/in/records/*", validate=False)
    _ = tf | "WriteRecords" >> beam.io.WriteToTFRecord("data/out/records/part")


def bigquery_job(p):
    table = p | "ReadTable" >> beam.io.ReadFromBigQuery(table="proj:ds.orders")
    _ = p | "ReadDirect" >> beam.io.ReadFromBigQuery(table="proj:ds.customers", method="DIRECT_READ")
    _ = p | "ReadQuery" >> beam.io.ReadFromBigQuery(query="SELECT id FROM ds.orders", use_standard_sql=True)
    _ = table | "Load" >> beam.io.WriteToBigQuery(
        "proj:ds.report", schema=BQ_SCHEMA, write_disposition="WRITE_TRUNCATE", create_disposition="CREATE_IF_NEEDED", method="FILE_LOADS")
    _ = table | "Stream" >> beam.io.WriteToBigQuery("proj:ds.events", write_disposition="WRITE_APPEND", method="STREAMING_INSERTS")
    _ = table | "Dynamic" >> beam.io.WriteToBigQuery(lambda row: "proj:ds.by_day", method="STREAMING_INSERTS")


def pubsub_job(p):
    (p | "ReadEvents" >> beam.io.ReadFromPubSub(topic="projects/my-project/topics/events")
       | "WriteAlerts" >> beam.io.WriteToPubSub(topic="projects/my-project/topics/alerts"))


def pandas_csv(p):
    """CSV read and written through pandas: the SDK states no location for either."""
    rows = p | "ReadCsv" >> beam.io.ReadFromCsv("data/in/orders.csv")
    _ = rows | "WriteCsv" >> beam.io.WriteToCsv("data/out/report")


def unlocated_read(p):
    """A read that states no location (CSV through pandas) feeding a write the adapter knows."""
    (p | "ReadCsv" >> beam.io.ReadFromCsv("data/in/orders.csv")
       | "Typed" >> beam.Map(lambda r: Order(r.id, r.amount)).with_output_types(Order)
       | "WriteReport" >> beam.io.WriteToParquet("data/out/report", ORDER_SCHEMA))


class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input", type=str)
        parser.add_value_provider_argument("--output", type=str)


def template_job(p):
    o = p.options.view_as(TemplateOptions)
    (p | "ReadInput" >> beam.io.ReadFromText(o.input)
       | "WriteOutput" >> beam.io.WriteToText(o.output))


def side_effects(p):
    """A write the adapter knows, a step that ends the pipeline without being one, a step that only reads what the write
    reports, and data the job makes itself."""
    rows = p | "ReadOrders" >> beam.io.ReadFromParquet(ORDERS, as_rows=True)
    written = rows | "WriteReport" >> beam.io.WriteToParquet("data/out/report", REPORT_SCHEMA)
    _ = written | "Notify" >> beam.Map(print)
    _ = rows | "PushToService" >> beam.Map(print)
    made = p | "Make" >> beam.Create([Order(1, 2)]).with_output_types(Order)
    _ = made | "WriteMade" >> beam.io.WriteToText("data/out/made")


def joined(p):
    """Two reads combined by CoGroupByKey, and an untyped Select."""
    orders = p | "ReadOrders" >> beam.io.ReadFromParquet(ORDERS, as_rows=True)
    lines = p | "ReadLines" >> beam.io.ReadFromText("data/in/lines/*.txt", validate=False)
    keyed = orders | "KeyOrders" >> beam.Map(lambda r: (r.id, r))
    keyed_lines = lines | "KeyLines" >> beam.Map(lambda l: (int(l), l))
    ({"orders": keyed, "lines": keyed_lines}
     | "Join" >> beam.CoGroupByKey()
     | "Merge" >> beam.Map(lambda kv: Order(kv[0], None)).with_output_types(Order)
     | "WriteJoined" >> beam.io.WriteToText("data/out/joined"))
    (orders
     | "Select" >> beam.Select("id", doubled=lambda r: r.amount * 2)
     | "WriteSelected" >> beam.io.WriteToText("data/out/selected"))
    ((orders, keyed) | "Union" >> beam.Flatten() | "WriteUnion" >> beam.io.WriteToText("data/out/union"))


def unknown_sources(p):
    """Reads the adapter cannot locate: a source of its own, an Impulse, files matched by fileio."""
    class OwnSource(beam.io.iobase.BoundedSource):
        def estimate_size(self):
            return 0
    _ = p | "ReadOwn" >> beam.io.Read(OwnSource()) | "WriteOwn" >> beam.io.WriteToText("data/out/own")
    _ = p | "Start" >> beam.Impulse() | "WriteStart" >> beam.io.WriteToText("data/out/start")
    _ = (p | "Match" >> fileio.MatchFiles("data/in/files/*") | "Open" >> fileio.ReadMatches()
           | "Names" >> beam.Map(lambda f: f.metadata.path) | "WriteNames" >> beam.io.WriteToText("data/out/names"))


def example_job(p):
    """The team's job BeamPythonAttachSpec attaches Invaract to (example_job.py), with its folders under data/."""
    import example_job as job
    job.build(p, "data", "out/report")


JOBS = [example_job, parquet_job, parquet_dicts, rows_to_text, text_and_avro, bigquery_job, pubsub_job, pandas_csv, unlocated_read, template_job,
        side_effects, joined, unknown_sources]


def main(out):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    os.makedirs("data/in/orders", exist_ok=True)
    pq.write_table(pa.table({"id": pa.array([], pa.int64()), "amount": pa.array([], pa.int64())}), "data/in/orders/part-0.parquet")
    with open("data/in/orders.csv", "w") as f:
        f.write("id,amount\n1,2\n")
    os.makedirs(out, exist_ok=True)
    for job in JOBS:
        p = beam.Pipeline(options=options())
        job(p)
        proto = p.to_runner_api()
        for transform in proto.components.transforms.values():
            transform.spec.payload = b""
        for environment in proto.components.environments.values():
            environment.payload = b""
            del environment.dependencies[:]
        with open(os.path.join(out, job.__name__ + ".pb"), "wb") as f:
            f.write(proto.SerializeToString())
        print(f"{job.__name__}: {len(proto.components.transforms)} steps, {proto.ByteSize()} bytes")


if __name__ == "__main__":
    main(sys.argv[1])
