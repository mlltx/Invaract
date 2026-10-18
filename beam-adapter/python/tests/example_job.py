# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A team's job that knows nothing about Invaract: it reads orders, keeps the positive ones and writes a report.

beam-adapter's BeamPythonAttachSpec runs it as a platform would, switching Invaract on with flags alone:

    python example_job.py --root=<folder> [--output=out/report] --runner=invaract_beam.InvaractRunner --invaract_contract=...

``--pin_runner`` makes it choose its own runner in code, which is how a job escapes the command line (the limit the Python
adapter's declaration states).
"""
import argparse
import sys

import apache_beam as beam
import pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions

SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False), ("amount", pa.int64())])


def build(p, root, output):
    (p | "ReadOrders" >> beam.io.ReadFromParquet(f"{root}/in/orders/*.parquet")
       | "Positive" >> beam.Filter(lambda r: (r["amount"] or 0) > 0)
       | "WriteReport" >> beam.io.WriteToParquet(f"{root}/{output}", SCHEMA, file_name_suffix=".parquet"))


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", default="out/report")
    parser.add_argument("--pin_runner", action="store_true")
    known, beam_args = parser.parse_known_args(argv)
    options = PipelineOptions(beam_args, runner="DirectRunner") if known.pin_runner else PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        build(p, known.root, known.output)


if __name__ == "__main__":
    run(sys.argv[1:])
