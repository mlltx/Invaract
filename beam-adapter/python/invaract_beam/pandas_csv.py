# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What a pipeline's pandas CSV reads and writes (``beam.io.ReadFromCsv``, ``beam.io.WriteToCsv``) are made with, stated on its
portable form for the checker.

The portable form states nothing about them: their path and their options are pickled inside the pandas steps they expand
to, which the Java checker cannot read. The live transform objects still hold both when the wrapper runner gets the
pipeline, so ``annotate`` reads them there and adds to each such step what a Python file read or write states for itself: a
``file_pattern`` display item (the read's path, the write's file prefix), published under a namespace of this module that
names its format, and a ``csv_options`` item, the options under the contract's names (docs/BEAM_PYTHON_ADAPTER.md, D12).

The options are what pandas reads or writes with: the job's, and pandas' own default for each one it leaves unset. Only what
pandas states without guessing is given: a delimiter pandas sniffs (``sep=None``) or a regular expression separator is not a
delimiter, a read's missing-value text is given only when the job turned pandas' list of them off for one of its own, and a
date or timestamp pattern (strftime, not the contract's Java pattern) is never given. An option not given is one the checker
refuses to pass unchecked.
"""
import csv
import json

from apache_beam.dataframe import io as dataframe_io
from apache_beam.pipeline import PipelineVisitor
from apache_beam.portability.api import beam_runner_api_pb2

#: The namespaces the two steps' display items are published under: what the checker recognises them by.
READ = "invaract_beam.pandas_csv.ReadFromCsv"
WRITE = "invaract_beam.pandas_csv.WriteToCsv"

_LABELLED = "beam:display_data:labelled:v1"


def annotate(pipeline, proto):
    """Adds to ``proto`` (``pipeline``'s portable form) the path and the CSV options of each pandas CSV read and write."""
    by_name = {t.unique_name: t for t in proto.components.transforms.values()}
    for label, namespace, path, options in csv_steps(pipeline):
        step = by_name.get(label)
        if step is not None:
            step.display_data.append(_item(namespace, "file_pattern", path))
            step.display_data.append(_item(namespace, "csv_options", json.dumps(options, sort_keys=True)))
    return proto


def csv_steps(pipeline):
    """``(step name, namespace, path, options)`` for each pandas CSV read and write in ``pipeline``."""
    found = []

    class Visitor(PipelineVisitor):
        def enter_composite_transform(self, applied):
            self.visit_transform(applied)

        def visit_transform(self, applied):
            step = describe(applied.transform)
            if step is not None:
                found.append((applied.full_label,) + step)

    pipeline.visit(Visitor())
    return found


def describe(transform):
    """``(namespace, path, options)`` when ``transform`` is a pandas CSV read or write, else ``None``."""
    if isinstance(transform, dataframe_io.ReadViaPandas):
        reader = getattr(transform, "_reader", None)
        if getattr(reader, "reader", None) is not None and getattr(reader.reader, "__name__", "") == "read_csv":
            return READ, reader.path, read_options(reader.kwargs)
    if isinstance(transform, dataframe_io.WriteViaPandas) and getattr(transform, "_writer_func", None) is dataframe_io.to_csv:
        if transform._args and isinstance(transform._args[0], str):
            return WRITE, transform._args[0], write_options(transform._kwargs)
    return None


def read_options(kwargs):
    """The options ``pandas.read_csv`` reads with, given the job's ``kwargs``, under the contract's names."""
    options = _common(kwargs)
    header = kwargs.get("header", "infer")
    # pandas' 'infer': the first line names the columns unless the job names them itself
    options["header"] = str(kwargs.get("names") is None if header == "infer" else header is not None).lower()
    na_values = kwargs.get("na_values")
    if kwargs.get("keep_default_na", True) is False and isinstance(na_values, (list, tuple)) and len(na_values) == 1:
        na_values = na_values[0]
    if kwargs.get("keep_default_na", True) is False and isinstance(na_values, str):
        options["nullValue"] = na_values
    return options


def write_options(kwargs):
    """The options ``DataFrame.to_csv`` writes with, given the job's ``kwargs``, under the contract's names."""
    options = _common(kwargs)
    options["header"] = str(kwargs.get("header", True) is not False).lower()
    options["nullValue"] = kwargs.get("na_rep", "")
    return options


def _common(kwargs):
    options = {"encoding": kwargs.get("encoding") or "utf-8"}
    separator = kwargs["delimiter"] if kwargs.get("delimiter") is not None else kwargs.get("sep", ",")
    if isinstance(separator, str) and len(separator) == 1:
        options["delimiter"] = separator
    # QUOTE_NONE: values are never quoted, so there is no quote character
    options["quote"] = "" if kwargs.get("quoting") == csv.QUOTE_NONE else (kwargs.get("quotechar") or '"')
    options["escape"] = kwargs.get("escapechar") or ""
    return options


def _item(namespace, key, value):
    payload = beam_runner_api_pb2.LabelledPayload(label=key, key=key, namespace=namespace, string_value=value)
    return beam_runner_api_pb2.DisplayData(urn=_LABELLED, payload=payload.SerializeToString())
