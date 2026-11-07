# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""What a run writes to: an output, staged while the job runs and published only when the whole run passed.

Every kind of output (plain files, ``_files``; Delta and Iceberg tables, ``_tables``; database tables, ``_postgres`` and
``_sqlite``) is an ``Output``, and a run (``_run``) knows them only as that.
"""
import collections


class Layout(collections.namedtuple("Layout", ["keys", "overwrite"])):
    """How an output lays out its data: the columns it is partitioned by, in order (none: not partitioned), and what an
    ``overwrite`` replaces, ``static`` (every partition) or ``dynamic`` (only those written)."""

    __slots__ = ()

    def describe(self):
        return f"partitioned by {', '.join(self.keys)}" if self.keys else "not partitioned"

    @property
    def dynamic(self):
        """Whether an ``overwrite`` replaces only the partitions written."""
        return bool(self.keys) and self.overwrite == "dynamic"


#: Not partitioned.
FLAT = Layout((), "static")


class Output:
    """One output of a run: where it is (``target``, as the job's messages name it), its save mode and its layout."""

    def __init__(self, target, mode, layout):
        self.target = target
        self.mode = mode
        self.layout = layout

    def has_data(self):
        """Whether the output already holds data (what an ``error`` save mode refuses, and ``ignore`` leaves alone)."""
        raise NotImplementedError

    def stage(self, schema, batches, required):
        """Stages ``batches`` (of ``schema``) where no reader sees them, each column in ``required`` written not-null, and
        returns the empty values each column held (``_frames.take``)."""
        raise NotImplementedError

    def publish(self):
        """Makes what is staged the output's data, as its save mode says."""
        raise NotImplementedError

    def forget_previous(self):
        """Once every output of the run is published: deletes what a publish kept to put back if a later one failed."""

    def discard(self):
        """Deletes what is staged and not published."""
        raise NotImplementedError
