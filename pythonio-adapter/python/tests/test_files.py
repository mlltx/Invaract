# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""A plain-file output renamed into place (``_files.RenamedOutput``) behaves the same on the local file system as through a
pyarrow file system, which is how it reaches HDFS. Here the pyarrow file system is the local one, so every run of the suite
holds the HDFS code path to the local one, publish mode by publish mode (a whole run on HDFS: test_tables.py, through a local
file system standing in for it, and test_storage.py, on a real cluster)."""
import os

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from invaract import _files, _outputs

ROWS = pa.table({"region": ["north", "south"], "total": pa.array([10, 20], pa.int64())})
DYNAMIC = _outputs.Layout(("region",), "dynamic")


@pytest.fixture(params=["local", "pyarrow"])
def files(request):
    return _files.LocalFiles() if request.param == "local" else _files.ArrowFiles(pafs.LocalFileSystem())


def renamed(files, mode, layout=_outputs.FLAT, fmt="parquet", csv=None, run_id="run1"):
    path = os.path.abspath("out")
    return _files.RenamedOutput(path, path, files, fmt, mode, run_id, layout, csv)


def write(out, table=ROWS, required=()):
    nulls = out.stage(table.schema, iter(table.to_batches()), set(required))
    out.publish()
    out.forget_previous()
    return nulls


def contents():
    """Every file under the output, relative to it, and what is left beside it."""
    found = sorted(os.path.relpath(os.path.join(d, f), "out").replace(os.sep, "/") for d, _, fs in os.walk("out") for f in fs)
    return found, sorted(f for f in os.listdir(".") if f.startswith(".out."))


def test_an_overwrite_replaces_the_output_and_leaves_nothing_beside_it(files):
    write(renamed(files, "overwrite"))
    write(renamed(files, "overwrite", run_id="run2"))
    assert contents() == (["part-run2-00001.parquet"], [])
    assert pq.read_table("out").to_pydict() == ROWS.to_pydict()


def test_an_append_keeps_what_was_there(files):
    write(renamed(files, "overwrite"))
    write(renamed(files, "append", run_id="run2"))
    assert contents() == (["part-run1-00001.parquet", "part-run2-00001.parquet"], [])


def test_ignore_publishes_nothing_to_an_output_that_holds_data(files):
    write(renamed(files, "overwrite"))
    out = renamed(files, "ignore", run_id="run2")
    assert out.has_data()
    write(out)
    assert contents() == (["part-run1-00001.parquet"], [])


def test_a_dynamic_overwrite_replaces_only_the_partitions_written(files):
    write(renamed(files, "overwrite", DYNAMIC))
    write(renamed(files, "overwrite", DYNAMIC, run_id="run2"), ROWS.slice(0, 1))
    assert contents() == (["region=north/part-run2-00001-00001-0.parquet", "region=south/part-run1-00001-00001-0.parquet"], [])


def test_a_partitioned_csv_with_options_is_converted_into_its_folders(files):
    write(renamed(files, "overwrite", DYNAMIC, fmt="csv", csv={"delimiter": ";"}))
    assert contents() == (["region=north/part-run1-00001-00001-0.csv", "region=south/part-run1-00001-00001-0.csv"], [])
    with open("out/region=south/part-run1-00001-00001-0.csv", encoding="utf-8") as f:
        assert f.read() == "total\n20\n"


def test_a_write_that_fails_part_way_leaves_nothing_staged(files):
    out = renamed(files, "overwrite", DYNAMIC)

    def failing():
        yield from ROWS.to_batches()
        raise RuntimeError("the job's frame failed")

    with pytest.raises(RuntimeError):
        out.stage(ROWS.schema, failing(), set())
    assert files.files(out.staging) == []
    out.discard()
    assert contents() == ([], [])


def test_a_required_columns_empty_value_ends_the_write(files):
    with_null = pa.table({"region": ["north", None], "total": pa.array([10, 20], pa.int64())})
    nulls = write(renamed(files, "overwrite"), with_null, required={"region"})
    assert nulls == {"region": 1, "total": 0}


# what a publish reports, and putting it back


def test_a_publish_reports_the_files_and_bytes_it_put_in_place(files):
    out = renamed(files, "overwrite", layout=DYNAMIC)
    out.stage(ROWS.schema, iter(ROWS.to_batches()), set())
    published = out.publish()
    on_disk = [os.path.join("out", f) for f in contents()[0]]
    assert published == _outputs.Published(files=2, bytes=sum(os.path.getsize(f) for f in on_disk))
    assert published.delta_version is None and published.iceberg_snapshot_id is None


def test_nothing_staged_or_ignored_publishes_nothing(files):
    assert renamed(files, "overwrite").publish() is None
    write(renamed(files, "overwrite"))
    out = renamed(files, "ignore", run_id="run2")
    out.stage(ROWS.schema, iter(ROWS.to_batches()), set())
    assert out.publish() is None and contents() == (["part-run1-00001.parquet"], [])


@pytest.mark.parametrize("mode,layout", [("overwrite", _outputs.FLAT), ("append", _outputs.FLAT), ("overwrite", DYNAMIC)])
def test_a_published_output_is_restored_to_what_it_was_before_the_run(files, mode, layout):
    write(renamed(files, "overwrite", layout=layout))
    before = contents()
    out = renamed(files, mode, layout=layout, run_id="run2")
    more = pa.table({"region": ["north", "east"], "total": pa.array([30, 40], pa.int64())})
    out.stage(more.schema, iter(more.to_batches()), set())
    assert out.publish() is not None and contents() != before
    assert out.restore() is True
    assert contents() == before
    assert out.restore() is True, "restoring twice changes nothing"
    assert contents() == before


class FailingMoves:
    """``files``, whose move number ``fail_at`` (counting from 1) fails, as storage can mid-publish."""

    def __init__(self, files, fail_at):
        self._files, self._left = files, fail_at

    def __getattr__(self, name):
        return getattr(self._files, name)

    def move(self, source, destination):
        self._left -= 1
        if self._left == 0:
            raise OSError("the storage failed")
        self._files.move(source, destination)


# each fails after at least one move of its own has been made: the location moved aside (overwrite), the first file
# appended (append), the first partition replaced and the second moved aside (dynamic overwrite)
@pytest.mark.parametrize("mode,layout,fail_at", [("overwrite", _outputs.FLAT, 2), ("append", DYNAMIC, 2), ("overwrite", DYNAMIC, 3)])
def test_a_publish_that_fails_part_way_leaves_the_output_as_it_was(files, mode, layout, fail_at):
    write(renamed(files, "overwrite", layout=layout))
    before = contents()
    out = renamed(FailingMoves(files, fail_at), mode, layout=layout, run_id="run2")
    more = pa.table({"region": ["north", "east"], "total": pa.array([30, 40], pa.int64())})
    out.stage(more.schema, iter(more.to_batches()), set())
    with pytest.raises(OSError, match="the storage failed"):
        out.publish()
    out.discard()
    assert contents() == before
    assert out.restore() is True and contents() == before, "a publish that did not happen has nothing to put back"


def test_an_output_that_did_not_exist_before_the_run_is_removed_by_a_restore(files):
    out = renamed(files, "overwrite")
    out.stage(ROWS.schema, iter(ROWS.to_batches()), set())
    out.publish()
    assert out.restore() is True and not os.path.exists("out") and contents()[1] == []


def test_an_output_whose_storage_cannot_undo_a_publish_stays_published():
    assert _outputs.Output("somewhere", "overwrite", _outputs.FLAT).restore() is False


def test_restoring_an_append_removes_the_folders_it_made(files):
    write(renamed(files, "overwrite", layout=DYNAMIC))
    before = sorted(os.walk("out"))
    out = renamed(files, "append", layout=DYNAMIC, run_id="run2")
    more = pa.table({"region": ["north", "east"], "total": pa.array([30, 40], pa.int64())})
    out.stage(more.schema, iter(more.to_batches()), set())
    out.publish()
    assert os.path.isdir(os.path.join("out", "region=east"))
    out.restore()
    # region=north/ held files before the append and keeps them; region=east/ was made by it, and is gone
    assert sorted(os.walk("out")) == before


def test_restoring_an_append_to_an_output_that_did_not_exist_removes_it(files):
    out = renamed(files, "append", layout=DYNAMIC)
    out.stage(ROWS.schema, iter(ROWS.to_batches()), set())
    out.publish()
    out.restore()
    assert not os.path.exists("out") and contents()[1] == []
