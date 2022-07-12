import os

import numpy as np
import pytest
import h5py

from libertem_live.files import FileWatcherSource, FilesController
from libertem.common import Slice, Shape
from libertem.common.executor import SimpleWorkerQueue


def _touch(path, num_frames):
    if not os.path.exists(path):
        with h5py.File(path, "w") as f:
            f.create_dataset("data", (num_frames, 64, 64), dtype=np.float32)


def test_watcher_source_in_order(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d, ds_path="/data")

    files = watcher.ordered_files(4096)

    assert next(files) is None

    # first file, as expected starting with index 1, will be yielded directly:
    _touch(d / "scan_000123_data_000001.h5", num_frames=128)
    n = next(files)
    assert n.name == "scan_000123_data_000001.h5"
    assert n.num_frames == 128
    assert n.start_idx == 0
    assert n.end_idx == 128

    # directly following in index order:
    _touch(d / "scan_000123_data_000002.h5", num_frames=128)

    n = next(files)
    assert n.name == "scan_000123_data_000002.h5"
    assert n.num_frames == 128
    assert n.start_idx == 128
    assert n.end_idx == 256


def test_watcher_source_out_of_order(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d, ds_path="/data")

    files = watcher.ordered_files(4096)

    assert next(files) is None

    # first file, as expected starting with index 1, will be yielded directly:
    _touch(d / "scan_000123_data_000001.h5", num_frames=128)
    n = next(files)
    assert n.name == "scan_000123_data_000001.h5"
    assert n.num_frames == 128
    assert n.start_idx == 0
    assert n.end_idx == 128

    # directly following in index order:
    _touch(d / "scan_000123_data_000002.h5", num_frames=128)
    n = next(files)
    assert n.name == "scan_000123_data_000002.h5"
    assert n.num_frames == 128
    assert n.start_idx == 128
    assert n.end_idx == 256

    # not in order, so not yielded but put into "back buffer":
    _touch(d / "scan_000123_data_000004.h5", num_frames=128)
    assert next(files) is None

    # so if we add the missing piece...
    _touch(d / "scan_000123_data_000003.h5", num_frames=128)

    # both will be yielded:
    n = next(files)
    assert n.start_idx == 256
    n = next(files)
    assert n.start_idx == 384

    # now let's go to the end:
    _touch(d / "scan_000123_data_000005.h5", num_frames=4096 - 512)
    n = next(files)
    assert n.start_idx == 512
    assert n.end_idx == 4096

    # and we are done:
    with pytest.raises(StopIteration):
        next(files)


def test_watcher_source_non_zero_start(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d, ds_path="/data")

    files = watcher.ordered_files(4096)

    assert next(files) is None
    _touch(d / "scan_000123_data_000002.h5", num_frames=128)
    n = next(files)
    assert n is None


class MockPartition:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    @property
    def slice(self):
        return Slice(
            (self.start, 0, 0),
            Shape((self.end - self.start, 16, 16), sig_dims=2),
        )


class MockTask:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def get_partition(self):
        return MockPartition(self.start, self.end)


def test_files_controller_timeout(tmp_path):
    d = tmp_path / "base_for_controller"
    d.mkdir()

    q = SimpleWorkerQueue()
    c = FilesController(
        base_path=str(d),
        ds_path="/data",
        end_idx=1024,
        timeout=0.4
    )

    t0 = MockTask(0, 128)

    # no files exist yet, so we will hit the timeout:
    with pytest.raises(RuntimeError):
        c.handle_task(t0, q)


def test_files_controller_handle_task(tmp_path):
    d = tmp_path / "base_for_controller"
    d.mkdir()

    q = SimpleWorkerQueue()
    c = FilesController(
        base_path=str(d),
        ds_path="/data",
        end_idx=2048,
        timeout=0.4
    )

    # single file per task:
    t0 = MockTask(0, 128)
    f0 = d / "scan_000123_data_000001.h5"
    _touch(f0, num_frames=128)
    c.handle_task(t0, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f0),
            "file_start_idx": 0,
            "file_end_idx": 128,
        }

    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "END_PARTITION",
        }

    # multiple files per task:
    t1 = MockTask(128, 256)
    f1 = d / "scan_000123_data_000002.h5"
    f2 = d / "scan_000123_data_000003.h5"
    _touch(f1, num_frames=64)
    _touch(f2, num_frames=64)
    c.handle_task(t1, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f1),
            "file_start_idx": 128,
            "file_end_idx": 192,
        }
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f2),
            "file_start_idx": 192,
            "file_end_idx": 256,
        }

    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "END_PARTITION",
        }

    # overlap into the next task:
    t2 = MockTask(256, 384)
    t3 = MockTask(384, 1024)
    f3 = d / "scan_000123_data_000004.h5"
    _touch(f3, num_frames=768)
    c.handle_task(t2, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f3),
            "file_start_idx": 256,
            "file_end_idx": 1024,
        }

    c.handle_task(t3, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f3),
            "file_start_idx": 256,
            "file_end_idx": 1024,
        }

    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "END_PARTITION",
        }

    # and now until the end:
    t4 = MockTask(1024, 2048)
    f4 = d / "scan_000123_data_000005.h5"
    _touch(f4, num_frames=1024)
    c.handle_task(t4, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f4),
            "file_start_idx": 1024,
            "file_end_idx": 2048,
        }

    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "END_PARTITION",
        }
