import os

import pytest

from libertem_live.files import FileWatcherSource, FilesController
from libertem.common import Slice, Shape
from libertem.common.executor import SimpleWorkerQueue


def _touch(path):
    if not os.path.exists(path):
        with open(path, "wb"):
            pass


def test_watcher_source_in_order(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d)

    files = watcher.ordered_files(4096)

    assert next(files) is None

    # first file, as expected starting with index 0, will be yielded directly:
    _touch(d / "prefix_0000_0128.ext")
    assert next(files) == (
        str(d / "prefix_0000_0128.ext"),
        (0, 128)
    )

    # directly following in index order:
    _touch(d / "prefix_0128_0256.ext")
    assert next(files) == (
        str(d / "prefix_0128_0256.ext"),
        (128, 256)
    )


def test_watcher_source_out_of_order(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d)

    files = watcher.ordered_files(4096)

    assert next(files) is None

    # first file, as expected starting with index 0, will be yielded directly:
    _touch(d / "prefix_0000_0128.ext")
    assert next(files) == (
        str(d / "prefix_0000_0128.ext"),
        (0, 128)
    )

    # directly following in index order:
    _touch(d / "prefix_0128_0256.ext")
    assert next(files) == (
        str(d / "prefix_0128_0256.ext"),
        (128, 256)
    )

    # not in order, so not yielded but put into "back buffer":
    _touch(d / "prefix_0512_0768.ext")
    assert next(files) is None

    # so if we add the missing piece...
    _touch(d / "prefix_0256_0512.ext")

    # both will be yielded:
    assert next(files) == (
        str(d / "prefix_0256_0512.ext"),
        (256, 512)
    )
    assert next(files) == (
        str(d / "prefix_0512_0768.ext"),
        (512, 768)
    )

    # now let's go to the end:
    _touch(d / "prefix_0768_4096.ext")
    assert next(files) == (
        str(d / "prefix_0768_4096.ext"),
        (768, 4096)
    )

    # and we are done:
    with pytest.raises(StopIteration):
        next(files)


def test_watcher_source_non_zero_start(tmp_path):
    d = tmp_path / "base_for_watcher"
    d.mkdir()

    watcher = FileWatcherSource(d)

    files = watcher.ordered_files(4096)

    assert next(files) is None
    _touch(d / "prefix_0128_0256.ext")
    assert next(files) is None


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
    c = FilesController(str(d), 1024, timeout=0.4)

    t0 = MockTask(0, 128)

    # no files exist yet, so we will hit the timeout:
    with pytest.raises(RuntimeError):
        c.handle_task(t0, q)


def test_files_controller_handle_task(tmp_path):
    d = tmp_path / "base_for_controller"
    d.mkdir()

    q = SimpleWorkerQueue()
    c = FilesController(str(d), 2048, timeout=0.4)

    # single file per task:
    t0 = MockTask(0, 128)
    f0 = d / "prefix_0000_0128"
    _touch(f0)
    c.handle_task(t0, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f0),
            "file_start_idx": 0,
            "file_end_idx": 128,
        }

    # multiple files per task:
    t1 = MockTask(128, 256)
    f1 = d / "prefix_0128_0192"
    f2 = d / "prefix_0192_0256"
    _touch(f1)
    _touch(f2)
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

    # overlap into the next task:

    t2 = MockTask(256, 384)
    t3 = MockTask(384, 1024)
    f3 = d / "prefix_0256_1024"
    _touch(f3)
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

    # and now until the end:
    t4 = MockTask(1024, 2048)
    f4 = d / "prefix_1024_2048"
    _touch(f4)
    c.handle_task(t4, q)
    with q.get(block=False) as msg:
        msg, _ = msg
        assert msg == {
            "type": "NEW_FILE",
            "file": str(f4),
            "file_start_idx": 1024,
            "file_end_idx": 2048,
        }
