from contextlib import contextmanager
import os
import re
import time
import logging
from typing import Callable, Generator, Iterable, List, Optional, Tuple

import numpy as np
from opentelemetry import trace

from libertem.common import Shape, Slice
from libertem.common.executor import (
    MainController, TaskProtocol, WorkerQueue, WorkerContext,
)
from libertem.io.dataset.base import (
    DataTile, DataSetMeta, BasePartition, Partition, DataSet, TilingScheme,
)
from libertem_live.detectors.base.acquisition import AcquisitionMixin


FN_PAT = re.compile(r'^.*_(?P<start_idx>[0-9]+)_(?P<end_idx>[0-9]+)(\..*)?$')

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class FileWatcherSource:
    def __init__(self, base_path: str):
        self._base_path = base_path

    def _get_start_stop(self, path: str) -> Tuple[int, int]:
        # FIXME: let's assume we get file names in the form
        # prefix_{startidx}_{endidx}.ext -> let's parse that!
        # in reailty, this will probably be different and
        # highly dependent on what software is writing these files
        m = FN_PAT.match(path)
        if not m:
            raise RuntimeError(
                "file does not match the given pattern"
            )
        match = m.groupdict()
        return (
            int(match['start_idx']),
            int(match['end_idx']),
        )

    def _poll_for_files(self) -> Iterable[Tuple[str, Tuple[int, int]]]:
        logger.debug(f"polling in {self._base_path}")
        # FIXME: use something more efficient, like inotify?
        files = os.listdir(self._base_path)
        full_paths = [
            os.path.join(self._base_path, f)
            for f in files
        ]
        return [
            (path, self._get_start_stop(path))
            for path in full_paths
        ]

    def ordered_files(self, end_idx: int) -> Generator[Optional[Tuple[str, Tuple[int, int]]], None, None]:
        """
        Generator that yields files sorted by (start, stop).
        Continuously watches a directory, and yields files
        and their (start, stop) indices. If there is a gap,
        it will wait for the respective file to manifest in the
        watched directory.

        In case of no new files, it yields `None` and gives the
        user a chance to do 

        Parameters
        ----------
        end_idx : int
            When reaching this index, we are done and will stop watching
        """
        # which files have we yielded already?
        files_done = {}

        # the next file should have this as start index:
        next_start_idx = 0

        while True:
            # slice here means a tuple (start_idx, end_idx)
            files_and_slices = self._poll_for_files()
            todo_files = [
                (f, slice_)
                for (f, slice_) in files_and_slices
                if f not in files_done
            ]
            # sort by slice
            todo_files = sorted(todo_files, key=lambda x: x[1])
            if len(todo_files) == 0:
                yield None
                continue
            yielded = 0
            for (f, slice_) in todo_files:
                if slice_[0] == next_start_idx:
                    # this is the one, mark and yield it!
                    files_done[f] = (f, slice_)
                    logger.debug(f"yielding {f}")
                    yield (f, slice_)
                    yielded += 1
                    next_start_idx = slice_[1]
                    # in case we get all files in-order, the next iteration
                    # of this loop will hit the condition again etc.
                    # otherwise we will again get the files we didn't
                    # handle here in `todo_files`
            if yielded == 0:
                yield None

            # we are done:
            if next_start_idx >= end_idx:
                break


class FileWatcherIterator:
    def __init__(self, base_path: str, end_idx: int, sleep_time: float = 0.3, timeout : float = 10):
        self._base_path = base_path
        self._sleep_time = sleep_time
        self._timeout = timeout

        # files and slices of already-sorted files, ascending by indices
        self._todo_sorted: List[Tuple[str, Tuple[int, int]]] = []
        self._watcher = FileWatcherSource(base_path)
        self._ordered_files = self._watcher.ordered_files(end_idx)

    def __iter__(self):
        return self

    def __next__(self):
        # we have ready-to-use files, return the next one:
        if len(self._todo_sorted) > 0:
            file, (start_idx, end_idx) = self._todo_sorted.pop(0)
            return file, (start_idx, end_idx)

        # otherwise, block and wait for new files:
        try:
            next_file = next(self._ordered_files)
            t0 = time.time()
            while next_file is None:
                if time.time() - t0 > self._timeout:
                    raise RuntimeError("timeout while waiting for new files")
                time.sleep(self._sleep_time)
                next_file = next(self._ordered_files)
            file, (start_idx, end_idx) = next_file
            return file, (start_idx, end_idx)
        except StopIteration:
            # FIXME: add check: are we really done already?
            raise

    def put_back(self, file, slice_):
        """
        Put back a file that was not completely processed into
        the queue, together with its slice information.

        Parameters
        ----------
        file : _type_
            _description_
        slice_ : _type_
            _description_
        """
        self._todo_sorted.insert(0, (file, slice_))


class FilesController(MainController):
    def __init__(self, base_path: str, end_idx: int, timeout: float = 10):
        self._base_path = base_path
        self._files_iterator = FileWatcherIterator(base_path, end_idx, timeout=timeout)

    def handle_task(self, task: TaskProtocol, queue: WorkerQueue):
        slice_ = task.get_partition().slice
        p_start_idx = slice_.origin[0]
        p_end_idx = slice_.origin[0] + slice_.shape[0]
        # NOTE: assumption: _files_iterator gives us the files sorted
        # by the start/end idx
        # NOTE: assumption: the files are generally structured like a 4D STEM
        # data set, mapping well to a linear index over the rectangular scanned
        # region
        for file, (file_start_idx, file_end_idx) in self._files_iterator:
            queue.put({
                "type": "NEW_FILE",
                "file": file,
                "file_start_idx": file_start_idx,
                "file_end_idx": file_end_idx,
            })
            # there is more data than belongs to this partition,
            # so we push it back to be returned on the next
            # call of __next__ on the iterator:
            if file_end_idx > p_end_idx:  # FIXME: off-by-one maybe?
                self._files_iterator.put_back(file, (file_start_idx, file_end_idx))
                return  # we are done with this task
            elif file_end_idx == p_end_idx:
                # self._files_iterator.
                return
            else:
                pass  # file_end_idx < p_end_idx, we still have work todo

    def start(self):
        ...

    def done(self):
        ...


class WatcherAcquisition(AcquisitionMixin, DataSet):
    def __init__(
        self,
        trigger: Callable,
        base_path: str,
        nav_shape: Tuple[int, int],
        sig_shape: Tuple[int, int],
        timeout: float = 10.0,
    ):
        self._base_path = base_path
        self._timeout = timeout
        self._nav_shape = nav_shape
        self._sig_shape = sig_shape
        super().__init__(trigger=trigger)

    def initialize(self, executor):
        dtype = np.uint8  # FIXME: don't know the dtype yet
        self._meta = DataSetMeta(
            shape=Shape(self._nav_shape + self._sig_shape, sig_dims=2),
            raw_dtype=dtype,
            dtype=dtype,
        )
        return self

    @property
    def dtype(self):
        return self._meta.dtype

    @property
    def raw_dtype(self):
        return self._meta.raw_dtype

    @property
    def shape(self):
        return self._meta.shape

    @property
    def meta(self):
        return self._meta

    @contextmanager
    def acquire(self):
        self.trigger()
        yield

    def check_valid(self):
        pass

    def need_decode(self, read_dtype, roi, corrections):
        return True  # FIXME: we just do this to get a large tile size

    def adjust_tileshape(self, tileshape, roi):
        depth = 24
        return (depth, *self.meta.shape.sig)

    def get_max_io_size(self):
        # return 12*256*256*8
        # FIXME magic numbers?
        return 24*np.prod(self.meta.shape.sig)*8

    def get_base_shape(self, roi):
        return (1, 1, self.meta.shape.sig[-1])

    def get_partitions(self):
        num_frames = np.prod(self._nav_shape, dtype=np.uint64)
        num_partitions = int(num_frames // self._frames_per_partition)

        header = self.raw_socket.get_acquisition_header()

        slices = BasePartition.make_slices(self.shape, num_partitions)
        for part_slice, start, stop in slices:
            yield WatcherLivePartition(
                start_idx=start,
                end_idx=stop,
                meta=self._meta,
                partition_slice=part_slice,
            )

    def get_controller(self) -> MainController:
        return FilesController(
            base_path=self._base_path,
            end_idx=self.shape.nav.size,
            timeout=self._timeout,
        )

    
class WatcherLivePartition(Partition):
    def __init__(self, start_idx, end_idx, partition_slice, meta):
        super().__init__(
            meta=meta, partition_slice=partition_slice, io_backend=None, decoder=None
        )
        self._start_idx = start_idx
        self._end_idx = end_idx

    def shape_for_roi(self, roi):
        return self.slice.adjust_for_roi(roi).shape

    @property
    def shape(self):
        return self.slice.shape

    @property
    def dtype(self):
        return self.meta.raw_dtype

    def set_corrections(self, corrections):
        self._corrections = corrections

    def set_worker_context(self, worker_context: WorkerContext):
        self._worker_context = worker_context

    def _get_tiles_fullframe(self, tiling_scheme: TilingScheme, dest_dtype="float32", roi=None):
        # assert len(tiling_scheme) == 1
        tiling_scheme = tiling_scheme.adjust_for_partition(self)
        logger.debug("reading up to frame idx %d for this partition", self._end_idx)

        queue = self._worker_context.get_worker_queue()
        frames = get_frames_from_queue(
            queue,
            tiling_scheme,
            self.shape.sig.to_tuple(),
            dtype=dest_dtype
        )

        # special case: copy from the decode buffer into a larger partition buffer
        # can be further optimized if needed (by directly decoding into said buffer)
        if tiling_scheme.intent == "partition":
            frame_stack = _accum_partition(
                frames,
                tiling_scheme,
                self.shape.sig.to_tuple(),
                dtype=dest_dtype
            )
            tile_shape = Shape(
                frame_stack.shape,
                sig_dims=2
            )
            tile_slice = Slice(
                origin=(self._start_idx,) + (0, 0),
                shape=tile_shape,
            )
            yield DataTile(
                frame_stack,
                tile_slice=tile_slice,
                scheme_idx=0,
            )
            return

        for frame_stack, start_idx in frames:
            tile_shape = Shape(
                frame_stack.shape,
                sig_dims=2
            )
            tile_slice = Slice(
                origin=(start_idx,) + (0, 0),
                shape=tile_shape,
            )
            yield DataTile(
                frame_stack,
                tile_slice=tile_slice,
                scheme_idx=0,
            )

    def get_tiles(self, tiling_scheme, dest_dtype="float32", roi=None):
        yield from self._get_tiles_fullframe(tiling_scheme, dest_dtype, roi)

    def __repr__(self):
        return f"<MerlinLivePartition {self._start_idx}:{self._end_idx}>"
