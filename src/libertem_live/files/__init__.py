from contextlib import contextmanager
import os
import re
import time
import glob
import logging
from typing import (
    Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple
)

import numpy as np
from opentelemetry import trace
import h5py

from libertem.common import Shape, Slice
from libertem.common.executor import (
    MainController, TaskProtocol, WorkerQueue, WorkerContext,
)
from libertem.io.dataset.base import (
    DataSetMeta, BasePartition, Partition, DataSet, DataTile,
)
from libertem.io.dataset.hdf5 import H5Reader, H5Partition
from libertem_live.detectors.base.acquisition import AcquisitionMixin

from . import zeromq


# scan_00026_data_000013.h5
FN_PAT = re.compile(r'^scan_(?P<scan>[0-9]+)_data_(?P<series_idx>[0-9]+)\.h5$')

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class SingleFile:
    def __init__(self, path: str, data_path: str):
        self._path = path
        self._data_path = data_path

    def is_successor_of(self, other: "Optional[SingleFile]") -> bool:
        """
        Return True if this file is the direct succesor of `other`.

        Parameters
        ----------
        other : SingleFile
            _description_
        """
        if other is None:
            return self.series_index == 1
        return self.series_index == other.series_index + 1

    @contextmanager
    def ds(self) -> h5py.Dataset:
        with h5py.File(self._path, "r") as f:
            ds = f[self._data_path]
            yield ds

    @property
    def name(self) -> str:
        return os.path.basename(self._path)

    @property
    def path(self):
        return self._path

    @property
    def series_index(self) -> int:
        """
        Index of this file in the series. Starts at 1.
        """
        m = FN_PAT.match(self.name)
        assert m is not None
        return int(m.groupdict()['series_idx'])

    @property
    def shape(self) -> Tuple[int, ...]:
        with self.ds() as ds:
            return ds.shape

    @property
    def num_frames(self) -> int:
        return self.shape[0]

    @property
    def dtype(self) -> np.dtype:
        with self.ds() as ds:
            return ds.dtype

    def __hash__(self):
        return hash((self._path, self._data_path))

    def __repr__(self):
        return f"<SingleFile {self.name}>"

    def add_indices(self, start_idx: int, end_idx: int) -> "FileWithIndices":
        return FileWithIndices(
            path=self._path,
            data_path=self._data_path,
            start_idx=start_idx,
            end_idx=end_idx,
        )

    def wait_until_exists(self, timeout: float, sleep: float = 1):
        t0 = time.time()
        while True:
            if time.time() - t0 > timeout:
                raise RuntimeError("timeout while waiting for file to manifest")
            if os.path.exists(self.path):
                return
            else:
                time.sleep(sleep)


class FileWithIndices(SingleFile):
    def __init__(self, path: str, data_path: str, start_idx: int, end_idx: int):
        super().__init__(path, data_path)
        self._start_idx = start_idx
        self._end_idx = end_idx

    @property
    def start_idx(self) -> int:
        return self._start_idx

    @property
    def end_idx(self) -> int:
        return self._end_idx


class FileWatcherSource:
    def __init__(self, base_path: str, data_path: str):
        self._base_path = base_path
        self._data_path = data_path

    def _poll_for_files(self) -> Iterable[SingleFile]:
        logger.debug(f"polling in {self._base_path}")
        # FIXME: use something more efficient, like inotify?
        full_paths = glob.glob(f"{self._base_path}/scan_*_data_*.h5")
        return [
            SingleFile(path, self._data_path)
            for path in full_paths
        ]

    def ordered_files(
        self,
        end_idx: int
    ) -> Generator[Optional[FileWithIndices], None, None]:
        """
        Generator that yields files sorted by (start, stop).
        Continuously watches a directory, and yields files
        and their (start, stop) indices. If there is a gap,
        it will wait for the respective file to manifest in the
        watched directory.

        In case of no new files, it yields `None` and gives the
        user a chance to wait, for example for external events,
        or perform any other work.

        Parameters
        ----------
        end_idx : int
            When reaching this index, we are done and will stop watching
        """
        # which files have we yielded already?
        files_done: Set[SingleFile] = set()

        # the last file we yielded was this one:
        last_file = None

        # number of frames we have already yielded
        idx = 0

        while True:
            # slice here means a tuple (start_idx, end_idx)
            files = self._poll_for_files()
            todo_files = [
                single_file
                for single_file in files
                if single_file not in files_done
            ]
            # sort by index
            todo_files = sorted(todo_files, key=lambda x: x.series_index)
            if len(todo_files) == 0:
                yield None
                continue
            yielded = 0
            for single_file in todo_files:
                if single_file.is_successor_of(last_file):
                    # this is the one, mark and yield it!
                    files_done.add(single_file)
                    logger.debug(f"yielding {single_file}")
                    yield single_file.add_indices(
                        start_idx=idx,
                        end_idx=idx + single_file.num_frames,
                    )
                    yielded += 1
                    idx += single_file.num_frames
                    last_file = single_file
                    # in case we get all files in-order, the next iteration
                    # of this loop will hit the condition again etc.
                    # otherwise we will again get the files we didn't
                    # handle here in `todo_files`
            if yielded == 0:
                yield None

            # we are done:
            if idx >= end_idx:
                break


class FileWatcherIterator:
    def __init__(
        self,
        base_path: str,
        end_idx: int,
        data_path: str,
        sleep_time: float = 0.3,
        timeout: float = 10,
    ):
        self._base_path = base_path
        self._sleep_time = sleep_time
        self._timeout = timeout

        # files and slices of already-sorted files, ascending by indices
        self._todo_sorted: List[FileWithIndices] = []
        self._watcher = FileWatcherSource(base_path=base_path, data_path=data_path)
        self._ordered_files = self._watcher.ordered_files(end_idx)

    def __iter__(self):
        return self

    def __next__(self) -> FileWithIndices:
        # we have ready-to-use files, return the next one:
        if len(self._todo_sorted) > 0:
            file = self._todo_sorted.pop(0)
            return file

        # otherwise, block and wait for new files:
        try:
            next_file = next(self._ordered_files)
            t0 = time.time()
            while next_file is None:
                if time.time() - t0 > self._timeout:
                    raise RuntimeError("timeout while waiting for new files")
                time.sleep(self._sleep_time)
                next_file = next(self._ordered_files)
            return next_file
        except StopIteration:
            # FIXME: add check: are we really done already?
            raise

    def put_back(self, file: FileWithIndices):
        """
        Put back a file that was not completely processed into
        the queue, together with its slice information.

        Parameters
        ----------
        file : _type_
            _description_
        """
        self._todo_sorted.insert(0, file)


class EventSource:
    def __init__(self):
        pass

    def receive_message(self, timeout: float):
        pass


class ZMQEventSource(EventSource):
    def __init__(self, host: str, port: int):
        self._client_sub = zeromq.ClientSub(host, port)
        # subscribe to interesting topics:
        self._client_sub.add_subscription_topic("scan_finished")
        self._client_sub.add_subscription_topic("scan_started")
        self._client_sub.add_subscription_topic("new_file")

    def receive_message(self, timeout: Optional[float] = None):
        return self._client_sub.receive_message(timeout=timeout)


class MockEventSource(EventSource):
    def __init__(self):
        self._messages = []

    def receive_message(self, timeout: float):
        if len(self._messages) > 0:
            return self._messages.pop(0)

    def put_msg(self, msg):
        self._messages.append(msg)


class EventFileIterator:
    def __init__(
        self,
        events: EventSource,
        detector_name: str,  # 
        sleep_time: float = 0.3,
        timeout: float = 10,
    ):
        self._sleep_time = sleep_time
        self._detector_name = detector_name
        self._timeout = timeout
        self._events = events

        # files and slices of already-sorted files, ascending by indices
        self._todo_sorted: List[FileWithIndices] = []

        self._scan_metadata = None

    def wait_for_start(self, timeout: Optional[float] = None) -> Dict:
        if timeout is None:
            timeout = self._timeout
        t0 = time.time()
        while True:
            t = time.time()
            spent = t - t0
            timeout_here = timeout - spent
            if timeout_here <= 0:
                raise RuntimeError("timeout while waiting for scan_started")
            msg = self._events.receive_message(timeout=timeout_here)
            if msg is None:
                raise RuntimeError("timeout while waiting for scan_started")
            metadata, topic = msg
            if topic == "scan_started":
                self._scan_metadata = metadata
                self._validate()
                return metadata
            else:
                continue

    def __iter__(self):
        return self

    @property
    def base_path(self):
        assert self._scan_metadata is not None
        return self._scan_metadata["scan"]["scan_directory_core"]

    def _validate(self):
        # throws an exception if data path can't be determined:
        assert self.data_path is not None

    @property
    def data_path(self):
        """
        Path to the Dataset inside the HDF5 file
        """
        if self._detector_name not in self._scan_metadata["detectors"]:
            names = ", ".join(list(self._scan_metadata["detectors"].keys()))
            raise ValueError(
                f"invalid detector name '{self._detector_name}', have {names}"
            )
        return self._scan_metadata["detectors"][self._detector_name]["data"]["image"]["0"]["data_path"]

    def __next__(self) -> FileWithIndices:
        if self._scan_metadata is None:
            raise RuntimeError("wrong state; call `wait_for_start` first")

        # we have ready-to-use files, return the next one:
        if len(self._todo_sorted) > 0:
            file = self._todo_sorted.pop(0)
            return file

        # otherwise, block and wait for new files:
        msg = self._events.receive_message(timeout=self._timeout)
        if msg is None:
            raise RuntimeError("timeout while waiting for message")
        metadata, topic = msg

        if topic == "new_file":
            # keys:
            #  - acquisition_index
            #  - acquisitions_in_file
            #  - detector_name
            #  - detector_type
            #  - file_index
            #  - filename
            #  - frame_axis
            #  - source
            new_file = metadata["file"]
            file = FileWithIndices(
                path=os.path.join(self.base_path, new_file["filename"]),
                # FIXME: get the `data_path` from the `scan_started` event?
                data_path=self.data_path,
                start_idx=new_file["acquisition_index"][0],
                end_idx=new_file["acquisition_index"][1],
            )
            # FIXME: different timeout?
            file.wait_until_exists(timeout=self._timeout)
            return file
        elif topic == "scan_finished":
            raise StopIteration()
        elif topic == "scan_started":
            raise RuntimeError("unexpected `scan_started` message")
        else:
            raise RuntimeError(f"unexpected topic {topic}")

    def put_back(self, file: FileWithIndices):
        """
        Put back a file that was not completely processed into
        the queue, together with its slice information.

        Parameters
        ----------
        file : _type_
            _description_
        """
        self._todo_sorted.insert(0, file)


class FilesController(MainController):
    def __init__(
        self,
        event_source: EventSource,
        detector_name: str,
        timeout: float = 10,
    ):
        self._files_iterator = EventFileIterator(
            events=event_source,
            timeout=timeout,
            detector_name=detector_name,
        )

    def handle_task(self, task: TaskProtocol, queue: WorkerQueue):
        slice_ = task.get_partition().slice
        p_start_idx = slice_.origin[0]
        p_end_idx = slice_.origin[0] + slice_.shape[0]
        # NOTE: assumption: _files_iterator gives us the files sorted
        # by the start/end idx
        # NOTE: assumption: the files are generally structured like a 4D STEM
        # data set, mapping well to a linear index over the rectangular scanned
        # region
        for file in self._files_iterator:
            queue.put({
                "type": "NEW_FILE",
                "file": file.path,
                "file_start_idx": file.start_idx,
                "file_end_idx": file.end_idx,
            })
            # there is more data than belongs to this partition,
            # so we push it back to be returned on the next
            # call of __next__ on the iterator:
            if file.end_idx > p_end_idx:  # FIXME: off-by-one maybe?
                self._files_iterator.put_back(file)
                return  # we are done with this task
            elif file.end_idx == p_end_idx:
                queue.put({
                    "type": "END_PARTITION",
                })
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
            data_path=self._data_path,
            end_idx=self.shape.nav.size,
            timeout=self._timeout,
        )


def _get_partitions(
    queue: WorkerQueue,
    meta: DataSetMeta,
    data_path: str,
    outer_slice: Slice
):
    while True:
        with queue.get() as msg:
            header, payload = msg
            header_type = header["type"]
            if header_type == "NEW_FILE":
                reader = H5Reader(header["file"], data_path)
                f_start_idx = header["file_start_idx"]
                f_end_idx = header["file_end_idx"]
                with reader.get_h5ds() as h5ds:
                    chunks = h5ds.chunks
                file_slice = Slice(
                    (f_start_idx, 0, 0),
                    Shape((f_end_idx - f_start_idx), + meta.shape.sig, sig_dims=meta.sig.dims)
                )
                pslice = file_slice.intersection_with(outer_slice)
                partition = H5Partition(
                    reader=reader,
                    chunks=chunks,
                    meta=meta,
                    partition_slice=pslice,
                    slice_nd=pslice,
                    io_backend=None,
                    decoder=None,
                )
                yield partition
            elif header_type == "END_PARTITION":
                return
            else:
                raise RuntimeError(
                    f"invalid header type {header['type']}; NEW_FILE or END_PARTITION expected"
                )


class WatcherLivePartition(Partition):
    def __init__(self, start_idx, end_idx, partition_slice, data_path, meta):
        super().__init__(
            meta=meta, partition_slice=partition_slice, io_backend=None, decoder=None
        )
        self._start_idx = start_idx
        self._end_idx = end_idx
        self._data_path = data_path

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

    def get_tiles(self, tiling_scheme, dest_dtype="float32", roi=None):
        tiling_scheme = tiling_scheme.adjust_for_partition(self)
        queue = self._worker_context.get_worker_queue()
        assert self.slice.shape.dims == 1
        assert tiling_scheme.intent != "partition",\
            "for now, assume we don't need to accumulate partitions"
        partitions = _get_partitions(
            queue=queue,
            meta=self.meta,
            data_path=self._data_path,
            outer_slice=self.slice,
        )
        for p in partitions:
            yield from p.get_tiles(tiling_scheme, dest_dtype, roi)

    def __repr__(self):
        return f"<WatcherLivePartition {self._start_idx}:{self._end_idx}>"


def enrich_with_metadata(tiles: Generator[DataTile, None, None]):
    for tile in tiles:
        # inspect `tile.tile_slice.nav`
        positions = get_positions_for(tile.tile_slice)
        tile.set_meta({
            "positions": positions,
        })
        yield tile
