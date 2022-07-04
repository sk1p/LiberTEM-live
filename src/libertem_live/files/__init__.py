from typing import List, Tuple
from libertem.common.executor import (
    MainController, TaskProtocol, WorkerQueue
)


def _poll_for_files(base_path: str) -> List[Tuple[str, Tuple[int, int]]]:
    pass


def _ordered_files(base_path: str, end_idx: int):
    # which files have we "yielded" already?
    files_done = {}

    # the next file should have this as start index:
    next_start_idx = 0

    while True:
        # slice here means a tuple (start_idx, end_idx)
        files_and_slices = _poll_for_files(base_path)
        todo_files = [
            (f, slice_)
            for (f, slice_) in files_and_slices
            if f not in files_done
        ]
        # sort by slice
        todo_files = sorted(todo_files, key=lambda x: x[1])
        for (f, slice_) in todo_files:
            if slice_[0] == next_start_idx:
                # this is the one, yield it!
                files_done[f] = (f, slice_)
                yield (f, slice_)
                next_start_idx = slice_[1]
                # in case we get all files in-order, the next iteration
                # of this loop will hit the condition again etc.
                # otherwise we will again get the files we didn't
                # handle here in `todo_files`

        # we are done:
        if next_start_idx >= end_idx:
            break


class FileWatcherIterator:
    def __init__(self, base_path: str, end_idx: int):
        self._base_path = base_path

        # files and slices of already-sorted files, ascending by indices
        self._todo_sorted: List[Tuple[str, Tuple[int, int]]] = []
        self._ordered_files = _ordered_files(base_path, end_idx)

    def __iter__(self):
        return self

    def __next__(self):
        # we have ready-to-use files, return the next one:
        if len(self._todo_sorted) > 0:
            file, (start_idx, end_idx) = self._todo_sorted.pop(0)
            return file, (start_idx, end_idx)

        # otherwise, block and wait for new files:
        try:
            file, (start_idx, end_idx) = next(self._ordered_files)
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
    def __init__(self, base_path: str, end_idx: int):
        self._base_path = base_path
        self._files_iterator = FileWatcherIterator(base_path, end_idx)

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
