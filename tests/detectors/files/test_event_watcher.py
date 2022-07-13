import os
import json
import pathlib

import pytest
import h5py
import numpy as np

from libertem_live.files import EventFileIterator, MockEventSource

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

SCAN_STARTED_PATH = os.path.join(DATA_DIR, "scan_started.json")
NEW_FILE_PATH = os.path.join(DATA_DIR, "new_file.json")


def _touch(path, num_frames):
    if not os.path.exists(path):
        with h5py.File(path, "w") as f:
            f.create_dataset("data", (num_frames, 64, 64), dtype=np.float32)


def test_validate_detector_name():
    events = MockEventSource()
    file_iterator = EventFileIterator(
        events=events,
        detector_name="what",
        timeout=0.1,
    )
    with open(SCAN_STARTED_PATH, "r") as f:
        events.put_msg((
            json.load(f),
            "scan_started"
        ))
    with open(NEW_FILE_PATH, "r") as f:
        events.put_msg((
            json.load(f),
            "new_file"
        ))
    with pytest.raises(ValueError):
        file_iterator.wait_for_start()


def test_event_file_iterator_timeout():
    events = MockEventSource()
    file_iterator = EventFileIterator(
        events=events,
        sleep_time=0.01,
        timeout=0.1,
        detector_name="eiger4m_01",
    )
    # if there is no event coming, we eventually get a timeout:
    with pytest.raises(RuntimeError):
        file_iterator.wait_for_start()


def test_event_file_iterator(tmp_path: pathlib.Path):
    events = MockEventSource()
    file_iterator = EventFileIterator(
        events=events,
        detector_name="eiger4m_01",
        timeout=0.1,
    )
    base_path = tmp_path / "files"
    base_path.mkdir()
    filename = "scan_000001_data_000001.h5"
    with open(SCAN_STARTED_PATH, "r") as f:
        scan_started_msg = json.load(f)
        # mock in the base path as a temporary directory:
        scan_started_msg["scan"]["scan_directory_core"] = base_path
        events.put_msg((
            scan_started_msg,
            "scan_started"
        ))
    with open(NEW_FILE_PATH, "r") as f:
        new_file_msg = json.load(f)
        new_file_msg["file"]["filename"] = filename
        _touch(str(base_path / filename), num_frames=128)

        events.put_msg((
            new_file_msg,
            "new_file"
        ))
    file_iterator.wait_for_start()
    file = next(file_iterator)
    assert file.start_idx == 0
    assert file.end_idx == 9
    assert file.name == "scan_000001_data_000001.h5"
    assert file.path == str(base_path / filename)
