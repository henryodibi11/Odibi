import json
from pathlib import Path

import pytest

from odibi.diagnostics.manager import HistoryManager
from odibi.story.metadata import PipelineStoryMetadata


@pytest.fixture
def temp_history_dir(tmp_path):
    """
    Fixture that creates a temporary directory to simulate the history storage.
    """
    history_dir = tmp_path / "history"
    history_dir.mkdir(exist_ok=True)
    return history_dir


@pytest.fixture
def history_manager(temp_history_dir):
    """
    Fixture that returns a HistoryManager instance using the temporary history directory.
    """
    return HistoryManager(history_path=str(temp_history_dir))


def create_run_file(directory: Path, pipeline_name: str, timestamp: str, content: dict = None):
    """
    Helper to create a dummy run file.
    Filename follows the pattern: {pipeline_name}_{timestamp}.json
    """
    filename = f"{pipeline_name}_{timestamp}.json"
    file_path = directory / filename
    if content is None:
        content = {"pipeline_name": pipeline_name, "nodes": []}
    file_path.write_text(json.dumps(content), encoding="utf-8")
    return file_path


def test_list_runs_directory_not_exist_returns_empty(tmp_path):
    """
    If the history directory does not exist, list_runs should return an empty list.
    """
    non_existing = tmp_path / "nonexistent"
    hm = HistoryManager(history_path=str(non_existing))
    runs = hm.list_runs("test_pipeline")
    assert runs == []


def test_list_runs_returns_valid_runs_sorted_by_timestamp(history_manager, temp_history_dir):
    """
    list_runs should return valid run entries extracted from filenames and sort them in descending
    order based on the timestamp.
    """
    # Create two valid run files with distinct timestamps
    create_run_file(temp_history_dir, "test_pipeline", "20260115_120000")
    create_run_file(temp_history_dir, "test_pipeline", "20260116_130000")
    # Create an invalid file that should be ignored
    invalid_file = temp_history_dir / "test_pipeline_invalid.json"
    invalid_file.write_text("{}", encoding="utf-8")

    runs = history_manager.list_runs("test_pipeline")
    # Expect only the two valid runs
    assert len(runs) == 2
    # Verify that the runs are sorted in descending order (newest first)
    assert runs[0]["run_id"] == "20260116_130000"
    assert runs[1]["run_id"] == "20260115_120000"


def test_get_latest_run_returns_latest_metadata(history_manager, temp_history_dir):
    """
    get_latest_run should return None if no runs exist, and otherwise return the metadata
    corresponding to the latest run.
    """
    # Initially, no runs exist
    assert history_manager.get_latest_run("test_pipeline") is None

    # Create two run files; the one with the later timestamp should be considered latest.
    create_run_file(temp_history_dir, "test_pipeline", "20260115_120000")
    latest_content = {"pipeline_name": "test_pipeline", "nodes": []}
    create_run_file(temp_history_dir, "test_pipeline", "20260116_130000", content=latest_content)

    latest = history_manager.get_latest_run("test_pipeline")
    assert latest is not None
    assert isinstance(latest, PipelineStoryMetadata)
    assert latest.pipeline_name == "test_pipeline"


def test_get_run_by_id_returns_correct_metadata(history_manager, temp_history_dir):
    """
    get_run_by_id should return the appropriate metadata when provided with a valid run_id,
    and None if the run_id does not exist.
    """
    content1 = {"pipeline_name": "test_pipeline", "nodes": []}
    create_run_file(temp_history_dir, "test_pipeline", "20260115_120000", content=content1)
    content2 = {"pipeline_name": "test_pipeline", "nodes": []}
    create_run_file(temp_history_dir, "test_pipeline", "20260116_130000", content=content2)

    metadata = history_manager.get_run_by_id("test_pipeline", "20260115_120000")
    assert metadata is not None
    assert metadata.pipeline_name == "test_pipeline"

    # Request for a non-existent run_id should return None
    assert history_manager.get_run_by_id("test_pipeline", "nonexistent") is None


def test_get_previous_run_returns_previous_metadata(history_manager, temp_history_dir):
    """
    get_previous_run should return the metadata for the run immediately preceding the specified run.
    If the specified run is the oldest or not found, it should return None.
    """
    # Create three run files with increasing timestamps
    create_run_file(temp_history_dir, "test_pipeline", "20260114_110000")
    create_run_file(temp_history_dir, "test_pipeline", "20260115_120000")
    create_run_file(temp_history_dir, "test_pipeline", "20260116_130000")

    # For the latest run, the previous run should be the one with the middle timestamp.
    prev_meta = history_manager.get_previous_run("test_pipeline", "20260116_130000")
    assert prev_meta is not None
    # For the oldest run, there is no previous run.
    assert history_manager.get_previous_run("test_pipeline", "20260114_110000") is None


def test_load_run_raises_not_implemented_for_remote(tmp_path):
    """
    When the history manager is configured with a remote path,
    load_run should raise NotImplementedError.
    """
    hm = HistoryManager(history_path="http://remote")
    dummy_file = tmp_path / "dummy.json"
    dummy_file.write_text("{}", encoding="utf-8")

    with pytest.raises(NotImplementedError):
        hm.load_run(str(dummy_file))


def test_load_run_successfully_loads_valid_json(history_manager, temp_history_dir):
    """
    load_run should successfully load valid JSON content from a file and convert it to a PipelineStoryMetadata object.
    """
    node_data = {"node_name": "node1", "operation": "run", "status": "success", "duration": 1.23}
    content = {"pipeline_name": "test_pipeline", "nodes": [node_data]}
    run_file = create_run_file(
        temp_history_dir, "test_pipeline", "20260116_130000", content=content
    )

    metadata = history_manager.load_run(str(run_file))
    assert isinstance(metadata, PipelineStoryMetadata)
    assert metadata.pipeline_name == "test_pipeline"
    # Validate that there is one node with the expected attributes.
    assert len(metadata.nodes) == 1
    node = metadata.nodes[0]
    assert node.node_name == "node1"
    assert node.operation == "run"
    assert node.status == "success"
    assert node.duration == 1.23
