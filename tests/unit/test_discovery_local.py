"""Tests for LocalConnection discovery methods."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from odibi.connections.local import LocalConnection


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def local_conn(temp_dir):
    return LocalConnection(base_path=str(temp_dir))


def test_list_files(local_conn, temp_dir):
    (temp_dir / "file1.csv").write_text("test")
    (temp_dir / "file2.parquet").write_text("test")

    files = local_conn.list_files()
    assert len(files) == 2
    assert any(f["name"] == "file1.csv" for f in files)
    assert any(f["format"] == "parquet" for f in files)


def test_list_folders(local_conn, temp_dir):
    (temp_dir / "folder1").mkdir()
    (temp_dir / "folder2").mkdir()

    folders = local_conn.list_folders()
    assert len(folders) == 2


def test_discover_catalog(local_conn, temp_dir):
    (temp_dir / "data.csv").write_text("test")
    (temp_dir / "folder1").mkdir()

    catalog = local_conn.discover_catalog()
    assert catalog["total_datasets"] == 2
    assert "csv" in catalog["formats"]


def test_get_schema(local_conn, temp_dir):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    csv_path = temp_dir / "test.csv"
    df.to_csv(csv_path, index=False)

    schema = local_conn.get_schema("test.csv")
    assert len(schema["columns"]) == 2
    assert schema["columns"][0]["name"] == "a"


def test_profile(local_conn, temp_dir):
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", None]})
    csv_path = temp_dir / "test.csv"
    df.to_csv(csv_path, index=False)

    profile = local_conn.profile("test.csv")
    assert profile["rows_sampled"] == 3
    assert len(profile["columns"]) == 2
    assert any(c["name"] == "id" for c in profile["columns"])


def test_get_freshness(local_conn, temp_dir):
    (temp_dir / "test.csv").write_text("test")

    freshness = local_conn.get_freshness("test.csv")
    assert "last_updated" in freshness
    assert freshness["source"] == "metadata"
