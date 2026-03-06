"""Tests for discovery utilities."""

from odibi.discovery.utils import detect_partitions, infer_format_from_path


def test_infer_format_from_path():
    assert infer_format_from_path("data.csv") == "csv"
    assert infer_format_from_path("data.parquet") == "parquet"
    assert infer_format_from_path("data.json") == "json"
    assert infer_format_from_path("file.xlsx") == "excel"
    assert infer_format_from_path("unknown.xyz") is None


def test_detect_partitions_hive():
    paths = [
        "/data/year=2024/month=01/file1.parquet",
        "/data/year=2024/month=02/file2.parquet",
        "/data/year=2023/month=12/file3.parquet",
    ]
    result = detect_partitions(paths)
    assert result["format"] == "hive"
    assert "year" in result["keys"]
    assert "month" in result["keys"]
    assert "2024" in result["example_values"]["year"]


def test_detect_partitions_date():
    paths = [
        "/data/2024/01/15/file1.parquet",
        "/data/2024/02/20/file2.parquet",
    ]
    result = detect_partitions(paths)
    assert result["format"] == "date"
    assert result["keys"] == ["year", "month", "day"]


def test_detect_partitions_none():
    paths = [
        "/data/file1.parquet",
        "/data/file2.parquet",
    ]
    result = detect_partitions(paths)
    assert result["format"] == "none"
    assert result["keys"] == []


def test_detect_file_format_delta():
    path = "/data/my_table/_delta_log"
    result = infer_format_from_path(path)
    assert result == "delta"
