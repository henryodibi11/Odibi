"""Discovery utilities for detecting partitions, formats, and schema."""

import re
from pathlib import Path
from typing import Dict, List, Optional

from odibi.utils.logging_context import get_logging_context


def detect_partitions(paths: List[str]) -> Dict:
    """Detect Hive-style or date-based partitioning from file paths.

    Args:
        paths: List of file or directory paths

    Returns:
        Dict with:
            - keys: List[str] - Detected partition keys
            - example_values: Dict[str, List[str]] - Sample values per key
            - format: str - "hive" or "date" or "none"
    """
    ctx = get_logging_context()
    ctx.debug("Detecting partitions from paths", path_count=len(paths))

    if not paths:
        return {"keys": [], "example_values": {}, "format": "none"}

    # Patterns to detect
    hive_pattern = re.compile(r"(\w+)=([^/\\]+)")
    date_patterns = [
        re.compile(r"/year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})"),
        re.compile(r"/(\d{4})/(\d{2})/(\d{2})"),  # year/month/day
        re.compile(r"/(\d{4})-(\d{2})-(\d{2})"),  # year-month-day
    ]

    # Check for Hive-style partitioning
    hive_keys = {}
    for path in paths[:100]:  # Sample first 100 paths
        matches = hive_pattern.findall(path)
        for key, value in matches:
            if key not in hive_keys:
                hive_keys[key] = set()
            hive_keys[key].add(value)

    if hive_keys:
        example_values = {k: sorted(list(v))[:10] for k, v in hive_keys.items()}
        ctx.info("Detected Hive-style partitions", keys=list(hive_keys.keys()))
        return {
            "keys": list(hive_keys.keys()),
            "example_values": example_values,
            "format": "hive",
        }

    # Check for date-based partitioning
    for pattern in date_patterns:
        for path in paths[:100]:
            match = pattern.search(path)
            if match:
                ctx.info("Detected date-based partitions")
                return {
                    "keys": ["year", "month", "day"],
                    "example_values": {
                        "year": [match.group(1)],
                        "month": [match.group(2)],
                        "day": [match.group(3)],
                    },
                    "format": "date",
                }

    ctx.debug("No partitioning detected")
    return {"keys": [], "example_values": {}, "format": "none"}


def infer_format_from_path(path: str) -> Optional[str]:
    """Infer file format from path extension.

    Args:
        path: File or folder path

    Returns:
        Format string: "csv", "parquet", "json", "delta", "excel", etc.
        None if cannot determine
    """
    # Check for Delta Lake
    if "_delta_log" in path or path.endswith("/_delta_log"):
        return "delta"

    # Extension-based detection
    ext_map = {
        ".csv": "csv",
        ".parquet": "parquet",
        ".pq": "parquet",
        ".json": "json",
        ".jsonl": "json",
        ".ndjson": "json",
        ".xlsx": "excel",
        ".xls": "excel",
        ".avro": "avro",
        ".orc": "orc",
        ".txt": "text",
        ".tsv": "csv",
    }

    path_obj = Path(path)
    suffix = path_obj.suffix

    return ext_map.get(suffix)


def detect_file_format(path: str, fs=None) -> Optional[str]:
    """Detect file format by checking for Delta Lake or using extension.

    Args:
        path: File or folder path
        fs: Optional filesystem object (fsspec or similar) for remote paths

    Returns:
        Format string or None
    """
    ctx = get_logging_context()

    # Check for _delta_log folder (Delta Lake)
    if fs:
        try:
            # Try to list contents
            if hasattr(fs, "ls"):
                contents = fs.ls(path, detail=False)
                if any("_delta_log" in str(p) for p in contents):
                    ctx.debug("Detected Delta Lake format", path=path)
                    return "delta"
        except Exception as e:
            ctx.warning("Failed to check for Delta Lake", path=path, error=str(e))
    else:
        # Local filesystem
        path_obj = Path(path)
        if path_obj.is_dir():
            delta_log = path_obj / "_delta_log"
            if delta_log.exists():
                ctx.debug("Detected Delta Lake format", path=path)
                return "delta"

    # Fallback to extension-based detection
    return infer_format_from_path(path)
