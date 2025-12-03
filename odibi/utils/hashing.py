"""Utilities for calculating configuration hashes."""

import hashlib
import json
from typing import Any, Optional, Set


DEFAULT_EXCLUDE_FIELDS: Set[str] = {"description", "tags", "log_level"}


def calculate_config_hash(
    config: Any,
    exclude: Optional[Set[str]] = None,
) -> str:
    """
    Calculate MD5 hash of a configuration object.

    Args:
        config: Pydantic model or object with model_dump/dict method
        exclude: Set of field names to exclude from hash calculation

    Returns:
        MD5 hex digest of the config
    """
    exclude = exclude if exclude is not None else DEFAULT_EXCLUDE_FIELDS

    if hasattr(config, "model_dump"):
        dump = config.model_dump(mode="json", exclude=exclude)
    elif hasattr(config, "dict"):
        dump = config.dict(exclude=exclude)
    else:
        dump = config

    dump_str = json.dumps(dump, sort_keys=True)
    return hashlib.md5(dump_str.encode("utf-8")).hexdigest()


def calculate_pipeline_hash(config: Any) -> str:
    """
    Calculate hash for a pipeline configuration.

    Pipeline hashes include all fields (no exclusions).
    """
    if hasattr(config, "model_dump"):
        dump = config.model_dump(mode="json")
    elif hasattr(config, "dict"):
        dump = config.dict()
    else:
        dump = config

    dump_str = json.dumps(dump, sort_keys=True)
    return hashlib.md5(dump_str.encode("utf-8")).hexdigest()


def calculate_node_hash(config: Any) -> str:
    """
    Calculate hash for a node configuration.

    Node hashes exclude description, tags, and log_level.
    """
    return calculate_config_hash(config, exclude=DEFAULT_EXCLUDE_FIELDS)
