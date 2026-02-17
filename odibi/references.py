"""Cross-pipeline reference resolution for Odibi.

This module handles the resolution of $pipeline.node references, enabling
pipelines to read data from other pipelines' outputs (e.g., bronze -> silver).

Example:
    inputs:
      events: $read_bronze.opsvisdata_ShiftDowntimeEventsview
      calendar: $read_bronze.opsvisdata_vw_calender
"""

from typing import Any, Dict, Union

from odibi.catalog import CatalogManager
from odibi.exceptions import OdibiException


class ReferenceResolutionError(OdibiException):
    """Raised when a cross-pipeline reference cannot be resolved."""

    pass


def resolve_input_reference(
    ref: str,
    catalog: CatalogManager,
) -> Dict[str, Any]:
    """
    Resolves $pipeline.node to read configuration.

    Args:
        ref: Reference string like "$read_bronze.opsvisdata_vw_calender"
        catalog: CatalogManager instance

    Returns:
        Dict with keys for engine.read():
            - For external_table: connection, path, format
            - For managed_table: table, format

    Raises:
        ValueError: If reference format is invalid
        ReferenceResolutionError: If referenced node output is not found
    """
    if not ref.startswith("$"):
        raise ValueError(f"Invalid reference: {ref}. Expected $pipeline.node format.")

    parts = ref[1:].split(".", 1)  # Remove $ and split
    if len(parts) != 2:
        raise ValueError(
            f"Invalid reference format: {ref}. Expected $pipeline.node (e.g., $read_bronze.my_node)"
        )

    pipeline_name, node_name = parts

    output = catalog.get_node_output(pipeline_name, node_name)

    if output is None:
        raise ReferenceResolutionError(
            f"No output found for {ref}. "
            f"Ensure pipeline '{pipeline_name}' has run and node '{node_name}' has a write block."
        )

    if output.get("output_type") == "managed_table":
        return {
            "table": output.get("table_name"),
            "format": output.get("format"),
        }
    else:  # external_table
        return {
            "connection": output.get("connection_name"),
            "path": output.get("path"),
            "format": output.get("format"),
        }


def is_pipeline_reference(value: Any) -> bool:
    """
    Check if a value is a cross-pipeline reference.

    Args:
        value: Value to check

    Returns:
        True if value is a string starting with $
    """
    return isinstance(value, str) and value.startswith("$")


def resolve_inputs(
    inputs: Dict[str, Union[str, Dict[str, Any]]],
    catalog: CatalogManager,
) -> Dict[str, Dict[str, Any]]:
    """
    Resolve all inputs, converting $pipeline.node references to read configs.

    Args:
        inputs: Dict mapping input name to either:
            - A $pipeline.node reference string
            - An explicit read config dict

    Returns:
        Dict mapping input name to resolved read configuration

    Example:
        inputs = {
            "events": "$read_bronze.shift_events",
            "calendar": {
                "connection": "goat_prod",
                "path": "bronze/OEE/vw_calender",
                "format": "delta"
            }
        }
        resolved = resolve_inputs(inputs, catalog)
        # Returns:
        # {
        #     "events": {"connection": "goat_prod", "path": "bronze/OEE/shift_events", "format": "delta"},
        #     "calendar": {"connection": "goat_prod", "path": "bronze/OEE/vw_calender", "format": "delta"}
        # }
    """
    resolved = {}

    for name, ref in inputs.items():
        if is_pipeline_reference(ref):
            resolved[name] = resolve_input_reference(ref, catalog)
        elif isinstance(ref, dict):
            resolved[name] = ref
        else:
            raise ValueError(
                f"Invalid input format for '{name}': {ref}. "
                "Expected $pipeline.node reference or read config dict."
            )

    return resolved


def validate_references(
    inputs: Dict[str, Union[str, Dict[str, Any]]],
    catalog: CatalogManager,
) -> None:
    """
    Validate all cross-pipeline references at pipeline load time (fail fast).

    Args:
        inputs: Dict of input configurations
        catalog: CatalogManager instance

    Raises:
        ReferenceResolutionError: If any reference cannot be resolved
    """
    for name, ref in inputs.items():
        if is_pipeline_reference(ref):
            resolve_input_reference(ref, catalog)
