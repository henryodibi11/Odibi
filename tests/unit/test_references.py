import pytest
from unittest.mock import MagicMock
from odibi.references import (
    resolve_input_reference,
    is_pipeline_reference,
    resolve_inputs,
    validate_references,
    ReferenceResolutionError,
)


@pytest.fixture
def fake_catalog():
    """
    Fixture for a fake CatalogManager using MagicMock.
    """
    return MagicMock()


# Tests for is_pipeline_reference


def test_is_pipeline_reference_with_valid_reference():
    assert is_pipeline_reference("$read_bronze.some_node") is True


def test_is_pipeline_reference_with_invalid_type():
    assert is_pipeline_reference("read_bronze.some_node") is False
    assert is_pipeline_reference(123) is False


# Tests for resolve_input_reference


def test_resolve_input_reference_invalid_prefix(fake_catalog):
    with pytest.raises(ValueError) as exc_info:
        resolve_input_reference("read_bronze.some_node", fake_catalog)
    assert "Invalid reference" in str(exc_info.value)


def test_resolve_input_reference_invalid_format_no_dot(fake_catalog):
    with pytest.raises(ValueError) as exc_info:
        resolve_input_reference("$invalidreference", fake_catalog)
    assert "Invalid reference format" in str(exc_info.value)


def test_resolve_input_reference_unresolved(fake_catalog):
    # Simulate get_node_output returning None for an unresolved reference.
    fake_catalog.get_node_output.return_value = None
    with pytest.raises(ReferenceResolutionError) as exc_info:
        resolve_input_reference("$read_bronze.shift_events", fake_catalog)
    assert "No output found for" in str(exc_info.value)


def test_resolve_input_reference_managed_table(fake_catalog):
    # Setup fake output for a managed table.
    fake_catalog.get_node_output.return_value = {
        "output_type": "managed_table",
        "table_name": "my_table",
        "format": "parquet",
    }
    result = resolve_input_reference("$read_bronze.shift_events", fake_catalog)
    assert result == {"table": "my_table", "format": "parquet"}
    fake_catalog.get_node_output.assert_called_once_with("read_bronze", "shift_events")


def test_resolve_input_reference_external_table(fake_catalog):
    # Setup fake output for an external table.
    fake_catalog.get_node_output.return_value = {
        "output_type": "external_table",
        "connection_name": "my_conn",
        "path": "data/path",
        "format": "csv",
    }
    result = resolve_input_reference("$read_bronze.shift_events", fake_catalog)
    assert result == {"connection": "my_conn", "path": "data/path", "format": "csv"}


# Tests for resolve_inputs


def test_resolve_inputs_with_mixed_configurations(fake_catalog):
    # Setup fake output for pipeline reference resolution.
    fake_catalog.get_node_output.return_value = {
        "output_type": "managed_table",
        "table_name": "resolved_table",
        "format": "parquet",
    }
    inputs = {
        "events": "$read_bronze.shift_events",
        "calendar": {
            "connection": "my_conn",
            "path": "data/calender",
            "format": "delta",
        },
    }
    resolved = resolve_inputs(inputs, fake_catalog)
    assert resolved["events"] == {"table": "resolved_table", "format": "parquet"}
    assert resolved["calendar"] == {
        "connection": "my_conn",
        "path": "data/calender",
        "format": "delta",
    }


def test_resolve_inputs_invalid_format(fake_catalog):
    inputs = {
        "invalid_input": 123,  # Neither a string nor a dict
    }
    with pytest.raises(ValueError) as exc_info:
        resolve_inputs(inputs, fake_catalog)
    assert "Invalid input format" in str(exc_info.value)


# Tests for validate_references


def test_validate_references_valid(fake_catalog):
    # For a valid pipeline reference, simulate a successful resolution.
    fake_catalog.get_node_output.return_value = {
        "output_type": "external_table",
        "connection_name": "conn",
        "path": "some/path",
        "format": "json",
    }
    inputs = {
        "data": "$read_bronze.shift_events",
        "config": {
            "connection": "another_conn",
            "path": "other/path",
            "format": "json",
        },
    }
    # Should not raise any exception.
    validate_references(inputs, fake_catalog)


def test_validate_references_invalid(fake_catalog):
    # Simulate failure in resolving the reference.
    fake_catalog.get_node_output.return_value = None
    inputs = {
        "data": "$read_bronze.shift_events",
    }
    with pytest.raises(ReferenceResolutionError) as exc_info:
        validate_references(inputs, fake_catalog)
    assert "No output found for" in str(exc_info.value)
