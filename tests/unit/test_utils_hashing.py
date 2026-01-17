import hashlib
import json
from odibi.utils import hashing


# A dummy model to simulate a Pydantic-like object with a model_dump method.
class DummyModel:
    def __init__(self, data):
        self.data = data

    def model_dump(self, mode=None, exclude=None):
        if exclude is None:
            return self.data
        # Exclude specified keys from the data.
        return {k: v for k, v in self.data.items() if k not in exclude}


# Helper function to compute the expected MD5 hash.
def compute_expected_md5(data):
    dump_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(dump_str.encode("utf-8")).hexdigest()


# ------------------ Tests for calculate_config_hash ------------------


def test_calculate_config_hash_with_dict():
    """
    When a dict is provided, calculate_config_hash should compute the MD5 hash based on the full dict.
    Note: Exclusions are only applied if the object has a model_dump method.
    """
    config = {"a": 1, "b": 2, "description": "should be present for dict input"}
    expected = compute_expected_md5(config)
    result = hashing.calculate_config_hash(config)
    assert result == expected


def test_calculate_config_hash_with_none():
    """
    When None is provided, json.dumps(None) returns "null", so the MD5 should be computed on that.
    """
    config = None
    expected = compute_expected_md5(config)
    result = hashing.calculate_config_hash(config)
    assert result == expected


def test_calculate_config_hash_with_string_special_characters():
    """
    When a string with special characters is provided, the hash should be consistent.
    """
    config = "!@#$%^&*()_+-=[]{}|;':,./<>?"
    expected = compute_expected_md5(config)
    result = hashing.calculate_config_hash(config)
    assert result == expected


def test_calculate_config_hash_with_large_input():
    """
    When a large string is provided, the hash should be computed consistently.
    """
    config = "a" * 10**6  # 1 million 'a' characters
    expected = compute_expected_md5(config)
    result = hashing.calculate_config_hash(config)
    assert result == expected


def test_calculate_config_hash_with_model_dump():
    """
    For objects with a model_dump method, calculate_config_hash should use the dumped data
    with DEFAULT_EXCLUDE_FIELDS applied.
    """
    data = {"name": "Test", "value": 123, "description": "to be excluded", "tags": ["x", "y"]}
    model = DummyModel(data)
    # Expected data after exclusion of default fields.
    expected_data = {k: v for k, v in data.items() if k not in hashing.DEFAULT_EXCLUDE_FIELDS}
    expected = compute_expected_md5(expected_data)
    result = hashing.calculate_config_hash(model)
    assert result == expected


# ------------------ Tests for calculate_pipeline_hash ------------------


def test_calculate_pipeline_hash_with_dict():
    """
    For pipeline configurations, calculate_pipeline_hash should include all fields.
    """
    config = {
        "pipeline": "pipe1",
        "value": 456,
        "description": "included even if normally excluded",
    }
    expected = compute_expected_md5(config)
    result = hashing.calculate_pipeline_hash(config)
    assert result == expected


def test_calculate_pipeline_hash_with_model_dump():
    """
    For objects with a model_dump method, calculate_pipeline_hash should use the full dumped data.
    """
    data = {"pipeline": "pipe1", "value": 456, "description": "included"}
    model = DummyModel(data)
    expected = compute_expected_md5(data)
    result = hashing.calculate_pipeline_hash(model)
    assert result == expected


# ------------------ Tests for calculate_node_hash ------------------


def test_calculate_node_hash_with_dict():
    """
    For node configurations provided as a dict, since dicts do not have a model_dump method,
    calculate_node_hash behaves like calculate_config_hash without exclusion.
    """
    config = {
        "node": "node1",
        "value": 789,
        "description": "should be present",
        "tags": ["alpha"],
        "log_level": "debug",
    }
    expected = compute_expected_md5(config)
    result = hashing.calculate_node_hash(config)
    assert result == expected


def test_calculate_node_hash_with_model_dump():
    """
    For objects with a model_dump method, calculate_node_hash should exclude the fields in DEFAULT_EXCLUDE_FIELDS.
    """
    data = {
        "node": "node1",
        "value": 789,
        "description": "remove this",
        "tags": ["alpha"],
        "log_level": "debug",
    }
    model = DummyModel(data)
    expected_data = {k: v for k, v in data.items() if k not in hashing.DEFAULT_EXCLUDE_FIELDS}
    expected = compute_expected_md5(expected_data)
    result = hashing.calculate_node_hash(model)
    assert result == expected
