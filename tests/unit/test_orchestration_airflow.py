import pytest

from odibi.orchestration.airflow import AirflowExporter

# Dummy classes to simulate configuration objects


class DummyNode:
    def __init__(self, name, depends_on):
        self.name = name
        self.depends_on = depends_on


class DummyPipeline:
    def __init__(self, pipeline, nodes, description=None, layer=None):
        self.pipeline = pipeline
        self.nodes = nodes
        self.description = description
        self.layer = layer


class DummyRetry:
    def __init__(self, enabled, max_attempts):
        self.enabled = enabled
        self.max_attempts = max_attempts


class DummyProjectConfig:
    def __init__(self, project, pipelines, owner, retry):
        self.project = project
        self.pipelines = pipelines
        self.owner = owner
        self.retry = retry


@pytest.fixture
def valid_project_config():
    # Create a dummy pipeline config with two nodes having names that require sanitization.
    node1 = DummyNode("node-1", depends_on=[])
    node2 = DummyNode("node/2", depends_on=["node-1"])
    pipeline = DummyPipeline(
        pipeline="pipeline1", nodes=[node1, node2], description="Test Pipeline", layer="test"
    )
    retry = DummyRetry(enabled=True, max_attempts=3)
    config = DummyProjectConfig(
        project="TestProject", pipelines=[pipeline], owner="tester", retry=retry
    )
    return config


@pytest.fixture
def exporter(valid_project_config):
    return AirflowExporter(config=valid_project_config)


def test_generate_code_invalid_pipeline_raises_value_error(exporter):
    """
    test_generate_code_invalid_pipeline_raises_value_error:
    If the specified pipeline is not found in the config, generate_code should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        exporter.generate_code("nonexistent_pipeline")
    assert "Pipeline 'nonexistent_pipeline' not found" in str(exc_info.value)


def test_generate_code_valid_config_returns_code(exporter):
    """
    test_generate_code_valid_config_returns_code:
    With a valid configuration, generate_code should return the generated DAG code as a string containing key elements.
    """
    generated_code = exporter.generate_code("pipeline1")
    # Check that the generated code contains key template markers and config values.
    assert "from airflow import DAG" in generated_code
    assert "TestProject" in generated_code
    assert "pipeline1" in generated_code
    assert "tester" in generated_code
    assert "Test Pipeline" in generated_code


def test_generate_code_includes_retry_attempts(exporter):
    """
    test_generate_code_includes_retry_attempts:
    When retry is enabled, the generated code should include the correct number of retries from the config.
    """
    generated_code = exporter.generate_code("pipeline1")
    # Verify that the retries value (3) is included in the default_args section.
    assert ("'retries': 3" in generated_code) or ('"retries": 3' in generated_code)


def test_generate_code_sanitizes_node_names(exporter):
    """
    test_generate_code_sanitizes_node_names:
    The generate_code method should sanitize node names, converting invalid characters to underscores.
    For example, "node-1" becomes "node_1" and "node/2" becomes "node_2".
    """
    generated_code = exporter.generate_code("pipeline1")
    # Check that sanitized variable names appear in the generated BashOperator assignments.
    assert "node_1 = BashOperator" in generated_code
    assert "node_2 = BashOperator" in generated_code
    # Additionally, verify that dependency lines include sanitized upstream variable names.
    # For node "node/2", its dependency ["node-1"] should be sanitized to "node_1".
    assert "node_1" in generated_code
