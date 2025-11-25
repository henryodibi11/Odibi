import pandas as pd
import pytest

from odibi.config import NodeConfig, ReadConfig
from odibi.context import PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.node import Node


class TestPIIRedaction:
    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def connections(self, tmp_path):
        class MockConn:
            def get_path(self, p):
                return str(tmp_path / p)

            def pandas_storage_options(self):
                return {}

        return {"local": MockConn()}

    def test_redaction_enabled(self, engine, context, connections, tmp_path):
        """Test that sample data is redacted when sensitive=True."""
        # Create sensitive data
        df = pd.DataFrame({"ssn": ["123-456-7890"], "name": ["John Doe"]})
        path = tmp_path / "sensitive.csv"
        df.to_csv(path, index=False)

        config = NodeConfig(
            name="sensitive_node",
            read=ReadConfig(connection="local", format="csv", path="sensitive.csv"),
            sensitive=True,
        )

        node = Node(config=config, context=context, engine=engine, connections=connections)

        result = node.execute()

        assert result.success
        # Check metadata for redaction
        assert "sample_data" in result.metadata
        assert len(result.metadata["sample_data"]) == 1
        assert result.metadata["sample_data"][0]["message"] == "[REDACTED: Sensitive Data]"
        # Ensure actual data is not there
        assert "ssn" not in str(result.metadata["sample_data"])

    def test_redaction_specific_columns(self, engine, context, connections, tmp_path):
        """Test that only specified columns are redacted."""
        df = pd.DataFrame({"ssn": ["123-456-7890"], "name": ["John Doe"], "city": ["New York"]})
        path = tmp_path / "mixed.csv"
        df.to_csv(path, index=False)

        config = NodeConfig(
            name="mixed_node",
            read=ReadConfig(connection="local", format="csv", path="mixed.csv"),
            sensitive=["ssn", "name"],  # Redact ssn and name, keep city
        )

        node = Node(config=config, context=context, engine=engine, connections=connections)

        result = node.execute()

        assert result.success
        assert "sample_data" in result.metadata
        sample = result.metadata["sample_data"][0]

        assert sample["ssn"] == "[REDACTED]"
        assert sample["name"] == "[REDACTED]"
        assert sample["city"] == "New York"
