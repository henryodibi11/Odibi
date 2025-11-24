import pytest
import pandas as pd
from odibi.engine.pandas_engine import PandasEngine
from odibi.node import Node
from odibi.config import NodeConfig
from odibi.context import PandasContext


@pytest.fixture
def engine():
    return PandasEngine()


class TestPandasEngineDiagnostics:
    """Test new diagnostic methods in PandasEngine."""

    def test_get_source_files_csv(self, engine, tmp_path):
        """Test get_source_files for CSV read."""
        # Simulate read which attaches attrs
        df = pd.DataFrame({"a": [1]})
        path = str(tmp_path / "test.csv")
        df.attrs["odibi_source_files"] = [path]

        files = engine.get_source_files(df)
        assert files == [path]

    def test_get_source_files_empty(self, engine):
        """Test get_source_files for DataFrame without source info."""
        df = pd.DataFrame({"a": [1]})
        assert engine.get_source_files(df) == []

    def test_profile_nulls(self, engine):
        """Test null profiling calculation."""
        df = pd.DataFrame({"full": [1, 2, 3], "partial": [1, None, 3], "empty": [None, None, None]})

        profile = engine.profile_nulls(df)

        assert profile["full"] == 0.0
        assert pytest.approx(profile["partial"]) == 0.333333333
        assert profile["empty"] == 1.0


class TestNodeMetadataCollection:
    """Test that Node collects diagnostic metadata correctly."""

    def test_collect_metadata_environment(self):
        """Test that execution environment is captured."""
        # Mock Node (partial)
        config = NodeConfig(name="test_node", transform={"steps": ["SELECT 1"]})
        node = Node(config=config, context=PandasContext(), engine=PandasEngine(), connections={})

        # Mock execution steps
        node._execution_steps = ["step 1"]

        # Test with simple DF
        df = pd.DataFrame({"a": [1]})

        meta = node._collect_metadata(df)

        assert "environment" in meta
        env = meta["environment"]
        assert "user" in env
        assert "host" in env
        assert "odibi" in env
        assert env["odibi"] == __import__("odibi").__version__
        assert env["pandas"] is not None

    def test_collect_metadata_source_and_profile(self):
        """Test that source files and null profile are collected."""
        config = NodeConfig(name="test_node", transform={"steps": ["SELECT 1"]})
        node = Node(config=config, context=PandasContext(), engine=PandasEngine(), connections={})

        df = pd.DataFrame({"a": [1, None]})
        df.attrs["odibi_source_files"] = ["file.csv"]

        meta = node._collect_metadata(df)

        assert meta["source_files"] == ["file.csv"]
        assert meta["null_profile"]["a"] == 0.5


class TestDeltaDiffLogic:
    """Test delta diff logic using mocks."""

    def test_delta_diff_result_structure(self):
        """Test that DeltaDiffResult is constructed correctly."""
        from odibi.diagnostics.delta import DeltaDiffResult

        res = DeltaDiffResult(
            table_path="path/to/table",
            version_a=1,
            version_b=2,
            rows_change=10,
            files_change=1,
            size_change_bytes=1000,
            schema_added=["new_col"],
            schema_removed=[],
            schema_current=["old_col", "new_col"],
            schema_previous=["old_col"],
        )

        assert res.rows_change == 10
        assert res.schema_added == ["new_col"]
