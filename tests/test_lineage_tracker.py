"""Tests for cross-pipeline lineage tracking functionality."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.lineage import LineageTracker


class MockCatalog:
    """Mock catalog for testing LineageTracker."""

    def __init__(self):
        self.recorded_lineage = []
        self._upstream_data = []
        self._downstream_data = []

    def record_lineage(self, **kwargs):
        self.recorded_lineage.append(kwargs)

    def get_upstream(self, table_path, depth=3):
        return [r for r in self._upstream_data if r.get("target_table") == table_path]

    def get_downstream(self, table_path, depth=3):
        return [r for r in self._downstream_data if r.get("source_table") == table_path]


class MockReadConfig:
    """Mock read config."""

    def __init__(self, connection, path=None, table=None):
        self.connection = connection
        self.path = path
        self.table = table


class MockWriteConfig:
    """Mock write config."""

    def __init__(self, connection, path=None, table=None):
        self.connection = connection
        self.path = path
        self.table = table


class MockConnection:
    """Mock connection config."""

    def __init__(self, schema_name=None, catalog=None):
        self.schema_name = schema_name
        self.catalog = catalog


@pytest.fixture
def mock_catalog():
    return MockCatalog()


@pytest.fixture
def lineage_tracker(mock_catalog):
    return LineageTracker(catalog=mock_catalog)


class TestLineageTrackerRecordLineage:
    """Tests for LineageTracker.record_lineage."""

    def test_record_lineage_with_read_write(self, lineage_tracker, mock_catalog):
        """Should record lineage from read to write."""
        read_config = MockReadConfig(connection="bronze", path="customers_raw")
        write_config = MockWriteConfig(connection="silver", path="customers_clean")
        connections = {"bronze": MagicMock(), "silver": MagicMock()}

        lineage_tracker.record_lineage(
            read_config=read_config,
            write_config=write_config,
            pipeline="silver_pipeline",
            node="clean_customers",
            run_id="run-123",
            connections=connections,
        )

        assert len(mock_catalog.recorded_lineage) == 1
        record = mock_catalog.recorded_lineage[0]
        assert record["source_table"] == "bronze/customers_raw"
        assert record["target_table"] == "silver/customers_clean"
        assert record["target_pipeline"] == "silver_pipeline"
        assert record["target_node"] == "clean_customers"

    def test_record_lineage_no_write_config(self, lineage_tracker, mock_catalog):
        """Should not record lineage without write config."""
        read_config = MockReadConfig(connection="bronze", path="customers_raw")
        connections = {"bronze": MagicMock()}

        lineage_tracker.record_lineage(
            read_config=read_config,
            write_config=None,
            pipeline="test",
            node="test",
            run_id="run-123",
            connections=connections,
        )

        assert len(mock_catalog.recorded_lineage) == 0

    def test_record_lineage_no_catalog(self):
        """Should handle missing catalog gracefully."""
        tracker = LineageTracker(catalog=None)
        read_config = MockReadConfig(connection="bronze", path="test")
        write_config = MockWriteConfig(connection="silver", path="test")

        tracker.record_lineage(
            read_config=read_config,
            write_config=write_config,
            pipeline="test",
            node="test",
            run_id="run-123",
            connections={},
        )


class TestLineageTrackerResolveTablePath:
    """Tests for table path resolution."""

    def test_resolve_path_based(self, lineage_tracker):
        """Should resolve path-based table reference."""
        config = MockWriteConfig(connection="bronze", path="customers/raw")
        connections = {"bronze": MagicMock()}

        result = lineage_tracker._resolve_table_path(config, connections)

        assert result == "bronze/customers/raw"

    def test_resolve_table_based(self, lineage_tracker):
        """Should resolve table-based reference without schema_name attribute."""
        config = MockWriteConfig(connection="silver", table="customers")

        class MockConn:
            pass

        connections = {"silver": MockConn()}

        result = lineage_tracker._resolve_table_path(config, connections)

        assert result == "silver.customers"

    def test_resolve_table_with_schema(self, lineage_tracker):
        """Should resolve table with schema from connection."""
        config = MockWriteConfig(connection="delta", table="customers")
        conn = MockConnection(schema_name="silver_db", catalog="spark_catalog")
        connections = {"delta": conn}

        result = lineage_tracker._resolve_table_path(config, connections)

        assert result == "spark_catalog.silver_db.customers"

    def test_resolve_table_with_schema_no_catalog(self, lineage_tracker):
        """Should resolve table with schema but no catalog."""
        config = MockWriteConfig(connection="delta", table="customers")
        conn = MockConnection(schema_name="silver_db")
        connections = {"delta": conn}

        result = lineage_tracker._resolve_table_path(config, connections)

        assert result == "silver_db.customers"


class TestLineageTrackerGetUpstreamDownstream:
    """Tests for upstream/downstream lineage traversal."""

    def test_get_upstream(self, mock_catalog):
        """Should return upstream lineage."""
        mock_catalog._upstream_data = [
            {
                "source_table": "bronze/customers",
                "target_table": "silver/customers",
                "depth": 0,
            },
            {
                "source_table": "external/source",
                "target_table": "bronze/customers",
                "depth": 1,
            },
        ]

        tracker = LineageTracker(catalog=mock_catalog)
        upstream = tracker.get_upstream("silver/customers")

        assert len(upstream) == 1
        assert upstream[0]["source_table"] == "bronze/customers"

    def test_get_downstream(self, mock_catalog):
        """Should return downstream lineage."""
        mock_catalog._downstream_data = [
            {
                "source_table": "silver/customers",
                "target_table": "gold/customer_360",
                "depth": 0,
            },
        ]

        tracker = LineageTracker(catalog=mock_catalog)
        downstream = tracker.get_downstream("silver/customers")

        assert len(downstream) == 1
        assert downstream[0]["target_table"] == "gold/customer_360"

    def test_get_upstream_no_catalog(self):
        """Should return empty list without catalog."""
        tracker = LineageTracker(catalog=None)
        upstream = tracker.get_upstream("silver/customers")
        assert upstream == []


class TestLineageTrackerImpactAnalysis:
    """Tests for impact analysis functionality."""

    def test_impact_analysis(self, mock_catalog):
        """Should calculate impact correctly."""
        mock_catalog._downstream_data = [
            {
                "source_table": "bronze/customers",
                "target_table": "silver/dim_customers",
                "target_pipeline": "silver_pipeline",
                "depth": 0,
            },
            {
                "source_table": "bronze/customers",
                "target_table": "silver/customer_events",
                "target_pipeline": "silver_pipeline",
                "depth": 0,
            },
            {
                "source_table": "bronze/customers",
                "target_table": "gold/customer_360",
                "target_pipeline": "gold_pipeline",
                "depth": 1,
            },
        ]

        tracker = LineageTracker(catalog=mock_catalog)
        impact = tracker.get_impact_analysis("bronze/customers")

        assert len(impact["affected_tables"]) == 3
        assert len(impact["affected_pipelines"]) == 2
        assert "silver_pipeline" in impact["affected_pipelines"]
        assert "gold_pipeline" in impact["affected_pipelines"]
        assert impact["downstream_count"] == 3

    def test_impact_analysis_no_downstream(self, mock_catalog):
        """Should handle tables with no downstream dependencies."""
        mock_catalog._downstream_data = []

        tracker = LineageTracker(catalog=mock_catalog)
        impact = tracker.get_impact_analysis("gold/customers")

        assert impact["affected_tables"] == []
        assert impact["affected_pipelines"] == []
        assert impact["downstream_count"] == 0


class TestCatalogManagerLineage:
    """Tests for CatalogManager lineage methods."""

    @pytest.fixture
    def temp_catalog(self, tmp_path):
        """Create a temp catalog manager."""
        config = SystemConfig(connection="local", path="_odibi_system")

        class MockEngine:
            name = "pandas"

            def write(self, df, **kwargs):
                return {}

        return CatalogManager(
            spark=None, config=config, base_path=str(tmp_path), engine=MockEngine()
        )

    def test_meta_lineage_schema_has_required_fields(self, temp_catalog):
        """meta_lineage should have all required fields."""
        schema = temp_catalog._get_schema_meta_lineage()
        field_names = [f.name for f in schema.fields]

        assert "source_table" in field_names
        assert "target_table" in field_names
        assert "source_pipeline" in field_names
        assert "source_node" in field_names
        assert "target_pipeline" in field_names
        assert "target_node" in field_names
        assert "relationship" in field_names
        assert "last_observed" in field_names
        assert "run_id" in field_names

    def test_record_lineage_stores_relationship(self, temp_catalog):
        """record_lineage should store the relationship."""
        with patch.object(temp_catalog.engine, "write") as mock_write:
            temp_catalog.record_lineage(
                source_table="bronze/customers",
                target_table="silver/customers",
                target_pipeline="silver_pipeline",
                target_node="clean",
                run_id="run-123",
            )

            assert mock_write.called
            call_args = mock_write.call_args
            df = call_args[0][0]
            assert df["source_table"].iloc[0] == "bronze/customers"
            assert df["target_table"].iloc[0] == "silver/customers"

    def test_get_upstream_empty(self, temp_catalog):
        """get_upstream should return empty list when no data."""
        with patch.object(temp_catalog, "_read_local_table", return_value=pd.DataFrame()):
            upstream = temp_catalog.get_upstream("silver/customers")

        assert upstream == []

    def test_get_downstream_empty(self, temp_catalog):
        """get_downstream should return empty list when no data."""
        with patch.object(temp_catalog, "_read_local_table", return_value=pd.DataFrame()):
            downstream = temp_catalog.get_downstream("bronze/customers")

        assert downstream == []


class TestDependencyLineage:
    """Tests for recording lineage from node dependencies."""

    def test_record_dependency_lineage(self, mock_catalog):
        """Should record lineage from depends_on nodes."""
        tracker = LineageTracker(catalog=mock_catalog)
        write_config = MockWriteConfig(connection="silver", path="deduped_customers")

        node_outputs = {
            "clean_customers": "silver/clean_customers",
            "filter_customers": "silver/filtered_customers",
        }

        tracker.record_dependency_lineage(
            depends_on=["clean_customers", "filter_customers"],
            write_config=write_config,
            pipeline="silver_pipeline",
            node="dedupe",
            run_id="run-123",
            node_outputs=node_outputs,
            connections={"silver": MagicMock()},
        )

        assert len(mock_catalog.recorded_lineage) == 2

        sources = [r["source_table"] for r in mock_catalog.recorded_lineage]
        assert "silver/clean_customers" in sources
        assert "silver/filtered_customers" in sources

    def test_record_dependency_lineage_missing_output(self, mock_catalog):
        """Should skip dependencies without output paths."""
        tracker = LineageTracker(catalog=mock_catalog)
        write_config = MockWriteConfig(connection="silver", path="result")

        node_outputs = {"node1": "silver/node1_output"}

        tracker.record_dependency_lineage(
            depends_on=["node1", "node_without_output"],
            write_config=write_config,
            pipeline="test",
            node="result",
            run_id="run-123",
            node_outputs=node_outputs,
            connections={"silver": MagicMock()},
        )

        assert len(mock_catalog.recorded_lineage) == 1
        assert mock_catalog.recorded_lineage[0]["source_table"] == "silver/node1_output"
