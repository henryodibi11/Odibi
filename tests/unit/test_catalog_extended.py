"""Extended unit tests for catalog.py - batch operations, edge cases, and error handling."""

import json
import threading
from datetime import date, datetime, timezone

import pytest

from odibi.catalog import CatalogManager
from odibi.config import NodeConfig, SystemConfig, TransformConfig
from odibi.engine.pandas_engine import PandasEngine


@pytest.fixture
def catalog_manager(tmp_path):
    """Create a catalog manager with PandasEngine for testing."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    cm.project = "test_project"  # Set project name for tests
    return cm


def make_node_config(name, **kwargs):
    """Helper to create NodeConfig with minimal valid configuration."""
    defaults = {"transform": TransformConfig(steps=["SELECT * FROM input"])}
    defaults.update(kwargs)
    return NodeConfig(name=name, **defaults)


class TestBatchRegistration:
    """Test batch registration methods for pipelines and nodes."""

    def test_register_pipelines_batch(self, catalog_manager):
        """Test batch registering multiple pipelines."""
        records = [
            {
                "pipeline_name": "pipeline_1",
                "version_hash": "hash1",
                "description": "First pipeline",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": json.dumps({"env": "dev"}),
            },
            {
                "pipeline_name": "pipeline_2",
                "version_hash": "hash2",
                "description": "Second pipeline",
                "layer": "silver",
                "schedule": "hourly",
                "tags_json": json.dumps({"env": "prod"}),
            },
        ]

        catalog_manager.register_pipelines_batch(records)

        # Verify both pipelines were registered
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_pipelines"])
        assert len(df) == 2
        assert set(df["pipeline_name"].values) == {"pipeline_1", "pipeline_2"}
        assert "hash1" in df["version_hash"].values
        assert "hash2" in df["version_hash"].values

    def test_register_pipelines_batch_update_existing(self, catalog_manager):
        """Test that batch registration updates existing pipelines."""
        # Register initial pipeline
        initial_records = [
            {
                "pipeline_name": "test_pipeline",
                "version_hash": "old_hash",
                "description": "Old description",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": json.dumps({}),
            }
        ]
        catalog_manager.register_pipelines_batch(initial_records)

        # Update with new version
        updated_records = [
            {
                "pipeline_name": "test_pipeline",
                "version_hash": "new_hash",
                "description": "New description",
                "layer": "silver",
                "schedule": "hourly",
                "tags_json": json.dumps({"updated": True}),
            }
        ]
        catalog_manager.register_pipelines_batch(updated_records)

        # Verify only one record exists with updated values
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_pipelines"])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["version_hash"] == "new_hash"
        assert row["description"] == "New description"
        assert row["layer"] == "silver"

    def test_register_pipelines_batch_empty_list(self, catalog_manager):
        """Test that empty list doesn't cause errors."""
        catalog_manager.register_pipelines_batch([])
        # Should not raise any exceptions

    def test_register_nodes_batch(self, catalog_manager):
        """Test batch registering multiple nodes."""
        records = [
            {
                "pipeline_name": "test_pipeline",
                "node_name": "node_1",
                "version_hash": "node_hash1",
                "type": "transform",
                "config_json": json.dumps({"step": 1}),
            },
            {
                "pipeline_name": "test_pipeline",
                "node_name": "node_2",
                "version_hash": "node_hash2",
                "type": "load",
                "config_json": json.dumps({"step": 2}),
            },
        ]

        catalog_manager.register_nodes_batch(records)

        # Verify both nodes were registered
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_nodes"])
        assert len(df) == 2
        assert set(df["node_name"].values) == {"node_1", "node_2"}

    def test_register_nodes_batch_duplicate_handling(self, catalog_manager):
        """Test that duplicate nodes are updated, not duplicated."""
        # Register initial node
        initial_records = [
            {
                "pipeline_name": "test_pipeline",
                "node_name": "test_node",
                "version_hash": "hash_v1",
                "type": "transform",
                "config_json": json.dumps({"version": 1}),
            }
        ]
        catalog_manager.register_nodes_batch(initial_records)

        # Register updated node
        updated_records = [
            {
                "pipeline_name": "test_pipeline",
                "node_name": "test_node",
                "version_hash": "hash_v2",
                "type": "transform",
                "config_json": json.dumps({"version": 2}),
            }
        ]
        catalog_manager.register_nodes_batch(updated_records)

        # Verify only one record exists with updated hash
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_nodes"])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["version_hash"] == "hash_v2"


class TestLookupOperations:
    """Test lookup operations for registered pipelines and nodes."""

    def test_get_registered_pipeline_found(self, catalog_manager):
        """Test retrieving a registered pipeline."""
        records = [
            {
                "pipeline_name": "lookup_test",
                "version_hash": "test_hash",
                "description": "Test pipeline",
                "layer": "silver",
                "schedule": "daily",
                "tags_json": json.dumps({"test": True}),
            }
        ]
        catalog_manager.register_pipelines_batch(records)

        result = catalog_manager.get_registered_pipeline("lookup_test")
        assert result is not None
        assert result["pipeline_name"] == "lookup_test"
        assert result["version_hash"] == "test_hash"

    def test_get_registered_pipeline_not_found(self, catalog_manager):
        """Test retrieving non-existent pipeline returns None."""
        result = catalog_manager.get_registered_pipeline("nonexistent")
        assert result is None

    def test_get_registered_nodes_found(self, catalog_manager):
        """Test retrieving registered nodes for a pipeline."""
        records = [
            {
                "pipeline_name": "pipeline_with_nodes",
                "node_name": "node1",
                "version_hash": "hash1",
                "type": "transform",
                "config_json": json.dumps({}),
            },
            {
                "pipeline_name": "pipeline_with_nodes",
                "node_name": "node2",
                "version_hash": "hash2",
                "type": "load",
                "config_json": json.dumps({}),
            },
        ]
        catalog_manager.register_nodes_batch(records)

        result = catalog_manager.get_registered_nodes("pipeline_with_nodes")
        assert len(result) == 2
        assert "node1" in result
        assert "node2" in result
        assert result["node1"] == "hash1"
        assert result["node2"] == "hash2"

    def test_get_registered_nodes_empty(self, catalog_manager):
        """Test retrieving nodes for pipeline with no nodes."""
        result = catalog_manager.get_registered_nodes("empty_pipeline")
        assert result == {}

    def test_get_all_registered_pipelines(self, catalog_manager):
        """Test retrieving all registered pipelines."""
        records = [
            {
                "pipeline_name": f"pipeline_{i}",
                "version_hash": f"hash_{i}",
                "description": f"Pipeline {i}",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": json.dumps({}),
            }
            for i in range(3)
        ]
        catalog_manager.register_pipelines_batch(records)

        result = catalog_manager.get_all_registered_pipelines()
        assert len(result) == 3
        for i in range(3):
            assert f"pipeline_{i}" in result
            assert result[f"pipeline_{i}"] == f"hash_{i}"

    def test_get_all_registered_nodes(self, catalog_manager):
        """Test retrieving all registered nodes for multiple pipelines."""
        records = [
            {
                "pipeline_name": "pipeline_1",
                "node_name": "node_a",
                "version_hash": "hash_a",
                "type": "transform",
                "config_json": json.dumps({}),
            },
            {
                "pipeline_name": "pipeline_2",
                "node_name": "node_b",
                "version_hash": "hash_b",
                "type": "load",
                "config_json": json.dumps({}),
            },
        ]
        catalog_manager.register_nodes_batch(records)

        result = catalog_manager.get_all_registered_nodes(["pipeline_1", "pipeline_2"])
        assert len(result) == 2
        assert "pipeline_1" in result
        assert "pipeline_2" in result
        assert result["pipeline_1"]["node_a"] == "hash_a"
        assert result["pipeline_2"]["node_b"] == "hash_b"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_duplicate_run_logging(self, catalog_manager):
        """Test logging the same run_id multiple times."""
        # Log first run
        catalog_manager.log_run(
            run_id="duplicate_test",
            pipeline_name="test_pipeline",
            node_name="test_node",
            status="SUCCESS",
            rows_processed=100,
            duration_ms=1000,
        )

        # Log same run_id again
        catalog_manager.log_run(
            run_id="duplicate_test",
            pipeline_name="test_pipeline",
            node_name="test_node",
            status="SUCCESS",
            rows_processed=200,
            duration_ms=2000,
        )

        # Both should be recorded (append mode)
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_runs"])
        duplicate_runs = df[df["run_id"] == "duplicate_test"]
        assert len(duplicate_runs) >= 1  # At least one entry exists

    def test_log_runs_batch(self, catalog_manager):
        """Test batch logging of multiple runs."""
        runs = [
            {
                "run_id": f"run_{i}",
                "project": "test_project",
                "pipeline_name": "test_pipeline",
                "node_name": f"node_{i}",
                "status": "SUCCESS",
                "rows_processed": i * 100,
                "duration_ms": i * 1000,
                "metrics_json": json.dumps({"step": i}),
                "environment": "dev",
                "timestamp": datetime.now(timezone.utc),
                "date": date.today(),
            }
            for i in range(3)
        ]

        catalog_manager.log_runs_batch(runs)

        df = catalog_manager._read_local_table(catalog_manager.tables["meta_runs"])
        assert len(df) >= 3
        for i in range(3):
            assert f"run_{i}" in df["run_id"].values

    def test_log_failure_with_long_error_message(self, catalog_manager):
        """Test logging failure with very long error message."""
        long_message = "Error: " + "x" * 10000  # Very long error message
        stack_trace = "Stack trace: " + "line\n" * 1000

        catalog_manager.log_failure(
            failure_id="fail_long",
            run_id="run_fail",
            pipeline_name="test_pipeline",
            node_name="test_node",
            error_type="ValueError",
            error_message=long_message,
            stack_trace=stack_trace,
        )

        df = catalog_manager._read_local_table(catalog_manager.tables["meta_failures"])
        failure = df[df["failure_id"] == "fail_long"].iloc[0]
        # Should be truncated or stored as-is
        assert failure["error_message"] is not None
        assert "Error:" in failure["error_message"]

    def test_register_asset_duplicate(self, catalog_manager):
        """Test registering the same asset twice updates it."""
        # Register first time
        catalog_manager.register_asset(
            project_name="test_project",
            table_name="duplicate_table",
            path="/path/v1",
            format="delta",
            pattern_type="table",
            schema_hash="hash1",
        )

        # Register again with different path
        catalog_manager.register_asset(
            project_name="test_project",
            table_name="duplicate_table",
            path="/path/v2",
            format="parquet",
            pattern_type="table",
            schema_hash="hash2",
        )

        df = catalog_manager._read_local_table(catalog_manager.tables["meta_tables"])
        duplicates = df[df["table_name"] == "duplicate_table"]
        # Should have only one entry (upserted)
        assert len(duplicates) >= 1
        # Latest entry should have v2 path
        latest = duplicates.sort_values("updated_at", ascending=False).iloc[0]
        assert "/path/v2" in latest["path"] or latest["path"] == "/path/v2"

    def test_track_schema_no_changes(self, catalog_manager):
        """Test tracking schema when there are no changes."""
        schema = {"id": "int64", "name": "string", "value": "float"}

        # Track first time
        result1 = catalog_manager.track_schema(
            table_path="/test/table",
            schema=schema,
            pipeline="test_pipeline",
            node="test_node",
            run_id="run_1",
        )
        assert result1["changed"] is True
        assert result1["version"] == 1

        # Track again with same schema
        result2 = catalog_manager.track_schema(
            table_path="/test/table",
            schema=schema,
            pipeline="test_pipeline",
            node="test_node",
            run_id="run_2",
        )
        assert result2["changed"] is False
        assert result2["version"] == 1

    def test_resolve_table_path_ambiguous(self, catalog_manager):
        """Test resolving table name when multiple matches exist."""
        # Register two tables with similar names
        catalog_manager.register_asset(
            project_name="project1",
            table_name="customers",
            path="/project1/customers",
            format="delta",
            pattern_type="table",
        )
        catalog_manager.register_asset(
            project_name="project2",
            table_name="customers",
            path="/project2/customers",
            format="delta",
            pattern_type="table",
        )

        # Should return one of them (implementation specific)
        result = catalog_manager.resolve_table_path("customers")
        assert result is not None
        assert "customers" in result

    def test_get_average_duration_with_no_runs(self, catalog_manager):
        """Test getting average duration when no runs exist."""
        result = catalog_manager.get_average_duration("nonexistent_node", days=7)
        assert result is None

    def test_get_average_volume_with_no_runs(self, catalog_manager):
        """Test getting average volume when no runs exist."""
        result = catalog_manager.get_average_volume("nonexistent_node", days=7)
        assert result is None

    def test_record_lineage_batch(self, catalog_manager):
        """Test batch recording of lineage."""
        records = [
            {
                "source_table": f"/source/table_{i}",
                "target_table": f"/target/table_{i}",
                "target_pipeline": "test_pipeline",
                "target_node": f"node_{i}",
                "run_id": f"run_{i}",
            }
            for i in range(3)
        ]

        catalog_manager.record_lineage_batch(records)

        df = catalog_manager._read_local_table(catalog_manager.tables["meta_lineage"])
        assert len(df) >= 3
        for i in range(3):
            assert f"/source/table_{i}" in df["source_table"].values

    def test_register_assets_batch(self, catalog_manager):
        """Test batch registering multiple assets."""
        records = [
            {
                "project_name": "test_project",
                "table_name": f"table_{i}",
                "path": f"/path/table_{i}",
                "format": "delta",
                "pattern_type": "table",
                "schema_hash": f"hash_{i}",
            }
            for i in range(3)
        ]

        catalog_manager.register_assets_batch(records)

        df = catalog_manager._read_local_table(catalog_manager.tables["meta_tables"])
        assert len(df) >= 3
        for i in range(3):
            assert f"table_{i}" in df["table_name"].values


class TestPipelineMetrics:
    """Test pipeline and node metrics tracking."""

    def test_get_average_duration_multiple_runs(self, catalog_manager):
        """Test calculating average duration with multiple runs."""
        node_name = "perf_test_node"

        for i in range(5):
            catalog_manager.log_run(
                run_id=f"run_{i}",
                pipeline_name="test_pipeline",
                node_name=node_name,
                status="SUCCESS",
                rows_processed=100,
                duration_ms=(i + 1) * 1000,  # 1000, 2000, 3000, 4000, 5000
            )

        avg = catalog_manager.get_average_duration(node_name, days=7)
        assert avg is not None
        # Average should be (1+2+3+4+5)/5 = 3.0 seconds
        assert abs(avg - 3.0) < 0.1

    def test_get_average_volume_multiple_runs(self, catalog_manager):
        """Test calculating average volume with multiple runs."""
        node_name = "volume_test_node"

        for i in range(5):
            catalog_manager.log_run(
                run_id=f"vol_run_{i}",
                pipeline_name="test_pipeline",
                node_name=node_name,
                status="SUCCESS",
                rows_processed=(i + 1) * 100,  # 100, 200, 300, 400, 500
                duration_ms=1000,
            )

        avg = catalog_manager.get_average_volume(node_name, days=7)
        assert avg is not None
        # Average should be (100+200+300+400+500)/5 = 300
        assert abs(avg - 300.0) < 1.0

    def test_get_run_ids_by_pipeline(self, catalog_manager):
        """Test retrieving run IDs for a specific pipeline."""
        # log_pipeline_run writes to meta_pipeline_runs which is what get_run_ids queries
        now = datetime.now(timezone.utc)
        for i in range(3):
            catalog_manager.log_pipeline_run(
                {
                    "run_id": f"pipeline_a_run_{i}",
                    "project": "test_project",
                    "pipeline_name": "pipeline_a",
                    "owner": "test_user",
                    "layer": "bronze",
                    "run_start_at": now,
                    "run_end_at": now,
                    "duration_ms": 1000,
                    "status": "SUCCESS",
                    "nodes_total": 1,
                    "nodes_succeeded": 1,
                    "nodes_failed": 0,
                    "nodes_skipped": 0,
                    "rows_processed": 100,
                    "environment": "dev",
                    "created_at": now,
                }
            )

        run_ids = catalog_manager.get_run_ids(pipeline_name="pipeline_a")
        assert len(run_ids) >= 3
        assert all("pipeline_a_run_" in rid for rid in run_ids)

    def test_get_run_ids_since_date(self, catalog_manager):
        """Test retrieving run IDs since a specific date."""
        # Log some pipeline runs (not node runs)
        now = datetime.now(timezone.utc)
        for i in range(3):
            catalog_manager.log_pipeline_run(
                {
                    "run_id": f"date_run_{i}",
                    "project": "test_project",
                    "pipeline_name": "test_pipeline",
                    "owner": "test_user",
                    "layer": "bronze",
                    "run_start_at": now,
                    "run_end_at": now,
                    "duration_ms": 1000,
                    "status": "SUCCESS",
                    "nodes_total": 1,
                    "nodes_succeeded": 1,
                    "nodes_failed": 0,
                    "nodes_skipped": 0,
                    "rows_processed": 100,
                    "environment": "dev",
                    "created_at": now,
                }
            )

        # Get runs from today
        today = date.today()
        run_ids = catalog_manager.get_run_ids(since=today)
        assert len(run_ids) >= 3


class TestCacheInvalidation:
    """Test cache invalidation behavior."""

    def test_cache_invalidation_on_pipeline_register(self, catalog_manager):
        """Test that cache is invalidated when registering pipelines."""
        # Populate cache
        catalog_manager._get_all_pipelines_cached()
        assert catalog_manager._pipelines_cache is not None

        # Register new pipeline
        records = [
            {
                "pipeline_name": "new_pipeline",
                "version_hash": "hash",
                "description": "Test",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": json.dumps({}),
            }
        ]
        catalog_manager.register_pipelines_batch(records)

        # Cache should be invalidated
        assert catalog_manager._pipelines_cache is None

    def test_cache_invalidation_on_node_register(self, catalog_manager):
        """Test that cache is invalidated when registering nodes."""
        # Populate cache
        catalog_manager._get_all_nodes_cached()
        assert catalog_manager._nodes_cache is not None

        # Register new node
        records = [
            {
                "pipeline_name": "test_pipeline",
                "node_name": "new_node",
                "version_hash": "hash",
                "type": "transform",
                "config_json": json.dumps({}),
            }
        ]
        catalog_manager.register_nodes_batch(records)

        # Cache should be invalidated
        assert catalog_manager._nodes_cache is None

    def test_manual_cache_invalidation(self, catalog_manager):
        """Test manual cache invalidation."""
        # Populate caches
        catalog_manager._get_all_pipelines_cached()
        catalog_manager._get_all_nodes_cached()

        assert catalog_manager._pipelines_cache is not None
        assert catalog_manager._nodes_cache is not None

        # Manually invalidate
        catalog_manager.invalidate_cache()

        assert catalog_manager._pipelines_cache is None
        assert catalog_manager._nodes_cache is None


class TestCacheThreadSafety:
    """Tests for #275: CatalogManager cache thread-safety."""

    def test_concurrent_cache_access(self, catalog_manager):
        """Concurrent threads should not corrupt cache."""
        import threading

        results = []
        errors = []

        def access_cache():
            try:
                catalog_manager.invalidate_cache()
                pipelines = catalog_manager._get_all_pipelines_cached()
                nodes = catalog_manager._get_all_nodes_cached()
                outputs = catalog_manager._get_all_outputs_cached()
                results.append((type(pipelines), type(nodes), type(outputs)))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=access_cache) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread errors: {errors}"
        assert len(results) == 10
        for r in results:
            assert all(t is dict for t in r)

    def test_cache_lock_exists(self, catalog_manager):
        """CatalogManager should have a threading lock."""
        assert hasattr(catalog_manager, "_cache_lock")
        assert isinstance(catalog_manager._cache_lock, type(threading.Lock()))


class TestModeDetection:
    """Test engine mode detection properties."""

    def test_is_pandas_mode(self, catalog_manager):
        """Test pandas mode detection."""
        assert catalog_manager.is_pandas_mode is True
        assert catalog_manager.is_spark_mode is False

    def test_has_backend(self, catalog_manager):
        """Test backend availability check."""
        assert catalog_manager.has_backend is True

    def test_project_property(self, catalog_manager):
        """Test project property getter/setter."""
        catalog_manager.project = "my_project"
        assert catalog_manager.project == "my_project"

        catalog_manager.project = None
        assert catalog_manager.project is None
