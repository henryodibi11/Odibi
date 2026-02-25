"""Unit tests for CatalogManager retry with backoff and caching layer (issue #289)."""

import json
import threading
from unittest.mock import patch

import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


@pytest.fixture
def catalog_manager(tmp_path):
    """Create a bootstrapped CatalogManager with PandasEngine."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    return cm


def _register_pipeline(cm, name="test_pipeline", version_hash="abc123"):
    """Helper to register a pipeline via batch method."""
    cm.register_pipelines_batch(
        [
            {
                "pipeline_name": name,
                "version_hash": version_hash,
                "description": "Test",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": json.dumps({}),
            }
        ]
    )


def _register_node(cm, pipeline_name="test_pipeline", node_name="node_a", version_hash="h1"):
    """Helper to register a node via batch method."""
    cm.register_nodes_batch(
        [
            {
                "pipeline_name": pipeline_name,
                "node_name": node_name,
                "version_hash": version_hash,
                "type": "transform",
                "config_json": json.dumps({}),
            }
        ]
    )


# ---------------------------------------------------------------------------
# _retry_with_backoff
# ---------------------------------------------------------------------------
class TestRetryWithBackoff:
    """Tests for CatalogManager._retry_with_backoff."""

    @patch("time.sleep")
    def test_success_on_first_attempt(self, mock_sleep, catalog_manager):
        """Successful call returns result immediately, no sleep."""
        result = catalog_manager._retry_with_backoff(lambda: 42)
        assert result == 42
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_concurrent_append_error_retried(self, mock_sleep, catalog_manager):
        """ConcurrentAppendException triggers retry and eventually succeeds."""
        calls = {"count": 0}

        def flaky():
            calls["count"] += 1
            if calls["count"] <= 2:
                raise Exception("ConcurrentAppendException: conflict detected")
            return "ok"

        result = catalog_manager._retry_with_backoff(flaky)
        assert result == "ok"
        assert calls["count"] == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_non_retryable_error_raised_immediately(self, mock_sleep, catalog_manager):
        """Non-concurrent errors are raised without retry."""
        with pytest.raises(ValueError, match="bad data"):
            catalog_manager._retry_with_backoff(
                lambda: (_ for _ in ()).throw(ValueError("bad data"))
            )
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_all_retries_exhausted_raises(self, mock_sleep, catalog_manager):
        """After max_retries, the original exception is raised."""

        def always_fail():
            raise Exception("concurrent write error")

        with pytest.raises(Exception, match="concurrent write error"):
            catalog_manager._retry_with_backoff(always_fail, max_retries=3)
        assert mock_sleep.call_count == 3

    @patch("time.sleep")
    def test_exponential_backoff_call_count(self, mock_sleep, catalog_manager):
        """Sleep is called the correct number of times matching retries."""
        attempt = {"n": 0}

        def fail_twice():
            attempt["n"] += 1
            if attempt["n"] <= 2:
                raise Exception("conflict")
            return "done"

        catalog_manager._retry_with_backoff(fail_twice, max_retries=5, base_delay=1.0)
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_conflict_keyword_triggers_retry(self, mock_sleep, catalog_manager):
        """'conflict' keyword in error message triggers retry."""
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("merge conflict on table")
            return "resolved"

        result = catalog_manager._retry_with_backoff(flaky)
        assert result == "resolved"
        assert mock_sleep.call_count == 1

    @patch("time.sleep")
    def test_concurrent_keyword_triggers_retry(self, mock_sleep, catalog_manager):
        """'concurrent' keyword in error message triggers retry."""
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("concurrent modification detected")
            return "success"

        result = catalog_manager._retry_with_backoff(flaky)
        assert result == "success"
        assert mock_sleep.call_count == 1

    @patch("time.sleep")
    def test_max_retries_respected_custom(self, mock_sleep, catalog_manager):
        """Setting max_retries=2 allows only 2 retries (3 total attempts)."""
        calls = {"n": 0}

        def always_fail():
            calls["n"] += 1
            raise Exception("concurrent error")

        with pytest.raises(Exception, match="concurrent error"):
            catalog_manager._retry_with_backoff(always_fail, max_retries=2)
        # 3 attempts total: initial + 2 retries
        assert calls["n"] == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_custom_base_delay(self, mock_sleep, catalog_manager):
        """Custom base_delay is used in sleep calculations."""
        calls = {"n": 0}

        def fail_once():
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("concurrent")
            return "ok"

        with patch("random.uniform", return_value=0.0):
            catalog_manager._retry_with_backoff(fail_once, base_delay=2.0)
        # First retry: base_delay * 2^0 + jitter = 2.0 * 1 + 0.0 = 2.0
        mock_sleep.assert_called_once_with(2.0)


# ---------------------------------------------------------------------------
# _get_all_pipelines_cached
# ---------------------------------------------------------------------------
class TestGetAllPipelinesCached:
    """Tests for CatalogManager._get_all_pipelines_cached."""

    def test_empty_dict_when_no_backend(self, tmp_path):
        """Returns empty dict when both engine and engine are None."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
        result = cm._get_all_pipelines_cached()
        assert result == {}

    def test_cached_value_on_second_call(self, catalog_manager):
        """Second call returns cached value without re-reading."""
        _register_pipeline(catalog_manager, "pipe1", "v1")
        catalog_manager.invalidate_cache()

        with patch.object(
            catalog_manager, "_read_local_table", wraps=catalog_manager._read_local_table
        ) as spy:
            result1 = catalog_manager._get_all_pipelines_cached()
            result2 = catalog_manager._get_all_pipelines_cached()
            assert result1 is result2
            assert spy.call_count == 1

    def test_returns_correct_data_after_registration(self, catalog_manager):
        """After registering a pipeline, cached data includes it."""
        _register_pipeline(catalog_manager, "my_pipeline", "hash_1")
        # Registration invalidates cache, so next call re-reads
        result = catalog_manager._get_all_pipelines_cached()
        assert "my_pipeline" in result
        assert result["my_pipeline"]["version_hash"] == "hash_1"

    def test_cache_invalidation_causes_reread(self, catalog_manager):
        """After invalidate_cache(), data is re-read from storage."""
        _register_pipeline(catalog_manager, "p1", "v1")
        catalog_manager._get_all_pipelines_cached()

        with patch.object(
            catalog_manager, "_read_local_table", wraps=catalog_manager._read_local_table
        ) as spy:
            catalog_manager.invalidate_cache()
            catalog_manager._get_all_pipelines_cached()
            assert spy.call_count == 1

    def test_thread_safe_concurrent_access(self, catalog_manager):
        """Concurrent calls do not corrupt the cache."""
        _register_pipeline(catalog_manager, "tp", "v1")
        catalog_manager.invalidate_cache()
        results = []
        errors = []

        def reader():
            try:
                r = catalog_manager._get_all_pipelines_cached()
                results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 10
        # All results should reference the same cached dict
        for r in results:
            assert "tp" in r


# ---------------------------------------------------------------------------
# _get_all_nodes_cached
# ---------------------------------------------------------------------------
class TestGetAllNodesCached:
    """Tests for CatalogManager._get_all_nodes_cached."""

    def test_empty_dict_when_no_backend(self, tmp_path):
        """Returns empty dict when both engine and engine are None."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
        result = cm._get_all_nodes_cached()
        assert result == {}

    def test_cached_value_on_second_call(self, catalog_manager):
        """Second call returns cached value without re-reading."""
        _register_node(catalog_manager, "pipe1", "n1", "h1")
        catalog_manager.invalidate_cache()

        with patch.object(
            catalog_manager, "_read_local_table", wraps=catalog_manager._read_local_table
        ) as spy:
            result1 = catalog_manager._get_all_nodes_cached()
            result2 = catalog_manager._get_all_nodes_cached()
            assert result1 is result2
            assert spy.call_count == 1

    def test_correct_grouped_structure(self, catalog_manager):
        """Returns {pipeline_name: {node_name: version_hash}} structure."""
        _register_node(catalog_manager, "pipe_a", "node_x", "hx")
        catalog_manager.invalidate_cache()

        result = catalog_manager._get_all_nodes_cached()
        assert "pipe_a" in result
        assert result["pipe_a"]["node_x"] == "hx"

    def test_cache_invalidation_causes_reread(self, catalog_manager):
        """After invalidate_cache(), nodes are re-read from storage."""
        _register_node(catalog_manager, "p1", "n1", "h1")
        catalog_manager._get_all_nodes_cached()

        with patch.object(
            catalog_manager, "_read_local_table", wraps=catalog_manager._read_local_table
        ) as spy:
            catalog_manager.invalidate_cache()
            catalog_manager._get_all_nodes_cached()
            assert spy.call_count == 1

    def test_multiple_nodes_same_pipeline(self, catalog_manager):
        """Multiple nodes under the same pipeline are grouped correctly."""
        _register_node(catalog_manager, "shared_pipe", "node_a", "ha")
        _register_node(catalog_manager, "shared_pipe", "node_b", "hb")
        catalog_manager.invalidate_cache()

        result = catalog_manager._get_all_nodes_cached()
        assert "shared_pipe" in result
        assert result["shared_pipe"]["node_a"] == "ha"
        assert result["shared_pipe"]["node_b"] == "hb"

    def test_thread_safe_concurrent_access(self, catalog_manager):
        """Concurrent calls do not corrupt the node cache."""
        _register_node(catalog_manager, "tp", "n1", "h1")
        catalog_manager.invalidate_cache()
        results = []
        errors = []

        def reader():
            try:
                r = catalog_manager._get_all_nodes_cached()
                results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 10
        for r in results:
            assert "tp" in r


# ---------------------------------------------------------------------------
# invalidate_cache
# ---------------------------------------------------------------------------
class TestInvalidateCache:
    """Tests for CatalogManager.invalidate_cache."""

    def test_sets_all_caches_to_none(self, catalog_manager):
        """All three caches are set to None after invalidation."""
        # Populate caches
        catalog_manager._pipelines_cache = {"a": {}}
        catalog_manager._nodes_cache = {"b": {}}
        catalog_manager._outputs_cache = {"c": {}}

        catalog_manager.invalidate_cache()

        assert catalog_manager._pipelines_cache is None
        assert catalog_manager._nodes_cache is None
        assert catalog_manager._outputs_cache is None

    def test_after_invalidation_next_access_rereads(self, catalog_manager):
        """After invalidation, the next cached call re-reads from storage."""
        _register_pipeline(catalog_manager, "p1", "v1")
        # Populate the cache
        catalog_manager._get_all_pipelines_cached()
        assert catalog_manager._pipelines_cache is not None

        catalog_manager.invalidate_cache()
        assert catalog_manager._pipelines_cache is None

        result = catalog_manager._get_all_pipelines_cached()
        assert "p1" in result
        assert catalog_manager._pipelines_cache is not None

    def test_thread_safe_multiple_invalidations(self, catalog_manager):
        """Multiple concurrent invalidations do not crash."""
        catalog_manager._pipelines_cache = {"x": {}}
        catalog_manager._nodes_cache = {"y": {}}
        catalog_manager._outputs_cache = {"z": {}}
        errors = []

        def invalidator():
            try:
                for _ in range(20):
                    catalog_manager.invalidate_cache()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=invalidator) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert catalog_manager._pipelines_cache is None
        assert catalog_manager._nodes_cache is None
        assert catalog_manager._outputs_cache is None


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------
class TestCatalogProperties:
    """Tests for CatalogManager property accessors."""

    def test_is_pandas_mode_true(self, catalog_manager):
        """is_pandas_mode returns True when engine.name == 'pandas'."""
        assert catalog_manager.is_pandas_mode is True

    def test_has_backend_true_with_engine(self, catalog_manager):
        """has_backend returns True when engine is set."""
        assert catalog_manager.has_backend is True

    def test_has_backend_false_when_neither(self, tmp_path):
        """has_backend returns False when both engine and engine are None."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
        assert cm.has_backend is False
