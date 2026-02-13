"""Unit tests for state/__init__.py backends - targeting uncovered lines."""

import os
import tempfile
from unittest.mock import Mock, patch

import pytest

from odibi.state import _retry_delta_operation


class TestRetryDeltaOperation:
    def test_success_first_try(self):
        result = _retry_delta_operation(lambda: 42)
        assert result == 42

    def test_retry_on_concurrent_error(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("ConcurrentAppendException: conflict")
            return "ok"

        with patch("odibi.state.time.sleep"):
            result = _retry_delta_operation(flaky, max_retries=5)
        assert result == "ok"
        assert call_count == 3

    def test_no_retry_on_non_concurrent_error(self):
        def fails():
            raise ValueError("not concurrent")

        with pytest.raises(ValueError, match="not concurrent"):
            _retry_delta_operation(fails)

    def test_max_retries_exhausted(self):
        def always_conflict():
            raise Exception("DELTA_CONCURRENT conflict error")

        with patch("odibi.state.time.sleep"):
            with pytest.raises(Exception, match="DELTA_CONCURRENT"):
                _retry_delta_operation(always_conflict, max_retries=2)

    def test_retry_various_conflict_messages(self):
        """All conflict message patterns should trigger retry."""
        patterns = [
            "ConcurrentAppendException",
            "ConcurrentDeleteReadException",
            "ConcurrentDeleteDeleteException",
            "DELTA_CONCURRENT",
            "concurrent write",
            "conflict detected",
        ]
        for pattern in patterns:
            call_count = 0

            def make_flaky(pat):
                def flaky():
                    nonlocal call_count
                    call_count += 1
                    if call_count < 2:
                        raise Exception(pat)
                    return "ok"

                return flaky

            call_count = 0
            with patch("odibi.state.time.sleep"):
                result = _retry_delta_operation(make_flaky(pattern), max_retries=3)
            assert result == "ok", f"Failed for pattern: {pattern}"


class TestLocalFileStateBackend:
    """Test the JSON file-based state backend."""

    def _make_backend(self, path):
        from odibi.state import LocalJSONStateBackend

        return LocalJSONStateBackend(path)

    def test_load_empty_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "state.json")
            backend = self._make_backend(path)
            state = backend.load_state()
            assert isinstance(state, dict)

    def test_set_hwm_persists_across_loads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "state.json")
            backend = self._make_backend(path)
            backend.set_hwm("etl_hwm", "2024-01-01")

            # Reload from same file
            backend2 = self._make_backend(path)
            loaded = backend2.load_state()
            assert "etl_hwm" in str(loaded) or backend2.get_hwm("etl_hwm") == "2024-01-01"

    def test_set_and_get_hwm(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "state.json")
            backend = self._make_backend(path)
            backend.set_hwm("orders_hwm", "2024-06-01")
            hwm = backend.get_hwm("orders_hwm")
            assert hwm == "2024-06-01"

    def test_get_hwm_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "state.json")
            backend = self._make_backend(path)
            hwm = backend.get_hwm("nonexistent")
            assert hwm is None


class TestStateManager:
    """Test the StateManager wrapper."""

    def _make_manager(self, tmpdir):
        from odibi.state import LocalJSONStateBackend, StateManager

        path = os.path.join(tmpdir, "state.json")
        backend = LocalJSONStateBackend(path)
        return StateManager(backend=backend)

    def test_get_set_hwm(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._make_manager(tmpdir)
            mgr.set_hwm("orders_hwm", "2024-01-15")
            hwm = mgr.get_hwm("orders_hwm")
            assert hwm == "2024-01-15"

    def test_get_hwm_missing_returns_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._make_manager(tmpdir)
            hwm = mgr.get_hwm("nonexistent")
            assert hwm is None

    def test_save_pipeline_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._make_manager(tmpdir)
            mgr.save_pipeline_run(
                "etl",
                {
                    "status": "success",
                    "duration": 10.5,
                    "nodes_completed": 3,
                },
            )
            mgr.get_last_run_info("etl", "node1")
            # May return None if run format doesn't include node-level info
            # but the call should not raise


class TestCreateStateBackend:
    """Test the factory function."""

    def test_create_local_backend(self):
        from odibi.state import create_state_backend

        with tempfile.TemporaryDirectory() as tmpdir:
            config = Mock()
            config.type = "local"
            config.path = os.path.join(tmpdir, "state.json")
            config.connection = None
            backend = create_state_backend(config, project_root=tmpdir)
            assert backend is not None

    def test_create_with_none_config_returns_local(self):
        from odibi.state import LocalJSONStateBackend, create_state_backend

        config = Mock()
        config.system = None
        config.state = None
        backend = create_state_backend(config)
        assert isinstance(backend, LocalJSONStateBackend)
