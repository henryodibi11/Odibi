"""Tests for CatalogManager deprecated skip-if-unchanged methods and utility helpers.

Covers:
- register_pipeline() with skip_if_unchanged=True (deprecated path)
- register_node() with skip_if_unchanged=True and existing_hash (deprecated path)
- _get_storage_options() storage credential resolution
- _table_exists() cloud path branch (mocked)

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

import warnings
from unittest.mock import MagicMock, patch

import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_manager(tmp_path):
    """CatalogManager in Pandas mode, bootstrapped, with project set."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    cm.project = "test_project"
    return cm


@pytest.fixture
def bare_catalog(tmp_path):
    """CatalogManager with no engine and no spark (no backend)."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


def _make_pipeline_config(name="test_pipe", description="desc", layer="bronze", tags=None):
    """Build a MagicMock that satisfies _prepare_pipeline_record expectations."""
    node = MagicMock()
    node.tags = tags or []
    cfg = MagicMock()
    cfg.pipeline = name
    cfg.description = description
    cfg.layer = layer
    cfg.nodes = [node]
    return cfg


def _make_node_config(name="test_node", read=None, write=None):
    """Build a MagicMock that satisfies _prepare_node_record expectations."""
    cfg = MagicMock()
    cfg.name = name
    cfg.read = read
    cfg.write = write
    cfg.model_dump.return_value = {"name": name}
    return cfg


# ---------------------------------------------------------------------------
# register_pipeline – deprecated skip-if-unchanged
# ---------------------------------------------------------------------------


class TestRegisterPipelineDeprecated:
    """Tests for the deprecated register_pipeline() method with skip logic."""

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_aaa")
    def test_register_pipeline_emits_deprecation_warning(self, _mock_hash, catalog_manager):
        """Calling register_pipeline should emit a DeprecationWarning."""
        cfg = _make_pipeline_config()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            catalog_manager.register_pipeline(cfg)
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1
            assert "register_pipelines_batch" in str(dep_warnings[0].message)

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_same")
    def test_skip_when_unchanged(self, _mock_hash, catalog_manager):
        """Second call with identical config should return False (skipped)."""
        cfg = _make_pipeline_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result1 = catalog_manager.register_pipeline(cfg, skip_if_unchanged=True)
            assert result1 is True
            result2 = catalog_manager.register_pipeline(cfg, skip_if_unchanged=True)
            assert result2 is False

    def test_update_when_changed(self, catalog_manager):
        """Second call with a different hash should return True (updated)."""
        cfg = _make_pipeline_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            with patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_v1"):
                result1 = catalog_manager.register_pipeline(cfg, skip_if_unchanged=True)
                assert result1 is True

            with patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_v2"):
                result2 = catalog_manager.register_pipeline(cfg, skip_if_unchanged=True)
                assert result2 is True

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_x")
    def test_register_without_skip_flag_always_writes(self, _mock_hash, catalog_manager):
        """Without skip_if_unchanged, every call should return True."""
        cfg = _make_pipeline_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assert catalog_manager.register_pipeline(cfg) is True
            assert catalog_manager.register_pipeline(cfg) is True

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_bare")
    def test_no_backend_returns_false(self, _mock_hash, bare_catalog):
        """With no engine and no spark, register_pipeline returns False."""
        cfg = _make_pipeline_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assert bare_catalog.register_pipeline(cfg) is False


# ---------------------------------------------------------------------------
# register_node – deprecated skip-if-unchanged
# ---------------------------------------------------------------------------


class TestRegisterNodeDeprecated:
    """Tests for the deprecated register_node() method with skip logic."""

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_aaa")
    def test_register_node_emits_deprecation_warning(self, _mock_hash, catalog_manager):
        """Calling register_node should emit a DeprecationWarning."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            catalog_manager.register_node("pipe_a", cfg)
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1
            assert "register_nodes_batch" in str(dep_warnings[0].message)

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_same")
    def test_skip_node_when_unchanged(self, _mock_hash, catalog_manager):
        """Second call with identical hash should return False (skipped)."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result1 = catalog_manager.register_node("pipe_a", cfg, skip_if_unchanged=True)
            assert result1 is True
            result2 = catalog_manager.register_node("pipe_a", cfg, skip_if_unchanged=True)
            assert result2 is False

    def test_update_node_when_changed(self, catalog_manager):
        """Second call with a different hash should return True (updated)."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            with patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_v1"):
                assert catalog_manager.register_node("pipe_a", cfg, skip_if_unchanged=True) is True

            with patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_v2"):
                assert catalog_manager.register_node("pipe_a", cfg, skip_if_unchanged=True) is True

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_pre")
    def test_skip_with_existing_hash_match(self, _mock_hash, catalog_manager):
        """When existing_hash is supplied and matches, skip without reading the catalog."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = catalog_manager.register_node(
                "pipe_a", cfg, skip_if_unchanged=True, existing_hash="nhash_pre"
            )
            assert result is False

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_new")
    def test_update_with_existing_hash_mismatch(self, _mock_hash, catalog_manager):
        """When existing_hash differs from computed hash, write should occur."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = catalog_manager.register_node(
                "pipe_a", cfg, skip_if_unchanged=True, existing_hash="nhash_old"
            )
            assert result is True

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash_bare")
    def test_no_backend_returns_false(self, _mock_hash, bare_catalog):
        """With no engine and no spark, register_node returns False."""
        cfg = _make_node_config()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assert bare_catalog.register_node("pipe_a", cfg) is False


# ---------------------------------------------------------------------------
# _get_storage_options
# ---------------------------------------------------------------------------


class TestGetStorageOptions:
    """Tests for the _get_storage_options helper."""

    def test_no_connection_returns_empty(self, catalog_manager):
        """With no connection set, returns empty dict."""
        catalog_manager.connection = None
        assert catalog_manager._get_storage_options() == {}

    def test_connection_with_storage_options(self, catalog_manager):
        """Connection that has pandas_storage_options returns its result."""
        conn = MagicMock()
        conn.pandas_storage_options.return_value = {"account_name": "myaccount", "account_key": "k"}
        catalog_manager.connection = conn
        opts = catalog_manager._get_storage_options()
        assert opts == {"account_name": "myaccount", "account_key": "k"}
        conn.pandas_storage_options.assert_called_once()

    def test_connection_without_attribute(self, catalog_manager):
        """Connection that lacks pandas_storage_options returns empty dict."""
        conn = object()  # plain object has no pandas_storage_options
        catalog_manager.connection = conn
        assert catalog_manager._get_storage_options() == {}


# ---------------------------------------------------------------------------
# _table_exists – cloud paths (mocked) and local paths
# ---------------------------------------------------------------------------


class TestTableExistsCloudPaths:
    """Tests for _table_exists with cloud (abfss://, s3://) and local paths."""

    def test_abfss_path_exists(self, catalog_manager):
        """abfss:// path should try loading via external table lib and return True on success."""
        with patch("odibi.catalog.CatalogManager._get_storage_options", return_value={}):
            with patch.dict("sys.modules", {"deltalake": MagicMock()}):
                import sys

                mock_dt_class = sys.modules["deltalake"].DeltaTable
                mock_dt_class.return_value = MagicMock()
                result = catalog_manager._table_exists("abfss://container@storage/path")
                assert result is True

    def test_abfss_path_not_exists(self, catalog_manager):
        """abfss:// path that raises an exception should return False."""
        with patch("odibi.catalog.CatalogManager._get_storage_options", return_value={}):
            with patch.dict("sys.modules", {"deltalake": MagicMock()}):
                import sys

                sys.modules["deltalake"].DeltaTable.side_effect = Exception("Not found")
                result = catalog_manager._table_exists("abfss://container@storage/missing")
                assert result is False

    def test_s3_path_exists(self, catalog_manager):
        """s3:// path should use the cloud branch and return True on success."""
        with patch("odibi.catalog.CatalogManager._get_storage_options", return_value={}):
            with patch.dict("sys.modules", {"deltalake": MagicMock()}):
                import sys

                sys.modules["deltalake"].DeltaTable.return_value = MagicMock()
                result = catalog_manager._table_exists("s3://bucket/prefix")
                assert result is True

    def test_local_path_nonexistent(self, catalog_manager, tmp_path):
        """Local path that doesn't exist should return False."""
        result = catalog_manager._table_exists(str(tmp_path / "no_such_table"))
        assert result is False

    def test_no_backend_returns_false(self, bare_catalog):
        """With no engine and no spark, _table_exists returns False."""
        assert bare_catalog._table_exists("/any/path") is False
