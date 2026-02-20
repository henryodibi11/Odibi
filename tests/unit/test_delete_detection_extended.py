"""
Extended tests for delete detection transformer internals.

Covers helper functions, edge cases, snapshot_diff pandas path,
_apply_deletes, _apply_hard_delete, _ensure_delete_column,
_build_source_keys_query, _get_row_count, _get_target_path,
_get_connection, _get_jdbc_url, _get_jdbc_properties, and
_get_sqlalchemy_engine.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    DeleteDetectionConfig,
    DeleteDetectionMode,
)
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.delete_detection import (
    DeleteThresholdExceeded,
    _apply_deletes,
    _apply_hard_delete,
    _build_source_keys_query,
    _ensure_delete_column,
    _get_connection,
    _get_jdbc_properties,
    _get_jdbc_url,
    _get_row_count,
    _get_sqlalchemy_engine,
    _get_target_path,
    detect_deletes,
)


# ============================================================
# Helpers
# ============================================================


def _make_ctx(df, engine=None):
    """Create a Pandas EngineContext for testing."""
    ctx = EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )
    return ctx


def _cfg(**overrides):
    """Build a DeleteDetectionConfig with sensible defaults for snapshot_diff."""
    defaults = {
        "mode": DeleteDetectionMode.SNAPSHOT_DIFF,
        "keys": ["id"],
    }
    defaults.update(overrides)
    return DeleteDetectionConfig(**defaults)


# ============================================================
# detect_deletes: mode=NONE and unknown mode
# ============================================================


class TestDetectDeletesDispatch:
    def test_mode_none_returns_context_unchanged(self):
        df = pd.DataFrame({"id": [1, 2]})
        ctx = _make_ctx(df)
        result = detect_deletes(ctx, mode="none")
        assert result is ctx
        pd.testing.assert_frame_equal(result.df, df)

    def test_mode_none_no_soft_delete_column(self):
        df = pd.DataFrame({"id": [1]})
        ctx = _make_ctx(df)
        result = detect_deletes(ctx, mode="none")
        assert "_is_deleted" not in result.df.columns

    def test_unknown_mode_raises_value_error(self):
        """Construct config with NONE, then monkey-patch mode to trigger fallthrough."""
        df = pd.DataFrame({"id": [1]})
        ctx = _make_ctx(df)
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE)
        # Force an unsupported mode value
        config.mode = "bogus"
        with pytest.raises(ValueError, match="Unknown delete detection mode"):
            detect_deletes(ctx, config=config)


# ============================================================
# _snapshot_diff_pandas
# ============================================================


class TestSnapshotDiffPandas:
    """Tests that exercise _snapshot_diff_pandas via detect_deletes."""

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_first_run_skip(self, mock_target_path, mock_dt_cls):
        """Version 0 with on_first_run=SKIP skips detection, adds False column."""
        mock_target_path.return_value = "/data/silver/t"
        mock_dt_instance = MagicMock()
        mock_dt_instance.version.return_value = 0
        mock_dt_cls.return_value = mock_dt_instance

        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        ctx = _make_ctx(df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
            on_first_run="skip",
        )

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_first_run_error(self, mock_target_path, mock_dt_cls):
        """Version 0 with on_first_run=ERROR raises ValueError."""
        mock_target_path.return_value = "/data/silver/t"
        mock_dt_instance = MagicMock()
        mock_dt_instance.version.return_value = 0
        mock_dt_cls.return_value = mock_dt_instance

        df = pd.DataFrame({"id": [1, 2]})
        ctx = _make_ctx(df)

        with pytest.raises(ValueError, match="No previous version"):
            detect_deletes(
                ctx,
                mode="snapshot_diff",
                keys=["id"],
                on_first_run="error",
            )

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_normal_run_detects_deletes(self, mock_target_path, mock_dt_cls):
        """Normal run: key in previous but not current is flagged deleted."""
        mock_target_path.return_value = "/data/silver/t"

        current_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        prev_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        # First call: current version (version=1)
        mock_dt_current = MagicMock()
        mock_dt_current.version.return_value = 1
        mock_dt_current.history.return_value = [{"version": 1}, {"version": 0}]
        # Second call: previous version (to_pandas)
        mock_dt_prev = MagicMock()
        mock_dt_prev.to_pandas.return_value = prev_df

        mock_dt_cls.side_effect = [mock_dt_current, mock_dt_prev]

        ctx = _make_ctx(current_df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
        )

        assert "_is_deleted" in result.df.columns
        deleted_rows = result.df[result.df["_is_deleted"]]
        assert list(deleted_rows["id"]) == [3]
        not_deleted = result.df[~result.df["_is_deleted"]]
        assert set(not_deleted["id"]) == {1, 2}

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_missing_keys_in_current_skips(self, mock_target_path, mock_dt_cls):
        """Missing key column in current df skips detection."""
        mock_target_path.return_value = "/data/silver/t"
        mock_dt_instance = MagicMock()
        mock_dt_instance.version.return_value = 1
        mock_dt_instance.history.return_value = [{"version": 1}, {"version": 0}]
        mock_dt_cls.return_value = mock_dt_instance

        df = pd.DataFrame({"other_col": [1, 2]})
        ctx = _make_ctx(df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
        )

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_missing_keys_in_previous_skips(self, mock_target_path, mock_dt_cls):
        """Missing key column in previous version skips detection."""
        mock_target_path.return_value = "/data/silver/t"

        current_df = pd.DataFrame({"id": [1, 2]})
        prev_df = pd.DataFrame({"other_col": [1, 2, 3]})

        mock_dt_current = MagicMock()
        mock_dt_current.version.return_value = 1
        mock_dt_current.history.return_value = [{"version": 1}, {"version": 0}]
        mock_dt_prev = MagicMock()
        mock_dt_prev.to_pandas.return_value = prev_df

        mock_dt_cls.side_effect = [mock_dt_current, mock_dt_prev]

        ctx = _make_ctx(current_df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
        )

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    def test_no_target_path_skips(self):
        """When target path cannot be determined, skip detection."""
        df = pd.DataFrame({"id": [1, 2]})
        ctx = _make_ctx(df)

        with patch(
            "odibi.transformers.delete_detection._get_target_path",
            return_value=None,
        ):
            result = detect_deletes(
                ctx,
                mode="snapshot_diff",
                keys=["id"],
            )

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_target_path")
    def test_table_open_error_skips(self, mock_target_path, mock_dt_cls):
        """If DeltaTable() raises, treated as non-Delta table and skipped."""
        mock_target_path.return_value = "/data/silver/t"
        mock_dt_cls.side_effect = Exception("Not a delta table")

        df = pd.DataFrame({"id": [1, 2]})
        ctx = _make_ctx(df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
        )

        assert "_is_deleted" in result.df.columns

    @patch("deltalake.DeltaTable", create=True)
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_explicit_connection_path(self, mock_get_conn, mock_dt_cls):
        """Explicit connection+path resolves table_path via conn.get_path."""
        conn = MagicMock()
        conn.get_path.return_value = "/resolved/path"
        mock_get_conn.return_value = conn

        current_df = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
        prev_df = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})

        mock_dt_current = MagicMock()
        mock_dt_current.version.return_value = 1
        mock_dt_current.history.return_value = [{"version": 1}, {"version": 0}]
        mock_dt_prev = MagicMock()
        mock_dt_prev.to_pandas.return_value = prev_df
        mock_dt_cls.side_effect = [mock_dt_current, mock_dt_prev]

        ctx = _make_ctx(current_df)

        result = detect_deletes(
            ctx,
            mode="snapshot_diff",
            keys=["id"],
            connection="my_conn",
            path="silver/table",
        )

        mock_get_conn.assert_called_once()
        conn.get_path.assert_called_once_with("silver/table")
        assert result.df is not None


# ============================================================
# _apply_deletes — Pandas path
# ============================================================


class TestApplyDeletesPandas:
    def test_snapshot_diff_mode_unions_deleted_rows(self):
        """With prev_df, deleted rows are unioned into result."""
        source_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        prev_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        deleted_keys = pd.DataFrame({"id": [3]})

        config = _cfg(soft_delete_col="_is_deleted")
        ctx = _make_ctx(source_df)

        result = _apply_deletes(ctx, deleted_keys, config, prev_df=prev_df)

        assert "_is_deleted" in result.df.columns
        assert len(result.df) == 3
        deleted = result.df[result.df["_is_deleted"]]
        assert list(deleted["id"]) == [3]
        not_deleted = result.df[~result.df["_is_deleted"]]
        assert set(not_deleted["id"]) == {1, 2}

    def test_sql_compare_mode_flags_existing_rows(self):
        """Without prev_df (sql_compare), rows are flagged in place."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        deleted_keys = pd.DataFrame({"id": [3]})

        config = _cfg(
            mode=DeleteDetectionMode.SQL_COMPARE,
            soft_delete_col="_is_deleted",
            source_connection="conn",
            source_table="tbl",
        )
        ctx = _make_ctx(silver_df)

        result = _apply_deletes(ctx, deleted_keys, config, prev_df=None)

        assert "_is_deleted" in result.df.columns
        assert len(result.df) == 3
        flagged = result.df[result.df["_is_deleted"]]
        assert list(flagged["id"]) == [3]

    def test_no_deletes_returns_ensure_column(self):
        """When zero deleted keys, _ensure_delete_column is used."""
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": pd.Series([], dtype="int64")})

        config = _cfg(soft_delete_col="_is_deleted")
        ctx = _make_ctx(df)

        result = _apply_deletes(ctx, deleted_keys, config)

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    def test_threshold_error_raises(self):
        """Threshold breach with ERROR action raises DeleteThresholdExceeded."""
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [1, 2]})

        config = _cfg(
            soft_delete_col="_is_deleted",
            max_delete_percent=10.0,
            on_threshold_breach="error",
        )
        ctx = _make_ctx(df)

        with pytest.raises(DeleteThresholdExceeded, match="exceeds threshold"):
            _apply_deletes(ctx, deleted_keys, config)

    def test_threshold_skip_returns_ensure_column(self):
        """Threshold breach with SKIP returns context with False column."""
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [1, 2]})

        config = _cfg(
            soft_delete_col="_is_deleted",
            max_delete_percent=10.0,
            on_threshold_breach="skip",
        )
        ctx = _make_ctx(df)

        result = _apply_deletes(ctx, deleted_keys, config)
        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    def test_hard_delete_when_no_soft_delete_col(self):
        """When soft_delete_col is None, rows are removed (hard delete)."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        deleted_keys = pd.DataFrame({"id": [2]})

        config = _cfg(soft_delete_col=None)
        ctx = _make_ctx(df)

        result = _apply_deletes(ctx, deleted_keys, config)
        assert "_is_deleted" not in result.df.columns
        assert set(result.df["id"]) == {1, 3}


# ============================================================
# _apply_hard_delete
# ============================================================


class TestApplyHardDelete:
    def test_removes_deleted_rows(self):
        df = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})
        deleted_keys = pd.DataFrame({"id": [2, 4]})
        config = _cfg(soft_delete_col=None)
        ctx = _make_ctx(df)

        result = _apply_hard_delete(ctx, deleted_keys, config)

        assert set(result.df["id"]) == {1, 3}
        assert len(result.df) == 2

    def test_no_deletions_returns_all_rows(self):
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        config = _cfg(soft_delete_col=None)
        ctx = _make_ctx(df)

        result = _apply_hard_delete(ctx, deleted_keys, config)

        assert len(result.df) == 2


# ============================================================
# _ensure_delete_column
# ============================================================


class TestEnsureDeleteColumn:
    def test_adds_column_when_missing(self):
        df = pd.DataFrame({"id": [1, 2]})
        config = _cfg(soft_delete_col="_is_deleted")
        ctx = _make_ctx(df)

        result = _ensure_delete_column(ctx, config)

        assert "_is_deleted" in result.df.columns
        assert (result.df["_is_deleted"] == False).all()  # noqa: E712

    def test_no_change_when_column_exists(self):
        df = pd.DataFrame({"id": [1], "_is_deleted": [True]})
        config = _cfg(soft_delete_col="_is_deleted")
        ctx = _make_ctx(df)

        result = _ensure_delete_column(ctx, config)

        assert result is ctx

    def test_no_soft_delete_col_returns_context(self):
        df = pd.DataFrame({"id": [1]})
        config = _cfg(soft_delete_col=None)
        ctx = _make_ctx(df)

        result = _ensure_delete_column(ctx, config)

        assert result is ctx
        assert "_is_deleted" not in result.df.columns


# ============================================================
# _build_source_keys_query
# ============================================================


class TestBuildSourceKeysQuery:
    def test_with_source_query(self):
        config = _cfg(
            mode=DeleteDetectionMode.SQL_COMPARE,
            source_connection="conn",
            source_query="SELECT DISTINCT id FROM t WHERE active=1",
            source_table="t",
        )
        assert _build_source_keys_query(config) == "SELECT DISTINCT id FROM t WHERE active=1"

    def test_with_source_table(self):
        config = _cfg(
            mode=DeleteDetectionMode.SQL_COMPARE,
            source_connection="conn",
            source_table="dbo.Customers",
        )
        assert _build_source_keys_query(config) == "SELECT DISTINCT id FROM dbo.Customers"

    def test_multiple_keys(self):
        config = _cfg(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["region", "customer_id"],
            source_connection="conn",
            source_table="dbo.Orders",
        )
        query = _build_source_keys_query(config)
        assert query == "SELECT DISTINCT region, customer_id FROM dbo.Orders"


# ============================================================
# _get_row_count
# ============================================================


class TestGetRowCount:
    def test_pandas_returns_len(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert _get_row_count(df, EngineType.PANDAS) == 3

    def test_pandas_empty(self):
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        assert _get_row_count(df, EngineType.PANDAS) == 0


# ============================================================
# _get_target_path
# ============================================================


class TestGetTargetPath:
    def test_write_path_from_engine(self):
        engine = SimpleNamespace(_current_write_path="/write/path")
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_target_path(ctx) == "/write/path"

    def test_input_path_from_engine(self):
        engine = SimpleNamespace(_current_input_path="/input/path")
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_target_path(ctx) == "/input/path"

    def test_current_table_path_from_engine(self):
        engine = SimpleNamespace(current_table_path="/table/path")
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_target_path(ctx) == "/table/path"

    def test_inner_context_table_path(self):
        ctx = _make_ctx(pd.DataFrame({"id": [1]}))
        ctx.context._current_table_path = "/inner/path"
        assert _get_target_path(ctx) == "/inner/path"

    def test_no_path_returns_none(self):
        ctx = _make_ctx(pd.DataFrame({"id": [1]}))
        assert _get_target_path(ctx) is None

    def test_priority_write_over_input(self):
        engine = SimpleNamespace(
            _current_write_path="/write",
            _current_input_path="/input",
        )
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_target_path(ctx) == "/write"


# ============================================================
# _get_connection
# ============================================================


class TestGetConnection:
    def test_returns_connection_by_name(self):
        conn = MagicMock()
        engine = SimpleNamespace(connections={"my_conn": conn})
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_connection(ctx, "my_conn") is conn

    def test_returns_none_for_missing_name(self):
        engine = SimpleNamespace(connections={"other": MagicMock()})
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_connection(ctx, "my_conn") is None

    def test_returns_none_when_no_engine(self):
        ctx = _make_ctx(pd.DataFrame({"id": [1]}))
        assert _get_connection(ctx, "my_conn") is None

    def test_returns_none_when_no_connections_attr(self):
        engine = SimpleNamespace()
        ctx = _make_ctx(pd.DataFrame({"id": [1]}), engine=engine)
        assert _get_connection(ctx, "my_conn") is None


# ============================================================
# _get_jdbc_url
# ============================================================


class TestGetJdbcUrl:
    def test_jdbc_url_attribute(self):
        conn = SimpleNamespace(jdbc_url="jdbc:sqlserver://host")
        assert _get_jdbc_url(conn) == "jdbc:sqlserver://host"

    def test_get_jdbc_url_method(self):
        conn = MagicMock(spec=[])
        conn.get_jdbc_url = MagicMock(return_value="jdbc:mysql://host")
        assert _get_jdbc_url(conn) == "jdbc:mysql://host"

    def test_url_attribute(self):
        conn = SimpleNamespace(url="jdbc:pg://host")
        assert _get_jdbc_url(conn) == "jdbc:pg://host"

    def test_get_options_url(self):
        conn = MagicMock(spec=[])
        conn.get_spark_options = MagicMock(return_value={"url": "jdbc:pg://host2"})
        assert _get_jdbc_url(conn) == "jdbc:pg://host2"

    def test_no_url_raises(self):
        conn = SimpleNamespace()
        with pytest.raises(ValueError, match="Cannot determine JDBC URL"):
            _get_jdbc_url(conn)


# ============================================================
# _get_jdbc_properties
# ============================================================


class TestGetJdbcProperties:
    def test_from_engine_options(self):
        conn = MagicMock(spec=[])
        conn.get_spark_options = MagicMock(
            return_value={"user": "admin", "password": "secret", "driver": "com.Driver"}
        )
        props = _get_jdbc_properties(conn)
        assert props == {"user": "admin", "password": "secret", "driver": "com.Driver"}

    def test_from_attributes(self):
        conn = SimpleNamespace(user="admin", password="secret", jdbc_driver="com.D")
        props = _get_jdbc_properties(conn)
        assert props["user"] == "admin"
        assert props["password"] == "secret"
        assert props["driver"] == "com.D"

    def test_jdbc_properties_dict_merged(self):
        conn = SimpleNamespace(jdbc_properties={"encrypt": "true"})
        props = _get_jdbc_properties(conn)
        assert props["encrypt"] == "true"

    def test_empty_when_nothing(self):
        conn = SimpleNamespace()
        assert _get_jdbc_properties(conn) == {}


# ============================================================
# _get_sqlalchemy_engine
# ============================================================


class TestGetSqlalchemyEngine:
    def test_engine_attribute(self):
        fake_engine = MagicMock()
        conn = SimpleNamespace(engine=fake_engine)
        assert _get_sqlalchemy_engine(conn) is fake_engine

    def test_get_engine_method(self):
        fake_engine = MagicMock()
        conn = MagicMock(spec=[])
        conn.get_engine = MagicMock(return_value=fake_engine)
        assert _get_sqlalchemy_engine(conn) is fake_engine

    @patch("odibi.transformers.delete_detection.create_engine", create=True)
    def test_connection_string(self, mock_create):
        fake_engine = MagicMock()
        mock_create.return_value = fake_engine
        conn = SimpleNamespace(connection_string="sqlite:///test.db")

        with patch(
            "sqlalchemy.create_engine",
            mock_create,
        ):
            result = _get_sqlalchemy_engine(conn)

        assert result is fake_engine

    def test_no_engine_raises(self):
        conn = SimpleNamespace()
        with pytest.raises(ValueError, match="Cannot create SQLAlchemy engine"):
            _get_sqlalchemy_engine(conn)
