"""Unit tests for odibi/transformers/delete_detection.py — Pandas paths only."""

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from odibi.config import (
    DeleteDetectionConfig,
    DeleteDetectionMode,
    FirstRunBehavior,
    ThresholdBreachAction,
)
from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.transformers.delete_detection import (
    DeleteThresholdExceeded,
    detect_deletes,
    _apply_deletes,
    _apply_soft_delete,
    _apply_hard_delete,
    _ensure_delete_column,
    _build_source_keys_query,
    _get_row_count,
    _get_target_path,
    _get_connection,
    _get_jdbc_url,
    _get_jdbc_properties,
    _get_sqlalchemy_engine,
    _snapshot_diff_pandas,
    _sql_compare_pandas,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(df, engine=None, extras=None):
    """Build a PandasContext-backed EngineContext."""
    ctx = PandasContext()
    ctx.register("df", df)
    if extras:
        for name, extra_df in extras.items():
            ctx.register(name, extra_df)
    eng = engine or PandasEngine()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=eng.execute_sql,
        engine=eng,
    )


# =========================================================================
# detect_deletes() entry point
# =========================================================================


class TestDetectDeletesEntryPoint:
    def test_mode_none_returns_unchanged(self):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE, keys=["id"])
        result = detect_deletes(ctx, config)
        pd.testing.assert_frame_equal(result.df, df)

    def test_mode_none_via_kwargs(self):
        df = pd.DataFrame({"id": [1]})
        ctx = _make_context(df)
        result = detect_deletes(ctx, mode="none", keys=["id"])
        pd.testing.assert_frame_equal(result.df, df)

    def test_unknown_mode_raises(self):
        df = pd.DataFrame({"id": [1]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(keys=["id"])
        # Force an unknown mode by setting it directly
        config.mode = "totally_fake"
        with pytest.raises(ValueError, match="Unknown delete detection mode"):
            detect_deletes(ctx, config)


# =========================================================================
# _ensure_delete_column()
# =========================================================================


class TestEnsureDeleteColumn:
    def test_adds_false_column(self):
        df = pd.DataFrame({"id": [1, 2]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.NONE, keys=["id"], soft_delete_col="_is_deleted"
        )
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" in result.df.columns
        assert result.df["_is_deleted"].tolist() == [False, False]

    def test_no_op_when_no_soft_delete_col(self):
        df = pd.DataFrame({"id": [1]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.NONE, keys=["id"], soft_delete_col=None
        )
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" not in result.df.columns

    def test_column_already_exists_no_change(self):
        df = pd.DataFrame({"id": [1], "_is_deleted": [True]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.NONE, keys=["id"], soft_delete_col="_is_deleted"
        )
        result = _ensure_delete_column(ctx, config)
        assert result.df["_is_deleted"].tolist() == [True]


# =========================================================================
# _build_source_keys_query()
# =========================================================================


class TestBuildSourceKeysQuery:
    def test_with_source_query(self):
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="conn",
            source_table="tbl",
            source_query="SELECT DISTINCT id FROM custom_view",
        )
        result = _build_source_keys_query(config)
        assert result == "SELECT DISTINCT id FROM custom_view"

    def test_without_source_query_builds_default(self):
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id", "name"],
            source_connection="conn",
            source_table="dbo.Customers",
        )
        result = _build_source_keys_query(config)
        assert result == "SELECT DISTINCT id, name FROM dbo.Customers"


# =========================================================================
# _get_row_count()
# =========================================================================


class TestGetRowCount:
    def test_pandas_returns_len(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert _get_row_count(df, EngineType.PANDAS) == 3

    def test_pandas_empty(self):
        df = pd.DataFrame({"a": []})
        assert _get_row_count(df, EngineType.PANDAS) == 0


# =========================================================================
# _get_target_path()
# =========================================================================


class TestGetTargetPath:
    def test_from_current_write_path(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine._current_write_path = "/data/silver/tbl"
        assert _get_target_path(ctx) == "/data/silver/tbl"

    def test_from_current_input_path(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine._current_input_path = "/data/input/tbl"
        assert _get_target_path(ctx) == "/data/input/tbl"

    def test_from_current_table_path(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine.current_table_path = "/data/table"
        assert _get_target_path(ctx) == "/data/table"

    def test_from_inner_context(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.context._current_table_path = "/inner/path"
        assert _get_target_path(ctx) == "/inner/path"

    def test_returns_none_when_not_found(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        assert _get_target_path(ctx) is None


# =========================================================================
# _get_connection()
# =========================================================================


class TestGetConnection:
    def test_found(self):
        eng = PandasEngine()
        eng.connections = {"my_conn": Mock()}
        ctx = _make_context(pd.DataFrame({"id": [1]}), engine=eng)
        result = _get_connection(ctx, "my_conn")
        assert result is not None

    def test_not_found(self):
        eng = PandasEngine()
        eng.connections = {}
        ctx = _make_context(pd.DataFrame({"id": [1]}), engine=eng)
        result = _get_connection(ctx, "missing")
        assert result is None

    def test_no_engine(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine = None
        assert _get_connection(ctx, "any") is None


# =========================================================================
# _get_jdbc_url()
# =========================================================================


class TestGetJdbcUrl:
    def test_from_jdbc_url_attr(self):
        conn = Mock(spec=["jdbc_url"])
        conn.jdbc_url = "jdbc:sqlserver://host"
        assert _get_jdbc_url(conn) == "jdbc:sqlserver://host"

    def test_from_get_jdbc_url_method(self):
        conn = Mock(spec=["get_jdbc_url"])
        conn.get_jdbc_url.return_value = "jdbc:pg://host"
        assert _get_jdbc_url(conn) == "jdbc:pg://host"

    def test_from_url_attr(self):
        conn = Mock(spec=["url"])
        conn.url = "jdbc:mysql://host"
        assert _get_jdbc_url(conn) == "jdbc:mysql://host"

    def test_from_get_spark_options(self):
        conn = Mock(spec=["get_spark_options"])
        conn.get_spark_options.return_value = {"url": "jdbc:oracle://host"}
        assert _get_jdbc_url(conn) == "jdbc:oracle://host"

    def test_raises_when_no_url(self):
        conn = Mock(spec=[])
        with pytest.raises(ValueError, match="Cannot determine JDBC URL"):
            _get_jdbc_url(conn)


# =========================================================================
# _get_jdbc_properties()
# =========================================================================


class TestGetJdbcProperties:
    def test_from_get_spark_options(self):
        conn = Mock(spec=["get_spark_options"])
        conn.get_spark_options.return_value = {
            "user": "admin",
            "password": "secret",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        props = _get_jdbc_properties(conn)
        assert props["user"] == "admin"
        assert props["password"] == "secret"
        assert props["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def test_from_direct_attrs(self):
        conn = Mock(spec=["user", "password", "jdbc_driver", "jdbc_properties"])
        conn.user = "u"
        conn.password = "p"
        conn.jdbc_driver = "drv"
        conn.jdbc_properties = {"extra": "val"}
        props = _get_jdbc_properties(conn)
        assert props["user"] == "u"
        assert props["password"] == "p"
        assert props["driver"] == "drv"
        assert props["extra"] == "val"

    def test_empty_when_no_attrs(self):
        conn = Mock(spec=[])
        props = _get_jdbc_properties(conn)
        assert props == {}


# =========================================================================
# _get_sqlalchemy_engine()
# =========================================================================


class TestGetSqlalchemyEngine:
    def test_from_engine_attr(self):
        conn = Mock(spec=["engine"])
        conn.engine = "fake_engine"
        assert _get_sqlalchemy_engine(conn) == "fake_engine"

    def test_from_get_engine_method(self):
        conn = Mock(spec=["get_engine"])
        conn.get_engine.return_value = "created_engine"
        assert _get_sqlalchemy_engine(conn) == "created_engine"

    def test_raises_when_no_engine(self):
        conn = Mock(spec=[])
        with pytest.raises(ValueError, match="Cannot create SQLAlchemy engine"):
            _get_sqlalchemy_engine(conn)


# =========================================================================
# _apply_deletes()
# =========================================================================


class TestApplyDeletes:
    def test_no_deletes_returns_with_column(self):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        deleted_keys = pd.DataFrame({"id": []}).astype(int)
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        assert "_is_deleted" in result.df.columns
        assert result.df["_is_deleted"].tolist() == [False, False]

    def test_threshold_exceeded_error(self):
        df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
        deleted_keys = pd.DataFrame({"id": [1, 2, 3, 4]})  # 80%
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.ERROR,
        )
        with pytest.raises(DeleteThresholdExceeded, match="exceeds threshold"):
            _apply_deletes(ctx, deleted_keys, config)

    def test_threshold_exceeded_warn_continues(self):
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [1, 2]})  # 100%
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.WARN,
            soft_delete_col=None,  # hard delete
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        # Hard delete removes all rows
        assert len(result.df) == 0

    def test_threshold_exceeded_skip(self):
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [1, 2]})  # 100%
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.SKIP,
            soft_delete_col="_is_deleted",
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        # Skip → returns unchanged with soft delete column
        assert "_is_deleted" in result.df.columns
        assert all(v is False for v in result.df["_is_deleted"])

    def test_soft_delete_dispatches(self):
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [2]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
            max_delete_percent=None,
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        assert "_is_deleted" in result.df.columns

    def test_hard_delete_dispatches(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        deleted_keys = pd.DataFrame({"id": [2]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col=None,
            max_delete_percent=None,
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        assert set(result.df["id"]) == {1, 3}


# =========================================================================
# _apply_soft_delete() Pandas path
# =========================================================================


class TestApplySoftDeletePandas:
    def test_snapshot_diff_mode_unions_deleted(self):
        """prev_df provided → union deleted rows with _is_deleted=True."""
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        deleted_keys = pd.DataFrame({"id": [3]})
        prev_df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=prev_df)
        res_df = result.df
        assert len(res_df) == 3
        deleted_row = res_df[res_df["id"] == 3]
        assert deleted_row["_is_deleted"].iloc[0]
        alive_rows = res_df[res_df["id"] != 3]
        assert all(not v for v in alive_rows["_is_deleted"])

    def test_sql_compare_mode_flags_existing(self):
        """No prev_df → flags deleted keys in existing df."""
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        deleted_keys = pd.DataFrame({"id": [2]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            soft_delete_col="_is_deleted",
            source_connection="conn",
            source_table="tbl",
        )
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=None)
        res_df = result.df
        assert len(res_df) == 3
        assert res_df.loc[res_df["id"] == 2, "_is_deleted"].iloc[0]
        assert not res_df.loc[res_df["id"] == 1, "_is_deleted"].iloc[0]

    def test_snapshot_diff_aligns_columns(self):
        """Deleted rows get missing columns filled with None."""
        df = pd.DataFrame({"id": [1], "val": ["a"], "extra": [99]})
        deleted_keys = pd.DataFrame({"id": [2]})
        prev_df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})  # no 'extra'
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=prev_df)
        assert "extra" in result.df.columns
        assert len(result.df) == 2


# =========================================================================
# _apply_hard_delete() Pandas path
# =========================================================================


class TestApplyHardDeletePandas:
    def test_removes_deleted_rows(self):
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        deleted_keys = pd.DataFrame({"id": [2]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col=None,
        )
        result = _apply_hard_delete(ctx, deleted_keys, config)
        assert set(result.df["id"]) == {1, 3}

    def test_removes_nothing_when_no_match(self):
        df = pd.DataFrame({"id": [1, 2]})
        deleted_keys = pd.DataFrame({"id": [99]})
        ctx = _make_context(df)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col=None,
        )
        result = _apply_hard_delete(ctx, deleted_keys, config)
        assert set(result.df["id"]) == {1, 2}


# =========================================================================
# _snapshot_diff_pandas() — integration with real Delta tables
# =========================================================================


class TestSnapshotDiffPandas:
    def _write_versions(self, table_path, v0_df, v1_df):
        """Write two versions of a Delta table."""
        import pyarrow as pa
        from deltalake import write_deltalake

        write_deltalake(table_path, pa.Table.from_pandas(v0_df))
        write_deltalake(table_path, pa.Table.from_pandas(v1_df), mode="overwrite")

    def test_detects_deleted_row(self, tmp_path):
        table_path = str(tmp_path / "tbl")
        v0 = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        v1 = pd.DataFrame({"id": [1, 3], "val": ["a", "c"]})  # row 2 removed
        self._write_versions(table_path, v0, v1)

        ctx = _make_context(v1)
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
            connection="local",
            path=str(tmp_path / "tbl"),
            max_delete_percent=None,
        )
        # Set up connection on engine
        mock_conn = Mock()
        mock_conn.get_path = Mock(return_value=table_path)
        ctx.engine.connections = {"local": mock_conn}

        result = _snapshot_diff_pandas(ctx, config)
        res = result.df
        assert len(res) == 3  # 2 alive + 1 deleted
        deleted = res[res["_is_deleted"]]
        assert len(deleted) == 1
        assert deleted["id"].iloc[0] == 2

    def test_first_run_skip(self, tmp_path):
        """Version 0 only → skip with on_first_run=SKIP."""
        import pyarrow as pa
        from deltalake import write_deltalake

        table_path = str(tmp_path / "tbl_first")
        write_deltalake(table_path, pa.Table.from_pandas(pd.DataFrame({"id": [1]})))

        ctx = _make_context(pd.DataFrame({"id": [1]}))
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
            on_first_run=FirstRunBehavior.SKIP,
            connection="local",
            path=str(tmp_path / "tbl_first"),
        )
        mock_conn = Mock()
        mock_conn.get_path = Mock(return_value=table_path)
        ctx.engine.connections = {"local": mock_conn}

        result = _snapshot_diff_pandas(ctx, config)
        assert "_is_deleted" in result.df.columns
        assert result.df["_is_deleted"].tolist() == [False]

    def test_first_run_error(self, tmp_path):
        """Version 0 only → raise with on_first_run=ERROR."""
        import pyarrow as pa
        from deltalake import write_deltalake

        table_path = str(tmp_path / "tbl_err")
        write_deltalake(table_path, pa.Table.from_pandas(pd.DataFrame({"id": [1]})))

        ctx = _make_context(pd.DataFrame({"id": [1]}))
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            on_first_run=FirstRunBehavior.ERROR,
            connection="local",
            path=str(tmp_path / "tbl_err"),
        )
        mock_conn = Mock()
        mock_conn.get_path = Mock(return_value=table_path)
        ctx.engine.connections = {"local": mock_conn}

        with pytest.raises(ValueError, match="No previous version"):
            _snapshot_diff_pandas(ctx, config)

    def test_no_target_path_skips(self):
        """No table path found → skips."""
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert "_is_deleted" in result.df.columns

    def test_not_a_table_skips(self, tmp_path):
        """Target path exists but is not a Delta table → skips."""
        not_delta = str(tmp_path / "not_delta")
        import os

        os.makedirs(not_delta, exist_ok=True)

        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine._current_write_path = not_delta
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert "_is_deleted" in result.df.columns

    def test_missing_keys_in_current(self, tmp_path):
        """Keys not in current DataFrame → skips."""
        import pyarrow as pa
        from deltalake import write_deltalake

        table_path = str(tmp_path / "tbl_mk")
        v0 = pd.DataFrame({"id": [1], "val": ["a"]})
        write_deltalake(table_path, pa.Table.from_pandas(v0))
        write_deltalake(table_path, pa.Table.from_pandas(v0), mode="overwrite")

        ctx = _make_context(pd.DataFrame({"other_col": [1]}))  # no 'id'
        ctx.engine._current_write_path = table_path
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert "_is_deleted" in result.df.columns

    def test_missing_keys_in_previous(self, tmp_path):
        """Keys not in previous version → skips."""
        import pyarrow as pa
        from deltalake import write_deltalake

        table_path = str(tmp_path / "tbl_mpk")
        v0 = pd.DataFrame({"other": [1]})  # no 'id' in v0
        v1 = pd.DataFrame({"id": [1], "val": ["a"]})
        write_deltalake(table_path, pa.Table.from_pandas(v0))
        write_deltalake(
            table_path, pa.Table.from_pandas(v1), mode="overwrite", schema_mode="overwrite"
        )

        ctx = _make_context(v1)
        ctx.engine._current_write_path = table_path
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert "_is_deleted" in result.df.columns

    def test_connection_path_config(self, tmp_path):
        """Uses explicit connection+path from config."""

        table_path = str(tmp_path / "tbl_cp")
        v0 = pd.DataFrame({"id": [1, 2]})
        v1 = pd.DataFrame({"id": [1]})
        self._write_versions(table_path, v0, v1)

        ctx = _make_context(v1)
        mock_conn = Mock()
        mock_conn.get_path = Mock(return_value=table_path)
        ctx.engine.connections = {"silver": mock_conn}

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
            connection="silver",
            path="doesnt_matter",
            max_delete_percent=None,
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert len(result.df) == 2  # 1 alive + 1 deleted

    def test_connection_not_found_falls_back(self, tmp_path):
        """Connection name not in engine.connections → falls back to _get_target_path."""

        table_path = str(tmp_path / "tbl_fb")
        v0 = pd.DataFrame({"id": [1, 2]})
        v1 = pd.DataFrame({"id": [1]})
        self._write_versions(table_path, v0, v1)

        ctx = _make_context(v1)
        ctx.engine.connections = {}  # empty
        ctx.engine._current_write_path = table_path

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col="_is_deleted",
            connection="nonexistent",
            path="doesnt_matter",
            max_delete_percent=None,
        )
        result = _snapshot_diff_pandas(ctx, config)
        assert len(result.df) == 2  # fallback worked


# =========================================================================
# _sql_compare_pandas()
# =========================================================================


class TestSqlComparePandas:
    def test_detects_deleted_keys(self):
        """Source has fewer keys → those missing are flagged as deleted."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        source_keys = pd.DataFrame({"id": [1, 3]})  # id=2 deleted from source
        ctx = _make_context(silver_df)

        mock_conn = Mock()
        mock_conn.engine = Mock()
        ctx.engine.connections = {"src_conn": mock_conn}

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="src_conn",
            source_table="dbo.Source",
            soft_delete_col="_is_deleted",
            max_delete_percent=None,
        )

        with patch("pandas.read_sql", return_value=source_keys):
            result = _sql_compare_pandas(ctx, config)

        res = result.df
        assert len(res) == 3
        assert res.loc[res["id"] == 2, "_is_deleted"].iloc[0]

    def test_connection_not_found_raises(self):
        ctx = _make_context(pd.DataFrame({"id": [1]}))
        ctx.engine.connections = {}
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="missing",
            source_table="tbl",
        )
        with pytest.raises(ValueError, match="not found"):
            _sql_compare_pandas(ctx, config)

    def test_hard_delete_via_sql_compare(self):
        """soft_delete_col=None → removes rows."""
        silver_df = pd.DataFrame({"id": [1, 2, 3]})
        source_keys = pd.DataFrame({"id": [1, 3]})
        ctx = _make_context(silver_df)

        mock_conn = Mock()
        mock_conn.engine = Mock()
        ctx.engine.connections = {"conn": mock_conn}

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="conn",
            source_table="tbl",
            soft_delete_col=None,
            max_delete_percent=None,
        )

        with patch("pandas.read_sql", return_value=source_keys):
            result = _sql_compare_pandas(ctx, config)

        assert set(result.df["id"]) == {1, 3}
