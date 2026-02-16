import logging
import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import ValidationError

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.merge_transformer import (
    AuditColumnsConfig,
    MergeParams,
    MergeStrategy,
    _merge_pandas,
    merge,
)

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pandas_ctx():
    ctx = PandasContext()
    return ctx


@pytest.fixture
def engine_ctx(pandas_ctx):
    df = pd.DataFrame({"id": [1], "val": ["a"]})
    return EngineContext(pandas_ctx, df, EngineType.PANDAS)


@pytest.fixture
def source_df():
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


@pytest.fixture
def target_df():
    return pd.DataFrame({"id": [2, 3, 4], "name": ["B", "C", "D"]})


# ===========================================================================
# 1. MergeStrategy enum
# ===========================================================================


class TestMergeStrategy:
    def test_upsert_value(self):
        assert MergeStrategy.UPSERT.value == "upsert"

    def test_append_only_value(self):
        assert MergeStrategy.APPEND_ONLY.value == "append_only"

    def test_delete_match_value(self):
        assert MergeStrategy.DELETE_MATCH.value == "delete_match"

    def test_from_string(self):
        assert MergeStrategy("upsert") is MergeStrategy.UPSERT
        assert MergeStrategy("append_only") is MergeStrategy.APPEND_ONLY
        assert MergeStrategy("delete_match") is MergeStrategy.DELETE_MATCH

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            MergeStrategy("invalid")


# ===========================================================================
# 2. AuditColumnsConfig
# ===========================================================================


class TestAuditColumnsConfig:
    def test_valid_created_only(self):
        cfg = AuditColumnsConfig(created_col="created_at")
        assert cfg.created_col == "created_at"
        assert cfg.updated_col is None

    def test_valid_updated_only(self):
        cfg = AuditColumnsConfig(updated_col="updated_at")
        assert cfg.updated_col == "updated_at"
        assert cfg.created_col is None

    def test_valid_both(self):
        cfg = AuditColumnsConfig(created_col="c", updated_col="u")
        assert cfg.created_col == "c"
        assert cfg.updated_col == "u"

    def test_invalid_neither(self):
        with pytest.raises(ValidationError, match="at least one"):
            AuditColumnsConfig()

    def test_invalid_empty_strings(self):
        with pytest.raises(ValidationError, match="at least one"):
            AuditColumnsConfig(created_col="", updated_col="")


# ===========================================================================
# 3. MergeParams
# ===========================================================================


class TestMergeParams:
    def test_valid_minimal_target(self):
        p = MergeParams(target="my_table", keys=["id"])
        assert p.target == "my_table"
        assert p.keys == ["id"]
        assert p.strategy == MergeStrategy.UPSERT

    def test_valid_connection_path(self):
        p = MergeParams(connection="conn1", path="silver/tbl", keys=["id"])
        assert p.connection == "conn1"
        assert p.path == "silver/tbl"
        assert p.target is None

    def test_empty_keys_raises(self):
        with pytest.raises(ValidationError, match="keys"):
            MergeParams(target="t", keys=[])

    def test_no_target_no_connection_raises(self):
        with pytest.raises(ValidationError, match="target.*OR.*connection"):
            MergeParams(keys=["id"])

    def test_both_target_and_connection_raises(self):
        with pytest.raises(ValidationError, match="not both"):
            MergeParams(target="t", connection="c", path="p", keys=["id"])

    def test_delete_match_with_audit_raises(self):
        with pytest.raises(ValidationError, match="audit_cols.*delete_match"):
            MergeParams(
                target="t",
                keys=["id"],
                strategy="delete_match",
                audit_cols={"created_col": "c"},
            )

    def test_default_strategy_is_upsert(self):
        p = MergeParams(target="t", keys=["id"])
        assert p.strategy == MergeStrategy.UPSERT

    def test_all_optional_fields(self):
        p = MergeParams(
            target="t",
            keys=["id"],
            strategy="append_only",
            optimize_write=True,
            vacuum_hours=168,
            zorder_by=["col1"],
            cluster_by=["col2"],
            update_condition="source.ver > target.ver",
            insert_condition="source.x = 1",
            delete_condition="source.del = true",
            table_properties={"key": "val"},
        )
        assert p.optimize_write is True
        assert p.vacuum_hours == 168
        assert p.zorder_by == ["col1"]
        assert p.cluster_by == ["col2"]
        assert p.update_condition == "source.ver > target.ver"
        assert p.table_properties == {"key": "val"}

    def test_connection_without_path_raises(self):
        with pytest.raises(ValidationError):
            MergeParams(connection="c", keys=["id"])

    def test_path_without_connection_raises(self):
        with pytest.raises(ValidationError):
            MergeParams(path="p", keys=["id"])


# ===========================================================================
# 4. _merge_pandas – Pandas fallback path
# ===========================================================================


class TestMergePandasFallback:
    """Tests for _merge_pandas using the Pandas fallback (non-.parquet extension)."""

    def _make_ctx(self):
        pandas_ctx = PandasContext()
        return EngineContext(pandas_ctx, None, EngineType.PANDAS)

    def test_upsert_new_file(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.UPSERT, None, {})
        assert os.path.exists(path)
        written = pd.read_parquet(path)
        assert len(written) == 3
        pd.testing.assert_frame_equal(result.reset_index(drop=True), source_df)

    def test_upsert_updates_and_inserts(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.pq")
        target_df.to_parquet(path, index=False)

        ctx = self._make_ctx()
        _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.UPSERT, None, {})

        merged = pd.read_parquet(path)
        assert set(merged["id"].tolist()) == {1, 2, 3, 4}
        row2 = merged.loc[merged["id"] == 2, "name"].iloc[0]
        assert row2 == "b"
        row3 = merged.loc[merged["id"] == 3, "name"].iloc[0]
        assert row3 == "c"
        row4 = merged.loc[merged["id"] == 4, "name"].iloc[0]
        assert row4 == "D"

    def test_append_only_new_file(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx()
        result = _merge_pandas(
            ctx, source_df.copy(), path, ["id"], MergeStrategy.APPEND_ONLY, None, {}
        )
        assert os.path.exists(path)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), source_df)

    def test_append_only_inserts_new_keys(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.pq")
        target_df.to_parquet(path, index=False)

        ctx = self._make_ctx()
        _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.APPEND_ONLY, None, {})

        merged = pd.read_parquet(path)
        assert set(merged["id"].tolist()) == {1, 2, 3, 4}
        row2 = merged.loc[merged["id"] == 2, "name"].iloc[0]
        assert row2 == "B"
        row3 = merged.loc[merged["id"] == 3, "name"].iloc[0]
        assert row3 == "C"

    def test_delete_match_removes_records(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.pq")
        target_df.to_parquet(path, index=False)

        delete_keys = pd.DataFrame({"id": [2, 3], "name": ["x", "x"]})
        ctx = self._make_ctx()
        _merge_pandas(ctx, delete_keys, path, ["id"], MergeStrategy.DELETE_MATCH, None, {})

        merged = pd.read_parquet(path)
        assert merged["id"].tolist() == [4]

    def test_delete_match_no_target_returns_source(self, tmp_path):
        path = str(tmp_path / "nonexistent.pq")
        src = pd.DataFrame({"id": [1], "name": ["a"]})
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, src, path, ["id"], MergeStrategy.DELETE_MATCH, None, {})
        pd.testing.assert_frame_equal(result, src)

    def test_audit_columns_set_on_insert(self, tmp_path):
        path = str(tmp_path / "out.pq")
        src = pd.DataFrame({"id": [1], "name": ["a"]})
        audit = AuditColumnsConfig(created_col="created_at", updated_col="updated_at")
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, src.copy(), path, ["id"], MergeStrategy.UPSERT, audit, {})
        assert "created_at" in result.columns
        assert "updated_at" in result.columns
        assert pd.notna(result["created_at"].iloc[0])
        assert pd.notna(result["updated_at"].iloc[0])

    def test_audit_updated_col_only(self, tmp_path):
        path = str(tmp_path / "out.pq")
        src = pd.DataFrame({"id": [1], "name": ["a"]})
        audit = AuditColumnsConfig(updated_col="modified_at")
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, src.copy(), path, ["id"], MergeStrategy.UPSERT, audit, {})
        assert "modified_at" in result.columns
        assert pd.notna(result["modified_at"].iloc[0])

    def test_audit_preserves_created_col_on_upsert(self, tmp_path):
        path = str(tmp_path / "out.pq")
        old_ts = pd.Timestamp("2020-01-01")
        target = pd.DataFrame({"id": [1], "name": ["old"], "created_at": [old_ts]})
        target.to_parquet(path, index=False)

        src = pd.DataFrame({"id": [1], "name": ["new"]})
        audit = AuditColumnsConfig(created_col="created_at", updated_col="updated_at")
        ctx = self._make_ctx()
        _merge_pandas(ctx, src.copy(), path, ["id"], MergeStrategy.UPSERT, audit, {})

        merged = pd.read_parquet(path)
        row = merged.loc[merged["id"] == 1]
        assert row["created_at"].iloc[0] == old_ts
        assert row["name"].iloc[0] == "new"

    def test_key_not_found_raises(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.pq")
        target_df.to_parquet(path, index=False)
        ctx = self._make_ctx()
        with pytest.raises(ValueError, match="not found"):
            _merge_pandas(
                ctx, source_df.copy(), path, ["missing_key"], MergeStrategy.UPSERT, None, {}
            )

    def test_delete_match_empty_target_returns_source(self, tmp_path):
        path = str(tmp_path / "out.pq")
        empty = pd.DataFrame(
            {"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")}
        )
        empty.to_parquet(path, index=False)

        src = pd.DataFrame({"id": [1], "name": ["a"]})
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, src, path, ["id"], MergeStrategy.DELETE_MATCH, None, {})
        pd.testing.assert_frame_equal(result, src)

    def test_multi_key_upsert(self, tmp_path):
        path = str(tmp_path / "out.pq")
        target = pd.DataFrame({"k1": [1, 1], "k2": ["a", "b"], "val": [10, 20]})
        target.to_parquet(path, index=False)

        src = pd.DataFrame({"k1": [1, 2], "k2": ["b", "c"], "val": [99, 30]})
        ctx = self._make_ctx()
        _merge_pandas(ctx, src.copy(), path, ["k1", "k2"], MergeStrategy.UPSERT, None, {})

        merged = pd.read_parquet(path)
        assert len(merged) == 3
        row_1b = merged.loc[(merged["k1"] == 1) & (merged["k2"] == "b"), "val"].iloc[0]
        assert row_1b == 99


# ===========================================================================
# 4b. _merge_pandas – DuckDB path
# ===========================================================================


class TestMergePandasDuckdb:
    """Tests for _merge_pandas using the DuckDB path (.parquet extension)."""

    def _make_ctx(self):
        pandas_ctx = PandasContext()
        return EngineContext(pandas_ctx, None, EngineType.PANDAS)

    @pytest.fixture(autouse=True)
    def _check_duckdb(self):
        pytest.importorskip("duckdb")

    def test_upsert_new_file(self, tmp_path, source_df):
        path = str(tmp_path / "out.parquet")
        ctx = self._make_ctx()
        result = _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.UPSERT, None, {})
        assert os.path.exists(path)
        written = pd.read_parquet(path)
        assert len(written) == 3
        pd.testing.assert_frame_equal(result.reset_index(drop=True), source_df)

    def test_upsert_existing(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.parquet")
        target_df.to_parquet(path, index=False)

        ctx = self._make_ctx()
        _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.UPSERT, None, {})

        merged = pd.read_parquet(path)
        assert set(merged["id"].tolist()) == {1, 2, 3, 4}

    def test_append_only_existing(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.parquet")
        target_df.to_parquet(path, index=False)

        ctx = self._make_ctx()
        _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.APPEND_ONLY, None, {})

        merged = pd.read_parquet(path)
        assert set(merged["id"].tolist()) == {1, 2, 3, 4}

    def test_delete_match_existing(self, tmp_path, target_df):
        path = str(tmp_path / "out.parquet")
        target_df.to_parquet(path, index=False)

        delete_src = pd.DataFrame({"id": [2], "name": ["x"]})
        ctx = self._make_ctx()
        _merge_pandas(ctx, delete_src, path, ["id"], MergeStrategy.DELETE_MATCH, None, {})

        merged = pd.read_parquet(path)
        assert 2 not in merged["id"].tolist()
        assert set(merged["id"].tolist()) == {3, 4}

    def test_delete_match_no_target(self, tmp_path, source_df):
        path = str(tmp_path / "nonexistent.parquet")
        ctx = self._make_ctx()
        result = _merge_pandas(
            ctx, source_df.copy(), path, ["id"], MergeStrategy.DELETE_MATCH, None, {}
        )
        pd.testing.assert_frame_equal(result.reset_index(drop=True), source_df)

    def test_atomic_write(self, tmp_path, source_df, target_df):
        path = str(tmp_path / "out.parquet")
        target_df.to_parquet(path, index=False)

        ctx = self._make_ctx()
        _merge_pandas(ctx, source_df.copy(), path, ["id"], MergeStrategy.UPSERT, None, {})

        tmp_file = path + ".tmp.parquet"
        assert not os.path.exists(tmp_file)
        assert os.path.exists(path)


# ===========================================================================
# 5. merge() function
# ===========================================================================


class TestMergeFunction:
    def _make_ctx(self, df=None):
        pandas_ctx = PandasContext()
        if df is not None:
            pandas_ctx.register("df", df)
        ctx = EngineContext(pandas_ctx, df, EngineType.PANDAS)
        return ctx

    def test_with_merge_params(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx(source_df)
        params = MergeParams(target=path, keys=["id"])
        result = merge(ctx, params=params)
        assert result is not None
        assert os.path.exists(path)

    def test_legacy_signature(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx()
        result = merge(ctx, source_df, target=path, keys=["id"])
        assert result is not None
        assert os.path.exists(path)

    def test_kwargs_signature(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx(source_df)
        result = merge(ctx, target=path, keys=["id"])
        assert result is not None
        assert os.path.exists(path)

    def test_no_dataframe_raises(self, tmp_path):
        ctx_no_df = MagicMock(spec=[])
        params = MergeParams(target=str(tmp_path / "out.pq"), keys=["id"])
        with pytest.raises(ValueError, match="DataFrame"):
            merge(ctx_no_df, params=params)

    def test_unsupported_context_raises(self, tmp_path, source_df):
        mock_context_cls = type("UnknownContext", (), {})
        unknown = mock_context_cls()
        ctx = EngineContext(unknown, source_df, EngineType.PANDAS)
        params = MergeParams(target=str(tmp_path / "out.pq"), keys=["id"])
        with pytest.raises(ValueError, match="Unsupported context"):
            merge(ctx, params=params)

    def test_connection_not_found_raises(self, source_df):
        engine = MagicMock()
        engine.connections = {}
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source_df, EngineType.PANDAS, engine=engine)
        params = MergeParams(connection="missing", path="a/b", keys=["id"])
        with pytest.raises(ValueError, match="not found"):
            merge(ctx, params=params)

    def test_connection_no_get_path_raises(self, source_df):
        conn = MagicMock(spec=[])
        engine = MagicMock()
        engine.connections = {"my_conn": conn}
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source_df, EngineType.PANDAS, engine=engine)
        params = MergeParams(connection="my_conn", path="a/b", keys=["id"])
        with pytest.raises(ValueError, match="get_path"):
            merge(ctx, params=params)

    def test_connection_resolves_path(self, tmp_path, source_df):
        conn = MagicMock()
        conn.get_path.return_value = str(tmp_path / "resolved.pq")
        engine = MagicMock()
        engine.connections = {"my_conn": conn}
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source_df, EngineType.PANDAS, engine=engine)
        params = MergeParams(connection="my_conn", path="a/b", keys=["id"])
        result = merge(ctx, params=params)
        assert result is not None
        conn.get_path.assert_called_once_with("a/b")

    def test_current_from_context_df(self, tmp_path, source_df):
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx(source_df)
        params = MergeParams(target=path, keys=["id"])
        merge(ctx, params=params)
        assert os.path.exists(path)
        written = pd.read_parquet(path)
        assert len(written) == len(source_df)

    def test_explicit_current_overrides_context(self, tmp_path, source_df):
        other_df = pd.DataFrame({"id": [99], "name": ["z"]})
        path = str(tmp_path / "out.pq")
        ctx = self._make_ctx(other_df)
        params = MergeParams(target=path, keys=["id"])
        merge(ctx, params=params, current=source_df)
        written = pd.read_parquet(path)
        assert set(written["id"].tolist()) == {1, 2, 3}
