"""Extended unit tests for merge_transformer.py.

Covers MergeParams/AuditColumnsConfig validation, merge entry point routing,
_merge_pandas strategies (upsert, append_only, delete_match), DuckDB path,
DuckDB fallback, initial write, connection path resolution, missing key column,
and audit column semantics.
"""

import os
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.merge_transformer import (
    AuditColumnsConfig,
    MergeParams,
    MergeStrategy,
    _merge_pandas,
    merge,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def source_df():
    return pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [100, 200, 300]}
    )


@pytest.fixture
def target_df():
    return pd.DataFrame(
        {"id": [1, 2, 4], "name": ["Alice_old", "Bob_old", "David"], "value": [50, 150, 400]}
    )


@pytest.fixture
def pandas_ctx():
    return PandasContext()


def _engine_context(pandas_ctx, df, engine=None):
    return EngineContext(pandas_ctx, df, EngineType.PANDAS, engine=engine)


# ===========================================================================
# 1. MergeParams validation
# ===========================================================================


class TestMergeParamsValidation:
    def test_empty_keys_raises(self):
        with pytest.raises(ValueError, match="keys.*must not be empty"):
            MergeParams(target="/tmp/t.parquet", keys=[])

    def test_target_and_connection_raises(self):
        with pytest.raises(ValueError, match="use 'target' OR 'connection'\\+'path', not both"):
            MergeParams(
                target="/tmp/t.parquet",
                connection="conn",
                path="rel/path",
                keys=["id"],
            )

    def test_delete_match_with_audit_cols_raises(self):
        with pytest.raises(ValueError, match="audit_cols.*not used with.*delete_match"):
            MergeParams(
                target="/tmp/t.parquet",
                keys=["id"],
                strategy=MergeStrategy.DELETE_MATCH,
                audit_cols=AuditColumnsConfig(updated_col="u"),
            )


# ===========================================================================
# 2. AuditColumnsConfig validation
# ===========================================================================


class TestAuditColumnsConfigValidation:
    def test_at_least_one_required(self):
        with pytest.raises(ValueError, match="specify at least one"):
            AuditColumnsConfig()

    def test_created_only_is_valid(self):
        cfg = AuditColumnsConfig(created_col="c")
        assert cfg.created_col == "c"
        assert cfg.updated_col is None

    def test_updated_only_is_valid(self):
        cfg = AuditColumnsConfig(updated_col="u")
        assert cfg.updated_col == "u"
        assert cfg.created_col is None


# ===========================================================================
# 3. merge() entry point
# ===========================================================================


class TestMergeEntryPoint:
    def test_legacy_signature_params_is_dataframe(self, source_df, pandas_ctx):
        """When params is a DataFrame (legacy), kwargs become MergeParams."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "out.parquet")
            ctx = _engine_context(pandas_ctx, None)
            merge(ctx, source_df, target=path, keys=["id"])
            assert os.path.exists(path)

    def test_merge_params_object(self, source_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "out.parquet")
            ctx = _engine_context(pandas_ctx, source_df)
            params = MergeParams(target=path, keys=["id"])
            merge(ctx, params)
            assert os.path.exists(path)

    def test_no_dataframe_raises(self):
        mock_ctx = Mock(spec=[])  # no df attribute
        params = MergeParams(target="/tmp/x.parquet", keys=["id"])
        with pytest.raises(ValueError, match="Merge requires a DataFrame"):
            merge(mock_ctx, params)


# ===========================================================================
# 4. _merge_pandas – strategies
# ===========================================================================


class TestMergePandasUpsert:
    def test_new_rows_inserted_existing_updated(self, source_df, target_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")
            target_df.to_parquet(path, index=False)

            ctx = _engine_context(pandas_ctx, source_df)
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.UPSERT,
                None,
                MergeParams(target=path, keys=["id"]),
            )

            result = pd.read_parquet(path).sort_values("id").reset_index(drop=True)
            assert len(result) == 4
            assert result.loc[result["id"] == 1, "name"].iloc[0] == "Alice"
            assert result.loc[result["id"] == 2, "name"].iloc[0] == "Bob"
            assert result.loc[result["id"] == 3, "name"].iloc[0] == "Charlie"
            assert result.loc[result["id"] == 4, "name"].iloc[0] == "David"


class TestMergePandasAppendOnly:
    def test_new_rows_added_existing_ignored(self, source_df, target_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")
            target_df.to_parquet(path, index=False)

            ctx = _engine_context(pandas_ctx, source_df)
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.APPEND_ONLY,
                None,
                MergeParams(target=path, keys=["id"]),
            )

            result = pd.read_parquet(path).sort_values("id").reset_index(drop=True)
            assert len(result) == 4
            # Existing rows keep old values
            assert result.loc[result["id"] == 1, "name"].iloc[0] == "Alice_old"
            assert result.loc[result["id"] == 2, "name"].iloc[0] == "Bob_old"
            # New row inserted
            assert result.loc[result["id"] == 3, "name"].iloc[0] == "Charlie"
            # Original target row kept
            assert result.loc[result["id"] == 4, "name"].iloc[0] == "David"


class TestMergePandasDeleteMatch:
    def test_matching_keys_removed(self, source_df, target_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")
            target_df.to_parquet(path, index=False)

            ctx = _engine_context(pandas_ctx, source_df)
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.DELETE_MATCH,
                None,
                MergeParams(target=path, keys=["id"]),
            )

            result = pd.read_parquet(path)
            # Source ids = {1,2,3}; target ids = {1,2,4} → only 4 survives
            assert list(result["id"]) == [4]

    def test_delete_match_no_target_returns_source(self, source_df, pandas_ctx):
        """delete_match with no existing target returns source unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nonexistent.parquet")
            ctx = _engine_context(pandas_ctx, source_df)
            result = _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.DELETE_MATCH,
                None,
                MergeParams(target=path, keys=["id"]),
            )
            assert len(result) == 3


# ===========================================================================
# 5. DuckDB path
# ===========================================================================


class TestMergePandasDuckDB:
    def test_duckdb_initial_write(self, source_df, pandas_ctx):
        """When target .parquet doesn't exist, DuckDB COPY creates it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "target.parquet")
            ctx = _engine_context(pandas_ctx, source_df)
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.UPSERT,
                None,
                MergeParams(target=path, keys=["id"]),
            )
            assert os.path.exists(path)
            result = pd.read_parquet(path)
            assert len(result) == 3

    @patch("odibi.transformers.merge_transformer.duckdb", create=True)
    def test_duckdb_failure_falls_back_to_pandas(
        self, mock_duckdb_mod, source_df, target_df, pandas_ctx
    ):
        """If DuckDB raises, _merge_pandas falls back to Pandas path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "target.parquet")
            target_df.to_parquet(path, index=False)

            # Force the duckdb import inside _merge_pandas to succeed but connect to fail
            mock_con = MagicMock()
            mock_duckdb_mod.connect.return_value = mock_con
            mock_con.register.side_effect = RuntimeError("boom")

            ctx = _engine_context(pandas_ctx, source_df)

            # Despite DuckDB failure the function should complete via Pandas
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.UPSERT,
                None,
                MergeParams(target=path, keys=["id"]),
            )

            result = pd.read_parquet(path).sort_values("id").reset_index(drop=True)
            assert len(result) == 4


# ===========================================================================
# 6. Initial write (no target)
# ===========================================================================


class TestMergePandasInitialWrite:
    def test_creates_directory_and_writes_parquet(self, source_df, pandas_ctx):
        """First write when target doesn't exist should create dirs and file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "deep", "nested", "init.parquet")
            ctx = _engine_context(pandas_ctx, source_df)
            _merge_pandas(
                ctx,
                source_df.copy(),
                path,
                ["id"],
                MergeStrategy.UPSERT,
                None,
                MergeParams(target=path, keys=["id"]),
            )
            assert os.path.exists(path)
            assert len(pd.read_parquet(path)) == 3


# ===========================================================================
# 7. Connection path resolution inside _merge_pandas
# ===========================================================================


class TestMergePandasConnectionResolution:
    def test_connection_path_resolved(self, source_df, pandas_ctx):
        """_merge_pandas resolves 'conn.relpath' via context.engine.connections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = os.path.join(tmpdir, "resolved.parquet")

            mock_conn = Mock()
            mock_conn.get_path.return_value = resolved

            mock_engine = Mock()
            mock_engine.connections = {"myconn": mock_conn}

            ctx = _engine_context(pandas_ctx, source_df, engine=mock_engine)

            _merge_pandas(
                ctx,
                source_df.copy(),
                "myconn.relpath",
                ["id"],
                MergeStrategy.UPSERT,
                None,
                MergeParams(target="myconn.relpath", keys=["id"]),
            )

            mock_conn.get_path.assert_called_once_with("relpath")
            assert os.path.exists(resolved)


# ===========================================================================
# 8. Missing key column
# ===========================================================================


class TestMergePandasMissingKey:
    def test_missing_key_column_raises(self, source_df, target_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")
            target_df.to_parquet(path, index=False)

            ctx = _engine_context(pandas_ctx, source_df)
            with pytest.raises(ValueError, match="Merge key column.*not found"):
                _merge_pandas(
                    ctx,
                    source_df.copy(),
                    path,
                    ["nonexistent_col"],
                    MergeStrategy.UPSERT,
                    None,
                    MergeParams(target=path, keys=["nonexistent_col"]),
                )


# ===========================================================================
# 9. Audit columns semantics
# ===========================================================================


class TestMergePandasAuditColumns:
    def test_updated_col_set_on_every_merge(self, source_df, pandas_ctx):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")

            ctx = _engine_context(pandas_ctx, source_df)
            src = source_df.copy()
            audit = AuditColumnsConfig(updated_col="dw_updated")
            _merge_pandas(
                ctx,
                src,
                path,
                ["id"],
                MergeStrategy.UPSERT,
                audit,
                MergeParams(
                    target=path,
                    keys=["id"],
                    audit_cols=audit,
                ),
            )

            result = pd.read_parquet(path)
            assert "dw_updated" in result.columns
            assert result["dw_updated"].notna().all()

    @patch.dict("sys.modules", {"duckdb": None})
    def test_created_col_preserved_on_update(self, pandas_ctx):
        """created_col on an existing row is NOT overwritten during upsert.

        Patches out DuckDB to force the Pandas fallback path which preserves
        the target's created_col on update.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.parquet")

            # Pre-existing target with created_at and updated_at already set
            old_ts = pd.Timestamp("2020-01-01")
            target = pd.DataFrame(
                {
                    "id": [1, 2],
                    "name": ["Alice_old", "Bob_old"],
                    "value": [50, 150],
                    "dw_created": [old_ts, old_ts],
                    "dw_updated": [old_ts, old_ts],
                }
            )
            target.to_parquet(path, index=False)

            # Source updates both existing rows (no new inserts)
            source = pd.DataFrame(
                {"id": [1, 2], "name": ["Alice_new", "Bob_new"], "value": [100, 200]}
            )
            audit = AuditColumnsConfig(created_col="dw_created", updated_col="dw_updated")
            ctx = _engine_context(pandas_ctx, source)

            _merge_pandas(
                ctx,
                source,
                path,
                ["id"],
                MergeStrategy.UPSERT,
                audit,
                MergeParams(target=path, keys=["id"], audit_cols=audit),
            )

            result = pd.read_parquet(path).sort_values("id").reset_index(drop=True)
            # created_col preserved from target (old timestamp)
            assert result["dw_created"].iloc[0] == old_ts
            assert result["dw_created"].iloc[1] == old_ts
            # updated_col was refreshed (newer than old_ts)
            assert result["dw_updated"].iloc[0] > old_ts
            # names were updated
            assert result["name"].iloc[0] == "Alice_new"
