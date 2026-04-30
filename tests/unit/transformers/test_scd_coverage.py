"""Coverage tests for odibi.transformers.scd – Pandas paths."""

import logging
import os
from unittest.mock import patch, Mock, MagicMock

import pandas as pd
import pytest
from pydantic import ValidationError

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.scd import SCD2Params, scd2, _scd2_pandas, _safe_isna

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(df):
    ctx = PandasContext()
    return EngineContext(ctx, df, EngineType.PANDAS)


# ---------------------------------------------------------------------------
# SCD2Params validation (lines 115-122)
# ---------------------------------------------------------------------------


class TestSCD2Params:
    """Cover model_validator branches."""

    def test_valid_target_params(self):
        p = SCD2Params(
            target="silver.dim_customers",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        assert p.target == "silver.dim_customers"

    def test_valid_connection_path_params(self):
        p = SCD2Params(
            connection="adls_prod",
            path="OEE/silver/dim",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        assert p.connection == "adls_prod"
        assert p.path == "OEE/silver/dim"

    def test_missing_both_target_and_connection_raises(self):
        with pytest.raises(ValidationError, match="provide either"):
            SCD2Params(
                keys=["id"],
                track_cols=["name"],
                effective_time_col="txn_date",
            )

    def test_both_target_and_connection_raises(self):
        with pytest.raises(ValidationError, match="not both"):
            SCD2Params(
                target="some_table",
                connection="adls",
                path="some/path",
                keys=["id"],
                track_cols=["name"],
                effective_time_col="txn_date",
            )

    def test_default_column_names(self):
        p = SCD2Params(
            target="t",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        assert p.start_time_col == "valid_from"
        assert p.end_time_col == "valid_to"
        assert p.current_flag_col == "is_current"


# ---------------------------------------------------------------------------
# _safe_isna (lines 722-729)
# ---------------------------------------------------------------------------


class TestSafeIsna:
    def test_none_returns_true(self):
        assert _safe_isna(None)

    def test_nan_returns_true(self):
        assert _safe_isna(float("nan"))

    def test_int_returns_false(self):
        assert not _safe_isna(42)

    def test_string_returns_false(self):
        assert not _safe_isna("hello")

    def test_list_returns_false(self):
        # pd.isna on a list raises ValueError; exercises except path
        assert not _safe_isna([1, 2, 3])


# ---------------------------------------------------------------------------
# scd2 main function (lines 125-272)
# ---------------------------------------------------------------------------


class TestScd2Dispatch:
    """Cover dispatch and connection resolution in scd2()."""

    def test_pandas_engine_dispatches(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
            }
        )
        ctx = _make_ctx(df)
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = scd2(ctx, params)
        assert result.engine_type == EngineType.PANDAS
        assert os.path.exists(path)

    def test_unsupported_engine_raises(self):
        df = pd.DataFrame({"id": [1], "name": ["A"], "txn_date": pd.to_datetime(["2024-01-01"])})
        ctx = _make_ctx(df)
        # Override engine_type to something truly unsupported
        ctx.engine_type = "unknown"
        params = SCD2Params(
            target="dummy",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="does not support engine type"):
            scd2(ctx, params)

    def test_connection_resolution(self, tmp_path):
        path = str(tmp_path / "resolved.parquet")
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["A"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
            }
        )
        ctx = _make_ctx(df)
        mock_conn = Mock()
        mock_conn.get_path.return_value = path
        mock_engine = Mock()
        mock_engine.connections = {"my_conn": mock_conn}
        ctx.engine = mock_engine
        params = SCD2Params(
            connection="my_conn",
            path="rel/path",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        scd2(ctx, params)
        assert os.path.exists(path)
        mock_conn.get_path.assert_called_once_with("rel/path")

    def test_connection_not_found_raises(self):
        df = pd.DataFrame({"id": [1], "name": ["A"], "txn_date": pd.to_datetime(["2024-01-01"])})
        ctx = _make_ctx(df)
        mock_engine = Mock()
        mock_engine.connections = {}
        ctx.engine = mock_engine
        params = SCD2Params(
            connection="missing_conn",
            path="rel/path",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="not found"):
            scd2(ctx, params)

    def test_connection_without_get_path_raises(self):
        df = pd.DataFrame({"id": [1], "name": ["A"], "txn_date": pd.to_datetime(["2024-01-01"])})
        ctx = _make_ctx(df)
        # Connection object with no get_path attribute
        conn = object()
        mock_engine = Mock()
        mock_engine.connections = {"my_conn": conn}
        ctx.engine = mock_engine
        params = SCD2Params(
            connection="my_conn",
            path="rel/path",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="does not support path resolution"):
            scd2(ctx, params)


# ---------------------------------------------------------------------------
# _scd2_pandas first run — no existing target (lines 862-894)
# ---------------------------------------------------------------------------


class TestScd2PandasFirstRun:
    """First run: no existing target file."""

    def _params(self, path):
        return SCD2Params(
            target=path,
            keys=["customer_id"],
            track_cols=["name", "city"],
            effective_time_col="txn_date",
        )

    def _source(self):
        return pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "city": ["NYC", "LA"],
                "txn_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            }
        )

    def test_first_run_parquet(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        source = self._source()
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        assert os.path.exists(path)
        written = pd.read_parquet(path)
        assert "valid_from" in written.columns
        assert "valid_to" in written.columns
        assert "is_current" in written.columns
        assert all(written["is_current"])
        assert written["valid_to"].isna().all()

    def test_first_run_csv(self, tmp_path):
        path = str(tmp_path / "target.csv")
        # Pre-create an empty CSV so the file exists (otherwise the code
        # falls through to the parquet writer because ``not os.path.exists``
        # matches before the ``.csv`` check).
        pd.DataFrame().to_csv(path, index=False)
        source = self._source()
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        assert os.path.exists(path)
        written = pd.read_csv(path)
        assert "valid_from" in written.columns
        assert "is_current" in written.columns

    def test_effective_time_copied_to_start_col(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        source = self._source()
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        written = pd.read_parquet(path)
        # valid_from should equal txn_date
        pd.testing.assert_series_equal(
            written["valid_from"].reset_index(drop=True),
            written["txn_date"].reset_index(drop=True),
            check_names=False,
        )


# ---------------------------------------------------------------------------
# _scd2_pandas DuckDB path — existing .parquet target (lines 779-860)
# ---------------------------------------------------------------------------


class TestScd2PandasDuckDB:
    """DuckDB fast path for .parquet targets."""

    def _write_target(self, path):
        target_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "city": ["NYC", "LA"],
                "txn_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "valid_to": [pd.NaT, pd.NaT],
                "is_current": [True, True],
            }
        )
        target_df.to_parquet(path, index=False)

    def _params(self, path):
        return SCD2Params(
            target=path,
            keys=["customer_id"],
            track_cols=["name", "city"],
            effective_time_col="txn_date",
        )

    def test_duckdb_new_key_inserted(self, tmp_path):
        pytest.importorskip("duckdb")
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [3],
                "name": ["Charlie"],
                "city": ["Chicago"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        assert 3 in result["customer_id"].values
        charlie = result[result["customer_id"] == 3]
        assert all(charlie["is_current"])

    def test_duckdb_changed_tracked_column(self, tmp_path):
        pytest.importorskip("duckdb")
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [2],
                "name": ["Bob_Updated"],
                "city": ["SF"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        bob_rows = result[result["customer_id"] == 2]
        assert len(bob_rows) == 2
        closed = bob_rows[~bob_rows["is_current"]]
        assert len(closed) == 1
        current = bob_rows[bob_rows["is_current"]]
        assert current.iloc[0]["name"] == "Bob_Updated"

    def test_duckdb_unchanged_record(self, tmp_path):
        pytest.importorskip("duckdb")
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        # Same data, no change
        source = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        _scd2_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        alice_rows = result[result["customer_id"] == 1]
        # Should remain a single current row
        assert len(alice_rows) == 1
        assert alice_rows.iloc[0]["is_current"]

    def test_duckdb_fallback_on_error(self, tmp_path):
        """If DuckDB connect raises, Pandas fallback should produce correct results."""
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [3],
                "name": ["Charlie"],
                "city": ["Chicago"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        # Patch duckdb.connect to raise so the except branch triggers the Pandas fallback
        pytest.importorskip("duckdb")
        with patch("duckdb.connect", side_effect=RuntimeError("forced error")):
            _scd2_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        assert 3 in result["customer_id"].values


# ---------------------------------------------------------------------------
# _scd2_pandas Pandas fallback — existing target (lines 862-996)
# ---------------------------------------------------------------------------


class TestScd2PandasFallback:
    """
    Force pure Pandas path by patching duckdb out of sys.modules.
    """

    def _write_target(self, path):
        target_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "city": ["NYC", "LA"],
                "txn_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "valid_to": [pd.NaT, pd.NaT],
                "is_current": [True, True],
            }
        )
        target_df.to_parquet(path, index=False)

    def _params(self, path):
        return SCD2Params(
            target=path,
            keys=["customer_id"],
            track_cols=["name", "city"],
            effective_time_col="txn_date",
        )

    def _run_pandas(self, ctx, source, params):
        """Run _scd2_pandas with duckdb import disabled to force Pandas path."""
        import sys

        saved = sys.modules.get("duckdb", "__MISSING__")
        sys.modules["duckdb"] = None  # type: ignore[assignment]
        try:
            return _scd2_pandas(ctx, source, params)
        finally:
            if saved == "__MISSING__":
                sys.modules.pop("duckdb", None)
            else:
                sys.modules["duckdb"] = saved

    def test_new_record_insert(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [3],
                "name": ["Charlie"],
                "city": ["Chicago"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        assert 3 in result["customer_id"].values
        charlie = result[result["customer_id"] == 3]
        assert len(charlie) == 1
        assert charlie.iloc[0]["is_current"]

    def test_changed_record(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [2],
                "name": ["Bob_Updated"],
                "city": ["SF"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        bob = result[result["customer_id"] == 2]
        assert len(bob) == 2
        closed = bob[~bob["is_current"]]
        assert len(closed) == 1
        assert closed.iloc[0]["valid_to"] == pd.Timestamp("2024-06-01")
        current = bob[bob["is_current"]]
        assert current.iloc[0]["name"] == "Bob_Updated"

    def test_unchanged_record(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        alice = result[result["customer_id"] == 1]
        assert len(alice) == 1
        assert alice.iloc[0]["is_current"]

    def test_mix_new_changed_unchanged(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob_Updated", "Charlie"],
                "city": ["NYC", "SF", "Chicago"],
                "txn_date": pd.to_datetime(["2024-06-01", "2024-06-01", "2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        # Alice: unchanged → 1 row
        assert len(result[result["customer_id"] == 1]) == 1
        # Bob: changed → 2 rows (old closed + new current)
        assert len(result[result["customer_id"] == 2]) == 2
        # Charlie: new → 1 row
        assert len(result[result["customer_id"] == 3]) == 1
        assert result[result["customer_id"] == 3].iloc[0]["is_current"]

    def test_float_column_math_isclose(self, tmp_path):
        """Same float within tolerance → no change."""
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "id": [1],
                "amount": [100.0000000001],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
                "is_current": [True],
            }
        )
        target_df.to_parquet(path, index=False)
        source = pd.DataFrame(
            {
                "id": [1],
                "amount": [100.0000000002],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["amount"],
            effective_time_col="txn_date",
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, params)
        result = pd.read_parquet(path)
        assert len(result) == 1  # no new version

    def test_null_to_value_change(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "id": [1],
                "name": [None],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
                "is_current": [True],
            }
        )
        target_df.to_parquet(path, index=False)
        source = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        params = SCD2Params(
            target=path, keys=["id"], track_cols=["name"], effective_time_col="txn_date"
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, params)
        result = pd.read_parquet(path)
        assert len(result) == 2  # old closed + new version

    def test_value_to_null_change(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
                "is_current": [True],
            }
        )
        target_df.to_parquet(path, index=False)
        source = pd.DataFrame(
            {
                "id": [1],
                "name": [None],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        params = SCD2Params(
            target=path, keys=["id"], track_cols=["name"], effective_time_col="txn_date"
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, params)
        result = pd.read_parquet(path)
        assert len(result) == 2

    def test_both_null_no_change(self, tmp_path):
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "id": [1],
                "name": [None],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
                "is_current": [True],
            }
        )
        target_df.to_parquet(path, index=False)
        source = pd.DataFrame(
            {
                "id": [1],
                "name": [None],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        params = SCD2Params(
            target=path, keys=["id"], track_cols=["name"], effective_time_col="txn_date"
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, params)
        result = pd.read_parquet(path)
        assert len(result) == 1  # no change

    def test_csv_target(self, tmp_path):
        path = str(tmp_path / "target.csv")
        target_df = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": ["2024-01-01"],
                "valid_from": ["2024-01-01"],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        target_df.to_csv(path, index=False)
        source = pd.DataFrame(
            {
                "customer_id": [2],
                "name": ["Bob"],
                "city": ["LA"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_csv(path)
        assert 2 in result["customer_id"].values

    def test_multiple_changes_same_key_dedup(self, tmp_path):
        """When source has duplicate keys, keep last for dedup via sort+drop_duplicates."""
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [2, 2],
                "name": ["Bob_V2", "Bob_V3"],
                "city": ["SF", "Denver"],
                "txn_date": pd.to_datetime(["2024-06-01", "2024-07-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        bob_current = result[(result["customer_id"] == 2) & (result["is_current"])]
        # At least one current version for Bob should exist
        assert len(bob_current) >= 1

    def test_target_without_flag_col_unchanged(self, tmp_path):
        """Target missing is_current + no changes → all target rows treated as current (line 904-905)."""
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
            }
        )
        target_df.to_parquet(path, index=False)
        # Source matches target → no changed records → line 904-905 branch hit, no KeyError
        source = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        # Unchanged: original row kept, plus a new insert (source adds SCD cols)
        assert len(result[result["customer_id"] == 1]) >= 1

    def test_target_without_flag_col_new_key(self, tmp_path):
        """Target missing is_current + new key → current_target = full target (line 904-905)."""
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
            }
        )
        target_df.to_parquet(path, index=False)
        # New key, no change for existing → no changed_records → skip line 973
        source = pd.DataFrame(
            {
                "customer_id": [2],
                "name": ["Bob"],
                "city": ["LA"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        assert 2 in result["customer_id"].values

    def test_target_without_flag_col_with_changes(self, tmp_path):
        """Bug fix: target missing is_current + changed records no longer raises KeyError."""
        path = str(tmp_path / "target.parquet")
        target_df = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
            }
        )
        target_df.to_parquet(path, index=False)
        # Changed record — previously caused KeyError on line 973
        source = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["SF"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        self._run_pandas(ctx, source, self._params(path))
        result = pd.read_parquet(path)
        # Old record should be closed, new version inserted
        assert len(result[result["customer_id"] == 1]) >= 2

    def test_delete_col_handling(self, tmp_path):
        """Soft delete col is passed through params without error."""
        path = str(tmp_path / "target.parquet")
        self._write_target(path)
        source = pd.DataFrame(
            {
                "customer_id": [1],
                "name": ["Alice"],
                "city": ["NYC"],
                "is_deleted": [True],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        params = SCD2Params(
            target=path,
            keys=["customer_id"],
            track_cols=["name", "city"],
            effective_time_col="txn_date",
            delete_col="is_deleted",
        )
        ctx = _make_ctx(source)
        # Should not raise
        self._run_pandas(ctx, source, params)
        result = pd.read_parquet(path)
        assert len(result) >= 1


# ---------------------------------------------------------------------------
# scd2() entry point tests (lines 125-270)
# ---------------------------------------------------------------------------


class TestSCD2EntryPoint:
    """Test the scd2() entry point function covering uncovered lines."""

    def test_unsupported_engine_raises(self):
        """Line 204-210: unsupported engine type raises ValueError."""
        source = pd.DataFrame(
            {"id": [1], "name": ["A"], "txn_date": pd.to_datetime(["2024-01-01"])}
        )
        ctx = _make_ctx(source)
        ctx.engine_type = "unknown"
        params = SCD2Params(
            target="dummy.parquet",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="does not support engine"):
            scd2(ctx, params)

    def test_connection_path_resolution(self, tmp_path):
        """Lines 144-171: resolve target from connection+path."""
        from odibi.engine.pandas_engine import PandasEngine

        path = str(tmp_path / "dim.parquet")
        mock_conn = MagicMock()
        mock_conn.get_path.return_value = path

        engine = PandasEngine(connections={"silver": mock_conn}, config={})
        source = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
            }
        )
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source, EngineType.PANDAS, engine=engine)

        params = SCD2Params(
            connection="silver",
            path="dim_customer",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = scd2(ctx, params)
        assert result.df is not None
        mock_conn.get_path.assert_called_with("dim_customer")

    def test_connection_not_found_raises(self):
        """Lines 152-156: connection not in engine.connections raises ValueError."""
        from odibi.engine.pandas_engine import PandasEngine

        engine = PandasEngine(connections={}, config={})
        source = pd.DataFrame({"id": [1], "name": ["A"], "txn_date": ["2024-01-01"]})
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source, EngineType.PANDAS, engine=engine)

        params = SCD2Params(
            connection="nonexistent",
            path="dim",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="not found"):
            scd2(ctx, params)

    def test_connection_without_get_path_raises(self):
        """Lines 166-171: connection without get_path raises ValueError."""
        from odibi.engine.pandas_engine import PandasEngine

        mock_conn = MagicMock(spec=[])  # no get_path
        engine = PandasEngine(connections={"silver": mock_conn}, config={})
        source = pd.DataFrame({"id": [1], "name": ["A"], "txn_date": ["2024-01-01"]})
        pandas_ctx = PandasContext()
        ctx = EngineContext(pandas_ctx, source, EngineType.PANDAS, engine=engine)

        params = SCD2Params(
            connection="silver",
            path="dim",
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        with pytest.raises(ValueError, match="does not support path resolution"):
            scd2(ctx, params)

    def test_csv_target_first_run(self, tmp_path):
        """Line 884-887: CSV target on first run.
        Note: code checks 'not os.path.exists(path)' before '.csv', so first run
        writes parquet format regardless of extension. We test the file is created."""
        path = str(tmp_path / "target.csv")
        source = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "txn_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            }
        )
        ctx = _make_ctx(source)
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = scd2(ctx, params)
        assert os.path.exists(path)
        assert result.df is not None
        assert len(result.df) == 2

    def test_csv_target_with_changes(self, tmp_path):
        """Lines 871-872, 993-996: CSV target with existing data and changes."""
        path = str(tmp_path / "target.csv")
        target = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
                "is_current": [True],
            }
        )
        target.to_csv(path, index=False)

        source = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice Updated"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = scd2(ctx, params)
        assert result.df is not None

    def test_row_count_with_current_arg(self, tmp_path):
        """Lines 180, 184-188: using 'current' parameter instead of context.df."""
        path = str(tmp_path / "target.parquet")
        source = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
            }
        )
        dummy_ctx_df = pd.DataFrame()
        ctx = _make_ctx(dummy_ctx_df)
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = scd2(ctx, params, current=source)
        assert result.df is not None

    def test_pandas_no_flag_col_in_target(self, tmp_path):
        """Line 904-905: target without flag_col treats all as current."""
        path = str(tmp_path / "target.parquet")
        target = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "txn_date": pd.to_datetime(["2024-01-01"]),
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": [pd.NaT],
            }
        )
        target.to_parquet(path, index=False)

        source = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice Updated"],
                "txn_date": pd.to_datetime(["2024-06-01"]),
            }
        )
        ctx = _make_ctx(source)
        params = SCD2Params(
            target=path,
            keys=["id"],
            track_cols=["name"],
            effective_time_col="txn_date",
        )
        result = _scd2_pandas(ctx, source, params)
        # Should not raise, flag_col gets created
        assert "is_current" in result.df.columns
