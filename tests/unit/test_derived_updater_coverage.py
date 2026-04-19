"""Tests for odibi.derived_updater — utility functions + Pandas mock paths."""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from odibi.derived_updater import (
    DerivedUpdater,
    VALID_DERIVED_TABLES,
    MAX_CLAIM_AGE_MINUTES,
    _convert_df_for_delta,
    _get_guard_table_arrow_schema,
    _retry_delta_operation,
    _sql_nullable_float,
    _sql_nullable_int,
    parse_duration_to_minutes,
    sql_escape,
)

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Pure utility function tests
# ---------------------------------------------------------------------------


class TestSqlEscape:
    def test_none_returns_empty(self):
        assert sql_escape(None) == ""

    def test_no_quotes(self):
        assert sql_escape("hello") == "hello"

    def test_single_quotes_escaped(self):
        assert sql_escape("it's") == "it''s"

    def test_multiple_quotes(self):
        assert sql_escape("a'b'c") == "a''b''c"


class TestSqlNullableFloat:
    def test_none_returns_null(self):
        assert _sql_nullable_float(None) == "NULL"

    def test_float_value(self):
        assert _sql_nullable_float(3.14) == "3.14"

    def test_zero(self):
        assert _sql_nullable_float(0.0) == "0.0"

    def test_int_converted(self):
        assert _sql_nullable_float(5) == "5.0"


class TestSqlNullableInt:
    def test_none_returns_null(self):
        assert _sql_nullable_int(None) == "NULL"

    def test_int_value(self):
        assert _sql_nullable_int(42) == "42"

    def test_zero(self):
        assert _sql_nullable_int(0) == "0"


class TestParseDurationToMinutes:
    def test_minutes(self):
        assert parse_duration_to_minutes("30m") == 30

    def test_hours(self):
        assert parse_duration_to_minutes("6h") == 360

    def test_days(self):
        assert parse_duration_to_minutes("1d") == 1440

    def test_weeks(self):
        assert parse_duration_to_minutes("2w") == 20160

    def test_empty_string(self):
        assert parse_duration_to_minutes("") == 0

    def test_invalid_format(self):
        assert parse_duration_to_minutes("abc") == 0

    def test_uppercase(self):
        assert parse_duration_to_minutes("6H") == 360

    def test_with_spaces(self):
        assert parse_duration_to_minutes("  30m  ") == 30


class TestRetryDeltaOperation:
    def test_success_first_try(self):
        result = _retry_delta_operation(lambda: 42)
        assert result == 42

    def test_non_concurrent_error_raises_immediately(self):
        def fail():
            raise ValueError("not a concurrent error")

        with pytest.raises(ValueError, match="not a concurrent error"):
            _retry_delta_operation(fail, max_retries=3, base_delay=0.01)

    def test_concurrent_error_retries(self):
        call_count = 0

        def sometimes_fail():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("ConcurrentAppendException: conflict")
            return "success"

        result = _retry_delta_operation(sometimes_fail, max_retries=5, base_delay=0.01)
        assert result == "success"
        assert call_count == 3

    def test_max_retries_exceeded_raises(self):
        def always_fail():
            raise Exception("concurrent conflict")

        with pytest.raises(Exception, match="concurrent conflict"):
            _retry_delta_operation(always_fail, max_retries=1, base_delay=0.01)


class TestGetGuardTableArrowSchema:
    def test_returns_schema(self):
        schema = _get_guard_table_arrow_schema()
        assert schema is not None
        assert len(schema) == 7
        assert schema.field("derived_table").type == pa.string()
        assert schema.field("claimed_at").type == pa.timestamp("us", tz="UTC")


class TestConvertDfForDelta:
    def test_normal_df(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = _convert_df_for_delta(df)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2

    def test_null_column_cast_to_string(self):
        df = pd.DataFrame({"a": [1, 2], "b": [None, None]})
        result = _convert_df_for_delta(df)
        assert result.schema.field("b").type == pa.string()


class TestConstants:
    def test_valid_derived_tables(self):
        assert "meta_daily_stats" in VALID_DERIVED_TABLES
        assert "meta_pipeline_health" in VALID_DERIVED_TABLES
        assert "meta_sla_status" in VALID_DERIVED_TABLES

    def test_max_claim_age(self):
        assert MAX_CLAIM_AGE_MINUTES == 60


# ---------------------------------------------------------------------------
# DerivedUpdater class — mock-based tests
# ---------------------------------------------------------------------------


def _mock_catalog(mode="pandas"):
    """Create a mock CatalogManager for testing."""
    cat = MagicMock()
    cat.tables = {
        "meta_derived_applied_runs": "/tmp/guard",
        "meta_observability_errors": "/tmp/errors",
    }
    cat.is_spark_mode = mode == "spark"
    cat.is_pandas_mode = mode == "pandas"
    cat.is_sql_server_mode = mode == "sql_server"
    cat._get_storage_options.return_value = None
    return cat


class TestDerivedUpdaterInit:
    def test_init_sets_paths(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        assert du._guard_path == "/tmp/guard"
        assert du._errors_path == "/tmp/errors"
        assert du.catalog is cat


class TestTryClaimDispatch:
    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.try_claim("meta_daily_stats", "run-1")


class TestMarkAppliedDispatch:
    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.mark_applied("meta_daily_stats", "run-1", "token")


class TestMarkFailedDispatch:
    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.mark_failed("meta_daily_stats", "run-1", "token", "error")

    def test_error_message_truncated(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        long_msg = "x" * 1000
        # Mock _mark_failed_pandas to capture the truncated message
        du._mark_failed_pandas = MagicMock()
        du.mark_failed("meta_daily_stats", "run-1", "token", long_msg)
        args = du._mark_failed_pandas.call_args[0]
        assert len(args[3]) == 500


class TestReclaimDispatch:
    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.reclaim_for_rebuild("meta_daily_stats", "run-1")


class TestLogObservabilityError:
    def test_swallows_all_exceptions(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du._log_error_pandas = MagicMock(side_effect=RuntimeError("boom"))
        # Should NOT raise
        du.log_observability_error("component", "error msg", "run-1", "pipeline")

    def test_pandas_mode_calls_log_error_pandas(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du._log_error_pandas = MagicMock()
        du.log_observability_error("comp", "msg", "run-1", "pipe")
        du._log_error_pandas.assert_called_once()

    def test_no_backend_silently_skips(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        # Should NOT raise
        du.log_observability_error("comp", "msg")


class TestTryClaimPandas:
    def test_already_claimed_returns_none(self, tmp_path):
        """If row already exists for derived_table+run_id, return None."""
        from deltalake import write_deltalake

        guard_path = str(tmp_path / "guard")
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["old-token"],
                "status": ["CLAIMED"],
                "claimed_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._try_claim_pandas("meta_daily_stats", "run-1")
        assert result is None

    def test_new_claim_succeeds(self, tmp_path):
        """New claim for unclaimed run returns a token."""
        guard_path = str(tmp_path / "guard")
        # No existing data — DeltaTable will fail, triggering empty DataFrame path
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._try_claim_pandas("meta_daily_stats", "run-new")
        assert result is not None
        assert len(result) == 36  # UUID format


class TestMarkAppliedPandas:
    def test_mark_applied_updates_status(self, tmp_path):
        from deltalake import DeltaTable, write_deltalake

        guard_path = str(tmp_path / "guard")
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["my-token"],
                "status": ["CLAIMED"],
                "claimed_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        du._mark_applied_pandas("meta_daily_stats", "run-1", "my-token")

        result = DeltaTable(guard_path).to_pandas()
        assert result.iloc[0]["status"] == "APPLIED"


class TestMarkFailedPandas:
    def test_mark_failed_updates_status(self, tmp_path):
        from deltalake import DeltaTable, write_deltalake

        guard_path = str(tmp_path / "guard")
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["my-token"],
                "status": ["CLAIMED"],
                "claimed_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        du._mark_failed_pandas("meta_daily_stats", "run-1", "my-token", "something broke")

        result = DeltaTable(guard_path).to_pandas()
        assert result.iloc[0]["status"] == "FAILED"
        assert result.iloc[0]["error_message"] == "something broke"

    def test_wrong_token_raises(self, tmp_path):
        from deltalake import write_deltalake

        guard_path = str(tmp_path / "guard")
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["my-token"],
                "status": ["CLAIMED"],
                "claimed_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        with pytest.raises(ValueError, match="not found"):
            du._mark_failed_pandas("meta_daily_stats", "run-1", "wrong-token", "error")


class TestReclaimPandas:
    def _write_guard(self, guard_path, status, claimed_at=None):
        from deltalake import write_deltalake

        if claimed_at is None:
            claimed_at = datetime.now(timezone.utc)
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["old-token"],
                "status": [status],
                "claimed_at": pa.array([claimed_at], type=pa.timestamp("us", tz="UTC")),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

    def test_applied_returns_none(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        self._write_guard(guard_path, "APPLIED")
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_pandas("meta_daily_stats", "run-1", 60)
        assert result is None

    def test_failed_reclaims(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        self._write_guard(guard_path, "FAILED")
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_pandas("meta_daily_stats", "run-1", 60)
        assert result is not None  # new token

    def test_fresh_claimed_returns_none(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        self._write_guard(guard_path, "CLAIMED", datetime.now(timezone.utc))
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_pandas("meta_daily_stats", "run-1", 60)
        assert result is None  # not stale

    def test_stale_claimed_reclaims(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        stale_time = datetime.now(timezone.utc) - timedelta(hours=2)
        self._write_guard(guard_path, "CLAIMED", stale_time)
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_pandas("meta_daily_stats", "run-1", 60)
        assert result is not None

    def test_no_row_falls_back_to_try_claim(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        # Write a row for different derived_table
        self._write_guard(guard_path, "CLAIMED")
        cat = _mock_catalog()
        cat.tables["meta_derived_applied_runs"] = guard_path
        du = DerivedUpdater(cat)
        # Reclaim for a run that doesn't exist in guard table
        result = du._reclaim_for_rebuild_pandas("meta_pipeline_health", "run-2", 60)
        assert result is not None  # falls back to try_claim


# ---------------------------------------------------------------------------
# Helper: SQL Server mock catalog
# ---------------------------------------------------------------------------


def _mock_catalog_sql():
    """Create a mock CatalogManager for SQL Server mode testing."""
    cat = _mock_catalog("sql_server")
    cat.connection = MagicMock()
    cat.config = MagicMock()
    cat.config.schema_name = "test_schema"
    cat.tables.update(
        {
            "meta_daily_stats": "meta_daily_stats",
            "meta_pipeline_health": "meta_pipeline_health",
            "meta_pipeline_runs": "meta_pipeline_runs",
            "meta_sla_status": "meta_sla_status",
        }
    )
    return cat


# ---------------------------------------------------------------------------
# apply_derived_update tests
# ---------------------------------------------------------------------------


class TestApplyDerivedUpdate:
    def test_invalid_table_name_logs_error_and_returns(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.log_observability_error = MagicMock()
        du.try_claim = MagicMock()

        du.apply_derived_update("bogus_table", "run-1", lambda: None)

        du.log_observability_error.assert_called_once()
        assert "Invalid derived_table" in du.log_observability_error.call_args[0][1]
        du.try_claim.assert_not_called()

    def test_successful_claim_and_update_marks_applied(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.try_claim = MagicMock(return_value="tok-123")
        du.mark_applied = MagicMock()
        du.mark_failed = MagicMock()
        called = []

        du.apply_derived_update("meta_daily_stats", "run-1", lambda: called.append(1))

        assert called == [1]
        du.mark_applied.assert_called_once_with("meta_daily_stats", "run-1", "tok-123")
        du.mark_failed.assert_not_called()

    def test_successful_claim_failed_update_marks_failed(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.try_claim = MagicMock(return_value="tok-123")
        du.mark_applied = MagicMock()
        du.mark_failed = MagicMock()
        du.log_observability_error = MagicMock()

        du.apply_derived_update(
            "meta_daily_stats", "run-1", MagicMock(side_effect=ValueError("boom"))
        )

        du.mark_failed.assert_called_once_with("meta_daily_stats", "run-1", "tok-123", "boom")
        du.mark_applied.assert_not_called()
        du.log_observability_error.assert_called_once()

    def test_try_claim_returns_none_skips(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.try_claim = MagicMock(return_value=None)
        du.mark_applied = MagicMock()
        update_fn = MagicMock()

        du.apply_derived_update("meta_daily_stats", "run-1", update_fn)

        update_fn.assert_not_called()
        du.mark_applied.assert_not_called()

    def test_claim_lifecycle_itself_fails_logs_error(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.try_claim = MagicMock(side_effect=RuntimeError("claim exploded"))
        du.log_observability_error = MagicMock()
        update_fn = MagicMock()

        du.apply_derived_update("meta_daily_stats", "run-1", update_fn)

        update_fn.assert_not_called()
        du.log_observability_error.assert_called_once()
        assert "Claim lifecycle failed" in du.log_observability_error.call_args[0][1]


# ---------------------------------------------------------------------------
# apply_derived_updates_batch tests
# ---------------------------------------------------------------------------


class TestApplyDerivedUpdatesBatch:
    def test_empty_updates_returns_immediately(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.log_observability_error = MagicMock()
        du.apply_derived_update = MagicMock()

        du.apply_derived_updates_batch("run-1", [])

        du.apply_derived_update.assert_not_called()
        du.log_observability_error.assert_not_called()

    def test_all_invalid_table_names_returns_after_logging(self):
        cat = _mock_catalog()
        du = DerivedUpdater(cat)
        du.log_observability_error = MagicMock()
        du.apply_derived_update = MagicMock()

        du.apply_derived_updates_batch("run-1", [("bogus", lambda: None)])

        du.log_observability_error.assert_called_once()
        du.apply_derived_update.assert_not_called()

    def test_non_pandas_mode_falls_back_to_sequential(self):
        cat = _mock_catalog("sql_server")
        du = DerivedUpdater(cat)
        du.apply_derived_update = MagicMock()

        updates = [("meta_daily_stats", lambda: None)]
        du.apply_derived_updates_batch("run-1", updates)

        du.apply_derived_update.assert_called_once_with("meta_daily_stats", "run-1", updates[0][1])

    def test_pandas_mode_already_processed_skipped(self, tmp_path):
        from deltalake import write_deltalake

        guard_path = str(tmp_path / "guard")
        existing = pa.table(
            {
                "derived_table": ["meta_daily_stats"],
                "run_id": ["run-1"],
                "claim_token": ["old-token"],
                "status": ["APPLIED"],
                "claimed_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "applied_at": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
                "error_message": pa.array([None], type=pa.string()),
            }
        )
        write_deltalake(guard_path, existing)

        cat = _mock_catalog("pandas")
        cat.tables["meta_derived_applied_runs"] = guard_path
        cat.tables["meta_pipeline_runs"] = str(tmp_path / "runs")
        du = DerivedUpdater(cat)
        update_fn = MagicMock()

        du.apply_derived_updates_batch("run-1", [("meta_daily_stats", update_fn)])

        update_fn.assert_not_called()

    def test_pandas_mode_batch_claim_execute_mark(self, tmp_path):
        guard_path = str(tmp_path / "guard")
        cat = _mock_catalog("pandas")
        cat.tables["meta_derived_applied_runs"] = guard_path
        cat.tables["meta_pipeline_runs"] = str(tmp_path / "runs")
        du = DerivedUpdater(cat)

        called = []
        du.apply_derived_updates_batch(
            "run-1",
            [("meta_daily_stats", lambda: called.append("stats"))],
        )

        assert called == ["stats"]
        from deltalake import DeltaTable

        result_df = DeltaTable(guard_path).to_pandas()
        assert len(result_df) == 1
        assert result_df.iloc[0]["status"] == "APPLIED"


# ---------------------------------------------------------------------------
# SQL Server core methods
# ---------------------------------------------------------------------------


class TestGetSqlServerConnection:
    def test_no_connection_raises(self):
        cat = _mock_catalog_sql()
        cat.connection = None
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="requires connection"):
            du._get_sql_server_connection()


class TestGetSqlServerSchema:
    def test_returns_config_schema(self):
        cat = _mock_catalog_sql()
        du = DerivedUpdater(cat)
        assert du._get_sql_server_schema() == "test_schema"

    def test_returns_default_when_no_config(self):
        cat = _mock_catalog_sql()
        cat.config.schema_name = None
        du = DerivedUpdater(cat)
        assert du._get_sql_server_schema() == "odibi_system"


class TestSqlServerTableExists:
    def test_returns_true(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = [(1,)]
        du = DerivedUpdater(cat)
        assert du._sql_server_table_exists("meta_daily_stats") is True

    def test_returns_false_empty(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []
        du = DerivedUpdater(cat)
        assert du._sql_server_table_exists("meta_daily_stats") is False

    def test_returns_false_on_exception(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.side_effect = RuntimeError("db error")
        du = DerivedUpdater(cat)
        assert du._sql_server_table_exists("meta_daily_stats") is False


class TestTryClaimSqlServer:
    def test_table_doesnt_exist_raises(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []  # table_exists → False
        du = DerivedUpdater(cat)
        with pytest.raises(NotImplementedError, match="does not exist"):
            du._try_claim_sql_server("meta_daily_stats", "run-1")

    def test_successful_claim_returns_token(self):
        cat = _mock_catalog_sql()
        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # _sql_server_table_exists
                return [(1,)]
            if call_count[0] == 2:
                # MERGE (claim)
                return None
            if call_count[0] == 3:
                # verify: return the token we claimed with
                params["derived_table"]  # won't match — need to match token
                return None
            return None

        # Use side_effect to simulate table exists + MERGE + verify
        tokens_seen = []

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "MERGE" in sql:
                tokens_seen.append(params["claim_token"])
                return None
            if "SELECT claim_token" in sql:
                return [(tokens_seen[0],)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        result = du._try_claim_sql_server("meta_daily_stats", "run-1")
        assert result is not None
        assert len(result) == 36  # UUID

    def test_already_claimed_returns_none(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "MERGE" in sql:
                return None
            if "SELECT claim_token" in sql:
                return [("other-token",)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        result = du._try_claim_sql_server("meta_daily_stats", "run-1")
        assert result is None


class TestMarkAppliedSqlServer:
    def test_calls_execute_with_update(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._mark_applied_sql_server("meta_daily_stats", "run-1", "tok-123")

        calls = cat.connection.execute.call_args_list
        update_call = [c for c in calls if "UPDATE" in str(c)]
        assert len(update_call) == 1
        assert "APPLIED" in str(update_call[0])


class TestMarkFailedSqlServer:
    def test_calls_execute_with_update(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._mark_failed_sql_server("meta_daily_stats", "run-1", "tok-123", "error msg")

        calls = cat.connection.execute.call_args_list
        update_call = [c for c in calls if "UPDATE" in str(c)]
        assert len(update_call) == 1
        assert "FAILED" in str(update_call[0])


class TestReclaimForRebuildSqlServer:
    def test_applied_returns_none(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "SELECT status" in sql:
                return [("APPLIED", datetime.now(timezone.utc))]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_sql_server("meta_daily_stats", "run-1", 60)
        assert result is None

    def test_failed_reclaims(self):
        cat = _mock_catalog_sql()
        tokens_seen = []

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "SELECT status" in sql:
                return [("FAILED", datetime.now(timezone.utc))]
            if "UPDATE" in sql:
                tokens_seen.append(params["claim_token"])
                return None
            if "SELECT claim_token" in sql:
                return [(tokens_seen[0],)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_sql_server("meta_daily_stats", "run-1", 60)
        assert result is not None
        assert len(result) == 36

    def test_no_rows_falls_back_to_try_claim(self):
        cat = _mock_catalog_sql()
        tokens_seen = []

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "SELECT status" in sql:
                return []
            if "MERGE" in sql:
                tokens_seen.append(params["claim_token"])
                return None
            if "SELECT claim_token" in sql:
                return [(tokens_seen[0],)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        result = du._reclaim_for_rebuild_sql_server("meta_daily_stats", "run-1", 60)
        assert result is not None


class TestLogErrorSqlServer:
    def test_table_doesnt_exist_silently_skips(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []  # table_exists → False
        du = DerivedUpdater(cat)
        # Should NOT raise
        du._log_error_sql_server(
            "err-1", "run-1", "pipe", "comp", "msg", datetime.now(timezone.utc)
        )

        # Only the table_exists check was called
        assert cat.connection.execute.call_count == 1

    def test_successful_insert(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._log_error_sql_server(
            "err-1", "run-1", "pipe", "comp", "msg", datetime.now(timezone.utc)
        )

        calls = cat.connection.execute.call_args_list
        insert_calls = [c for c in calls if "INSERT" in str(c)]
        assert len(insert_calls) == 1


# ---------------------------------------------------------------------------
# update_daily_stats dispatch
# ---------------------------------------------------------------------------


class TestUpdateDailyStatsDispatch:
    def test_pandas_mode_dispatch(self):
        cat = _mock_catalog("pandas")
        du = DerivedUpdater(cat)
        du._update_daily_stats_pandas = MagicMock()
        run = {
            "pipeline_name": "p",
            "run_end_at": datetime.now(timezone.utc),
            "duration_ms": 100,
            "status": "SUCCESS",
            "rows_processed": 10,
        }
        du.update_daily_stats("run-1", run)
        du._update_daily_stats_pandas.assert_called_once_with("run-1", run)

    def test_sql_server_mode_dispatch(self):
        cat = _mock_catalog_sql()
        du = DerivedUpdater(cat)
        du._update_daily_stats_sql_server = MagicMock()
        run = {
            "pipeline_name": "p",
            "run_end_at": datetime.now(timezone.utc),
            "duration_ms": 100,
            "status": "SUCCESS",
        }
        du.update_daily_stats("run-1", run)
        du._update_daily_stats_sql_server.assert_called_once_with("run-1", run)

    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.update_daily_stats("run-1", {})


# ---------------------------------------------------------------------------
# update_pipeline_health dispatch
# ---------------------------------------------------------------------------


class TestUpdatePipelineHealthDispatch:
    def test_pandas_mode_dispatch(self):
        cat = _mock_catalog("pandas")
        du = DerivedUpdater(cat)
        du._update_pipeline_health_pandas = MagicMock()
        run = {"pipeline_name": "p", "status": "SUCCESS", "duration_ms": 100}
        du.update_pipeline_health(run)
        du._update_pipeline_health_pandas.assert_called_once_with(run)

    def test_sql_server_mode_dispatch(self):
        cat = _mock_catalog_sql()
        du = DerivedUpdater(cat)
        du._update_pipeline_health_sql_server = MagicMock()
        run = {"pipeline_name": "p", "status": "SUCCESS", "duration_ms": 100}
        du.update_pipeline_health(run)
        du._update_pipeline_health_sql_server.assert_called_once_with(run)

    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.update_pipeline_health({})


# ---------------------------------------------------------------------------
# update_sla_status dispatch
# ---------------------------------------------------------------------------


class TestUpdateSlaStatusDispatch:
    def test_no_freshness_sla_returns_early(self):
        cat = _mock_catalog("pandas")
        du = DerivedUpdater(cat)
        du._update_sla_status_pandas = MagicMock()
        du.update_sla_status("proj", "pipe", "owner", None, "run_completion")
        du._update_sla_status_pandas.assert_not_called()

    def test_pandas_dispatch(self):
        cat = _mock_catalog("pandas")
        du = DerivedUpdater(cat)
        du._update_sla_status_pandas = MagicMock()
        du.update_sla_status("proj", "pipe", "owner", "6h", "run_completion")
        du._update_sla_status_pandas.assert_called_once_with(
            "proj", "pipe", "owner", "6h", "run_completion"
        )

    def test_no_backend_raises(self):
        cat = _mock_catalog()
        cat.is_spark_mode = False
        cat.is_pandas_mode = False
        cat.is_sql_server_mode = False
        du = DerivedUpdater(cat)
        with pytest.raises(RuntimeError, match="No supported backend"):
            du.update_sla_status("proj", "pipe", "owner", "6h", "run_completion")


# ---------------------------------------------------------------------------
# _update_daily_stats_sql_server
# ---------------------------------------------------------------------------


class TestUpdateDailyStatsSqlServer:
    def _make_run(self, **overrides):
        run = {
            "pipeline_name": "test_pipe",
            "run_end_at": datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            "duration_ms": 5000,
            "status": "SUCCESS",
            "rows_processed": 100,
            "estimated_cost_usd": 1.5,
            "actual_cost_usd": None,
        }
        run.update(overrides)
        return run

    def test_table_doesnt_exist_raises(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []
        du = DerivedUpdater(cat)
        with pytest.raises(NotImplementedError, match="does not exist"):
            du._update_daily_stats_sql_server("run-1", self._make_run())

    def test_success_path_calls_merge(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._update_daily_stats_sql_server("run-1", self._make_run())

        calls = cat.connection.execute.call_args_list
        merge_calls = [c for c in calls if "MERGE" in str(c)]
        assert len(merge_calls) == 1

    def test_negative_duration_raises(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        with pytest.raises(ValueError, match="Invalid duration_ms"):
            du._update_daily_stats_sql_server("run-1", self._make_run(duration_ms=-1))


# ---------------------------------------------------------------------------
# _update_pipeline_health_sql_server
# ---------------------------------------------------------------------------


class TestUpdatePipelineHealthSqlServer:
    def _make_run(self, **overrides):
        run = {
            "pipeline_name": "test_pipe",
            "owner": "team-a",
            "layer": "silver",
            "status": "SUCCESS",
            "duration_ms": 5000,
            "rows_processed": 100,
            "run_end_at": datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        }
        run.update(overrides)
        return run

    def test_table_doesnt_exist_raises(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []
        du = DerivedUpdater(cat)
        with pytest.raises(NotImplementedError, match="does not exist"):
            du._update_pipeline_health_sql_server(self._make_run())

    def test_calls_merge(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._update_pipeline_health_sql_server(self._make_run())

        calls = cat.connection.execute.call_args_list
        merge_calls = [c for c in calls if "MERGE" in str(c)]
        assert len(merge_calls) >= 1

    def test_window_metrics_when_pipeline_runs_exists(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            if "WITH base" in sql:
                return [(0.9, 0.85, 3000.0, 5000)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._update_pipeline_health_sql_server(self._make_run())

        calls = cat.connection.execute.call_args_list
        update_calls = [
            c
            for c in calls
            if "UPDATE" in str(c) and "success_rate_7d" in str(c) and "MERGE" not in str(c)
        ]
        assert len(update_calls) == 1


# ---------------------------------------------------------------------------
# _update_sla_status_sql_server
# ---------------------------------------------------------------------------


class TestUpdateSlaStatusSqlServer:
    def test_table_doesnt_exist_raises(self):
        cat = _mock_catalog_sql()
        cat.connection.execute.return_value = []
        du = DerivedUpdater(cat)
        with pytest.raises(NotImplementedError, match="does not exist"):
            du._update_sla_status_sql_server("proj", "pipe", "owner", "6h", "run_completion")

    def test_invalid_sla_raises(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        with pytest.raises(ValueError, match="Invalid freshness_sla"):
            du._update_sla_status_sql_server("proj", "pipe", "owner", "abc", "run_completion")

    def test_calls_merge(self):
        cat = _mock_catalog_sql()

        def smart_execute(sql, params=None):
            if "sys.tables" in sql:
                return [(1,)]
            return None

        cat.connection.execute.side_effect = smart_execute
        du = DerivedUpdater(cat)
        du._update_sla_status_sql_server("proj", "pipe", "owner", "6h", "run_completion")

        calls = cat.connection.execute.call_args_list
        merge_calls = [c for c in calls if "MERGE" in str(c)]
        assert len(merge_calls) == 1


# ---------------------------------------------------------------------------
# Pandas-mode updater helpers
# ---------------------------------------------------------------------------


def _make_updater(tmp_path):
    catalog = MagicMock()
    catalog.is_spark_mode = False
    catalog.is_pandas_mode = True
    catalog.is_sql_server_mode = False
    catalog._get_storage_options.return_value = None
    catalog.tables = {
        "meta_daily_stats": str(tmp_path / "daily_stats"),
        "meta_pipeline_health": str(tmp_path / "health"),
        "meta_sla_status": str(tmp_path / "sla"),
        "meta_pipeline_runs": str(tmp_path / "runs"),
        "meta_derived_applied_runs": str(tmp_path / "guard"),
        "meta_observability_errors": str(tmp_path / "errors"),
    }
    return DerivedUpdater(catalog)


def _make_runs_df(rows):
    """Build a runs DataFrame from a list of dicts (fills in defaults)."""
    defaults = {
        "pipeline_name": "pipe_a",
        "run_end_at": datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc),
        "status": "SUCCESS",
        "duration_ms": 5000,
        "rows_processed": 100,
        "estimated_cost_usd": None,
        "actual_cost_usd": None,
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows)


# ---------------------------------------------------------------------------
# _update_daily_stats_pandas
# ---------------------------------------------------------------------------


class TestUpdateDailyStatsPandas:
    """Tests for DerivedUpdater._update_daily_stats_pandas."""

    def _make_pipeline_run(self, **overrides):
        run = {
            "pipeline_name": "pipe_a",
            "run_end_at": datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc),
            "status": "SUCCESS",
            "duration_ms": 5000,
            "rows_processed": 100,
            "estimated_cost_usd": None,
            "actual_cost_usd": None,
        }
        run.update(overrides)
        return run

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_basic_success_run(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"status": "SUCCESS"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run())

        mock_write.assert_called_once()
        written = mock_write.call_args[0][1]
        tbl = written.to_pandas()
        assert len(tbl) == 1
        row = tbl.iloc[0]
        assert row["pipeline_name"] == "pipe_a"
        assert row["runs"] == 1
        assert row["successes"] == 1
        assert row["failures"] == 0
        assert row["cost_source"] == "none"

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_failure_run(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"status": "FAILURE"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run(status="FAILURE"))

        written = mock_write.call_args[0][1].to_pandas()
        assert written.iloc[0]["failures"] == 1
        assert written.iloc[0]["successes"] == 0

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_existing_daily_stats_row_replaced(self, mock_dt_cls, mock_write, tmp_path):
        existing = pd.DataFrame(
            [
                {
                    "date": datetime(2024, 6, 15).date(),
                    "pipeline_name": "pipe_a",
                    "runs": 5,
                    "successes": 4,
                    "failures": 1,
                    "total_rows": 500,
                    "total_duration_ms": 25000,
                    "estimated_cost_usd": None,
                    "actual_cost_usd": None,
                    "cost_source": "none",
                    "cost_is_actual": 0,
                }
            ]
        )
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = existing
        mock_dt_cls.return_value = mock_dt_instance

        runs_df = _make_runs_df([{"status": "SUCCESS"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        pipe_rows = written[written["pipeline_name"] == "pipe_a"]
        assert len(pipe_rows) == 1
        assert pipe_rows.iloc[0]["runs"] == 1

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_actual_cost_usd_sets_databricks_billing(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"actual_cost_usd": 2.5, "estimated_cost_usd": 1.0}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run(actual_cost_usd=2.5))

        written = mock_write.call_args[0][1].to_pandas()
        assert written.iloc[0]["cost_source"] == "databricks_billing"

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_estimated_cost_only_sets_configured_rate(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"estimated_cost_usd": 1.5, "actual_cost_usd": None}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run(estimated_cost_usd=1.5))

        written = mock_write.call_args[0][1].to_pandas()
        assert written.iloc[0]["cost_source"] == "configured_rate"

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_no_costs_sets_none(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"estimated_cost_usd": None, "actual_cost_usd": None}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        assert written.iloc[0]["cost_source"] == "none"

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_empty_runs_df_returns_early(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run())

        mock_write.assert_not_called()

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_uses_runs_df_cache(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        cached = _make_runs_df([{"status": "SUCCESS"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = cached

        du._update_daily_stats_pandas("run-1", self._make_pipeline_run())

        mock_write.assert_called_once()

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_run_end_at_as_string(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"status": "SUCCESS"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        run = self._make_pipeline_run(run_end_at="2024-06-15T12:00:00+00:00")
        du._update_daily_stats_pandas("run-1", run)

        mock_write.assert_called_once()
        written = mock_write.call_args[0][1].to_pandas()
        assert written.iloc[0]["pipeline_name"] == "pipe_a"

    @patch("odibi.derived_updater.write_deltalake", None)
    @patch("odibi.derived_updater.DeltaTable", None)
    def test_import_error_when_lib_none(self, tmp_path):
        du = _make_updater(tmp_path)
        with pytest.raises(ImportError, match="deltalake library required"):
            du._update_daily_stats_pandas("run-1", self._make_pipeline_run())


# ---------------------------------------------------------------------------
# _update_pipeline_health_pandas
# ---------------------------------------------------------------------------


class TestUpdatePipelineHealthPandas:
    """Tests for DerivedUpdater._update_pipeline_health_pandas."""

    def _make_pipeline_run(self, **overrides):
        run = {
            "pipeline_name": "pipe_a",
            "owner": "team-x",
            "layer": "silver",
            "status": "SUCCESS",
            "duration_ms": 5000,
            "rows_processed": 100,
            "run_end_at": datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc),
        }
        run.update(overrides)
        return run

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_new_pipeline_success(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        du._update_pipeline_health_pandas(self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        assert len(written) == 1
        row = written.iloc[0]
        assert row["pipeline_name"] == "pipe_a"
        assert row["total_runs"] == 1
        assert row["total_successes"] == 1
        assert row["total_failures"] == 0

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_existing_pipeline_increments(self, mock_dt_cls, mock_write, tmp_path):
        existing = pd.DataFrame(
            [
                {
                    "pipeline_name": "pipe_a",
                    "owner": "team-x",
                    "layer": "silver",
                    "total_runs": 10,
                    "total_successes": 8,
                    "total_failures": 2,
                    "success_rate_7d": 0.9,
                    "success_rate_30d": 0.85,
                    "avg_duration_ms_7d": 3000.0,
                    "total_rows_30d": 5000,
                    "estimated_cost_30d": None,
                    "last_success_at": datetime(2024, 6, 14, 12, 0, tzinfo=timezone.utc),
                    "last_failure_at": datetime(2024, 6, 13, 12, 0, tzinfo=timezone.utc),
                    "last_run_at": datetime(2024, 6, 14, 12, 0, tzinfo=timezone.utc),
                    "updated_at": datetime(2024, 6, 14, 12, 0, tzinfo=timezone.utc),
                }
            ]
        )
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = existing
        mock_dt_cls.return_value = mock_dt_instance

        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        du._update_pipeline_health_pandas(self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        row = written[written["pipeline_name"] == "pipe_a"].iloc[0]
        assert row["total_runs"] == 11
        assert row["total_successes"] == 9
        assert row["total_failures"] == 2

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_failure_status_increments_failures(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        du._update_pipeline_health_pandas(self._make_pipeline_run(status="FAILURE"))

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["total_failures"] == 1
        assert row["total_successes"] == 0
        assert row["last_failure_at"] is not None

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_window_metrics(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        now = datetime.now(timezone.utc)
        runs_df = _make_runs_df(
            [
                {
                    "run_end_at": now - timedelta(days=2),
                    "status": "SUCCESS",
                    "duration_ms": 1000,
                    "rows_processed": 50,
                },
                {
                    "run_end_at": now - timedelta(days=5),
                    "status": "FAILURE",
                    "duration_ms": 2000,
                    "rows_processed": 30,
                },
                {
                    "run_end_at": now - timedelta(days=20),
                    "status": "SUCCESS",
                    "duration_ms": 3000,
                    "rows_processed": 80,
                },
            ]
        )
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_pipeline_health_pandas(self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["success_rate_7d"] == pytest.approx(0.5)
        assert row["success_rate_30d"] == pytest.approx(2 / 3)
        assert row["avg_duration_ms_7d"] == pytest.approx(1500.0)

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_empty_runs_df_window_metrics_none(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        du._update_pipeline_health_pandas(self._make_pipeline_run())

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["success_rate_7d"] is None
        assert row["success_rate_30d"] is None
        assert row["avg_duration_ms_7d"] is None

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_uses_runs_df_cache(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        cached = _make_runs_df([{"status": "SUCCESS"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = cached

        du._update_pipeline_health_pandas(self._make_pipeline_run())

        mock_write.assert_called_once()

    @patch("odibi.derived_updater.write_deltalake", None)
    @patch("odibi.derived_updater.DeltaTable", None)
    def test_import_error_when_lib_none(self, tmp_path):
        du = _make_updater(tmp_path)
        with pytest.raises(ImportError, match="deltalake library required"):
            du._update_pipeline_health_pandas(self._make_pipeline_run())


# ---------------------------------------------------------------------------
# _update_sla_status_pandas
# ---------------------------------------------------------------------------


class TestUpdateSlaStatusPandas:
    """Tests for DerivedUpdater._update_sla_status_pandas."""

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_sla_met(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        now = datetime.now(timezone.utc)
        runs_df = _make_runs_df(
            [
                {
                    "status": "SUCCESS",
                    "run_end_at": now - timedelta(minutes=30),
                }
            ]
        )
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["sla_met"] == 1
        assert row["hours_overdue"] is None

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_sla_violated(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        now = datetime.now(timezone.utc)
        runs_df = _make_runs_df(
            [
                {
                    "status": "SUCCESS",
                    "run_end_at": now - timedelta(hours=10),
                }
            ]
        )
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["sla_met"] == 0
        assert row["hours_overdue"] is not None
        assert row["hours_overdue"] > 0

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_no_success_runs(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        runs_df = _make_runs_df([{"status": "FAILURE"}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")

        written = mock_write.call_args[0][1].to_pandas()
        row = written.iloc[0]
        assert row["sla_met"] == 0
        assert row["last_success_at"] is None

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_existing_sla_row_replaced(self, mock_dt_cls, mock_write, tmp_path):
        now = datetime.now(timezone.utc)
        existing = pd.DataFrame(
            [
                {
                    "project_name": "proj",
                    "pipeline_name": "pipe_a",
                    "owner": "owner",
                    "freshness_sla": "6h",
                    "freshness_anchor": "run_completion",
                    "freshness_sla_minutes": 360,
                    "last_success_at": now - timedelta(hours=1),
                    "minutes_since_success": 60,
                    "sla_met": 1,
                    "hours_overdue": None,
                    "updated_at": now - timedelta(hours=1),
                }
            ]
        )
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = existing
        mock_dt_cls.return_value = mock_dt_instance

        runs_df = _make_runs_df([{"status": "SUCCESS", "run_end_at": now - timedelta(minutes=10)}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = runs_df

        du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")

        written = mock_write.call_args[0][1].to_pandas()
        pipe_rows = written[
            (written["project_name"] == "proj") & (written["pipeline_name"] == "pipe_a")
        ]
        assert len(pipe_rows) == 1

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_invalid_sla_string_raises(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        du = _make_updater(tmp_path)
        du._runs_df_cache = pd.DataFrame()

        with pytest.raises(ValueError, match="Invalid freshness_sla"):
            du._update_sla_status_pandas("proj", "pipe_a", "owner", "abc", "run_completion")

    @patch("odibi.derived_updater.write_deltalake")
    @patch("odibi.derived_updater.DeltaTable")
    def test_uses_runs_df_cache(self, mock_dt_cls, mock_write, tmp_path):
        mock_dt_cls.side_effect = Exception("empty")
        now = datetime.now(timezone.utc)
        cached = _make_runs_df([{"status": "SUCCESS", "run_end_at": now - timedelta(minutes=5)}])
        du = _make_updater(tmp_path)
        du._runs_df_cache = cached

        du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")

        mock_write.assert_called_once()

    @patch("odibi.derived_updater.write_deltalake", None)
    @patch("odibi.derived_updater.DeltaTable", None)
    def test_import_error_when_lib_none(self, tmp_path):
        du = _make_updater(tmp_path)
        with pytest.raises(ImportError, match="deltalake library required"):
            du._update_sla_status_pandas("proj", "pipe_a", "owner", "6h", "run_completion")
