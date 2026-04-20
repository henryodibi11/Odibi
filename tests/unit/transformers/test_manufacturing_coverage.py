"""Coverage tests for odibi.transformers.manufacturing — Polars paths (lines 1022-1350)."""

import logging
from datetime import datetime, timedelta

import pytest

pl = pytest.importorskip("polars")

from odibi.context import EngineContext, PolarsContext
from odibi.enums import EngineType
from odibi.transformers.manufacturing import (
    DetectSequentialPhasesParams,
    PhaseConfig,
    _calculate_status_times_polars,
    _detect_phases_polars,
    _detect_single_phase_polars,
    _extract_metadata_polars,
    _fill_null_numeric_columns_polars,
    _get_expected_columns_polars,
    detect_sequential_phases,
)

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE = datetime(2024, 1, 1, 8, 0, 0)


def _ts(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


def _make_params(**overrides):
    defaults = dict(
        group_by="batch_id",
        timestamp_col="timestamp",
        phases=["timer1"],
        start_threshold=240,
    )
    defaults.update(overrides)
    return DetectSequentialPhasesParams(**defaults)


def _sample_df():
    """Single-batch, single-phase timer that ramps then plateaus."""
    return pl.DataFrame(
        {
            "batch_id": ["B1"] * 8,
            "timestamp": [_ts(i * 5) for i in range(8)],
            # timer: 0→60→120→180→240→240→0→0  (plateau at idx 4-5)
            "timer1": [0, 60, 120, 180, 240, 240, 0, 0],
        }
    )


# ===========================================================================
# _detect_single_phase_polars
# ===========================================================================


class TestDetectSinglePhasePolars:
    def test_basic_phase_detection(self):
        df = _sample_df()
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is not None
        assert "timer1_start" in result["columns"]
        assert "timer1_end" in result["columns"]
        assert result["columns"]["timer1_max_minutes"] > 0

    def test_no_plateau_uses_last_ts(self):
        """When timer never repeats, end_time = last timestamp."""
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 4,
                "timestamp": [_ts(i * 5) for i in range(4)],
                "timer1": [0, 60, 120, 180],
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is not None
        assert result["end_time"] == _ts(15)

    def test_no_valid_starts_returns_none(self):
        """All timer values exceed threshold → no start candidates."""
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 3,
                "timestamp": [_ts(i) for i in range(3)],
                "timer1": [500, 600, 700],
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is None

    def test_empty_data_returns_none(self):
        df = pl.DataFrame(
            {
                "batch_id": pl.Series([], dtype=pl.Utf8),
                "timestamp": pl.Series([], dtype=pl.Datetime),
                "timer1": pl.Series([], dtype=pl.Int64),
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is None

    def test_all_zeros_returns_none(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 3,
                "timestamp": [_ts(i) for i in range(3)],
                "timer1": [0, 0, 0],
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is None

    def test_with_previous_phase_end(self):
        df = _sample_df()
        # Set previous_phase_end to after the plateau → no data left
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=_ts(999),
            status_col=None,
            status_mapping=None,
            phase_metrics=None,
        )
        assert result is None

    def test_with_status_tracking(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 6,
                "timestamp": [_ts(i * 5) for i in range(6)],
                "timer1": [0, 60, 120, 180, 240, 240],
                "status": [1, 1, 2, 2, 2, 1],
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col="status",
            status_mapping={1: "idle", 2: "active"},
            phase_metrics=None,
        )
        assert result is not None
        cols = result["columns"]
        assert "timer1_idle_minutes" in cols
        assert "timer1_active_minutes" in cols

    def test_with_phase_metrics(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 6,
                "timestamp": [_ts(i * 5) for i in range(6)],
                "timer1": [0, 60, 120, 180, 240, 240],
                "temperature": [100.0, 110.0, 120.0, 130.0, 140.0, 145.0],
            }
        )
        result = _detect_single_phase_polars(
            group=df,
            timer_col="timer1",
            timestamp_col="timestamp",
            threshold=240,
            previous_phase_end=None,
            status_col=None,
            status_mapping=None,
            phase_metrics={"temperature": "max"},
        )
        assert result is not None
        assert result["columns"]["timer1_temperature"] >= 100.0

    def test_phase_metrics_all_agg_funcs(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 5,
                "timestamp": [_ts(i * 5) for i in range(5)],
                "timer1": [0, 60, 120, 180, 180],
                "val": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        for agg in ("mean", "sum", "max", "min", "std", "count"):
            result = _detect_single_phase_polars(
                group=df,
                timer_col="timer1",
                timestamp_col="timestamp",
                threshold=240,
                previous_phase_end=None,
                status_col=None,
                status_mapping=None,
                phase_metrics={"val": agg},
            )
            assert result is not None
            assert "timer1_val" in result["columns"]


# ===========================================================================
# _calculate_status_times_polars
# ===========================================================================


class TestCalculateStatusTimesPolars:
    def test_status_transitions(self):
        df = pl.DataFrame(
            {
                "timestamp": [_ts(0), _ts(10), _ts(20), _ts(30)],
                "status": [1, 2, 2, 1],
            }
        )
        result = _calculate_status_times_polars(
            group=df,
            start_time=_ts(0),
            end_time=_ts(30),
            timestamp_col="timestamp",
            status_col="status",
            status_mapping={1: "idle", 2: "active"},
        )
        assert result["idle"] > 0
        assert result["active"] > 0
        assert pytest.approx(result["idle"] + result["active"], abs=0.01) == 30.0

    def test_no_valid_statuses(self):
        df = pl.DataFrame(
            {
                "timestamp": [_ts(0), _ts(10)],
                "status": [99, 99],
            }
        )
        result = _calculate_status_times_polars(
            group=df,
            start_time=_ts(0),
            end_time=_ts(10),
            timestamp_col="timestamp",
            status_col="status",
            status_mapping={1: "idle", 2: "active"},
        )
        assert result["idle"] == 0.0
        assert result["active"] == 0.0

    def test_single_status_throughout(self):
        df = pl.DataFrame(
            {
                "timestamp": [_ts(0), _ts(10), _ts(20)],
                "status": [1, 1, 1],
            }
        )
        result = _calculate_status_times_polars(
            group=df,
            start_time=_ts(0),
            end_time=_ts(20),
            timestamp_col="timestamp",
            status_col="status",
            status_mapping={1: "running"},
        )
        assert result["running"] == pytest.approx(20.0, abs=0.01)

    def test_empty_window(self):
        df = pl.DataFrame(
            {
                "timestamp": pl.Series([], dtype=pl.Datetime),
                "status": pl.Series([], dtype=pl.Int64),
            }
        )
        result = _calculate_status_times_polars(
            group=df,
            start_time=_ts(0),
            end_time=_ts(10),
            timestamp_col="timestamp",
            status_col="status",
            status_mapping={1: "idle"},
        )
        assert result["idle"] == 0.0


# ===========================================================================
# _extract_metadata_polars
# ===========================================================================


class TestExtractMetadataPolars:
    def _group(self):
        return pl.DataFrame(
            {
                "timestamp": [_ts(0), _ts(10), _ts(20)],
                "product": ["A", "B", "C"],
                "weight": [10.0, 20.0, 30.0],
            }
        )

    def test_first_method(self):
        result = _extract_metadata_polars(self._group(), {"product": "first"}, "timestamp", _ts(0))
        assert result["product"] == "A"

    def test_last_method(self):
        result = _extract_metadata_polars(self._group(), {"product": "last"}, "timestamp", _ts(0))
        assert result["product"] == "C"

    def test_first_after_start(self):
        result = _extract_metadata_polars(
            self._group(), {"product": "first_after_start"}, "timestamp", _ts(10)
        )
        assert result["product"] == "B"

    def test_max_min_mean_sum(self):
        result = _extract_metadata_polars(self._group(), {"weight": "max"}, "timestamp", _ts(0))
        assert result["weight"] == 30.0

        result = _extract_metadata_polars(self._group(), {"weight": "min"}, "timestamp", _ts(0))
        assert result["weight"] == 10.0

        result = _extract_metadata_polars(self._group(), {"weight": "mean"}, "timestamp", _ts(0))
        assert result["weight"] == pytest.approx(20.0)

        result = _extract_metadata_polars(self._group(), {"weight": "sum"}, "timestamp", _ts(0))
        assert result["weight"] == 60.0

    def test_unknown_method_fallback(self):
        result = _extract_metadata_polars(
            self._group(), {"product": "custom_unknown"}, "timestamp", _ts(0)
        )
        assert result["product"] == "A"

    def test_missing_column(self):
        result = _extract_metadata_polars(
            self._group(), {"nonexistent": "first"}, "timestamp", _ts(0)
        )
        assert result["nonexistent"] is None


# ===========================================================================
# _get_expected_columns_polars & _fill_null_numeric_columns_polars
# ===========================================================================


class TestPolarsHelpers:
    def test_get_expected_columns(self):
        params = _make_params(
            phases=["timer1"],
            status_mapping={1: "idle", 2: "active"},
            phase_metrics={"temp": "max"},
            metadata={"product": "first"},
        )
        cols = _get_expected_columns_polars(params)
        assert "timer1_start" in cols
        assert "timer1_end" in cols
        assert "timer1_max_minutes" in cols
        assert "timer1_idle_minutes" in cols
        assert "timer1_active_minutes" in cols
        assert "timer1_temp" in cols
        assert "product" in cols

    def test_fill_null_numeric_columns(self):
        params = _make_params(phases=["timer1"])
        df = pl.DataFrame(
            {
                "timer1_max_minutes": [1.0, None, 3.0],
                "other": ["a", "b", "c"],
            }
        )
        result = _fill_null_numeric_columns_polars(df, params)
        assert result["timer1_max_minutes"].to_list() == [1.0, 0.0, 3.0]


# ===========================================================================
# _detect_phases_polars (full integration)
# ===========================================================================


class TestDetectPhasesPolars:
    def test_basic_single_phase(self):
        df = _sample_df()
        params = _make_params()
        result = _detect_phases_polars(df, params)
        assert len(result) == 1
        assert "timer1_start" in result.columns
        assert "timer1_end" in result.columns
        assert result["timer1_max_minutes"][0] > 0

    def test_multi_phase_sequential(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 12,
                "timestamp": [_ts(i * 5) for i in range(12)],
                "timer1": [0, 60, 120, 180, 180, 0, 0, 0, 0, 0, 0, 0],
                "timer2": [0, 0, 0, 0, 0, 0, 60, 120, 180, 240, 240, 0],
            }
        )
        params = _make_params(phases=["timer1", "timer2"])
        result = _detect_phases_polars(df, params)
        assert len(result) == 1
        assert "timer1_start" in result.columns
        assert "timer2_start" in result.columns

    def test_with_status_mapping(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 6,
                "timestamp": [_ts(i * 5) for i in range(6)],
                "timer1": [0, 60, 120, 180, 180, 0],
                "status": [1, 1, 2, 2, 1, 1],
            }
        )
        params = _make_params(
            status_col="status",
            status_mapping={1: "idle", 2: "active"},
        )
        result = _detect_phases_polars(df, params)
        assert "timer1_idle_minutes" in result.columns
        assert "timer1_active_minutes" in result.columns

    def test_with_metadata(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 6,
                "timestamp": [_ts(i * 5) for i in range(6)],
                "timer1": [0, 60, 120, 180, 180, 0],
                "product": ["X", "X", "X", "X", "X", "X"],
            }
        )
        params = _make_params(metadata={"product": "first"})
        result = _detect_phases_polars(df, params)
        assert "product" in result.columns
        assert result["product"][0] == "X"

    def test_fill_null_minutes(self):
        df = _sample_df()
        params = _make_params(fill_null_minutes=True)
        result = _detect_phases_polars(df, params)
        # max_minutes should not be null
        assert result["timer1_max_minutes"][0] is not None

    def test_empty_result(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 3,
                "timestamp": [_ts(i) for i in range(3)],
                "timer1": [0, 0, 0],
            }
        )
        params = _make_params()
        result = _detect_phases_polars(df, params)
        # Should still have a row for the group but with null phases
        assert len(result) == 1

    def test_multiple_groups(self):
        df = pl.DataFrame(
            {
                "batch_id": ["B1"] * 5 + ["B2"] * 5,
                "timestamp": [_ts(i * 5) for i in range(5)] + [_ts(i * 5) for i in range(5)],
                "timer1": [0, 60, 120, 120, 0] * 2,
            }
        )
        params = _make_params()
        result = _detect_phases_polars(df, params)
        assert len(result) == 2

    def test_phase_config_objects(self):
        df = _sample_df()
        params = _make_params(
            phases=[PhaseConfig(timer_col="timer1", start_threshold=300)],
        )
        result = _detect_phases_polars(df, params)
        assert len(result) == 1
        assert result["timer1_max_minutes"][0] > 0


# ===========================================================================
# detect_sequential_phases — Polars engine dispatch
# ===========================================================================


class TestDetectSequentialPhasesPolarsEngine:
    def test_polars_engine_dispatch(self):
        df = _sample_df()
        ctx = PolarsContext()
        engine_ctx = EngineContext(ctx, df, EngineType.POLARS)
        params = _make_params()
        result = detect_sequential_phases(engine_ctx, params)
        assert len(result.df) == 1
        assert "timer1_start" in result.df.columns
