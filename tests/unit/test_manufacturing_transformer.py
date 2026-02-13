"""Tests for odibi.transformers.manufacturing — detect_sequential_phases."""

from datetime import datetime, timedelta

import pandas as pd
import pytest

try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

from odibi.context import EngineContext, PandasContext

if pl is not None:
    from odibi.context import PolarsContext
from odibi.enums import EngineType
from odibi.transformers.manufacturing import (
    DetectSequentialPhasesParams,
    PhaseConfig,
    _fill_null_numeric_columns,
    _get_expected_columns,
    _get_numeric_columns,
    _normalize_group_by,
    detect_sequential_phases,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TS = datetime(2024, 6, 1, 8, 0, 0)


def _ts(offset_sec: int) -> datetime:
    return BASE_TS + timedelta(seconds=offset_sec)


def _make_pandas_context(df: pd.DataFrame) -> EngineContext:
    ctx = PandasContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.PANDAS)


def _make_polars_context(df: "pl.DataFrame") -> EngineContext:
    ctx = PolarsContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.POLARS)


# ---------------------------------------------------------------------------
# _normalize_group_by
# ---------------------------------------------------------------------------


class TestNormalizeGroupBy:
    def test_string_input(self):
        assert _normalize_group_by("BatchID") == ["BatchID"]

    def test_list_input(self):
        assert _normalize_group_by(["BatchID", "AssetID"]) == ["BatchID", "AssetID"]


# ---------------------------------------------------------------------------
# _get_expected_columns
# ---------------------------------------------------------------------------


class TestGetExpectedColumns:
    def test_basic(self):
        params = DetectSequentialPhasesParams(group_by="B", phases=["LoadTime", "CookTime"])
        cols = _get_expected_columns(params)
        assert "LoadTime_start" in cols
        assert "LoadTime_end" in cols
        assert "LoadTime_max_minutes" in cols
        assert "CookTime_start" in cols

    def test_with_status_mapping(self):
        params = DetectSequentialPhasesParams(
            group_by="B",
            phases=["P1"],
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )
        cols = _get_expected_columns(params)
        assert "P1_idle_minutes" in cols
        assert "P1_active_minutes" in cols

    def test_with_phase_metrics(self):
        params = DetectSequentialPhasesParams(
            group_by="B", phases=["P1"], phase_metrics={"Level": "max"}
        )
        cols = _get_expected_columns(params)
        assert "P1_Level" in cols

    def test_with_metadata(self):
        params = DetectSequentialPhasesParams(
            group_by="B", phases=["P1"], metadata={"Product": "first"}
        )
        cols = _get_expected_columns(params)
        assert "Product" in cols


# ---------------------------------------------------------------------------
# _get_numeric_columns
# ---------------------------------------------------------------------------


class TestGetNumericColumns:
    def test_basic(self):
        params = DetectSequentialPhasesParams(group_by="B", phases=["P1"])
        assert _get_numeric_columns(params) == ["P1_max_minutes"]

    def test_with_status_mapping(self):
        params = DetectSequentialPhasesParams(
            group_by="B",
            phases=["P1"],
            status_col="S",
            status_mapping={1: "idle"},
        )
        nums = _get_numeric_columns(params)
        assert "P1_max_minutes" in nums
        assert "P1_idle_minutes" in nums

    def test_with_phase_metrics(self):
        params = DetectSequentialPhasesParams(
            group_by="B", phases=["P1"], phase_metrics={"Temp": "mean"}
        )
        nums = _get_numeric_columns(params)
        assert "P1_Temp" in nums

    def test_combined(self):
        params = DetectSequentialPhasesParams(
            group_by="B",
            phases=["P1"],
            status_col="S",
            status_mapping={1: "idle"},
            phase_metrics={"Temp": "mean"},
        )
        nums = _get_numeric_columns(params)
        assert set(nums) == {"P1_max_minutes", "P1_idle_minutes", "P1_Temp"}


# ---------------------------------------------------------------------------
# _fill_null_numeric_columns
# ---------------------------------------------------------------------------


class TestFillNullNumericColumns:
    def test_fills_nan(self):
        params = DetectSequentialPhasesParams(group_by="B", phases=["P1"])
        df = pd.DataFrame({"P1_max_minutes": [None, 2.5]})
        result = _fill_null_numeric_columns(df, params)
        assert result["P1_max_minutes"].iloc[0] == 0.0
        assert result["P1_max_minutes"].iloc[1] == 2.5


# ---------------------------------------------------------------------------
# Shared test-data builders
# ---------------------------------------------------------------------------


def _two_phase_rows(batch_id: str = "B1"):
    """
    Generate rows for two sequential phases: LoadTime then CookTime.

    LoadTime ramps 0 → 120 → 240 → 240 (plateau at t=180s)
    CookTime ramps 0 → 60 → 120 → 120 (plateau at t=300s)
    Rows are spaced 60 s apart.
    """
    rows = []
    for i, offset in enumerate(range(0, 360, 60)):
        load = [0, 60, 120, 240, 240, 240][i]
        cook = [0, 0, 0, 0, 60, 120][i]
        # Add a duplicate cook value to trigger plateau in phase 2
        rows.append(
            {
                "BatchID": batch_id,
                "ts": _ts(offset),
                "LoadTime": load,
                "CookTime": cook,
            }
        )
    # Add one more row so CookTime plateaus
    rows.append(
        {
            "BatchID": batch_id,
            "ts": _ts(360),
            "LoadTime": 240,
            "CookTime": 120,
        }
    )
    return rows


def _two_phase_pandas_df(batch_id: str = "B1") -> pd.DataFrame:
    return pd.DataFrame(_two_phase_rows(batch_id))


def _two_phase_polars_df(batch_id: str = "B1") -> "pl.DataFrame":
    rows = _two_phase_rows(batch_id)
    return pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))


# ---------------------------------------------------------------------------
# detect_sequential_phases — PANDAS
# ---------------------------------------------------------------------------


class TestDetectSequentialPhasesPandas:
    @pytest.fixture()
    def base_params(self):
        return DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

    def test_single_group_two_phases(self, base_params):
        df = _two_phase_pandas_df()
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, base_params).df

        assert len(result) == 1
        row = result.iloc[0]
        assert row["BatchID"] == "B1"
        # LoadTime should have non-null start/end
        assert pd.notna(row["LoadTime_start"])
        assert pd.notna(row["LoadTime_end"])
        assert row["LoadTime_max_minutes"] > 0
        # CookTime should also be detected
        assert pd.notna(row["CookTime_start"])
        assert pd.notna(row["CookTime_end"])
        assert row["CookTime_max_minutes"] > 0
        # CookTime starts after LoadTime ends
        assert row["CookTime_start"] >= row["LoadTime_end"]

    def test_status_tracking(self):
        rows = _two_phase_rows()
        for r in rows:
            r["Status"] = 2  # all active
        # Flip first row to idle
        rows[0]["Status"] = 1

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        assert "LoadTime_idle_minutes" in result.columns
        assert "LoadTime_active_minutes" in result.columns
        row = result.iloc[0]
        total = row["LoadTime_idle_minutes"] + row["LoadTime_active_minutes"]
        assert total > 0

    def test_phase_metrics_max_mean(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Level"] = float(10 + i * 5)  # 10, 15, 20, 25, 30, 35, 40

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            phase_metrics={"Level": "max"},
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        assert "LoadTime_Level" in result.columns
        assert result.iloc[0]["LoadTime_Level"] > 0

    def test_metadata_extraction(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Product"] = "PROD_A"
            r["Weight"] = float(100 + i)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={
                "Product": "first",
                "Weight": "max",
            },
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        assert result.iloc[0]["Product"] == "PROD_A"
        assert result.iloc[0]["Weight"] == max(100.0 + i for i in range(len(rows)))

    def test_metadata_all_methods(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Code"] = f"C{i}"
            r["Val"] = float(10 + i)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={
                "Code": "first_after_start",
                "Val": "min",
            },
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        assert result.iloc[0]["Code"] is not None
        assert result.iloc[0]["Val"] == 10.0

    def test_metadata_last_and_sum(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Tag"] = f"T{i}"
            r["Qty"] = float(i + 1)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={"Tag": "last", "Qty": "sum"},
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        assert result.iloc[0]["Tag"] == f"T{len(rows) - 1}"
        assert result.iloc[0]["Qty"] == sum(float(i + 1) for i in range(len(rows)))

    def test_multi_group(self, base_params):
        rows_b1 = _two_phase_rows("B1")
        rows_b2 = _two_phase_rows("B2")
        # Shift B2 timestamps forward so they don't clash
        for r in rows_b2:
            r["ts"] = r["ts"] + timedelta(hours=1)

        df = pd.DataFrame(rows_b1 + rows_b2)
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, base_params).df

        assert len(result) == 2
        assert set(result["BatchID"].tolist()) == {"B1", "B2"}

    def test_fill_null_minutes(self):
        # Only LoadTime has data; CookTime timer stays 0 → phase won't detect
        rows = []
        for i, offset in enumerate(range(0, 240, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 120][i],
                    "CookTime": 0,
                }
            )

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
            fill_null_minutes=True,
        )
        ctx = _make_pandas_context(pd.DataFrame(rows))
        result = detect_sequential_phases(ctx, params).df

        row = result.iloc[0]
        assert row["CookTime_max_minutes"] == 0.0
        # LoadTime should still have real value
        assert row["LoadTime_max_minutes"] > 0


# ---------------------------------------------------------------------------
# detect_sequential_phases — POLARS
# ---------------------------------------------------------------------------


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestDetectSequentialPhasesPolars:
    @pytest.fixture()
    def base_params(self):
        return DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

    def test_single_group_two_phases(self, base_params):
        df = _two_phase_polars_df()
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, base_params).df

        assert result.height == 1
        row = result.row(0, named=True)
        assert row["BatchID"] == "B1"
        assert row["LoadTime_start"] is not None
        assert row["LoadTime_end"] is not None
        assert row["LoadTime_max_minutes"] > 0
        assert row["CookTime_start"] is not None
        assert row["CookTime_end"] is not None
        assert row["CookTime_max_minutes"] > 0

    def test_phase_no_data(self):
        """Phase timer is always 0 → no phase detected, values are None."""
        rows = []
        for i, offset in enumerate(range(0, 240, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 120][i],
                    "EmptyPhase": 0,
                }
            )
        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "EmptyPhase"],
            start_threshold=240,
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert row["EmptyPhase_start"] is None
        assert row["EmptyPhase_max_minutes"] is None

    def test_status_transitions(self):
        """Test status tracking with transitions within a phase."""
        rows = []
        statuses = [1, 2, 2, 2, 2, 1, 1]
        for i, offset in enumerate(range(0, 420, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 180, 240, 240, 240][i],
                    "Status": statuses[i],
                }
            )
        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert "LoadTime_idle_minutes" in result.columns
        assert "LoadTime_active_minutes" in result.columns
        total = row["LoadTime_idle_minutes"] + row["LoadTime_active_minutes"]
        assert total > 0

    def test_metadata_all_methods(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Code"] = f"C{i}"
            r["Val"] = float(10 + i)
            r["Tag"] = f"T{i}"
            r["Qty"] = float(i + 1)

        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={
                "Code": "first",
                "Tag": "last",
                "Val": "max",
                "Qty": "sum",
            },
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert row["Code"] == "C0"
        assert row["Tag"] == f"T{len(rows) - 1}"
        assert row["Val"] == max(10.0 + i for i in range(len(rows)))
        assert row["Qty"] == sum(float(i + 1) for i in range(len(rows)))

    def test_metadata_first_after_start_and_min(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Sensor"] = f"S{i}"
            r["Pressure"] = float(50 - i)

        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={"Sensor": "first_after_start", "Pressure": "min"},
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert row["Sensor"] is not None
        assert row["Pressure"] == min(50.0 - i for i in range(len(rows)))

    def test_metadata_mean(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Temp"] = float(20 + i)

        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={"Temp": "mean"},
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        expected_mean = sum(20.0 + i for i in range(len(rows))) / len(rows)
        assert abs(row["Temp"] - expected_mean) < 0.01

    def test_fill_null_numeric(self):
        rows = []
        for i, offset in enumerate(range(0, 240, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 120][i],
                    "CookTime": 0,
                }
            )
        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
            fill_null_minutes=True,
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert row["CookTime_max_minutes"] == 0.0
        assert row["LoadTime_max_minutes"] > 0

    def test_phase_metrics(self):
        rows = _two_phase_rows()
        for i, r in enumerate(rows):
            r["Level"] = float(10 + i * 5)

        df = pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            phase_metrics={"Level": "max"},
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        assert "LoadTime_Level" in result.columns
        row = result.row(0, named=True)
        assert row["LoadTime_Level"] > 0


# ---------------------------------------------------------------------------
# PhaseConfig with per-phase start_threshold
# ---------------------------------------------------------------------------


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestPhaseConfigThreshold:
    def test_per_phase_threshold_pandas(self):
        """A tight threshold on LoadTime should still detect the phase
        when the first non-zero reading is within that threshold."""
        rows = _two_phase_rows()
        df = pd.DataFrame(rows)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=[
                PhaseConfig(timer_col="LoadTime", start_threshold=60),
                "CookTime",
            ],
            start_threshold=240,
        )
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.iloc[0]
        # LoadTime's first non-zero value is 60, threshold is 60 → should detect
        assert pd.notna(row["LoadTime_start"])

    def test_per_phase_threshold_polars(self):
        df = _two_phase_polars_df()

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=[
                PhaseConfig(timer_col="LoadTime", start_threshold=60),
                "CookTime",
            ],
            start_threshold=240,
        )
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        row = result.row(0, named=True)
        assert row["LoadTime_start"] is not None


# ---------------------------------------------------------------------------
# group_by as list of multiple columns
# ---------------------------------------------------------------------------


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestMultiColumnGroupBy:
    def _build_df(self, engine: str):
        rows = []
        for i, offset in enumerate(range(0, 240, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "AssetID": "A1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 120][i],
                }
            )
        if engine == "polars":
            return pl.DataFrame(rows).with_columns(pl.col("ts").cast(pl.Datetime))
        return pd.DataFrame(rows)

    @pytest.fixture()
    def params(self):
        return DetectSequentialPhasesParams(
            group_by=["BatchID", "AssetID"],
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

    def test_multi_group_by_pandas(self, params):
        df = self._build_df("pandas")
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df

        assert "BatchID" in result.columns
        assert "AssetID" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["BatchID"] == "B1"
        assert result.iloc[0]["AssetID"] == "A1"

    def test_multi_group_by_polars(self, params):
        df = self._build_df("polars")
        ctx = _make_polars_context(df)
        result = detect_sequential_phases(ctx, params).df

        assert "BatchID" in result.columns
        assert "AssetID" in result.columns
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["BatchID"] == "B1"
        assert row["AssetID"] == "A1"
