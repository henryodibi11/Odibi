"""
Tests for manufacturing transformers.
"""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.manufacturing import (
    DetectSequentialPhasesParams,
    detect_sequential_phases,
)


@pytest.fixture
def sample_batch_data():
    """
    Sample manufacturing data simulating batch process with two phases.

    BatchID B001:
    - LoadTime phase: starts ~10:00:15, ends ~10:05:00 (timer plateaus at 285)
    - CookTime phase: starts ~10:06:00, ends ~10:15:00 (timer plateaus at 540)
    - Status changes: idle(1) -> active(2) -> hold(3) -> active(2)
    """
    data = {
        "ts": pd.to_datetime(
            [
                "2024-01-01 10:00:00",
                "2024-01-01 10:01:00",
                "2024-01-01 10:02:00",
                "2024-01-01 10:03:00",
                "2024-01-01 10:04:00",
                "2024-01-01 10:05:00",  # LoadTime plateaus
                "2024-01-01 10:06:00",
                "2024-01-01 10:07:00",
                "2024-01-01 10:10:00",
                "2024-01-01 10:15:00",  # CookTime plateaus
                "2024-01-01 10:16:00",
            ]
        ),
        "BatchID": ["B001"] * 11,
        "LoadTime": [0, 45, 105, 165, 225, 285, 285, 285, 285, 285, 285],
        "CookTime": [0, 0, 0, 0, 0, 0, 60, 120, 300, 540, 540],
        "Status": [1, 1, 2, 2, 2, 2, 2, 3, 2, 2, 1],
        "Level": [10, 15, 25, 40, 55, 70, 72, 75, 80, 85, 50],
        "Weight": [100, 100, 102, 105, 108, 110, 110, 110, 110, 110, 110],
        "ProductCode": ["PROD-A"] * 11,
    }
    return pd.DataFrame(data)


@pytest.fixture
def multi_batch_data(sample_batch_data):
    """Two batches to test grouping."""
    batch2 = sample_batch_data.copy()
    batch2["BatchID"] = "B002"
    batch2["ts"] = batch2["ts"] + pd.Timedelta(hours=2)
    batch2["ProductCode"] = "PROD-B"
    return pd.concat([sample_batch_data, batch2], ignore_index=True)


class TestDetectSequentialPhases:
    """Tests for detect_sequential_phases transformer."""

    def test_basic_phase_detection(self, sample_batch_data):
        """Test that phases are detected correctly."""
        context = EngineContext(PandasContext(), sample_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        assert len(df) == 1
        assert df.iloc[0]["BatchID"] == "B001"

        assert "LoadTime_start" in df.columns
        assert "LoadTime_end" in df.columns
        assert "LoadTime_max_minutes" in df.columns

        assert "CookTime_start" in df.columns
        assert "CookTime_end" in df.columns
        assert "CookTime_max_minutes" in df.columns

        assert df.iloc[0]["LoadTime_max_minutes"] == pytest.approx(285 / 60, rel=0.01)
        assert df.iloc[0]["CookTime_max_minutes"] == pytest.approx(540 / 60, rel=0.01)

    def test_status_time_calculation(self, sample_batch_data):
        """Test that time in each status is calculated correctly."""
        context = EngineContext(PandasContext(), sample_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active", 3: "hold"},
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        assert "LoadTime_idle_minutes" in df.columns
        assert "LoadTime_active_minutes" in df.columns
        assert "LoadTime_hold_minutes" in df.columns

        assert df.iloc[0]["LoadTime_active_minutes"] > 0

    def test_phase_metrics(self, sample_batch_data):
        """Test that phase metrics are aggregated correctly."""
        context = EngineContext(PandasContext(), sample_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            phase_metrics={"Level": "max"},
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        assert "LoadTime_Level" in df.columns
        assert df.iloc[0]["LoadTime_Level"] == 70

    def test_metadata_extraction(self, sample_batch_data):
        """Test metadata column extraction."""
        context = EngineContext(PandasContext(), sample_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={
                "ProductCode": "first_after_start",
                "Weight": "max",
            },
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        assert df.iloc[0]["ProductCode"] == "PROD-A"
        assert df.iloc[0]["Weight"] == 110

    def test_multi_batch_grouping(self, multi_batch_data):
        """Test that multiple batches are processed correctly."""
        context = EngineContext(PandasContext(), multi_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        assert len(df) == 2
        assert set(df["BatchID"].tolist()) == {"B001", "B002"}

    def test_sequential_chaining(self, sample_batch_data):
        """Test that phases are chained sequentially."""
        context = EngineContext(PandasContext(), sample_batch_data, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        df = result.df

        load_end = pd.to_datetime(df.iloc[0]["LoadTime_end"])
        cook_start = pd.to_datetime(df.iloc[0]["CookTime_start"])

        assert cook_start >= load_end

    def test_empty_phase_skipped(self):
        """Test that phases with no data are skipped gracefully."""
        data = {
            "ts": pd.to_datetime(["2024-01-01 10:00:00", "2024-01-01 10:01:00"]),
            "BatchID": ["B001", "B001"],
            "LoadTime": [0, 0],
            "CookTime": [60, 120],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert "LoadTime_start" not in result_df.columns or pd.isna(
            result_df.iloc[0].get("LoadTime_start")
        )
        assert "CookTime_start" in result_df.columns

    def test_nan_status_handled(self):
        """Test that NaN status values don't cause errors."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                ]
            ),
            "BatchID": ["B001"] * 4,
            "LoadTime": [0, 60, 120, 120],
            "Status": [None, 2, None, 2],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )

        result = detect_sequential_phases(context, params)
        assert len(result.df) == 1
        assert "LoadTime_active_minutes" in result.df.columns

    def test_multi_column_groupby(self):
        """Test grouping by multiple columns."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001", "B001", "B001", "B001", "B001", "B001"],
            "AssetID": ["A1", "A1", "A1", "A2", "A2", "A2"],
            "LoadTime": [0, 60, 120, 0, 45, 90],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by=["BatchID", "AssetID"],
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 2
        assert "BatchID" in result_df.columns
        assert "AssetID" in result_df.columns
        assert set(result_df["AssetID"].tolist()) == {"A1", "A2"}

    def test_skipped_phase_returns_all_columns_with_nulls(self):
        """
        Test that skipped phases still produce all expected columns with null values.

        This is critical for Spark applyInPandas which requires the returned DataFrame
        to have all columns defined in the schema.
        """
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001", "B001", "B001"],
            "LoadTime": [0, 0, 0],
            "CookTime": [60, 120, 180],
            "CoolTime": [0, 0, 0],
            "Status": [1, 2, 2],
            "Level": [10, 20, 30],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime", "CoolTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
            phase_metrics={"Level": "max"},
            metadata={"BatchID": "first"},
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1

        for phase in ["LoadTime", "CookTime", "CoolTime"]:
            assert f"{phase}_start" in result_df.columns
            assert f"{phase}_end" in result_df.columns
            assert f"{phase}_max_minutes" in result_df.columns
            assert f"{phase}_idle_minutes" in result_df.columns
            assert f"{phase}_active_minutes" in result_df.columns
            assert f"{phase}_Level" in result_df.columns

        assert pd.isna(result_df.iloc[0]["LoadTime_start"])
        assert pd.isna(result_df.iloc[0]["LoadTime_max_minutes"])
        assert pd.isna(result_df.iloc[0]["CoolTime_start"])
        assert pd.isna(result_df.iloc[0]["CoolTime_max_minutes"])

        assert pd.notna(result_df.iloc[0]["CookTime_start"])
        assert result_df.iloc[0]["CookTime_max_minutes"] == pytest.approx(180 / 60, rel=0.01)

    def test_duplicate_timestamps_do_not_cause_false_plateau(self):
        """
        Test that duplicate rows per timestamp don't trigger false plateau detection.

        Real PLC data often has multiple readings per timestamp. Plateau should only
        be detected when timer values are equal across DIFFERENT timestamps.
        """
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                    "2024-01-01 10:03:00",
                ]
            ),
            "BatchID": ["B001"] * 8,
            "LoadTime": [60, 60, 120, 120, 180, 180, 180, 180],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert result_df.iloc[0]["LoadTime_max_minutes"] == pytest.approx(180 / 60, rel=0.01)
        assert result_df.iloc[0]["LoadTime_end"] == pd.Timestamp("2024-01-01 10:02:00")

    def test_fill_null_minutes_replaces_nulls_with_zero(self):
        """
        Test that fill_null_minutes=True replaces null numeric columns with 0.

        Timestamp columns should remain null for skipped phases.
        """
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001", "B001", "B001"],
            "LoadTime": [0, 0, 0],
            "CookTime": [60, 120, 180],
            "Status": [1, 2, 2],
            "Level": [10, 20, 30],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
            phase_metrics={"Level": "max"},
            fill_null_minutes=True,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert result_df.iloc[0]["LoadTime_max_minutes"] == 0
        assert result_df.iloc[0]["LoadTime_idle_minutes"] == 0
        assert result_df.iloc[0]["LoadTime_active_minutes"] == 0
        assert result_df.iloc[0]["LoadTime_Level"] == 0

        assert pd.isna(result_df.iloc[0]["LoadTime_start"])
        assert pd.isna(result_df.iloc[0]["LoadTime_end"])

        assert result_df.iloc[0]["CookTime_max_minutes"] > 0


class TestManufacturingEdgeCases:
    """Tests for manufacturing-specific edge cases and error conditions."""

    def test_negative_timer_values(self):
        """Test that negative timer values are handled gracefully."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                ]
            ),
            "BatchID": ["B001"] * 4,
            "LoadTime": [-5, 60, 120, 180],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert "LoadTime_max_minutes" in result_df.columns
        assert result_df.iloc[0]["LoadTime_max_minutes"] == pytest.approx(180 / 60, rel=0.01)

    def test_timer_reset_non_monotonic(self):
        """Test handling of timer resets (non-monotonic timer values).
        
        When a timer resets mid-phase, the phase detects the last plateau value.
        """
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                    "2024-01-01 10:04:00",
                ]
            ),
            "BatchID": ["B001"] * 5,
            "LoadTime": [60, 120, 180, 60, 120],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        # After reset, the plateau is detected at 120 seconds
        assert result_df.iloc[0]["LoadTime_max_minutes"] == pytest.approx(120 / 60, rel=0.01)
        assert pd.notna(result_df.iloc[0]["LoadTime_start"])
        assert pd.notna(result_df.iloc[0]["LoadTime_end"])

    def test_very_large_timer_values(self):
        """Test handling of very large timer values (days/weeks scale)."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-02 10:00:00",
                    "2024-01-03 10:00:00",
                ]
            ),
            "BatchID": ["B001"] * 3,
            "CureTime": [86400, 172800, 259200],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["CureTime"],
            start_threshold=300000,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert result_df.iloc[0]["CureTime_max_minutes"] == pytest.approx(259200 / 60, rel=0.01)
        assert result_df.iloc[0]["CureTime_max_minutes"] == pytest.approx(4320, rel=0.01)

    def test_all_zero_timer_phases(self):
        """Test batch with all-zero timer values (no activity)."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001"] * 3,
            "LoadTime": [0, 0, 0],
            "CookTime": [0, 0, 0],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert pd.isna(result_df.iloc[0].get("LoadTime_start"))
        assert pd.isna(result_df.iloc[0].get("CookTime_start"))

    def test_single_row_batch(self):
        """Test batch with only a single data point."""
        data = {
            "ts": pd.to_datetime(["2024-01-01 10:00:00"]),
            "BatchID": ["B001"],
            "LoadTime": [180],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert "LoadTime_start" in result_df.columns

    def test_empty_dataframe(self):
        """Test handling of empty input DataFrame."""
        df = pd.DataFrame(
            {
                "ts": pd.to_datetime([]),
                "BatchID": [],
                "LoadTime": [],
            }
        )
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 0

    def test_threshold_boundary_cases(self):
        """Test timer values exactly at and just over start_threshold."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                ]
            ),
            "BatchID": ["B001"] * 4,
            "LoadTime": [240, 300, 360, 420],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert pd.notna(result_df.iloc[0]["LoadTime_start"])

    def test_single_phase_no_status_or_metrics(self):
        """Test minimal configuration with single phase, no status or metrics."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001"] * 3,
            "LoadTime": [60, 120, 180],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert "LoadTime_start" in result_df.columns
        assert "LoadTime_end" in result_df.columns
        assert "LoadTime_max_minutes" in result_df.columns
        assert result_df.iloc[0]["LoadTime_max_minutes"] == pytest.approx(180 / 60, rel=0.01)

    def test_extreme_status_codes(self):
        """Test handling of unusual status code values."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                    "2024-01-01 10:03:00",
                ]
            ),
            "BatchID": ["B001"] * 4,
            "LoadTime": [60, 120, 180, 240],
            "Status": [999, 0, -1, 100],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={999: "extreme_high", 0: "zero", -1: "negative", 100: "high"},
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert "LoadTime_extreme_high_minutes" in result_df.columns
        assert "LoadTime_zero_minutes" in result_df.columns
        assert "LoadTime_negative_minutes" in result_df.columns
        assert "LoadTime_high_minutes" in result_df.columns

    def test_metadata_with_all_nulls(self):
        """Test metadata extraction when all values are null."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:01:00",
                    "2024-01-01 10:02:00",
                ]
            ),
            "BatchID": ["B001"] * 3,
            "LoadTime": [60, 120, 180],
            "ProductCode": [None, None, None],
            "Weight": [None, None, None],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={
                "ProductCode": "first_after_start",
                "Weight": "max",
            },
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert pd.isna(result_df.iloc[0]["ProductCode"])
        assert pd.isna(result_df.iloc[0]["Weight"])

    def test_mixed_timer_scales(self):
        """Test phases with vastly different timer scales (seconds vs hours).
        
        When the first reading of LongPhase is above threshold, it's filtered out.
        """
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 10:10:00",
                    "2024-01-01 12:00:00",
                    "2024-01-01 14:00:00",
                ]
            ),
            "BatchID": ["B001"] * 5,
            "QuickPhase": [60, 120, 180, 180, 180],
            "LongPhase": [0, 0, 0, 60, 120],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["QuickPhase", "LongPhase"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert result_df.iloc[0]["QuickPhase_max_minutes"] == pytest.approx(180 / 60, rel=0.01)
        assert result_df.iloc[0]["LongPhase_max_minutes"] == pytest.approx(120 / 60, rel=0.01)


class TestOEECalculations:
    """
    Tests for OEE-related calculations using detect_sequential_phases.
    
    Note: The manufacturing module doesn't have dedicated OEE functions,
    but detect_sequential_phases provides the building blocks for OEE:
    - Availability: time_in_active_status / total_time
    - Performance: actual_cycle_time / ideal_cycle_time
    - Quality: good_units / total_units (requires external data)
    """

    def test_availability_calculation_from_status_times(self):
        """Test that status times can be used to calculate availability."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 10:10:00",
                    "2024-01-01 10:15:00",
                    "2024-01-01 10:20:00",
                    "2024-01-01 10:25:00",
                ]
            ),
            "BatchID": ["B001"] * 6,
            "CycleTime": [60, 300, 600, 900, 1200, 1500],
            "Status": [1, 2, 2, 3, 2, 2],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["CycleTime"],
            start_threshold=240,
            status_col="Status",
            status_mapping={1: "idle", 2: "running", 3: "fault"},
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        running_time = result_df.iloc[0]["CycleTime_running_minutes"]
        fault_time = result_df.iloc[0]["CycleTime_fault_minutes"]
        idle_time = result_df.iloc[0]["CycleTime_idle_minutes"]
        
        # All status times should be present
        assert pd.notna(running_time)
        assert pd.notna(fault_time)
        assert pd.notna(idle_time)
        
        total_time = running_time + fault_time + idle_time
        availability = running_time / total_time if total_time > 0 else 0
        assert 0 <= availability <= 1
        assert running_time > 0
        assert fault_time > 0

    def test_cycle_time_detection_multiple_cycles(self):
        """Test detecting multiple complete cycles in a single batch."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 10:10:00",
                ]
            ),
            "BatchID": ["B001"] * 3,
            "Cycle1": [60, 300, 300],
            "Cycle2": [0, 0, 0],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["Cycle1", "Cycle2"],
            start_threshold=240,
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        assert result_df.iloc[0]["Cycle1_max_minutes"] == pytest.approx(300 / 60, rel=0.01)

    def test_yield_rate_supporting_data(self):
        """Test that metadata can capture yield-related data."""
        data = {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 10:10:00",
                    "2024-01-01 10:15:00",
                    "2024-01-01 10:20:00",
                ]
            ),
            "BatchID": ["B001"] * 5,
            "ProcessTime": [60, 300, 600, 900, 900],
            "InputWeight": [1000, 1000, 1000, 1000, 1000],
            "OutputWeight": [0, 0, 950, 950, 950],
            "RejectCount": [0, 0, 2, 5, 5],
        }
        df = pd.DataFrame(data)
        context = EngineContext(PandasContext(), df, EngineType.PANDAS)

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["ProcessTime"],
            start_threshold=240,
            metadata={
                "InputWeight": "first_after_start",
                "OutputWeight": "max",
                "RejectCount": "max",
            },
        )

        result = detect_sequential_phases(context, params)
        result_df = result.df

        assert len(result_df) == 1
        input_weight = result_df.iloc[0]["InputWeight"]
        output_weight = result_df.iloc[0]["OutputWeight"]
        reject_count = result_df.iloc[0]["RejectCount"]

        assert input_weight == 1000
        assert output_weight == 950
        assert reject_count == 5
        yield_rate = output_weight / input_weight if input_weight > 0 else 0
        assert 0 <= yield_rate <= 1
        assert yield_rate == pytest.approx(0.95, rel=0.01)
