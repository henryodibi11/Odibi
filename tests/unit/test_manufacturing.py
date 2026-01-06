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
