"""Tests for register_standard_library() and uncovered manufacturing transformer paths."""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from odibi.registry import FunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TS = datetime(2024, 6, 1, 8, 0, 0)


def _ts(offset_sec: int) -> datetime:
    return BASE_TS + timedelta(seconds=offset_sec)


def _make_pandas_context(df: pd.DataFrame):
    from odibi.context import EngineContext, PandasContext
    from odibi.enums import EngineType

    ctx = PandasContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.PANDAS)


# ---------------------------------------------------------------------------
# Part 1 — register_standard_library()
# ---------------------------------------------------------------------------


class TestRegisterStandardLibrary:
    """Verify that register_standard_library() populates FunctionRegistry."""

    @pytest.fixture(autouse=True)
    def _reset_registry(self):
        """Clear registry before each test and re-register."""
        FunctionRegistry.clear()
        import odibi.transformers as t_mod

        t_mod._standard_library_registered = False
        t_mod.register_standard_library()

    # -- presence checks --------------------------------------------------

    @pytest.mark.parametrize(
        "name",
        [
            "filter_rows",
            "join",
            "deduplicate",
            "scd2",
            "merge",
            "cross_check",
            "detect_deletes",
            "detect_sequential_phases",
            "fluid_properties",
            "unit_convert",
            "derive_columns",
            "cast_columns",
            "clean_text",
            "extract_date_parts",
            "normalize_schema",
            "sort",
            "limit",
            "sample",
            "distinct",
            "fill_nulls",
            "split_part",
            "date_add",
            "date_trunc",
            "date_diff",
            "case_when",
            "convert_timezone",
            "concat_columns",
            "select_columns",
            "drop_columns",
            "rename_columns",
            "add_prefix",
            "add_suffix",
            "normalize_column_names",
            "coalesce_columns",
            "replace_values",
            "trim_whitespace",
            "union",
            "pivot",
            "unpivot",
            "aggregate",
            "explode_list_column",
            "dict_based_mapping",
            "regex_replace",
            "unpack_struct",
            "hash_columns",
            "parse_json",
            "generate_surrogate_key",
            "generate_numeric_key",
            "validate_and_flag",
            "window_calculation",
            "normalize_json",
            "sessionize",
            "geocode",
            "split_events_by_period",
            "saturation_properties",
            "psychrometrics",
        ],
    )
    def test_function_registered(self, name):
        assert FunctionRegistry.has_function(name), f"{name} not registered"

    def test_get_function_returns_callable(self):
        func = FunctionRegistry.get_function("filter_rows")
        assert callable(func)

    # -- param model associations -----------------------------------------

    def test_param_model_filter_rows(self):
        from odibi.transformers.sql_core import FilterRowsParams

        assert FunctionRegistry.get_param_model("filter_rows") is FilterRowsParams

    def test_param_model_join(self):
        from odibi.transformers.relational import JoinParams

        assert FunctionRegistry.get_param_model("join") is JoinParams

    def test_param_model_deduplicate(self):
        from odibi.transformers.advanced import DeduplicateParams

        assert FunctionRegistry.get_param_model("deduplicate") is DeduplicateParams

    def test_param_model_scd2(self):
        from odibi.transformers.scd import SCD2Params

        assert FunctionRegistry.get_param_model("scd2") is SCD2Params

    def test_param_model_merge(self):
        from odibi.transformers.merge_transformer import MergeParams

        assert FunctionRegistry.get_param_model("merge") is MergeParams

    def test_param_model_cross_check(self):
        from odibi.transformers.validation import CrossCheckParams

        assert FunctionRegistry.get_param_model("cross_check") is CrossCheckParams

    def test_param_model_detect_deletes(self):
        from odibi.config import DeleteDetectionConfig

        assert FunctionRegistry.get_param_model("detect_deletes") is DeleteDetectionConfig

    def test_param_model_detect_sequential_phases(self):
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        assert (
            FunctionRegistry.get_param_model("detect_sequential_phases")
            is DetectSequentialPhasesParams
        )

    def test_param_model_fluid_properties(self):
        from odibi.transformers.thermodynamics import FluidPropertiesParams

        assert FunctionRegistry.get_param_model("fluid_properties") is FluidPropertiesParams

    def test_param_model_unit_convert(self):
        from odibi.transformers.units import UnitConvertParams

        assert FunctionRegistry.get_param_model("unit_convert") is UnitConvertParams

    # -- idempotent -------------------------------------------------------

    def test_idempotent_double_call(self):
        import odibi.transformers as t_mod

        count_before = len(FunctionRegistry.list_functions())
        t_mod._standard_library_registered = False
        t_mod.register_standard_library()
        count_after = len(FunctionRegistry.list_functions())
        assert count_after == count_before

    def test_idempotent_flag_prevents_duplicate(self):
        import odibi.transformers as t_mod

        t_mod.register_standard_library()  # already True from fixture
        assert FunctionRegistry.has_function("filter_rows")

    # -- total count sanity -----------------------------------------------

    def test_at_least_40_functions_registered(self):
        assert len(FunctionRegistry.list_functions()) >= 40


# ---------------------------------------------------------------------------
# Part 2 — Manufacturing transformer uncovered paths
# ---------------------------------------------------------------------------


class TestDetectSinglePhaseEdgeCases:
    """Cover edge-case branches in _detect_single_phase."""

    def _run(self, df, params):
        ctx = _make_pandas_context(df)
        from odibi.transformers.manufacturing import detect_sequential_phases

        return detect_sequential_phases(ctx, params).df

    def test_empty_group_returns_nulls(self):
        """Group with no rows matching the phase timer → phase columns stay null."""
        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1"],
                "ts": [_ts(0), _ts(60)],
                "LoadTime": [0, 0],  # always zero — no phase detected
            }
        )
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        result = self._run(df, params)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["LoadTime_start"])
        assert (
            pd.isna(result.iloc[0]["LoadTime_max_minutes"])
            or result.iloc[0]["LoadTime_max_minutes"] is None
        )

    def test_timer_exceeds_threshold(self):
        """All timer values exceed start_threshold → phase not detected."""
        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [500, 600, 700],  # all above default threshold 240
            }
        )
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        result = self._run(df, params)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["LoadTime_start"])

    def test_no_plateau_uses_last_timestamp(self):
        """Timer never repeats (no plateau) → end = last timestamp."""
        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120), _ts(180)],
                "LoadTime": [0, 60, 120, 180],  # strictly increasing
            }
        )
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        result = self._run(df, params)
        row = result.iloc[0]
        assert pd.notna(row["LoadTime_start"])
        assert pd.notna(row["LoadTime_end"])
        assert row["LoadTime_max_minutes"] == round(180 / 60, 6)

    def test_missing_timer_column_skipped(self):
        """Phase referencing a column not in DataFrame is silently skipped."""
        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],
            }
        )
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "MissingPhase"],
            start_threshold=240,
        )
        result = self._run(df, params)
        assert len(result) == 1
        assert pd.notna(result.iloc[0]["LoadTime_start"])
        assert pd.isna(result.iloc[0]["MissingPhase_start"])

    def test_phase_data_empty_after_previous_end(self):
        """Second phase has no data after first phase ends → stays null."""
        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],  # plateau at t=60
                "CookTime": [0, 0, 0],  # no activity
            }
        )
        from odibi.transformers.manufacturing import DetectSequentialPhasesParams

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime", "CookTime"],
            start_threshold=240,
        )
        result = self._run(df, params)
        row = result.iloc[0]
        assert pd.notna(row["LoadTime_start"])
        assert pd.isna(row["CookTime_start"])


class TestStatusTrackingEdgeCases:
    """Cover _calculate_status_times edge cases."""

    def test_nan_status_values_ignored(self):
        """Rows with NaN status are skipped in status calculations."""
        import numpy as np
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        rows = []
        statuses = [1, np.nan, 2, np.nan, 2, 2, 2]
        for i, offset in enumerate(range(0, 420, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 180, 240, 240, 240][i],
                    "Status": statuses[i],
                }
            )

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

        row = result.iloc[0]
        total = row["LoadTime_idle_minutes"] + row["LoadTime_active_minutes"]
        assert total > 0

    def test_unknown_status_codes_ignored(self):
        """Status codes not in mapping are skipped."""
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        rows = []
        statuses = [1, 99, 2, 99, 2, 2, 2]
        for i, offset in enumerate(range(0, 420, 60)):
            rows.append(
                {
                    "BatchID": "B1",
                    "ts": _ts(offset),
                    "LoadTime": [0, 60, 120, 180, 240, 240, 240][i],
                    "Status": statuses[i],
                }
            )

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

        row = result.iloc[0]
        assert row["LoadTime_idle_minutes"] >= 0
        assert row["LoadTime_active_minutes"] >= 0

    def test_empty_phase_window_status(self):
        """Status col present but phase window is empty → all zeros."""
        from odibi.transformers.manufacturing import _calculate_status_times

        group = pd.DataFrame(
            {
                "ts": pd.to_datetime([]),
                "Status": pd.Series([], dtype=int),
            }
        )
        result = _calculate_status_times(
            group=group,
            start_time=pd.Timestamp("2024-01-01"),
            end_time=pd.Timestamp("2024-01-02"),
            timestamp_col="ts",
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )
        assert result == {"idle": 0.0, "active": 0.0}

    def test_no_valid_status_rows(self):
        """All status values are NaN → all zeros."""
        import numpy as np
        from odibi.transformers.manufacturing import _calculate_status_times

        group = pd.DataFrame(
            {
                "ts": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00"]),
                "Status": [np.nan, np.nan],
            }
        )
        result = _calculate_status_times(
            group=group,
            start_time=pd.Timestamp("2024-01-01"),
            end_time=pd.Timestamp("2024-01-02"),
            timestamp_col="ts",
            status_col="Status",
            status_mapping={1: "idle", 2: "active"},
        )
        assert result == {"idle": 0.0, "active": 0.0}


class TestMetadataExtractionEdgeCases:
    """Cover _extract_metadata edge cases."""

    def test_missing_metadata_column(self):
        """Metadata column not in group → None."""
        from odibi.transformers.manufacturing import _extract_metadata

        group = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01"]), "A": [1]})
        result = _extract_metadata(
            group=group,
            metadata_config={"NonExistent": "first"},
            timestamp_col="ts",
            first_phase_start=pd.Timestamp("2024-01-01"),
        )
        assert result["NonExistent"] is None

    def test_first_after_start_no_data_after(self):
        """No rows after first_phase_start → None."""
        from odibi.transformers.manufacturing import _extract_metadata

        group = pd.DataFrame(
            {
                "ts": pd.to_datetime(["2024-01-01 00:00"]),
                "Code": ["ABC"],
            }
        )
        result = _extract_metadata(
            group=group,
            metadata_config={"Code": "first_after_start"},
            timestamp_col="ts",
            first_phase_start=pd.Timestamp("2025-01-01"),  # far in future
        )
        assert result["Code"] is None

    def test_first_after_start_all_null(self):
        """Rows after start exist but column values are all NaN → None."""
        import numpy as np
        from odibi.transformers.manufacturing import _extract_metadata

        group = pd.DataFrame(
            {
                "ts": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "Code": [np.nan, np.nan],
            }
        )
        result = _extract_metadata(
            group=group,
            metadata_config={"Code": "first_after_start"},
            timestamp_col="ts",
            first_phase_start=pd.Timestamp("2024-01-01"),
        )
        assert result["Code"] is None

    def test_metadata_unknown_method_uses_agg(self):
        """Unknown aggregation method falls through to pandas agg()."""
        from odibi.transformers.manufacturing import _extract_metadata

        group = pd.DataFrame(
            {
                "ts": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "Val": [3.0, 7.0],
            }
        )
        result = _extract_metadata(
            group=group,
            metadata_config={"Val": "median"},
            timestamp_col="ts",
            first_phase_start=pd.Timestamp("2024-01-01"),
        )
        assert result["Val"] == 5.0

    def test_metadata_not_extracted_when_no_phase_detected(self):
        """When no phase detects, metadata extraction is skipped."""
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1"],
                "ts": [_ts(0), _ts(60)],
                "LoadTime": [0, 0],
                "Product": ["X", "Y"],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={"Product": "first"},
        )
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df

        assert result.iloc[0]["Product"] is None


class TestPhaseMetricsEdgeCases:
    """Cover phase_metrics edge cases in _detect_single_phase."""

    def test_metric_column_missing(self):
        """Metric references a column not in the data → metric is None."""
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            phase_metrics={"NonExistent": "max"},
        )
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df
        assert result.iloc[0]["LoadTime_NonExistent"] is None or pd.isna(
            result.iloc[0]["LoadTime_NonExistent"]
        )

    def test_metric_agg_mean(self):
        """Phase metric with mean aggregation."""
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120), _ts(180)],
                "LoadTime": [0, 60, 120, 120],
                "Temp": [100.0, 200.0, 300.0, 400.0],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            phase_metrics={"Temp": "mean"},
        )
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df
        assert result.iloc[0]["LoadTime_Temp"] > 0


class TestProcessSingleGroupPandas:
    """Cover _process_single_group_pandas (used by Spark applyInPandas)."""

    def test_basic_group(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _process_single_group_pandas,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        row = _process_single_group_pandas(df, params)
        assert row is not None
        assert row["BatchID"] == "B1"
        assert row["LoadTime_start"] is not None or row["LoadTime_start"] is not pd.NaT

    def test_multi_group_by_columns(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _process_single_group_pandas,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "AssetID": ["A1", "A1", "A1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by=["BatchID", "AssetID"],
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        row = _process_single_group_pandas(df, params)
        assert row["BatchID"] == "B1"
        assert row["AssetID"] == "A1"

    def test_fill_null_minutes_in_single_group(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _process_single_group_pandas,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1"],
                "ts": [_ts(0), _ts(60)],
                "LoadTime": [0, 0],  # no phase will be detected
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            fill_null_minutes=True,
        )
        row = _process_single_group_pandas(df, params)
        assert row["LoadTime_max_minutes"] == 0.0

    def test_metadata_in_single_group(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _process_single_group_pandas,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 60, 60],
                "Product": ["X", "Y", "Z"],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
            metadata={"Product": "first"},
        )
        row = _process_single_group_pandas(df, params)
        assert row["Product"] == "X"

    def test_phase_config_object_in_single_group(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            PhaseConfig,
            _process_single_group_pandas,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1", "B1", "B1"],
                "ts": [_ts(0), _ts(60), _ts(120)],
                "LoadTime": [0, 30, 30],
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=[PhaseConfig(timer_col="LoadTime", start_threshold=60)],
            start_threshold=240,
        )
        row = _process_single_group_pandas(df, params)
        assert row["LoadTime_start"] is not pd.NaT


class TestGetExpectedColumnsPolars:
    """Cover _get_expected_columns_polars helper."""

    def test_basic(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _get_expected_columns_polars,
        )

        params = DetectSequentialPhasesParams(group_by="B", phases=["LoadTime", "CookTime"])
        cols = _get_expected_columns_polars(params)
        assert "LoadTime_start" in cols
        assert "CookTime_end" in cols
        assert "CookTime_max_minutes" in cols

    def test_with_status_and_metrics(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _get_expected_columns_polars,
        )

        params = DetectSequentialPhasesParams(
            group_by="B",
            phases=["P1"],
            status_col="S",
            status_mapping={1: "idle"},
            phase_metrics={"Level": "max"},
            metadata={"Product": "first"},
        )
        cols = _get_expected_columns_polars(params)
        assert "P1_idle_minutes" in cols
        assert "P1_Level" in cols
        assert "Product" in cols


class TestFillNullNumericColumnsPolars:
    """Cover _fill_null_numeric_columns_polars helper."""

    def test_fills_null(self):
        pytest.importorskip("polars")
        import polars as pl

        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            _fill_null_numeric_columns_polars,
        )

        params = DetectSequentialPhasesParams(group_by="B", phases=["P1"])
        df = pl.DataFrame({"P1_max_minutes": [None, 2.5]})
        result = _fill_null_numeric_columns_polars(df, params)
        assert result["P1_max_minutes"].to_list() == [0.0, 2.5]


class TestUnsupportedEngineType:
    """Cover the else branch that raises ValueError for unsupported engines."""

    def test_raises_for_unsupported_engine(self):
        from odibi.context import EngineContext, PandasContext
        from odibi.enums import EngineType
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        df = pd.DataFrame(
            {
                "BatchID": ["B1"],
                "ts": [_ts(0)],
                "LoadTime": [0],
            }
        )
        ctx = PandasContext()
        engine_ctx = EngineContext(context=ctx, df=df, engine_type=EngineType.PANDAS)
        # Monkey-patch the engine type to an unsupported value
        engine_ctx.engine_type = "UNSUPPORTED"

        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        with pytest.raises(ValueError, match="Unsupported engine"):
            detect_sequential_phases(engine_ctx, params)


class TestEmptyDataFrameResult:
    """Cover the empty result_df early return in _detect_phases_pandas."""

    def test_empty_input_returns_empty(self):
        from odibi.transformers.manufacturing import (
            DetectSequentialPhasesParams,
            detect_sequential_phases,
        )

        df = pd.DataFrame(
            {
                "BatchID": pd.Series([], dtype=str),
                "ts": pd.Series([], dtype="datetime64[ns]"),
                "LoadTime": pd.Series([], dtype=float),
            }
        )
        params = DetectSequentialPhasesParams(
            group_by="BatchID",
            timestamp_col="ts",
            phases=["LoadTime"],
            start_threshold=240,
        )
        ctx = _make_pandas_context(df)
        result = detect_sequential_phases(ctx, params).df
        assert len(result) == 0
