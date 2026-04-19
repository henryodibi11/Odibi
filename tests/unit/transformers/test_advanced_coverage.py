"""
Additional coverage tests for odibi/transformers/advanced.py

Targets the uncovered Pandas paths in:
- _split_by_day  (lines 1194-1248)
- _split_by_hour (lines 1312-1368)
- _split_by_shift (lines 1440-1496)
- sessionize (lines 952-981)
"""

import logging

logging.getLogger("odibi").propagate = False

import pytest  # noqa: E402
import pandas as pd  # noqa: E402

from odibi.context import EngineContext, PandasContext  # noqa: E402
from odibi.enums import EngineType  # noqa: E402
from odibi.engine.pandas_engine import PandasEngine  # noqa: E402
from odibi.transformers.advanced import (  # noqa: E402
    sessionize,
    SessionizeParams,
    split_events_by_period,
    SplitEventsByPeriodParams,
    ShiftDefinition,
)


# -------------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------------


def setup_context(df):
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())
    pandas_engine = PandasEngine()
    return EngineContext(
        pandas_ctx,
        df.copy(),
        EngineType.PANDAS,
        sql_executor=pandas_engine.execute_sql,
        engine=pandas_engine,
    )


# =========================================================================
# _split_by_day  Pandas path
# =========================================================================


class TestSplitByDayPandas:
    """Tests for split_events_by_period with period='day' on Pandas."""

    def test_multi_day_event_with_duration(self):
        """Event spanning 3 days should produce 3 rows with duration_col."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 10:00:00"],
                "end": ["2024-01-03 14:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="day", duration_col="dur_min"
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 3
        assert "dur_min" in result.columns

        result = result.sort_values("start").reset_index(drop=True)
        # Day 1: 10:00 -> midnight = 14h = 840 min
        assert result.loc[0, "dur_min"] == pytest.approx(840.0)
        # Day 2: midnight -> midnight = 24h = 1440 min
        assert result.loc[1, "dur_min"] == pytest.approx(1440.0)
        # Day 3: midnight -> 14:00 = 14h = 840 min
        assert result.loc[2, "dur_min"] == pytest.approx(840.0)

    def test_single_day_event_with_duration(self):
        """Event within same day should produce 1 row with duration."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-03-15 08:00:00"],
                "end": ["2024-03-15 17:30:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="day", duration_col="dur_min"
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 1
        assert result.iloc[0]["dur_min"] == pytest.approx(570.0)  # 9.5h

    def test_single_day_event_without_duration(self):
        """No duration_col param means no extra column."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-03-15 08:00:00"],
                "end": ["2024-03-15 17:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(start_col="start", end_col="end", period="day")
        result = split_events_by_period(ctx, params).df

        assert len(result) == 1
        assert "dur_min" not in result.columns

    def test_multi_day_event_without_duration(self):
        """Multi-day without duration_col — no duration column added."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 06:00:00"],
                "end": ["2024-01-02 18:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(start_col="start", end_col="end", period="day")
        result = split_events_by_period(ctx, params).df

        assert len(result) == 2
        assert "dur_min" not in result.columns

    def test_mixed_single_and_multi_day_events(self):
        """Multiple rows: some single-day, others multi-day."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "start": ["2024-01-01 10:00:00", "2024-02-05 08:00:00"],
                "end": ["2024-01-03 14:00:00", "2024-02-05 16:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="day", duration_col="dur"
        )
        result = split_events_by_period(ctx, params).df

        # id=1 spans 3 days, id=2 spans 1 day => 4 rows
        assert len(result) == 4
        assert "dur" in result.columns

    def test_event_ending_at_midnight_boundary(self):
        """Start 23:00, end next day 00:00:00 — spans 2 days."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-06-10 23:00:00"],
                "end": ["2024-06-11 00:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="day", duration_col="dur"
        )
        result = split_events_by_period(ctx, params).df

        # Normalize difference: day 10 -> day 11 = 2 days
        assert len(result) == 2
        result = result.sort_values("start").reset_index(drop=True)
        # First segment: 23:00 -> midnight = 60 min
        assert result.loc[0, "dur"] == pytest.approx(60.0)
        # Second segment: midnight -> midnight = 0 min
        assert result.loc[1, "dur"] == pytest.approx(0.0)


# =========================================================================
# _split_by_hour  Pandas path
# =========================================================================


class TestSplitByHourPandas:
    """Tests for split_events_by_period with period='hour' on Pandas."""

    def test_multi_hour_event_with_duration(self):
        """Event spanning 3 hours should produce 3 rows."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 10:15:00"],
                "end": ["2024-01-01 12:45:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="hour", duration_col="dur"
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 3
        result = result.sort_values("start").reset_index(drop=True)
        # 10:15 -> 11:00 = 45 min
        assert result.loc[0, "dur"] == pytest.approx(45.0)
        # 11:00 -> 12:00 = 60 min
        assert result.loc[1, "dur"] == pytest.approx(60.0)
        # 12:00 -> 12:45 = 45 min
        assert result.loc[2, "dur"] == pytest.approx(45.0)

    def test_single_hour_event(self):
        """Event within same hour stays as 1 row."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 10:00:00"],
                "end": ["2024-01-01 10:30:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(start_col="start", end_col="end", period="hour")
        result = split_events_by_period(ctx, params).df

        assert len(result) == 1

    def test_multi_hour_with_duration_col(self):
        """Duration computed per segment for multi-hour events."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-05-20 14:20:00"],
                "end": ["2024-05-20 16:10:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="hour", duration_col="dur"
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 3
        result = result.sort_values("start").reset_index(drop=True)
        # 14:20 -> 15:00 = 40 min
        assert result.loc[0, "dur"] == pytest.approx(40.0)
        # 15:00 -> 16:00 = 60 min
        assert result.loc[1, "dur"] == pytest.approx(60.0)
        # 16:00 -> 16:10 = 10 min
        assert result.loc[2, "dur"] == pytest.approx(10.0)

    def test_all_single_hour_events(self):
        """When all events are within 1 hour, no explosion occurs."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "start": ["2024-01-01 10:00:00", "2024-01-01 11:05:00"],
                "end": ["2024-01-01 10:30:00", "2024-01-01 11:55:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start", end_col="end", period="hour", duration_col="dur"
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 2
        assert result.iloc[0]["dur"] == pytest.approx(30.0)
        assert result.iloc[1]["dur"] == pytest.approx(50.0)


# =========================================================================
# _split_by_shift  Pandas path
# =========================================================================


class TestSplitByShiftPandas:
    """Tests for split_events_by_period with period='shift' on Pandas."""

    def _morning_evening_shifts(self):
        return [
            ShiftDefinition(name="Morning", start="06:00", end="14:00"),
            ShiftDefinition(name="Evening", start="14:00", end="22:00"),
        ]

    def _three_shifts(self):
        return [
            ShiftDefinition(name="Day", start="06:00", end="14:00"),
            ShiftDefinition(name="Swing", start="14:00", end="22:00"),
            ShiftDefinition(name="Night", start="22:00", end="06:00"),
        ]

    def test_event_spanning_two_shifts(self):
        """Event 07:00-15:00 across Morning(06-14) and Evening(14-22)."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 07:00:00"],
                "end": ["2024-01-01 15:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=self._morning_evening_shifts(),
            duration_col="dur",
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 2
        result = result.sort_values("start").reset_index(drop=True)
        assert result.loc[0, "shift_name"] == "Morning"
        assert result.loc[0, "dur"] == pytest.approx(420.0)  # 7h
        assert result.loc[1, "shift_name"] == "Evening"
        assert result.loc[1, "dur"] == pytest.approx(60.0)  # 1h

    def test_event_spanning_overnight_shift(self):
        """Night shift 22:00-06:00 crossing midnight."""
        shifts = [
            ShiftDefinition(name="Night", start="22:00", end="06:00"),
        ]
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 23:00:00"],
                "end": ["2024-01-02 03:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=shifts,
            duration_col="dur",
        )
        result = split_events_by_period(ctx, params).df

        # The event 23:00-03:00 should fall within Night (22:00 -> next day 06:00)
        assert len(result) >= 1
        assert (result["shift_name"] == "Night").all()
        total_dur = result["dur"].sum()
        assert total_dur == pytest.approx(240.0)  # 4 hours

    def test_event_with_duration_col(self):
        """Verify duration calculated per shift segment."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 12:00:00"],
                "end": ["2024-01-01 16:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=self._morning_evening_shifts(),
            duration_col="minutes",
        )
        result = split_events_by_period(ctx, params).df

        assert len(result) == 2
        result = result.sort_values("start").reset_index(drop=True)
        # 12:00 -> 14:00 = 120 min Morning
        assert result.loc[0, "minutes"] == pytest.approx(120.0)
        # 14:00 -> 16:00 = 120 min Evening
        assert result.loc[1, "minutes"] == pytest.approx(120.0)

    def test_no_matching_segments_empty_rows(self):
        """Event outside all shift boundaries produces empty-ish result."""
        shifts = [
            ShiftDefinition(name="Day", start="06:00", end="14:00"),
        ]
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 15:00:00"],
                "end": ["2024-01-01 17:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=shifts,
            duration_col="dur",
        )
        result = split_events_by_period(ctx, params).df

        # No segments match => fallback with shift_name=None and dur=0
        assert "shift_name" in result.columns
        assert result.iloc[0]["shift_name"] is None
        assert result.iloc[0]["dur"] == pytest.approx(0.0)

    def test_multiple_events_multiple_shifts(self):
        """Several events spanning different shifts."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "start": [
                    "2024-01-01 07:00:00",
                    "2024-01-01 13:00:00",
                ],
                "end": [
                    "2024-01-01 09:00:00",
                    "2024-01-01 15:00:00",
                ],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=self._morning_evening_shifts(),
        )
        result = split_events_by_period(ctx, params).df

        # id=1: fully within Morning => 1 row
        # id=2: 13-14 Morning + 14-15 Evening => 2 rows
        assert len(result) == 3

    def test_three_shift_setup_spanning_all(self):
        """Event spanning Day(06-14), Swing(14-22), Night(22-06)."""
        df = pd.DataFrame(
            {
                "id": [1],
                "start": ["2024-01-01 10:00:00"],
                "end": ["2024-01-02 02:00:00"],
            }
        )
        ctx = setup_context(df)
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="shift",
            shifts=self._three_shifts(),
            duration_col="dur",
        )
        result = split_events_by_period(ctx, params).df

        shift_names = set(result["shift_name"].tolist())
        assert "Day" in shift_names
        assert "Swing" in shift_names
        assert "Night" in shift_names
        assert result["dur"].sum() == pytest.approx(960.0)  # 16 hours total


# =========================================================================
# Sessionize  Pandas path
# =========================================================================


class TestSessionizePandas:
    """Tests for sessionize on Pandas engine."""

    def test_string_timestamps_auto_conversion(self):
        """Pass string timestamps (not datetime), verify auto-conversion."""
        df = pd.DataFrame(
            {
                "user": ["A", "A", "A"],
                "ts": [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 11:00:00",
                ],
            }
        )
        ctx = setup_context(df)
        params = SessionizeParams(timestamp_col="ts", user_col="user", threshold_seconds=1800)
        result = sessionize(ctx, params).df

        assert "session_id" in result.columns
        sessions = result["session_id"].unique()
        # First two within 30 min, third >30 min gap => 2 sessions
        assert len(sessions) == 2

    def test_all_events_in_one_session(self):
        """All timestamps within threshold => 1 session."""
        df = pd.DataFrame(
            {
                "user": ["A", "A", "A", "A"],
                "ts": pd.to_datetime(
                    [
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:10:00",
                        "2024-01-01 10:20:00",
                        "2024-01-01 10:29:00",
                    ]
                ),
            }
        )
        ctx = setup_context(df)
        params = SessionizeParams(timestamp_col="ts", user_col="user", threshold_seconds=1800)
        result = sessionize(ctx, params).df

        assert result["session_id"].nunique() == 1

    def test_each_event_own_session(self):
        """All timestamps far apart => each gets own session."""
        df = pd.DataFrame(
            {
                "user": ["A", "A", "A"],
                "ts": pd.to_datetime(
                    [
                        "2024-01-01 08:00:00",
                        "2024-01-01 10:00:00",
                        "2024-01-01 14:00:00",
                    ]
                ),
            }
        )
        ctx = setup_context(df)
        params = SessionizeParams(timestamp_col="ts", user_col="user", threshold_seconds=1800)
        result = sessionize(ctx, params).df

        assert result["session_id"].nunique() == 3

    def test_multiple_users_different_sessions(self):
        """Complex multi-user scenario."""
        df = pd.DataFrame(
            {
                "user": ["A", "A", "A", "B", "B", "B"],
                "ts": pd.to_datetime(
                    [
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:05:00",
                        "2024-01-01 11:00:00",  # new session for A
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:10:00",
                        "2024-01-01 10:20:00",  # all 1 session for B
                    ]
                ),
            }
        )
        ctx = setup_context(df)
        params = SessionizeParams(timestamp_col="ts", user_col="user", threshold_seconds=1800)
        result = sessionize(ctx, params).df

        # A: 2 sessions, B: 1 session => 3 unique session IDs
        assert result["session_id"].nunique() == 3
        # Session IDs should contain user prefix
        for sid in result["session_id"].unique():
            assert sid.startswith("A-") or sid.startswith("B-")
