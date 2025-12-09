import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.advanced import (
    NormalizeJsonParams,
    SessionizeParams,
    ShiftDefinition,
    SplitEventsByPeriodParams,
    normalize_json,
    sessionize,
    split_events_by_period,
)


def test_normalize_json_pandas():
    import json

    data = {"id": [1], "raw": [json.dumps({"a": 1, "b": {"c": 2}})]}
    df = pd.DataFrame(data)
    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)

    res_ctx = normalize_json(ctx, NormalizeJsonParams(column="raw"))
    res_df = res_ctx.df

    # pd.json_normalize results in columns 'a', 'b.c'
    assert "a" in res_df.columns
    assert "b_c" in res_df.columns
    assert res_df["b_c"][0] == 2


def test_sessionize_pandas():
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1", "u2"],
            "ts": [
                "2023-01-01 10:00:00",
                "2023-01-01 10:10:00",  # diff 10m (ok)
                "2023-01-01 12:00:00",  # diff 110m (new session)
                "2023-01-01 10:00:00",
            ],
        }
    )
    df["ts"] = pd.to_datetime(df["ts"])

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SessionizeParams(timestamp_col="ts", user_col="user", threshold_seconds=1800)  # 30m

    res_ctx = sessionize(ctx, params)
    res_df = res_ctx.df

    # Sort to match expectation (sessionize sorts by user, ts)
    res_df = res_df.sort_values(["user", "ts"]).reset_index(drop=True)

    # u1 first session: 10:00, 10:10
    assert res_df.loc[0, "session_id"] == "u1-1"
    assert res_df.loc[1, "session_id"] == "u1-1"
    # u1 second session: 12:00
    assert res_df.loc[2, "session_id"] == "u1-2"
    # u2 first session
    assert res_df.loc[3, "session_id"] == "u2-1"


# -------------------------------------------------------------------------
# Split Events By Period Tests
# -------------------------------------------------------------------------


def test_split_events_by_period_day_single_day():
    """Test that single-day events remain unchanged."""
    df = pd.DataFrame(
        {
            "event_id": [1],
            "start_time": ["2023-09-02 08:00:00"],
            "end_time": ["2023-09-02 12:00:00"],
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SplitEventsByPeriodParams(
        start_col="start_time",
        end_col="end_time",
        period="day",
        duration_col="duration_min",
    )

    res_ctx = split_events_by_period(ctx, params)
    res_df = res_ctx.df

    assert len(res_df) == 1
    assert res_df.loc[0, "duration_min"] == 240.0  # 4 hours = 240 minutes


def test_split_events_by_period_day_multi_day():
    """Test that multi-day events are split at day boundaries."""
    df = pd.DataFrame(
        {
            "event_id": [1],
            "start_time": ["2023-09-01 18:00:00"],
            "end_time": ["2023-09-03 06:00:00"],
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SplitEventsByPeriodParams(
        start_col="start_time",
        end_col="end_time",
        period="day",
        duration_col="duration_min",
    )

    res_ctx = split_events_by_period(ctx, params)
    res_df = res_ctx.df.sort_values("start_time").reset_index(drop=True)

    # Should be split into 3 segments
    assert len(res_df) == 3

    # Day 1: 18:00 - 00:00 (6 hours = 360 min)
    assert res_df.loc[0, "start_time"] == pd.Timestamp("2023-09-01 18:00:00")
    assert res_df.loc[0, "end_time"] == pd.Timestamp("2023-09-02 00:00:00")
    assert res_df.loc[0, "duration_min"] == 360.0

    # Day 2: 00:00 - 00:00 (24 hours = 1440 min)
    assert res_df.loc[1, "start_time"] == pd.Timestamp("2023-09-02 00:00:00")
    assert res_df.loc[1, "end_time"] == pd.Timestamp("2023-09-03 00:00:00")
    assert res_df.loc[1, "duration_min"] == 1440.0

    # Day 3: 00:00 - 06:00 (6 hours = 360 min)
    assert res_df.loc[2, "start_time"] == pd.Timestamp("2023-09-03 00:00:00")
    assert res_df.loc[2, "end_time"] == pd.Timestamp("2023-09-03 06:00:00")
    assert res_df.loc[2, "duration_min"] == 360.0


def test_split_events_by_period_hour():
    """Test splitting events by hour boundaries."""
    df = pd.DataFrame(
        {
            "event_id": [1],
            "start_time": ["2023-09-01 10:30:00"],
            "end_time": ["2023-09-01 12:15:00"],
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SplitEventsByPeriodParams(
        start_col="start_time",
        end_col="end_time",
        period="hour",
        duration_col="duration_min",
    )

    res_ctx = split_events_by_period(ctx, params)
    res_df = res_ctx.df.sort_values("start_time").reset_index(drop=True)

    # Should be split into 3 segments (10:30-11:00, 11:00-12:00, 12:00-12:15)
    assert len(res_df) == 3

    # Hour 1: 10:30 - 11:00 (30 min)
    assert res_df.loc[0, "start_time"] == pd.Timestamp("2023-09-01 10:30:00")
    assert res_df.loc[0, "end_time"] == pd.Timestamp("2023-09-01 11:00:00")
    assert res_df.loc[0, "duration_min"] == 30.0

    # Hour 2: 11:00 - 12:00 (60 min)
    assert res_df.loc[1, "start_time"] == pd.Timestamp("2023-09-01 11:00:00")
    assert res_df.loc[1, "end_time"] == pd.Timestamp("2023-09-01 12:00:00")
    assert res_df.loc[1, "duration_min"] == 60.0

    # Hour 3: 12:00 - 12:15 (15 min)
    assert res_df.loc[2, "start_time"] == pd.Timestamp("2023-09-01 12:00:00")
    assert res_df.loc[2, "end_time"] == pd.Timestamp("2023-09-01 12:15:00")
    assert res_df.loc[2, "duration_min"] == 15.0


def test_split_events_by_period_shift():
    """Test splitting events by shift boundaries."""
    df = pd.DataFrame(
        {
            "event_id": [1],
            "start_time": ["2023-09-01 18:00:00"],
            "end_time": ["2023-09-02 08:00:00"],
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    shifts = [
        ShiftDefinition(name="Day", start="06:00", end="14:00"),
        ShiftDefinition(name="Swing", start="14:00", end="22:00"),
        ShiftDefinition(name="Night", start="22:00", end="06:00"),
    ]

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SplitEventsByPeriodParams(
        start_col="start_time",
        end_col="end_time",
        period="shift",
        duration_col="duration_min",
        shifts=shifts,
        shift_col="shift_name",
    )

    res_ctx = split_events_by_period(ctx, params)
    res_df = res_ctx.df.sort_values("start_time").reset_index(drop=True)

    # Event spans: 18:00 (Swing) -> 22:00 (Night) -> 06:00 (Day) -> 08:00
    # Expected segments:
    # - Swing: 18:00 - 22:00 (4 hours = 240 min)
    # - Night: 22:00 - 06:00 (8 hours = 480 min)
    # - Day: 06:00 - 08:00 (2 hours = 120 min)
    assert len(res_df) == 3

    swing_row = res_df[res_df["shift_name"] == "Swing"].iloc[0]
    assert swing_row["duration_min"] == 240.0

    night_row = res_df[res_df["shift_name"] == "Night"].iloc[0]
    assert night_row["duration_min"] == 480.0

    day_row = res_df[res_df["shift_name"] == "Day"].iloc[0]
    assert day_row["duration_min"] == 120.0


def test_split_events_by_period_no_duration_col():
    """Test that duration column is optional."""
    df = pd.DataFrame(
        {
            "event_id": [1],
            "start_time": ["2023-09-01 08:00:00"],
            "end_time": ["2023-09-01 10:00:00"],
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)
    params = SplitEventsByPeriodParams(
        start_col="start_time",
        end_col="end_time",
        period="day",
    )

    res_ctx = split_events_by_period(ctx, params)
    res_df = res_ctx.df

    assert len(res_df) == 1
    assert "duration_min" not in res_df.columns
