import pandas as pd
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.advanced import (
    normalize_json,
    NormalizeJsonParams,
    sessionize,
    SessionizeParams,
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
