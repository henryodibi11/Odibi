import pandas as pd
from odibi.registry import transform


@transform
def resample_timeseries(context, current, time_col, rule="1h", agg="mean"):
    """Resample time series data."""
    df = current.copy()

    # Ensure datetime
    if df[time_col].dtype == "object":
        df[time_col] = pd.to_datetime(df[time_col])

    # Set index
    df = df.set_index(time_col)

    # Resample
    # Note: resample() requires a datetime index
    resampled = df.resample(rule)

    # Aggregate
    if agg == "mean":
        result = resampled.mean()
    elif agg == "sum":
        result = resampled.sum()
    elif agg == "max":
        result = resampled.max()
    elif agg == "min":
        result = resampled.min()
    elif agg == "std":
        result = resampled.std()
    else:
        raise ValueError(f"Unsupported aggregation: {agg}")

    # Filter out empty bins if any
    result = result.dropna()

    return result.reset_index()
