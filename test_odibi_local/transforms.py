import pandas as pd

from odibi.registry import transform


@transform("add_status_column")
def add_status_column(df: pd.DataFrame, status: str = "active") -> pd.DataFrame:
    """Add a status column to the DataFrame."""
    df = df.copy()
    df["status"] = status
    return df


@transform("uppercase_name")
def uppercase_name(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase the name column."""
    df = df.copy()
    if "name" in df.columns:
        df["name"] = df["name"].str.upper()
    return df
