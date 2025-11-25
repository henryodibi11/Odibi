import pandas as pd

from odibi.engine import PandasEngine


def read_weather_custom(path, **options):
    """Custom reader for .weather files (pipe separated)."""
    print(f"DEBUG: Custom weather reader invoked for {path}")

    # Handle glob expansion result
    if isinstance(path, list):
        # Naive implementation: concat all
        dfs = [pd.read_csv(p, sep="|", names=["date", "temp", "humidity"]) for p in path]
        return pd.concat(dfs, ignore_index=True)

    return pd.read_csv(path, sep="|", names=["date", "temp", "humidity"])


# Register the format
# This runs when the module is imported by the CLI
PandasEngine.register_format("weather", reader=read_weather_custom)
