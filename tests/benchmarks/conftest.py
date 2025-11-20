import pytest
import pandas as pd
import numpy as np


@pytest.fixture(scope="session")
def benchmark_data(tmp_path_factory):
    """Creates a temporary directory for benchmark data and generates synthetic files."""
    data_dir = tmp_path_factory.mktemp("benchmark_data")

    # Generate synthetic data
    # Using 100,000 rows for a reasonable benchmark that doesn't take too long
    num_rows = 100_000
    np.random.seed(42)

    data = {
        "id": np.arange(num_rows),
        "category": np.random.choice(["A", "B", "C", "D"], size=num_rows),
        "value1": np.random.rand(num_rows) * 100,
        "value2": np.random.randint(0, 1000, size=num_rows),
        # Force microsecond precision for Spark compatibility (Spark doesn't support nanoseconds in Parquet)
        "date": pd.date_range(start="2023-01-01", periods=num_rows, freq="min").astype(
            "datetime64[us]"
        ),
    }

    df = pd.DataFrame(data)

    # Save as CSV
    csv_path = data_dir / "data.csv"
    df.to_csv(csv_path, index=False)

    # Save as Parquet
    parquet_path = data_dir / "data.parquet"
    df.to_parquet(parquet_path, index=False)

    return {
        "dir": str(data_dir),
        "csv": str(csv_path),
        "parquet": str(parquet_path),
        "rows": num_rows,
        "df": df,  # Keep reference to expected data for validation if needed
    }
