"""
Testing Fixtures
================

Reusable fixtures for testing pipelines and transformations.
"""

import shutil
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

import numpy as np
import pandas as pd


@contextmanager
def temp_directory() -> Generator[str, None, None]:
    """
    Create a temporary directory for test artifacts.

    Yields:
        Path to the temporary directory.

    Example:
        with temp_directory() as temp_dir:
            path = os.path.join(temp_dir, "test.csv")
            df.to_csv(path)
    """
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


def generate_sample_data(
    rows: int = 10, engine_type: str = "pandas", schema: Optional[Dict[str, str]] = None
) -> Any:
    """
    Generate a sample DataFrame (Pandas or Spark).

    Args:
        rows: Number of rows to generate
        engine_type: "pandas" or "spark"
        schema: Optional dictionary of {column_name: type}
               Supported types: "int", "float", "str", "date"

    Returns:
        DataFrame (pd.DataFrame or pyspark.sql.DataFrame)
    """
    from datetime import datetime, timedelta

    # Default schema if none provided
    if not schema:
        schema = {"id": "int", "value": "float", "category": "str", "timestamp": "date"}

    data = {}
    for col, dtype in schema.items():
        if dtype == "int":
            data[col] = np.random.randint(0, 1000, rows)
        elif dtype == "float":
            data[col] = np.random.rand(rows) * 100
        elif dtype == "str":
            data[col] = [f"val_{i}" for i in range(rows)]
        elif dtype == "date":
            base_date = datetime.now()
            data[col] = [base_date - timedelta(days=i) for i in range(rows)]

    pdf = pd.DataFrame(data)

    if engine_type == "pandas":
        return pdf

    if engine_type == "spark":
        try:
            from pyspark.sql import SparkSession

            # Try to get existing session or create new one
            spark = SparkSession.builder.master("local[*]").appName("odibi-test").getOrCreate()
            return spark.createDataFrame(pdf)
        except ImportError:
            raise ImportError("Spark not installed. Run 'pip install odibi[spark]'")

    raise ValueError(f"Unknown engine type: {engine_type}")
