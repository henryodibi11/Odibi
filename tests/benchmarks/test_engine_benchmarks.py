import pytest
import time
from pathlib import Path
from odibi.engine.pandas_engine import PandasEngine

# Try importing SparkEngine, but don't fail if missing
try:
    from odibi.engine.spark_engine import SparkEngine

    # Also check if pyspark is actually importable (SparkEngine might import it inside __init__ or methods,
    # but checking here just in case)
    import pyspark  # noqa: F401

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class TestEngineBenchmarks:
    @pytest.fixture
    def pandas_engine(self):
        return PandasEngine()

    @pytest.fixture
    def spark_engine(self):
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")
        try:
            return SparkEngine()
        except Exception as e:
            pytest.skip(f"Spark initialization failed (likely missing winutils on Windows): {e}")

    @pytest.fixture
    def fake_connection(self):
        # Minimal fake connection object required by engine.read/write
        class FakeConn:
            def __init__(self):
                self.base_path = Path(".")

            def get_path(self, p):
                return p

            def pandas_storage_options(self):
                return {}

        return FakeConn()

    def _run_benchmark(self, engine_name, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        # Output in a format that can be easily parsed if needed
        print(f"BENCHMARK: {engine_name} - {operation_name}: {duration:.4f}s")
        return result

    def test_pandas_engine_benchmarks(
        self, pandas_engine, benchmark_data, fake_connection, tmp_path
    ):
        """Benchmark PandasEngine operations."""
        print("\n--- PandasEngine Benchmarks ---")

        # Read CSV
        df = self._run_benchmark(
            "PandasEngine",
            "Read CSV",
            pandas_engine.read,
            fake_connection,
            "csv",
            path=benchmark_data["csv"],
        )
        assert len(df) == benchmark_data["rows"]

        # Read Parquet
        df_parquet = self._run_benchmark(
            "PandasEngine",
            "Read Parquet",
            pandas_engine.read,
            fake_connection,
            "parquet",
            path=benchmark_data["parquet"],
        )
        assert len(df_parquet) == benchmark_data["rows"]

        # Filter
        # Filter where category is 'A'
        self._run_benchmark("PandasEngine", "Filter", lambda: df[df["category"] == "A"])

        # GroupBy + Aggregate
        # Group by 'category' and sum 'value1'
        self._run_benchmark(
            "PandasEngine", "GroupBy+Agg", lambda: df.groupby("category")["value1"].sum()
        )

        # Write Parquet
        output_path = str(tmp_path / "output_pandas.parquet")
        self._run_benchmark(
            "PandasEngine",
            "Write Parquet",
            pandas_engine.write,
            df,
            fake_connection,
            "parquet",
            path=output_path,
        )

    @pytest.mark.skipif(not SPARK_AVAILABLE, reason="Spark not installed")
    def test_spark_engine_benchmarks(self, spark_engine, benchmark_data, fake_connection, tmp_path):
        """Benchmark SparkEngine operations."""
        print("\n--- SparkEngine Benchmarks ---")

        # Read CSV
        df = self._run_benchmark(
            "SparkEngine",
            "Read CSV",
            spark_engine.read,
            fake_connection,
            "csv",
            path=benchmark_data["csv"],
        )
        # Spark read is lazy, so count() to trigger execution
        self._run_benchmark("SparkEngine", "Count (trigger read)", df.count)

        # Read Parquet
        df_parquet = self._run_benchmark(  # noqa: F841
            "SparkEngine",
            "Read Parquet",
            spark_engine.read,
            fake_connection,
            "parquet",
            path=benchmark_data["parquet"],
        )

        # Filter
        # Spark filter is lazy
        filtered = df.filter(df.category == "A")
        self._run_benchmark("SparkEngine", "Filter+Count", filtered.count)

        # GroupBy + Aggregate
        grouped = df.groupby("category").sum("value1")
        self._run_benchmark("SparkEngine", "GroupBy+Agg+Collect", grouped.collect)

        # Write Parquet
        output_path = str(tmp_path / "output_spark.parquet")
        self._run_benchmark(
            "SparkEngine",
            "Write Parquet",
            spark_engine.write,
            df,
            fake_connection,
            "parquet",
            path=output_path,
            mode="overwrite",
        )
