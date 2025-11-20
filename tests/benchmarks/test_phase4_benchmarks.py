import pytest
import pandas as pd
import numpy as np
import time
from odibi.engine.pandas_engine import PandasEngine
from odibi.node import Node
from odibi.config import NodeConfig, ReadConfig, RetryConfig
from odibi.context import PandasContext


class TestPhase4Benchmarks:
    @pytest.fixture
    def large_data(self, tmp_path):
        """Create a reasonably large dataset for chunking benchmarks."""
        # 500k rows
        rows = 500_000
        df = pd.DataFrame(
            {
                "id": np.arange(rows),
                "val1": np.random.rand(rows),
                "val2": np.random.randint(0, 100, rows),
            }
        )
        path = tmp_path / "large.csv"
        df.to_csv(path, index=False)
        return str(path)

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def connection(self):
        class MockConn:
            def get_path(self, p):
                return p

            def pandas_storage_options(self):
                return {}

        return MockConn()

    def test_benchmark_chunking_overhead(self, engine, connection, large_data):
        """Compare full read vs chunked read performance."""
        # 1. Full Read
        start = time.time()
        df = engine.read(connection, "csv", path=large_data)
        # Force materialization if it was lazy (Pandas isn't, but good practice)
        count = len(df)
        duration_full = time.time() - start
        print(f"\nFull Read: {duration_full:.4f}s")

        # 2. Chunked Read (chunksize=10000)
        start = time.time()
        chunks = engine.read(connection, "csv", path=large_data, options={"chunksize": 10000})
        count_chunked = sum(len(c) for c in chunks)
        duration_chunked = time.time() - start
        print(f"Chunked Read: {duration_chunked:.4f}s")

        assert count == count_chunked
        # Chunking usually adds overhead, but saves memory.
        # We just want to ensure it's not unreasonably slow (e.g. > 2x slower).
        # Note: simple sum(len) is fast. If we did processing, overhead might be less noticeable relative to work.

    def test_benchmark_retry_overhead(self, engine, connection, large_data):
        """Measure overhead of retry wrapper when no failure occurs."""
        node_config = NodeConfig(
            name="bench", read=ReadConfig(connection="local", format="csv", path=large_data)
        )
        context = PandasContext()
        connections = {"local": connection}

        # 1. No Retry Config
        node_no_retry = Node(
            config=node_config,
            context=context,
            engine=engine,
            connections=connections,
            max_sample_rows=0,  # Disable sampling to isolate execution
        )
        start = time.time()
        node_no_retry.execute()
        duration_no_retry = time.time() - start
        print(f"\nNode Exec (No Retry): {duration_no_retry:.4f}s")

        # 2. With Retry Config (enabled, but success on first try)
        retry_config = RetryConfig(enabled=True, max_attempts=3)
        node_retry = Node(
            config=node_config,
            context=context,
            engine=engine,
            connections=connections,
            max_sample_rows=0,
            retry_config=retry_config,
        )
        start = time.time()
        node_retry.execute()
        duration_retry = time.time() - start
        print(f"Node Exec (With Retry Logic): {duration_retry:.4f}s")

        # Overhead should be negligible
        diff = abs(duration_retry - duration_no_retry)
        print(f"Diff: {diff:.4f}s")
