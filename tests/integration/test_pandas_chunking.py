import pytest
import pandas as pd
import numpy as np
from odibi.engine.pandas_engine import PandasEngine
from odibi.context import PandasContext


class TestPandasChunking:
    @pytest.fixture
    def large_csv(self, tmp_path):
        """Create a CSV file that we can read in chunks."""
        # Create 100 rows
        df = pd.DataFrame(
            {"id": range(100), "value": np.random.rand(100), "category": ["A", "B"] * 50}
        )
        path = tmp_path / "large.csv"
        df.to_csv(path, index=False)
        return path

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def connection(self, tmp_path):
        class MockConnection:
            def get_path(self, p):
                return str(p)

            def pandas_storage_options(self):
                return {}

        return MockConnection()

    def test_read_chunking_returns_iterator(self, engine, connection, large_csv):
        """Test that read returns an iterator when chunksize is provided."""
        result = engine.read(
            connection=connection, format="csv", path=str(large_csv), options={"chunksize": 10}
        )

        from collections.abc import Iterator

        assert isinstance(result, Iterator)

        # Verify we can consume it
        chunks = list(result)
        assert len(chunks) == 10  # 100 rows / 10 per chunk = 10 chunks
        assert isinstance(chunks[0], pd.DataFrame)
        assert len(chunks[0]) == 10

    def test_sql_transform_with_chunks(self, engine, context, connection, large_csv):
        """Test executing SQL on chunked data."""
        # 1. Read in chunks
        chunks = engine.read(
            connection=connection, format="csv", path=str(large_csv), options={"chunksize": 10}
        )

        # 2. Register in context
        context.register("source", chunks)

        # 3. Execute SQL
        # This is expected to fail if execute_sql doesn't handle iterators
        sql = "SELECT id, value FROM source WHERE category = 'A'"

        try:
            result = engine.execute_sql(sql, context)

            # If it succeeds, it should return either a DataFrame or an Iterator
            if isinstance(result, pd.DataFrame):
                assert len(result) == 50  # 50 'A's total
            else:
                # If it returns iterator
                results = list(result)
                total_rows = sum(len(df) for df in results)
                assert total_rows == 50

        except Exception as e:
            pytest.fail(f"SQL execution failed with chunks: {e}")

    def test_write_chunks(self, engine, connection, large_csv, tmp_path):
        """Test writing chunked data."""
        chunks = engine.read(
            connection=connection, format="csv", path=str(large_csv), options={"chunksize": 10}
        )

        output_path = tmp_path / "output.csv"

        engine.write(
            df=chunks, connection=connection, format="csv", path=str(output_path), mode="overwrite"
        )

        # Verify output
        df_out = pd.read_csv(output_path)
        assert len(df_out) == 100

    def test_execute_operation_pivot_with_chunks(self, engine, connection, large_csv):
        """Test executing 'pivot' operation on chunked data."""
        # 1. Read in chunks
        chunks = engine.read(
            connection=connection, format="csv", path=str(large_csv), options={"chunksize": 10}
        )

        # 2. Setup pivot params
        # Data has: id (0-99), value (random), category (A, B)
        # Let's pivot so 'category' becomes columns, 'value' is values, index is 'id'
        params = {
            "group_by": ["id"],
            "pivot_column": "category",
            "value_column": "value",
            "agg_func": "first",
        }

        # 3. Execute operation
        # This should handle the iterator by consuming it
        try:
            result = engine.execute_operation("pivot", params, chunks)

            assert isinstance(result, pd.DataFrame)
            assert "A" in result.columns
            assert "B" in result.columns
            assert len(result) == 100
        except Exception as e:
            pytest.fail(f"Pivot execution failed with chunks: {e}")
