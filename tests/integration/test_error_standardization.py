import pytest

from odibi.context import PandasContext
from odibi.engine.pandas_engine import PandasEngine


class TestErrorStandardization:
    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def connection(self):
        class MockConnection:
            def get_path(self, p):
                return p

            def pandas_storage_options(self):
                return {}

        return MockConnection()

    def test_read_nonexistent_file(self, engine, connection):
        """Test error when reading non-existent file."""
        # Pandas raises FileNotFoundError
        with pytest.raises(FileNotFoundError):
            engine.read(connection, "csv", path="nonexistent.csv")

    def test_invalid_sql_syntax(self, engine, context):
        """Test error for invalid SQL."""
        # Should raise something related to SQL error
        # PandasEngine.execute_sql wraps pandasql/duckdb errors
        # Currently it might let them bubble up

        context.register("df", pytest.importorskip("pandas").DataFrame({"a": [1]}))

        # execute_sql might raise different errors depending on backend
        # We want to check if it's descriptive
        try:
            engine.execute_sql("SELECT * FROM df WHERE", context)
            pytest.fail("Should have raised exception")
        except Exception:
            # Just verifying we get an exception.
            # Ideally we want to standardize this to TransformError
            pass

    def test_invalid_operation(self, engine, context):
        """Test error for unsupported operation."""
        import pandas as pd

        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Unsupported operation"):
            engine.execute_operation("invalid_op", {}, df)

    def test_missing_columns_in_pivot(self, engine):
        """Test error when pivoting with missing columns."""
        import pandas as pd

        df = pd.DataFrame({"a": [1]})

        # Missing pivot_column 'b'
        # Pandas 2.x might raise KeyError. Let's catch generic Exception and inspect
        try:
            engine.execute_operation("pivot", {"pivot_column": "b", "value_column": "a"}, df)
            pytest.fail("Should have raised exception")
        except KeyError:
            pass  # Expected
        except Exception as e:
            # If it raises something else, let's see what it is
            assert "b" in str(e) or "None" in str(e)
