"""Tests for unified Context API."""

import pytest
import pandas as pd

from odibi.context import PandasContext, create_context


class TestPandasContext:
    """Test PandasContext implementation."""

    def test_register_and_get_dataframe(self):
        """Can register and retrieve DataFrame."""
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        ctx.register("test_df", df)
        retrieved = ctx.get("test_df")

        pd.testing.assert_frame_equal(retrieved, df)

    def test_has_returns_true_for_registered(self):
        """has() returns True for registered DataFrames."""
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1, 2]})

        ctx.register("exists", df)

        assert ctx.has("exists") is True
        assert ctx.has("does_not_exist") is False

    def test_get_raises_keyerror_for_missing(self):
        """get() raises KeyError with helpful message for missing DataFrame."""
        ctx = PandasContext()

        with pytest.raises(KeyError) as exc_info:
            ctx.get("missing_df")

        error_msg = str(exc_info.value)
        assert "missing_df" in error_msg
        assert "not found in context" in error_msg

    def test_get_error_shows_available_names(self):
        """Error message shows available DataFrame names."""
        ctx = PandasContext()
        ctx.register("df1", pd.DataFrame({"a": [1]}))
        ctx.register("df2", pd.DataFrame({"b": [2]}))

        with pytest.raises(KeyError) as exc_info:
            ctx.get("df3")

        error_msg = str(exc_info.value)
        assert "df1" in error_msg
        assert "df2" in error_msg

    def test_list_names_returns_all_registered(self):
        """list_names() returns all registered DataFrame names."""
        ctx = PandasContext()

        assert ctx.list_names() == []

        ctx.register("df1", pd.DataFrame({"a": [1]}))
        ctx.register("df2", pd.DataFrame({"b": [2]}))

        names = ctx.list_names()
        assert set(names) == {"df1", "df2"}

    def test_clear_removes_all_dataframes(self):
        """clear() removes all registered DataFrames."""
        ctx = PandasContext()
        ctx.register("df1", pd.DataFrame({"a": [1]}))
        ctx.register("df2", pd.DataFrame({"b": [2]}))

        assert len(ctx.list_names()) == 2

        ctx.clear()

        assert len(ctx.list_names()) == 0
        assert ctx.has("df1") is False

    def test_register_overwrites_existing(self):
        """Registering same name overwrites previous DataFrame."""
        ctx = PandasContext()

        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"a": [3, 4, 5]})

        ctx.register("data", df1)
        assert len(ctx.get("data")) == 2

        ctx.register("data", df2)
        assert len(ctx.get("data")) == 3

    def test_register_validates_dataframe_type(self):
        """register() rejects non-DataFrame objects."""
        ctx = PandasContext()

        with pytest.raises(TypeError) as exc_info:
            ctx.register("invalid", {"not": "a dataframe"})

        assert "Expected pandas.DataFrame" in str(exc_info.value)


class TestContextFactory:
    """Test context factory function."""

    def test_create_pandas_context(self):
        """Factory creates PandasContext for pandas engine."""
        ctx = create_context("pandas")

        assert isinstance(ctx, PandasContext)

    def test_create_context_invalid_engine(self):
        """Factory raises error for invalid engine."""
        with pytest.raises(ValueError) as exc_info:
            create_context("invalid_engine")

        assert "Unsupported engine: invalid_engine" in str(exc_info.value)
        assert "pandas" in str(exc_info.value)
        assert "spark" in str(exc_info.value)


class TestContextDataIsolation:
    """Test that different contexts are isolated."""

    def test_different_contexts_are_isolated(self):
        """Multiple context instances don't share data."""
        ctx1 = PandasContext()
        ctx2 = PandasContext()

        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [2]})

        ctx1.register("data", df1)
        ctx2.register("data", df2)

        # Each context has its own data
        assert ctx1.get("data").columns.tolist() == ["a"]
        assert ctx2.get("data").columns.tolist() == ["b"]


class TestContextUsagePatterns:
    """Test common usage patterns."""

    def test_chaining_multiple_dataframes(self):
        """Can register multiple DataFrames and reference them."""
        ctx = PandasContext()

        # Simulate node pipeline
        raw = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        ctx.register("raw_data", raw)

        # Next node processes it
        filtered = ctx.get("raw_data")
        filtered = filtered[filtered["value"] > 15]
        ctx.register("filtered_data", filtered)

        # Verify
        assert len(ctx.get("raw_data")) == 3
        assert len(ctx.get("filtered_data")) == 2
        assert ctx.has("raw_data")
        assert ctx.has("filtered_data")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
