"""Tests for explanation system."""

import pytest
from odibi.transformations import transformation, get_registry


class TestExplanationDecorator:
    """Tests for @func.explain pattern."""

    def setup_method(self):
        """Clear registry before each test."""
        get_registry().clear()

    def test_transformation_has_explain_method(self):
        """Should add explain() method to transformation."""

        @transformation("test_transform")
        def test_transform(df):
            """Test transformation."""
            return df

        assert hasattr(test_transform, "explain")
        assert callable(test_transform.explain)

    def test_explain_decorator_registers_function(self):
        """Should register explanation function."""

        @transformation("filter_data")
        def filter_data(df, threshold):
            """Filter data above threshold."""
            return df[df.value > threshold]

        @filter_data.explain
        def explain(threshold, **context):
            return f"Filter records above {threshold}"

        assert filter_data.has_explanation()

    def test_get_explanation_returns_text(self):
        """Should return explanation text."""

        @transformation("calc_sum")
        def calc_sum(df, column):
            """Calculate sum."""
            return df[column].sum()

        @calc_sum.explain
        def explain(column, **context):
            return f"Calculate sum of {column} column"

        explanation = calc_sum.get_explanation(column="sales")
        assert explanation == "Calculate sum of sales column"

    def test_get_explanation_receives_context(self):
        """Should pass context kwargs to explanation function."""

        @transformation("efficiency_calc")
        def efficiency_calc(df, fuel_col, output_col):
            """Calculate efficiency."""
            df["efficiency"] = df[output_col] / df[fuel_col]
            return df

        @efficiency_calc.explain
        def explain(fuel_col, output_col, **context):
            plant = context.get("plant", "Unknown")
            asset = context.get("asset", "Unknown")
            return f"Calculate efficiency for {plant} {asset} using {fuel_col} and {output_col}"

        explanation = efficiency_calc.get_explanation(
            fuel_col="natural_gas", output_col="steam", plant="NKC", asset="Germ Dryer 1"
        )

        assert "NKC" in explanation
        assert "Germ Dryer 1" in explanation
        assert "natural_gas" in explanation
        assert "steam" in explanation

    def test_get_explanation_without_registration_fails(self):
        """Should raise error if explain() not registered."""

        @transformation("no_explanation")
        def no_explanation(df):
            """No explanation."""
            return df

        with pytest.raises(ValueError, match="No explanation registered"):
            no_explanation.get_explanation()

    def test_has_explanation_returns_false_initially(self):
        """Should return False if no explanation registered."""

        @transformation("test")
        def test_func(df):
            """Test function with proper docstring."""
            return df

        assert not test_func.has_explanation()

    def test_has_explanation_returns_true_after_registration(self):
        """Should return True after explanation registered."""

        @transformation("test")
        def test_func(df):
            """Test function with proper docstring."""
            return df

        @test_func.explain
        def explain(**context):
            return "Explanation"

        assert test_func.has_explanation()

    def test_transformation_still_callable(self):
        """Should still call original function."""
        import pandas as pd

        @transformation("double_values")
        def double_values(df, column):
            """Double column values."""
            df = df.copy()
            df[column] = df[column] * 2
            return df

        @double_values.explain
        def explain(column, **context):
            return f"Double values in {column}"

        # Test function still works
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = double_values(df, "value")

        assert result["value"].tolist() == [2, 4, 6]

    def test_multiple_transformations_with_explanations(self):
        """Should support multiple transformations each with explanations."""

        @transformation("transform1")
        def transform1(df):
            """First transform."""
            return df

        @transform1.explain
        def explain1(**context):
            return "Explanation 1"

        @transformation("transform2")
        def transform2(df):
            """Second transform."""
            return df

        @transform2.explain
        def explain2(**context):
            return "Explanation 2"

        assert transform1.get_explanation() == "Explanation 1"
        assert transform2.get_explanation() == "Explanation 2"
