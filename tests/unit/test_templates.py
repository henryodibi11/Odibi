"""Tests for explanation templates."""

from odibi.transformations.templates import (
    purpose_detail_result,
    with_formula,
    table_explanation,
    simple_explanation,
)


class TestExplanationTemplates:
    """Tests for explanation template functions."""

    def test_purpose_detail_result(self):
        """Should generate standard explanation format."""
        explanation = purpose_detail_result(
            purpose="Test purpose",
            details=["Detail 1", "Detail 2"],
            result="Test result",
        )

        assert "**Purpose:**" in explanation
        assert "Test purpose" in explanation
        assert "- Detail 1" in explanation
        assert "- Detail 2" in explanation
        assert "**Result:**" in explanation
        assert "Test result" in explanation

    def test_with_formula(self):
        """Should generate explanation with formula."""
        explanation = with_formula(
            purpose="Calculate efficiency",
            details=["Uses fuel and output"],
            formula="efficiency = output / fuel",
            result="Efficiency percentage",
        )

        assert "**Purpose:**" in explanation
        assert "**Formula:**" in explanation
        assert "efficiency = output / fuel" in explanation
        assert "**Result:**" in explanation

    def test_table_explanation(self):
        """Should generate explanation with table."""
        table_data = [
            {"Column": "Fuel", "Type": "Input"},
            {"Column": "Output", "Type": "Result"},
        ]

        explanation = table_explanation(
            purpose="Data overview",
            details=["Shows columns"],
            table_data=table_data,
            result="Complete dataset",
        )

        assert "| Column | Type |" in explanation
        assert "| Fuel | Input |" in explanation
        assert "| Output | Result |" in explanation
        assert "**Purpose:**" in explanation
        assert "**Result:**" in explanation

    def test_table_explanation_empty_data(self):
        """Should handle empty table data gracefully."""
        explanation = table_explanation(
            purpose="Empty test",
            details=["No data"],
            table_data=[],
            result="Falls back to standard format",
        )

        assert "**Purpose:**" in explanation
        assert "**Details:**" in explanation
        assert "**Result:**" in explanation
        # Should not have table
        assert "|" not in explanation

    def test_simple_explanation(self):
        """Should generate simple one-liner explanation."""
        explanation = simple_explanation(
            purpose="Filter high values", result="Records above threshold"
        )

        assert "Filter high values" in explanation
        assert "Records above threshold" in explanation
        assert "→" in explanation

    def test_purpose_detail_result_with_multiple_details(self):
        """Should handle multiple detail items."""
        details = [
            "First detail",
            "Second detail",
            "Third detail",
            "Fourth detail",
        ]

        explanation = purpose_detail_result(
            purpose="Multi-detail test", details=details, result="All details shown"
        )

        for detail in details:
            assert detail in explanation
            assert f"- {detail}" in explanation

    def test_with_formula_multiline_formula(self):
        """Should handle multiline formulas."""
        formula = """total_efficiency = (
    output_power / input_fuel
) * 100"""

        explanation = with_formula(
            purpose="Calculate efficiency",
            details=["Complex calculation"],
            formula=formula,
            result="Percentage value",
        )

        assert "output_power / input_fuel" in explanation
        assert "* 100" in explanation

    def test_table_explanation_multiple_columns(self):
        """Should handle tables with multiple columns."""
        table_data = [
            {"Name": "Plant A", "Capacity": "100 MW", "Status": "Active"},
            {"Name": "Plant B", "Capacity": "150 MW", "Status": "Maintenance"},
            {"Name": "Plant C", "Capacity": "200 MW", "Status": "Active"},
        ]

        explanation = table_explanation(
            purpose="Plant overview",
            details=["Shows all plants", "Includes capacity and status"],
            table_data=table_data,
            result="Complete plant inventory",
        )

        # Check headers
        assert "| Name | Capacity | Status |" in explanation
        # Check separator
        assert "| --- | --- | --- |" in explanation
        # Check data rows
        assert "| Plant A | 100 MW | Active |" in explanation
        assert "| Plant B | 150 MW | Maintenance |" in explanation
        assert "| Plant C | 200 MW | Active |" in explanation
