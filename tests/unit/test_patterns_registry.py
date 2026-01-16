"""Unit tests for patterns registry."""

import pytest

from odibi.patterns import get_pattern_class
from odibi.patterns.aggregation import AggregationPattern
from odibi.patterns.base import Pattern
from odibi.patterns.date_dimension import DateDimensionPattern
from odibi.patterns.dimension import DimensionPattern
from odibi.patterns.fact import FactPattern
from odibi.patterns.merge import MergePattern
from odibi.patterns.scd2 import SCD2Pattern


class TestGetPatternClass:
    """Test get_pattern_class function."""

    def test_get_scd2_pattern(self):
        """Test getting SCD2Pattern."""
        pattern_class = get_pattern_class("scd2")
        assert pattern_class is SCD2Pattern

    def test_get_merge_pattern(self):
        """Test getting MergePattern."""
        pattern_class = get_pattern_class("merge")
        assert pattern_class is MergePattern

    def test_get_dimension_pattern(self):
        """Test getting DimensionPattern."""
        pattern_class = get_pattern_class("dimension")
        assert pattern_class is DimensionPattern

    def test_get_date_dimension_pattern(self):
        """Test getting DateDimensionPattern."""
        pattern_class = get_pattern_class("date_dimension")
        assert pattern_class is DateDimensionPattern

    def test_get_aggregation_pattern(self):
        """Test getting AggregationPattern."""
        pattern_class = get_pattern_class("aggregation")
        assert pattern_class is AggregationPattern

    def test_get_fact_pattern(self):
        """Test getting FactPattern."""
        pattern_class = get_pattern_class("fact")
        assert pattern_class is FactPattern

    def test_unknown_pattern_raises_error(self):
        """Test that unknown pattern name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown pattern"):
            get_pattern_class("nonexistent")

    def test_error_message_includes_available_patterns(self):
        """Test that error message lists available patterns."""
        with pytest.raises(ValueError) as exc_info:
            get_pattern_class("invalid")

        error_msg = str(exc_info.value)
        assert "scd2" in error_msg
        assert "merge" in error_msg
        assert "dimension" in error_msg
        assert "fact" in error_msg

    def test_case_sensitive_pattern_names(self):
        """Test that pattern names are case-sensitive."""
        with pytest.raises(ValueError, match="Unknown pattern"):
            get_pattern_class("SCD2")

        with pytest.raises(ValueError, match="Unknown pattern"):
            get_pattern_class("Merge")


class TestPatternRegistry:
    """Test pattern registry completeness."""

    def test_all_patterns_extend_base(self):
        """Test that all registered patterns extend Pattern base class."""
        patterns = [
            SCD2Pattern,
            MergePattern,
            DimensionPattern,
            DateDimensionPattern,
            AggregationPattern,
            FactPattern,
        ]

        for pattern_class in patterns:
            assert issubclass(pattern_class, Pattern)

    def test_all_patterns_have_execute(self):
        """Test that all patterns implement execute method."""
        patterns = [
            SCD2Pattern,
            MergePattern,
            DimensionPattern,
            DateDimensionPattern,
            AggregationPattern,
            FactPattern,
        ]

        for pattern_class in patterns:
            assert hasattr(pattern_class, "execute")
            assert callable(getattr(pattern_class, "execute"))

    def test_all_patterns_have_validate(self):
        """Test that all patterns have validate method."""
        patterns = [
            SCD2Pattern,
            MergePattern,
            DimensionPattern,
            DateDimensionPattern,
            AggregationPattern,
            FactPattern,
        ]

        for pattern_class in patterns:
            assert hasattr(pattern_class, "validate")
            assert callable(getattr(pattern_class, "validate"))
