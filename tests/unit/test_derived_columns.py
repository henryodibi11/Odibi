"""Tests for derived column functionality."""

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    DerivedGeneratorConfig,
    EntityConfig,
    RangeGeneratorConfig,
    SequentialGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
)
from odibi.simulation import SimulationEngine


class TestDerivedColumns:
    """Test derived column generation and dependency resolution."""

    def test_simple_derived_column(self):
        """Test basic derived column from another column."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="celsius",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
                ColumnGeneratorConfig(
                    name="fahrenheit",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="celsius * 1.8 + 32",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify conversion
        for row in rows:
            expected_f = row["celsius"] * 1.8 + 32
            assert row["fahrenheit"] == pytest.approx(expected_f), (
                f"Fahrenheit conversion incorrect: {row['celsius']}°C should be {expected_f}°F, "
                f"got {row['fahrenheit']}°F"
            )

    def test_conditional_derived_column(self):
        """Test derived column with conditional logic."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="temperature",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=50, max=100),
                ),
                ColumnGeneratorConfig(
                    name="status",
                    data_type=SimulationDataType.STRING,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="'CRITICAL' if temperature > 90 else 'NORMAL' if temperature > 70 else 'LOW'",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify conditional logic
        for row in rows:
            temp = row["temperature"]
            expected_status = "CRITICAL" if temp > 90 else "NORMAL" if temp > 70 else "LOW"
            assert row["status"] == expected_status, (
                f"Status incorrect for temp {temp}: expected {expected_status}, got {row['status']}"
            )

    def test_chain_derived_columns(self):
        """Test derived columns depending on other derived columns."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="base",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1, step=1),
                ),
                ColumnGeneratorConfig(
                    name="doubled",
                    data_type=SimulationDataType.INT,
                    generator=DerivedGeneratorConfig(type="derived", expression="base * 2"),
                ),
                ColumnGeneratorConfig(
                    name="quadrupled",
                    data_type=SimulationDataType.INT,
                    generator=DerivedGeneratorConfig(type="derived", expression="doubled * 2"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify chained calculations
        for row in rows:
            assert row["doubled"] == row["base"] * 2
            assert row["quadrupled"] == row["base"] * 4

    def test_multiple_column_dependencies(self):
        """Test derived column depending on multiple columns."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="width",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=10),
                ),
                ColumnGeneratorConfig(
                    name="height",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=10),
                ),
                ColumnGeneratorConfig(
                    name="area",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(type="derived", expression="width * height"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify area calculation
        for row in rows:
            assert row["area"] == pytest.approx(row["width"] * row["height"])

    def test_circular_dependency_detection(self):
        """Test that circular dependencies are detected."""
        with pytest.raises(ValueError, match="Circular dependency"):
            config = SimulationConfig(
                scope=SimulationScope(
                    start_time="2026-01-01T00:00:00Z",
                    timestep="1h",
                    row_count=5,
                ),
                entities=EntityConfig(count=1),
                columns=[
                    ColumnGeneratorConfig(
                        name="a",
                        data_type=SimulationDataType.FLOAT,
                        generator=DerivedGeneratorConfig(type="derived", expression="b * 2"),
                    ),
                    ColumnGeneratorConfig(
                        name="b",
                        data_type=SimulationDataType.FLOAT,
                        generator=DerivedGeneratorConfig(type="derived", expression="a * 2"),
                    ),
                ],
            )
            SimulationEngine(config)

    def test_undefined_column_reference(self):
        """Test error when derived column references undefined column."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=1,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="result",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="nonexistent_column * 2",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        with pytest.raises(ValueError, match="undefined column"):
            engine.generate()

    def test_derived_with_builtin_functions(self):
        """Test derived columns using allowed builtin functions."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=-100, max=100),
                ),
                ColumnGeneratorConfig(
                    name="absolute_value",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(type="derived", expression="abs(value)"),
                ),
                ColumnGeneratorConfig(
                    name="rounded",
                    data_type=SimulationDataType.INT,
                    generator=DerivedGeneratorConfig(
                        type="derived", expression="int(round(abs(value)))"
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify function calculations
        for row in rows:
            assert row["absolute_value"] == abs(row["value"])
            assert row["rounded"] == int(round(abs(row["value"])))

    def test_dependency_order_preserved(self):
        """Test that columns are generated in correct dependency order."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=3,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                # Define columns out of order - engine should reorder
                ColumnGeneratorConfig(
                    name="c",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(type="derived", expression="b + 1"),
                ),
                ColumnGeneratorConfig(
                    name="a",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1, step=1),
                ),
                ColumnGeneratorConfig(
                    name="b",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(type="derived", expression="a * 10"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should work despite out-of-order definition
        for row in rows:
            assert row["b"] == row["a"] * 10
            assert row["c"] == row["b"] + 1

    def test_complex_expression(self):
        """Test complex derived expression with multiple operations."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="input",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=100),
                ),
                ColumnGeneratorConfig(
                    name="output",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
                ColumnGeneratorConfig(
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="(output / input * 100) if input > 0 else 0",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify efficiency calculation
        for row in rows:
            expected = (row["output"] / row["input"] * 100) if row["input"] > 0 else 0
            assert row["efficiency"] == pytest.approx(expected)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
