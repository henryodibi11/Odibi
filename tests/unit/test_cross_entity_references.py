"""Tests for cross-entity references in simulation.

Tests the ability for entities to reference other entities' column values
at the same timestamp using dot notation (e.g., Tank_A.level).
"""

import pandas as pd
import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    DerivedGeneratorConfig,
    EntityConfig,
    RandomWalkGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
)
from odibi.simulation import SimulationEngine


def test_basic_cross_entity_reference():
    """Test basic cross-entity reference: Tank_B depends on Tank_A."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(names=["Tank_A", "Tank_B"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="level",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=50.0,
                    min=0.0,
                    max=100.0,
                    volatility=2.0,
                ),
            ),
            # Tank_B's flow depends on Tank_A's level
            ColumnGeneratorConfig(
                name="flow_from_tank_a",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="Tank_A.level * 0.1 if entity_id == 'Tank_B' else 0.0",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 2 entities × 10 rows = 20 rows
    assert len(df) == 20

    # Split by entity
    tank_a = df[df["entity_id"] == "Tank_A"].reset_index(drop=True)
    tank_b = df[df["entity_id"] == "Tank_B"].reset_index(drop=True)

    assert len(tank_a) == 10
    assert len(tank_b) == 10

    # Tank_A should have flow_from_tank_a = 0
    assert (tank_a["flow_from_tank_a"] == 0.0).all()

    # Tank_B's flow should equal Tank_A's level * 0.1
    for i in range(10):
        expected_flow = tank_a.loc[i, "level"] * 0.1
        actual_flow = tank_b.loc[i, "flow_from_tank_a"]
        assert abs(actual_flow - expected_flow) < 0.01, (
            f"Row {i}: Tank_B flow should be Tank_A.level * 0.1. "
            f"Expected {expected_flow:.2f}, got {actual_flow:.2f}"
        )


def test_cross_entity_with_arithmetic():
    """Test cross-entity reference with arithmetic expressions."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=100,
        ),
        entities=EntityConfig(names=["Source_A", "Source_B", "Destination"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="flow",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=10.0),
            ),
            # Destination receives from both sources
            ColumnGeneratorConfig(
                name="total_inflow",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "Source_A.flow + Source_B.flow if entity_id == 'Destination' else 0.0"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    dest = df[df["entity_id"] == "Destination"]
    assert len(dest) == 5
    # Should be 10 + 10 = 20
    assert (dest["total_inflow"] == 20.0).all()


def test_cross_entity_chain():
    """Test chain of dependencies: A -> B -> C."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=200,
        ),
        entities=EntityConfig(names=["A", "B", "C"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=100.0),
            ),
            # B depends on A
            ColumnGeneratorConfig(
                name="from_upstream",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="A.value * 2.0 if entity_id == 'B' else (B.value * 3.0 if entity_id == 'C' else 0.0)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    a = df[df["entity_id"] == "A"].reset_index(drop=True)
    b = df[df["entity_id"] == "B"].reset_index(drop=True)
    c = df[df["entity_id"] == "C"].reset_index(drop=True)

    # A.from_upstream = 0
    assert (a["from_upstream"] == 0.0).all()

    # B.from_upstream = A.value * 2 = 100 * 2 = 200
    assert (b["from_upstream"] == 200.0).all()

    # C.from_upstream = B.value * 3 = 100 * 3 = 300
    assert (c["from_upstream"] == 300.0).all()


def test_circular_dependency_detection():
    """Test that circular cross-entity dependencies are detected."""
    # A depends on B, B depends on A
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(names=["A", "B"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="B.value if entity_id == 'A' else A.value",
                ),
            ),
        ],
    )

    # This creates a circular dependency within the column itself
    # (both A and B reference each other in the same column expression)
    with pytest.raises(ValueError, match="Circular dependency detected"):
        SimulationEngine(config)


def test_cross_entity_prev_not_supported():
    """Test that Entity.prev() is rejected with clear error."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(names=["A", "B"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="A.prev('value', 0) if entity_id == 'B' else 10.0",
                ),
            ),
        ],
    )

    with pytest.raises(ValueError, match="cross-entity prev.*not supported"):
        SimulationEngine(config)


def test_cross_entity_with_stateful_functions():
    """Test cross-entity refs combined with prev() within same entity."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=300,
        ),
        entities=EntityConfig(names=["Tank_A", "Tank_B"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            # Tank_A has constant outflow
            ColumnGeneratorConfig(
                name="outflow",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=5.0),
            ),
            # Tank_B level integrates inflow from Tank_A minus its own outflow
            ColumnGeneratorConfig(
                name="level",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "prev('level', 50.0) + Tank_A.outflow - outflow "
                        "if entity_id == 'Tank_B' else 100.0"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    tank_b = df[df["entity_id"] == "Tank_B"].reset_index(drop=True)

    # Tank_B starts at 50, receives 5 from Tank_A, loses 5 (its own outflow)
    # So level should stay constant at 50
    assert abs(tank_b["level"].iloc[0] - 50.0) < 0.01
    assert abs(tank_b["level"].iloc[-1] - 50.0) < 0.01


def test_cross_entity_multiple_references():
    """Test entity referencing multiple other entities."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(names=["A", "B", "C", "Sum"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=10.0),
            ),
            ColumnGeneratorConfig(
                name="total",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="A.value + B.value + C.value if entity_id == 'Sum' else 0.0",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    sum_entity = df[df["entity_id"] == "Sum"]
    # Should be 10 + 10 + 10 = 30
    assert (sum_entity["total"] == 30.0).all()


def test_no_cross_entity_refs_uses_fast_path():
    """Test that simulations without cross-entity refs use entity-major generation."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(names=["Entity_A", "Entity_B"]),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=50.0,
                    min=0.0,
                    max=100.0,
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)

    # Should use entity-major (cross_entity_enabled = False)
    assert engine.cross_entity_enabled is False
    assert engine.entity_proxies == {}

    rows = engine.generate()
    assert len(rows) == 20  # 2 entities × 10 rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
