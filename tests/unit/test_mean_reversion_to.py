"""Tests for mean_reversion_to parameter in random_walk generator.

This tests the ability for a random walk to track a dynamic setpoint column
instead of a static start value - critical for realistic process simulation.
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


def test_mean_reversion_to_basic():
    """Test basic mean_reversion_to functionality - walk tracks a changing reference."""
    # Old window: 00:00 to 00:10 every 1m => 11 rows
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=11,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="reactor_01"),
        columns=[
            # Reference column that drifts upward with trend
            ColumnGeneratorConfig(
                name="setpoint_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=350.0,
                    min=300.0,
                    max=400.0,
                    volatility=0.2,
                    mean_reversion=0.0,  # Pure drift
                    trend=0.5,  # Increases by 0.5°C per timestep
                ),
            ),
            # PV that tracks the setpoint
            ColumnGeneratorConfig(
                name="actual_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=350.0,
                    min=300.0,
                    max=400.0,
                    volatility=1.0,
                    mean_reversion=0.3,  # Strong pull toward setpoint
                    mean_reversion_to="setpoint_temp_c",  # Track the SP column
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 11 rows (0-10 minutes inclusive)
    assert len(df) == 11

    # Check that setpoint increases over time (due to trend)
    assert df["setpoint_temp_c"].iloc[-1] > df["setpoint_temp_c"].iloc[0]

    # Check that actual_temp_c tracks setpoint_temp_c (not drifting to 350 static start)
    # The difference should stay bounded due to mean reversion toward the SP
    diff = (df["actual_temp_c"] - df["setpoint_temp_c"]).abs()

    # Maximum deviation should be reasonable (not unbounded)
    # With mean_reversion=0.3 and volatility=1.0, should stay within ~10°C
    assert diff.max() < 15.0, f"PV deviated too far from SP (max diff: {diff.max():.2f})"


def test_mean_reversion_to_vs_static_start():
    """Compare behavior: mean_reversion_to vs static start.

    Validates that mean_reversion_to actually tracks the dynamic column
    and doesn't just fall back to static start value.
    """
    # Old window: 00:00 to 00:20 every 1m => 21 rows
    config_static = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=21,
            seed=100,
        ),
        entities=EntityConfig(count=1, id_prefix="tank_01"),
        columns=[
            ColumnGeneratorConfig(
                name="ambient_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=20.0,
                    min=15.0,
                    max=25.0,
                    volatility=0.3,
                    mean_reversion=0.0,
                    trend=0.2,  # Warming trend
                ),
            ),
            ColumnGeneratorConfig(
                name="tank_temp_static",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=20.0,  # Reverts to this static value
                    min=15.0,
                    max=30.0,
                    volatility=0.5,
                    mean_reversion=0.2,
                ),
            ),
        ],
    )

    config_dynamic = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=21,
            seed=100,
        ),
        entities=EntityConfig(count=1, id_prefix="tank_01"),
        columns=[
            ColumnGeneratorConfig(
                name="ambient_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=20.0,
                    min=15.0,
                    max=25.0,
                    volatility=0.3,
                    mean_reversion=0.0,
                    trend=0.2,  # Warming trend
                ),
            ),
            ColumnGeneratorConfig(
                name="tank_temp_dynamic",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=20.0,
                    min=15.0,
                    max=30.0,
                    volatility=0.5,
                    mean_reversion=0.2,
                    mean_reversion_to="ambient_temp_c",  # Track ambient
                ),
            ),
        ],
    )

    df_static = pd.DataFrame(SimulationEngine(config_static).generate())
    df_dynamic = pd.DataFrame(SimulationEngine(config_dynamic).generate())

    # Ambient should warm over time (same in both)
    assert df_static["ambient_temp_c"].iloc[-1] > df_static["ambient_temp_c"].iloc[0]
    assert df_dynamic["ambient_temp_c"].iloc[-1] > df_dynamic["ambient_temp_c"].iloc[0]

    # Static version should revert toward 20°C (fixed start)
    # Dynamic version should track ambient (which increases)
    final_ambient = df_dynamic["ambient_temp_c"].iloc[-1]
    final_tank_static = df_static["tank_temp_static"].iloc[-1]
    final_tank_dynamic = df_dynamic["tank_temp_dynamic"].iloc[-1]

    # Dynamic tank should be closer to final ambient than static tank is
    diff_static = abs(final_tank_static - final_ambient)
    diff_dynamic = abs(final_tank_dynamic - final_ambient)

    assert diff_dynamic < diff_static, (
        f"Dynamic tank should track ambient better (static diff: {diff_static:.2f}, dynamic diff: {diff_dynamic:.2f})"
    )


def test_mean_reversion_to_dependency_order():
    """Test that mean_reversion_to respects column dependency order.

    If the referenced column hasn't been generated yet in the row,
    should fall back to static start value gracefully.
    """
    # Old window: 00:00 to 00:05 every 1m => 6 rows
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=6,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="test"),
        columns=[
            # PV references SP that's defined AFTER it (bad order)
            ColumnGeneratorConfig(
                name="pv",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=100.0,
                    min=50.0,
                    max=150.0,
                    volatility=1.0,
                    mean_reversion=0.3,
                    mean_reversion_to="sp",  # Forward reference (doesn't exist yet)
                ),
            ),
            ColumnGeneratorConfig(
                name="sp",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=100.0,
                    min=90.0,
                    max=110.0,
                    volatility=0.2,
                    mean_reversion=0.1,
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should complete without error
    assert len(df) == 6
    assert "pv" in df.columns
    assert "sp" in df.columns

    # PV should fall back to reverting toward static start (100.0)
    # since sp isn't available during PV generation
    pv_mean = df["pv"].mean()
    assert 80.0 < pv_mean < 120.0  # Should hover around 100 (start value)


def test_mean_reversion_to_with_multiple_entities():
    """Test mean_reversion_to with multiple independent entities.

    Each entity should track its own reference column independently.
    """
    # Old window: 00:00 to 00:10 every 1m => 11 rows per entity
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=11,
            seed=777,
        ),
        entities=EntityConfig(count=3, id_prefix="battery"),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="ambient_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=25.0,
                    min=20.0,
                    max=30.0,
                    volatility=0.3,
                    mean_reversion=0.05,
                ),
            ),
            ColumnGeneratorConfig(
                name="battery_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=28.0,
                    min=22.0,
                    max=35.0,
                    volatility=0.5,
                    mean_reversion=0.15,
                    mean_reversion_to="ambient_temp_c",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 3 entities × 11 rows each = 33 rows
    assert len(df) == 33

    # Each entity should have its own trajectory
    # Sequential format with id_prefix="battery" creates "battery01", "battery02", "battery03"
    for entity_name in ["battery01", "battery02", "battery03"]:
        entity_df = df[df["entity_id"] == entity_name]
        assert len(entity_df) == 11, f"Entity {entity_name} should have 11 rows"

        # Battery temp should track ambient for this entity
        diff = (entity_df["battery_temp_c"] - entity_df["ambient_temp_c"]).abs()
        assert diff.max() < 10.0, f"Battery temp for {entity_name} deviated too far from ambient"


def test_mean_reversion_to_renewable_energy_example():
    """Realistic renewable energy example: PV tracking setpoint with disturbances.

    Simulates a battery thermal management system where the cooling setpoint
    varies based on load, and the actual temperature tracks the setpoint.
    """
    # Old window: 1 hour at 5 min => 13 rows
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="5m",
            row_count=13,
            seed=2026,
        ),
        entities=EntityConfig(count=1, id_prefix="BESS_Module_01"),
        columns=[
            # Load-dependent setpoint (higher load → lower SP for cooling)
            ColumnGeneratorConfig(
                name="power_kw",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=500.0,
                    min=0.0,
                    max=1000.0,
                    volatility=50.0,
                    mean_reversion=0.05,
                ),
            ),
            ColumnGeneratorConfig(
                name="temp_setpoint_c",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="28.0 - (power_kw / 1000.0) * 3.0",  # 28°C at 0kW, 25°C at 1000kW
                ),
            ),
            # Actual temperature tracks the dynamic setpoint
            ColumnGeneratorConfig(
                name="battery_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=27.5,
                    min=20.0,
                    max=35.0,
                    volatility=0.3,
                    mean_reversion=0.2,  # Strong PID-like control
                    mean_reversion_to="temp_setpoint_c",
                ),
            ),
            # Thermal deviation (PV - SP)
            ColumnGeneratorConfig(
                name="temp_deviation_c",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="battery_temp_c - temp_setpoint_c",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 13 rows (0-60 minutes in 5 min steps)
    assert len(df) == 13

    # Setpoint should vary with load
    assert df["temp_setpoint_c"].min() >= 25.0
    assert df["temp_setpoint_c"].max() <= 28.0

    # Temperature should track setpoint (deviation should be small)
    assert df["temp_deviation_c"].abs().max() < 2.0, "Temperature control too poor"

    # Mean deviation should be reasonably small (random walk will have some bias)
    assert abs(df["temp_deviation_c"].mean()) < 1.0, "Temperature control has excessive bias"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
