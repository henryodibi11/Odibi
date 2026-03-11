"""Tests for mean_reversion_to parameter in random_walk generator.

This tests the ability for a random walk to track a dynamic setpoint column
instead of a static start value - critical for realistic process simulation.
"""

import pytest
from odibi.config import SimulationConfig
from odibi.simulation.generator import SimulationGenerator


def test_mean_reversion_to_basic():
    """Test basic mean_reversion_to functionality - walk tracks a changing reference."""
    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "reactor_01", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:10:00Z",
            "timestep_seconds": 60,
            "seed": 42,
            "columns": [
                # Reference column that drifts upward with trend
                {
                    "name": "setpoint_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 350.0,
                        "min": 300.0,
                        "max": 400.0,
                        "volatility": 0.2,
                        "mean_reversion": 0.0,  # Pure drift
                        "trend": 0.5,  # Increases by 0.5°C per timestep
                    },
                },
                # PV that tracks the setpoint
                {
                    "name": "actual_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 350.0,
                        "min": 300.0,
                        "max": 400.0,
                        "volatility": 1.0,
                        "mean_reversion": 0.3,  # Strong pull toward setpoint
                        "mean_reversion_to": "setpoint_temp_c",  # Track the SP column
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

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
    # Config with static start (baseline)
    config_static = SimulationConfig.model_validate(
        {
            "entities": [{"name": "tank_01", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:20:00Z",
            "timestep_seconds": 60,
            "seed": 100,
            "columns": [
                {
                    "name": "ambient_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,
                        "min": 15.0,
                        "max": 25.0,
                        "volatility": 0.3,
                        "mean_reversion": 0.0,
                        "trend": 0.2,  # Warming trend
                    },
                },
                {
                    "name": "tank_temp_static",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,  # Reverts to this static value
                        "min": 15.0,
                        "max": 30.0,
                        "volatility": 0.5,
                        "mean_reversion": 0.2,
                    },
                },
            ],
        }
    )

    # Config with dynamic mean_reversion_to
    config_dynamic = SimulationConfig.model_validate(
        {
            "entities": [{"name": "tank_01", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:20:00Z",
            "timestep_seconds": 60,
            "seed": 100,
            "columns": [
                {
                    "name": "ambient_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,
                        "min": 15.0,
                        "max": 25.0,
                        "volatility": 0.3,
                        "mean_reversion": 0.0,
                        "trend": 0.2,  # Warming trend
                    },
                },
                {
                    "name": "tank_temp_dynamic",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,
                        "min": 15.0,
                        "max": 30.0,
                        "volatility": 0.5,
                        "mean_reversion": 0.2,
                        "mean_reversion_to": "ambient_temp_c",  # Track ambient
                    },
                },
            ],
        }
    )

    df_static = SimulationGenerator(config_static).generate()
    df_dynamic = SimulationGenerator(config_dynamic).generate()

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

    assert (
        diff_dynamic < diff_static
    ), f"Dynamic tank should track ambient better (static diff: {diff_static:.2f}, dynamic diff: {diff_dynamic:.2f})"


def test_mean_reversion_to_dependency_order():
    """Test that mean_reversion_to respects column dependency order.
    
    If the referenced column hasn't been generated yet in the row,
    should fall back to static start value gracefully.
    """
    # This config violates dependency order (backward reference)
    # Should still work without crashing - falls back to start value
    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "test", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:05:00Z",
            "timestep_seconds": 60,
            "seed": 42,
            "columns": [
                # PV references SP that's defined AFTER it (bad order)
                {
                    "name": "pv",
                    "generator": {
                        "type": "random_walk",
                        "start": 100.0,
                        "min": 50.0,
                        "max": 150.0,
                        "volatility": 1.0,
                        "mean_reversion": 0.3,
                        "mean_reversion_to": "sp",  # Forward reference (doesn't exist yet)
                    },
                },
                {
                    "name": "sp",
                    "generator": {
                        "type": "random_walk",
                        "start": 100.0,
                        "min": 90.0,
                        "max": 110.0,
                        "volatility": 0.2,
                        "mean_reversion": 0.1,
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

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
    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "battery", "count": 3}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:10:00Z",
            "timestep_seconds": 60,
            "seed": 777,
            "columns": [
                {
                    "name": "ambient_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 25.0,
                        "min": 20.0,
                        "max": 30.0,
                        "volatility": 0.3,
                        "mean_reversion": 0.05,
                    },
                },
                {
                    "name": "battery_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 28.0,
                        "min": 22.0,
                        "max": 35.0,
                        "volatility": 0.5,
                        "mean_reversion": 0.15,
                        "mean_reversion_to": "ambient_temp_c",
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

    # Should have 3 entities × 11 rows each = 33 rows
    assert len(df) == 33

    # Each entity should have its own trajectory
    for entity_name in ["battery_0", "battery_1", "battery_2"]:
        entity_df = df[df["entity_id"] == entity_name]
        assert len(entity_df) == 11

        # Battery temp should track ambient for this entity
        diff = (entity_df["battery_temp_c"] - entity_df["ambient_temp_c"]).abs()
        assert diff.max() < 10.0, f"Battery temp for {entity_name} deviated too far from ambient"


def test_mean_reversion_to_renewable_energy_example():
    """Realistic renewable energy example: PV tracking setpoint with disturbances.
    
    Simulates a battery thermal management system where the cooling setpoint
    varies based on load, and the actual temperature tracks the setpoint.
    """
    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "BESS_Module_01", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T01:00:00Z",
            "timestep_seconds": 300,  # 5 min intervals
            "seed": 2026,
            "columns": [
                # Load-dependent setpoint (higher load → lower SP for cooling)
                {
                    "name": "power_kw",
                    "generator": {
                        "type": "random_walk",
                        "start": 500.0,
                        "min": 0.0,
                        "max": 1000.0,
                        "volatility": 50.0,
                        "mean_reversion": 0.05,
                    },
                },
                {
                    "name": "temp_setpoint_c",
                    "generator": {
                        "type": "derived",
                        "expression": "28.0 - (power_kw / 1000.0) * 3.0",  # 28°C at 0kW, 25°C at 1000kW
                    },
                },
                # Actual temperature tracks the dynamic setpoint
                {
                    "name": "battery_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 27.5,
                        "min": 20.0,
                        "max": 35.0,
                        "volatility": 0.3,
                        "mean_reversion": 0.2,  # Strong PID-like control
                        "mean_reversion_to": "temp_setpoint_c",
                    },
                },
                # Thermal deviation (PV - SP)
                {
                    "name": "temp_deviation_c",
                    "generator": {
                        "type": "derived",
                        "expression": "battery_temp_c - temp_setpoint_c",
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

    # Should have 13 rows (0-60 minutes in 5 min steps)
    assert len(df) == 13

    # Setpoint should vary with load
    assert df["temp_setpoint_c"].min() >= 25.0
    assert df["temp_setpoint_c"].max() <= 28.0

    # Temperature should track setpoint (deviation should be small)
    assert df["temp_deviation_c"].abs().max() < 2.0, "Temperature control too poor"

    # Mean deviation should be near zero (unbiased control)
    assert abs(df["temp_deviation_c"].mean()) < 0.5, "Temperature control has bias"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
