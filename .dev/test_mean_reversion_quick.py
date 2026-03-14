"""Quick validation script for mean_reversion_to feature.

Run this directly to verify the implementation works before committing.
"""

import sys
import os

# Add odibi to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from odibi.config import SimulationConfig
from odibi.simulation.generator import SimulationGenerator


def test_basic():
    """Test basic mean_reversion_to functionality."""
    print("\n=== Test 1: Basic mean_reversion_to tracking ===")

    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "reactor", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:10:00Z",
            "timestep_seconds": 60,
            "seed": 42,
            "columns": [
                {
                    "name": "setpoint",
                    "generator": {
                        "type": "random_walk",
                        "start": 350.0,
                        "min": 300.0,
                        "max": 400.0,
                        "volatility": 0.2,
                        "mean_reversion": 0.0,
                        "trend": 0.5,  # Increases
                    },
                },
                {
                    "name": "actual",
                    "generator": {
                        "type": "random_walk",
                        "start": 350.0,
                        "min": 300.0,
                        "max": 400.0,
                        "volatility": 1.0,
                        "mean_reversion": 0.3,
                        "mean_reversion_to": "setpoint",  # KEY: Track dynamic setpoint
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

    print(f"Generated {len(df)} rows")
    print("\nFirst 5 rows:")
    print(df[["setpoint", "actual"]].head())
    print("\nLast 5 rows:")
    print(df[["setpoint", "actual"]].tail())

    # Validation
    assert len(df) == 11, f"Expected 11 rows, got {len(df)}"

    # Setpoint should increase due to trend
    sp_increase = df["setpoint"].iloc[-1] - df["setpoint"].iloc[0]
    print(f"\nSetpoint increased by: {sp_increase:.2f}°C")
    assert sp_increase > 0, "Setpoint should increase due to trend"

    # Actual should track setpoint (bounded deviation)
    diff = (df["actual"] - df["setpoint"]).abs()
    max_diff = diff.max()
    print(f"Max deviation from setpoint: {max_diff:.2f}°C")
    assert max_diff < 15.0, f"Deviation too large: {max_diff:.2f}°C"

    print("✅ Test 1 PASSED\n")


def test_comparison():
    """Compare static vs dynamic mean reversion."""
    print("\n=== Test 2: Static vs Dynamic comparison ===")

    # Static reversion
    config_static = SimulationConfig.model_validate(
        {
            "entities": [{"name": "tank", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:20:00Z",
            "timestep_seconds": 60,
            "seed": 100,
            "columns": [
                {
                    "name": "ambient",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,
                        "min": 15.0,
                        "max": 25.0,
                        "volatility": 0.3,
                        "trend": 0.2,  # Warming
                    },
                },
                {
                    "name": "tank_temp_static",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,  # Reverts to 20°C
                        "min": 15.0,
                        "max": 30.0,
                        "volatility": 0.5,
                        "mean_reversion": 0.2,
                    },
                },
            ],
        }
    )

    # Dynamic reversion
    config_dynamic = SimulationConfig.model_validate(
        {
            "entities": [{"name": "tank", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T00:20:00Z",
            "timestep_seconds": 60,
            "seed": 100,
            "columns": [
                {
                    "name": "ambient",
                    "generator": {
                        "type": "random_walk",
                        "start": 20.0,
                        "min": 15.0,
                        "max": 25.0,
                        "volatility": 0.3,
                        "trend": 0.2,  # Warming
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
                        "mean_reversion_to": "ambient",  # Track ambient
                    },
                },
            ],
        }
    )

    df_static = SimulationGenerator(config_static).generate()
    df_dynamic = SimulationGenerator(config_dynamic).generate()

    final_ambient = df_dynamic["ambient"].iloc[-1]
    final_static = df_static["tank_temp_static"].iloc[-1]
    final_dynamic = df_dynamic["tank_temp_dynamic"].iloc[-1]

    print(f"Final ambient: {final_ambient:.2f}°C")
    print(f"Final tank (static reversion to 20°C): {final_static:.2f}°C")
    print(f"Final tank (dynamic tracking ambient): {final_dynamic:.2f}°C")

    diff_static = abs(final_static - final_ambient)
    diff_dynamic = abs(final_dynamic - final_ambient)

    print(f"\nDeviation from ambient (static): {diff_static:.2f}°C")
    print(f"Deviation from ambient (dynamic): {diff_dynamic:.2f}°C")

    assert diff_dynamic < diff_static, "Dynamic should track ambient better"
    print("✅ Test 2 PASSED\n")


def test_renewable_energy():
    """Realistic renewable energy example."""
    print("\n=== Test 3: Renewable energy (battery thermal) ===")

    config = SimulationConfig.model_validate(
        {
            "entities": [{"name": "BESS", "count": 1}],
            "start_time": "2026-03-11T00:00:00Z",
            "end_time": "2026-03-11T01:00:00Z",
            "timestep_seconds": 300,  # 5 min
            "seed": 2026,
            "columns": [
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
                    "name": "temp_sp_c",
                    "generator": {
                        "type": "derived",
                        "expression": "28.0 - (power_kw / 1000.0) * 3.0",  # Lower SP at high load
                    },
                },
                {
                    "name": "battery_temp_c",
                    "generator": {
                        "type": "random_walk",
                        "start": 27.5,
                        "min": 20.0,
                        "max": 35.0,
                        "volatility": 0.3,
                        "mean_reversion": 0.2,
                        "mean_reversion_to": "temp_sp_c",  # Track dynamic SP
                    },
                },
                {
                    "name": "temp_error_c",
                    "generator": {
                        "type": "derived",
                        "expression": "battery_temp_c - temp_sp_c",
                    },
                },
            ],
        }
    )

    generator = SimulationGenerator(config)
    df = generator.generate()

    print(f"Generated {len(df)} rows (5-min intervals over 1 hour)")
    print("\nSample data:")
    print(df[["power_kw", "temp_sp_c", "battery_temp_c", "temp_error_c"]].head(5))

    # Setpoint should vary with load
    sp_range = df["temp_sp_c"].max() - df["temp_sp_c"].min()
    print(f"\nSetpoint range: {sp_range:.2f}°C")
    assert sp_range > 0, "Setpoint should vary"

    # Temperature control should be tight
    max_error = df["temp_error_c"].abs().max()
    mean_error = abs(df["temp_error_c"].mean())

    print(f"Max temperature error: {max_error:.2f}°C")
    print(f"Mean temperature error: {mean_error:.2f}°C")

    assert max_error < 2.0, f"Control too poor: {max_error:.2f}°C"
    assert mean_error < 0.5, f"Control bias too large: {mean_error:.2f}°C"

    print("✅ Test 3 PASSED\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing mean_reversion_to implementation")
    print("=" * 60)

    try:
        test_basic()
        test_comparison()
        test_renewable_energy()

        print("=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        print("\nFeature ready for commit!")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
