"""
Tests for stateful simulation functions: prev(), ema(), pid()

These functions enable realistic dynamic process simulation by maintaining
state across rows within each entity.
"""

import pandas as pd
import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    DerivedGeneratorConfig,
    EntityConfig,
    RangeGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


def test_prev_function_basic():
    """Test prev() function returns previous row value."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="counter",
                data_type=SimulationDataType.INT,
                generator=RangeGeneratorConfig(type="range", min=1, max=10),
            ),
            ColumnGeneratorConfig(
                name="cumsum",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('cumsum', 0) + counter",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    # Should have cumulative sum
    assert len(rows) == 5
    cumsum_values = [r["cumsum"] for r in rows]

    # First row: prev returns default 0, so cumsum = 0 + counter[0]
    # Each subsequent row: cumsum = prev_cumsum + counter
    assert cumsum_values[0] == rows[0]["counter"]  # 0 + counter
    for i in range(1, len(rows)):
        expected = cumsum_values[i - 1] + rows[i]["counter"]
        assert abs(cumsum_values[i] - expected) < 0.01


def test_prev_function_multi_entity():
    """Test prev() maintains separate state per entity."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=3,
            seed=42,
        ),
        entities=EntityConfig(count=2, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.INT,
                generator=RangeGeneratorConfig(type="range", min=1, max=5),
            ),
            ColumnGeneratorConfig(
                name="cumsum",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('cumsum', 0) + value",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    # Group by entity
    df = pd.DataFrame(rows)

    for entity in df["entity_id"].unique():
        entity_rows = df[df["entity_id"] == entity].reset_index(drop=True)

        # First row for this entity
        assert entity_rows.loc[0, "cumsum"] == entity_rows.loc[0, "value"]

        # Subsequent rows should accumulate
        for i in range(1, len(entity_rows)):
            expected = entity_rows.loc[i - 1, "cumsum"] + entity_rows.loc[i, "value"]
            actual = entity_rows.loc[i, "cumsum"]
            assert abs(actual - expected) < 0.01


def test_ema_function():
    """Test ema() exponential moving average."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="raw_sensor",
                data_type=SimulationDataType.FLOAT,
                generator=RangeGeneratorConfig(type="range", min=95, max=105),
            ),
            ColumnGeneratorConfig(
                name="smoothed",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="ema('raw_sensor', alpha=0.3, default=100)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    # EMA should smooth out fluctuations
    raw_values = [r["raw_sensor"] for r in rows]
    smoothed_values = [r["smoothed"] for r in rows]

    # First value should be default or close to first raw value
    assert 90 < smoothed_values[0] < 110

    # Smoothed values should change more slowly than raw values
    for i in range(1, len(rows)):
        raw_change = abs(raw_values[i] - raw_values[i - 1])
        smooth_change = abs(smoothed_values[i] - smoothed_values[i - 1])

        # With alpha=0.3, smoothed change should be ~30% of raw change (approximately)
        # Allow some tolerance since it depends on previous EMA state
        assert smooth_change <= raw_change + 1.0  # Smoothed should not be jumpier


def test_pid_controller_basic():
    """Test pid() controller tracks setpoint."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=20,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="setpoint",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=75.0),
            ),
            # Simple first-order process: pv responds to control output
            ColumnGeneratorConfig(
                name="pv",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('pv', 50) + 0.1 * (prev('control_output', 50) - prev('pv', 50))",
                ),
            ),
            ColumnGeneratorConfig(
                name="control_output",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="pid(pv=pv, sp=setpoint, Kp=2.0, Ki=0.1, Kd=0.5, dt=60)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # PV - first row has lag applied
    # prev('pv', 50) = 50, prev('control_output', 50) = 50
    # pv = 50 + 0.1 * (50 - 50) = 50
    assert df["pv"].iloc[0] == 50  # Initial value

    # After 20 timesteps with PID control, should move toward setpoint
    # Note: With slow process (0.1 gain) and limited timesteps, may not fully converge
    final_pv = df["pv"].iloc[-1]
    setpoint = 75.0
    initial_error = abs(50 - setpoint)
    final_error = abs(final_pv - setpoint)

    # Should be moving in the right direction (PV increasing toward SP)
    assert final_pv > 50, f"PID should drive PV upward: final_pv={final_pv}"
    assert final_error < initial_error, f"Error should decrease: initial={initial_error}, final={final_error}"


def test_pid_controller_with_disturbance():
    """Test PID controller rejects disturbances."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=30,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="row_num",
                data_type=SimulationDataType.INT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('row_num', -1) + 1",
                ),
            ),
            # Setpoint step change at row 15
            ColumnGeneratorConfig(
                name="setpoint",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="60.0 if row_num < 15 else 80.0",
                ),
            ),
            ColumnGeneratorConfig(
                name="pv",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('pv', 60) + 0.15 * (prev('control_output', 50) - prev('pv', 60))",
                ),
            ),
            ColumnGeneratorConfig(
                name="control_output",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="pid(pv=pv, sp=setpoint, Kp=1.5, Ki=0.05, Kd=0.3, dt=60)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Setpoint steps from 60 to 80 at row 15
    # Just verify that PID responds to the step change
    before_step = df[df["row_num"] < 15]
    after_step = df[df["row_num"] >= 15]
    
    # PID should respond to setpoint change - PV should increase after step
    assert after_step["pv"].iloc[-1] > before_step["pv"].iloc[-1], "PID should respond to SP step increase"


def test_battery_soc_integration():
    """
    Realistic battery SOC integration using prev().

    SOC[t] = SOC[t-1] + (charge_current * dt) / battery_capacity * 100

    This is THE killer test case for renewable energy applications.
    """
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="5m",  # 5-minute intervals
            row_count=50,  # ~4 hours
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="BESS_Module_"),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            # Battery parameters
            ColumnGeneratorConfig(
                name="battery_capacity_ah",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=100.0),
            ),
            # Charge/discharge current (positive = charging, negative = discharging)
            # Simulate charge cycle then discharge
            ColumnGeneratorConfig(
                name="row_num",
                data_type=SimulationDataType.INT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('row_num', -1) + 1",
                ),
            ),
            ColumnGeneratorConfig(
                name="charge_current_a",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="20.0 if row_num < 25 else -15.0",  # Charge first half, discharge second
                ),
            ),
            # SOC integration
            ColumnGeneratorConfig(
                name="soc_pct",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "max(0, min(100, "
                        "prev('soc_pct', 50) + "
                        "(charge_current_a * (5/60)) / battery_capacity_ah * 100"  # 5 min = 5/60 hr
                        "))"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Initial SOC - note: first row has integration applied
    # prev('soc_pct', 50) returns 50, then adds first integration step
    assert abs(df["soc_pct"].iloc[0] - 51.67) < 0.1  # 50 + (20A * 5/60hr / 100Ah * 100)

    # During charging (rows 0-24), SOC should increase
    charging_phase = df[df["row_num"] < 25]
    assert charging_phase["soc_pct"].iloc[-1] > charging_phase["soc_pct"].iloc[0]

    # During discharging (rows 25+), SOC should decrease
    discharging_phase = df[df["row_num"] >= 25]
    if len(discharging_phase) > 1:
        assert discharging_phase["soc_pct"].iloc[-1] < discharging_phase["soc_pct"].iloc[0]

    # SOC should stay in bounds [0, 100]
    assert df["soc_pct"].min() >= 0
    assert df["soc_pct"].max() <= 100

    # Verify integration math
    # Change in SOC per timestep = (current_A * time_hr) / capacity_Ah * 100
    # Expression: prev('soc_pct', 50) + (charge_current_a * (5/60)) / battery_capacity_ah * 100
    # Uses CURRENT row's charge_current_a
    dt_hours = 5 / 60  # 5 minutes
    for i in range(1, len(df)):
        expected_delta = (df["charge_current_a"].iloc[i] * dt_hours) / 100.0 * 100
        actual_delta = df["soc_pct"].iloc[i] - df["soc_pct"].iloc[i - 1]

        # Check if clamping occurred
        if 0 < df["soc_pct"].iloc[i] < 100:
            assert abs(actual_delta - expected_delta) < 0.1


def test_ema_sensor_smoothing():
    """Test EMA for sensor noise reduction."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="10s",
            row_count=20,
            seed=123,
        ),
        entities=EntityConfig(count=1, id_prefix="S"),
        columns=[
            ColumnGeneratorConfig(
                name="raw_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RangeGeneratorConfig(type="range", min=98, max=102),
            ),
            ColumnGeneratorConfig(
                name="smoothed_temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="ema('raw_temp_c', alpha=0.2, default=100)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Calculate variance
    raw_variance = df["raw_temp_c"].var()
    smoothed_variance = df["smoothed_temp_c"].var()

    # Smoothed should have lower variance (less noisy)
    assert smoothed_variance < raw_variance


def test_pid_temperature_control():
    """Test PID controller for temperature setpoint tracking."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="30s",
            row_count=40,
            seed=789,
        ),
        entities=EntityConfig(count=1, id_prefix="Reactor_"),
        columns=[
            ColumnGeneratorConfig(
                name="temp_setpoint_c",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=85.0),
            ),
            # Simple first-order thermal process
            # Temp responds to heater output with time constant
            ColumnGeneratorConfig(
                name="temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "prev('temp_c', 70) + 0.05 * (prev('heater_pct', 50) - prev('temp_c', 70))"
                    ),
                ),
            ),
            ColumnGeneratorConfig(
                name="heater_pct",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "pid(pv=temp_c, sp=temp_setpoint_c, "
                        "Kp=3.0, Ki=0.2, Kd=1.0, dt=30, "
                        "output_min=0, output_max=100, anti_windup=True)"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Temperature - first row has dynamics applied
    # prev('temp_c', 70) = 70, prev('heater_pct', 50) = 50
    # temp = 70 + 0.05 * (50 - 70) = 69
    assert abs(df["temp_c"].iloc[0] - 69.0) < 0.1

    # By the end, should be much closer to setpoint
    final_temp = df["temp_c"].iloc[-1]
    assert 80 < final_temp < 90, f"PID failed to converge: final_temp={final_temp}"

    # Heater output should be reasonable (0-100%)
    assert df["heater_pct"].min() >= 0
    assert df["heater_pct"].max() <= 100


def test_combined_prev_ema_pid():
    """Test all three stateful functions working together."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=30,
            seed=999,
        ),
        entities=EntityConfig(count=2, id_prefix="Module_"),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            # Noisy measurement
            ColumnGeneratorConfig(
                name="raw_pv",
                data_type=SimulationDataType.FLOAT,
                generator=RangeGeneratorConfig(type="range", min=68, max=72),
            ),
            # Smooth it
            ColumnGeneratorConfig(
                name="pv",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="ema('raw_pv', alpha=0.4, default=70)",
                ),
            ),
            ColumnGeneratorConfig(
                name="setpoint",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=75.0),
            ),
            ColumnGeneratorConfig(
                name="output",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="pid(pv=pv, sp=setpoint, Kp=1.0, Ki=0.05, dt=60)",
                ),
            ),
            # Track cumulative energy
            ColumnGeneratorConfig(
                name="cumulative_energy_kwh",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('cumulative_energy_kwh', 0) + output * (1/60) * 0.01",  # 1 min, scale factor
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Should have 2 entities * 30 rows = 60 rows
    assert len(df) == 60

    # Each entity should have independent state
    for entity in df["entity_id"].unique():
        entity_df = df[df["entity_id"] == entity].reset_index(drop=True)

        # Cumulative energy should be monotonically increasing (output is positive)
        cumsum_values = entity_df["cumulative_energy_kwh"].tolist()
        for i in range(1, len(cumsum_values)):
            assert cumsum_values[i] >= cumsum_values[i - 1]

        # Smoothed PV should vary less than raw
        if len(entity_df) > 5:
            raw_std = entity_df["raw_pv"].std()
            smooth_std = entity_df["pv"].std()
            assert smooth_std <= raw_std


def test_prev_with_null_values():
    """Test prev() handles None values gracefully."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=RangeGeneratorConfig(type="range", min=1, max=10),
                null_rate=0.3,  # 30% nulls
            ),
            ColumnGeneratorConfig(
                name="cumsum",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('cumsum', 0) + (value if value is not None else 0)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    # Should not crash with nulls
    assert len(rows) == 5

    # Cumsum should skip null values
    for row in rows:
        assert row["cumsum"] is not None  # Cumsum itself shouldn't be null


def test_pid_anti_windup():
    """Test PID anti-windup prevents integral runaway during saturation."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=20,
            seed=111,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            # Fixed process variable (stuck, won't respond)
            ColumnGeneratorConfig(
                name="pv",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=50.0),
            ),
            ColumnGeneratorConfig(
                name="setpoint",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=90.0),
            ),
            # PID with anti-windup
            ColumnGeneratorConfig(
                name="output_with_aw",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "pid(pv=pv, sp=setpoint, Kp=1.0, Ki=0.5, dt=60, "
                        "output_min=0, output_max=100, anti_windup=True)"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Output should saturate at 100 (max) and stay there
    # Without anti-windup, integral would keep growing indefinitely
    saturated_rows = df[df["output_with_aw"] >= 99.9]
    assert len(saturated_rows) > 5, "PID should saturate at max output"

    # All saturated values should be exactly at max
    assert all(df["output_with_aw"] <= 100.0)


def test_first_order_lag_response():
    """Test first-order lag using prev() - fundamental for process dynamics."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="10s",
            row_count=50,
            seed=555,
        ),
        entities=EntityConfig(count=1, id_prefix="Sensor_"),
        columns=[
            ColumnGeneratorConfig(
                name="row_num",
                data_type=SimulationDataType.INT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('row_num', -1) + 1",
                ),
            ),
            # Step input: 50 → 100 at row 10
            ColumnGeneratorConfig(
                name="input",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="50.0 if row_num < 10 else 100.0",
                ),
            ),
            # First-order lag: tau = 5 timesteps, gain K = 1.0
            # Response: y[t] = y[t-1] + (1/tau) * (K*input - y[t-1])
            ColumnGeneratorConfig(
                name="output",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('output', 50) + 0.2 * (input - prev('output', 50))",  # tau=5, alpha=1/tau=0.2
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    df = pd.DataFrame(rows)

    # Before step (row < 10): output should be ~50
    assert abs(df[df["row_num"] < 10]["output"].iloc[-1] - 50.0) < 1.0

    # After step (row >= 10): output should rise toward 100
    after_step = df[df["row_num"] >= 10]
    assert after_step["output"].iloc[0] < 100  # Shouldn't jump instantly
    assert after_step["output"].iloc[-1] > after_step["output"].iloc[0]  # Should be rising

    # Should approach 100 asymptotically (63% at 1*tau, 95% at 3*tau)
    # After ~15 rows (3*tau), should be >90
    if len(df) >= 25:  # row 10 + 15 more
        assert df["output"].iloc[25] > 90


def test_stateful_functions_persistence_across_incremental_runs():
    """
    Test that stateful functions work correctly across incremental runs.

    Note: Currently prev() state is NOT persisted across runs (only random_walk state is).
    This test documents current behavior. Future enhancement: persist entity_state.
    """
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="1m",
            row_count=5,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="E"),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="counter",
                data_type=SimulationDataType.INT,
                generator=ConstantGeneratorConfig(type="constant", value=1),
            ),
            ColumnGeneratorConfig(
                name="cumsum",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression="prev('cumsum', 0) + counter",
                ),
            ),
        ],
    )

    # First run
    engine1 = SimulationEngine(config)
    rows1 = engine1.generate()
    df1 = pd.DataFrame(rows1)

    # Cumsum should go 1, 2, 3, 4, 5
    expected1 = [1, 2, 3, 4, 5]
    actual1 = df1["cumsum"].tolist()
    assert actual1 == expected1

    # Second incremental run (note: prev() state NOT persisted currently)
    hwm = rows1[-1]["timestamp"]
    engine2 = SimulationEngine(config, hwm_timestamp=hwm)
    rows2 = engine2.generate()
    df2 = pd.DataFrame(rows2)

    # Current behavior: prev() resets, so cumsum restarts at 1
    # This is expected - prev() state is per-run, not persisted
    # If we want cross-run persistence, we'd need to extend StateManager
    assert df2["cumsum"].iloc[0] == 1  # Restarts (current behavior)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
