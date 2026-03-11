"""Quick standalone test for stateful simulation features."""

import sys
sys.path.insert(0, "D:/odibi")

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


def test_prev():
    """Test prev() function."""
    print("\n=== Testing prev() function ===")
    
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

    engine = SimulationEngine(config)
    rows = engine.generate()

    print(f"Generated {len(rows)} rows")
    for i, row in enumerate(rows):
        print(f"  Row {i}: counter={row['counter']}, cumsum={row['cumsum']}")
    
    # Verify cumulative sum: 1, 2, 3, 4, 5
    expected = [1, 2, 3, 4, 5]
    actual = [r["cumsum"] for r in rows]
    assert actual == expected, f"Expected {expected}, got {actual}"
    print("✓ prev() works correctly!\n")


def test_battery_soc():
    """Test battery SOC integration - THE killer use case."""
    print("\n=== Testing Battery SOC Integration ===")
    
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="5m",
            row_count=20,
            seed=42,
        ),
        entities=EntityConfig(count=1, id_prefix="BESS_"),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="battery_capacity_ah",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=100.0),
            ),
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
                    expression="20.0 if row_num < 10 else -15.0",
                ),
            ),
            ColumnGeneratorConfig(
                name="soc_pct",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "max(0, min(100, "
                        "prev('soc_pct', 50) + "
                        "(charge_current_a * (5/60)) / battery_capacity_ah * 100"
                        "))"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    print(f"Generated {len(rows)} rows for battery SOC simulation")
    print("\nFirst 5 rows:")
    for i in range(min(5, len(rows))):
        r = rows[i]
        print(f"  Row {i}: time={r['timestamp']}, current={r['charge_current_a']:6.1f}A, SOC={r['soc_pct']:5.1f}%")
    
    print("\nLast 5 rows:")
    for i in range(max(0, len(rows) - 5), len(rows)):
        r = rows[i]
        print(f"  Row {i}: time={r['timestamp']}, current={r['charge_current_a']:6.1f}A, SOC={r['soc_pct']:5.1f}%")
    
    # Verify SOC increases during charging
    charging_start_soc = rows[0]["soc_pct"]
    charging_end_soc = rows[9]["soc_pct"]
    assert charging_end_soc > charging_start_soc, "SOC should increase during charging"
    
    # Verify SOC decreases during discharging
    discharging_start_soc = rows[10]["soc_pct"]
    discharging_end_soc = rows[-1]["soc_pct"]
    assert discharging_end_soc < discharging_start_soc, "SOC should decrease during discharging"
    
    print("\n✓ Battery SOC integration works!")
    print(f"  Charging: {charging_start_soc:.1f}% → {charging_end_soc:.1f}% (Δ={charging_end_soc - charging_start_soc:.1f}%)")
    print(f"  Discharging: {discharging_start_soc:.1f}% → {discharging_end_soc:.1f}% (Δ={discharging_end_soc - discharging_start_soc:.1f}%)\n")


def test_pid_control():
    """Test PID controller."""
    print("\n=== Testing PID Controller ===")
    
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="30s",
            row_count=25,
            seed=789,
        ),
        entities=EntityConfig(count=1, id_prefix="Reactor_"),
        columns=[
            ColumnGeneratorConfig(
                name="temp_setpoint_c",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=85.0),
            ),
            ColumnGeneratorConfig(
                name="temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=DerivedGeneratorConfig(
                    type="derived",
                    expression=(
                        "prev('temp_c', 70) + "
                        "0.05 * (prev('heater_pct', 50) - prev('temp_c', 70))"
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
                        "Kp=3.0, Ki=0.2, Kd=1.0, dt=30)"
                    ),
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    print(f"Generated {len(rows)} rows for PID temperature control")
    print("\nSample rows:")
    for i in [0, 5, 10, 15, 20, 24]:
        if i < len(rows):
            r = rows[i]
            print(f"  Row {i:2d}: temp={r['temp_c']:5.2f}°C, heater={r['heater_pct']:5.1f}%, SP=85°C")
    
    initial_temp = rows[0]["temp_c"]
    final_temp = rows[-1]["temp_c"]
    setpoint = 85.0
    
    initial_error = abs(initial_temp - setpoint)
    final_error = abs(final_temp - setpoint)
    
    print("\n✓ PID controller works!")
    print(f"  Initial error: {initial_error:.2f}°C")
    print(f"  Final error: {final_error:.2f}°C")
    print(f"  Error reduction: {(1 - final_error/initial_error)*100:.1f}%\n")
    
    assert final_error < initial_error * 0.6, "PID should reduce error"


def test_ema():
    """Test EMA smoothing."""
    print("\n=== Testing EMA (Exponential Moving Average) ===")
    
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-01-01T00:00:00Z",
            timestep="10s",
            row_count=15,
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
                    expression="ema('raw_temp_c', alpha=0.3, default=100)",
                ),
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()

    import statistics
    raw_values = [r["raw_temp_c"] for r in rows]
    smoothed_values = [r["smoothed_temp_c"] for r in rows]
    
    raw_stdev = statistics.stdev(raw_values)
    smooth_stdev = statistics.stdev(smoothed_values)
    
    print(f"Generated {len(rows)} rows")
    print(f"Raw temperature std dev: {raw_stdev:.3f}°C")
    print(f"Smoothed temperature std dev: {smooth_stdev:.3f}°C")
    print(f"Noise reduction: {(1 - smooth_stdev/raw_stdev)*100:.1f}%")
    
    assert smooth_stdev < raw_stdev, "EMA should reduce variance"
    print("✓ EMA smoothing works!\n")


if __name__ == "__main__":
    try:
        test_prev()
        test_battery_soc()
        test_pid_control()
        test_ema()
        
        print("\n" + "="*60)
        print("🎉 ALL STATEFUL SIMULATION TESTS PASSED!")
        print("="*60)
        print("\nStateful functions ready for renewable energy applications:")
        print("  ✓ prev() - Access previous row values")
        print("  ✓ ema() - Exponential moving average")  
        print("  ✓ pid() - PID controller with anti-windup")
        print("\nYou can now simulate:")
        print("  • Battery SOC integration")
        print("  • PID temperature/pressure control")
        print("  • Sensor smoothing and filtering")
        print("  • First-order process dynamics")
        print("  • Any time-dependent process behavior")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
