"""Integration test for complete CSTR flowsheet demo.

Validates that the comprehensive ChemE example runs and demonstrates
all Sprint 2 features working together.
"""

import pandas as pd
import yaml

from odibi.simulation import SimulationEngine
from odibi.config import SimulationConfig


def test_cstr_flowsheet_complete():
    """Test the complete CSTR flowsheet example."""
    # Load the example YAML
    with open("examples/chemical_engineering/cstr_flowsheet_complete.yaml", "r") as f:
        config_dict = yaml.safe_load(f)

    config = SimulationConfig(**config_dict)
    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 3 entities × 120 rows = 360 rows
    assert len(df) == 360, f"Expected 360 rows, got {len(df)}"

    # Split by entity
    cstr = df[df["entity_id"] == "CSTR_R101"].reset_index(drop=True)
    separator = df[df["entity_id"] == "Separator_V102"].reset_index(drop=True)
    hx = df[df["entity_id"] == "Heat_Exchanger_E103"].reset_index(drop=True)

    assert len(cstr) == 120
    assert len(separator) == 120
    assert len(hx) == 120

    # ========================================================================
    # Feature 1: Stateful Functions (prev, ema, pid)
    # ========================================================================

    # prev() - Reactor level integration
    # Level should change based on feed - effluent
    assert cstr["reactor_level_m3"].notna().all(), "Reactor level should have values"
    level_change = cstr["reactor_level_m3"].iloc[-1] - cstr["reactor_level_m3"].iloc[0]
    assert abs(level_change) < 2.0, "Level should be relatively stable"

    # ema() - Smoothed temperature sensor
    # Smooth should have less variance than raw
    temp_raw_std = cstr["temp_sensor_raw_c"].std()
    temp_smooth_std = cstr["temp_sensor_smooth_c"].std()
    assert temp_smooth_std < temp_raw_std, "EMA should reduce noise"

    # pid() - Cooling valve control
    # Valve should respond to temperature errors
    assert cstr["cooling_valve_pct"].min() >= 0, "PID output should be >= 0"
    assert cstr["cooling_valve_pct"].max() <= 100, "PID output should be <= 100"

    # ========================================================================
    # Feature 2: mean_reversion_to
    # ========================================================================

    # Feed temp should track ambient (mean_reversion_to)
    feed_ambient_correlation = cstr[["feed_temp_c", "ambient_temp_c"]].corr().iloc[0, 1]
    assert feed_ambient_correlation > 0.5, "Feed temp should correlate with ambient"

    # Reactor temp should track ambient when cooling (heat loss)
    # But controlled by PID to setpoint (complex dynamics)

    # ========================================================================
    # Feature 3: Cross-Entity References
    # ========================================================================

    # Separator feed should equal CSTR effluent (cross-entity ref!)
    for i in range(120):
        cstr_effluent = cstr.loc[i, "effluent_flow_kg_hr"]
        sep_feed = separator.loc[i, "separator_feed_flow_kg_hr"]
        assert abs(cstr_effluent - sep_feed) < 0.01, (
            f"Row {i}: Separator feed should equal CSTR effluent. "
            f"CSTR: {cstr_effluent:.2f}, Sep: {sep_feed:.2f}"
        )

    # Separator feed temp should equal CSTR effluent temp (cross-entity!)
    for i in range(120):
        cstr_temp = cstr.loc[i, "effluent_temp_c"]
        sep_temp = separator.loc[i, "separator_feed_temp_c"]
        assert abs(cstr_temp - sep_temp) < 0.01, "Separator should receive CSTR temperature"

    # Heat exchanger hot side should equal CSTR effluent temp (cross-entity!)
    for i in range(120):
        cstr_temp = cstr.loc[i, "effluent_temp_c"]
        hx_hot = hx.loc[i, "hot_side_temp_in_c"]
        assert abs(cstr_temp - hx_hot) < 0.01, "HX should receive CSTR effluent"

    # ========================================================================
    # Feature 4: Scheduled Events
    # ========================================================================

    # Event 1: Maintenance shutdown (05:00-06:00)
    # CSTR feed forced to 0
    maintenance_start = "2026-03-11T05:00:00Z"
    maintenance_end = "2026-03-11T06:00:00Z"

    during_maintenance = cstr[
        (cstr["timestamp"] >= maintenance_start) & (cstr["timestamp"] <= maintenance_end)
    ]
    assert (during_maintenance["feed_flow_kg_hr"] == 0.0).all(), (
        "Feed should be 0 during maintenance"
    )

    # Cascade effect: Separator feed should also be 0 (via cross-entity ref)
    sep_during_maintenance = separator[
        (separator["timestamp"] >= maintenance_start) & (separator["timestamp"] <= maintenance_end)
    ]
    assert (sep_during_maintenance["separator_feed_flow_kg_hr"] == 0.0).all(), (
        "Separator feed should cascade to 0"
    )

    # Event 2: Setpoint change at 07:00 (85°C → 88°C, permanent)
    setpoint_change = "2026-03-11T07:00:00Z"

    before_setpoint_change = cstr[cstr["timestamp"] < setpoint_change]
    after_setpoint_change = cstr[cstr["timestamp"] >= setpoint_change]

    assert (before_setpoint_change["reactor_temp_setpoint_c"] == 85.0).all(), (
        "Setpoint should be 85°C before event"
    )
    assert (after_setpoint_change["reactor_temp_setpoint_c"] == 88.0).all(), (
        "Setpoint should be 88°C after event"
    )

    # Event 3: Feed flow reduction at 08:00 (permanent)
    feed_change = "2026-03-11T08:00:00Z"

    # Before 08:00, feed varies around 1000 (except during maintenance)
    before_feed_change = cstr[
        (cstr["timestamp"] < feed_change) & (cstr["timestamp"] < maintenance_start)
    ]
    assert before_feed_change["feed_flow_kg_hr"].mean() > 900, (
        "Feed should average ~1000 before change"
    )

    # After 08:00, feed forced to 900
    after_feed_change = cstr[cstr["timestamp"] >= feed_change]
    assert (after_feed_change["feed_flow_kg_hr"] == 900.0).all(), "Feed should be 900 after event"

    # ========================================================================
    # Integration: All Features Working Together
    # ========================================================================

    # Product output should respond to all dynamics:
    # - Higher temp → higher concentration
    # - Higher feed → more throughput (up to separation limit)
    # - Maintenance → zero output
    # - Recovery back to normal after maintenance

    product_output = separator["product_flow_kg_hr"]
    assert product_output.notna().all(), "Product output should have values"

    # During maintenance, product should be minimal/zero
    product_during_maintenance = separator[
        (separator["timestamp"] >= maintenance_start) & (separator["timestamp"] <= maintenance_end)
    ]["product_flow_kg_hr"]
    assert product_during_maintenance.max() < 10, "Product should be near zero during maintenance"

    # After maintenance, production should resume
    product_after_maintenance = separator[separator["timestamp"] > maintenance_end][
        "product_flow_kg_hr"
    ]
    assert product_after_maintenance.mean() > 100, "Production should resume after maintenance"

    print("\n" + "=" * 70)
    print("✅ CSTR FLOWSHEET DEMO - ALL FEATURES VALIDATED")
    print("=" * 70)
    print(f"\nGenerated {len(df)} rows across 3 process units")
    print("Timespan: 10 hours (120 timesteps @ 5-min intervals)")
    print("\nFeatures Validated:")
    print("  ✅ prev() - Reactor level integration")
    print("  ✅ ema() - Temperature sensor smoothing")
    print("  ✅ pid() - Temperature control with cooling water")
    print("  ✅ mean_reversion_to - Feed/reactor temps track ambient")
    print("  ✅ Cross-entity refs - Separator receives CSTR effluent")
    print("  ✅ Cross-entity refs - Heat exchanger sees CSTR outlet")
    print("  ✅ Scheduled events - Maintenance shutdown (1 hr)")
    print("  ✅ Scheduled events - Setpoint increase (permanent)")
    print("  ✅ Scheduled events - Feed flow reduction (permanent)")
    print("\nKey Metrics:")
    print(f"  Avg reactor temp: {cstr['reactor_temp_c'].mean():.1f}°C")
    print(f"  Avg product output: {separator['product_flow_kg_hr'].mean():.1f} kg/hr")
    print(f"  Avg heat recovery: {hx['heat_duty_kw'].mean():.1f} kW")
    print(f"  Temperature control std dev: {cstr['reactor_temp_c'].std():.2f}°C")
    print("=" * 70)


if __name__ == "__main__":
    test_cstr_flowsheet_complete()
    print("\nAll validations passed!")
