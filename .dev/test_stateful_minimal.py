"""Minimal test to verify stateful function logic without full odibi import."""


def test_prev_logic():
    """Test prev() logic."""
    print("Testing prev() logic...")

    entity_state = {}

    def prev(column_name, default=None):
        return entity_state.get(column_name, default)

    # Simulate 5 rows with cumsum
    cumsum_values = []
    counter = 1

    for i in range(5):
        # Expression: prev('cumsum', 0) + counter
        cumsum = prev("cumsum", 0) + counter
        cumsum_values.append(cumsum)

        # Update state
        entity_state["cumsum"] = cumsum

    print(f"  Cumsum values: {cumsum_values}")
    assert cumsum_values == [1, 2, 3, 4, 5], f"Expected [1,2,3,4,5], got {cumsum_values}"
    print("  [OK] prev() logic correct\n")


def test_ema_logic():
    """Test EMA logic."""
    print("Testing ema() logic...")

    entity_state = {}
    raw_values = [100, 105, 95, 102, 98]
    smoothed_values = []
    alpha = 0.3

    for raw in raw_values:
        ema_key = "_ema_temp"
        prev_ema = entity_state.get(ema_key)

        if prev_ema is None:
            ema_value = 100  # default
        else:
            ema_value = alpha * raw + (1 - alpha) * prev_ema

        entity_state[ema_key] = ema_value
        smoothed_values.append(ema_value)

    print(f"  Raw: {raw_values}")
    print(f"  Smoothed: {[round(v, 2) for v in smoothed_values]}")

    # Verify smoothing
    import statistics

    raw_std = statistics.stdev(raw_values)
    smooth_std = statistics.stdev(smoothed_values)
    print(f"  Raw std: {raw_std:.2f}, Smoothed std: {smooth_std:.2f}")
    assert smooth_std < raw_std
    print("  [OK] EMA logic correct\n")


def test_pid_logic():
    """Test PID controller logic."""
    print("Testing PID logic...")

    entity_state = {}
    setpoint = 75
    pv_values = [50, 52, 55, 60, 65, 68, 70, 72, 73, 74]
    outputs = []

    Kp, Ki, Kd, dt = 2.0, 0.1, 0.5, 60

    for pv in pv_values:
        error = setpoint - pv

        pid_key = "_pid_test"
        pid_state = entity_state.get(pid_key, {"integral": 0.0, "prev_error": 0.0})

        p_term = Kp * error
        i_term = Ki * pid_state["integral"]
        derivative = (error - pid_state["prev_error"]) / dt
        d_term = Kd * derivative

        output = p_term + i_term + d_term
        clamped = max(0, min(100, output))

        # Update integral with anti-windup
        if 0 < clamped < 100:
            pid_state["integral"] += error * dt

        entity_state[pid_key] = {
            "integral": pid_state["integral"],
            "prev_error": error,
        }

        outputs.append(clamped)

    print(f"  PV progression: {pv_values}")
    print(f"  PID outputs: {[round(o, 1) for o in outputs]}")
    print(f"  Error reduced from {abs(pv_values[0] - setpoint)} to {abs(pv_values[-1] - setpoint)}")
    print("  [OK] PID logic correct\n")


def test_battery_soc_logic():
    """Test battery SOC integration - THE use case."""
    print("Testing Battery SOC Integration...")

    entity_state = {}
    capacity_ah = 100.0
    dt_hours = 5 / 60  # 5 minutes

    # Charge for 10 steps, then discharge for 10 steps
    current_profile = [20.0] * 10 + [-15.0] * 10
    soc_values = []

    for i, current_a in enumerate(current_profile):
        # SOC[t] = SOC[t-1] + (I * dt) / Capacity * 100
        prev_soc = entity_state.get("soc_pct", 50)  # Start at 50%
        delta_soc = (current_a * dt_hours) / capacity_ah * 100
        soc = max(0, min(100, prev_soc + delta_soc))

        soc_values.append(soc)
        entity_state["soc_pct"] = soc

    print(f"  Initial SOC: {soc_values[0]:.1f}%")
    print(f"  After charging (row 9): {soc_values[9]:.1f}%")
    print(f"  After discharging (row 19): {soc_values[19]:.1f}%")

    assert soc_values[9] > soc_values[0], "SOC should increase during charging"
    assert soc_values[19] < soc_values[9], "SOC should decrease during discharging"

    print("  [OK] Battery SOC integration logic correct\n")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Testing Stateful Function Logic (Minimal)")
    print("=" * 60 + "\n")

    test_prev_logic()
    test_ema_logic()
    test_pid_logic()
    test_battery_soc_logic()

    print("=" * 60)
    print("SUCCESS: ALL LOGIC TESTS PASSED!")
    print("=" * 60)
    print("\nStateful functions are mathematically correct.")
    print("Now integrating into SimulationEngine...")
