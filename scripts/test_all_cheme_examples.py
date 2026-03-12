#!/usr/bin/env python
"""Test all ChemE course YAML examples to ensure they work."""

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

# Python executable
PYTHON_EXE = r"D:\odibi\.venv-windows\Scripts\python.exe"

# All ChemE course YAML files
YAML_FILES = [
    # L00 - Setup
    "examples/cheme_course/L00_setup/tank_data.yaml",
    "examples/cheme_course/L00_setup/tank_data_parquet.yaml",
    "examples/cheme_course/L00_setup/tank_realistic.yaml",
    "examples/cheme_course/L00_setup/multi_entity.yaml",
    # L01 - CV/MV/DV
    "examples/cheme_course/L01_cv_mv_dv/mixing_tank_ph.yaml",
    "examples/cheme_course/L01_cv_mv_dv/heat_exchanger.yaml",
    # L02 - DoF & Balances
    "examples/cheme_course/L02_dof_balances/tank_mass_balance.yaml",
    "examples/cheme_course/L02_dof_balances/cstr_energy_balance.yaml",
    # L03 - First Order
    "examples/cheme_course/L03_first_order/tank_temperature_fo.yaml",
    "examples/cheme_course/L03_first_order/ema_filtering.yaml",
    # L04 - FOPTD
    "examples/cheme_course/L04_foptd/tank_foptd.yaml",
    "examples/cheme_course/L04_foptd/heat_exchanger_foptd.yaml",
    "examples/cheme_course/L04_foptd/foptd_parameterized.yaml",
    # L05 - Second Order
    "examples/cheme_course/L05_second_order/pressure_underdamped.yaml",
    "examples/cheme_course/L05_second_order/valve_actuator.yaml",
    "examples/cheme_course/L05_second_order/damping_comparison.yaml",
    # L06 - PID Basics
    "examples/cheme_course/L06_pid_basics/tank_pi.yaml",
    # L07 - Tuning
    "examples/cheme_course/L07_tuning/ziegler_nichols.yaml",
    "examples/cheme_course/L07_tuning/cohen_coon.yaml",
    # L08 - Disturbances
    "examples/cheme_course/L08_disturbances/load_disturbance.yaml",
    "examples/cheme_course/L08_disturbances/feedwater_disturbance.yaml",
    # L09 - System ID
    "examples/cheme_course/L09_system_id/step_response.yaml",
    "examples/cheme_course/L09_system_id/pulse_test.yaml",
    # L10 - Interacting Loops
    "examples/cheme_course/L10_interacting_loops/dual_temperature.yaml",
    "examples/cheme_course/L10_interacting_loops/pressure_flow.yaml",
    # L11 - Cascade
    "examples/cheme_course/L11_cascade/temperature_cascade.yaml",
    "examples/cheme_course/L11_cascade/level_flow_cascade.yaml",
    # L12 - Feedforward
    "examples/cheme_course/L12_feedforward/simple_feedforward.yaml",
    "examples/cheme_course/L12_feedforward/ratio_control.yaml",
    # L13 - Nonlinearity
    "examples/cheme_course/L13_nonlinearity/valve_nonlinearity.yaml",
    "examples/cheme_course/L13_nonlinearity/ph_neutralization.yaml",
    # L14 - MPC Lite
    "examples/cheme_course/L14_mpc_lite/mpc_basics.yaml",
    "examples/cheme_course/L14_mpc_lite/constrained_control.yaml",
    # L15 - CSTR Digital Twin
    "examples/cheme_course/L15_cstr_digital_twin/cstr_full_model.yaml",
    "examples/cheme_course/L15_cstr_digital_twin/optimization.yaml",
]


def test_yaml_file(yaml_path: str) -> Tuple[bool, str]:
    """Test a single YAML file.

    Returns:
        (success, error_message)
    """
    cmd = [PYTHON_EXE, "-m", "odibi.cli", "run", yaml_path]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd="d:/odibi")

        if result.returncode == 0:
            return True, ""
        else:
            # Extract just the error line
            error_lines = result.stderr.split("\n")
            for line in reversed(error_lines):
                if line.strip() and "error" in line.lower():
                    return False, line.strip()
            return False, result.stderr[-200:] if result.stderr else "Unknown error"

    except subprocess.TimeoutExpired:
        return False, "Timeout (>30s)"
    except Exception as e:
        return False, str(e)


def main():
    """Test all YAML files and report results."""
    print(f"Testing {len(YAML_FILES)} ChemE course YAML examples...\n")
    print("=" * 80)

    results: List[Tuple[str, bool, str]] = []

    for yaml_file in YAML_FILES:
        # Extract lesson and filename
        parts = Path(yaml_file).parts
        lesson = parts[-2] if len(parts) >= 2 else "Unknown"
        filename = parts[-1]

        print(f"\nTesting: {lesson}/{filename}...")
        success, error = test_yaml_file(yaml_file)

        results.append((yaml_file, success, error))

        if success:
            print("  [OK] SUCCESS")
        else:
            print(f"  [FAIL] FAILED: {error[:100]}")

    # Summary
    print("\n" + "=" * 80)
    print("\nSUMMARY\n" + "-" * 80)

    passed = sum(1 for _, success, _ in results if success)
    failed = len(results) - passed

    print(f"\nTotal: {len(results)}")
    print(f"[OK] Passed: {passed}")
    print(f"[FAIL] Failed: {failed}")

    if failed > 0:
        print("\n\nFailed files:")
        for yaml_file, success, error in results:
            if not success:
                print(f"  - {yaml_file}")
                print(f"    Error: {error[:150]}")
        sys.exit(1)
    else:
        print("\n*** All tests passed! ***")
        sys.exit(0)


if __name__ == "__main__":
    main()
