"""Dedicated stdin-only command for immutable logical pipeline planning."""

from __future__ import annotations

import argparse
import sys

from odibi.planning import (
    DEFAULT_PLANNING_LIMITS,
    PlanningResponse,
    plan_pipeline_bytes,
)


INTERNAL_SERIALIZATION_FAILURE_JSON = (
    '{"diagnostics":[{"category":"internal","code":"INTERNAL_SERIALIZATION_FAILURE",'
    '"message":"Immutable planning output could not be serialized.","severity":"error",'
    '"subject":{"kind":"document","name":null}}],"plan":null,'
    '"schema_version":"1.0","status":"invalid","truncated":false}'
)


def _planning_exit_code(response: PlanningResponse) -> int:
    """Map planner status and category to the stable CLI exit contract."""
    if any(item.category == "internal" for item in response.diagnostics):
        return 4
    return {"planned": 0, "invalid": 2, "unresolved": 3}[response.status]


def plan_command(args: argparse.Namespace) -> int:
    """Read bounded UTF-8 YAML from stdin and write one canonical response."""
    raw = sys.stdin.buffer.read(DEFAULT_PLANNING_LIMITS.max_input_bytes + 1)
    response = plan_pipeline_bytes(raw)
    exit_code = _planning_exit_code(response)
    try:
        output = response.to_json()
    except Exception:
        output = INTERNAL_SERIALIZATION_FAILURE_JSON
        exit_code = 4
    try:
        sys.stdout.write(output + "\n")
    except Exception:
        return 4
    return exit_code
