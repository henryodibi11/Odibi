"""Validation transformers."""

from typing import Any, List

from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.exceptions import ValidationError
from odibi.registry import transform


class CrossCheckParams(BaseModel):
    """
    Configuration for cross-node validation checks.

    Example (Row Count Mismatch):
    ```yaml
    transformer: "cross_check"
    params:
      type: "row_count_diff"
      inputs: ["node_a", "node_b"]
      threshold: 0.05  # Allow 5% difference
    ```

    Example (Schema Match):
    ```yaml
    transformer: "cross_check"
    params:
      type: "schema_match"
      inputs: ["staging_orders", "prod_orders"]
    ```
    """

    type: str = Field(description="Check type: 'row_count_diff', 'schema_match'")
    inputs: List[str] = Field(description="List of node names to compare")
    threshold: float = Field(default=0.0, description="Threshold for diff (0.0-1.0)")


@transform("cross_check", param_model=CrossCheckParams)
def cross_check(context: EngineContext, params: CrossCheckParams) -> Any:
    """
    Perform cross-node validation checks.

    Does not return a DataFrame (returns None).
    Raises ValidationError on failure.
    """
    if len(params.inputs) < 2:
        raise ValueError("Cross-check requires at least 2 inputs")

    dfs = {}
    for name in params.inputs:
        df = context.context.get(name)
        if df is None:
            raise ValueError(f"Input '{name}' not found in context")
        dfs[name] = df

    if params.type == "row_count_diff":
        # Check if row counts differ by more than threshold
        counts = {name: context.engine.count_rows(df) for name, df in dfs.items()}
        base_name = params.inputs[0]
        base_count = counts[base_name]

        failures = []
        for name, count in counts.items():
            if name == base_name:
                continue

            # Calculate diff relative to base
            if base_count == 0:
                if count > 0:
                    diff = 1.0
                else:
                    diff = 0.0
            else:
                diff = abs(count - base_count) / base_count

            if diff > params.threshold:
                failures.append(
                    f"Row count mismatch: {name} ({count}) vs {base_name} ({base_count}). "
                    f"Diff {diff:.1%} > {params.threshold:.1%}"
                )

        if failures:
            raise ValidationError("cross_check", failures)

    elif params.type == "schema_match":
        # Check if schemas are identical
        base_name = params.inputs[0]
        base_schema = context.engine.get_schema(dfs[base_name])

        failures = []
        for name, df in dfs.items():
            if name == base_name:
                continue

            schema = context.engine.get_schema(df)
            # Compare dicts
            if base_schema != schema:
                # Find diffs
                set_base = set(base_schema.items())
                set_curr = set(schema.items())

                missing = set_base - set_curr
                extra = set_curr - set_base

                msg = f"Schema mismatch: {name} vs {base_name}."
                if missing:
                    msg += f" Missing/Changed: {missing}"
                if extra:
                    msg += f" Extra/Changed: {extra}"
                failures.append(msg)

        if failures:
            raise ValidationError("cross_check", failures)

    return None
