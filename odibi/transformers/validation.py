"""Validation transformers."""

import time
from typing import Any, List

from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.exceptions import ValidationError
from odibi.registry import transform
from odibi.utils.logging_context import get_logging_context


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
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "CrossCheck starting",
        check_type=params.type,
        inputs=params.inputs,
        threshold=params.threshold,
    )

    if len(params.inputs) < 2:
        ctx.error(
            "CrossCheck failed: insufficient inputs",
            inputs_count=len(params.inputs),
        )
        raise ValueError("Cross-check requires at least 2 inputs")

    dfs = {}
    for name in params.inputs:
        df = context.context.get(name)
        if df is None:
            ctx.error(
                "CrossCheck failed: input not found",
                missing_input=name,
                available_inputs=(
                    list(context.context._data.keys())
                    if hasattr(context.context, "_data")
                    else None
                ),
            )
            raise ValueError(f"Input '{name}' not found in context")
        dfs[name] = df

    if params.type == "row_count_diff":
        counts = {name: context.engine.count_rows(df) for name, df in dfs.items()}
        base_name = params.inputs[0]
        base_count = counts[base_name]

        ctx.debug(
            "CrossCheck row counts",
            counts=counts,
        )

        failures = []
        for name, count in counts.items():
            if name == base_name:
                continue

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
            ctx.warning(
                "CrossCheck validation failed",
                failures=failures,
            )
            raise ValidationError("cross_check", failures)

    elif params.type == "schema_match":
        base_name = params.inputs[0]
        base_schema = context.engine.get_schema(dfs[base_name])

        failures = []
        for name, df in dfs.items():
            if name == base_name:
                continue

            schema = context.engine.get_schema(df)
            if base_schema != schema:
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
            ctx.warning(
                "CrossCheck validation failed",
                failures=failures,
            )
            raise ValidationError("cross_check", failures)

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "CrossCheck completed",
        check_type=params.type,
        passed=True,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return None
