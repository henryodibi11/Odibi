import time
from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.transformers.scd import SCD2Params, scd2
from odibi.utils.logging_context import get_logging_context


class SCD2Pattern(Pattern):
    """
    SCD2 Pattern: Slowly Changing Dimension Type 2.

    Tracks history by creating new rows for updates.

    Configuration Options (via params dict):
        - **keys** (list): Business keys.
        - **time_col** (str): Timestamp column for versioning (default: current time).
        - **valid_from_col** (str): Name of start date column (default: valid_from).
        - **valid_to_col** (str): Name of end date column (default: valid_to).
        - **is_current_col** (str): Name of current flag column (default: is_current).
    """

    def validate(self) -> None:
        ctx = get_logging_context()
        ctx.debug(
            "SCD2Pattern validation starting",
            pattern="SCD2Pattern",
            keys=self.params.get("keys"),
            target=self.params.get("target"),
        )

        if not self.params.get("keys"):
            ctx.error(
                "SCD2Pattern validation failed: 'keys' parameter is required",
                pattern="SCD2Pattern",
            )
            raise ValueError("SCD2Pattern: 'keys' parameter is required.")
        if not self.params.get("target"):
            ctx.error(
                "SCD2Pattern validation failed: 'target' parameter is required",
                pattern="SCD2Pattern",
            )
            raise ValueError("SCD2Pattern: 'target' parameter is required (table name or path).")

        ctx.debug(
            "SCD2Pattern validation passed",
            pattern="SCD2Pattern",
            keys=self.params.get("keys"),
            target=self.params.get("target"),
        )

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        keys = self.params.get("keys")
        target = self.params.get("target")
        valid_from_col = self.params.get("valid_from_col", "valid_from")
        valid_to_col = self.params.get("valid_to_col", "valid_to")
        is_current_col = self.params.get("is_current_col", "is_current")
        track_columns = self.params.get("track_columns")

        ctx.debug(
            "SCD2 pattern starting",
            pattern="SCD2Pattern",
            keys=keys,
            target=target,
            valid_from_col=valid_from_col,
            valid_to_col=valid_to_col,
            is_current_col=is_current_col,
            track_columns=track_columns,
        )

        source_count = None
        try:
            if context.engine_type == "spark":
                source_count = context.df.count()
            else:
                source_count = len(context.df)
            ctx.debug("SCD2 source data loaded", pattern="SCD2Pattern", source_rows=source_count)
        except Exception:
            ctx.debug("SCD2 could not determine source row count", pattern="SCD2Pattern")

        valid_keys = SCD2Params.model_fields.keys()
        filtered_params = {k: v for k, v in self.params.items() if k in valid_keys}

        try:
            scd_params = SCD2Params(**filtered_params)
        except Exception as e:
            ctx.error(
                f"SCD2 invalid parameters: {e}",
                pattern="SCD2Pattern",
                error_type=type(e).__name__,
                params=filtered_params,
            )
            raise ValueError(f"Invalid SCD2 parameters: {e}")

        try:
            result_ctx = scd2(context, scd_params)
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"SCD2 pattern execution failed: {e}",
                pattern="SCD2Pattern",
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise

        result_df = result_ctx.df
        elapsed_ms = (time.time() - start_time) * 1000

        result_count = None
        try:
            if context.engine_type == "spark":
                result_count = result_df.count()
            else:
                result_count = len(result_df)
        except Exception:
            pass

        ctx.info(
            "SCD2 pattern completed",
            pattern="SCD2Pattern",
            elapsed_ms=round(elapsed_ms, 2),
            source_rows=source_count,
            result_rows=result_count,
            keys=keys,
            target=target,
            valid_from_col=valid_from_col,
            valid_to_col=valid_to_col,
        )

        return result_df
