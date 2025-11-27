from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.transformers.scd import SCD2Params, scd2


class SCD2Pattern(Pattern):
    """
    SCD2 Pattern: Slowly Changing Dimension Type 2.

    Tracks history by creating new rows for updates.

    Params:
        keys (list): Business keys.
        time_col (str): Timestamp column for versioning (default: current time).
        valid_from_col (str): Name of start date column (default: valid_from).
        valid_to_col (str): Name of end date column (default: valid_to).
        is_current_col (str): Name of current flag column (default: is_current).
    """

    def validate(self) -> None:
        if not self.params.get("keys"):
            raise ValueError("SCD2Pattern: 'keys' parameter is required.")
        if not self.params.get("target"):
            raise ValueError("SCD2Pattern: 'target' parameter is required (table name or path).")

    def execute(self, context: EngineContext) -> Any:
        # Map dictionary params to SCD2Params model
        valid_keys = SCD2Params.model_fields.keys()
        filtered_params = {k: v for k, v in self.params.items() if k in valid_keys}

        try:
            scd_params = SCD2Params(**filtered_params)
        except Exception as e:
            raise ValueError(f"Invalid SCD2 parameters: {e}")

        # Execute transformer logic
        # scd2 returns an EngineContext, we need the df
        result_ctx = scd2(context, scd_params)

        # The result of scd2 is the FULL history to be overwritten
        return result_ctx.df
