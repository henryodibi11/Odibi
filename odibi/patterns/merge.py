import time
from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.transformers.merge_transformer import MergeParams, merge
from odibi.utils.logging_context import get_logging_context


class MergePattern(Pattern):
    """
    Merge Pattern: Upsert/Merge logic.

    Configuration Options (via params dict):
        - **target** (str): Target table/path.
        - **keys** (list): Join keys.
        - **strategy** (str): 'upsert', 'append_only', 'delete_match'.
    """

    def validate(self) -> None:
        ctx = get_logging_context()

        # Support both 'target' and 'path' for compatibility with merge transformer
        target = self.params.get("target") or self.params.get("path")

        ctx.debug(
            "MergePattern validation starting",
            pattern="MergePattern",
            target=target,
            keys=self.params.get("keys"),
            strategy=self.params.get("strategy"),
        )

        if not target:
            ctx.error(
                "MergePattern validation failed: 'target' or 'path' is required",
                pattern="MergePattern",
            )
            raise ValueError("MergePattern: 'target' or 'path' is required.")
        if not self.params.get("keys"):
            ctx.error(
                "MergePattern validation failed: 'keys' is required",
                pattern="MergePattern",
            )
            raise ValueError("MergePattern: 'keys' is required.")

        ctx.debug(
            "MergePattern validation passed",
            pattern="MergePattern",
            target=self.params.get("target"),
            keys=self.params.get("keys"),
            strategy=self.params.get("strategy", "upsert"),
        )

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        # Support both 'target' and 'path' for compatibility
        target = self.params.get("target") or self.params.get("path")
        keys = self.params.get("keys")
        strategy = self.params.get("strategy", "upsert")

        ctx.debug(
            "Merge pattern starting",
            pattern="MergePattern",
            target=target,
            keys=keys,
            strategy=strategy,
        )

        source_count = None
        try:
            if context.engine_type == "spark":
                source_count = context.df.count()
            else:
                source_count = len(context.df)
            ctx.debug(
                "Merge source data loaded",
                pattern="MergePattern",
                source_rows=source_count,
            )
        except Exception:
            ctx.debug("Merge could not determine source row count", pattern="MergePattern")

        valid_keys = MergeParams.model_fields.keys()
        filtered_params = {k: v for k, v in self.params.items() if k in valid_keys}

        try:
            merge(context, context.df, **filtered_params)
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"Merge pattern execution failed: {e}",
                pattern="MergePattern",
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
                target=target,
                keys=keys,
                strategy=strategy,
            )
            raise

        elapsed_ms = (time.time() - start_time) * 1000

        ctx.info(
            "Merge pattern completed",
            pattern="MergePattern",
            elapsed_ms=round(elapsed_ms, 2),
            source_rows=source_count,
            target=target,
            keys=keys,
            strategy=strategy,
        )

        return context.df
