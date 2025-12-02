import time
from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class SnapshotPattern(Pattern):
    """
    Snapshot Pattern: Full overwrite of state.

    Typically used for small dimensions or full refreshes.

    Params:
        snapshot_column (str): Optional column to identify snapshot date/time.
        target (str): Target table/path for the snapshot.
    """

    def validate(self) -> None:
        ctx = get_logging_context()
        snapshot_column = self.params.get("snapshot_column")
        target = self.params.get("target")

        ctx.debug(
            "SnapshotPattern validation starting",
            pattern="SnapshotPattern",
            snapshot_column=snapshot_column,
            target=target,
        )

        ctx.debug(
            "SnapshotPattern validation passed",
            pattern="SnapshotPattern",
            snapshot_column=snapshot_column,
            target=target,
        )

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        snapshot_column = self.params.get("snapshot_column")
        target = self.params.get("target")

        ctx.debug(
            "Snapshot pattern starting",
            pattern="SnapshotPattern",
            snapshot_column=snapshot_column,
            target=target,
        )

        row_count = None
        try:
            if context.engine_type == "spark":
                row_count = context.df.count()
            else:
                row_count = len(context.df)
            ctx.debug(
                "Snapshot data loaded",
                pattern="SnapshotPattern",
                row_count=row_count,
            )
        except Exception:
            ctx.debug("Snapshot could not determine row count", pattern="SnapshotPattern")

        elapsed_ms = (time.time() - start_time) * 1000

        ctx.info(
            "Snapshot pattern completed",
            pattern="SnapshotPattern",
            elapsed_ms=round(elapsed_ms, 2),
            row_count=row_count,
            snapshot_column=snapshot_column,
            target=target,
            mode="overwrite",
        )

        return context.df
