from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern


class SnapshotPattern(Pattern):
    """
    Snapshot Pattern: Full overwrite of state.

    Typically used for small dimensions or full refreshes.

    Params:
        None required (default behavior)
    """

    def execute(self, context: EngineContext) -> Any:
        # Snapshot is essentially a pass-through that relies on WriteMode.OVERWRITE
        # The Node's write logic handles the mode.
        # We just ensure the data is passed through.
        return context.df
