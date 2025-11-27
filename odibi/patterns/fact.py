from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern


class FactPattern(Pattern):
    """
    Fact Pattern: Immutable event log (Insert-only).

    Params:
        deduplicate (bool): If true, removes duplicates before insert.
        keys (list): Keys for deduplication.
    """

    def validate(self) -> None:
        if self.params.get("deduplicate") and not self.params.get("keys"):
            raise ValueError("FactPattern: 'keys' required when 'deduplicate' is True.")

    def execute(self, context: EngineContext) -> Any:
        df = context.df

        if self.params.get("deduplicate"):
            keys = self.params.get("keys")
            # Use engine's deduplicate capability
            # We can use the 'deduplicate' transformer logic here
            # But better to delegate to engine operation if possible
            # Or use the registered function?

            # For now, simple Spark/Pandas dropDuplicates
            if context.engine_type == "spark":
                df = df.dropDuplicates(keys)
            else:
                df = df.drop_duplicates(subset=keys)

        return df
