import time
from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class FactPattern(Pattern):
    """
    Fact Pattern: Immutable event log (Insert-only).

    Params:
        deduplicate (bool): If true, removes duplicates before insert.
        keys (list): Keys for deduplication.
    """

    def validate(self) -> None:
        ctx = get_logging_context()
        deduplicate = self.params.get("deduplicate")
        keys = self.params.get("keys")

        ctx.debug(
            "FactPattern validation starting",
            pattern="FactPattern",
            deduplicate=deduplicate,
            keys=keys,
        )

        if deduplicate and not keys:
            ctx.error(
                "FactPattern validation failed: 'keys' required when 'deduplicate' is True",
                pattern="FactPattern",
                deduplicate=deduplicate,
            )
            raise ValueError("FactPattern: 'keys' required when 'deduplicate' is True.")

        ctx.debug(
            "FactPattern validation passed",
            pattern="FactPattern",
            deduplicate=deduplicate,
            keys=keys,
        )

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        deduplicate = self.params.get("deduplicate")
        keys = self.params.get("keys")
        target = self.params.get("target")

        ctx.debug(
            "Fact pattern starting",
            pattern="FactPattern",
            deduplicate=deduplicate,
            keys=keys,
            target=target,
        )

        df = context.df

        source_count = None
        try:
            if context.engine_type == "spark":
                source_count = df.count()
            else:
                source_count = len(df)
            ctx.debug(
                "Fact source data loaded",
                pattern="FactPattern",
                source_rows=source_count,
            )
        except Exception:
            ctx.debug("Fact could not determine source row count", pattern="FactPattern")

        dedup_count = None
        if deduplicate:
            ctx.debug(
                "Fact pattern applying deduplication",
                pattern="FactPattern",
                keys=keys,
            )
            try:
                if context.engine_type == "spark":
                    df = df.dropDuplicates(keys)
                    dedup_count = df.count()
                else:
                    df = df.drop_duplicates(subset=keys)
                    dedup_count = len(df)

                removed_count = (
                    (source_count - dedup_count)
                    if source_count is not None and dedup_count is not None
                    else None
                )
                ctx.debug(
                    "Fact pattern deduplication complete",
                    pattern="FactPattern",
                    rows_after_dedup=dedup_count,
                    duplicates_removed=removed_count,
                )
            except Exception as e:
                elapsed_ms = (time.time() - start_time) * 1000
                ctx.error(
                    f"Fact pattern deduplication failed: {e}",
                    pattern="FactPattern",
                    error_type=type(e).__name__,
                    elapsed_ms=round(elapsed_ms, 2),
                    keys=keys,
                )
                raise

        elapsed_ms = (time.time() - start_time) * 1000
        result_count = dedup_count if deduplicate else source_count

        ctx.info(
            "Fact pattern completed",
            pattern="FactPattern",
            elapsed_ms=round(elapsed_ms, 2),
            source_rows=source_count,
            result_rows=result_count,
            deduplicate=deduplicate,
            keys=keys,
            target=target,
        )

        return df
