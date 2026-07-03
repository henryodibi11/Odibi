"""Serverless-safe DataFrame caching helpers.

Databricks serverless compute blocks ``DataFrame.persist()`` / ``.cache()``
(``[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported``). Caching is
a performance optimization only — it never changes results — so on serverless we
skip it and let Spark recompute. Serverless also disk-caches Delta/Parquet reads
automatically, so the cost of skipping is small.

These wrappers attempt the operation and, on the first failure, sticky-disable
caching for the rest of the process so we neither retry nor spam logs. On
clusters where caching works, behavior is unchanged.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Flipped to True the first time persist/cache raises (e.g. on serverless). Once
# set, all subsequent cache attempts are skipped for the life of the process.
_caching_disabled = False


def caching_disabled() -> bool:
    """Return whether caching has been sticky-disabled this process (test hook)."""
    return _caching_disabled


def reset_caching_state() -> None:
    """Re-enable caching attempts. Intended for tests, not runtime use."""
    global _caching_disabled
    _caching_disabled = False


def _safe_call(df: Any, method: str, args: tuple, kwargs: dict) -> Any:
    """Invoke ``df.<method>(...)``; on failure sticky-disable caching and return ``df``.

    Caching is optional, so any failure (notably serverless blocking PERSIST) is
    swallowed: we disable further attempts and return the uncached frame. Results
    are unaffected — only recomputation cost changes.
    """
    global _caching_disabled
    if _caching_disabled:
        return df
    try:
        return getattr(df, method)(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - caching must never break a run
        _caching_disabled = True
        # Use only the first line of the exception to avoid dumping full JVM
        # stacktraces from Py4J / Spark Connect into the user-visible log.
        short_msg = str(exc).split("\n", 1)[0]
        logger.info(
            "DataFrame caching unavailable (%s: %s) — continuing without cache; "
            "results are unaffected.",
            type(exc).__name__,
            short_msg,
        )
        return df


def safe_persist(df: Any, *args: Any, **kwargs: Any) -> Any:
    """``df.persist(...)``, or return ``df`` unchanged where caching is unsupported."""
    return _safe_call(df, "persist", args, kwargs)


def safe_cache(df: Any) -> Any:
    """``df.cache()``, or return ``df`` unchanged where caching is unsupported."""
    return _safe_call(df, "cache", (), {})


def safe_unpersist(df: Any, *args: Any, **kwargs: Any) -> Any:
    """``df.unpersist(...)`` best-effort; never raises.

    The frame may never have been cached (caching disabled) or unpersist may be
    restricted; either way, cleanup failure must not break a run.
    """
    try:
        return df.unpersist(*args, **kwargs)
    except Exception:  # noqa: BLE001 - best-effort cleanup
        return df
