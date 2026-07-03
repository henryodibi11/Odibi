"""Serverless-safe caching helpers (odibi.utils.spark_cache).

Caching is a performance optimization only, so on serverless (which blocks
PERSIST) these must degrade gracefully: return the frame unchanged, sticky-disable
further attempts, and never raise.
"""

import pytest

from odibi.utils import spark_cache
from odibi.utils.spark_cache import (
    caching_disabled,
    reset_caching_state,
    safe_cache,
    safe_persist,
    safe_unpersist,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_caching_state()
    yield
    reset_caching_state()


class _OkDF:
    def __init__(self):
        self.persisted = False
        self.cached = False

    def persist(self, *a, **k):
        self.persisted = True
        return self

    def cache(self, *a, **k):
        self.cached = True
        return self

    def unpersist(self, *a, **k):
        self.persisted = False
        return self


class _ServerlessDF:
    def persist(self, *a, **k):
        raise Exception("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported")

    def cache(self, *a, **k):
        raise Exception("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported")

    def unpersist(self, *a, **k):
        raise Exception("unpersist blocked")


def test_persist_works_when_supported():
    df = _OkDF()
    assert safe_persist(df) is df
    assert df.persisted is True
    assert caching_disabled() is False


def test_cache_calls_cache_not_persist():
    # safe_cache must call df.cache() (not persist) — callers/tests assert cache().
    df = _OkDF()
    assert safe_cache(df) is df
    assert df.cached is True
    assert df.persisted is False


def test_serverless_failure_returns_df_and_sticky_disables():
    df = _ServerlessDF()
    # Must not raise; returns the (uncached) frame.
    assert safe_cache(df) is df
    assert caching_disabled() is True


def test_no_retry_after_disabled():
    reset_caching_state()
    safe_cache(_ServerlessDF())  # trips the sticky flag
    assert caching_disabled() is True
    # A working frame is now skipped without even attempting cache.
    ok = _OkDF()
    assert safe_cache(ok) is ok
    assert ok.cached is False


def test_unpersist_never_raises():
    assert safe_unpersist(_ServerlessDF()) is not None  # swallows the error


def test_unpersist_works_when_supported():
    df = _OkDF()
    df.persisted = True
    safe_unpersist(df)
    assert df.persisted is False


def test_reset_reenables():
    safe_cache(_ServerlessDF())
    assert caching_disabled() is True
    reset_caching_state()
    assert caching_disabled() is False
    df = _OkDF()
    safe_cache(df)
    assert df.cached is True


def test_module_flag_isolated():
    # Sanity: the fixture reset leaves us enabled at test start.
    assert spark_cache.caching_disabled() is False
