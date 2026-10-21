"""Skip guard for tests that need a usable pyspark session.

GitHub Actions runners install pyspark but some of its symbols can resolve to
"unknown location" (see AGENTS.md), so importing ``pyspark.sql`` succeeds while
later lazy imports inside pyspark raise. ``SparkSession.getActiveSession()`` is
one of them: it does ``from pyspark import SparkContext`` and fails with

    ImportError: cannot import name 'SparkContext' from 'pyspark' (unknown location)

Tests must skip in that environment instead of failing, and must never build a
session in a process that already has one (single-process CI runs share state).
"""

import pytest


def usable_spark_session(spark_session_cls):
    """Return ``spark_session_cls`` or skip when it cannot be used here.

    Skips when pyspark's symbols are unimportable (the CI runner quirk) or when
    a SparkSession is already active in this process, so the caller can safely
    build a session of its own and stop it afterwards.
    """
    try:
        active = spark_session_cls.getActiveSession()
    except ImportError as exc:
        pytest.skip(f"pyspark is not usable in this environment: {exc}")
    if active is not None:
        pytest.skip("needs a process with no active session so the caller builds one")
    return spark_session_cls
