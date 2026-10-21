"""The roundtrip test must skip, not fail, when pyspark is present but unusable.

Regression for the CI failure on PR #361: the Test Extras and Test SQL
Dependencies jobs install pyspark, but ``SparkSession.getActiveSession()``
raises ``ImportError: cannot import name 'SparkContext' from 'pyspark' (unknown
location)``, so tests/engine/test_spark_local_delta_roundtrip.py failed instead
of skipping. The guard is pinned here with fake session classes, so this file
runs on Windows and in the required CI job without importing pyspark at all.
"""

import pytest

from tests.engine.engine_session_guard import usable_spark_session


class _UnimportableSymbols:
    """Mimics pyspark on CI runners: getActiveSession raises ImportError."""

    @staticmethod
    def getActiveSession():
        raise ImportError("cannot import name 'SparkContext' from 'pyspark' (unknown location)")


class _AlreadyActive:
    """Mimics a process that already runs a SparkSession."""

    @staticmethod
    def getActiveSession():
        return object()


class _Idle:
    """Mimics a healthy process with no active SparkSession."""

    @staticmethod
    def getActiveSession():
        return None


class _NonImportErrorFailure:
    """Mimics an unexpected failure that must propagate, not turn into a skip."""

    @staticmethod
    def getActiveSession():
        raise RuntimeError("boom")


def test_guard_skips_when_pyspark_symbols_are_unimportable():
    with pytest.raises(pytest.skip.Exception) as excinfo:
        usable_spark_session(_UnimportableSymbols)

    assert "pyspark is not usable in this environment" in str(excinfo.value)


def test_guard_propagates_non_import_errors():
    try:
        usable_spark_session(_NonImportErrorFailure)
    except RuntimeError as exc:
        assert "boom" in str(exc)
    except pytest.skip.Exception as exc:
        pytest.fail(f"guard swallowed a non-ImportError failure: {exc}")
    else:
        pytest.fail("guard returned instead of propagating the failure")


def test_guard_skips_when_a_session_is_already_active():
    with pytest.raises(pytest.skip.Exception):
        usable_spark_session(_AlreadyActive)


def test_guard_returns_the_class_when_no_session_is_active():
    assert usable_spark_session(_Idle) is _Idle
