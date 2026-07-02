"""SparkEngine reuses an already-running SparkSession instead of building one.

Regression for the Databricks-serverless INVALID_CONNECT_URL failure: when no
session is passed, the engine must adopt the active notebook session rather than
constructing a new local Spark / Connect client (no sc:// URL on serverless).
"""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from odibi.engine.spark_engine import SparkEngine  # noqa: E402


def _fake_spark_session(active):
    """Stand-in for pyspark.sql.SparkSession whose getActiveSession() returns `active`.

    Uses a plain lambda (not staticmethod) so it stays callable across Python
    3.9–3.12 when accessed off a MagicMock.
    """
    cls = MagicMock(name="SparkSession")
    cls.getActiveSession = lambda: active
    return cls


def _patch_spark_session(monkeypatch, fake):
    """Patch via the string target so monkeypatch resolves pyspark.sql fresh from
    sys.modules — SparkEngine.__init__ does `from pyspark.sql import SparkSession`
    at call time, and other suites (test_catalog_mock_engine_*) permanently swap
    sys.modules["pyspark.sql"] for a mock module. Patching the object we imported
    at module top would miss that swapped module; the string form hits the same
    object the engine reads. monkeypatch restores it on teardown.
    """
    monkeypatch.setattr("pyspark.sql.SparkSession", fake)


def test_reuses_active_session_when_none_passed(monkeypatch):
    sentinel = MagicMock(name="active_session")
    _patch_spark_session(monkeypatch, _fake_spark_session(sentinel))

    engine = SparkEngine()

    # Identity proves the build path was not taken — a built session would be a
    # real SparkSession, never our sentinel.
    assert engine.spark is sentinel


def test_explicit_session_takes_precedence_over_active(monkeypatch):
    active = MagicMock(name="active_session")
    explicit = MagicMock(name="explicit_session")
    _patch_spark_session(monkeypatch, _fake_spark_session(active))

    engine = SparkEngine(spark_session=explicit)

    assert engine.spark is explicit


def test_active_session_flag_is_logged_as_existing(monkeypatch):
    sentinel = MagicMock(name="active_session")
    _patch_spark_session(monkeypatch, _fake_spark_session(sentinel))

    engine = SparkEngine()

    # Reused session must be treated as "existing" (not a fresh build).
    assert engine.spark is sentinel
    assert engine.connections == {}
