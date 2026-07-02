"""SparkEngine reuses an already-running SparkSession instead of building one.

Regression for the Databricks-serverless INVALID_CONNECT_URL failure: when no
session is passed, the engine must adopt the active notebook session rather than
constructing a new local Spark / Connect client (no sc:// URL on serverless).
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from odibi.engine.spark_engine import SparkEngine  # noqa: E402


def test_reuses_active_session_when_none_passed(monkeypatch):
    sentinel = MagicMock(name="active_session")
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: sentinel))

    engine = SparkEngine()

    # Identity proves the build path was not taken — a built session would be a
    # real SparkSession, never our sentinel.
    assert engine.spark is sentinel


def test_explicit_session_takes_precedence_over_active(monkeypatch):
    active = MagicMock(name="active_session")
    explicit = MagicMock(name="explicit_session")
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: active))

    engine = SparkEngine(spark_session=explicit)

    assert engine.spark is explicit


def test_active_session_flag_is_logged_as_existing(monkeypatch):
    sentinel = MagicMock(name="active_session")
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: sentinel))

    engine = SparkEngine()

    # Reused session must be treated as "existing" (not a fresh build).
    assert engine.spark is sentinel
    assert engine.connections == {}
