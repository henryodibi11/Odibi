import builtins
from contextlib import contextmanager
import http.client
import importlib.metadata
import importlib.util
import io
import locale
import logging
import multiprocessing
import os
from pathlib import Path
import random
import signal
import smtplib
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, ContextManager, Iterator, List
import urllib.request
import warnings

import pytest

if sys.platform == "win32":
    try:
        from odibi.engine import spark_engine

        spark_engine.SparkEngine.__init__ = lambda self, *args, **kwargs: pytest.skip(
            "Skipping Spark tests on Windows due to missing winutils"
        )
    except Exception:
        pass
    try:
        from pyspark import SparkContext

        SparkContext.__init__ = lambda self, *args, **kwargs: pytest.skip(
            "Skipping Spark tests (SparkContext) on Windows due to missing winutils"
        )
    except Exception:
        pass
    try:
        from pyspark import SparkContext

        SparkContext.__new__ = lambda cls, *args, **kwargs: pytest.skip(
            "Skipping Spark tests via __new__ on Windows due to missing winutils"
        )
    except Exception:
        pass


@pytest.fixture(autouse=True)
def _reset_logging_context():
    """Reset the global LoggingContext singleton between tests.

    Prevents state pollution when tests run in batch — the singleton
    accumulates pipeline/node context and Rich handler state that can
    cause TypeError or AttributeError in later tests.
    """
    import odibi.utils.logging_context as lc

    original = lc._global_context
    lc._global_context = None
    yield
    lc._global_context = original


def _immutable_process_snapshot():
    """Capture process-global state that planning operations must preserve."""

    def handler_state(handler):
        formatter = handler.formatter
        return (
            id(handler),
            handler.level,
            tuple(id(item) for item in handler.filters),
            None
            if formatter is None
            else (
                id(formatter),
                formatter._fmt,
                formatter.datefmt,
                formatter.default_time_format,
                formatter.default_msec_format,
            ),
        )

    def mapping_state(module_name, owner_name, attribute):
        module = sys.modules.get(module_name)
        owner = (
            module if module is not None and not owner_name else getattr(module, owner_name, None)
        )
        value = getattr(owner, attribute, None) if owner is not None else None
        if not isinstance(value, dict):
            return None
        return tuple(sorted((key, id(item)) for key, item in value.items()))

    root = logging.getLogger()
    loggers = {
        name: (
            logger.level,
            logger.disabled,
            logger.propagate,
            tuple(handler_state(handler) for handler in logger.handlers),
            tuple(id(item) for item in logger.filters),
        )
        for name, logger in logging.Logger.manager.loggerDict.items()
        if isinstance(logger, logging.Logger)
    }
    return {
        "cwd": os.getcwd(),
        "environment": dict(os.environ),
        "sys_path": tuple(sys.path),
        "modules": tuple(sorted((name, id(module)) for name, module in sys.modules.items())),
        "warnings": tuple(warnings.filters),
        "root_logger": (
            root.level,
            root.disabled,
            tuple(handler_state(handler) for handler in root.handlers),
            tuple(id(item) for item in root.filters),
        ),
        "loggers": loggers,
        "threads": tuple((thread.ident, thread.name) for thread in threading.enumerate()),
        "locale": locale.setlocale(locale.LC_ALL),
        "timezone": (time.tzname, time.timezone, time.altzone, time.daylight),
        "random": random.getstate(),
        "signals": tuple(
            (number, signal.getsignal(number))
            for number in sorted(signal.valid_signals())
            if number not in {signal.SIGKILL, signal.SIGSTOP}
        ),
        "transform_functions": mapping_state("odibi.registry", "FunctionRegistry", "_functions"),
        "transform_signatures": mapping_state("odibi.registry", "FunctionRegistry", "_signatures"),
        "transform_models": mapping_state("odibi.registry", "FunctionRegistry", "_param_models"),
        "connection_factories": mapping_state("odibi.plugins", "", "_CONNECTION_FACTORIES"),
        "tool_registry": mapping_state("odibi_mcp.tools.workflows", "", "TOOL_REGISTRY"),
        "action_effects": mapping_state("odibi_mcp.dispatcher", "", "ACTION_EFFECTS"),
        "alert_throttle": (
            mapping_state("odibi.utils.alerting", "_throttler", "_last_alerts"),
            mapping_state("odibi.utils.alerting", "_throttler", "_alert_counts"),
        ),
    }


@pytest.fixture
def immutable_planning_tripwires() -> Callable[[], ContextManager[List[str]]]:
    """Deny generic I/O/import/global mutation and verify exact state restoration."""

    @contextmanager
    def installed() -> Iterator[List[str]]:
        attempts: List[str] = []

        def deny(name: str):
            def blocked(*args, **kwargs):
                attempts.append(name)
                raise AssertionError("effect-tripwire-canary-e933")

            return blocked

        def guarded_open(file, mode="r", *args, **kwargs):
            attempts.append(f"open:{mode}")
            raise AssertionError("effect-tripwire-canary-e933")

        def patch_loaded(monkeypatch, module_name, attributes):
            module = sys.modules.get(module_name)
            if module is None:
                return
            for attribute in attributes:
                owner = module
                parts = attribute.split(".")
                for part in parts[:-1]:
                    owner = getattr(owner, part, None)
                    if owner is None:
                        break
                else:
                    name = parts[-1]
                    if hasattr(owner, name):
                        monkeypatch.setattr(owner, name, deny(f"{module_name}.{attribute}"))

        before = _immutable_process_snapshot()
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(builtins, "open", guarded_open)
            for owner, names in (
                (io, ("open",)),
                (
                    os,
                    (
                        "open",
                        "remove",
                        "unlink",
                        "rename",
                        "replace",
                        "mkdir",
                        "makedirs",
                        "chdir",
                        "putenv",
                        "unsetenv",
                        "system",
                        "popen",
                        "spawnl",
                        "spawnle",
                        "spawnlp",
                        "spawnlpe",
                        "spawnv",
                        "spawnve",
                        "spawnvp",
                        "spawnvpe",
                    ),
                ),
                (subprocess, ("run", "Popen", "call", "check_call", "check_output")),
                (multiprocessing, ("Process", "Pool")),
                (socket, ("socket", "create_connection", "getaddrinfo")),
                (urllib.request, ("urlopen",)),
                (http.client, ("HTTPConnection", "HTTPSConnection")),
                (smtplib, ("SMTP", "SMTP_SSL")),
                (importlib.metadata, ("entry_points",)),
                (importlib.util, ("spec_from_file_location", "module_from_spec")),
                (
                    tempfile,
                    (
                        "TemporaryFile",
                        "NamedTemporaryFile",
                        "TemporaryDirectory",
                        "mkstemp",
                        "mkdtemp",
                    ),
                ),
                (
                    Path,
                    (
                        "open",
                        "read_text",
                        "read_bytes",
                        "write_text",
                        "write_bytes",
                        "touch",
                        "mkdir",
                        "unlink",
                        "rename",
                        "replace",
                    ),
                ),
            ):
                for name in names:
                    if hasattr(owner, name):
                        monkeypatch.setattr(owner, name, deny(f"{owner.__name__}.{name}"))
            for module_name, attributes in (
                ("requests.sessions", ("Session.request",)),
                ("httpx", ("Client.request", "AsyncClient.request")),
                (
                    "azure.identity",
                    (
                        "DefaultAzureCredential",
                        "ClientSecretCredential",
                        "ManagedIdentityCredential",
                        "AzureCliCredential",
                    ),
                ),
                (
                    "odibi.plugins",
                    (
                        "entry_points",
                        "load_plugins",
                        "register_connection_factory",
                        "get_connection_factory",
                    ),
                ),
                ("odibi.utils.extensions", ("load_extensions",)),
                ("odibi.utils.logging", ("configure_logging",)),
                ("odibi.utils.alerting", ("send_alert", "AlertThrottler.should_send")),
                (
                    "odibi.utils.telemetry",
                    ("setup_telemetry", "get_tracer", "get_meter"),
                ),
                ("odibi.engine.spark_engine", ("SparkEngine",)),
                ("odibi.engine.pandas_engine", ("PandasEngine",)),
                ("odibi.engine.polars_engine", ("PolarsEngine",)),
                ("pyspark.sql", ("SparkSession",)),
                ("odibi.catalog", ("CatalogManager",)),
                (
                    "odibi.state",
                    ("StateManager", "create_state_backend", "create_sync_source_backend"),
                ),
                ("odibi.story.generator", ("StoryGenerator",)),
                ("odibi.story.lineage", ("LineageGenerator",)),
                ("odibi.lineage", ("OpenLineageAdapter", "LineageTracker")),
                (
                    "odibi.connections.factory",
                    (
                        "create_local_connection",
                        "create_http_connection",
                        "create_azure_blob_connection",
                        "create_delta_connection",
                        "create_sql_server_connection",
                        "create_postgres_connection",
                        "create_unity_catalog_connection",
                        "register_builtins",
                    ),
                ),
                ("importlib.metadata", ("EntryPoint.load",)),
            ):
                patch_loaded(monkeypatch, module_name, attributes)
            monkeypatch.setattr(logging, "basicConfig", deny("logging.basicConfig"))
            monkeypatch.setattr(builtins, "__import__", deny("builtins.__import__"))
            yield attempts

        assert _immutable_process_snapshot() == before

    return installed


def _is_spark_or_delta_name(name: str) -> bool:
    """Check if a filename (not full path) contains spark or delta keywords."""
    name = name.lower()
    return "spark" in name or "delta" in name


def pytest_ignore_collect(collection_path, config):
    # Ignore collection of test FILES whose name contains spark/delta on Windows.
    # Only checks the filename, not the full path, to avoid false positives
    # (e.g., a parent directory named "delta" or test content mentioning delta).
    if sys.platform == "win32" and _is_spark_or_delta_name(collection_path.name):
        return True


def pytest_collection_modifyitems(config, items):
    if sys.platform == "win32":
        items[:] = [item for item in items if not _is_spark_or_delta_name(item.fspath.basename)]
