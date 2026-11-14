"""Spark catalog reads respect connection namespaces, without changing other reads."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from odibi.connections.factory import create_delta_connection
from odibi.connections.local import LocalConnection
from odibi.connections.unity_catalog import UnityCatalogConnection
from odibi.engine.spark_engine import SparkEngine


@pytest.fixture
def engine():
    engine = SparkEngine.__new__(SparkEngine)
    engine.spark = MagicMock()
    for reader in (engine.spark.read, engine.spark.readStream):
        reader.format.return_value = reader
        reader.option.return_value = reader
        reader.options.return_value = reader
        reader.table.side_effect = lambda name: ("table", name)
        reader.load.side_effect = lambda *args: ("load", args)
    return engine


@pytest.fixture(params=["delta", "unity_catalog"])
def catalog_connection(request):
    if request.param == "delta":
        return create_delta_connection(
            "warehouse", {"catalog": "configured_catalog", "schema_name": "configured_schema"}
        )
    return UnityCatalogConnection("configured_catalog", "configured_schema", create_schema=False)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "table,expected",
    [
        ("orders", "configured_catalog.configured_schema.orders"),
        ("other_schema.orders", "other_schema.orders"),
        ("other_catalog.other_schema.orders", "other_catalog.other_schema.orders"),
    ],
)
def test_catalog_table_namespace(engine, catalog_connection, streaming, table, expected):
    assert engine.read(
        catalog_connection, "delta", table=table, streaming=streaming, as_of_version=7
    ) == ("table", expected)


@pytest.mark.parametrize("streaming", [False, True])
def test_local_connection_does_not_turn_session_table_into_file_path(engine, tmp_path, streaming):
    assert engine.read(
        LocalConnection(str(tmp_path)), "delta", table="session_table", streaming=streaming
    ) == ("table", "session_table")


@pytest.mark.parametrize("streaming", [False, True])
def test_local_file_path_still_uses_connection_root(engine, tmp_path, streaming):
    assert engine.read(
        LocalConnection(str(tmp_path)), "parquet", path="orders.parquet", streaming=streaming
    ) == ("load", (str(tmp_path / "orders.parquet"),))


@pytest.mark.parametrize("streaming", [False, True])
def test_catalog_volume_path_remains_file_read(engine, catalog_connection, streaming):
    path = "/Volumes/other_catalog/other_schema/incoming/orders"
    assert engine.read(catalog_connection, "parquet", path=path, streaming=streaming) == (
        "load",
        (path,),
    )


def test_jdbc_table_remains_unqualified_by_catalog_resolver(engine):
    connection = SimpleNamespace(
        catalog="unrelated_catalog",
        schema="unrelated_schema",
        get_spark_options=lambda: {"url": "jdbc:sqlserver://synthetic", "driver": "synthetic"},
        get_path=lambda name: pytest.fail("JDBC table must not use catalog resolver"),
    )
    assert engine.read(connection, "sql", table="dbo.orders") == ("load", ())
    assert engine.spark.read.options.call_args.kwargs["dbtable"] == "dbo.orders"
