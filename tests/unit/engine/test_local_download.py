"""Client-local Delta snapshots through the public Spark write interface."""

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from pydantic import ValidationError

from odibi.config import NodeConfig, PerformanceConfig, WriteConfig
from odibi.connections.local import LocalConnection
from odibi.engine.polars_engine import PolarsEngine
from odibi.engine.spark_engine import SparkEngine
from odibi.node import NodeExecutor

pytest.importorskip("polars")


class RemoteResult:
    """A collected-result fixture; intentionally provides no Spark write API."""

    isStreaming = False

    def __init__(self, table):
        self.table = table

    def limit(self, rows):
        return RemoteResult(self.table.slice(0, rows))

    def toArrow(self):
        return self.table


@pytest.fixture
def engine():
    spark = MagicMock()
    spark.version = "4.1.0"
    return SparkEngine(spark_session=spark)


@pytest.fixture
def data():
    return pa.table(
        {
            "id": pa.array([8, 3, 8], type=pa.int64()),
            "amount": pa.array(
                [Decimal("12.34"), None, Decimal("-0.25")], type=pa.decimal128(12, 2)
            ),
            "day": pa.array([date(2026, 1, 2), None, date(2026, 2, 3)], type=pa.date32()),
            "at": pa.array(
                [datetime(2026, 1, 2, tzinfo=timezone.utc), None, None],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "nullable": pa.array([None, None, None], type=pa.int64()),
            "tags": pa.array([["a", "b"], None, []], type=pa.list_(pa.string())),
        }
    )


def test_delta_snapshot_values_schema_modes_and_limit(engine, data, tmp_path):
    connection = LocalConnection(str(tmp_path))
    kwargs = dict(
        connection=connection,
        format="delta",
        path="snapshot",
        options={"local_download": {"max_rows": 3}},
    )
    result = engine.write(RemoteResult(data), **kwargs)
    path = connection.get_path("snapshot")
    saved = DeltaTable(path).to_pyarrow_table()
    assert saved.to_pylist() == data.to_pylist()
    assert saved.schema.field("amount").type == pa.decimal128(12, 2)
    assert saved.schema.field("nullable").type == pa.int64()
    assert saved.schema.field("at").type == pa.timestamp("us", tz="UTC")
    assert result["_cached_row_count"] == 3
    assert result["version"] == 0
    engine.write(RemoteResult(data), mode="append", **kwargs)
    assert sorted(DeltaTable(path).to_pyarrow_table().to_pylist(), key=str) == sorted(
        data.to_pylist() * 2, key=str
    )
    version = DeltaTable(path).version()
    with pytest.raises(ValueError, match="exceeds max_rows"):
        engine.write(RemoteResult(pa.concat_tables([data, data])), **kwargs)
    assert DeltaTable(path).version() == version
    assert DeltaTable(path).to_pyarrow_table().num_rows == 6
    engine.write(RemoteResult(data.slice(1, 1)), **kwargs)
    assert DeltaTable(path).to_pyarrow_table().to_pylist() == data.slice(1, 1).to_pylist()
    local = PolarsEngine()
    reread = local.materialize(local.read(connection, "delta", path="snapshot"))
    assert reread.to_dicts() == data.slice(1, 1).to_pylist()
    assert reread.to_arrow().schema.field("amount").type == pa.decimal128(12, 2)
    original = local.materialize(
        local.read(connection, "delta", path="snapshot", options={"versionAsOf": 0})
    )
    assert sorted(original.to_dicts(), key=str) == sorted(data.to_pylist(), key=str)


def test_empty_typed_snapshot_and_partitioned_output(engine, data, tmp_path):
    connection = LocalConnection(str(tmp_path))
    engine.write(
        RemoteResult(data.slice(0, 0)),
        connection,
        "delta",
        path="empty",
        options={"local_download": {"max_rows": 1}},
    )
    assert DeltaTable(connection.get_path("empty")).to_pyarrow_table().num_rows == 0
    engine.write(
        RemoteResult(data),
        connection,
        "delta",
        path="partitioned",
        options={"local_download": {"max_rows": 3}, "partition_by": ["id"]},
    )
    assert DeltaTable(connection.get_path("partitioned")).metadata().partition_columns == ["id"]
    local = PolarsEngine()
    assert local.materialize(local.read(connection, "delta", path="empty")).height == 0
    partitioned = local.materialize(local.read(connection, "delta", path="partitioned"))
    assert sorted(partitioned.to_dicts(), key=str) == sorted(data.to_pylist(), key=str)


def test_download_schema_evolution(engine, data, tmp_path):
    connection = LocalConnection(str(tmp_path))
    options = {"local_download": {"max_rows": 3}}
    engine.write(RemoteResult(data), connection, "delta", path="evolve", options=options)
    evolved = data.append_column("new_column", pa.array(["a", "b", "c"]))
    engine.write(
        RemoteResult(evolved),
        connection,
        "delta",
        path="evolve",
        mode="append",
        options={**options, "mergeSchema": True},
    )
    saved = DeltaTable(connection.get_path("evolve")).to_pyarrow_table()
    assert saved.num_rows == 6
    assert sorted(x for x in saved.column("new_column").to_pylist() if x is not None) == [
        "a",
        "b",
        "c",
    ]
    engine.write(
        RemoteResult(data.select(["id"])),
        connection,
        "delta",
        path="evolve",
        options={**options, "overwriteSchema": True},
    )
    assert DeltaTable(connection.get_path("evolve")).to_pyarrow_table().column_names == ["id"]


def test_ordinary_spark_write_still_uses_spark_writer(engine, tmp_path):
    df = MagicMock()
    df.isStreaming = False
    df.rdd.getNumPartitions.return_value = 1
    engine.write(df, LocalConnection(str(tmp_path)), "parquet", path="normal")
    df.write.format.return_value.mode.return_value.save.assert_called_once_with(
        str(tmp_path / "normal")
    )


@pytest.mark.parametrize("max_rows", [0, -1, True, "3"])
def test_download_bound_is_a_positive_integer(max_rows):
    with pytest.raises(ValidationError):
        WriteConfig(connection="local", format="delta", path="out", download={"max_rows": max_rows})


@pytest.mark.parametrize(
    "extra",
    [
        {"format": "parquet"},
        {"mode": "upsert", "options": {"keys": ["id"]}},
        {"register_table": "scratch"},
        {"auto_optimize": True},
        {"skip_if_unchanged": True},
        {"options": {"deep_diagnostics": True}},
    ],
)
def test_unsupported_write_combinations_fail_at_configuration(extra):
    values = dict(connection="local", format="delta", path="out", download={"max_rows": 3})
    values.update(extra)
    with pytest.raises(ValidationError):
        WriteConfig(**values)


def test_materialized_download_rejected():
    with pytest.raises(ValidationError, match="materialized"):
        NodeConfig(
            name="snapshot",
            materialized="incremental",
            write={
                "connection": "local",
                "format": "delta",
                "path": "out",
                "download": {"max_rows": 3},
            },
        )


def test_uri_destination_rejected_before_collection(engine, data):
    with pytest.raises(ValueError, match="local filesystem"):
        engine.write(
            RemoteResult(data),
            LocalConnection("abfss://container@account/out"),
            "delta",
            path="snapshot",
            options={"local_download": {"max_rows": 3}},
        )


def test_yaml_node_routes_download_and_reports_local_commit(engine, data, tmp_path):
    connection = LocalConnection(str(tmp_path))
    node_engine = MagicMock()
    node_engine.name = "spark"
    node_engine.write.side_effect = engine.write
    node_engine.count_rows.return_value = 3
    node_engine.get_schema.return_value = {"id": "long"}
    node_engine.get_source_files.return_value = []
    executor = NodeExecutor(
        context=MagicMock(),
        engine=node_engine,
        connections={"local": connection},
        max_sample_rows=0,
        performance_config=PerformanceConfig(
            skip_null_profiling=True,
            skip_catalog_writes=True,
            delta_table_properties={"delta.autoOptimize.optimizeWrite": "true"},
        ),
    )
    import yaml

    config = NodeConfig.model_validate(
        yaml.safe_load("""
name: snapshot
write:
  connection: local
  format: delta
  path: snapshot
  download:
    max_rows: 3
""")
    )
    first = executor.execute(config, input_df=RemoteResult(data))
    assert first.success, first.error
    assert first.rows_written == 3
    assert first.metadata["delta_info"]["version"] == 0
    second = executor.execute(config, input_df=RemoteResult(data))
    assert second.success, second.error
    assert second.metadata["delta_info"]["version"] == 1
    assert (
        DeltaTable(connection.get_path("snapshot")).to_pyarrow_table().to_pylist()
        == data.to_pylist()
    )
