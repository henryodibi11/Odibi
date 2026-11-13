"""Opt-in remote synthetic-data qualification; never executes the supplied pipeline.

Run with the dedicated Connect interpreter, an existing credential YAML, and a
new output directory outside the repository. Configuration stays in-process.
"""

import argparse
from datetime import date
from decimal import Decimal
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True, help="New directory outside the source checkout")
    args = parser.parse_args()
    stage = "configuration"
    try:
        import yaml
        from deltalake import DeltaTable

        from odibi.config import NodeConfig, PerformanceConfig
        from odibi.connections.databricks import DatabricksConnection
        from odibi.connections.local import LocalConnection
        from odibi.context import SparkContext
        from odibi.engine.polars_engine import PolarsEngine
        from odibi.engine.spark_engine import SparkEngine
        from odibi.node import NodeExecutor

        output = Path(args.output).resolve()
        repo = Path(__file__).resolve().parents[1]
        if output == repo or repo in output.parents:
            raise ValueError("Output must be outside the checkout")
        if output.exists():
            raise ValueError("Output must be a new directory")
        # Deliberately parse only connection settings. Do not execute pipelines,
        # imports, custom functions, storage validation or schema creation from YAML.
        with Path(args.config).open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        compute = [v for v in raw["connections"].values() if v.get("type") == "databricks"]
        if len(compute) != 1:
            raise ValueError("Expected exactly one Databricks connection")
        values = dict(compute[0])
        values.pop("type")
        connection = DatabricksConnection(**values)
        del raw, compute, values
        stage = "session"
        engine = SparkEngine(connections={"compute": connection})
        writer = PolarsEngine()  # Fail before queries if Polars is missing.
        spark = engine.spark
        stage = "remote synthetic query"
        remote = spark.sql("""
            SELECT * FROM VALUES
              (8, CAST(12.34 AS DECIMAL(12,2)), DATE '2026-01-02'),
              (3, CAST(NULL AS DECIMAL(12,2)), CAST(NULL AS DATE)),
              (8, CAST(-0.25 AS DECIMAL(12,2)), DATE '2026-02-03')
            AS synthetic(id, amount, day)
        """)
        if not callable(getattr(remote, "toArrow", None)):
            raise RuntimeError("Client lacks public Arrow collection")
        output.mkdir(parents=True)
        local = LocalConnection(str(output))
        executor = NodeExecutor(
            context=SparkContext(spark),
            engine=engine,
            connections={"snapshot": local},
            max_sample_rows=0,
            performance_config=PerformanceConfig(
                skip_catalog_writes=True, skip_null_profiling=True
            ),
        )
        # This is the new YAML write contract, independent of the supplied project.
        config = NodeConfig.model_validate(
            yaml.safe_load("""
name: synthetic_snapshot
write:
  connection: snapshot
  format: delta
  path: result
  mode: overwrite
  download:
    max_rows: 3
""")
        )
        stage = "YAML node local overwrite"
        result = executor.execute(config, input_df=remote, suppress_error_log=True)
        if not result.success or result.rows_written != 3:
            raise RuntimeError("Snapshot node failed")
        path = local.get_path("result")
        expected = [
            {"id": 8, "amount": Decimal("12.34"), "day": date(2026, 1, 2)},
            {"id": 3, "amount": None, "day": None},
            {"id": 8, "amount": Decimal("-0.25"), "day": date(2026, 2, 3)},
        ]
        table = DeltaTable(path).to_pyarrow_table()
        assert sorted(table.to_pylist(), key=str) == sorted(expected, key=str)
        assert str(table.schema.field("amount").type) == "decimal128(12, 2)"
        assert Path(path, "_delta_log").is_dir()
        stage = "local append"
        config.write.mode = "append"
        result = executor.execute(config, input_df=remote, suppress_error_log=True)
        assert result.success and result.rows_written == 3
        assert sorted(DeltaTable(path).to_pyarrow_table().to_pylist(), key=str) == sorted(
            expected * 2, key=str
        )
        stage = "limit rejection"
        version = DeltaTable(path).version()
        result = executor.execute(
            config, input_df=remote.unionByName(remote), suppress_error_log=True
        )
        assert not result.success
        assert "exceeds max_rows" in result.error.message
        assert DeltaTable(path).version() == version
        assert DeltaTable(path).to_pyarrow_table().num_rows == 6
        stage = "offline local read"
        assert writer.materialize(writer.read(local, "delta", path="result")).height == 6
        print("PASS: remote synthetic Spark -> YAML node -> laptop-local Delta")
        print(
            "PASS: exact values, decimal schema, overwrite, append, limit rejection, local reread"
        )
        print(f"Local output: {output}")
        return 0
    except Exception as error:
        # Raw parser, SDK and validation errors can contain credentials/config.
        print(f"FAIL: stage={stage}; error_type={type(error).__name__}; details suppressed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
