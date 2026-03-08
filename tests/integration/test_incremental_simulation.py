"""Integration tests for incremental simulation with HWM persistence."""

import pandas as pd
import pytest
import yaml

from odibi.pipeline import PipelineManager


class TestIncrementalSimulation:
    """Test incremental simulation with StateManager."""

    def test_incremental_simulation_pandas(self, tmp_path):
        """Test incremental simulation with Pandas engine."""
        # Create project config with local state backend
        project_config = {
            "project": "incremental_sim_test",
            "engine": "pandas",
            "connections": {
                "local": {
                    "type": "local",
                    "base_path": str(tmp_path),
                }
            },
            "story": {
                "connection": "local",
                "path": "stories",
            },
            "system": {
                "connection": "local",
                "path": ".odibi/catalog",
            },
        }

        # Pipeline with incremental simulation
        pipeline_config = {
            "pipeline": "continuous_telemetry",
            "nodes": [
                {
                    "name": "sensor_stream",
                    "read": {
                        "connection": "local",
                        "format": "simulation",
                        "options": {
                            "simulation": {
                                "scope": {
                                    "start_time": "2026-01-01T00:00:00Z",
                                    "timestep": "1h",
                                    "row_count": 24,  # 24 hours per run
                                    "seed": 42,
                                },
                                "entities": {
                                    "count": 2,
                                    "id_prefix": "sensor_",
                                },
                                "columns": [
                                    {
                                        "name": "sensor_id",
                                        "data_type": "string",
                                        "generator": {
                                            "type": "constant",
                                            "value": "{entity_id}",
                                        },
                                    },
                                    {
                                        "name": "timestamp",
                                        "data_type": "timestamp",
                                        "generator": {
                                            "type": "timestamp",
                                        },
                                    },
                                    {
                                        "name": "value",
                                        "data_type": "float",
                                        "generator": {
                                            "type": "range",
                                            "min": 0,
                                            "max": 100,
                                        },
                                    },
                                ],
                            }
                        },
                        "incremental": {
                            "mode": "stateful",
                            "column": "timestamp",
                            "state_key": "sensor_stream_hwm",
                        },
                    },
                    "write": {
                        "connection": "local",
                        "format": "parquet",
                        "path": "sensor_data.parquet",
                        "mode": "append",
                    },
                }
            ],
        }

        # Combine configs
        project_config["pipelines"] = [pipeline_config]

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # RUN 1: First execution
        manager1 = PipelineManager.from_yaml(str(project_path))
        result1 = manager1.run("continuous_telemetry")

        assert len(result1.failed) == 0, f"Pipeline failed: {result1.failed}"
        node_result1 = result1.node_results["sensor_stream"]
        assert node_result1.success

        # Read data from parquet file
        output_file = tmp_path / "sensor_data.parquet"
        df1 = pd.read_parquet(output_file)

        # Should have 48 rows (2 sensors × 24 hours)
        assert len(df1) == 48

        # Get timestamps from first run
        timestamps1 = sorted(df1["timestamp"].unique())
        first_ts = timestamps1[0]
        last_ts = timestamps1[-1]

        print(f"Run 1: First timestamp: {first_ts}, Last timestamp: {last_ts}")

        # RUN 2: Second execution (should pick up where it left off)
        manager2 = PipelineManager.from_yaml(str(project_path))
        result2 = manager2.run("continuous_telemetry")

        assert len(result2.failed) == 0, f"Pipeline failed: {result2.failed}"
        node_result2 = result2.node_results["sensor_stream"]
        assert node_result2.success

        # Read combined data from parquet file
        combined = pd.read_parquet(output_file)

        # Get only the new rows from run 2 (after last_ts from run 1)
        df2 = combined[combined["timestamp"] > last_ts]

        # Should have another 48 rows
        assert len(df2) == 48

        # Get timestamps from second run
        timestamps2 = sorted(df2["timestamp"].unique())
        first_ts2 = timestamps2[0]
        last_ts2 = timestamps2[-1]

        print(f"Run 2: First timestamp: {first_ts2}, Last timestamp: {last_ts2}")

        # Second run should start AFTER first run ended
        assert first_ts2 > last_ts, (
            f"Second run should start after first run. "
            f"Run 1 ended at {last_ts}, Run 2 started at {first_ts2}"
        )

        # Should have data from both runs
        # Each run: 2 sensors × 24 rows = 48 rows
        # Total: 96 rows
        assert len(combined) == 96

        # Verify no duplicate timestamps per sensor
        for sensor in ["sensor_01", "sensor_02"]:
            sensor_data = combined[combined["sensor_id"] == sensor]
            unique_timestamps = sensor_data["timestamp"].nunique()
            total_rows = len(sensor_data)
            assert unique_timestamps == total_rows, (
                f"Duplicate timestamps detected for {sensor}: "
                f"{total_rows} rows but only {unique_timestamps} unique timestamps"
            )

    def test_incremental_simulation_determinism(self, tmp_path):
        """Test that incremental simulation is deterministic with same seed."""
        project_config = {
            "project": "determinism_test",
            "engine": "pandas",
            "connections": {
                "local": {
                    "type": "local",
                    "base_path": str(tmp_path),
                }
            },
            "story": {
                "connection": "local",
                "path": "stories",
            },
            "system": {
                "connection": "local",
                "path": ".odibi/catalog",
            },
        }

        pipeline_config = {
            "pipeline": "deterministic_sim",
            "nodes": [
                {
                    "name": "data_gen",
                    "read": {
                        "connection": "local",
                        "format": "simulation",
                        "options": {
                            "simulation": {
                                "scope": {
                                    "start_time": "2026-01-01T00:00:00Z",
                                    "timestep": "5m",
                                    "row_count": 10,
                                    "seed": 42,  # Fixed seed
                                },
                                "entities": {
                                    "count": 1,
                                },
                                "columns": [
                                    {
                                        "name": "value",
                                        "data_type": "float",
                                        "generator": {
                                            "type": "range",
                                            "min": 0,
                                            "max": 100,
                                        },
                                    },
                                ],
                            }
                        },
                    },
                    "write": {
                        "connection": "local",
                        "format": "parquet",
                        "path": "test_data.parquet",
                        "mode": "overwrite",
                    },
                }
            ],
        }

        project_config["pipelines"] = [pipeline_config]

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run twice
        manager1 = PipelineManager.from_yaml(str(project_path))
        result1 = manager1.run("deterministic_sim")
        assert len(result1.failed) == 0
        df1 = pd.read_parquet(tmp_path / "test_data.parquet")
        values1 = df1["value"].tolist()

        manager2 = PipelineManager.from_yaml(str(project_path))
        result2 = manager2.run("deterministic_sim")
        assert len(result2.failed) == 0
        df2 = pd.read_parquet(tmp_path / "test_data.parquet")
        values2 = df2["value"].tolist()

        # Should be identical
        assert values1 == pytest.approx(values2), "Same seed should produce identical values"

    def test_incremental_reset_on_seed_change(self, tmp_path):
        """Test that changing seed restarts simulation."""
        project_config = {
            "project": "seed_change_test",
            "engine": "pandas",
            "connections": {
                "local": {
                    "type": "local",
                    "base_path": str(tmp_path),
                }
            },
            "story": {
                "connection": "local",
                "path": "stories",
            },
            "system": {
                "connection": "local",
                "path": ".odibi/catalog",
            },
        }

        pipeline_config = {
            "pipeline": "seeded_sim",
            "nodes": [
                {
                    "name": "data",
                    "read": {
                        "connection": "local",
                        "format": "simulation",
                        "options": {
                            "simulation": {
                                "scope": {
                                    "start_time": "2026-01-01T00:00:00Z",
                                    "timestep": "1h",
                                    "row_count": 5,
                                    "seed": 42,
                                },
                                "entities": {"count": 1},
                                "columns": [
                                    {
                                        "name": "timestamp",
                                        "data_type": "timestamp",
                                        "generator": {"type": "timestamp"},
                                    },
                                    {
                                        "name": "value",
                                        "data_type": "float",
                                        "generator": {"type": "range", "min": 0, "max": 100},
                                    },
                                ],
                            }
                        },
                        "incremental": {
                            "mode": "stateful",
                            "column": "timestamp",
                        },
                    },
                    "write": {
                        "connection": "local",
                        "format": "parquet",
                        "path": "test_data.parquet",
                        "mode": "overwrite",
                    },
                }
            ],
        }

        project_config["pipelines"] = [pipeline_config]

        # Run 1 with seed=42
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        manager1 = PipelineManager.from_yaml(str(project_path))
        result1 = manager1.run("seeded_sim")
        assert len(result1.failed) == 0
        df1 = pd.read_parquet(tmp_path / "test_data.parquet")
        values1 = df1["value"].tolist()

        # Change seed to 100
        project_config["pipelines"][0]["nodes"][0]["read"]["options"]["simulation"]["scope"][
            "seed"
        ] = 100

        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run 2 with different seed
        manager2 = PipelineManager.from_yaml(str(project_path))
        result2 = manager2.run("seeded_sim")
        assert len(result2.failed) == 0
        df2 = pd.read_parquet(tmp_path / "test_data.parquet")
        values2 = df2["value"].tolist()

        # Values should be different (different seed)
        assert values1 != pytest.approx(values2), "Different seeds should produce different values"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
