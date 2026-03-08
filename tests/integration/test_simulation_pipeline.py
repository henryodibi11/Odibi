"""Integration tests for simulation pipelines."""

import pandas as pd
import pytest
import yaml

from odibi.pipeline import PipelineManager


class TestSimulationPipeline:
    """Integration tests for simulation feature in complete pipelines."""

    def test_basic_simulation_pipeline(self, tmp_path):
        """Test a basic simulation pipeline."""
        # Create project config
        project_config = {
            "project": "simulation_test",
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
            "pipelines": [
                {
                    "pipeline": "sim_telemetry",
                    "nodes": [
                        {
                            "name": "simulated_data",
                            "read": {
                                "connection": "local",
                                "format": "simulation",
                                "options": {
                                    "simulation": {
                                        "scope": {
                                            "start_time": "2026-01-01T00:00:00Z",
                                            "timestep": "5m",
                                            "row_count": 100,
                                            "seed": 42,
                                        },
                                        "entities": {
                                            "count": 3,
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
                                                "name": "temperature",
                                                "data_type": "float",
                                                "generator": {
                                                    "type": "range",
                                                    "min": 20.0,
                                                    "max": 30.0,
                                                    "distribution": "uniform",
                                                },
                                            },
                                            {
                                                "name": "status",
                                                "data_type": "categorical",
                                                "generator": {
                                                    "type": "categorical",
                                                    "values": ["active", "idle", "error"],
                                                    "weights": [0.7, 0.2, 0.1],
                                                },
                                            },
                                            {
                                                "name": "record_id",
                                                "data_type": "int",
                                                "generator": {
                                                    "type": "sequential",
                                                    "start": 1,
                                                    "step": 1,
                                                },
                                            },
                                        ],
                                    }
                                },
                            },
                            "write": {
                                "connection": "local",
                                "format": "parquet",
                                "path": "simulated_data.parquet",
                                "mode": "overwrite",
                            },
                        }
                    ],
                }
            ],
        }

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run pipeline
        manager = PipelineManager.from_yaml(str(project_path))
        result = manager.run("sim_telemetry")

        # Verify success
        assert len(result.failed) == 0, f"Pipeline failed: {result.failed}"
        assert len(result.node_results) == 1
        assert "simulated_data" in result.node_results

        # Get the generated DataFrame
        node_result = result.node_results["simulated_data"]
        assert node_result.success

        # Read data from parquet file
        df = pd.read_parquet(tmp_path / "simulated_data.parquet")

        # Verify data
        assert len(df) == 300  # 3 sensors * 100 rows each
        assert list(df.columns) == ["sensor_id", "timestamp", "temperature", "status", "record_id"]

        # Verify sensor IDs
        assert set(df["sensor_id"].unique()) == {"sensor_01", "sensor_02", "sensor_03"}

        # Verify temperature range
        assert df["temperature"].min() >= 20.0
        assert df["temperature"].max() <= 30.0

        # Verify status values
        assert set(df["status"].unique()).issubset({"active", "idle", "error"})

        # Verify sequential IDs (per entity)
        for sensor in ["sensor_01", "sensor_02", "sensor_03"]:
            sensor_data = df[df["sensor_id"] == sensor]
            record_ids = sensor_data["record_id"].tolist()
            assert record_ids == list(range(1, 101))

    def test_simulation_with_transformations(self, tmp_path):
        """Test simulation followed by transformations."""
        project_config = {
            "project": "sim_transform_test",
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
            "pipelines": [
                {
                    "pipeline": "sim_with_transform",
                    "nodes": [
                        {
                            "name": "raw_data",
                            "read": {
                                "connection": "local",
                                "format": "simulation",
                                "options": {
                                    "simulation": {
                                        "scope": {
                                            "start_time": "2026-01-01T00:00:00Z",
                                            "timestep": "1h",
                                            "row_count": 24,
                                            "seed": 42,
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
                            "transform": {
                                "steps": [
                                    {
                                        "sql": """
                                            SELECT
                                                value,
                                                value * 2 AS value_doubled,
                                                CASE WHEN value > 50 THEN 'HIGH' ELSE 'LOW' END AS value_category
                                            FROM df
                                        """
                                    }
                                ]
                            },
                            "write": {
                                "connection": "local",
                                "format": "parquet",
                                "path": "transformed_data.parquet",
                                "mode": "overwrite",
                            },
                        }
                    ],
                }
            ],
        }

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run pipeline
        manager = PipelineManager.from_yaml(str(project_path))
        result = manager.run("sim_with_transform")

        assert len(result.failed) == 0, f"Pipeline failed: {result.failed}"

        # Read data from parquet file
        df = pd.read_parquet(tmp_path / "transformed_data.parquet")

        # Verify transformations
        assert "value_doubled" in df.columns
        assert "value_category" in df.columns

        # Verify derived column logic
        assert all(df["value_doubled"] == df["value"] * 2)
        assert all((df["value"] > 50) == (df["value_category"] == "HIGH"))

    def test_simulation_with_chaos(self, tmp_path):
        """Test simulation with chaos parameters."""
        project_config = {
            "project": "chaos_test",
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
            "pipelines": [
                {
                    "pipeline": "chaos_sim",
                    "nodes": [
                        {
                            "name": "chaotic_data",
                            "read": {
                                "connection": "local",
                                "format": "simulation",
                                "options": {
                                    "simulation": {
                                        "scope": {
                                            "start_time": "2026-01-01T00:00:00Z",
                                            "timestep": "1m",
                                            "row_count": 100,
                                            "seed": 42,
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
                                                    "min": 50,
                                                    "max": 100,
                                                },
                                                "null_rate": 0.1,  # 10% nulls
                                            },
                                        ],
                                        "chaos": {
                                            "outlier_rate": 0.05,
                                            "outlier_factor": 5.0,
                                            "duplicate_rate": 0.05,
                                        },
                                    }
                                },
                            },
                            "write": {
                                "connection": "local",
                                "format": "parquet",
                                "path": "chaotic_data.parquet",
                                "mode": "overwrite",
                            },
                        }
                    ],
                }
            ],
        }

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run pipeline
        manager = PipelineManager.from_yaml(str(project_path))
        result = manager.run("chaos_sim")

        assert len(result.failed) == 0, f"Pipeline failed: {result.failed}"

        # Read data from parquet file
        df = pd.read_parquet(tmp_path / "chaotic_data.parquet")

        # Should have some nulls (around 10%)
        null_count = df["value"].isna().sum()
        assert null_count > 0

        # Should have duplicates (more than 100 rows)
        assert len(df) > 100

        # Should have some outliers (values > 100 * 5 = 500)
        max_value = df["value"].max()
        assert max_value > 200  # Some outliers should exist

    def test_simulation_entity_overrides(self, tmp_path):
        """Test entity-specific overrides."""
        project_config = {
            "project": "override_test",
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
            "pipelines": [
                {
                    "pipeline": "entity_override_sim",
                    "nodes": [
                        {
                            "name": "override_data",
                            "read": {
                                "connection": "local",
                                "format": "simulation",
                                "options": {
                                    "simulation": {
                                        "scope": {
                                            "start_time": "2026-01-01T00:00:00Z",
                                            "timestep": "1m",
                                            "row_count": 10,
                                            "seed": 42,
                                        },
                                        "entities": {
                                            "names": ["pump_normal", "pump_hot"],
                                        },
                                        "columns": [
                                            {
                                                "name": "entity",
                                                "data_type": "string",
                                                "generator": {
                                                    "type": "constant",
                                                    "value": "{entity_id}",
                                                },
                                            },
                                            {
                                                "name": "temperature",
                                                "data_type": "float",
                                                "generator": {
                                                    "type": "range",
                                                    "min": 60,
                                                    "max": 80,
                                                },
                                                "entity_overrides": {
                                                    "pump_hot": {
                                                        "type": "range",
                                                        "min": 90,
                                                        "max": 110,
                                                    }
                                                },
                                            },
                                        ],
                                    }
                                },
                            },
                            "write": {
                                "connection": "local",
                                "format": "parquet",
                                "path": "override_data.parquet",
                                "mode": "overwrite",
                            },
                        }
                    ],
                }
            ],
        }

        # Write config
        project_path = tmp_path / "project.yaml"
        with open(project_path, "w") as f:
            yaml.dump(project_config, f)

        # Run pipeline
        manager = PipelineManager.from_yaml(str(project_path))
        result = manager.run("entity_override_sim")

        assert len(result.failed) == 0, f"Pipeline failed: {result.failed}"

        # Read data from parquet file
        df = pd.read_parquet(tmp_path / "override_data.parquet")

        # Check temperature ranges for each entity
        normal_temps = df[df["entity"] == "pump_normal"]["temperature"]
        hot_temps = df[df["entity"] == "pump_hot"]["temperature"]

        # pump_normal should be in 60-80 range
        assert normal_temps.min() >= 60
        assert normal_temps.max() <= 80

        # pump_hot should be in 90-110 range
        assert hot_temps.min() >= 90
        assert hot_temps.max() <= 110


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
