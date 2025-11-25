import sys

from odibi.pipeline import PipelineManager, ProjectConfig


def test_smoke_pandas():
    print("\n--- Smoke Test: Pandas Engine ---")
    try:
        config_data = {
            "project": "test_project",
            "engine": "pandas",
            "connections": {"local_data": {"type": "local", "base_path": "./data"}},
            "pipelines": [
                {
                    "pipeline": "test_pipeline",
                    "nodes": [
                        {
                            "name": "read_node",
                            "read": {
                                "connection": "local_data",
                                "format": "csv",
                                "path": "test.csv",
                            },
                        }
                    ],
                }
            ],
            "story": {"connection": "local_data", "path": "stories"},
        }

        # Validate config model
        project_config = ProjectConfig(**config_data)
        print("PASS: ProjectConfig validation passed")

        # Build manager
        manager = PipelineManager(
            project_config, PipelineManager._build_connections(project_config.connections)
        )
        print("PASS: PipelineManager instantiation passed")

        # Check engine type
        pipeline = manager.get_pipeline("test_pipeline")
        print(f"PASS: Pipeline engine: {type(pipeline.engine).__name__}")

        if type(pipeline.engine).__name__ != "PandasEngine":
            print("FAIL: Error: Expected PandasEngine")
            return False

        return True
    except Exception as e:
        print(f"FAIL: Failed: {e}")
        return False


def test_smoke_spark_structure():
    print("\n--- Smoke Test: Spark Engine Structure ---")
    # This tests if we can Configure it, even if we don't have Spark installed
    # We expect ImportError if pyspark is missing, but ValueError if logic is wrong.
    try:
        config_data = {
            "project": "test_project_spark",
            "engine": "spark",
            "connections": {
                "spark_catalog": {"type": "delta", "catalog": "spark_catalog", "schema": "default"}
            },
            "pipelines": [
                {
                    "pipeline": "spark_pipeline",
                    "nodes": [
                        {
                            "name": "read_node",
                            "read": {
                                "connection": "spark_catalog",
                                "format": "delta",
                                "table": "test_table",
                            },
                        }
                    ],
                }
            ],
            "story": {"connection": "spark_catalog", "path": "stories"},
        }

        project_config = ProjectConfig(**config_data)
        print("PASS: ProjectConfig validation passed")

        connections = PipelineManager._build_connections(project_config.connections)
        print(f"PASS: Connections built: {connections.keys()}")

        if "spark_catalog" not in connections:
            print("FAIL: Error: Delta connection failed to build")
            return False

        try:
            manager = PipelineManager(project_config, connections)
            print("PASS: PipelineManager instantiated (Spark available)")
            pipeline = manager.get_pipeline("spark_pipeline")
            print(f"PASS: Pipeline engine: {type(pipeline.engine).__name__}")
        except ImportError as e:
            print(f"WARN: Spark not installed (Expected in this env?): {e}")
            return True  # Pass if it fails only due to missing library

        return True
    except Exception as e:
        print(f"FAIL: Failed with unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    p = test_smoke_pandas()
    s = test_smoke_spark_structure()

    if p and s:
        print("\nPASS: All Smoke Tests Passed")
        sys.exit(0)
    else:
        print("\nFAIL: Smoke Tests Failed")
        sys.exit(1)
